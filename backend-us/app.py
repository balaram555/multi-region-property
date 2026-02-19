import os
import json
import time
import threading
from datetime import datetime
from flask import Flask, request, jsonify
import psycopg2
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable

app = Flask(__name__)

REGION = os.getenv("REGION")
DATABASE_URL = os.getenv("DATABASE_URL")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")

last_kafka_timestamp = None  # Track replication lag

# ----------------------------
# Database Connection (Retry)
# ----------------------------
def get_db_connection():
    while True:
        try:
            conn = psycopg2.connect(DATABASE_URL)
            conn.autocommit = False
            print("✅ Connected to database")
            return conn
        except psycopg2.OperationalError:
            print("⏳ Waiting for database...")
            time.sleep(3)

conn = get_db_connection()

# ----------------------------
# Kafka Producer (Retry)
# ----------------------------
def connect_kafka_producer():
    while True:
        try:
            print("⏳ Connecting to Kafka Producer...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print("✅ Connected to Kafka Producer")
            return producer
        except NoBrokersAvailable:
            print("❌ Kafka not ready. Retrying in 5 seconds...")
            time.sleep(5)

producer = connect_kafka_producer()

# ----------------------------
# Kafka Consumer (Retry)
# ----------------------------
def start_consumer():
    global last_kafka_timestamp

    while True:
        try:
            print("⏳ Connecting to Kafka Consumer...")
            consumer = KafkaConsumer(
                'property-updates',
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset='earliest',
                group_id=f"{REGION}-group"
            )
            print("✅ Kafka Consumer Connected")

            for message in consumer:
                data = message.value

                if data["region_origin"] != REGION:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE properties
                            SET price=%s, version=%s, updated_at=%s
                            WHERE id=%s
                        """, (
                            data["price"],
                            data["version"],
                            data["updated_at"],
                            data["id"]
                        ))
                        conn.commit()

                    # Track last replication time
                    last_kafka_timestamp = data["updated_at"]

        except Exception as e:
            print("❌ Kafka Consumer error:", e)
            print("🔁 Retrying consumer in 5 seconds...")
            time.sleep(5)

threading.Thread(target=start_consumer, daemon=True).start()

# ----------------------------
# Health Check
# ----------------------------
@app.route("/health")
def health():
    return "OK", 200

# ----------------------------
# Replication Lag Endpoint
# ----------------------------
@app.route("/replication-lag")
def replication_lag():
    global last_kafka_timestamp

    if not last_kafka_timestamp:
        return jsonify({"lag_seconds": 0})

    last_time = datetime.strptime(last_kafka_timestamp, "%Y-%m-%dT%H:%M:%S")
    lag = (datetime.utcnow() - last_time).total_seconds()

    return jsonify({"lag_seconds": lag})

# ----------------------------
# Update Property API
# ----------------------------
@app.route("/properties/<int:id>", methods=["PUT"])
def update_property(id):
    request_id = request.headers.get("X-Request-ID")
    data = request.json

    if not request_id:
        return jsonify({"error": "Missing X-Request-ID"}), 400

    with conn.cursor() as cur:

        # Idempotency Check
        cur.execute(
            "SELECT 1 FROM idempotency_keys WHERE request_id=%s",
            (request_id,)
        )
        if cur.fetchone():
            return jsonify({"error": "Duplicate request"}), 422

        # Optimistic Locking
        cur.execute("SELECT version FROM properties WHERE id=%s", (id,))
        row = cur.fetchone()

        if not row:
            return jsonify({"error": "Property not found"}), 404

        current_version = row[0]

        if current_version != data["version"]:
            return jsonify({"error": "Conflict"}), 409

        new_version = current_version + 1

        cur.execute("""
            UPDATE properties
            SET price=%s, version=%s, updated_at=NOW()
            WHERE id=%s
        """, (data["price"], new_version, id))

        cur.execute(
            "INSERT INTO idempotency_keys (request_id) VALUES (%s)",
            (request_id,)
        )

        conn.commit()

        event = {
            "id": id,
            "price": data["price"],
            "bedrooms": 3,
            "bathrooms": 2,
            "region_origin": REGION,
            "version": new_version,
            "updated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        }

        producer.send("property-updates", event)
        producer.flush()

        return jsonify(event), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
