import os
import json
import time
from flask import Flask, request, jsonify
import psycopg2
from kafka import KafkaProducer, KafkaConsumer

app = Flask(__name__)

REGION = os.getenv("REGION")
DATABASE_URL = os.getenv("DATABASE_URL")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")

conn = psycopg2.connect(DATABASE_URL)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode()
)

def start_consumer():
    consumer = KafkaConsumer(
        'property-updates',
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode()),
        auto_offset_reset='earliest',
        group_id=f"{REGION}-group"
    )
    for message in consumer:
        data = message.value
        if data["region_origin"] != REGION:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE properties
                    SET price=%s, version=%s, updated_at=%s
                    WHERE id=%s
                """, (data["price"], data["version"], data["updated_at"], data["id"]))
                conn.commit()

import threading
threading.Thread(target=start_consumer, daemon=True).start()

@app.route("/health")
def health():
    return "OK", 200

@app.route("/<region>/properties/<int:id>", methods=["PUT"])
def update_property(region, id):
    request_id = request.headers.get("X-Request-ID")
    data = request.json

    with conn.cursor() as cur:
        # Idempotency check
        cur.execute("SELECT 1 FROM idempotency_keys WHERE request_id=%s", (request_id,))
        if cur.fetchone():
            return jsonify({"error": "Duplicate request"}), 422

        # Optimistic locking
        cur.execute("SELECT version FROM properties WHERE id=%s", (id,))
        current_version = cur.fetchone()[0]

        if current_version != data["version"]:
            return jsonify({"error": "Conflict"}), 409

        new_version = current_version + 1

        cur.execute("""
            UPDATE properties
            SET price=%s, version=%s, updated_at=NOW()
            WHERE id=%s
        """, (data["price"], new_version, id))

        cur.execute("INSERT INTO idempotency_keys VALUES (%s)", (request_id,))
        conn.commit()

        event = {
            "id": id,
            "price": data["price"],
            "bedrooms": 3,
            "bathrooms": 2,
            "region_origin": REGION,
            "version": new_version,
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S")
        }

        producer.send("property-updates", event)

        return jsonify(event), 200
