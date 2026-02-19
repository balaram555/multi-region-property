import threading
import requests
import uuid

URL = "http://localhost:8080/us/properties/10"

def update_property():
    headers = {
        "Content-Type": "application/json",
        "X-Request-ID": str(uuid.uuid4())
    }
    payload = {
        "price": 700000,
        "version": 1
    }

    response = requests.put(URL, json=payload, headers=headers)
    print(response.status_code, response.text)

threads = []

for _ in range(2):
    t = threading.Thread(target=update_property)
    threads.append(t)
    t.start()

for t in threads:
    t.join()
