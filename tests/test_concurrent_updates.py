import requests
import threading
import uuid

def update():
    requests.put(
        "http://localhost:8080/us/properties/1",
        json={"price": 999999, "version": 1},
        headers={"X-Request-ID": str(uuid.uuid4())}
    )

threads = []
for _ in range(2):
    t = threading.Thread(target=update)
    threads.append(t)
    t.start()

for t in threads:
    t.join()
