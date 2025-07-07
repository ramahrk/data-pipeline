import time

from prometheus_client import Counter, start_http_server

c = Counter("my_counter", "An example counter")

if __name__ == "__main__":
    start_http_server(8002)
    print("Prometheus metrics server running on http://localhost:8002/metrics")

    while True:
        c.inc()
        time.sleep(1)
