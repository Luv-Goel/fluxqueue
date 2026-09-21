"""fluxqueue demo — scripted tour of embedded + durability + DLQ + HTTP modes."""

import os
import shutil
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fluxqueue import Broker, QueueClient  # noqa: E402
from fluxqueue.server import make_server  # noqa: E402


def section(title):
    print("\n=== %s ===" % title)


def main():
    section("fluxqueue demo (stdlib only, zero dependencies)")
    d = tempfile.mkdtemp(prefix="fluxqueue-demo-")
    print("data dir:", d)
    b = Broker(d).open()
    b.create_queue("orders")
    b.create_queue("orders-dlq")
    b.set_queue_config("orders", {"visibility_timeout": 0.4,
                                  "max_receives": 2,
                                  "dead_letter_queue": "orders-dlq"})

    section("1. send + receive + ack (FIFO)")
    for i in range(3):
        b.send_message("orders", "order-%d" % i, attributes={"idx": i})
    batch = b.receive_messages("orders", max_messages=3)
    print("received:", [m["body"] for m in batch])
    b.delete_message("orders", batch[0]["receipt_handle"])
    print("acked:", batch[0]["message_id"][:8],
          "| inflight:", b.stats("orders")["orders"]["inflight"])

    section("2. visibility timeout -> redelivery (at-least-once)")
    time.sleep(0.5)
    again = b.receive_messages("orders", max_messages=3)
    print("redelivered:", [(m["body"], m["approximate_receive_count"]) for m in again])
    for m in again:
        b.delete_message("orders", m["receipt_handle"])
    print("all acked. pending:", b.stats("orders")["orders"]["pending"])

    section("3. delay queues")
    b.send_message("orders", "future-order", delay_seconds=0.4)
    print("immediate receive:", b.receive_messages("orders"))
    time.sleep(0.5)
    print("after delay:", [m["body"] for m in b.receive_messages("orders", max_messages=5)])

    section("4. poison message -> DLQ -> redrive")
    b.purge("orders")
    b.purge("orders-dlq")
    b.send_message("orders", "poison")
    for attempt in range(3):
        got = b.receive_messages("orders")
        print("attempt %d: main=%s" % (attempt + 1, [m["body"] for m in got]))
        time.sleep(0.5)
    print("main now:", b.receive_messages("orders"))
    print("dlq:", [m["body"] for m in b.receive_messages("orders-dlq")])
    print("redrive:", b.redrive_dlq("orders"))
    print("main after redrive:",
          [m["body"] for m in b.receive_messages("orders", max_messages=5)])

    section("5. crash recovery (reopen, no data loss)")
    b.purge("orders")
    b.send_message("orders", "survivor-1")
    b.send_message("orders", "survivor-2")
    b.close()
    b2 = Broker(d).open()
    print("after reopen:", [m["body"] for m in
                             b2.receive_messages("orders", max_messages=5)])
    b2.checkpoint()
    print("snapshot written:", os.path.exists(os.path.join(d, "snapshot.json")))
    b2.close()

    section("6. HTTP server + QueueClient round-trip")
    b3 = Broker(d).open()
    server = make_server(b3, "127.0.0.1", 0)
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever,
                         kwargs={"poll_interval": 0.05}, daemon=True)
    t.start()
    c = QueueClient("http://127.0.0.1:%d" % port)
    print("health:", c.health()["status"])
    c.create_queue("web")
    c.send_message("web", "hello-http")
    msgs = c.receive_messages("web")
    print("received over HTTP:", [m["body"] for m in msgs])
    c.delete_message("web", msgs[0]["receipt_handle"])
    print("stats:", c.stats()["queues"]["web"])
    server.shutdown()
    t.join(timeout=5)
    server.server_close()
    b3.close()
    shutil.rmtree(d, ignore_errors=True)

    section("done — all durability + delivery paths exercised")
    print("demo complete.")


if __name__ == "__main__":
    main()
