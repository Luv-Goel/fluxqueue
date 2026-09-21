"""HTTP API tests: full round-trip via QueueClient + error mapping."""

import tempfile
import threading
import time
import unittest

from fluxqueue.broker import Broker
from fluxqueue.client import FluxClientError, QueueClient
from fluxqueue.server import make_server


class ServerCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.broker = Broker(self.tmp.name, redrive_interval=0.02).open()
        self.server = make_server(self.broker, "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       kwargs={"poll_interval": 0.02},
                                       daemon=True)
        self.thread.start()
        self.c = QueueClient("http://127.0.0.1:%d" % self.port, timeout=10.0)

    def tearDown(self):
        self.server.shutdown()
        self.thread.join(timeout=5)
        self.server.server_close()
        self.broker.close()
        self.tmp.cleanup()


class TestHttpApi(ServerCase):
    def test_health(self):
        h = self.c.health()
        self.assertEqual(h["status"], "ok")

    def test_round_trip(self):
        self.c.create_queue("jobs")
        self.c.send_message("jobs", "task-1", attributes={"p": "1"})
        msgs = self.c.receive_messages("jobs")
        self.assertEqual(len(msgs), 1)
        self.assertEqual(msgs[0]["body"], "task-1")
        self.c.delete_message("jobs", msgs[0]["receipt_handle"])
        self.assertEqual(self.c.receive_messages("jobs"), [])

    def test_fifo_over_http(self):
        self.c.create_queue("q")
        for i in range(5):
            self.c.send_message("q", "m%d" % i)
        got = self.c.receive_messages("q", max_messages=5)
        self.assertEqual([m["body"] for m in got], ["m%d" % i for i in range(5)])

    def test_create_with_dlq_config(self):
        self.c.create_queue("dlq")
        info = self.c.create_queue("main", {"visibility_timeout": 0.05,
                                            "max_receives": 1,
                                            "dead_letter_queue": "dlq"})
        self.assertEqual(info["config"]["dead_letter_queue"], "dlq")

    def test_404_unknown_queue(self):
        with self.assertRaises(FluxClientError) as ctx:
            self.c.receive_messages("missing")
        self.assertEqual(ctx.exception.status, 404)

    def test_404_bad_receipt(self):
        self.c.create_queue("q")
        with self.assertRaises(FluxClientError) as ctx:
            self.c.delete_message("q", "bad-receipt")
        self.assertEqual(ctx.exception.status, 404)

    def test_400_bad_input(self):
        with self.assertRaises(FluxClientError) as ctx:
            self.c.create_queue("bad name!")
        self.assertEqual(ctx.exception.status, 400)

    def test_change_visibility_http(self):
        self.c.create_queue("q", {"visibility_timeout": 0.06})
        self.c.send_message("q", "a")
        got = self.c.receive_messages("q")[0]
        self.c.change_visibility("q", got["receipt_handle"], 5.0)
        time.sleep(0.12)
        self.assertEqual(self.c.receive_messages("q"), [])

    def test_purge_http(self):
        self.c.create_queue("q")
        self.c.send_message("q", "a")
        self.c.send_message("q", "b")
        res = self.c.purge("q")
        self.assertEqual(res["purged_pending"], 2)
        self.assertEqual(self.c.receive_messages("q"), [])

    def test_stats_http(self):
        self.c.create_queue("q")
        self.c.send_message("q", "a")
        s = self.c.queue_stats("q")
        self.assertEqual(s["queue"], "q")
        self.assertEqual(s["pending"], 1)
        all_stats = self.c.stats()
        self.assertIn("q", all_stats["queues"])
        self.assertEqual(self.c.list_queues(), ["q"])

    def test_dlq_redrive_http(self):
        self.c.create_queue("dlq")
        self.c.create_queue("main", {"visibility_timeout": 0.05,
                                     "max_receives": 2,
                                     "dead_letter_queue": "dlq"})
        self.c.send_message("main", "poison")
        self.c.receive_messages("main")
        time.sleep(0.12)
        self.c.receive_messages("main")
        time.sleep(0.25)
        self.c.receive_messages("main")
        time.sleep(0.1)
        dlq_msgs = self.c.receive_messages("dlq")
        self.assertEqual(len(dlq_msgs), 1)
        res = self.c.redrive("main")
        self.assertEqual(res["moved"], 1)
        got = self.c.receive_messages("main")
        self.assertEqual(len(got), 1)

    def test_long_poll_http(self):
        self.c.create_queue("q")

        def sender():
            time.sleep(0.15)
            self.c.send_message("q", "late")

        t = threading.Thread(target=sender)
        t.start()
        got = self.c.receive_messages("q", wait_time_seconds=3.0)
        t.join()
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0]["body"], "late")

    def test_concurrent_http_producers(self):
        self.c.create_queue("q")
        errors = []

        def produce(base):
            try:
                for i in range(10):
                    self.c.send_message("q", "t%s-%d" % (base, i))
            except Exception as e:  # pragma: no cover
                errors.append(e)

        threads = [threading.Thread(target=produce, args=(t,)) for t in range(4)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        self.assertEqual(errors, [])
        self.assertEqual(self.c.queue_stats("q")["pending"], 40)

    def test_checkpoint_http(self):
        self.c.create_queue("q")
        self.c.send_message("q", "a")
        self.assertEqual(self.c.checkpoint()["checkpoint"], "ok")


if __name__ == "__main__":
    unittest.main()
