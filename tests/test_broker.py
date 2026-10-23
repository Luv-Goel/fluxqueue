"""Broker unit tests: FIFO, visibility, receipts, delay, DLQ/redrive, stats."""

import tempfile
import threading
import time
import unittest

from fluxqueue.broker import Broker
from fluxqueue.models import InvalidArgumentError, QueueNotFoundError
from fluxqueue.models import ReceiptNotFoundError


class BrokerCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.b = Broker(self.tmp.name, redrive_interval=0.02).open()

    def tearDown(self):
        self.b.close()
        self.tmp.cleanup()


class TestQueues(BrokerCase):
    def test_create_and_list(self):
        self.b.create_queue("a")
        self.b.create_queue("b")
        self.assertEqual(self.b.list_queues(), ["a", "b"])

    def test_create_idempotent(self):
        self.b.create_queue("a", {"visibility_timeout": 5})
        self.b.create_queue("a", {"visibility_timeout": 9})
        self.assertEqual(self.b.get_queue_config("a")["visibility_timeout"], 9.0)

    def test_bad_queue_name(self):
        with self.assertRaises(InvalidArgumentError):
            self.b.create_queue("bad name!")
        with self.assertRaises(InvalidArgumentError):
            self.b.create_queue("")

    def test_delete_queue(self):
        self.b.create_queue("a")
        self.b.delete_queue("a")
        self.assertEqual(self.b.list_queues(), [])
        with self.assertRaises(QueueNotFoundError):
            self.b.delete_queue("a")

    def test_send_unknown_queue(self):
        with self.assertRaises(QueueNotFoundError):
            self.b.send_message("nope", "x")

    def test_bad_config(self):
        self.b.create_queue("ok")
        with self.assertRaises(InvalidArgumentError):
            self.b.create_queue("x", {"visibility_timeout": -1})
        with self.assertRaises(InvalidArgumentError):
            self.b.create_queue("x", {"max_receives": -2})
        with self.assertRaises(InvalidArgumentError):
            self.b.create_queue("x", {"bogus": 1})
        with self.assertRaises(InvalidArgumentError):
            self.b.create_queue("x", {"dead_letter_queue": "missing-dlq"})


class TestFifo(BrokerCase):
    def test_fifo_order(self):
        self.b.create_queue("q")
        for i in range(5):
            self.b.send_message("q", "m%d" % i)
        got = self.b.receive_messages("q", max_messages=5)
        self.assertEqual([m["body"] for m in got], ["m%d" % i for i in range(5)])

    def test_batch_limit(self):
        self.b.create_queue("q")
        for i in range(15):
            self.b.send_message("q", "m%d" % i)
        got = self.b.receive_messages("q", max_messages=50)
        self.assertEqual(len(got), 10)

    def test_message_fields(self):
        self.b.create_queue("q")
        r = self.b.send_message("q", "hi", attributes={"k": "v"})
        self.assertIn("message_id", r)
        self.assertIn("sent_timestamp", r)
        got = self.b.receive_messages("q")[0]
        self.assertEqual(got["body"], "hi")
        self.assertEqual(got["attributes"], {"k": "v"})
        self.assertEqual(got["approximate_receive_count"], 1)
        self.assertIn("receipt_handle", got)

    def test_body_limit(self):
        self.b.create_queue("q")
class TestVisibility(BrokerCase):
    def test_invisible_until_timeout(self):
        self.b.create_queue("q", {"visibility_timeout": 10})
        self.b.send_message("q", "a")
        self.assertEqual(len(self.b.receive_messages("q")), 1)
        self.assertEqual(self.b.receive_messages("q"), [])

    def test_redelivery_after_timeout(self):
        self.b.create_queue("q", {"visibility_timeout": 0.05})
        self.b.send_message("q", "a")
        first = self.b.receive_messages("q")[0]
        self.assertEqual(first["approximate_receive_count"], 1)
        time.sleep(0.15)
        second = self.b.receive_messages("q")[0]
        self.assertEqual(second["message_id"], first["message_id"])
        self.assertEqual(second["approximate_receive_count"], 2)

    def test_delete_prevents_redelivery(self):
        self.b.create_queue("q", {"visibility_timeout": 0.05})
        self.b.send_message("q", "a")
        got = self.b.receive_messages("q")[0]
        self.b.delete_message("q", got["receipt_handle"])
        time.sleep(0.12)
        self.assertEqual(self.b.receive_messages("q"), [])

    def test_receipt_single_use(self):
        self.b.create_queue("q")
        self.b.send_message("q", "a")
        got = self.b.receive_messages("q")[0]
        self.b.delete_message("q", got["receipt_handle"])
        with self.assertRaises(ReceiptNotFoundError):
            self.b.delete_message("q", got["receipt_handle"])

    def test_bad_receipt(self):
        self.b.create_queue("q")
        with self.assertRaises(ReceiptNotFoundError):
            self.b.delete_message("q", "nope")
        with self.assertRaises(ReceiptNotFoundError):
            self.b.change_visibility("q", "nope", 5)

    def test_change_visibility_extend(self):
        self.b.create_queue("q", {"visibility_timeout": 0.08})
        self.b.send_message("q", "a")
        got = self.b.receive_messages("q")[0]
        self.b.change_visibility("q", got["receipt_handle"], 5.0)
        time.sleep(0.15)
        self.assertEqual(self.b.receive_messages("q"), [])

    def test_change_visibility_zero(self):
        self.b.create_queue("q", {"visibility_timeout": 30})
        self.b.send_message("q", "a")
        got = self.b.receive_messages("q")[0]
        self.b.change_visibility("q", got["receipt_handle"], 0)
        again = self.b.receive_messages("q")
        self.assertEqual(len(again), 1)
        self.assertEqual(again[0]["message_id"], got["message_id"])

    def test_change_visibility_bad_value(self):
        self.b.create_queue("q")
        self.b.send_message("q", "a")
        got = self.b.receive_messages("q")[0]
        with self.assertRaises(InvalidArgumentError):
            self.b.change_visibility("q", got["receipt_handle"], -1)

    def test_long_poll_arrival(self):
        self.b.create_queue("q")

        def sender():
            time.sleep(0.1)
            self.b.send_message("q", "late")

        t = threading.Thread(target=sender)
        t.start()
        start = time.time()
        got = self.b.receive_messages("q", wait_time_seconds=2.0)
        elapsed = time.time() - start
        t.join()
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0]["body"], "late")
        self.assertLess(elapsed, 2.0)

    def test_long_poll_timeout(self):
        self.b.create_queue("q")
        start = time.time()
        self.assertEqual(self.b.receive_messages("q", wait_time_seconds=0.15), [])
        self.assertGreaterEqual(time.time() - start, 0.1)


class TestDelayReal(BrokerCase):
    def test_delayed_not_visible(self):
        self.b.create_queue("q")
        self.b.send_message("q", "later", delay_seconds=0.3)
        self.assertEqual(self.b.receive_messages("q"), [])
        time.sleep(0.4)
        got = self.b.receive_messages("q")
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0]["body"], "later")

    def test_delay_zero(self):
        self.b.create_queue("q")
        self.b.send_message("q", "now", delay_seconds=0)
        self.assertEqual(len(self.b.receive_messages("q")), 1)

    def test_delay_bad_value(self):
        self.b.create_queue("q")
        with self.assertRaises(InvalidArgumentError):
            self.b.send_message("q", "x", delay_seconds=-1)


class TestDelay(BrokerCase):
    pass


class TestDlq(BrokerCase):
    def _setup_dlq(self, max_receives=2):
        self.b.create_queue("dlq")
        self.b.create_queue("main", {"visibility_timeout": 0.05,
                                     "max_receives": max_receives,
                                     "dead_letter_queue": "dlq"})
        self.b.send_message("main", "poison")

    def test_dlq_move(self):
        # max_receives=2: two deliveries, then the message goes to the DLQ.
        self._setup_dlq(max_receives=2)
        first = self.b.receive_messages("main")[0]
        self.assertEqual(first["approximate_receive_count"], 1)
        time.sleep(0.15)
        second = self.b.receive_messages("main")[0]
        self.assertEqual(second["approximate_receive_count"], 2)
        time.sleep(0.2)
        self.assertEqual(self.b.receive_messages("main"), [])
        time.sleep(0.1)
        dlq_msgs = self.b.receive_messages("dlq")
        self.assertEqual(len(dlq_msgs), 1)
        self.assertEqual(dlq_msgs[0]["body"], "poison")

    def test_dlq_move_single_receive(self):
        # max_receives=1: one delivery; the next receive attempt routes to DLQ.
        self._setup_dlq(max_receives=1)
        first = self.b.receive_messages("main")[0]
        self.assertEqual(first["approximate_receive_count"], 1)
        time.sleep(0.15)
        self.assertEqual(self.b.receive_messages("main"), [])
        dlq_msgs = self.b.receive_messages("dlq")
        self.assertEqual(len(dlq_msgs), 1)
        self.assertEqual(dlq_msgs[0]["body"], "poison")

    def test_no_dlq_redelivers_forever(self):
        self.b.create_queue("q", {"visibility_timeout": 0.05})
        self.b.send_message("q", "a")
        for _ in range(3):
            got = self.b.receive_messages("q")
            if got:
                time.sleep(0.1)
        self.assertEqual(len(self.b.receive_messages("q")), 1)

    def test_redrive(self):
        self._setup_dlq(max_receives=2)
        self.b.receive_messages("main")
        time.sleep(0.15)
        self.b.receive_messages("main")
        time.sleep(0.25)
        # The 3rd receive attempt routes the message straight to the DLQ,
        # so drain `main` first; the sweep may have already moved it on expiry.
        self.b.receive_messages("main")
        time.sleep(0.1)
        dlq_msgs = self.b.receive_messages("dlq")
        self.assertEqual(len(dlq_msgs), 1)
        res = self.b.redrive_dlq("main")
        self.assertEqual(res["moved"], 1)
        got = self.b.receive_messages("main")
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0]["body"], "poison")
        self.assertEqual(got[0]["approximate_receive_count"], 1)

    def test_redrive_no_dlq_configured(self):
        self.b.create_queue("plain")
        with self.assertRaises(InvalidArgumentError):
            self.b.redrive_dlq("plain")


class TestPeek(BrokerCase):
    def test_peek_returns_messages_without_receipt(self):
        self.b.create_queue("q")
        for i in range(3):
            self.b.send_message("q", "m%d" % i)
        got = self.b.peek_messages("q", max_messages=3)
        self.assertEqual(len(got), 3)
        for m in got:
            self.assertNotIn("receipt_handle", m)
            self.assertNotIn("visibility_deadline", m)
            self.assertIn("message_id", m)
            self.assertIn("body", m)
        self.assertEqual([m["body"] for m in got], ["m0", "m1", "m2"])

    def test_peek_does_not_hide_messages(self):
        self.b.create_queue("q")
        self.b.send_message("q", "a")
        self.b.peek_messages("q")
        self.b.peek_messages("q")
        received = self.b.receive_messages("q")
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0]["body"], "a")

    def test_peek_does_not_increment_receive_count(self):
        self.b.create_queue("q")
        self.b.send_message("q", "a")
        self.b.peek_messages("q")
        self.b.peek_messages("q")
        got = self.b.receive_messages("q")
        self.assertEqual(got[0]["approximate_receive_count"], 1)

    def test_peek_respects_max_messages(self):
        self.b.create_queue("q")
        for i in range(5):
            self.b.send_message("q", "m%d" % i)
        got = self.b.peek_messages("q", max_messages=2)
        self.assertEqual(len(got), 2)

    def test_peek_unknown_queue_raises(self):
        with self.assertRaises(QueueNotFoundError):
            self.b.peek_messages("nope")

    def test_peek_skips_delayed_messages(self):
        self.b.create_queue("q")
        self.b.send_message("q", "later", delay_seconds=0.3)
        self.assertEqual(self.b.peek_messages("q"), [])
        time.sleep(0.4)
        got = self.b.peek_messages("q")
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0]["body"], "later")


class TestPurgeStats(BrokerCase):
    def test_purge(self):
        self.b.create_queue("q")
        for i in range(3):
            self.b.send_message("q", "m%d" % i)
        self.b.receive_messages("q", max_messages=1)
        res = self.b.purge("q")
        self.assertEqual(res["purged_pending"], 2)
        self.assertEqual(res["purged_inflight"], 1)
        self.assertEqual(self.b.receive_messages("q"), [])

    def test_stats(self):
        self.b.create_queue("q")
        self.b.send_message("q", "a")
        self.b.send_message("q", "b", delay_seconds=30)
        self.b.receive_messages("q", max_messages=1)
        s = self.b.stats("q")["q"]
        self.assertEqual(s["pending"], 1)
        self.assertEqual(s["inflight"], 1)
        self.assertEqual(s["delayed"], 1)
        self.assertIn("q", self.b.stats())

    def test_checkpoint_runs(self):
        self.b.create_queue("q")
        self.b.send_message("q", "a")
        self.b.checkpoint()
        self.assertEqual(len(self.b.receive_messages("q")), 1)


class TestConcurrency(BrokerCase):
    def test_concurrent_producers(self):
        self.b.create_queue("q")
        errors = []

        def produce(base):
            try:
                for i in range(20):
                    self.b.send_message("q", "t%s-%d" % (base, i))
            except Exception as e:  # pragma: no cover
                errors.append(e)

        threads = [threading.Thread(target=produce, args=(t,)) for t in range(5)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        self.assertEqual(errors, [])
        self.assertEqual(self.b.stats("q")["q"]["pending"], 100)

    def test_concurrent_consume_no_duplicates(self):
        self.b.create_queue("q", {"visibility_timeout": 30})
        for i in range(40):
            self.b.send_message("q", "m%d" % i)
        seen = []
        lock = threading.Lock()

        def consume():
            while True:
                batch = self.b.receive_messages("q", max_messages=5)
                if not batch:
                    return
                with lock:
                    seen.extend(m["message_id"] for m in batch)

        threads = [threading.Thread(target=consume) for _ in range(4)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        self.assertEqual(len(seen), 40)
        self.assertEqual(len(set(seen)), 40)


if __name__ == "__main__":
    unittest.main()

    def test_delayed_not_visible(self):
        self.b.create_queue("q")
        self.b.send_message("q", "later", delay_seconds=0.3)
        self.assertEqual(self.b.receive_messages("q"), [])
        time.sleep(0.4)
        got = self.b.receive_messages("q")
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0]["body"], "later")

    def test_delay_zero(self):
        self.b.create_queue("q")
        self.b.send_message("q", "now", delay_seconds=0)
        self.assertEqual(len(self.b.receive_messages("q")), 1)

    def test_delay_bad_value(self):
        self.b.create_queue("q")
        with self.assertRaises(InvalidArgumentError):
            self.b.send_message("q", "x", delay_seconds=-1)

        with self.assertRaises(InvalidArgumentError):
            self.b.send_message("q", "x" * (256 * 1024 + 1))

    def test_attributes_roundtrip(self):
        self.b.create_queue("q")
        attrs = {"s": "x", "i": 3, "f": 1.5, "b": True, "n": None}
        self.b.send_message("q", "x", attributes=attrs)
        self.assertEqual(self.b.receive_messages("q")[0]["attributes"], attrs)


if __name__ == "__main__":
    unittest.main()
