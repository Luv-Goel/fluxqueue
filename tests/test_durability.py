"""Durability tests: reopen recovery, inflight recovery, snapshot."""

import os
import tempfile
import time
import unittest

from fluxqueue.broker import Broker


def _open(data_dir, **kw):
    kw.setdefault("redrive_interval", 0.02)
    return Broker(data_dir, **kw).open()


class TestCrashRecovery(unittest.TestCase):
    def test_reopen_survives_pending(self):
        with tempfile.TemporaryDirectory() as d:
            b = _open(d)
            b.create_queue("q")
            for i in range(5):
                b.send_message("q", "m%d" % i, attributes={"i": i})
            b.close()
            b2 = _open(d)
            try:
                got = b2.receive_messages("q", max_messages=5)
                self.assertEqual([m["body"] for m in got],
                                 ["m%d" % i for i in range(5)])
                self.assertEqual(got[2]["attributes"], {"i": 2})
            finally:
                b2.close()

    def test_reopen_survives_inflight(self):
        with tempfile.TemporaryDirectory() as d:
            b = _open(d)
            b.create_queue("q", {"visibility_timeout": 30})
            b.send_message("q", "a")
            first = b.receive_messages("q")[0]
            b.close()  # crash with message inflight (no ack)
            b2 = _open(d)
            try:
                b2.delete_message("q", first["receipt_handle"])
                self.assertEqual(b2.receive_messages("q"), [])
            finally:
                b2.close()

    def test_reopen_requeued_visible(self):
        with tempfile.TemporaryDirectory() as d:
            b = _open(d)
            b.create_queue("q", {"visibility_timeout": 0.05})
            b.send_message("q", "a")
            b.receive_messages("q")
            time.sleep(0.15)
            b.close()
            b2 = _open(d)
            try:
                got = b2.receive_messages("q")
                self.assertEqual(len(got), 1)
                self.assertEqual(got[0]["approximate_receive_count"], 2)
            finally:
                b2.close()

    def test_reopen_after_purge(self):
        with tempfile.TemporaryDirectory() as d:
            b = _open(d)
            b.create_queue("q")
            b.send_message("q", "a")
            b.purge("q")
            b.close()
            b2 = _open(d)
            try:
                self.assertEqual(b2.receive_messages("q"), [])
            finally:
                b2.close()

    def test_reopen_preserves_dlq(self):
        with tempfile.TemporaryDirectory() as d:
            b = _open(d)
            b.create_queue("dlq")
            b.create_queue("main", {"visibility_timeout": 0.05,
                                    "max_receives": 2,
                                    "dead_letter_queue": "dlq"})
            b.send_message("main", "poison")
            b.receive_messages("main")
            time.sleep(0.15)
            b.receive_messages("main")
            time.sleep(0.25)
            b.receive_messages("main")
            time.sleep(0.1)
            b.close()
            b2 = _open(d)
            try:
                got = b2.receive_messages("dlq")
                self.assertEqual(len(got), 1)
                self.assertEqual(got[0]["body"], "poison")
                self.assertEqual(b2.redrive_dlq("main")["moved"], 1)
            finally:
                b2.close()

    def test_checkpoint_then_recover(self):
        with tempfile.TemporaryDirectory() as d:
            b = _open(d)
            b.create_queue("q")
            for i in range(10):
                b.send_message("q", "m%d" % i)
            b.receive_messages("q", max_messages=3)
            b.checkpoint()
            self.assertTrue(os.path.exists(os.path.join(d, "snapshot.json")))
            b.send_message("q", "after")
            b.close()
            b2 = _open(d)
            try:
                s = b2.stats("q")["q"]
                self.assertEqual(s["pending"], 8)
                self.assertEqual(s["inflight"], 3)
            finally:
                b2.close()

    def test_fsync_files_exist(self):
        with tempfile.TemporaryDirectory() as d:
            b = _open(d)
            b.create_queue("q")
            b.send_message("q", "a")
            b.close()
            logs = [f for f in os.listdir(d) if f.endswith(".jsonl")]
            self.assertTrue(logs)

    def test_torn_tail_tolerated(self):
        with tempfile.TemporaryDirectory() as d:
            b = _open(d)
            b.create_queue("q")
            b.send_message("q", "good")
            b.close()
            logs = sorted(f for f in os.listdir(d) if f.endswith(".jsonl"))
            with open(os.path.join(d, logs[-1]), "a", encoding="utf-8") as f:
                f.write('{"op": "send", "queue": "q", "message": BROKEN\n')
            b2 = _open(d)
            try:
                got = b2.receive_messages("q")
                self.assertEqual(len(got), 1)
                self.assertEqual(got[0]["body"], "good")
            finally:
                b2.close()

    def test_segment_rotation(self):
        with tempfile.TemporaryDirectory() as d:
            b = _open(d, segment_bytes=64 * 1024)
            b.create_queue("q")
            for i in range(300):
                b.send_message("q", "payload-%d-%s" % (i, "x" * 200))
            b.close()
            logs = sorted(f for f in os.listdir(d) if f.endswith(".jsonl"))
            self.assertGreaterEqual(len(logs), 2)
            b2 = _open(d)
            try:
                self.assertEqual(b2.stats("q")["q"]["pending"], 300)
            finally:
                b2.close()


if __name__ == "__main__":
    unittest.main()
