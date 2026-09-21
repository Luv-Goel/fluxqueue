"""fluxqueue.broker — thread-safe durable FIFO broker core."""

from __future__ import annotations

import bisect
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from fluxqueue.models import (
    FluxError,
    InvalidArgumentError,
    Message,
    QueueNotFoundError,
    ReceiptNotFoundError,
)
from fluxqueue.store import SegmentLog

MAX_BODY_BYTES = 256 * 1024
MAX_QUEUE_NAME_LEN = 80
MAX_RECEIVE_BATCH = 10
MAX_WAIT_SECONDS = 20.0
MAX_VISIBILITY_SECONDS = 43200.0
POLL_STEP = 0.02


def _check_queue_name(name: Any) -> str:
    if not isinstance(name, str) or not name:
        raise InvalidArgumentError("queue name must be a non-empty string")
    if len(name) > MAX_QUEUE_NAME_LEN:
        raise InvalidArgumentError("queue name too long (max 80 chars)")
    for ch in name:
        if not (ch.isalnum() or ch in ("-", "_")):
            raise InvalidArgumentError(
                "queue name may only contain letters, digits, '-' and '_'")
    return name


def _check_body(body: Any) -> str:
    if not isinstance(body, str):
        raise InvalidArgumentError("message body must be a string")
    if len(body.encode("utf-8")) > MAX_BODY_BYTES:
        raise InvalidArgumentError("message body exceeds 256 KiB limit")
    return body


def _check_attributes(attrs: Any) -> Dict[str, Any]:
    if attrs is None:
        return {}
    if not isinstance(attrs, dict):
        raise InvalidArgumentError("attributes must be an object")
    if len(attrs) > 10:
        raise InvalidArgumentError("at most 10 attributes per message")
    out: Dict[str, Any] = {}
    for k, v in attrs.items():
        if not isinstance(k, str) or not k:
            raise InvalidArgumentError("attribute names must be non-empty strings")
        if not isinstance(v, (str, int, float, bool)) and v is not None:
            raise InvalidArgumentError(
                "attribute values must be JSON scalars (str/int/float/bool/null)")
        out[k] = v
    return out


@dataclass
class _Inflight:
    message: Message
    deadline: float
    queue: str


@dataclass
class _Queue:
    name: str
    config: Dict[str, Any] = field(default_factory=dict)
    pending: List[Message] = field(default_factory=list)  # seq-sorted
    inflight: Dict[str, _Inflight] = field(default_factory=dict)


class Broker:
    """Durable, thread-safe, at-least-once FIFO message broker."""

    def __init__(self, data_dir: str, default_visibility_timeout: float = 30.0,
                 redrive_interval: float = 0.05, segment_bytes: int = 1_000_000,
                 fsync: bool = True) -> None:
        if default_visibility_timeout < 0 or default_visibility_timeout > MAX_VISIBILITY_SECONDS:
            raise InvalidArgumentError("bad default_visibility_timeout")
        self.data_dir = data_dir
        self.default_visibility_timeout = float(default_visibility_timeout)
        self.redrive_interval = max(0.01, float(redrive_interval))
        self._lock = threading.RLock()
        self._queues: Dict[str, _Queue] = {}
        self._origins: Dict[str, str] = {}  # message_id -> source queue (DLQ redrive)
        self._seq = 0
        self._log = SegmentLog(data_dir, segment_bytes=segment_bytes, fsync=fsync)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._opened = False

    # -- lifecycle ------------------------------------------------------
    def open(self) -> "Broker":
        with self._lock:
            if self._opened:
                return self
            self._log.open()
            self._recover()
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._sweep_loop, name="fluxqueue-redrive", daemon=True)
            self._thread.start()
            self._opened = True
            return self

    def close(self) -> None:
        with self._lock:
            if not self._opened:
                return
            self._opened = False
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None
        with self._lock:
            self._log.close()

    def __enter__(self) -> "Broker":
        return self.open()

    def __exit__(self, *exc) -> None:
        self.close()

    def checkpoint(self) -> None:
        """Persist a snapshot of all state and truncate the log."""
        with self._lock:
            self._sweep_locked(time.time())
            state = {
                "seq": self._seq,
                "origins": dict(self._origins),
                "queues": {
                    name: {
                        "config": dict(q.config),
                        "pending": [m.to_dict() for m in q.pending],
                        "inflight": [
                            {"receipt": r, "deadline": e.deadline,
                             "queue": e.queue, "message": e.message.to_dict()}
                            for r, e in q.inflight.items()
                        ],
                    }
                    for name, q in self._queues.items()
                },
            }
            self._log.save_snapshot(state)

    # -- queues -------------------------------------------------------
    def create_queue(self, name: str, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        _check_queue_name(name)
        cfg = self._normalize_config(config)
        with self._lock:
            q = self._queues.get(name)
            if q is not None:
                q.config.update(cfg)
                return self._queue_info_locked(name)
            queue_cfg = {
                "visibility_timeout": cfg.get("visibility_timeout",
                                              self.default_visibility_timeout),
                "max_receives": int(cfg.get("max_receives", 0)),
                "dead_letter_queue": cfg.get("dead_letter_queue"),
            }
            dlq = queue_cfg["dead_letter_queue"]
            if dlq is not None and dlq not in self._queues:
                raise InvalidArgumentError(
                    "dead_letter_queue '%s' does not exist" % dlq)
            self._log.append({"op": "create_queue", "queue": name,
                              "config": dict(queue_cfg)})
            self._queues[name] = _Queue(name=name, config=dict(queue_cfg))
            return self._queue_info_locked(name)

    def delete_queue(self, name: str) -> None:
        _check_queue_name(name)
        with self._lock:
            if name not in self._queues:
                raise QueueNotFoundError("queue '%s' not found" % name)
            self._log.append({"op": "delete_queue", "queue": name})
            del self._queues[name]

    def list_queues(self) -> List[str]:
        with self._lock:
            return sorted(self._queues)

    def get_queue_config(self, name: str) -> Dict[str, Any]:
        with self._lock:
            return dict(self._require_locked(name).config)

    def set_queue_config(self, name: str, config: Dict[str, Any]) -> Dict[str, Any]:
        return self.create_queue(name, config)

    def _normalize_config(self, config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if config is None:
            return {}
        if not isinstance(config, dict):
            raise InvalidArgumentError("queue config must be an object")
        out: Dict[str, Any] = {}
        if "visibility_timeout" in config:
            vt = config["visibility_timeout"]
            if not isinstance(vt, bool) and isinstance(vt, (int, float)) \
                    and 0 <= vt <= MAX_VISIBILITY_SECONDS:
                out["visibility_timeout"] = float(vt)
            else:
                raise InvalidArgumentError("visibility_timeout must be 0..43200")
        if "max_receives" in config:
            mr = config["max_receives"]
            if isinstance(mr, bool) or not isinstance(mr, int) or mr < 0 or mr > 1000:
                raise InvalidArgumentError("max_receives must be int 0..1000")
            out["max_receives"] = mr
        if "dead_letter_queue" in config:
            dlq = config["dead_letter_queue"]
            if dlq is not None:
                if not isinstance(dlq, str) or not dlq:
                    raise InvalidArgumentError("dead_letter_queue must be name or null")
                _check_queue_name(dlq)
            out["dead_letter_queue"] = dlq
        for k in config:
            if k not in ("visibility_timeout", "max_receives", "dead_letter_queue"):
                raise InvalidArgumentError("unknown queue option '%s'" % k)
        return out

    def _require_locked(self, name: str) -> _Queue:
        try:
            return self._queues[name]
        except KeyError:
            raise QueueNotFoundError("queue '%s' not found" % name)

    def _queue_info_locked(self, name: str) -> Dict[str, Any]:
        q = self._queues[name]
        return {"queue": name, "config": dict(q.config)}

    # -- send -----------------------------------------------------------
    def send_message(self, queue: str, body: str,
                     attributes: Optional[Dict[str, Any]] = None,
                     delay_seconds: float = 0.0) -> Dict[str, Any]:
        _check_queue_name(queue)
        _check_body(body)
        attrs = _check_attributes(attributes)
        if isinstance(delay_seconds, bool) or not isinstance(delay_seconds, (int, float)) \
                or delay_seconds < 0 or delay_seconds > MAX_VISIBILITY_SECONDS:
            raise InvalidArgumentError("delay_seconds must be 0..43200")
        with self._lock:
            q = self._require_locked(queue)
            now = time.time()
            self._seq += 1
            msg = Message(message_id=uuid.uuid4().hex, body=body,
                          attributes=attrs, sent_timestamp=now,
                          approximate_receive_count=0,
                          available_at=now + float(delay_seconds), seq=self._seq)
            self._log.append({"op": "send", "queue": queue,
                              "message": msg.to_dict()})
            self._insert_pending_locked(q, msg)
            return {"message_id": msg.message_id, "seq": msg.seq,
                    "sent_timestamp": msg.sent_timestamp}

    # -- receive --------------------------------------------------------
    def receive_messages(self, queue: str, max_messages: int = 1,
                         wait_time_seconds: float = 0.0,
                         visibility_timeout: Optional[float] = None) -> List[Dict[str, Any]]:
        _check_queue_name(queue)
        if isinstance(max_messages, bool) or not isinstance(max_messages, int):
            raise InvalidArgumentError("max_messages must be an int")
        max_messages = max(1, min(MAX_RECEIVE_BATCH, max_messages))
        if isinstance(wait_time_seconds, bool) or \
                not isinstance(wait_time_seconds, (int, float)) or wait_time_seconds < 0:
            raise InvalidArgumentError("wait_time_seconds must be >= 0")
        wait_time_seconds = min(float(wait_time_seconds), MAX_WAIT_SECONDS)
        if visibility_timeout is not None:
            if isinstance(visibility_timeout, bool) or \
                    not isinstance(visibility_timeout, (int, float)):
                raise InvalidArgumentError("visibility_timeout must be a number")
            if visibility_timeout < 0 or visibility_timeout > MAX_VISIBILITY_SECONDS:
                raise InvalidArgumentError("visibility_timeout must be 0..43200")
        deadline = time.time() + wait_time_seconds
        while True:
            with self._lock:
                q = self._require_locked(queue)
                self._sweep_locked(time.time())
                vt = q.config.get("visibility_timeout",
                                  self.default_visibility_timeout) \
                    if visibility_timeout is None else float(visibility_timeout)
                out = self._take_locked(q, queue, max_messages, vt)
                if out or time.time() >= deadline:
                    return out
            remaining = deadline - time.time()
            if remaining <= 0:
                return []
            time.sleep(min(POLL_STEP, remaining))

    def _take_locked(self, q: _Queue, queue: str, n: int,
                     vt: float) -> List[Dict[str, Any]]:
        now = time.time()
        out: List[Dict[str, Any]] = []
        log_recs: List[Dict[str, Any]] = []
        for msg in list(q.pending):
            if len(out) >= n:
                break
            if msg.available_at > now:
                continue
            msg.approximate_receive_count += 1
            max_recv = int(q.config.get("max_receives", 0) or 0)
            dlq = q.config.get("dead_letter_queue")
            if max_recv > 0 and msg.approximate_receive_count > max_recv and dlq \
                    and dlq in self._queues:
                q.pending.remove(msg)
                self._origins[msg.message_id] = queue
                self._insert_pending_locked(self._queues[dlq], msg)
                log_recs.append({"op": "dlq_move", "from": queue, "to": dlq,
                                 "message": msg.to_dict()})
                continue
            receipt = uuid.uuid4().hex
            exp = now + vt
            q.pending.remove(msg)
            q.inflight[receipt] = _Inflight(message=msg, deadline=exp, queue=queue)
            log_recs.append({"op": "recv", "queue": queue, "receipt": receipt,
                             "message_id": msg.message_id, "seq": msg.seq,
                             "count": msg.approximate_receive_count, "deadline": exp})
            out.append({"message_id": msg.message_id, "receipt_handle": receipt,
                        "body": msg.body, "attributes": dict(msg.attributes),
                        "sent_timestamp": msg.sent_timestamp,
                        "approximate_receive_count": msg.approximate_receive_count,
                        "visibility_deadline": exp})
        if log_recs:
            self._log.append_many(log_recs)
        return out

    # -- ack / visibility -----------------------------------------------
    def delete_message(self, queue: str, receipt_handle: str) -> None:
        _check_queue_name(queue)
        if not isinstance(receipt_handle, str) or not receipt_handle:
            raise ReceiptNotFoundError("invalid receipt handle")
        with self._lock:
            q = self._require_locked(queue)
            try:
                entry = q.inflight.pop(receipt_handle)
            except KeyError:
                raise ReceiptNotFoundError("receipt handle not found or expired")
            self._log.append({"op": "ack", "queue": queue,
                              "receipt": receipt_handle,
                              "message_id": entry.message.message_id,
                              "seq": entry.message.seq})

    def change_visibility(self, queue: str, receipt_handle: str,
                          visibility_timeout: float) -> Dict[str, Any]:
        _check_queue_name(queue)
        if not isinstance(receipt_handle, str) or not receipt_handle:
            raise ReceiptNotFoundError("invalid receipt handle")
        if isinstance(visibility_timeout, bool) or \
                not isinstance(visibility_timeout, (int, float)):
            raise InvalidArgumentError("visibility_timeout must be a number")
        if visibility_timeout < 0 or visibility_timeout > MAX_VISIBILITY_SECONDS:
            raise InvalidArgumentError("visibility_timeout must be 0..43200")
        with self._lock:
            q = self._require_locked(queue)
            try:
                entry = q.inflight[receipt_handle]
            except KeyError:
                raise ReceiptNotFoundError("receipt handle not found or expired")
            now = time.time()
            if visibility_timeout == 0:
                msg = entry.message
                del q.inflight[receipt_handle]
                msg.available_at = now
                self._insert_pending_locked(q, msg)
                self._log.append({"op": "requeue", "queue": queue,
                                  "receipt": receipt_handle,
                                  "message_id": msg.message_id, "seq": msg.seq})
                return {"message_id": msg.message_id, "visible_at": now}
            entry.deadline = now + float(visibility_timeout)
    # -- purge / stats --------------------------------------------------
    def purge(self, queue: str) -> Dict[str, int]:
        _check_queue_name(queue)
        with self._lock:
            q = self._require_locked(queue)
            pending_n = len(q.pending)
            inflight_n = len(q.inflight)
            self._log.append({"op": "purge", "queue": queue,
                              "pending": pending_n, "inflight": inflight_n})
            q.pending.clear()
            q.inflight.clear()
            return {"purged_pending": pending_n, "purged_inflight": inflight_n}

    def stats(self, queue: Optional[str] = None) -> Dict[str, Any]:
        with self._lock:
            self._sweep_locked(time.time())
            if queue is not None:
                _check_queue_name(queue)
                q = self._require_locked(queue)
                return {queue: self._stats_locked(q)}
            return {name: self._stats_locked(q)
                    for name, q in sorted(self._queues.items())}

    def _stats_locked(self, q: _Queue) -> Dict[str, Any]:
        now = time.time()
        delayed = sum(1 for m in q.pending if m.available_at > now)
        oldest = min([m.sent_timestamp for m in q.pending]) if q.pending else None
        return {"pending": len(q.pending), "inflight": len(q.inflight),
                "delayed": delayed, "config": dict(q.config),
                "oldest_pending_sent_at": oldest}

    def redrive_dlq(self, source_queue: str, dlq_name: Optional[str] = None,
                    max_messages: int = 100) -> Dict[str, Any]:
        _check_queue_name(source_queue)
        if dlq_name is not None:
            _check_queue_name(dlq_name)
        if isinstance(max_messages, bool) or not isinstance(max_messages, int) \
                or max_messages < 1:
            raise InvalidArgumentError("max_messages must be a positive int")
        max_messages = min(max_messages, 1000)
        with self._lock:
            src = self._require_locked(source_queue)
            target_name = dlq_name or src.config.get("dead_letter_queue")
            if not target_name:
                raise InvalidArgumentError(
                    "queue '%s' has no dead-letter queue configured" % source_queue)
            if target_name not in self._queues:
                raise QueueNotFoundError("queue '%s' not found" % target_name)
            dlq = self._queues[target_name]
            self._sweep_locked(time.time())
            moved = 0
            moved_ids: List[str] = []
            log_recs: List[Dict[str, Any]] = []
            for msg in list(dlq.pending):
                if moved >= max_messages:
                    break
                dlq.pending.remove(msg)
                msg.approximate_receive_count = 0
                msg.available_at = time.time()
                origin = self._origins.pop(msg.message_id, source_queue)
                dest = self._queues.get(origin, src)
                self._insert_pending_locked(dest, msg)
                log_recs.append({"op": "redrive", "from": target_name,
                                 "to": dest.name, "message_id": msg.message_id,
                                 "message": msg.to_dict()})
                moved += 1
                moved_ids.append(msg.message_id)
            for receipt in [r for r in dlq.inflight]:
                if moved >= max_messages:
                    break
                entry = dlq.inflight.pop(receipt)
                msg = entry.message
                msg.approximate_receive_count = 0
                msg.available_at = time.time()
                origin = self._origins.pop(msg.message_id, source_queue)
                dest = self._queues.get(origin, src)
                self._insert_pending_locked(dest, msg)
                log_recs.append({"op": "redrive", "from": target_name,
                                 "to": dest.name, "message_id": msg.message_id,
                                 "message": msg.to_dict()})
                moved += 1
                moved_ids.append(msg.message_id)
            if log_recs:
                self._log.append_many(log_recs)
            return {"moved": moved, "from": target_name, "to": source_queue,
                    "message_ids": moved_ids}

    def inflight_count(self, queue: str) -> int:
        with self._lock:
            return len(self._require_locked(queue).inflight)

    def _sweep_loop(self) -> None:
        while not self._stop.wait(self.redrive_interval):
            try:
                with self._lock:
                    self._sweep_locked(time.time())
            except Exception:
                pass

    def _sweep_locked(self, now: float) -> None:
        log_recs: List[Dict[str, Any]] = []
        for name, q in self._queues.items():
            expired = [r for r, e in q.inflight.items() if e.deadline <= now]
            for receipt in expired:
                entry = q.inflight.pop(receipt)
                msg = entry.message
                msg.available_at = now
                max_recv = int(q.config.get("max_receives", 0) or 0)
                dlq = q.config.get("dead_letter_queue")
                if max_recv > 0 and msg.approximate_receive_count > max_recv \
                        and dlq and dlq in self._queues:
                    self._origins[msg.message_id] = name
                    self._insert_pending_locked(self._queues[dlq], msg)
                    log_recs.append({"op": "dlq_move", "from": name, "to": dlq,
                                     "message": msg.to_dict()})
                else:
                    self._insert_pending_locked(q, msg)
                    log_recs.append({"op": "requeue", "queue": name,
                                     "receipt": receipt,
                                     "message_id": msg.message_id, "seq": msg.seq})
        if log_recs:
            try:
                self._log.append_many(log_recs)
            except (OSError, ValueError, RuntimeError):
                pass

    # -- recovery ---------------------------------------------------------
    def _recover(self) -> None:
        snap = self._log.load_snapshot()
        if snap:
            self._apply_snapshot(snap)
        for rec in self._log.replay():
            try:
                self._apply_record(rec)
            except (FluxError, KeyError, ValueError, TypeError):
                continue

    def _apply_snapshot(self, snap: Dict[str, Any]) -> None:
        self._seq = int(snap.get("seq", 0))
        self._origins = dict(snap.get("origins") or {})
        self._queues.clear()
        for name, qd in (snap.get("queues") or {}).items():
            q = _Queue(name=name, config=dict(qd.get("config") or {}))
            for md in qd.get("pending") or []:
                try:
                    q.pending.append(Message.from_dict(md))
                except (KeyError, ValueError, TypeError):
                    continue
            q.pending.sort(key=lambda m: m.seq)
            for item in qd.get("inflight") or []:
                try:
                    msg = Message.from_dict(item["message"])
                    q.inflight[item["receipt"]] = _Inflight(
                        message=msg, deadline=float(item["deadline"]),
                        queue=item.get("queue", name))
                except (KeyError, ValueError, TypeError):
                    continue
            self._seq = max([self._seq] + [m.seq for m in q.pending] +
                            [e.message.seq for e in q.inflight.values()])
            self._queues[name] = q

    def _apply_record(self, rec: Dict[str, Any]) -> None:
        op = rec.get("op")
        if op == "create_queue":
            name = rec["queue"]
            if name not in self._queues:
                self._queues[name] = _Queue(name=name,
                                            config=dict(rec.get("config") or {}))
            else:
                self._queues[name].config.update(dict(rec.get("config") or {}))
        elif op == "delete_queue":
            self._queues.pop(rec["queue"], None)
        elif op == "send":
            msg = Message.from_dict(rec["message"])
            q = self._queues.get(rec["queue"])
            if q is None:
                return
            self._seq = max(self._seq, msg.seq)
            if msg.message_id not in {m.message_id for m in q.pending}:
                self._insert_pending_locked(q, msg)
        elif op == "recv":
            q = self._queues.get(rec["queue"])
            if q is None:
                return
            receipt, mid = rec["receipt"], rec["message_id"]
            if receipt in q.inflight:
                return
            msg = next((m for m in q.pending if m.message_id == mid), None)
            if msg is None:
                return
            msg.approximate_receive_count = int(
                rec.get("count", msg.approximate_receive_count + 1))
            self._seq = max(self._seq, int(rec.get("seq", msg.seq)))
            q.pending.remove(msg)
            q.inflight[receipt] = _Inflight(
                message=msg,
                deadline=float(rec.get("deadline", time.time() + 30)),
                queue=rec["queue"])
        elif op == "ack":
            q = self._queues.get(rec["queue"])
            if q is None:
                return
            q.inflight.pop(rec["receipt"], None)
        elif op == "vis":
            q = self._queues.get(rec["queue"])
            if q is None:
                return
            entry = q.inflight.get(rec["receipt"])
            if entry is not None:
                entry.deadline = float(rec["deadline"])
        elif op == "requeue":
            q = self._queues.get(rec["queue"])
            if q is None:
                return
            entry = q.inflight.pop(rec["receipt"], None)
            if entry is None:
                return
            entry.message.available_at = time.time()
            self._insert_pending_locked(q, entry.message)
        elif op == "dlq_move":
            self._apply_dlq_move(rec)
        elif op == "redrive":
            self._apply_redrive(rec)
        elif op == "purge":
            q = self._queues.get(rec["queue"])
            if q is None:
                return
            q.pending.clear()
            q.inflight.clear()

    def _apply_dlq_move(self, rec: Dict[str, Any]) -> None:
        src = self._queues.get(rec["from"])
        dst = self._queues.get(rec["to"])
        if src is None or dst is None:
            return
        msg = Message.from_dict(rec["message"])
        for receipt, entry in list(src.inflight.items()):
            if entry.message.message_id == msg.message_id:
                del src.inflight[receipt]
                break
        else:
            existing = next((m for m in src.pending
                             if m.message_id == msg.message_id), None)
            if existing is not None:
                src.pending.remove(existing)
        self._origins[msg.message_id] = rec["from"]
        if msg.message_id not in {m.message_id for m in dst.pending}:
            self._insert_pending_locked(dst, msg)

    def _apply_redrive(self, rec: Dict[str, Any]) -> None:
        src = self._queues.get(rec["from"])
        dst = self._queues.get(rec["to"])
        if src is None or dst is None:
            return
        msg = Message.from_dict(rec["message"])
        existing = next((m for m in src.pending
                         if m.message_id == msg.message_id), None)
        if existing is not None:
            src.pending.remove(existing)
        for receipt, entry in list(src.inflight.items()):
            if entry.message.message_id == msg.message_id:
                del src.inflight[receipt]
                break
        self._origins.pop(msg.message_id, None)
        if msg.message_id not in {m.message_id for m in dst.pending}:
            self._insert_pending_locked(dst, msg)

    def _insert_pending_locked(self, q: _Queue, msg: Message) -> None:
        idx = bisect.bisect_right([m.seq for m in q.pending], msg.seq)
        q.pending.insert(idx, msg)

