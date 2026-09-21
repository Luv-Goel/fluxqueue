"""fluxqueue.store — append-only JSONL segment log with fsync + snapshots."""

from __future__ import annotations

import glob
import json
import os
import time
from typing import Any, Dict, Iterator, List, Optional

LOG_PREFIX = "log-"
LOG_SUFFIX = ".jsonl"
SNAPSHOT_NAME = "snapshot.json"


def _fsync_file(f) -> None:
    try:
        f.flush()
        os.fsync(f.fileno())
    except (OSError, ValueError):
        pass


def _fsync_dir(directory: str) -> None:
    try:
        fd = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


class SegmentLog:
    """Append-only JSONL log. Every batch is flushed + fsynced."""

    def __init__(self, directory: str, segment_bytes: int = 1_000_000,
                 fsync: bool = True) -> None:
        self.directory = directory
        self.segment_bytes = max(64 * 1024, int(segment_bytes))
        self.fsync = bool(fsync)
        self._active_path: Optional[str] = None
        self._active_file = None
        self._seg_index = 0

    def open(self) -> "SegmentLog":
        os.makedirs(self.directory, exist_ok=True)
        segments = self._list_segments()
        if segments:
            self._seg_index = int(os.path.basename(segments[-1])[4:10])
        else:
            self._seg_index = 1
            segments = [self._segment_path(1)]
        self._active_path = segments[-1]
        self._active_file = open(self._active_path, "a", encoding="utf-8")
        return self

    def close(self) -> None:
        if self._active_file is not None:
            try:
                if self.fsync:
                    _fsync_file(self._active_file)
                self._active_file.close()
            finally:
                self._active_file = None
                self._active_path = None

    def append(self, record: Dict[str, Any]) -> None:
        self.append_many([record])

    def append_many(self, records: List[Dict[str, Any]]) -> None:
        if not records or self._active_file is None:
            if not records:
                return
            raise RuntimeError("SegmentLog is not open")
        est = sum(len(json.dumps(r, separators=(",", ":"))) + 1 for r in records)
        self._maybe_rotate(est)
        self._active_file.write("".join(
            json.dumps(r, separators=(",", ":")) + "\n" for r in records))
        if self.fsync:
            _fsync_file(self._active_file)
        else:
            self._active_file.flush()

    def _maybe_rotate(self, pending: int) -> None:
        try:
            size = os.path.getsize(self._active_path or "")
        except OSError:
            size = 0
        if size + pending > self.segment_bytes and size > 0:
            if self.fsync:
                _fsync_file(self._active_file)
            self._active_file.close()
            self._seg_index += 1
            self._active_path = self._segment_path(self._seg_index)
            self._active_file = open(self._active_path, "a", encoding="utf-8")
            if self.fsync:
                _fsync_dir(self.directory)

    def replay(self) -> Iterator[Dict[str, Any]]:
        for path in self._list_segments():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            rec = json.loads(line)
                        except (ValueError, json.JSONDecodeError):
                            continue
                        if isinstance(rec, dict) and "op" in rec:
                            yield rec
            except FileNotFoundError:
                continue

    def log_size_bytes(self) -> int:
        total = 0
        for path in self._list_segments():
            try:
                total += os.path.getsize(path)
            except OSError:
                pass
        return total

    def save_snapshot(self, state: Dict[str, Any]) -> None:
        payload = {"format": "fluxqueue-snapshot/1",
                   "saved_at": time.time(), "state": state}
        tmp = os.path.join(self.directory, SNAPSHOT_NAME + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(json.dumps(payload))
            if self.fsync:
                _fsync_file(f)
        os.replace(tmp, os.path.join(self.directory, SNAPSHOT_NAME))
        if self.fsync:
            _fsync_dir(self.directory)
        if self._active_file is not None:
            self._active_file.close()
            self._active_file = None
        for path in self._list_segments():
            try:
                os.remove(path)
            except OSError:
                pass
        self._seg_index = 1
        self._active_path = self._segment_path(1)
        self._active_file = open(self._active_path, "a", encoding="utf-8")
        if self.fsync:
            _fsync_dir(self.directory)

    def load_snapshot(self) -> Optional[Dict[str, Any]]:
        path = os.path.join(self.directory, SNAPSHOT_NAME)
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except (FileNotFoundError, ValueError, OSError):
            return None
        if not isinstance(payload, dict) or "state" not in payload:
            return None
        return payload["state"] if isinstance(payload["state"], dict) else None

    def _segment_path(self, index: int) -> str:
        return os.path.join(self.directory, "%s%06d%s" % (LOG_PREFIX, index, LOG_SUFFIX))

    def _list_segments(self) -> List[str]:
        return sorted(glob.glob(os.path.join(self.directory, LOG_PREFIX + "*" + LOG_SUFFIX)))
