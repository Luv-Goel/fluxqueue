"""fluxqueue.models — message model and error types (stdlib only)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass
class Message:
    """A single queue message.

    Attributes:
        message_id: Globally unique id (uuid4 hex).
        body: Opaque string payload.
        attributes: Arbitrary string->string (or JSON scalar) user metadata.
        sent_timestamp: time.time() when sent.
        approximate_receive_count: Number of times received (SQS-style).
        available_at: time.time() when the message becomes visible (delay).
        seq: Per-broker monotonic sequence number used for FIFO ordering.
    """

    message_id: str
    body: str
    attributes: Dict[str, Any] = field(default_factory=dict)
    sent_timestamp: float = 0.0
    approximate_receive_count: int = 0
    available_at: float = 0.0
    seq: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "message_id": self.message_id,
            "body": self.body,
            "attributes": dict(self.attributes),
            "sent_timestamp": self.sent_timestamp,
            "approximate_receive_count": self.approximate_receive_count,
            "available_at": self.available_at,
            "seq": self.seq,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Message":
        return cls(
            message_id=str(d["message_id"]),
            body=str(d["body"]),
            attributes=dict(d.get("attributes") or {}),
            sent_timestamp=float(d.get("sent_timestamp", 0.0)),
            approximate_receive_count=int(d.get("approximate_receive_count", 0)),
            available_at=float(d.get("available_at", d.get("sent_timestamp", 0.0))),
            seq=int(d.get("seq", 0)),
        )


class FluxError(Exception):
    """Base error for fluxqueue."""


class QueueNotFoundError(FluxError):
    """Raised when a queue does not exist."""


class QueueExistsError(FluxError):
    """Raised on conflicting queue creation (reserved; create is idempotent)."""


class ReceiptError(FluxError):
    """Base class for receipt problems."""


class ReceiptNotFoundError(ReceiptError):
    """Raised when a receipt handle is unknown, expired, or already used."""


class InvalidArgumentError(FluxError):
    """Raised when an argument is out of range or malformed."""
