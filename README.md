# fluxqueue

[![CI](https://github.com/Luv-Goel/fluxqueue/actions/workflows/ci.yml/badge.svg)](https://github.com/Luv-Goel/fluxqueue/actions/workflows/ci.yml)
![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)
![Zero dependencies](https://img.shields.io/badge/deps-zero-brightgreen)
![License: MIT](https://img.shields.io/badge/license-MIT-green)

A **durable, at-least-once message queue in pure Python — stdlib only, zero
third-party dependencies** — inspired by SQS / RabbitMQ durable queues.
Named FIFO queues, visibility timeouts, delay queues, dead-letter queues,
crash recovery from an fsync'd append-only log, an HTTP/JSON API, a client
library, and a CLI.

## Feature matrix

| Feature | Supported | Notes |
|---|---|---|
| Named FIFO queues | yes | per-queue, seq-ordered |
| `send_message` + `delay_seconds` | yes | delayed visibility |
| `receive_messages` long-poll | yes | `wait_time_seconds` up to 20s |
| Visibility timeout | yes | per queue or per receive |
| `delete_message` (receipt handle) | yes | uuid tokens, single-use |
| `change_visibility` | yes | extend, or `0` = immediate requeue |
| `purge` | yes | drops pending + inflight |
| Dead-letter queue | yes | `max_receives` -> DLQ move |
| Redrive DLQ -> source | yes | resets receive count |
| Durability (JSONL + fsync) | yes | replay on open |
| Snapshot / checkpoint | yes | `broker.checkpoint()` / `POST /checkpoint` |
| Message attributes | yes | up to 10 JSON-scalar attrs |
| `MessageId`, `SentTimestamp`, `ApproximateReceiveCount` | yes | SQS-style fields |
| Thread-safe broker + redrive thread | yes | `RLock`, background sweep |
| HTTP server + client + CLI | yes | `http.server` + `urllib` + `argparse` |

## Quickstart

### Embedded mode

```python
from fluxqueue import Broker

b = Broker("./fluxqueue-data").open()
b.create_queue("orders")
b.send_message("orders", "order-42", attributes={"priority": "high"})
msg = b.receive_messages("orders", wait_time_seconds=2)[0]
print(msg["body"], msg["approximate_receive_count"])
b.delete_message("orders", msg["receipt_handle"])
b.close()
```

### Server mode

```bash
pip install .
fluxqueue-server --data-dir ./fluxqueue-data --port 8766
```

```bash
fluxqueue --url http://127.0.0.1:8766 create orders
fluxqueue --url http://127.0.0.1:8766 send orders "hello"
fluxqueue --url http://127.0.0.1:8766 receive orders --wait 5
fluxqueue --url http://127.0.0.1:8766 stats
```

```python
from fluxqueue import QueueClient
c = QueueClient("http://127.0.0.1:8766")
c.create_queue("orders", {"visibility_timeout": 30})
c.send_message("orders", "hi", delay_seconds=0)
msgs = c.receive_messages("orders", wait_time_seconds=5)
c.delete_message("orders", msgs[0]["receipt_handle"])
```

## Architecture

```
              +-------------- Broker (RLock) ------------------+
              | queues: name -> {config, pending, inflight}    |
send -------> | seq counter, origins map, sweep thread        | -> log-*.jsonl (fsync)
receive <---- > visibility deadlines, DLQ routing, redrive    | -> snapshot.json
ack --------> |                                               |
              +-- ThreadingHTTPServer JSON API <-> QueueClient -+
```

- **Durability:** every mutation appends JSONL + fsyncs (`send` = 1 fsync;
  `receive` batch = 1 fsync). Reopen replays snapshot then segments
  oldest-first; torn tail lines are skipped. `checkpoint()` snapshots and
  truncates the log. Segments (`log-000001.jsonl`, ...) rotate at 1 MiB.
- **Record ops:** `create_queue send recv ack vis requeue dlq_move redrive
  purge delete_queue`.

## Delivery semantics

**At-least-once.** `receive` hides a message for `visibility_timeout` and
hands you a single-use `receipt_handle`:

- `delete_message` before the deadline -> gone (acked).
- Deadline passes with no ack -> visible again, count + 1 (may duplicate
  after crashes — keep consumers idempotent).
- Count > `max_receives` (DLQ configured) -> moved to the dead-letter queue.
- `change_visibility(..., 0)` = visible now (receipt invalidated).
- `delay_seconds` hides a message until `now + delay`.
- Crash between `receive` and `ack`: recovered and redelivered.

## HTTP API

| Method & path | Description |
|---|---|
| `GET /health` | liveness probe |
| `GET /queues` | list queue names |
| `PUT /queues/{name}` | create/update queue |
| `DELETE /queues/{name}` | delete queue |
| `GET /queues/{name}` | per-queue stats |
| `POST /queues/{name}/messages` | send `{body, attributes?, delay_seconds?}` |
| `GET /queues/{name}/messages?max&wait&visibility` | receive (long-poll) |
| `DELETE /queues/{name}/messages/{receipt}` | ack |
| `POST /queues/{name}/messages/visibility` | `{receipt_handle, visibility_timeout}` |
| `POST /queues/{name}/purge` | purge queue |
| `POST /queues/{name}/redrive` | `{dlq?, max_messages?}` DLQ -> source |
| `POST /queues/{name}/config` / `GET .../config` | update / read config |
| `GET /stats` | all queue stats |
| `POST /checkpoint` | snapshot + truncate log |

Errors are JSON `{"error": ...}`: `400` bad input, `404` unknown
queue/receipt, `401` bad token when `--token` is set.

## Layout

```
fluxqueue/
  fluxqueue/__init__.py   exports Broker, QueueClient, __version__
  fluxqueue/models.py     Message dataclass + error types
  fluxqueue/store.py      SegmentLog: JSONL segments, fsync, snapshots
  fluxqueue/broker.py     core: queues, inflight, DLQ, sweep, recovery
  fluxqueue/server.py     ThreadingHTTPServer JSON API
  fluxqueue/client.py     urllib QueueClient
  fluxqueue/cli.py        argparse CLI
  tests/test_broker.py    FIFO/visibility/DLQ/concurrency tests
  tests/test_durability.py crash-recovery + snapshot tests
  tests/test_server.py    HTTP round-trip tests
  examples/demo.py        scripted end-to-end tour
```

## Tests

```bash
python -m unittest discover -s tests -v   # 56 tests, stdlib only
python examples/demo.py                    # end-to-end tour
```

## Limits (deliberate)

- Max body **256 KiB**, max **10** messages per receive, max **10**
  attributes (JSON scalars), queue names `[A-Za-z0-9_-]` max 80 chars;
  visibility/delay capped at 12h; long-poll capped at 20s.
- FIFO per queue by send sequence; no cross-queue ordering, no
  partitioning, no exactly-once (use idempotent consumers).
- Optional shared-token header auth, not mTLS. Single-node, no clustering.
