"""fluxqueue.cli — argparse CLI for broker + server + client ops."""

from __future__ import annotations

import argparse
import json
import sys

from fluxqueue import __version__
from fluxqueue.broker import Broker
from fluxqueue.client import QueueClient

DEFAULT_URL = "http://127.0.0.1:8766"
DEFAULT_DIR = "./fluxqueue-data"


def _add_common(sub: argparse.ArgumentParser) -> None:
    sub.add_argument("--url", default=DEFAULT_URL, help="server base URL")
    sub.add_argument("--token", default=None, help="auth token (if server uses one)")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="fluxqueue",
                                description="fluxqueue durable message queue")
    p.add_argument("--version", action="version", version="fluxqueue " + __version__)
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("create", help="create a queue")
    _add_common(s)
    s.add_argument("queue")
    s.add_argument("--visibility-timeout", type=float, default=None)
    s.add_argument("--max-receives", type=int, default=None)
    s.add_argument("--dlq", default=None)

    s = sub.add_parser("send", help="send a message")
    _add_common(s)
    s.add_argument("queue")
    s.add_argument("body")
    s.add_argument("--attr", action="append", default=[],
                   help="attribute as key=value (repeatable)")
    s.add_argument("--delay", type=float, default=0.0)

    s = sub.add_parser("receive", help="receive message(s)")
    _add_common(s)
    s.add_argument("queue")
    s.add_argument("--max", type=int, default=1)
    s.add_argument("--wait", type=float, default=0.0)
    s.add_argument("--visibility", type=float, default=None)

    s = sub.add_parser("delete", help="delete with receipt handle")
    _add_common(s)
    s.add_argument("queue")
    s.add_argument("receipt")

    s = sub.add_parser("change-visibility", help="change visibility timeout")
    _add_common(s)
    s.add_argument("queue")
    s.add_argument("receipt")
    s.add_argument("timeout", type=float)

    s = sub.add_parser("purge", help="purge a queue")
    _add_common(s)
    s.add_argument("queue")

    s = sub.add_parser("stats", help="show stats (one queue or all)")
    _add_common(s)
    s.add_argument("queue", nargs="?")

    s = sub.add_parser("redrive", help="redrive DLQ messages back")
    _add_common(s)
    s.add_argument("queue")
    s.add_argument("--dlq", default=None)
    s.add_argument("--max", type=int, default=100)

    s = sub.add_parser("serve", help="run the HTTP server")
    s.add_argument("--data-dir", default=DEFAULT_DIR)
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8766)
    s.add_argument("--token", default=None)

    s = sub.add_parser("checkpoint", help="trigger a server snapshot")
    _add_common(s)
    return p


def _parse_attrs(pairs) -> dict:
    out = {}
    for item in pairs:
        if "=" not in item:
            raise SystemExit("bad --attr '%s' (expected key=value)" % item)
        k, v = item.split("=", 1)
        out[k] = v
    return out


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    if args.cmd == "serve":
        from fluxqueue.server import serve_forever
        serve_forever(args.data_dir, host=args.host, port=args.port,
                      token=args.token)
        return 0
    client = QueueClient(args.url, token=args.token)
    if args.cmd == "create":
        cfg = {}
        if args.visibility_timeout is not None:
            cfg["visibility_timeout"] = args.visibility_timeout
        if args.max_receives is not None:
            cfg["max_receives"] = args.max_receives
        if args.dlq is not None:
            cfg["dead_letter_queue"] = args.dlq
        print(json.dumps(client.create_queue(args.queue, cfg), indent=2))
    elif args.cmd == "send":
        print(json.dumps(client.send_message(
            args.queue, args.body, attributes=_parse_attrs(args.attr),
            delay_seconds=args.delay), indent=2))
    elif args.cmd == "receive":
        print(json.dumps(client.receive_messages(
            args.queue, max_messages=args.max, wait_time_seconds=args.wait,
            visibility_timeout=args.visibility), indent=2))
    elif args.cmd == "delete":
        print(json.dumps(client.delete_message(args.queue, args.receipt), indent=2))
    elif args.cmd == "change-visibility":
        print(json.dumps(client.change_visibility(
            args.queue, args.receipt, args.timeout), indent=2))
    elif args.cmd == "purge":
        print(json.dumps(client.purge(args.queue), indent=2))
    elif args.cmd == "stats":
        if args.queue:
            print(json.dumps(client.queue_stats(args.queue), indent=2))
        else:
            print(json.dumps(client.stats(), indent=2))
    elif args.cmd == "redrive":
        print(json.dumps(client.redrive(args.queue, dlq=args.dlq,
                                        max_messages=args.max), indent=2))
    elif args.cmd == "checkpoint":
        print(json.dumps(client.checkpoint(), indent=2))
    return 0


def server_main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="fluxqueue-server")
    p.add_argument("--data-dir", default=DEFAULT_DIR)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8766)
    p.add_argument("--token", default=None)
    a = p.parse_args(argv)
    from fluxqueue.server import serve_forever
    serve_forever(a.data_dir, host=a.host, port=a.port, token=a.token)
    return 0


if __name__ == "__main__":
    sys.exit(main())
