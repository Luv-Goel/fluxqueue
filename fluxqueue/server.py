"""fluxqueue.server — stdlib-only HTTP/JSON API (ThreadingHTTPServer)."""

from __future__ import annotations

import json
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Optional, Tuple

from fluxqueue.broker import Broker
from fluxqueue.models import FluxError, InvalidArgumentError
from fluxqueue.models import QueueNotFoundError, ReceiptNotFoundError


def _send_json(handler: BaseHTTPRequestHandler, status: int, obj: Any) -> None:
    body = json.dumps(obj).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _read_json(handler: BaseHTTPRequestHandler) -> Dict[str, Any]:
    length = handler.headers.get("Content-Length")
    if not length:
        return {}
    try:
        n = int(length)
    except ValueError:
        return {}
    if n <= 0:
        return {}
    raw = handler.rfile.read(min(n, 4 * 1024 * 1024))
    if not raw:
        return {}
    try:
        obj = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        raise InvalidArgumentError("request body must be valid JSON")
    if obj is None:
        return {}
    if not isinstance(obj, dict):
        raise InvalidArgumentError("request body must be a JSON object")
    return obj


class _Handler(BaseHTTPRequestHandler):
    broker: Broker = None  # type: ignore
    token: Optional[str] = None
    server_version = "fluxqueue/0.1.0"

    def log_message(self, fmt: str, *args: Any) -> None:
        pass

    def do_GET(self):
        self._dispatch("GET")

    def do_PUT(self):
        self._dispatch("PUT")

    def do_POST(self):
        self._dispatch("POST")

    def do_DELETE(self):
        self._dispatch("DELETE")

    def _dispatch(self, method: str):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        query = urllib.parse.parse_qs(parsed.query)
        try:
            if self.token and self.headers.get("X-Fluxqueue-Token") != self.token:
                if path != "/health":
                    _send_json(self, 401, {"error": "unauthorized"})
                    return
            result = self._route(method, path, query)
            if result is None:
                _send_json(self, 404, {"error": "not found"})
                return
            status, obj = result
            _send_json(self, status, obj)
        except QueueNotFoundError as e:
            _send_json(self, 404, {"error": str(e)})
        except ReceiptNotFoundError as e:
            _send_json(self, 404, {"error": str(e)})
        except InvalidArgumentError as e:
            _send_json(self, 400, {"error": str(e)})
        except FluxError as e:
            _send_json(self, 400, {"error": str(e)})
        except BrokenPipeError:
            pass
        except Exception as e:
            _send_json(self, 500, {"error": "internal error: %s" % e})

    def _route(self, method: str, path: str,
               query: Dict[str, list]) -> Optional[Tuple[int, Any]]:
        broker = self.broker
        parts = [p for p in path.split("/") if p]

        if path == "/health" and method == "GET":
            return 200, {"status": "ok", "version": "0.1.0", "time": time.time()}

        if parts == ["queues"] and method == "GET":
            return 200, {"queues": broker.list_queues()}

        if len(parts) == 2 and parts[0] == "queues":
            name = urllib.parse.unquote(parts[1])
            if method == "PUT":
                info = broker.create_queue(name, _read_json(self) or None)
                return 200, info
            if method == "DELETE":
                broker.delete_queue(name)
                return 200, {"deleted": name}
            if method == "GET":
                stats = broker.stats(name)
                return 200, {"queue": name, **stats[name]}

        if len(parts) == 3 and parts[0] == "queues" and parts[2] == "messages":
            name = urllib.parse.unquote(parts[1])
            if method == "POST":
                body = _read_json(self)
                if "delay_seconds" in body and not isinstance(
                        body["delay_seconds"], (int, float)):
                    raise InvalidArgumentError("delay_seconds must be a number")
                res = broker.send_message(
                    name, body.get("body", ""),
                    attributes=body.get("attributes"),
                    delay_seconds=float(body.get("delay_seconds", 0.0)))
                return 201, res
            if method == "GET":
                def q1(key, default):
                    return query.get(key, [default])[0]
                vis_raw = query.get("visibility", [None])[0]
                msgs = broker.receive_messages(
                    name, max_messages=int(q1("max", "1")),
                    wait_time_seconds=float(q1("wait", "0")),
                    visibility_timeout=None if vis_raw is None else float(vis_raw))
                return 200, {"messages": msgs}

        if len(parts) == 4 and parts[0] == "queues" and parts[2] == "messages":
            name = urllib.parse.unquote(parts[1])
            receipt = urllib.parse.unquote(parts[3])
            if method == "DELETE":
                broker.delete_message(name, receipt)
                return 200, {"deleted": receipt}

        if len(parts) == 4 and parts[0] == "queues" and parts[2] == "messages" \
                and parts[3] == "visibility" and method == "POST":
            name = urllib.parse.unquote(parts[1])
            body = _read_json(self)
            res = broker.change_visibility(name, body.get("receipt_handle", ""),
                                           body.get("visibility_timeout", 0))
            return 200, res

        if len(parts) == 3 and parts[0] == "queues" and parts[2] == "purge" \
                and method == "POST":
            return 200, broker.purge(urllib.parse.unquote(parts[1]))

        if len(parts) == 3 and parts[0] == "queues" and parts[2] == "redrive" \
                and method == "POST":
            name = urllib.parse.unquote(parts[1])
            body = _read_json(self)
            res = broker.redrive_dlq(name, dlq_name=body.get("dlq"),
                                     max_messages=int(body.get("max_messages", 100)))
            return 200, res

        if len(parts) == 3 and parts[0] == "queues" and parts[2] == "config":
            name = urllib.parse.unquote(parts[1])
            if method == "GET":
                return 200, {"queue": name, "config": broker.get_queue_config(name)}
            if method == "POST":
                return 200, broker.set_queue_config(name, _read_json(self))

        if parts == ["stats"] and method == "GET":
            return 200, {"queues": broker.stats()}

        if parts == ["checkpoint"] and method == "POST":
            broker.checkpoint()
            return 200, {"checkpoint": "ok"}

        return None


def make_server(broker: Broker, host: str = "127.0.0.1", port: int = 8766,
                token: Optional[str] = None) -> ThreadingHTTPServer:
    handler = type("FluxHandler", (_Handler,), {"broker": broker, "token": token})
    server = ThreadingHTTPServer((host, port), handler)
    server.daemon_threads = True
    return server


def serve_forever(data_dir: str, host: str = "127.0.0.1", port: int = 8766,
                  token: Optional[str] = None) -> None:
    broker = Broker(data_dir).open()
    server = make_server(broker, host, port, token)
    print("fluxqueue serving on http://%s:%d (data=%s)" % (host, port, data_dir),
          flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        broker.close()
