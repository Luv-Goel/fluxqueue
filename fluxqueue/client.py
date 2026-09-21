"""fluxqueue.client — urllib-based JSON client (stdlib only)."""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional


class FluxClientError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__("HTTP %d: %s" % (status, message))
        self.status = status
        self.message = message


def _parse_error(status: int, body: bytes) -> FluxClientError:
    try:
        obj = json.loads(body.decode("utf-8") or "{}")
        msg = str(obj.get("error", body.decode("utf-8")[:300]))
    except (ValueError, UnicodeDecodeError):
        msg = body.decode("utf-8", "replace")[:300]
    return FluxClientError(status, msg)


class QueueClient:
    """HTTP client for a fluxqueue server. Stdlib only (urllib)."""

    def __init__(self, base_url: str = "http://127.0.0.1:8766",
                 timeout: float = 30.0, token: Optional[str] = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = float(timeout)
        self.token = token

    # -- low level ----------------------------------------------------
    def _request(self, method: str, path: str,
                 body: Optional[Dict[str, Any]] = None,
                 query: Optional[Dict[str, Any]] = None) -> Any:
        url = self.base_url + path
        if query:
            url += "?" + urllib.parse.urlencode(
                {k: v for k, v in query.items() if v is not None})
        data = None
        headers = {"Accept": "application/json"}
        if self.token:
            headers["X-Fluxqueue-Token"] = self.token
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as e:
            raise _parse_error(e.code, e.read())
        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8"))
        except ValueError:
            raise FluxClientError(-1, "invalid JSON response")

    # -- API ----------------------------------------------------------
    def health(self) -> Dict[str, Any]:
        return self._request("GET", "/health")

    def create_queue(self, name: str,
                     config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self._request("PUT", "/queues/" + urllib.parse.quote(name, safe=""),
                             body=dict(config or {}))

    def delete_queue(self, name: str) -> Dict[str, Any]:
        return self._request("DELETE", "/queues/" + urllib.parse.quote(name, safe=""))

    def list_queues(self) -> List[str]:
        return self._request("GET", "/queues")["queues"]

    def send_message(self, queue: str, body: str,
                     attributes: Optional[Dict[str, Any]] = None,
                     delay_seconds: float = 0.0) -> Dict[str, Any]:
        return self._request("POST",
                             "/queues/" + urllib.parse.quote(queue, safe="") + "/messages",
                             body={"body": body, "attributes": attributes or {},
                                   "delay_seconds": delay_seconds})

    def receive_messages(self, queue: str, max_messages: int = 1,
                         wait_time_seconds: float = 0.0,
                         visibility_timeout: Optional[float] = None) -> List[Dict[str, Any]]:
        q: Dict[str, Any] = {"max": max_messages, "wait": wait_time_seconds}
        if visibility_timeout is not None:
            q["visibility"] = visibility_timeout
        return self._request(
            "GET", "/queues/" + urllib.parse.quote(queue, safe="") + "/messages",
            query=q)["messages"]

    def delete_message(self, queue: str, receipt_handle: str) -> Dict[str, Any]:
        return self._request(
            "DELETE", "/queues/" + urllib.parse.quote(queue, safe="") +
            "/messages/" + urllib.parse.quote(receipt_handle, safe=""))

    def change_visibility(self, queue: str, receipt_handle: str,
                          visibility_timeout: float) -> Dict[str, Any]:
        return self._request(
            "POST", "/queues/" + urllib.parse.quote(queue, safe="") +
            "/messages/visibility",
            body={"receipt_handle": receipt_handle,
                  "visibility_timeout": visibility_timeout})

    def purge(self, queue: str) -> Dict[str, Any]:
        return self._request(
            "POST", "/queues/" + urllib.parse.quote(queue, safe="") + "/purge",
            body={})

    def redrive(self, queue: str, dlq: Optional[str] = None,
                max_messages: int = 100) -> Dict[str, Any]:
        body: Dict[str, Any] = {"max_messages": max_messages}
        if dlq is not None:
            body["dlq"] = dlq
        return self._request(
            "POST", "/queues/" + urllib.parse.quote(queue, safe="") + "/redrive",
            body=body)

    def queue_stats(self, queue: str) -> Dict[str, Any]:
        return self._request("GET", "/queues/" + urllib.parse.quote(queue, safe=""))

    def stats(self) -> Dict[str, Any]:
        return self._request("GET", "/stats")

    def checkpoint(self) -> Dict[str, Any]:
        return self._request("POST", "/checkpoint", body={})
