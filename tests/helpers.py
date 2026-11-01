"""Helpers shared by the tests (signing, fixture payloads, waiting, the live server)."""

import hashlib
import hmac
import json
import logging
import socket
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import httpx
import uvicorn
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from restock.api import list_requests
from restock.outbound import OutboundWorker
from restock.runtime import REPO_ROOT, JsonFormatter, Settings
from restock.schemas import Base

SECRET = "assessment-secret"  # the synthetic secret the supplied fixtures are signed with
FIXTURES = REPO_ROOT / "fixtures" / "inbound"
EVENTS_URL = "/api/v1/inventory-events"
# Every table, children before parents, so deleting rows in this order never trips a
# foreign key (conftest.clean_database).
TABLES = ("outbound_attempts", "replenishment_requests", "inventory_state", "observations")


def fixture_bytes(name: str) -> bytes:
    """A supplied fixture file, byte for byte (the signature covers the exact bytes)."""
    return (FIXTURES / name).read_bytes()


def sign_headers(body: bytes, *, secret: str = SECRET, timestamp: int | None = None) -> dict:
    """The headers a gateway sends with `body`:"""
    ts = str(int(time.time()) if timestamp is None else timestamp)
    digest = hmac.new(secret.encode(), ts.encode() + b"." + body, hashlib.sha256).hexdigest()
    return {
        "Content-Type": "application/json",
        "X-Webhook-Timestamp": ts,
        "X-Webhook-Signature": f"v1={digest}",
    }


def event_body(**fields: Any) -> bytes:
    """A valid event: 02_low_stock.json with some fields replaced."""
    payload = json.loads(fixture_bytes("02_low_stock.json"))
    payload.update(fields)
    return json.dumps(payload).encode()


def post_event(client: httpx.Client, body: bytes, **sign: Any) -> httpx.Response:
    """POST `body` to the webhook, signed; `sign` goes to sign_headers (secret, timestamp)."""
    return client.post(EVENTS_URL, content=body, headers=sign_headers(body, **sign))


def post_fixture(client: httpx.Client, name: str) -> httpx.Response:
    """POST a supplied fixture file, unchanged and signed."""
    return post_event(client, fixture_bytes(name))


def wait_for(predicate: Callable[[], Any], timeout: float = 20.0, what: str = "condition") -> Any:
    """Call `predicate` every 50 ms until it returns something true, and return that; after
    `timeout` seconds, fail the test with a message naming `what`."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.05)
    raise AssertionError(f"timed out after {timeout}s waiting for {what}")


def make_settings(database_url: str, **overrides: Any) -> Settings:
    """Settings for a test app:"""
    values: dict[str, Any] = {
        "database_url": database_url,
        "webhook_secret": SECRET,
        "webhook_replay_window_seconds": 300,
        "erp_url": "http://testserver/mock-erp",
        "erp_timeout_seconds": 1.0,
        "erp_max_attempts": 6,
        "erp_backoff_base_seconds": 0.05,
        "erp_backoff_cap_seconds": 0.2,
        "log_level": "INFO",
        "worker_enabled": False,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def deliver_due(app: FastAPI, client: TestClient) -> int:
    """One worker pass using the TestClient as transport, so delivery to the in-app mock ERP is
    synchronous and deterministic."""
    worker = OutboundWorker(app.state.sessions, app.state.settings, http_client=client)
    return worker.run_once()


def requests_now(app_or_client: Any, status: str = "all") -> list[dict]:
    """The replenishment requests, newest first, in the API's JSON shape."""
    app = getattr(app_or_client, "app", app_or_client)
    with app.state.sessions() as session:
        found = list_requests(session, status=status, limit=500)
    return [r.model_dump(mode="json", by_alias=True) for r in found]


def count_rows(app: FastAPI, model: type[Base]) -> int:
    """The number of rows in `model`'s table."""
    with app.state.sessions() as session:
        return session.scalar(select(func.count()).select_from(model)) or 0


class InFlightProbe:
    """ASGI wrapper that records the peak number of HTTP requests in progress at once, so a
    concurrency test can show its requests really overlapped inside the server."""

    def __init__(self, app: FastAPI) -> None:
        self.app = app
        self.current = 0
        self.peak = 0
        self._lock = threading.Lock()

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        with self._lock:
            self.current += 1
            self.peak = max(self.peak, self.current)
        try:
            await self.app(scope, receive, send)
        finally:
            with self._lock:
                self.current -= 1


@dataclass
class LiveServer:
    """A running server started by conftest.live_server:"""

    url: str
    app: FastAPI
    server: uvicorn.Server
    thread: threading.Thread
    probe: InFlightProbe

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=30)


def free_port() -> int:
    """A free TCP port: the OS picks one when port 0 is bound, and the socket closes again."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class ListHandler(logging.Handler):
    """Formats records at emit time, while the request's log context is still bound."""

    def __init__(self) -> None:
        super().__init__()
        self.setFormatter(JsonFormatter())
        self.lines: list[dict[str, Any]] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(json.loads(self.format(record)))
