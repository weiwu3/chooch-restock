"""Webhook authentication: HMAC over "<timestamp>.<raw body>", replay window, ordering."""

import hashlib
import hmac
import json
import re
import socket
import subprocess
import sys
import time
from typing import Any
from urllib.parse import urlsplit

import pytest
from fastapi.testclient import TestClient

from restock import api
from restock.main import create_app
from restock.runtime import REPO_ROOT
from restock.schemas import Observation, ReplenishmentRequest

from .helpers import (
    EVENTS_URL,
    FIXTURES,
    SECRET,
    count_rows,
    event_body,
    fixture_bytes,
    make_settings,
    post_event,
    post_fixture,
    sign_headers,
)


def test_valid_signature_is_accepted(client):
    response = post_fixture(client, "01_normal_stock.json")
    assert response.status_code == 201
    assert response.json()["status"] == "accepted"


def test_headers_from_the_supplied_signing_tool_are_accepted(client):
    path = FIXTURES / "02_low_stock.json"
    tool = REPO_ROOT / "tools" / "sign_payload.py"
    output = subprocess.run(
        [sys.executable, str(tool), str(path), SECRET], capture_output=True, text=True, check=True
    ).stdout
    headers = dict(line.split(": ", 1) for line in output.strip().splitlines())
    response = client.post(
        EVENTS_URL,
        content=path.read_bytes(),
        headers={"Content-Type": "application/json", **headers},
    )
    assert response.status_code == 201, response.text


def test_wrong_secret_is_rejected(client):
    response = post_event(client, fixture_bytes("01_normal_stock.json"), secret="not-the-secret")
    assert response.status_code == 401
    assert response.json()["error"] == "invalid_signature"


def test_body_changed_after_signing_is_rejected(client):
    body = fixture_bytes("01_normal_stock.json")
    tampered = body.replace(b'"observed_quantity": 8', b'"observed_quantity": 80')
    assert tampered != body
    response = client.post(EVENTS_URL, content=tampered, headers=sign_headers(body))
    assert response.status_code == 401
    assert response.json()["error"] == "invalid_signature"


@pytest.mark.parametrize("offset", [-301, 301])
def test_timestamp_outside_the_window_is_rejected(client, offset):
    body = fixture_bytes("01_normal_stock.json")
    response = post_event(client, body, timestamp=int(time.time()) + offset)
    assert response.status_code == 401
    assert response.json()["error"] == "stale_timestamp"


def test_timestamp_inside_the_window_is_accepted(client):
    body = fixture_bytes("01_normal_stock.json")
    response = post_event(client, body, timestamp=int(time.time()) - 290)
    assert response.status_code == 201


@pytest.mark.parametrize(
    "make_timestamp",
    [
        pytest.param(lambda now: "1" * 13, id="13-digits"),
        pytest.param(lambda now: "1" * 400, id="400-digits"),  # float() overflowed: 500
        pytest.param(lambda now: "1" * 5000, id="5000-digits"),  # past int()'s limit: 500
        pytest.param(lambda now: "0" * 4291 + now, id="zero-padded-past-int-limit"),  # 500
        pytest.param(lambda now: "0" * 3 + now, id="zero-padded-13-digits"),
    ],
)
def test_signed_timestamp_longer_than_twelve_digits_is_invalid(client, app, make_timestamp):
    """The length is bounded before int() sees the text:"""
    body = fixture_bytes("01_normal_stock.json")
    response = post_event(client, body, timestamp=make_timestamp(str(int(time.time()))))
    assert response.status_code == 401
    assert response.json()["error"] == "invalid_timestamp"
    assert count_rows(app, Observation) == 0


def test_twelve_digit_timestamp_with_leading_zeros_is_accepted(client):
    body = fixture_bytes("01_normal_stock.json")
    response = post_event(client, body, timestamp="00" + str(int(time.time())))
    assert response.status_code == 201


def verify_at(timestamp: str, now: float, signed_text: str | None = None) -> None:
    """api.verify with a signature over `signed_text` (default:"""
    body = b"{}"
    message = (signed_text or timestamp).encode("latin-1") + b"." + body
    digest = hmac.new(SECRET.encode(), message, hashlib.sha256).hexdigest()
    api.verify(
        timestamp=timestamp,
        signature=f"v1={digest}",
        body=body,
        secret=SECRET.encode(),
        window_seconds=300,
        now=now,
    )


@pytest.mark.parametrize("header", ["{ts}\xa0", " {ts}", "{ts}\t", "{ts}\n"])
@pytest.mark.parametrize("signed", ["digits", "header"])
def test_timestamp_header_is_taken_exactly_as_sent(header, signed):
    """The application layer:"""
    ts = str(int(time.time()))
    value = header.format(ts=ts)
    with pytest.raises(api.AuthError) as error:
        verify_at(value, time.time(), signed_text=ts if signed == "digits" else value)
    assert error.value.code == "invalid_timestamp"


def test_timestamp_header_over_http_is_what_the_server_passes_on(live_server):
    """Over real HTTP the server drops spaces and tabs around a header value (RFC 9110 optional
    whitespace) before the app sees it:"""
    live = live_server()
    body = fixture_bytes("01_normal_stock.json")
    ts = str(int(time.time()))
    cases = [  # (header value sent, text signed, status, error)
        (ts + "\xa0", ts, 401, "invalid_timestamp"),
        ("\x85" + ts, ts, 401, "invalid_timestamp"),
        (ts[:5] + " " + ts[5:], ts[:5] + " " + ts[5:], 401, "invalid_timestamp"),
        (ts + " ", ts + " ", 401, "invalid_signature"),
        ("\t" + ts + " ", ts, 201, None),
    ]
    for value, signed, status, error in cases:
        message = signed.encode("latin-1") + b"." + body
        digest = hmac.new(SECRET.encode(), message, hashlib.sha256).hexdigest()
        head = (
            f"POST {EVENTS_URL} HTTP/1.1\nHost: test\nContent-Type: application/json\n"
            f"X-Webhook-Timestamp: {value}\nX-Webhook-Signature: v1={digest}\n"
            f"Content-Length: {len(body)}\n\n"
        )
        answer = send_partial(live.url, head, body)
        assert (answer[0], answer[1].get("error")) == (status, error), repr(value)


@pytest.mark.parametrize(
    ("timestamp", "now", "message"),
    [
        ("1301", 1000.7, "timestamp is -301 s from server time"),  # int(skew) printed -300
        ("699", 1000.7, "timestamp is 301 s from server time"),
        ("1300", 1000.7, None),
        ("700", 1000.7, None),  # the float compare said stale while printing 300
    ],
)
def test_replay_window_compares_and_reports_whole_seconds(timestamp, now, message):
    if message is None:
        verify_at(timestamp, now)
        return
    with pytest.raises(api.AuthError) as error:
        verify_at(timestamp, now)
    assert error.value.code == "stale_timestamp"
    assert error.value.message.startswith(message)


@pytest.mark.parametrize("missing", ["X-Webhook-Timestamp", "X-Webhook-Signature"])
def test_missing_header_is_rejected(client, missing):
    body = fixture_bytes("01_normal_stock.json")
    headers = sign_headers(body)
    del headers[missing]
    response = client.post(EVENTS_URL, content=body, headers=headers)
    assert response.status_code == 401
    assert response.json()["error"] == "missing_signature"


@pytest.mark.parametrize(
    ("header", "value", "error"),
    [
        ("X-Webhook-Signature", "sha256=abcdef", "invalid_signature"),
        ("X-Webhook-Signature", "v1=", "invalid_signature"),
        ("X-Webhook-Timestamp", "yesterday", "invalid_timestamp"),
    ],
)
def test_malformed_headers_are_rejected(client, header, value, error):
    body = fixture_bytes("01_normal_stock.json")
    headers = {**sign_headers(body), header: value}
    response = client.post(EVENTS_URL, content=body, headers=headers)
    assert response.status_code == 401
    assert response.json()["error"] == error


def test_authentication_runs_before_json_parsing(client):
    unsigned = client.post(
        EVENTS_URL, content=b"{not json", headers={"Content-Type": "application/json"}
    )
    assert unsigned.status_code == 401
    signed = post_event(client, b"{not json")
    assert signed.status_code == 422
    assert signed.json()["errors"][0]["type"] == "json_invalid"


def test_rejected_requests_persist_nothing(client, app):
    body = fixture_bytes("02_low_stock.json")
    post_event(client, body, secret="nope")
    post_event(client, body, timestamp=int(time.time()) - 3600)
    assert count_rows(app, Observation) == 0
    assert count_rows(app, ReplenishmentRequest) == 0


def padded_event(size: int) -> bytes:
    """A valid event of exactly `size` bytes."""
    body = event_body(metadata={"pad": ""})
    body = event_body(metadata={"pad": "x" * (size - len(body))})
    assert len(body) == size
    return body


def test_body_at_the_limit_is_accepted(client):
    assert post_event(client, padded_event(65_536)).status_code == 201


def test_body_over_the_limit_is_413(client, app):
    response = post_event(client, padded_event(65_537))  # Content-Length says so up front
    assert response.status_code == 413
    assert response.json() == {
        "error": "body_too_large",
        "message": "request body exceeds 65536 bytes",
    }
    assert count_rows(app, Observation) == 0


def test_streamed_body_over_the_limit_is_413(client, app):
    body = padded_event(65_537)

    def chunks():  # no Content-Length: the limit applies while reading
        yield body[:40_000]
        yield body[40_000:]

    response = client.post(EVENTS_URL, content=chunks(), headers=sign_headers(body))
    assert response.status_code == 413
    assert response.json()["error"] == "body_too_large"
    assert count_rows(app, Observation) == 0


def test_body_limit_follows_the_setting(database_url):
    app = create_app(make_settings(database_url, webhook_max_body_bytes=1024))
    with TestClient(app) as client:
        assert post_fixture(client, "01_normal_stock.json").status_code == 201
        assert post_event(client, padded_event(1025)).status_code == 413


def send_partial(url: str, head: str, body_start: bytes) -> tuple[int, dict[str, Any]]:
    """Send a request head and the start of its body over a raw socket and wait up to 5 s for the
    answer."""
    parts = urlsplit(url)
    with socket.create_connection((parts.hostname, parts.port), timeout=5) as sock:
        sock.sendall(head.replace("\n", "\r\n").encode("latin-1") + body_start)
        reply = b""
        while not complete(reply):
            chunk = sock.recv(65_536)  # TimeoutError if the server keeps waiting
            assert chunk, "the server closed the connection without answering"
            reply += chunk
    head_bytes, _, content = reply.partition(b"\r\n\r\n")
    return int(head_bytes.split()[1]), json.loads(content)


def complete(reply: bytes) -> bool:
    head, separator, content = reply.partition(b"\r\n\r\n")
    found = re.search(rb"(?im)^content-length: *(\d+)", head)
    return bool(separator) and found is not None and len(content) >= int(found.group(1))


def test_unsigned_or_oversized_uploads_are_refused_before_the_body_arrives(live_server):
    """Before, the whole body was buffered before even the header check:"""
    live = live_server()
    start = f"POST {EVENTS_URL} HTTP/1.1\nHost: test\nContent-Type: application/json\n"
    signed = f"X-Webhook-Timestamp: {int(time.time())}\nX-Webhook-Signature: v1=00\n"

    status, body = send_partial(live.url, start + "Content-Length: 10000000\n\n", b"{" * 1024)
    assert (status, body["error"]) == (401, "missing_signature")

    status, body = send_partial(live.url, start + signed + "Content-Length: 10000000\n\n", b"")
    assert (status, body["error"]) == (413, "body_too_large")

    chunk = b"9c40\r\n" + b"x" * 0x9C40 + b"\r\n"  # two 40,000-byte chunks, no last chunk
    head = start + signed + "Transfer-Encoding: chunked\n\n"
    status, body = send_partial(live.url, head, chunk * 2)
    assert (status, body["error"]) == (413, "body_too_large")
