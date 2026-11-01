"""ERP delivery:"""

import asyncio
import threading
import time
import uuid
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import null, select, update

from restock import api, outbound
from restock.main import create_app
from restock.outbound import OutboundWorker, backoff_delay, classify, parse_retry_after
from restock.runtime import make_engine, make_sessionmaker
from restock.schemas import Observation, OutboundAttempt, ReplenishmentRequest

from .helpers import (
    deliver_due,
    event_body,
    fixture_bytes,
    make_settings,
    post_event,
    post_fixture,
    requests_now,
    wait_for,
)

ORDER = {"erp_order_id": "ERP-7001", "status": "accepted", "duplicate": False}


@pytest.mark.parametrize(
    ("status", "body", "classification"),
    [
        (200, ORDER, "success"),
        (201, ORDER, "success"),
        (200, {**ORDER, "duplicate": True}, "success"),
        (201, {"erp_order_id": "E" * 128}, "success"),  # as long as its column holds
        (200, None, "ambiguous"),  # no body
        (200, {"raw": "<html>Welcome</html>"}, "ambiguous"),  # not JSON
        (201, {"status": "accepted"}, "ambiguous"),  # no erp_order_id
        (201, {"erp_order_id": 7001}, "ambiguous"),  # not a string
        (201, {"erp_order_id": ""}, "ambiguous"),
        (201, {"erp_order_id": "E" * 129}, "ambiguous"),  # would not fit its column
        (201, {"erp_order_id": "ERP-\x00"}, "ambiguous"),  # PostgreSQL cannot store NUL
        (201, ["ERP-7001"], "ambiguous"),
        (429, None, "retryable"),
        (500, None, "retryable"),
        (502, None, "retryable"),
        (503, None, "retryable"),
        (504, None, "retryable"),
        (400, None, "non_retryable"),
        (404, None, "non_retryable"),
        (409, None, "non_retryable"),
        (422, ORDER, "non_retryable"),
    ],
)
def test_classify(status, body, classification):
    assert classify(status, body) == classification


def test_backoff_is_exponential_capped_and_jittered():
    assert backoff_delay(1, base=1, cap=30, rand=lambda: 0.0) == 0.5
    assert backoff_delay(1, base=1, cap=30, rand=lambda: 1.0) == 1.0
    assert backoff_delay(3, base=1, cap=30, rand=lambda: 1.0) == 4.0
    assert backoff_delay(6, base=1, cap=30, rand=lambda: 1.0) == 30.0  # 32 capped
    assert 15.0 <= backoff_delay(9, base=1, cap=30) <= 30.0


def test_retry_after_parsing():
    now = datetime(2026, 10, 27, 12, 0, 0, tzinfo=UTC)
    assert parse_retry_after("2", now) == 2.0
    assert parse_retry_after("Tue, 27 Oct 2026 12:00:05 GMT", now) == 5.0
    assert parse_retry_after("soon", now) is None
    assert parse_retry_after(None, now) is None


# --- real HTTP through a live server ---------------------------------------------------------


def start(live_server, scenarios, **overrides):
    live = live_server(**overrides)
    http = httpx.Client(base_url=live.url, timeout=30)
    assert http.post("/mock-erp/_control", json={"scenarios": scenarios}).status_code == 200
    return live, http


def wait_for_status(live, request_id, statuses, timeout=20.0):
    def current():
        items = requests_now(live)
        found = next((r for r in items if r["id"] == request_id), None)
        return found if found and found["status"] in statuses else None

    return wait_for(current, timeout=timeout, what=f"request {request_id} to be {statuses}")


def attempts_for(http, request_id):
    params = {"status": "all", "request_id": request_id}
    items = http.get("/api/v1/outbound-attempts", params=params).json()["items"]
    return sorted(items, key=lambda a: a["attempt_no"])


def test_rate_limited_then_unavailable_then_success(live_server):
    live, http = start(live_server, ["rate_limited", "temporary_failure", "success"])
    request_id = post_fixture(http, "02_low_stock.json").json()["replenishment_request"]["id"]

    request = wait_for_status(live, request_id, {"delivered"})
    attempts = attempts_for(http, request_id)
    assert [(a["classification"], a["http_status"]) for a in attempts] == [
        ("retryable", 429),
        ("retryable", 503),
        ("success", 201),
    ]
    # Retry-After: 2 from the 429 was honoured before attempt 2.
    gap = datetime.fromisoformat(attempts[1]["started_at"]) - datetime.fromisoformat(
        attempts[0]["finished_at"]
    )
    assert gap >= timedelta(seconds=1.95)
    assert request["attempt_count"] == 3 and request["erp_order_id"]
    assert request["erp_order_id"] == attempts[2]["response_body"]["erp_order_id"]

    mock = http.get("/mock-erp/_control").json()
    key = f"replenishment:{request_id}"
    assert [r["idempotency_key"] for r in mock["received"]] == [key] * 3
    assert {r["correlation_id"] for r in mock["received"]} == {"corr_5002"}
    assert mock["orders"] == {key: request["erp_order_id"]}  # one logical order


def test_timeout_after_accept_resolves_to_one_order_with_the_same_key(live_server):
    live, http = start(live_server, ["timeout_after_accept"], erp_timeout_seconds=1.0)
    request_id = post_fixture(http, "02_low_stock.json").json()["replenishment_request"]["id"]

    request = wait_for_status(live, request_id, {"delivered"})
    first, second = attempts_for(http, request_id)
    assert first["classification"] == "ambiguous" and first["http_status"] is None
    assert "timeout" in first["error"]
    assert second["classification"] == "success" and second["http_status"] == 200
    assert second["response_body"]["duplicate"] is True
    assert first["idempotency_key"] == second["idempotency_key"] == f"replenishment:{request_id}"

    mock = http.get("/mock-erp/_control").json()
    received = [r["idempotency_key"] for r in mock["received"]]
    assert received == [f"replenishment:{request_id}"] * 2  # same key on the wire, twice
    assert list(mock["orders"].values()) == [request["erp_order_id"]]  # one order


def test_permanent_rejection_then_concurrent_manual_retries(live_server):
    live, http = start(live_server, ["permanent_rejection"])
    request_id = post_fixture(http, "02_low_stock.json").json()["replenishment_request"]["id"]
    wait_for_status(live, request_id, {"failed"})

    failed = http.get("/api/v1/outbound-attempts", params={"status": "failed"}).json()["items"]
    assert len(failed) == 1
    attempt = failed[0]
    assert attempt["classification"] == "non_retryable" and attempt["http_status"] == 422
    assert attempt["response_body"]["error"] == "unknown_sku"
    assert attempt["request"]["status"] == "failed"
    assert attempt["request"]["last_error"] == "HTTP 422: unknown_sku"

    # Ten operators click Retry at the same moment: exactly one schedules it.
    barrier = threading.Barrier(10)
    results: list[httpx.Response] = []

    def click() -> None:
        with httpx.Client(base_url=live.url, timeout=30) as client:
            barrier.wait()
            results.append(client.post(f"/api/v1/outbound-attempts/{attempt['id']}/retry"))

    threads = [threading.Thread(target=click) for _ in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    # Every click is answered, never with a 500 (a rolled-back click is not a safe retry).
    assert sorted(r.status_code for r in results) == [200] * 9 + [202]
    assert {r.json()["result"] for r in results if r.status_code == 200} <= {
        "already_scheduled",
        "already_delivered",
    }

    request = wait_for_status(live, request_id, {"delivered"})
    attempts = attempts_for(http, request_id)
    assert [(a["triggered_by"], a["classification"]) for a in attempts] == [
        ("worker", "non_retryable"),
        ("manual", "success"),
    ]
    mock = http.get("/mock-erp/_control").json()
    assert list(mock["orders"].values()) == [request["erp_order_id"]]
    assert http.get("/api/v1/outbound-attempts", params={"status": "failed"}).json()["items"] == []
    again = http.post(f"/api/v1/outbound-attempts/{attempt['id']}/retry")
    assert (again.status_code, again.json()["result"]) == (200, "already_delivered")


def test_exhausted_retries_fail_then_manual_retry_delivers(live_server):
    live, http = start(live_server, ["temporary_failure"] * 3, erp_max_attempts=3)
    request_id = post_fixture(http, "02_low_stock.json").json()["replenishment_request"]["id"]

    request = wait_for_status(live, request_id, {"failed"})
    assert request["last_error"] == "exhausted" and request["attempt_count"] == 3
    assert [a["http_status"] for a in attempts_for(http, request_id)] == [503, 503, 503]
    failed = http.get("/api/v1/outbound-attempts", params={"status": "failed"}).json()["items"]
    assert [(a["request_id"], a["attempt_no"]) for a in failed] == [(request_id, 3)]

    response = http.post(f"/api/v1/outbound-attempts/{failed[0]['id']}/retry")
    assert (response.status_code, response.json()["result"]) == (202, "scheduled")
    request = wait_for_status(live, request_id, {"delivered"})
    assert request["attempt_count"] == 4  # the manual retry is one more attempt


@pytest.mark.parametrize(
    "unsafe",
    ["corr_€42", "corr_Zürich_5002", "corr_a\r\nX-Injected: 1", "corr_5002\n"],
    ids=["euro", "latin1", "crlf", "trailing-newline"],
)
def test_unsafe_event_correlation_id_is_kept_out_of_headers(live_server, unsafe):
    """The correlation id goes into the X-Request-ID response header and the ERP's X-Correlation-ID
    header."""
    live, http = start(live_server, [])
    body = event_body(metadata={"snapshot_id": "snap_8841", "correlation_id": unsafe})
    first, again = post_event(http, body), post_event(http, body)
    assert (first.status_code, again.status_code) == (201, 200)
    generated = str(uuid.UUID(first.headers["X-Request-ID"]))

    request_id = first.json()["replenishment_request"]["id"]
    wait_for_status(live, request_id, {"delivered"})
    received = http.get("/mock-erp/_control").json()["received"]
    assert [r["idempotency_key"] for r in received] == [f"replenishment:{request_id}"]
    assert received[0]["correlation_id"] == generated  # one id still traces event -> ERP
    with live.app.state.sessions() as session:
        observation = session.query(Observation).one()
    assert observation.correlation_id == generated
    assert observation.meta["correlation_id"] == unsafe  # the gateway's value is kept as data


# --- deterministic worker passes through the TestClient ------------------------------------


def test_manual_retry_state_machine(client, app):
    client.post("/mock-erp/_control", json={"scenarios": ["permanent_rejection"]})
    post_fixture(client, "02_low_stock.json")
    assert deliver_due(app, client) == 1
    attempt_id = client.get("/api/v1/outbound-attempts").json()["items"][0]["id"]

    first = client.post(f"/api/v1/outbound-attempts/{attempt_id}/retry")
    assert (first.status_code, first.json()["result"]) == (202, "scheduled")
    second = client.post(f"/api/v1/outbound-attempts/{attempt_id}/retry")
    assert (second.status_code, second.json()["result"]) == (200, "already_scheduled")
    assert deliver_due(app, client) == 1  # queue now empty -> success
    third = client.post(f"/api/v1/outbound-attempts/{attempt_id}/retry")
    assert (third.status_code, third.json()["result"]) == (200, "already_delivered")
    assert deliver_due(app, client) == 0

    attempts = client.get("/api/v1/outbound-attempts", params={"status": "all"}).json()["items"]
    assert sorted(a["triggered_by"] for a in attempts) == ["manual", "worker"]
    assert len({a["idempotency_key"] for a in attempts}) == 1
    assert len(app.state.mock_erp.snapshot()["orders"]) == 1


def supersede_a_failed_request(client, app) -> tuple[int, str, str]:
    """02's request is refused by the ERP (failed); then 05 (a recovery) and 06 (low again) start a
    new cycle, whose request supersedes it."""
    client.post("/mock-erp/_control", json={"scenarios": ["permanent_rejection"]})
    old_id = post_fixture(client, "02_low_stock.json").json()["replenishment_request"]["id"]
    assert deliver_due(app, client) == 1
    post_fixture(client, "05_restocked.json")
    new_id = post_fixture(client, "06_new_depletion.json").json()["replenishment_request"]["id"]
    (failed,) = client.get("/api/v1/outbound-attempts", params={"status": "failed"}).json()["items"]
    return failed["id"], old_id, new_id


def test_retry_of_a_superseded_request_is_refused(client, app):
    """Once a newer request exists for the item, retrying the old failed one would make two orders
    for one bin."""
    attempt_id, old_id, new_id = supersede_a_failed_request(client, app)
    (failed,) = client.get("/api/v1/outbound-attempts", params={"status": "failed"}).json()["items"]
    assert (failed["request_id"], failed["request"]["superseded_by"]) == (old_id, new_id)

    def click() -> None:
        response = client.post(f"/api/v1/outbound-attempts/{attempt_id}/retry")
        assert response.status_code == 409
        assert response.json() == {
            "error": "superseded",
            "message": f"a newer replenishment request exists for this item: {new_id}",
        }

    def requests() -> dict[str, tuple[str, int, str | None]]:
        items = requests_now(client)
        return {r["id"]: (r["status"], r["attempt_count"], r["superseded_by"]) for r in items}

    click()
    click()
    assert requests() == {old_id: ("failed", 1, new_id), new_id: ("pending", 0, None)}
    assert deliver_due(app, client) == 1  # only the new request is due; the ERP accepts it
    click()
    assert requests() == {old_id: ("failed", 1, new_id), new_id: ("delivered", 1, None)}
    attempts = client.get("/api/v1/outbound-attempts", params={"status": "all"}).json()["items"]
    assert sorted((a["request_id"], a["attempt_no"]) for a in attempts) == sorted(
        [(old_id, 1), (new_id, 1)]
    )
    assert len(app.state.mock_erp.snapshot()["orders"]) == 1  # the new request's order only


def test_unique_index_backstop_turns_a_racing_retry_into_409(client, app, monkeypatch, json_logs):
    """A retry and a new low event for the same item at the same moment:"""
    attempt_id, old_id, new_id = supersede_a_failed_request(client, app)
    real_lookup = api.newer_request_id
    looks: list[int] = []

    def miss_the_first_look():
        looks.append(len(looks))
        return null() if len(looks) == 1 else real_lookup()

    monkeypatch.setattr(api, "newer_request_id", miss_the_first_look)
    response = client.post(f"/api/v1/outbound-attempts/{attempt_id}/retry")

    assert response.status_code == 409 and response.json()["error"] == "superseded"
    assert response.json()["message"].endswith(new_id)
    assert looks == [0, 1]  # the UPDATE's look, then the read after the index refused it
    items = requests_now(client)
    assert {r["id"]: r["status"] for r in items} == {old_id: "failed", new_id: "pending"}
    attempts = client.get("/api/v1/outbound-attempts", params={"status": "all"}).json()["items"]
    assert [a["id"] for a in attempts] == [attempt_id]
    messages = [line["msg"] for line in json_logs.lines if line["level"] == "WARNING"]
    assert "a newer request for the item was created at the same moment; not retried" in messages


def test_retry_of_a_delivered_request_stays_already_delivered(client, app):
    """A delivered request is not superseded by its item's next request:"""
    request_id = post_fixture(client, "02_low_stock.json").json()["replenishment_request"]["id"]
    assert deliver_due(app, client) == 1
    post_fixture(client, "05_restocked.json")
    assert post_fixture(client, "06_new_depletion.json").json()["decision"] == "request_created"
    params = {"status": "all", "request_id": request_id}
    (attempt,) = client.get("/api/v1/outbound-attempts", params=params).json()["items"]
    assert attempt["request"]["superseded_by"] is None

    response = client.post(f"/api/v1/outbound-attempts/{attempt['id']}/retry")
    assert (response.status_code, response.json()["result"]) == (200, "already_delivered")


def mock_erp(response) -> httpx.Client:
    """An ERP that answers every call with response() (an httpx.Response factory)."""
    return httpx.Client(transport=httpx.MockTransport(lambda request: response()))


class HeldErp:
    """An ERP that holds each call open until release(), then answers with `response`."""

    def __init__(self, response: httpx.Response) -> None:
        self.response = response
        self.calls = 0
        self.entered = threading.Event()
        self._released = threading.Event()
        self.client = httpx.Client(transport=httpx.MockTransport(self._answer))

    def _answer(self, request: httpx.Request) -> httpx.Response:
        self.calls += 1
        self.entered.set()
        assert self._released.wait(10)
        return self.response

    def release(self) -> None:
        self._released.set()


def run_in_thread(target, errors: list[Exception]) -> threading.Thread:
    def run() -> None:
        try:
            target()
        except Exception as exc:  # the test asserts errors == [] at the end
            errors.append(exc)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread


def test_retry_clicked_while_the_request_is_being_sent_changes_nothing(client, app):
    """An operator clicks Retry again while the retry the first click scheduled is on the wire."""
    client.post("/mock-erp/_control", json={"scenarios": ["permanent_rejection"]})
    post_fixture(client, "02_low_stock.json")
    assert deliver_due(app, client) == 1
    failed_id = client.get("/api/v1/outbound-attempts").json()["items"][0]["id"]
    assert client.post(f"/api/v1/outbound-attempts/{failed_id}/retry").status_code == 202

    erp = HeldErp(httpx.Response(201, json={"erp_order_id": "ERP-9001"}))
    worker = OutboundWorker(app.state.sessions, app.state.settings, http_client=erp.client)
    errors: list[Exception] = []
    thread = run_in_thread(worker.run_once, errors)
    assert erp.entered.wait(10)  # the manual retry's call is in flight

    def attempts():
        items = client.get("/api/v1/outbound-attempts", params={"status": "all"}).json()["items"]
        return sorted((a["attempt_no"], a["triggered_by"], a["classification"]) for a in items)

    click = client.post(f"/api/v1/outbound-attempts/{failed_id}/retry")
    assert (click.status_code, click.json()["result"]) == (200, "already_scheduled")
    assert click.json()["request_status"] == "in_flight"
    assert worker._due_request_ids() == []  # not queued a second time
    assert attempts() == [(1, "worker", "non_retryable"), (2, "manual", "in_progress")]

    erp.release()
    thread.join(10)
    assert errors == [] and erp.calls == 1
    request = requests_now(client)[0]
    assert (request["status"], request["attempt_count"]) == ("delivered", 2)
    assert attempts() == [(1, "worker", "non_retryable"), (2, "manual", "success")]


def test_a_manual_retry_keeps_the_operators_correlation_id(client, app, json_logs):
    """The id the operator's Retry call was logged under (its X-Request-ID) reaches the attempt it
    triggers:"""
    client.post("/mock-erp/_control", json={"scenarios": ["permanent_rejection"]})
    request_id = post_fixture(client, "02_low_stock.json").json()["replenishment_request"]["id"]
    assert deliver_due(app, client) == 1
    failed_id = client.get("/api/v1/outbound-attempts").json()["items"][0]["id"]

    click = client.post(
        f"/api/v1/outbound-attempts/{failed_id}/retry", headers={"X-Request-ID": "op-retry-7"}
    )
    assert (click.status_code, click.headers["X-Request-ID"]) == (202, "op-retry-7")
    (retry_line,) = [line for line in json_logs.lines if line["msg"] == "manual retry requested"]
    assert (retry_line["correlation_id"], retry_line["replenishment_id"]) == (
        "op-retry-7",
        request_id,
    )
    assert deliver_due(app, client) == 1

    items = client.get("/api/v1/outbound-attempts", params={"status": "all"}).json()["items"]
    assert sorted(
        (a["attempt_no"], a["triggered_by"], a["correlation_id"], a["manual_retry_correlation_id"])
        for a in items
    ) == [(1, "worker", "corr_5002", None), (2, "manual", "corr_5002", "op-retry-7")]
    (manual,) = [
        line
        for line in json_logs.lines
        if line["msg"] == "erp attempt finished" and line["triggered_by"] == "manual"
    ]
    assert (manual["manual_retry_correlation_id"], manual["correlation_id"]) == (
        "op-retry-7",
        "corr_5002",
    )
    received = app.state.mock_erp.snapshot()["received"]
    assert [r["correlation_id"] for r in received] == ["corr_5002", "corr_5002"]


@pytest.mark.parametrize(
    ("response", "status", "erp_order_id"),
    [
        pytest.param(
            lambda: httpx.Response(429, headers={"Retry-After": "300000000000"}),
            "pending",
            None,
            id="retry-after-beyond-year-9999",
        ),
        pytest.param(
            lambda: httpx.Response(429, headers=[(b"Retry-After", b"\xb2")]),  # decodes as "²"
            "pending",
            None,
            id="retry-after-non-ascii-digit",
        ),
        pytest.param(
            lambda: httpx.Response(503, content=b'{"error": "\\ud800"}'),
            "pending",
            None,
            id="unpaired-surrogate-in-error",
        ),
        pytest.param(
            lambda: httpx.Response(503, content=b"[" * 100_000 + b"]" * 100_000),
            "pending",
            None,
            id="deeply-nested-body",
        ),
        pytest.param(  # PostgreSQL text and JSONB cannot hold NUL
            lambda: httpx.Response(503, content=b'{"error": "db\\u0000down"}'),
            "pending",
            None,
            id="nul-in-error",
        ),
        pytest.param(  # PostgreSQL JSONB rejects NaN
            lambda: httpx.Response(201, content=b'{"erp_order_id": "ERP-1", "score": NaN}'),
            "delivered",
            "ERP-1",
            id="nan-in-body",
        ),
    ],
)
def test_every_erp_response_is_recorded_as_an_attempt(client, app, response, status, erp_order_id):
    """Whatever the ERP answers, one attempt row is written and the request leaves in_flight."""
    post_fixture(client, "02_low_stock.json")
    worker = OutboundWorker(app.state.sessions, app.state.settings, http_client=mock_erp(response))
    assert worker.run_once() == 1

    request = requests_now(client)[0]
    assert (request["status"], request["attempt_count"]) == (status, 1)
    assert request["erp_order_id"] == erp_order_id
    attempts = client.get("/api/v1/outbound-attempts", params={"status": "all"}).json()["items"]
    assert [(a["attempt_no"], a["http_status"]) for a in attempts] == [(1, response().status_code)]
    assert attempts[0]["classification"] not in ("in_progress", "lost")  # the row was completed


@pytest.mark.parametrize(
    "response",
    [
        pytest.param(
            lambda: httpx.Response(200, text="<html><body>It works!</body></html>"),
            id="html",
        ),
        pytest.param(lambda: httpx.Response(200), id="no-body"),
        pytest.param(lambda: httpx.Response(200, json={"status": "accepted"}), id="json-no-id"),
        pytest.param(
            lambda: httpx.Response(201, json={"erp_order_id": 7001}), id="id-not-a-string"
        ),
        pytest.param(lambda: httpx.Response(201, json={"erp_order_id": ""}), id="empty-id"),
        pytest.param(  # erp_order_id is VARCHAR(128): the id would be cut, so it is no proof
            lambda: httpx.Response(201, json={"erp_order_id": "ERP-" + "9" * 296}),
            id="id-of-300-characters",
        ),
        pytest.param(  # PostgreSQL text cannot hold NUL, so the id could not be stored
            lambda: httpx.Response(201, content=b'{"erp_order_id": "ERP-\\u00001"}'),
            id="nul-in-id",
        ),
    ],
)
def test_a_2xx_that_does_not_name_the_order_is_ambiguous(client, app, response):
    """A 2xx counts as delivered only if its JSON body names the order with an erp_order_id (a non-
    empty string of at most 128 characters)."""
    post_fixture(client, "02_low_stock.json")
    worker = OutboundWorker(app.state.sessions, app.state.settings, http_client=mock_erp(response))
    assert worker.run_once() == 1

    (attempt,) = client.get("/api/v1/outbound-attempts", params={"status": "all"}).json()["items"]
    assert (attempt["classification"], attempt["http_status"]) == (
        "ambiguous",
        response().status_code,
    )
    assert attempt["error"].startswith("2xx without erp_order_id")
    request = attempt["request"]
    assert (request["status"], request["attempt_count"], request["erp_order_id"]) == (
        "pending",
        1,
        None,
    )
    assert request["next_attempt_at"] is not None
    assert request["last_error"].startswith("2xx without erp_order_id")


def test_a_2xx_without_an_order_id_exhausts_and_a_later_201_delivers(client, app, database_url):
    """Every attempt gets a 200 that does not name the order:"""
    request_id = post_fixture(client, "02_low_stock.json").json()["replenishment_request"]["id"]
    answers = [httpx.Response(200, json={"status": "accepted"})] * 3
    answers.append(httpx.Response(201, json={"erp_order_id": "ERP-9001", "duplicate": False}))
    calls: list[str] = []

    def erp(request: httpx.Request) -> httpx.Response:
        calls.append(request.headers["Idempotency-Key"])
        return answers[len(calls) - 1]

    settings = make_settings(database_url, erp_max_attempts=3, erp_backoff_base_seconds=0)
    erp_client = httpx.Client(transport=httpx.MockTransport(erp))
    worker = OutboundWorker(app.state.sessions, settings, http_client=erp_client)
    for _ in range(4):  # one pass more than the attempts allowed
        worker.run_once()

    (failed,) = client.get("/api/v1/outbound-attempts", params={"status": "failed"}).json()["items"]
    assert (failed["request_id"], failed["attempt_no"], failed["classification"]) == (
        request_id,
        3,
        "ambiguous",
    )
    assert failed["error"] == "2xx without erp_order_id"
    assert (failed["request"]["status"], failed["request"]["last_error"]) == ("failed", "exhausted")
    page = client.get("/").text
    assert "Stopped after 3 tries. Last answer: The ERP gave an unexpected answer." in page
    assert f'data-attempt="{failed["id"]}"' in page  # its Retry button

    retry = client.post(f"/api/v1/outbound-attempts/{failed['id']}/retry")
    assert retry.status_code == 202
    assert worker.run_once() == 1
    request = requests_now(client)[0]
    assert (request["status"], request["erp_order_id"], request["attempt_count"]) == (
        "delivered",
        "ERP-9001",
        4,
    )
    assert calls == [f"replenishment:{request_id}"] * 4  # one key throughout


@pytest.mark.parametrize(
    ("retry_after", "shortest", "longest"),
    [
        ("0", 0.5, 1.0),  # never sooner than my own backoff (attempt 1 with base 1 s: 0.5-1 s)
        ("2", 2.0, 2.0),  # the fixture's value is honoured as given
        ("86400", 300.0, 300.0),  # a day is cut to ERP_RETRY_AFTER_MAX_SECONDS
        ("1000000000000", 300.0, 300.0),  # beyond year 9999: cut too, and the body is kept
    ],
)
def test_retry_after_is_honoured_between_the_backoff_and_the_cap(
    client, app, database_url, retry_after, shortest, longest
):
    post_fixture(client, "02_low_stock.json")
    settings = make_settings(
        database_url,
        erp_backoff_base_seconds=1,
        erp_backoff_cap_seconds=30,
        erp_retry_after_max_seconds=300,
    )
    erp = mock_erp(lambda: httpx.Response(429, headers={"Retry-After": retry_after}, json={}))
    assert OutboundWorker(app.state.sessions, settings, http_client=erp).run_once() == 1

    with app.state.sessions() as session:
        request = session.scalars(select(ReplenishmentRequest)).one()
        attempt = session.scalars(select(OutboundAttempt)).one()
    wait = (request.next_attempt_at - request.updated_at).total_seconds()
    assert request.status == "pending" and shortest <= wait <= longest
    assert attempt.response_body == {} and attempt.error == "HTTP 429"  # recorded in full


def test_retry_of_unknown_attempt_is_404(client):
    response = client.post("/api/v1/outbound-attempts/999999/retry")
    assert response.status_code == 404 and response.json()["error"] == "not_found"


def test_timeouts_are_retried_a_bounded_number_of_times(client, app, database_url):
    """A timeout is ambiguous (the ERP may hold the order), so it is retried with the same key, but
    only ERP_MAX_ATTEMPTS times:"""
    request_id = post_fixture(client, "02_low_stock.json").json()["replenishment_request"]["id"]
    keys: list[str] = []

    def time_out(request: httpx.Request) -> httpx.Response:
        keys.append(request.headers["Idempotency-Key"])
        raise httpx.ReadTimeout("no answer", request=request)

    settings = make_settings(database_url, erp_max_attempts=3, erp_backoff_base_seconds=0)
    erp = httpx.Client(transport=httpx.MockTransport(time_out))
    worker = OutboundWorker(app.state.sessions, settings, http_client=erp)
    for _ in range(5):  # two more passes than attempts allowed
        worker.run_once()

    request = requests_now(client)[0]
    assert (request["status"], request["last_error"], request["attempt_count"]) == (
        "failed",
        "exhausted",
        3,
    )
    params = {"status": "all", "request_id": request_id}
    attempts = client.get("/api/v1/outbound-attempts", params=params).json()["items"]
    assert sorted((a["attempt_no"], a["classification"], a["http_status"]) for a in attempts) == [
        (1, "ambiguous", None),
        (2, "ambiguous", None),
        (3, "ambiguous", None),
    ]
    assert all("timeout" in a["error"] for a in attempts)
    assert keys == [f"replenishment:{request_id}"] * 3
    failed = client.get("/api/v1/outbound-attempts", params={"status": "failed"}).json()["items"]
    assert [(a["request_id"], a["attempt_no"]) for a in failed] == [(request_id, 3)]


def test_a_new_cycle_orders_again_after_a_failed_order_the_erp_took(client, app, database_url):
    """The cost of treating a failed request as final (docs/REPORT.md, Manual retry)."""
    old_id = post_fixture(client, "02_low_stock.json").json()["replenishment_request"]["id"]
    orders: dict[str, str] = {}  # what the ERP holds: the order id under each Idempotency-Key

    def erp(request: httpx.Request) -> httpx.Response:
        key = request.headers["Idempotency-Key"]
        orders.setdefault(key, f"ERP-{7001 + len(orders)}")
        if key == f"replenishment:{old_id}":  # stored, but the answer never arrives
            raise httpx.ReadTimeout("no answer", request=request)
        return httpx.Response(201, json={"erp_order_id": orders[key], "duplicate": False})

    settings = make_settings(database_url, erp_max_attempts=2, erp_backoff_base_seconds=0)
    erp_client = httpx.Client(transport=httpx.MockTransport(erp))
    worker = OutboundWorker(app.state.sessions, settings, http_client=erp_client)
    for _ in range(3):  # one pass more than the attempts allowed
        worker.run_once()
    (failed,) = client.get("/api/v1/outbound-attempts", params={"status": "failed"}).json()["items"]
    assert (failed["request_id"], failed["request"]["last_error"]) == (old_id, "exhausted")
    assert orders == {f"replenishment:{old_id}": "ERP-7001"}  # yet the ERP holds the order

    assert post_fixture(client, "05_restocked.json").json()["decision"] == "recovered"
    low_again = post_fixture(client, "06_new_depletion.json").json()
    assert low_again["decision"] == "request_created"  # blocked_open_request while failed was open
    new_id = low_again["replenishment_request"]["id"]
    assert worker.run_once() == 1

    items = requests_now(client)
    assert {r["id"]: (r["status"], r["erp_order_id"], r["superseded_by"]) for r in items} == {
        old_id: ("failed", None, new_id),
        new_id: ("delivered", "ERP-7002", None),
    }
    assert orders == {  # two orders for one bin
        f"replenishment:{old_id}": "ERP-7001",
        f"replenishment:{new_id}": "ERP-7002",
    }
    retry = client.post(f"/api/v1/outbound-attempts/{failed['id']}/retry")
    assert (retry.status_code, retry.json()["error"]) == (409, "superseded")
    assert worker.run_once() == 0 and len(orders) == 2  # no third order


def test_the_erp_receives_the_low_stock_order(client, app):
    """What the ERP actually received, read from the mock's received calls:"""
    client.post("/mock-erp/_control", json={"scenarios": ["temporary_failure"]})
    request_id = post_fixture(client, "02_low_stock.json").json()["replenishment_request"]["id"]
    assert deliver_due(app, client) == 1  # 503
    with app.state.sessions.begin() as session:  # make the retry due now
        session.execute(update(ReplenishmentRequest).values(next_attempt_at=datetime.now(UTC)))
    assert deliver_due(app, client) == 1  # 201

    received = client.get("/mock-erp/_control").json()["received"]
    assert [r["idempotency_key"] for r in received] == [f"replenishment:{request_id}"] * 2
    first, second = (r["body"] for r in received)
    assert {k: first[k] for k in ("request_id", "facility_id", "room_id", "sku", "quantity")} == {
        "request_id": request_id,
        "facility_id": "FAC-101",
        "room_id": "ROOM-3A",
        "sku": "MED-30224",
        "quantity": 8,
    }
    assert second == first  # the retry sent the same order
    with app.state.sessions() as session:  # and the attempt rows recorded what was sent
        rows = session.scalars(select(OutboundAttempt).order_by(OutboundAttempt.attempt_no))
        assert [row.request_body for row in rows] == [first, second]


def test_order_body_is_identical_on_every_attempt(client, app):
    client.post("/mock-erp/_control", json={"scenarios": ["temporary_failure"]})
    post_fixture(client, "02_low_stock.json")
    deliver_due(app, client)
    with app.state.sessions.begin() as session:  # make the retry due now
        session.execute(update(ReplenishmentRequest).values(next_attempt_at=datetime.now(UTC)))
    deliver_due(app, client)

    attempts = client.get("/api/v1/outbound-attempts", params={"status": "all"}).json()["items"]
    first, second = sorted(attempts, key=lambda a: a["attempt_no"])
    assert [first["classification"], second["classification"]] == ["retryable", "success"]
    with app.state.sessions() as session:
        bodies = [a.request_body for a in session.query(OutboundAttempt)]
    assert bodies[0] == bodies[1]
    assert bodies[0]["quantity"] == 8 and bodies[0]["triggering_event_id"] == "evt_1002"
    assert set(bodies[0]) == {
        "request_id",
        "facility_id",
        "room_id",
        "sku",
        "quantity",
        "par_level",
        "reorder_point",
        "observed_quantity",
        "triggering_event_id",
        "requested_at",
    }


def test_a_claim_writes_its_attempt_row_and_an_expired_one_is_kept_as_lost(client, app, json_logs):
    """The attempt row is written with the claim, before the call, so a process that dies mid-call
    still leaves a row:"""
    request_id = post_fixture(client, "02_low_stock.json").json()["replenishment_request"]["id"]
    worker = OutboundWorker(app.state.sessions, app.state.settings, http_client=client)
    assert worker._claim(uuid.UUID(request_id)) is not None  # ...and the process dies here

    def attempts():
        params = {"status": "all", "request_id": request_id}
        items = client.get("/api/v1/outbound-attempts", params=params).json()["items"]
        return sorted(items, key=lambda a: a["attempt_no"])

    def failed():
        return client.get("/api/v1/outbound-attempts", params={"status": "failed"}).json()["items"]

    (row,) = attempts()
    assert (row["attempt_no"], row["classification"], row["http_status"]) == (
        1,
        "in_progress",
        None,
    )
    assert (row["triggered_by"], row["correlation_id"]) == ("worker", "corr_5002")
    assert row["idempotency_key"] == f"replenishment:{request_id}"
    assert row["request"]["status"] == "in_flight" and failed() == []

    worker.lease_seconds = 0  # the lease has run out
    worker._requeue_expired_claims()
    (row,) = attempts()
    assert (row["classification"], row["error"]) == (
        "lost",
        "claim expired before a result was recorded",
    )
    assert row["http_status"] is None and row["finished_at"] >= row["started_at"]
    request = row["request"]
    assert (request["status"], request["attempt_count"]) == ("pending", 1)
    assert failed() == []  # a lost call does not make the request failed
    (line,) = [line for line in json_logs.lines if "lost" in line["msg"]]
    assert (line["replenishment_id"], line["correlation_id"]) == (request_id, "corr_5002")
    assert (line["event_id"], line["attempt_no"], line["requeued"]) == ("evt_1002", 1, True)
    assert line["level"] == "WARNING"

    assert deliver_due(app, client) == 1  # the retry reuses the key
    assert [(a["attempt_no"], a["classification"]) for a in attempts()] == [
        (1, "lost"),
        (2, "success"),
    ]


def test_late_result_of_an_expired_claim_does_not_decide_the_request(client, app, json_logs):
    """Worker A's call outlives its lease, so worker B requeues the request and sends it again (same
    key)."""
    post_fixture(client, "02_low_stock.json")
    entered = {"a": threading.Event(), "b": threading.Event()}
    answer = {"a": threading.Event(), "b": threading.Event()}

    def slow_erp(name: str, response: httpx.Response) -> httpx.Client:
        def handler(request: httpx.Request) -> httpx.Response:
            entered[name].set()
            assert answer[name].wait(10)
            return response

        return httpx.Client(transport=httpx.MockTransport(handler))

    sessions, settings = app.state.sessions, app.state.settings
    rejecting = slow_erp("a", httpx.Response(422, json={"error": "unknown_sku"}))
    accepting = slow_erp("b", httpx.Response(201, json={"erp_order_id": "ERP-9001"}))
    workers = {
        "a": OutboundWorker(sessions, settings, http_client=rejecting),
        "b": OutboundWorker(sessions, settings, http_client=accepting),
    }
    errors: list[Exception] = []

    def run(name: str) -> threading.Thread:
        def target() -> None:
            try:
                workers[name].run_once()
            except Exception as exc:
                errors.append(exc)

        thread = threading.Thread(target=target)
        thread.start()
        assert entered[name].wait(10)
        return thread

    thread_a = run("a")
    with sessions.begin() as session:  # A's claim has now outlived its lease
        stale = datetime.now(UTC) - timedelta(hours=1)
        session.execute(update(ReplenishmentRequest).values(updated_at=stale))
    thread_b = run("b")
    answer["a"].set()  # A's late answer arrives while B holds the claim
    thread_a.join(10)
    answer["b"].set()
    thread_b.join(10)
    assert errors == []

    request = requests_now(client)[0]
    assert (request["status"], request["erp_order_id"]) == ("delivered", "ERP-9001")
    attempts = client.get("/api/v1/outbound-attempts", params={"status": "all"}).json()["items"]
    assert sorted((a["attempt_no"], a["classification"]) for a in attempts) == [
        (1, "non_retryable"),
        (2, "success"),
    ]
    failed = client.get("/api/v1/outbound-attempts", params={"status": "failed"}).json()
    assert failed["items"] == []
    warnings = [line["msg"] for line in json_logs.lines if line["level"] == "WARNING"]
    assert warnings.count("the request was claimed again during this attempt; recorded only") == 1


@pytest.mark.parametrize(
    "current_status", [503, 409], ids=["current-503-at-the-last-try", "current-409"]
)
@pytest.mark.parametrize("late_first", [True, False], ids=["late-201-first", "late-201-last"])
def test_a_late_success_is_never_thrown_away(client, app, database_url, current_status, late_first):
    """Worker A's call outlives its lease, so worker B requeues the request and sends it again with
    the same key."""
    post_fixture(client, "02_low_stock.json")
    settings = make_settings(database_url, erp_max_attempts=2)
    late = HeldErp(httpx.Response(201, json={"erp_order_id": "ERP-9001"}))
    current = HeldErp(httpx.Response(current_status, json={"error": "busy or conflict"}))
    worker_a = OutboundWorker(app.state.sessions, settings, http_client=late.client)
    worker_b = OutboundWorker(app.state.sessions, settings, http_client=current.client)
    worker_b.lease_seconds = 0  # A's claim has outlived its lease by the time B polls
    errors: list[Exception] = []
    thread_a = run_in_thread(worker_a.run_once, errors)
    assert late.entered.wait(10)
    thread_b = run_in_thread(worker_b.run_once, errors)
    assert current.entered.wait(10)

    answers = [(late, thread_a), (current, thread_b)]
    if not late_first:
        answers.reverse()
    for erp, thread in answers:
        erp.release()
        thread.join(10)
    assert errors == [] and (late.calls, current.calls) == (1, 1)

    request = requests_now(client)[0]
    assert (request["status"], request["erp_order_id"]) == ("delivered", "ERP-9001")
    attempts = client.get("/api/v1/outbound-attempts", params={"status": "all"}).json()["items"]
    assert sorted((a["attempt_no"], a["http_status"]) for a in attempts) == [
        (1, 201),
        (2, current_status),
    ]
    assert (
        client.get("/api/v1/outbound-attempts", params={"status": "failed"}).json()["items"] == []
    )

    recovered = event_body(
        event_id="evt_1500", observed_at="2026-08-18T20:00:00Z", observed_quantity=12
    )
    assert post_event(client, recovered).json()["decision"] == "recovered"
    low = event_body(event_id="evt_1501", observed_at="2026-08-18T21:00:00Z", observed_quantity=3)
    assert post_event(client, low).json()["decision"] == "request_created"


@pytest.mark.parametrize(
    ("late_status", "current_status", "ends"),
    [(503, 422, "failed"), (422, 503, "pending")],
    ids=["late-503-then-current-422", "late-422-then-current-503"],
)
def test_a_late_failure_does_not_decide_for_the_current_claim(
    client, app, late_status, current_status, ends
):
    """Worker A's call outlives its lease and worker B claims the request again (attempt 2)."""
    post_fixture(client, "02_low_stock.json")
    sessions, settings = app.state.sessions, app.state.settings
    late = HeldErp(httpx.Response(late_status, json={"error": "late answer"}))
    current = HeldErp(httpx.Response(current_status, json={"error": "current answer"}))
    worker_a = OutboundWorker(sessions, settings, http_client=late.client)
    worker_b = OutboundWorker(sessions, settings, http_client=current.client)
    worker_b.lease_seconds = 0  # A's claim has outlived its lease by the time B polls
    errors: list[Exception] = []
    thread_a = run_in_thread(worker_a.run_once, errors)
    assert late.entered.wait(10)
    thread_b = run_in_thread(worker_b.run_once, errors)
    assert current.entered.wait(10)
    for erp, thread in ((late, thread_a), (current, thread_b)):  # A's answer comes first
        erp.release()
        thread.join(10)
    assert errors == []

    request = requests_now(client)[0]
    assert (request["status"], request["attempt_count"]) == (ends, 2)
    assert request["last_error"] == f"HTTP {current_status}: current answer"
    attempts = client.get("/api/v1/outbound-attempts", params={"status": "all"}).json()["items"]
    assert sorted((a["attempt_no"], a["http_status"]) for a in attempts) == [
        (1, late_status),
        (2, current_status),
    ]


def test_a_late_failure_does_not_decide_a_requeued_request(client, app, json_logs):
    """The claim expired and the request went back to pending, but no claim has taken it yet, so its
    attempt count is still the old claim's."""
    post_fixture(client, "02_low_stock.json")
    sessions, settings = app.state.sessions, app.state.settings
    erp = HeldErp(httpx.Response(422, json={"error": "unknown_sku"}))
    errors: list[Exception] = []
    thread = run_in_thread(
        OutboundWorker(sessions, settings, http_client=erp.client).run_once, errors
    )
    assert erp.entered.wait(10)
    janitor = OutboundWorker(sessions, settings, http_client=client)
    janitor.lease_seconds = 0
    janitor._requeue_expired_claims()  # requeued; nobody claims it before the 422 lands
    erp.release()
    thread.join(10)
    assert errors == []

    request = requests_now(client)[0]
    assert (request["status"], request["attempt_count"]) == ("pending", 1)
    attempts = client.get("/api/v1/outbound-attempts", params={"status": "all"}).json()["items"]
    assert [(a["attempt_no"], a["classification"]) for a in attempts] == [(1, "non_retryable")]
    assert (
        client.get("/api/v1/outbound-attempts", params={"status": "failed"}).json()["items"] == []
    )
    messages = [line["msg"] for line in json_logs.lines]
    assert "the claim expired during this attempt and was requeued; recorded only" in messages
    assert not any("claimed again" in message for message in messages)

    assert deliver_due(app, client) == 1
    request = requests_now(client)[0]
    assert (request["status"], request["attempt_count"]) == ("delivered", 2)


def test_a_claim_rechecks_that_the_request_is_due(client, app):
    """Worker B read its list of due requests just before worker A got a 429 with Retry-After:"""
    post_fixture(client, "02_low_stock.json")
    sessions, settings = app.state.sessions, app.state.settings
    accepting = mock_erp(lambda: httpx.Response(201, json={"erp_order_id": "ERP-9001"}))
    limited = mock_erp(lambda: httpx.Response(429, headers={"Retry-After": "30"}))
    worker_b = OutboundWorker(sessions, settings, http_client=accepting)
    due = worker_b._due_request_ids()  # B's poll, then A's whole attempt:
    assert OutboundWorker(sessions, settings, http_client=limited).run_once() == 1
    assert worker_b._claim(due[0]) is None


def test_a_claim_requires_the_request_to_still_be_pending(client, app):
    """Two workers race for one pending request:"""
    request_id = post_fixture(client, "02_low_stock.json").json()["replenishment_request"]["id"]
    sessions, settings = app.state.sessions, app.state.settings
    worker_a = OutboundWorker(sessions, settings, http_client=client)
    sent: list[str] = []

    def erp(request: httpx.Request) -> httpx.Response:
        sent.append(request.headers["Idempotency-Key"])
        return httpx.Response(201, json={"erp_order_id": "ERP-9001"})

    worker_b = OutboundWorker(
        sessions, settings, http_client=httpx.Client(transport=httpx.MockTransport(erp))
    )
    polled = worker_b._due_request_ids

    def poll_then_a_claims() -> list[uuid.UUID]:
        due = polled()  # B's poll sees the request pending...
        assert worker_a._claim(due[0]) is not None  # ...and A claims it before B's UPDATE
        return due

    worker_b._due_request_ids = poll_then_a_claims
    assert worker_b.run_once() == 0 and sent == []

    with sessions() as session:
        request = session.get_one(ReplenishmentRequest, uuid.UUID(request_id))
        attempts = session.scalars(select(OutboundAttempt)).all()
    assert (request.status, request.attempt_count) == ("in_flight", 1)
    assert [(a.attempt_no, a.classification) for a in attempts] == [(1, "in_progress")]


def test_mock_erp_scenario_header_and_control(client):
    headers = {
        "Idempotency-Key": f"replenishment:{uuid.uuid4()}",
        "X-Mock-Scenario": "rate_limited",
    }
    response = client.post("/mock-erp/orders", json={"quantity": 1}, headers=headers)
    assert response.status_code == 429 and response.headers["Retry-After"] == "2"

    headers["X-Mock-Scenario"] = "success"
    created = client.post("/mock-erp/orders", json={"quantity": 1}, headers=headers)
    repeated = client.post("/mock-erp/orders", json={"quantity": 1}, headers=headers)
    assert created.status_code == 201 and created.json()["duplicate"] is False
    assert repeated.status_code == 200 and repeated.json()["duplicate"] is True
    assert repeated.json()["erp_order_id"] == created.json()["erp_order_id"]

    bad = client.post("/mock-erp/_control", json={"scenarios": ["nope"]})
    assert bad.status_code == 422 and bad.json()["unknown"] == ["nope"]


@pytest.mark.parametrize(
    ("body", "why"),
    [
        pytest.param(
            b'{"sku": "\\ud800"}', "must not contain unpaired surrogates", id="lone-surrogate"
        ),
        pytest.param(
            b"[" * 300 + b"]" * 300,
            "must not nest more than 64 levels of objects and arrays",
            id="nested-300-levels",
        ),
    ],
)
def test_mock_erp_control_view_survives_any_order_body(client, body, why):
    """The control view lists the bodies the mock received."""
    headers = {"Idempotency-Key": "odd-body-1", "Content-Type": "application/json"}
    assert client.post("/mock-erp/orders", content=body, headers=headers).status_code == 201
    response = client.get("/mock-erp/_control")
    assert response.status_code == 200
    assert response.json()["received"][-1]["body"] == {"_unstorable": why}


def test_attempt_log_lines_carry_the_ids(client, app, json_logs):
    request_id = post_fixture(client, "02_low_stock.json").json()["replenishment_request"]["id"]
    deliver_due(app, client)
    finished = next(line for line in json_logs.lines if line["msg"] == "erp attempt finished")
    assert finished["replenishment_id"] == request_id and "request_id" not in finished
    assert (finished["correlation_id"], finished["event_id"]) == ("corr_5002", "evt_1002")
    assert (finished["attempt_no"], finished["classification"]) == (1, "success")


def test_a_result_that_cannot_be_recorded_is_logged_with_the_ids(client, app, json_logs):
    """The database is down when the result is to be written, so the fallback fails too."""
    request_id = post_fixture(client, "02_low_stock.json").json()["replenishment_request"]["id"]
    worker = OutboundWorker(app.state.sessions, app.state.settings, http_client=client)

    def database_down(*args, **kwargs):
        raise RuntimeError("database is down")

    worker._record = database_down
    assert worker.run_once() == 1  # nothing escapes to the poll loop
    errors = [line for line in json_logs.lines if line["level"] == "ERROR"]
    assert [line["msg"] for line in errors] == [
        "could not record the attempt; recording it without the response",
        "could not record the attempt; it is marked lost and resent when the claim expires",
    ]
    for line in errors:
        assert (line["replenishment_id"], line["correlation_id"]) == (request_id, "corr_5002")
        assert line["attempt_no"] == 1 and "database is down" in line["exc"]
    request = requests_now(client)[0]
    assert request["status"] == "in_flight"


def test_stop_during_a_held_call_returns_and_sends_nothing_new(client, app):
    """Shutdown while an ERP call is held open."""
    post_fixture(client, "02_low_stock.json")
    post_fixture(client, "08_concurrent_a.json")  # another item: a second due request
    erp = HeldErp(httpx.Response(201, json={"erp_order_id": "ERP-9001"}))
    worker = OutboundWorker(app.state.sessions, app.state.settings)
    worker._http.close()
    worker._http = erp.client  # still owned by the worker, so it must close it itself
    worker.start()
    assert erp.entered.wait(10)

    started = time.monotonic()
    worker.stop()
    assert time.monotonic() - started < app.state.settings.erp_timeout_seconds + 1.5
    assert worker.is_alive() and not erp.client.is_closed  # the held call is still running

    erp.release()
    worker._thread.join(10)
    assert not worker.is_alive() and erp.client.is_closed  # closed once nothing used it
    assert erp.calls == 1  # nothing new was sent after stop()
    requests = requests_now(client)
    assert sorted((r["status"], r["attempt_count"]) for r in requests) == [
        ("delivered", 1),  # the call in progress was still recorded
        ("pending", 0),
    ]


def test_shutdown_stops_the_worker_off_the_event_loop(database_url):
    """stop() can wait for an ERP call; the lifespan runs it in a thread, not on the loop."""
    app = create_app(make_settings(database_url, worker_enabled=True))
    worker, seen = app.state.worker, []
    stop = worker.stop

    def watched_stop() -> bool:
        try:
            asyncio.get_running_loop()
            seen.append("on the event loop")
        except RuntimeError:
            seen.append("off the event loop")
        return stop()

    worker.stop = watched_stop
    with TestClient(app):
        pass
    assert seen == ["off the event loop"] and not worker.is_alive()


def test_only_a_pass_that_completes_refreshes_the_heartbeat(client, app, tmp_path):
    """Readiness reads the worker's heartbeat."""
    engine = make_engine(f"sqlite:///{tmp_path / 'never-migrated.db'}")
    try:
        for sessions, moves in ((make_sessionmaker(engine), False), (app.state.sessions, True)):
            worker = OutboundWorker(sessions, app.state.settings, http_client=client)
            worker._heartbeat = before = time.monotonic() - 60
            assert worker._tick() is False  # nothing attempted: it failed, or nothing was due
            assert (worker._heartbeat > before) is moves
    finally:
        engine.dispose()


def test_ready_reports_a_worker_that_no_longer_completes_a_pass(database_url, monkeypatch):
    """The thread is alive but every polling pass fails (here a bug in run_once; an unmigrated or
    unreachable table does the same)."""
    app = create_app(make_settings(database_url, worker_enabled=True))
    worker = app.state.worker
    with TestClient(app) as client:
        assert client.get("/health/ready").json()["checks"]["worker"] == "ok"
        # 1.5 s instead of 5 s, but above the 0.5 s poll: a healthy worker never reads stalled
        monkeypatch.setattr(outbound, "STALL_SECONDS", 1.5, raising=False)
        healthy_until = time.monotonic() + 2.0
        while time.monotonic() < healthy_until:
            assert worker.status() == "ok"
            time.sleep(0.05)
        failing = threading.Event()

        def broken() -> int:
            failing.set()
            raise RuntimeError("every pass fails")

        worker.run_once = broken
        assert failing.wait(5)

        def worker_check():
            return client.get("/health/ready").json()["checks"]["worker"]

        wait_for(lambda: worker_check() == "stalled", timeout=5, what="worker: stalled")
        response = client.get("/health/ready")
        assert response.status_code == 503
        assert response.json()["checks"] == {
            "database": "ok",
            "migrations": "ok",
            "worker": "stalled",
        }
        del worker.run_once  # fixed: a pass completes again, and that refreshes the heartbeat
        wait_for(lambda: worker.status() == "ok", timeout=3, what="worker: ok")


def test_a_slow_erp_call_is_not_a_stall(client, app, monkeypatch):
    """An ERP call in progress counts as progress until the claim's lease runs out, so a single slow
    answer (a 5 s timeout is as long as STALL_SECONDS) does not flip readiness."""
    post_fixture(client, "02_low_stock.json")
    erp = HeldErp(httpx.Response(201, json={"erp_order_id": "ERP-9001"}))
    worker = OutboundWorker(app.state.sessions, app.state.settings, http_client=erp.client)
    monkeypatch.setattr(outbound, "STALL_SECONDS", 0.1, raising=False)
    worker.start()
    try:
        assert erp.entered.wait(10)
        time.sleep(0.3)  # no heartbeat for longer than STALL_SECONDS, but inside a call
        assert worker.status() == "ok"
        worker.lease_seconds = 0.2  # the same call has now outlived its lease
        assert worker.status() == "stalled"
    finally:
        erp.release()
        worker.stop()
    assert worker.status() == "stopped"


def test_unsent_event_payloads_do_not_reach_the_erp(client, app):
    response = post_event(client, fixture_bytes("07_invalid.json"))
    assert response.status_code == 422
    with app.state.sessions() as session:
        assert session.scalars(select(Observation)).all() == []
    assert deliver_due(app, client) == 0
    assert app.state.mock_erp.snapshot()["received"] == []
