"""Concurrency through a real uvicorn server:"""

import threading
from collections import Counter

import httpx
from sqlalchemy import func, select

from restock.outbound import OutboundWorker
from restock.schemas import InventoryState, Observation, ReplenishmentRequest

from .helpers import (
    EVENTS_URL,
    LiveServer,
    event_body,
    fixture_bytes,
    post_event,
    requests_now,
    sign_headers,
)


def fire_together(base_url: str, bodies: list[bytes]) -> list[httpx.Response]:
    """POST every body to the webhook, signed, all released together."""
    return post_together(base_url, [(EVENTS_URL, body) for body in bodies])


def post_together(base_url: str, calls: list[tuple[str, bytes]]) -> list[httpx.Response]:
    """POST each (path, body) from a thread of its own, released together by a barrier."""
    barrier = threading.Barrier(len(calls))
    responses: list[httpx.Response | None] = [None] * len(calls)

    def send(index: int) -> None:
        path, body = calls[index]
        with httpx.Client(base_url=base_url, timeout=60) as http:
            headers = sign_headers(body) if path == EVENTS_URL else {}
            barrier.wait()
            responses[index] = http.post(path, content=body, headers=headers)

    threads = [threading.Thread(target=send, args=(i,)) for i in range(len(calls))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert all(response is not None for response in responses)
    return responses  # type: ignore[return-value]


def assert_overlapped(live: LiveServer, sent: int) -> None:
    # I require half the batch inside the server at once, not all of it: on a loaded machine a
    peak = live.probe.peak
    assert peak >= sent // 2, f"only {peak} of {sent} requests were in the server at once"


def test_concurrent_08_and_09_with_redeliveries(live_server):
    live = live_server()
    bodies = (
        [fixture_bytes("08_concurrent_a.json")] * 6
        + [fixture_bytes("09_concurrent_b.json")] * 6
        + [fixture_bytes("03_duplicate_low_stock.json")] * 4
    )
    responses = fire_together(live.url, bodies)
    assert_overlapped(live, len(bodies))

    outcomes = Counter((r.json()["event_id"], r.status_code) for r in responses)
    assert outcomes == {
        ("evt_2001", 201): 1,
        ("evt_2001", 200): 5,
        ("evt_2002", 201): 1,
        ("evt_2002", 200): 5,
        ("evt_1002", 201): 1,
        ("evt_1002", 200): 3,
    }
    with live.app.state.sessions() as session:
        events = sorted(session.scalars(select(Observation.event_id)))
        assert events == ["evt_1002", "evt_2001", "evt_2002"]  # both unique events retained

        state = session.get(InventoryState, ("FAC-101", "ROOM-4B", "MED-40230"))
        assert state.last_event_id == "evt_2002"  # the later observation is current
        assert state.current_quantity == 2

        requests = session.scalars(
            select(ReplenishmentRequest).where(ReplenishmentRequest.room_id == "ROOM-4B")
        ).all()
        assert [r.requested_quantity for r in requests] == [8]  # exactly one request of 8

        decisions = dict(
            session.execute(
                select(Observation.event_id, Observation.decision).where(
                    Observation.room_id == "ROOM-4B"
                )
            ).all()
        )
        assert sorted(decisions.values()) in (
            ["blocked_not_armed", "request_created"],  # 08 applied first, then 09
            ["request_created", "stale"],  # 09 applied first, 08 arrived "late"
        )

        room_3a = session.scalars(
            select(ReplenishmentRequest).where(ReplenishmentRequest.room_id == "ROOM-3A")
        ).all()
        assert [r.requested_quantity for r in room_3a] == [8]


def test_twenty_parallel_redeliveries_store_one_row(live_server):
    live = live_server()
    responses = fire_together(live.url, [fixture_bytes("02_low_stock.json")] * 20)
    assert_overlapped(live, 20)

    assert Counter(r.status_code for r in responses) == {201: 1, 200: 19}
    assert {r.json()["replenishment_request"]["id"] for r in responses} == {
        next(r for r in responses if r.status_code == 201).json()["replenishment_request"]["id"]
    }
    with live.app.state.sessions() as session:
        assert session.scalars(select(Observation.event_id)).all() == ["evt_1002"]
        assert len(session.scalars(select(ReplenishmentRequest)).all()) == 1


def test_parallel_distinct_low_events_create_one_request(live_server):
    """Ten different low events for one item at once: one request, state = the newest."""
    live = live_server()
    template = fixture_bytes("02_low_stock.json").decode()
    bodies = [
        template.replace('"evt_1002"', f'"evt_3{n:03d}"')
        .replace("19:22:10Z", f"19:22:{10 + n:02d}Z")
        .encode()
        for n in range(10)
    ]
    responses = fire_together(live.url, bodies)
    assert_overlapped(live, len(bodies))

    assert [r.status_code for r in responses] == [201] * 10
    with live.app.state.sessions() as session:
        requests = session.scalars(select(ReplenishmentRequest)).all()
        assert len(requests) == 1 and requests[0].requested_quantity == 8
        state = session.get(InventoryState, ("FAC-101", "ROOM-3A", "MED-30224"))
        assert state.last_event_id == "evt_3009"


def test_parallel_events_for_an_existing_item_are_serialised(live_server):
    """The item exists before the burst, so nothing but the row lock (FOR UPDATE on PostgreSQL, the
    write lock on SQLite) orders the read-decide-write of each event."""
    live = live_server()
    with httpx.Client(base_url=live.url, timeout=30) as http:
        body = fixture_bytes("01_normal_stock.json")  # quantity 8 > reorder point 5: armed
        assert http.post(EVENTS_URL, content=body, headers=sign_headers(body)).status_code == 201

    template = fixture_bytes("02_low_stock.json").decode()
    bodies = [
        template.replace('"evt_1002"', f'"evt_4{n:03d}"')
        .replace("19:22:10Z", f"19:23:{n:02d}Z")
        .encode()
        for n in range(20)
    ]
    responses = fire_together(live.url, bodies)
    assert_overlapped(live, len(bodies))
    assert [r.status_code for r in responses] == [201] * 20

    with live.app.state.sessions() as session:
        state = session.get(InventoryState, ("FAC-101", "ROOM-3A", "MED-30224"))
        assert state.last_event_id == "evt_4019"  # newest observed_at, whatever the order
        requests = session.scalars(select(ReplenishmentRequest)).all()
        assert [r.requested_quantity for r in requests] == [8]
        decisions = Counter(
            session.scalars(select(Observation.decision).where(Observation.event_id.like("evt_4%")))
        )
    # Serialised, the first low event disarms the item, so the others are blocked_not_armed
    # (or stale); only racing transactions would ever reach the unique-index backstop.
    assert decisions["request_created"] == 1
    assert decisions["blocked_open_request"] == 0
    assert decisions["blocked_not_armed"] + decisions["stale"] == 19


def test_a_retry_racing_a_new_low_event_leaves_one_open_request(live_server):
    """For eight items whose request the ERP refused, a Retry click on that request and a new low
    event for the item are released together."""
    # No worker thread: only this test moves requests, so the race is all that happens.
    live = live_server(worker_enabled=False)
    skus = [f"MED-RACE-{n}" for n in range(8)]
    with httpx.Client(base_url=live.url, timeout=30) as http:
        http.post("/mock-erp/_control", json={"scenarios": ["permanent_rejection"] * len(skus)})
        for sku in skus:
            low = event_body(event_id=f"evt_{sku}_1", sku=sku, observed_at="2026-08-18T19:00:00Z")
            assert post_event(http, low).json()["decision"] == "request_created"
        worker = OutboundWorker(live.app.state.sessions, live.app.state.settings)
        try:
            assert worker.run_once() == len(skus)  # every order refused: 422, failed
        finally:
            worker.stop()
        for sku in skus:
            refilled = event_body(
                event_id=f"evt_{sku}_2",
                sku=sku,
                observed_at="2026-08-18T20:00:00Z",
                observed_quantity=12,
            )
            assert post_event(http, refilled).json()["decision"] == "recovered"
        # The race exists only because a failed request is not open.
        assert requests_now(live, "open") == []
        failed = http.get("/api/v1/outbound-attempts", params={"status": "failed"}).json()
        attempt_of = {attempt["request"]["sku"]: attempt["id"] for attempt in failed["items"]}
    assert sorted(attempt_of) == sorted(skus)

    calls = []
    for sku in skus:
        calls.append((f"/api/v1/outbound-attempts/{attempt_of[sku]}/retry", b""))
        low_again = event_body(event_id=f"evt_{sku}_3", sku=sku, observed_at="2026-08-18T21:00:00Z")
        calls.append((EVENTS_URL, low_again))
    responses = post_together(live.url, calls)
    assert_overlapped(live, len(calls))

    outcomes = []
    for n in range(len(skus)):
        retry, event = responses[2 * n], responses[2 * n + 1]
        assert event.status_code == 201
        if retry.status_code == 409:
            assert retry.json()["error"] == "superseded"
        outcomes.append((retry.status_code, event.json()["decision"]))
    assert set(outcomes) <= {(202, "blocked_open_request"), (409, "request_created")}

    with live.app.state.sessions() as session:
        open_per_item = dict(
            session.execute(
                select(ReplenishmentRequest.sku, func.count())
                .where(ReplenishmentRequest.status.in_(["pending", "in_flight"]))
                .group_by(ReplenishmentRequest.sku)
            ).all()
        )
    assert open_per_item == {sku: 1 for sku in skus}
