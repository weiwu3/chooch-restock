"""The FIXTURE_GUIDE processing sequence, step by step, plus the eligibility edge cases."""

import uuid

import pytest
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError

from restock import inbound
from restock.runtime import utcnow
from restock.schemas import Observation, ReplenishmentRequest

from .helpers import count_rows, deliver_due, event_body, post_event, post_fixture, requests_now

HISTORY = "/api/v1/items/MED-30224/history"


def requests_for(app, room_id="ROOM-3A"):
    with app.state.sessions() as session:
        stmt = (
            select(ReplenishmentRequest)
            .where(ReplenishmentRequest.room_id == room_id)
            .order_by(ReplenishmentRequest.created_at)
        )
        return session.scalars(stmt).all()


def test_guide_sequence_01_to_06(client, app):
    # Step 1: normal stock -> state stored, no request.
    body = post_fixture(client, "01_normal_stock.json").json()
    assert (body["status"], body["applied"], body["decision"]) == ("accepted", True, "none")
    assert body["current"]["current_quantity"] == 8 and body["current"]["status"] == "ok"
    assert body["replenishment_request"] is None

    # Step 2: low stock -> quantity 4 and one request for 12 - 4 = 8 units.
    response = post_fixture(client, "02_low_stock.json")
    assert response.status_code == 201
    body = response.json()
    assert body["decision"] == "request_created"
    assert body["current"]["current_quantity"] == 4 and body["current"]["status"] == "low"
    first = body["replenishment_request"]
    assert first["quantity"] == 8 and first["status"] == "pending"

    # Step 3: redelivery of step 2 -> duplicate, same request, nothing new stored.
    response = post_fixture(client, "03_duplicate_low_stock.json")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "duplicate" and body["decision"] == "request_created"
    assert body["replenishment_request"]["id"] == first["id"]
    assert count_rows(app, Observation) == 2
    assert len(requests_for(app)) == 1

    # Step 4: older observed_at -> kept in history, state and requests unchanged.
    response = post_fixture(client, "04_out_of_order.json")
    assert response.status_code == 201
    body = response.json()
    assert (body["applied"], body["decision"]) == (False, "stale")
    assert body["current"]["last_event_id"] == "evt_1002"
    assert body["current"]["current_quantity"] == 4
    history = client.get(HISTORY, params={"room_id": "ROOM-3A"}).json()["items"]
    assert [row["event_id"] for row in history] == ["evt_1002", "evt_1001", "evt_0999"]
    assert len(requests_for(app)) == 1

    # Step 5: restock -> quantity 12, recovery recorded: the decision says so (it is stored on
    # the observation) and the item is re-armed.
    body = post_fixture(client, "05_restocked.json").json()
    assert (body["applied"], body["decision"]) == (True, "recovered")
    assert body["current"]["current_quantity"] == 12
    assert body["current"]["status"] == "ok" and body["current"]["armed"] is True
    newest = client.get(HISTORY, params={"room_id": "ROOM-3A"}).json()["items"][0]
    assert (newest["event_id"], newest["decision"]) == ("evt_1003", "recovered")

    # The worker delivers the first request (synchronously here).
    assert deliver_due(app, client) == 1
    assert requests_for(app)[0].status == "delivered"

    # Step 6: new depletion after delivery + recovery -> a new cycle, 12 - 3 = 9 units.
    body = post_fixture(client, "06_new_depletion.json").json()
    assert body["decision"] == "request_created"
    assert body["current"]["current_quantity"] == 3 and body["current"]["armed"] is False
    second = body["replenishment_request"]
    assert second["quantity"] == 9 and second["id"] != first["id"]
    assert [r.status for r in requests_for(app)] == ["delivered", "pending"]
    # The values used for each decision are kept on the request.
    stored = requests_for(app)[1]
    assert (stored.observed_quantity, stored.par_level, stored.reorder_point) == (3, 12, 5)
    assert stored.triggering_event_id == "evt_1004"


def test_depletion_while_the_first_request_is_open_is_blocked(client, app):
    for name in ("01_normal_stock.json", "02_low_stock.json", "05_restocked.json"):
        post_fixture(client, name)

    body = post_fixture(client, "06_new_depletion.json").json()
    assert (body["applied"], body["decision"]) == (True, "blocked_open_request")
    assert body["current"]["current_quantity"] == 3
    assert body["current"]["armed"] is True  # the recovery is not used up by a blocked event
    assert body["replenishment_request"] is None
    inventory = client.get("/api/v1/rooms/ROOM-3A/inventory").json()["items"][0]
    assert inventory["open_request"]["requested_quantity"] == 8

    # Once the first request is delivered, the next newer low event starts the new cycle.
    assert deliver_due(app, client) == 1
    later = event_body(event_id="evt_1005", observed_at="2026-08-18T22:00:00Z", observed_quantity=2)
    body = post_event(client, later).json()
    assert body["decision"] == "request_created"
    assert body["replenishment_request"]["quantity"] == 10


def test_a_failed_request_does_not_block_a_new_cycle(client, app):
    """A failed request is final for the one-open-request rule."""
    client.post("/mock-erp/_control", json={"scenarios": ["permanent_rejection"]})
    post_fixture(client, "02_low_stock.json")
    assert deliver_due(app, client) == 1
    assert [r.status for r in requests_for(app)] == ["failed"]

    lower = event_body(
        event_id="evt_1002b", observed_at="2026-08-18T19:30:00Z", observed_quantity=2
    )
    assert post_event(client, lower).json()["decision"] == "blocked_not_armed"
    assert post_fixture(client, "05_restocked.json").json()["decision"] == "recovered"
    body = post_fixture(client, "06_new_depletion.json").json()
    assert body["decision"] == "request_created"
    assert body["replenishment_request"]["quantity"] == 9

    requests = [(r.triggering_event_id, r.status) for r in requests_for(app)]
    assert requests == [("evt_1002", "failed"), ("evt_1004", "pending")]
    open_now = {"items": requests_now(client, "open")}
    assert [r["triggering_event_id"] for r in open_now["items"]] == ["evt_1004"]
    inventory = client.get("/api/v1/rooms/ROOM-3A/inventory").json()["items"][0]
    assert inventory["open_request"]["triggering_event_id"] == "evt_1004"


def test_low_again_without_recovery_is_not_armed(client, app):
    post_fixture(client, "02_low_stock.json")
    assert deliver_due(app, client) == 1
    lower = event_body(
        event_id="evt_1002b", observed_at="2026-08-18T19:30:00Z", observed_quantity=1
    )
    body = post_event(client, lower).json()
    assert (body["applied"], body["decision"]) == (True, "blocked_not_armed")
    assert body["current"]["current_quantity"] == 1
    assert len(requests_for(app)) == 1


def test_late_recovery_does_not_rearm(client, app):
    # 02 creates a request; 06 (21:00, low) arrives; then 05 (20:00, recovery) arrives late.
    post_fixture(client, "02_low_stock.json")
    deliver_due(app, client)
    assert post_fixture(client, "06_new_depletion.json").json()["decision"] == "blocked_not_armed"
    body = post_fixture(client, "05_restocked.json").json()
    assert (body["applied"], body["decision"]) == (False, "stale")
    assert body["current"]["armed"] is False and body["current"]["current_quantity"] == 3
    assert len(requests_for(app)) == 1


def test_each_event_thresholds_decide_for_that_event(client, app):
    """The brief:"""
    post_fixture(client, "01_normal_stock.json")  # 8 units, par 12, reorder at 5
    raised = event_body(
        event_id="evt_t1",
        observed_at="2026-08-18T19:30:00Z",
        observed_quantity=8,
        par_level=20,
        reorder_point=10,
    )
    body = post_event(client, raised).json()
    assert body["decision"] == "request_created"
    assert body["replenishment_request"]["quantity"] == 12  # 20 - 8, with this event's par
    current = client.get("/api/v1/rooms/ROOM-3A/inventory").json()["items"][0]
    assert (current["status"], current["par_level"], current["reorder_point"]) == ("low", 20, 10)
    stored = requests_for(app)[0]  # the values the decision used
    assert (stored.observed_quantity, stored.par_level, stored.reorder_point) == (8, 20, 10)


def test_a_lowered_reorder_point_creates_no_request(client, app):
    """The inverse: 4 units is low at reorder point 5 but not at 3, so no ERP order."""
    post_fixture(client, "01_normal_stock.json")
    lowered = event_body(
        event_id="evt_t2", observed_at="2026-08-18T19:30:00Z", observed_quantity=4, reorder_point=3
    )
    body = post_event(client, lowered).json()
    assert (body["decision"], body["replenishment_request"]) == ("none", None)
    current = client.get("/api/v1/rooms/ROOM-3A/inventory").json()["items"][0]
    assert (current["status"], current["current_quantity"], current["reorder_point"]) == (
        "ok",
        4,
        3,
    )
    assert requests_for(app) == []


def test_database_allows_one_open_request_per_item(app):
    """The partial unique index from the migration, checked directly in the database:"""
    now = utcnow()

    def request_row(event_id: str, status: str) -> ReplenishmentRequest:
        request_id = uuid.uuid4()
        return ReplenishmentRequest(
            id=request_id,
            facility_id="FAC-1",
            room_id="R-1",
            sku="SKU-1",
            triggering_event_id=event_id,
            requested_quantity=5,
            observed_quantity=1,
            par_level=6,
            reorder_point=2,
            status=status,
            idempotency_key=f"replenishment:{request_id}",
            attempt_count=0,
            created_at=now,
            updated_at=now,
        )

    with app.state.sessions.begin() as session:
        for n in range(6):
            session.add(
                Observation(
                    event_id=f"e{n}",
                    facility_id="FAC-1",
                    room_id="R-1",
                    sku="SKU-1",
                    observed_at=now,
                    observed_quantity=1,
                    par_level=6,
                    reorder_point=2,
                    confidence=0.9,
                    received_at=now,
                    correlation_id="c",
                    applied=True,
                    decision="none",
                )
            )
        session.add_all([request_row("e0", "delivered"), request_row("e1", "delivered")])
        session.add_all([request_row("e2", "failed"), request_row("e3", "failed")])
        session.add(request_row("e4", "pending"))  # the item's one open request

    for status in ("pending", "in_flight"):
        with pytest.raises(IntegrityError), app.state.sessions.begin() as session:
            session.add(request_row("e5", status))
    reopen = (
        update(ReplenishmentRequest)
        .where(ReplenishmentRequest.triggering_event_id == "e2")
        .values(status="pending")
    )
    with pytest.raises(IntegrityError), app.state.sessions.begin() as session:
        session.execute(reopen)


def test_unique_index_backstop_records_blocked_open_request(client, app, monkeypatch):
    """Under the item lock the explicit open-request check cannot miss, so the partial unique index
    never fires in normal runs."""
    for name in ("01_normal_stock.json", "02_low_stock.json", "05_restocked.json"):
        post_fixture(client, name)  # one request open, the item re-armed
    real_check = inbound._open_request
    looks = []

    def miss_the_first_look(session, event):
        looks.append(event.event_id)
        return None if len(looks) == 1 else real_check(session, event)

    monkeypatch.setattr(inbound, "_open_request", miss_the_first_look)
    response = post_fixture(client, "06_new_depletion.json")

    assert response.status_code == 201
    body = response.json()
    assert (body["applied"], body["decision"]) == (True, "blocked_open_request")
    assert body["current"]["armed"] is True and body["replenishment_request"] is None
    assert looks == ["evt_1004", "evt_1004"]  # the pre-check, then the re-read after the index
    requests = [(r.triggering_event_id, r.requested_quantity) for r in requests_for(app)]
    assert requests == [("evt_1002", 8)]
    with app.state.sessions() as session:
        stored = session.scalars(select(Observation).where(Observation.event_id == "evt_1004"))
        assert [(o.applied, o.decision) for o in stored] == [(True, "blocked_open_request")]


def test_redelivery_with_a_different_payload_keeps_the_stored_event(client, app, json_logs):
    post_fixture(client, "02_low_stock.json")
    changed = event_body(observed_quantity=0)  # same event_id evt_1002, different reading
    response = post_event(client, changed)
    assert response.status_code == 200 and response.json()["status"] == "duplicate"
    assert response.json()["current"]["current_quantity"] == 4
    assert any("different payload" in line["msg"] for line in json_logs.lines)
    assert count_rows(app, Observation) == 1
