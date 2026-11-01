"""Input validation:"""

import base64
import json
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from restock.main import create_app
from restock.schemas import InventoryState, Observation, OutboundAttempt, ReplenishmentRequest

from .helpers import EVENTS_URL, count_rows, event_body, make_settings, post_event, post_fixture

HISTORY = "/api/v1/items/MED-30224/history"


def test_invalid_fixture_lists_every_problem_and_persists_nothing(client, app):
    response = post_fixture(client, "07_invalid.json")

    assert response.status_code == 422
    body = response.json()
    assert body["error"] == "validation_failed"
    errors = {error["field"]: error for error in body["errors"]}
    assert set(errors) == {
        "room_id",
        "observed_at",
        "observed_quantity",
        "reorder_point",
        "confidence",
    }
    assert errors["room_id"]["type"] == "missing"
    assert errors["observed_at"]["input"] == "not-a-timestamp"
    assert errors["observed_quantity"]["input"] == -3
    assert "must not exceed par_level (5)" in errors["reorder_point"]["message"]
    assert errors["confidence"]["input"] == 1.4
    for model in (Observation, InventoryState, ReplenishmentRequest, OutboundAttempt):
        assert count_rows(app, model) == 0
    assert app.state.mock_erp.snapshot()["received"] == []  # no outbound work either


@pytest.mark.parametrize(
    ("field", "value", "error_type"),
    [
        ("observed_at", "2026-08-18T19:22:10", "value_error"),  # no offset
        ("observed_at", "2026-08-18 19:22:10Z", "value_error"),  # not RFC 3339 'T'
        ("observed_at", 1755544930, "value_error"),  # unix seconds are not the contract
        ("observed_at", "2026-02-30T10:00:00Z", "value_error"),  # no such date
        ("observed_quantity", "4", "int_type"),
        ("observed_quantity", 4.5, "int_type"),
        ("observed_quantity", True, "int_type"),
        ("par_level", -1, "greater_than_equal"),
        ("confidence", -0.01, "greater_than_equal"),
        ("confidence", "0.9", "float_type"),
        ("event_id", "", "string_too_short"),
        ("sku", 30224, "string_type"),
        ("metadata", "not-an-object", "dict_type"),
    ],
)
def test_field_rules(client, app, field, value, error_type):
    response = post_event(client, event_body(**{field: value}))
    assert response.status_code == 422
    assert [(e["field"], e["type"]) for e in response.json()["errors"]] == [(field, error_type)]
    assert count_rows(app, Observation) == 0


@pytest.mark.parametrize(
    ("raw", "error_type"),
    [(b"[]", "object_type"), (b'{"confidence": NaN}', "json_invalid"), (b"", "json_invalid")],
)
def test_body_must_be_a_json_object(client, raw, error_type):
    response = post_event(client, raw)
    assert response.status_code == 422
    assert response.json()["errors"][0]["type"] == error_type


def with_raw(old: bytes, new: bytes) -> bytes:
    """02_low_stock.json with one literal swapped (for numbers json.dumps cannot write)."""
    body = event_body()
    assert old in body
    return body.replace(old, new)


@pytest.mark.parametrize(
    ("body", "field"),
    [
        pytest.param(
            event_body(observed_at="0001-01-01T00:00:00+01:00"), "observed_at", id="year-1"
        ),
        pytest.param(
            event_body(observed_at="0001-01-01T00:00:00Z"), "observed_at", id="year-1-utc"
        ),
        pytest.param(
            event_body(observed_at="9999-12-31T23:59:59-01:00"), "observed_at", id="year-9999"
        ),
        pytest.param(with_raw(b"0.94", b"1e309"), "confidence", id="confidence-overflows-to-inf"),
        pytest.param(
            with_raw(b'"observed_quantity": 4', b'"observed_quantity": 1e309'),
            "observed_quantity",
            id="quantity-overflows-to-inf",
        ),
        pytest.param(event_body(event_id="\ud800"), "event_id", id="unpaired-surrogate-in-id"),
        pytest.param(  # 30,000 levels: past the parser's limit, under the body size limit
            b'{"metadata": ' + b"[" * 30_000 + b"]" * 30_000 + b"}",
            "(body)",
            id="nested-too-deeply-to-parse",
        ),
        pytest.param(event_body(event_id="evt_\u0000"), "event_id", id="nul-in-event-id"),
        pytest.param(event_body(room_id="ROOM\u00003A"), "room_id", id="nul-in-room-id"),
        pytest.param(event_body(camera_id="CAM\u0000"), "camera_id", id="nul-in-camera-id"),
        pytest.param(event_body(metadata={"note": "a\u0000b"}), "metadata", id="nul-in-metadata"),
        pytest.param(
            event_body(metadata={"no\u0000te": "b"}), "metadata", id="nul-in-metadata-key"
        ),
        pytest.param(
            event_body(metadata={"note": "\ud800"}), "metadata", id="surrogate-in-metadata"
        ),
        pytest.param(with_raw(b'"snap_8841"', b"1e309"), "metadata", id="infinite-in-metadata"),
        pytest.param(event_body(firmware="4.1\u0000"), "(body)", id="nul-in-unknown-field"),
    ],
)
def test_values_no_database_can_store_are_rejected_up_front(client, app, body, field):
    """Each of these used to reach an unhandled error (HTTP 500, inviting gateway retries) on both
    databases, or only on PostgreSQL, which cannot store NUL, unpaired surrogates or infinities
    that SQLite accepts."""
    response = post_event(client, body)
    assert response.status_code == 422
    assert [e["field"] for e in response.json()["errors"]] == [field]
    assert count_rows(app, Observation) == 0


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(
            with_raw(
                b'"observed_quantity": 4', b'"observed_quantity": 100, "observed_quantity": 2'
            ),
            id="top-level",
        ),
        pytest.param(
            with_raw(b'"metadata": {', b'"metadata": {"note": "a", "note": "b", '),
            id="in-metadata",
        ),
        pytest.param(
            with_raw(b'"metadata": {', b'"metadata": {"\\ud800": 1, "\\ud800": 2, '),
            id="unpaired-surrogate-key",
        ),
    ],
)
def test_duplicate_keys_are_rejected(client, app, body):
    """json.loads keeps the last of two values for one key, so a body with two different
    observed_quantity values was accepted with whichever came last."""
    response = post_event(client, body)
    assert response.status_code == 422
    errors = response.json()["errors"]
    assert [(e["field"], e["type"]) for e in errors] == [("(body)", "json_invalid")]
    assert "duplicate key" in errors[0]["message"]
    assert count_rows(app, Observation) == 0


def nested(depth: int) -> bytes:
    """A JSON value of `depth` nested arrays."""
    return b"[" * depth + b"]" * depth


@pytest.mark.parametrize(
    ("body", "field"),
    [
        pytest.param(
            with_raw(b'"metadata": {', b'"metadata": {"deep": ' + nested(64) + b", "),
            "metadata",
            id="metadata-65-levels",
        ),
        pytest.param(
            event_body()[:-1] + b', "deep": ' + nested(65) + b"}",
            "(body)",
            id="unknown-field-65-levels",
        ),
        pytest.param(
            event_body()[:-1] + b', "deep": ' + nested(5_000) + b"}",
            "(body)",
            id="unknown-field-5000-levels",
        ),
    ],
)
def test_metadata_and_unknown_fields_nest_at_most_64_levels(client, app, body, field):
    """Deeper JSON parsed, but near the parser's limit it failed to encode at insert (500) or was
    stored and then failed to decode on every read of the item (500 until deleted)."""
    response = post_event(client, body)
    assert response.status_code == 422
    errors = response.json()["errors"]
    assert [e["field"] for e in errors] == [field]
    assert "64 levels" in errors[0]["message"]
    assert count_rows(app, Observation) == 0


def test_metadata_and_unknown_fields_at_64_levels_are_accepted(client, app):
    body = with_raw(b'"metadata": {', b'"metadata": {"deep": ' + nested(63) + b", ")
    body = body[:-1] + b', "deep": ' + nested(64) + b"}"
    assert post_event(client, body).status_code == 201
    history = client.get(HISTORY, params={"room_id": "ROOM-3A"})
    assert history.status_code == 200 and len(history.json()["items"]) == 1


OUT_OF_YEARS = "must fall in the years 1970-9998 in UTC"


@pytest.mark.parametrize(
    ("observed_at", "status", "message"),
    [
        ("1970-01-01T00:30:00+01:00", 422, OUT_OF_YEARS),  # 1969-12-31T23:30Z
        ("1969-12-31T23:30:00-01:00", 201, None),  # 1970-01-01T00:30Z
        ("9998-12-31T23:30:00-02:00", 422, OUT_OF_YEARS),  # 9999-01-01T01:30Z
        # 9998-12-31T23:30Z passes the year check, then is refused as far in the future.
        ("9999-01-01T00:30:00+01:00", 422, "s in the future; at most 300 s is accepted"),
    ],
)
def test_observed_at_year_range_applies_in_utc(client, observed_at, status, message):
    response = post_event(client, event_body(observed_at=observed_at))
    assert response.status_code == status, response.text
    if message is not None:
        [error] = response.json()["errors"]
        assert error["field"] == "observed_at" and message in error["message"]


def test_every_accepted_observed_at_pages_through_history(client):
    """The webhook used to check the year in the sender's offset and the cursor in UTC, so a row
    stored at 1970-01-01T00:30+01:00 handed out a cursor (1969Z) that page 2 refused."""
    stamps = [
        "1970-01-01T00:30:00+01:00",
        "1970-01-01T00:20:00+01:00",
        "1969-12-31T23:30:00-01:00",
        "1970-01-01T00:00:00Z",
        "9998-12-31T23:59:59Z",
        "9999-01-01T00:30:00+01:00",
        "9998-12-31T23:30:00-02:00",
        "9998-12-31T23:20:00-02:00",
    ]
    accepted = []
    for n, stamp in enumerate(stamps):
        event_id = f"evt_edge_{n}"
        body = event_body(event_id=event_id, observed_at=stamp, observed_quantity=9)
        if post_event(client, body).status_code == 201:
            accepted.append(event_id)
    seen, cursor = [], None
    for _ in range(len(stamps) + 1):
        params = {"room_id": "ROOM-3A", "limit": 1, **({"cursor": cursor} if cursor else {})}
        page = client.get(HISTORY, params=params)
        assert page.status_code == 200, page.text
        seen += [row["event_id"] for row in page.json()["items"]]
        cursor = page.json()["next_cursor"]
        if cursor is None:
            break
    assert sorted(seen) == sorted(accepted) == ["evt_edge_2", "evt_edge_3"]


def seconds_from_now(seconds: float) -> str:
    """An observed_at this many seconds after now, in RFC 3339 with an offset."""
    return (datetime.now(UTC) + timedelta(seconds=seconds)).isoformat()


def test_observed_at_more_than_300_s_ahead_is_refused(client, app):
    """observed_at may be at most OBSERVED_AT_MAX_FUTURE_SECONDS (300 by default) ahead of server
    time."""
    just_inside = event_body(event_id="evt_future_299", observed_at=seconds_from_now(299))
    assert post_event(client, just_inside).status_code == 201

    too_far = event_body(event_id="evt_future_301", observed_at=seconds_from_now(301))
    response = post_event(client, too_far)
    assert response.status_code == 422
    [error] = response.json()["errors"]
    assert (error["field"], error["type"], error["message"]) == (
        "observed_at",
        "value_error",
        "observed_at is 301 s in the future; at most 300 s is accepted",
    )
    with app.state.sessions() as session:
        stored = session.query(Observation.event_id).all()
    assert [row.event_id for row in stored] == ["evt_future_299"]


def test_a_future_observed_at_is_listed_with_the_other_field_errors(client, app):
    body = event_body(observed_at=seconds_from_now(3600), observed_quantity=-1, confidence=2)
    response = post_event(client, body)
    assert response.status_code == 422
    errors = {error["field"]: error["message"] for error in response.json()["errors"]}
    assert set(errors) == {"observed_at", "observed_quantity", "confidence"}
    assert errors["observed_at"].endswith("s in the future; at most 300 s is accepted")
    assert count_rows(app, Observation) == 0


def test_the_future_limit_is_a_setting(database_url):
    """OBSERVED_AT_MAX_FUTURE_SECONDS sets the limit, and the message names it."""
    app = create_app(make_settings(database_url, observed_at_max_future_seconds=60))
    with TestClient(app) as client:
        inside = event_body(event_id="evt_future_59", observed_at=seconds_from_now(59))
        assert post_event(client, inside).status_code == 201
        outside = event_body(event_id="evt_future_61", observed_at=seconds_from_now(61))
        response = post_event(client, outside)
    assert response.status_code == 422
    assert response.json()["errors"][0]["message"] == (
        "observed_at is 61 s in the future; at most 60 s is accepted"
    )


def cursor_for(observed_at: str, event_id: str) -> str:
    raw = json.dumps([observed_at, event_id]).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


@pytest.mark.parametrize(
    "cursor",
    [
        pytest.param(cursor_for("2026-08-18T19:22:10+00:00", "e\ud800"), id="surrogate-event-id"),
        pytest.param(cursor_for("1970-01-01T00:30:00+01:00", "e"), id="1969-in-utc"),
        pytest.param(cursor_for("9998-12-31T23:30:00-02:00", "e"), id="9999-in-utc"),
    ],
)
def test_cursor_outside_what_the_api_returns_is_400(client, cursor):
    post_fixture(client, "02_low_stock.json")
    response = client.get(HISTORY, params={"room_id": "ROOM-3A", "cursor": cursor})
    assert response.status_code == 400 and response.json()["error"] == "invalid_cursor"


def test_reorder_point_equal_to_par_and_integer_confidence_are_valid(client):
    response = post_event(client, event_body(par_level=5, reorder_point=5, confidence=1))
    assert response.status_code == 201


def test_offsets_are_normalised_to_utc(client):
    response = post_event(client, event_body(observed_at="2026-08-18T21:22:10.250+02:00"))
    assert response.status_code == 201
    assert response.json()["current"]["last_observed_at"] == "2026-08-18T19:22:10.250000Z"


def test_unknown_fields_are_accepted_and_kept_in_metadata(client, app):
    response = post_event(client, event_body(firmware="4.1.0"))
    assert response.status_code == 201
    with app.state.sessions() as session:
        observation = session.query(Observation).one()
    assert observation.meta == {
        "snapshot_id": "snap_8841",
        "correlation_id": "corr_5002",
        "_extra": {"firmware": "4.1.0"},
    }


def test_unknown_fields_never_overwrite_the_gateways_own_metadata(client, app):
    """Unknown fields go under "_extra", or under the first of "__extra", "___extra", ..."""
    metadata = {"_extra": "gateway-value", "__extra": {"kept": True}}
    response = post_event(client, event_body(metadata=metadata, firmware="4.1"))
    assert response.status_code == 201
    with app.state.sessions() as session:
        observation = session.query(Observation).one()
    assert observation.meta == {
        "_extra": "gateway-value",
        "__extra": {"kept": True},
        "___extra": {"firmware": "4.1"},
    }


def test_valid_fixtures_are_accepted_byte_for_byte(client):
    for name in ("01_normal_stock.json", "08_concurrent_a.json", "09_concurrent_b.json"):
        response = post_fixture(client, name)
        assert response.status_code == 201, (name, json.dumps(response.json()))


@pytest.mark.parametrize(
    ("method", "path", "status", "error", "allow"),
    [
        ("GET", "/api/v1/nope", 404, "not_found", None),
        ("GET", "/api/v1/rooms/ROOM%2F3A/inventory", 404, "not_found", None),  # "/" in an id
        ("GET", EVENTS_URL, 405, "method_not_allowed", "POST"),
        ("POST", "/api/v1/rooms/ROOM-3A/inventory", 405, "method_not_allowed", "GET"),
    ],
)
def test_routing_errors_use_the_error_envelope(client, method, path, status, error, allow):
    """Unknown paths and wrong methods answered FastAPI's {"detail":"""
    response = client.request(method, path)
    assert response.status_code == status
    body = response.json()
    assert set(body) == {"error", "message"} and body["error"] == error
    assert response.headers.get("allow") == allow


def test_openapi_documents_the_error_envelope(client):
    """FastAPI advertised its own {"detail":"""
    spec = client.get("/openapi.json").json()

    def model(path: str, method: str, status: str) -> str:
        response = spec["paths"][path][method]["responses"][status]
        return response["content"]["application/json"]["schema"]["$ref"].rsplit("/", 1)[-1]

    expected = {
        (EVENTS_URL, "post"): {"401": "ErrorOut", "413": "ErrorOut", "422": "ValidationErrorOut"},
        ("/api/v1/rooms/{room_id}/inventory", "get"): {"422": "ValidationErrorOut"},
        ("/api/v1/items/{sku}/history", "get"): {"400": "ErrorOut", "422": "ValidationErrorOut"},
        ("/api/v1/outbound-attempts", "get"): {"422": "ValidationErrorOut"},
        ("/api/v1/outbound-attempts/{attempt_id}/retry", "post"): {
            "404": "ErrorOut",
            "409": "ErrorOut",
            "422": "ValidationErrorOut",
        },
        ("/health/ready", "get"): {"200": "ReadyOut", "503": "ReadyOut"},
    }
    for (path, method), statuses in expected.items():
        for status, name in statuses.items():
            assert model(path, method, status) == name, (method, path, status)
    for path, operations in spec["paths"].items():
        if not path.startswith("/mock-erp"):  # the test double keeps FastAPI's defaults
            assert "HTTPValidationError" not in json.dumps(operations), path
