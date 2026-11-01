"""Read API, health/readiness, correlation ids and JSON logs, schema-vs-migration drift."""

import base64
import json
import os
import socket
import sqlite3
import threading
import time
from typing import Any

import anyio
import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from fastapi.testclient import TestClient
from sqlalchemy import CheckConstraint, Connection, event, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError

from restock import api, runtime
from restock.main import create_app
from restock.runtime import alembic_config, make_engine
from restock.schemas import Base, Observation

from .helpers import (
    deliver_due,
    event_body,
    make_settings,
    post_event,
    post_fixture,
    requests_now,
    sign_headers,
)

HISTORY = "/api/v1/items/MED-30224/history"
# Same rule as the database_url fixture: TEST_DATABASE_URL set means the suite runs on Postgres.
postgres_only = pytest.mark.skipif(
    not os.environ.get("TEST_DATABASE_URL"), reason="PostgreSQL only (make test-pg)"
)


def test_history_is_newest_first_and_paginates_without_gaps(client):
    for name in (
        "01_normal_stock.json",
        "02_low_stock.json",
        "04_out_of_order.json",
        "05_restocked.json",
        "06_new_depletion.json",
    ):
        post_fixture(client, name)

    pages, cursor = [], None
    while True:
        params = {"room_id": "ROOM-3A", "limit": 2, **({"cursor": cursor} if cursor else {})}
        body = client.get(HISTORY, params=params).json()
        pages.append([row["event_id"] for row in body["items"]])
        cursor = body["next_cursor"]
        if cursor is None:
            break
    assert pages == [["evt_1004", "evt_1003"], ["evt_1002", "evt_1001"], ["evt_0999"]]

    stale = client.get(HISTORY, params={"room_id": "ROOM-3A"}).json()["items"][-1]
    assert (stale["event_id"], stale["applied"], stale["decision"]) == ("evt_0999", False, "stale")


def test_history_tie_break_and_cursor_on_equal_timestamps(client):
    same = "2026-08-18T19:30:00Z"
    for event_id in ("evt_a", "evt_c", "evt_b"):
        post_event(client, event_body(event_id=event_id, observed_at=same, observed_quantity=9))

    seen, cursor = [], None
    for _ in range(3):
        params = {"room_id": "ROOM-3A", "limit": 1, **({"cursor": cursor} if cursor else {})}
        body = client.get(HISTORY, params=params).json()
        seen += [row["event_id"] for row in body["items"]]
        cursor = body["next_cursor"]
    assert seen == ["evt_c", "evt_b", "evt_a"] and cursor is None
    inventory = client.get("/api/v1/rooms/ROOM-3A/inventory").json()["items"][0]
    assert inventory["last_event_id"] == "evt_c"  # larger event_id wins the tie


def test_history_order_matches_the_tie_break_for_mixed_case_ids(client):
    """The rule compares event ids as plain strings ("EVT_B" < "evt_a")."""
    same = "2026-08-18T19:30:00Z"
    for event_id in ("EVT_B", "evt_a"):
        post_event(client, event_body(event_id=event_id, observed_at=same, observed_quantity=9))
    items = client.get(HISTORY, params={"room_id": "ROOM-3A"}).json()["items"]
    assert [row["event_id"] for row in items] == ["evt_a", "EVT_B"]
    current = client.get("/api/v1/rooms/ROOM-3A/inventory").json()["items"][0]
    assert current["last_event_id"] == "evt_a" == items[0]["event_id"]


def test_history_errors(client):
    bad_cursor = client.get(HISTORY, params={"room_id": "ROOM-3A", "cursor": "garbage"})
    assert bad_cursor.status_code == 400 and bad_cursor.json()["error"] == "invalid_cursor"
    no_room = client.get(HISTORY)
    assert no_room.status_code == 422
    assert no_room.json()["errors"][0]["field"] == "query.room_id"


def cursor_for(observed_at: str, event_id: str) -> str:
    raw = json.dumps([observed_at, event_id]).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


@pytest.mark.parametrize(
    ("method", "path", "params", "status"),
    [
        ("GET", HISTORY, {"cursor": cursor_for("0001-01-01T00:00:00+01:00", "e")}, 400),
        ("GET", HISTORY, {"cursor": cursor_for("9999-12-31T23:59:59-01:00", "e")}, 400),
        ("GET", HISTORY, {"cursor": cursor_for("2026-08-18T19:22:10+00:00", "e\u0000")}, 400),
        ("POST", "/api/v1/outbound-attempts/9223372036854775808/retry", {}, 404),
        ("GET", "/api/v1/rooms/ROOM%00X/inventory", {}, 422),
        ("GET", "/api/v1/rooms/ROOM-3A/inventory", {"facility_id": "FAC\u0000"}, 422),
        ("GET", "/api/v1/items/MED%00X/history", {}, 422),
        ("GET", HISTORY, {"room_id": "ROOM\u00003A"}, 422),
    ],
    ids=[
        "cursor-year-1",
        "cursor-year-9999",
        "cursor-nul",
        "attempt-id-beyond-bigint",
        "nul-in-room-path",
        "nul-in-facility-query",
        "nul-in-sku-path",
        "nul-in-room-query",
    ],
)
def test_malformed_read_parameters_are_client_errors(client, method, path, params, status):
    """These used to be unhandled errors (500) on both databases or on PostgreSQL only."""
    post_fixture(client, "01_normal_stock.json")
    params = {"room_id": "ROOM-3A", **params} if "history" in path else params
    assert client.request(method, path, params=params).status_code == status


def test_room_inventory_shape_and_filters(client):
    post_fixture(client, "01_normal_stock.json")
    post_fixture(client, "02_low_stock.json")

    body = client.get("/api/v1/rooms/ROOM-3A/inventory").json()
    assert body["room_id"] == "ROOM-3A" and len(body["items"]) == 1
    item = body["items"][0]
    assert {
        k: item[k]
        for k in (
            "facility_id",
            "sku",
            "current_quantity",
            "par_level",
            "reorder_point",
            "status",
            "armed",
            "last_event_id",
        )
    } == {
        "facility_id": "FAC-101",
        "sku": "MED-30224",
        "current_quantity": 4,
        "par_level": 12,
        "reorder_point": 5,
        "status": "low",
        "armed": False,
        "last_event_id": "evt_1002",
    }
    assert item["last_observed_at"] == "2026-08-18T19:22:10Z"
    assert item["last_observation"]["confidence"] == 0.94
    assert item["last_observation"]["camera_id"] == "CAM-3A-07"
    assert item["open_request"]["requested_quantity"] == 8
    assert item["open_request"]["idempotency_key"].startswith("replenishment:")

    other = client.get("/api/v1/rooms/ROOM-3A/inventory", params={"facility_id": "FAC-999"})
    assert other.json()["items"] == []
    assert client.get("/api/v1/rooms/ROOM-404/inventory").json()["items"] == []


def test_failed_attempts_list_has_diagnostic_context(client, app):
    client.post("/mock-erp/_control", json={"scenarios": ["permanent_rejection"]})
    post_fixture(client, "02_low_stock.json")
    deliver_due(app, client)
    post_fixture(client, "08_concurrent_a.json")  # a second request that delivers fine
    deliver_due(app, client)

    failed = client.get("/api/v1/outbound-attempts", params={"status": "failed"}).json()
    assert failed["status"] == "failed" and len(failed["items"]) == 1
    attempt = failed["items"][0]
    assert attempt["request"]["sku"] == "MED-30224"
    assert attempt["http_status"] == 422 and attempt["classification"] == "non_retryable"
    assert attempt["error"] == "HTTP 422: unknown_sku"
    assert attempt["response_body"]["message"].startswith("The ERP does not recognize")
    assert attempt["correlation_id"] == "corr_5002"
    assert attempt["duration_ms"] >= 0 and attempt["triggered_by"] == "worker"

    everything = client.get("/api/v1/outbound-attempts", params={"status": "all"}).json()
    assert sorted(a["classification"] for a in everything["items"]) == ["non_retryable", "success"]
    # Nothing newer exists for its item, so it is not superseded; and failed is not open: it
    # no longer blocks a new request for the item.
    assert attempt["request"]["superseded_by"] is None
    requests = {"items": requests_now(client, "open")}
    assert requests["items"] == []


def test_health_live_and_ready(client):
    assert client.get("/health/live").json() == {"status": "ok"}
    ready = client.get("/health/ready")
    assert ready.status_code == 200
    assert ready.json() == {
        "status": "ready",
        "checks": {"database": "ok", "migrations": "ok", "worker": "disabled"},
    }


def test_ready_is_503_until_the_schema_is_migrated(tmp_path, json_logs):
    url = f"sqlite:///{tmp_path / 'empty.db'}"
    with TestClient(create_app(make_settings(url))) as client:
        response = client.get("/health/ready")
        assert response.status_code == 503
        assert response.json()["checks"]["migrations"] == "at [], head is ['0001']"
        command.upgrade(alembic_config(url), "head")
        assert client.get("/health/ready").status_code == 200
    failed = [line for line in json_logs.lines if line["msg"] == "readiness check failed"]
    assert [(line["check"], line["error"]) for line in failed] == [
        ("migrations", "at [], head is ['0001']")
    ]


def test_live_answers_while_every_worker_thread_is_busy(client):
    """Liveness needs neither the database nor a worker thread, so it answers even when every thread
    serving the database endpoints is stuck (on a database that does not answer, say)."""
    limiter = client.portal.call(anyio.to_thread.current_default_thread_limiter)
    client.portal.call(setattr, limiter, "total_tokens", 1)
    holder = object()
    client.portal.call(limiter.acquire_on_behalf_of, holder)
    answered: list[int] = []
    probe = threading.Thread(target=lambda: answered.append(client.get("/health/live").status_code))
    try:
        probe.start()
        probe.join(timeout=5)
        assert answered == [200]
    finally:
        client.portal.call(limiter.release_on_behalf_of, holder)
        probe.join(timeout=5)


def test_unreachable_database_bounds_startup_and_readiness(monkeypatch, json_logs):
    """A PostgreSQL port that accepts nothing and answers nothing."""
    monkeypatch.setattr(runtime, "CONNECT_TIMEOUT_SECONDS", 2)
    outcome: dict[str, Any] = {}
    with socket.create_server(("127.0.0.1", 0)) as silent:
        url = f"postgresql+psycopg://restock@127.0.0.1:{silent.getsockname()[1]}/restock"
        app = create_app(make_settings(url))

        def start_and_probe() -> None:
            with TestClient(app) as client:
                started = time.monotonic()
                outcome["ready"] = client.get("/health/ready")
                outcome["seconds"] = time.monotonic() - started

        thread = threading.Thread(target=start_and_probe, daemon=True)
        thread.start()
        thread.join(timeout=20)
    assert "ready" in outcome, "startup and one readiness probe took more than 20 s"
    assert outcome["ready"].status_code == 503 and outcome["seconds"] < 5
    assert outcome["ready"].json()["checks"]["database"] == "error: OperationalError"
    messages = [line["msg"] for line in json_logs.lines]
    assert "cannot read the schema version" in messages
    assert not [m for m in messages if "not at head" in m]  # unreachable, not unmigrated
    causes = [line["error"] for line in json_logs.lines if line["msg"] == "readiness check failed"]
    assert [cause for cause in causes if "connection timeout expired" in cause]


def test_ready_answers_503_when_the_database_does_not_answer_in_time(tmp_path, monkeypatch):
    """Whatever holds the database up (here another connection's exclusive SQLite lock; on
    PostgreSQL, a stale connection to a host that went away), readiness answers 503 after
    READY_TIMEOUT_SECONDS instead of waiting with it."""
    monkeypatch.setattr(api, "READY_TIMEOUT_SECONDS", 0.5)
    path = tmp_path / "busy.db"
    url = f"sqlite:///{path}"
    command.upgrade(alembic_config(url), "head")
    outcome: dict[str, Any] = {}
    with TestClient(create_app(make_settings(url))) as client:

        def probe() -> None:
            started = time.monotonic()
            outcome["ready"] = client.get("/health/ready")
            outcome["seconds"] = time.monotonic() - started

        holder = sqlite3.connect(path, isolation_level=None)
        holder.execute("BEGIN EXCLUSIVE")  # no other connection may even read
        thread = threading.Thread(target=probe, daemon=True)
        try:
            thread.start()
            thread.join(timeout=10)
        finally:
            holder.execute("ROLLBACK")
            holder.close()
            thread.join(timeout=70)
    assert outcome["ready"].status_code == 503 and outcome["seconds"] < 2
    assert outcome["ready"].json()["checks"]["database"] == "error: no answer within 0.5s"


def test_sqlite_readiness_does_not_wait_for_the_write_lock(tmp_path):
    """On SQLite every write transaction starts with BEGIN IMMEDIATE, the single write lock."""
    path = tmp_path / "writer.db"
    url = f"sqlite:///{path}"
    command.upgrade(alembic_config(url), "head")
    with TestClient(create_app(make_settings(url))) as client:
        writer = sqlite3.connect(path, isolation_level=None)
        writer.execute("BEGIN IMMEDIATE")
        try:
            started = time.monotonic()
            response = client.get("/health/ready")
            seconds = time.monotonic() - started
        finally:
            writer.execute("ROLLBACK")
            writer.close()
    assert response.status_code == 200 and seconds < 1


def test_ready_is_503_when_the_worker_has_stopped(database_url):
    app = create_app(make_settings(database_url, worker_enabled=True))
    with TestClient(app) as client:
        assert client.get("/health/ready").json()["checks"]["worker"] == "ok"
        app.state.worker.stop()
        response = client.get("/health/ready")
        assert response.status_code == 503
        assert response.json()["checks"]["worker"] == "stopped"


@pytest.mark.parametrize("secret", [None, "", "change-me"])
def test_missing_or_placeholder_webhook_secret_refuses_to_start(database_url, secret):
    """change-me is the published .env.example placeholder: anyone could sign with it."""
    with pytest.raises(RuntimeError, match="WEBHOOK_SECRET"):
        create_app(make_settings(database_url, webhook_secret=secret))


def test_mock_erp_is_not_mounted_when_switched_off(database_url):
    """ERP_MOCK_ENABLED=false: nobody can reprogram or call the mock where ERP_URL is real."""
    app = create_app(make_settings(database_url, erp_mock_enabled=False))
    with TestClient(app) as client:
        assert client.post("/mock-erp/orders", json={}).status_code == 404
        assert client.get("/mock-erp/_control").status_code == 404
        assert client.post("/mock-erp/_control", json={"scenarios": []}).status_code == 404
        assert not [path for path in client.get("/openapi.json").json()["paths"] if "mock" in path]
        assert client.get("/health/live").status_code == 200


def test_request_id_header_wins_and_is_echoed(client, app):
    body = event_body()
    response = client.post(
        "/api/v1/inventory-events",
        content=body,
        headers={**sign_headers(body), "X-Request-ID": "req-42"},
    )
    assert response.status_code == 201
    assert response.headers["X-Request-ID"] == "req-42"
    with app.state.sessions() as session:
        assert session.query(Observation).one().correlation_id == "req-42"


def test_unexpected_error_keeps_the_correlation_id(tmp_path, json_logs):
    """A 500 carries the caller's id like every other response:"""
    app = create_app(make_settings(f"sqlite:///{tmp_path / 'never-migrated.db'}"))
    body = event_body()
    headers = {**sign_headers(body), "X-Request-ID": "trace-500"}
    with TestClient(app) as client:
        response = client.post("/api/v1/inventory-events", content=body, headers=headers)
    assert response.status_code == 500
    assert response.json()["error"] == "internal_error"
    assert response.json()["correlation_id"] == response.headers["X-Request-ID"] == "trace-500"
    logged = [line for line in json_logs.lines if line["msg"] == "unhandled error"]
    assert [(line.get("correlation_id"), line.get("event_id")) for line in logged] == [
        ("trace-500", "evt_1002")
    ]
    assert "OperationalError" in logged[0]["exc"]


def test_event_correlation_id_is_used_without_a_header(client):
    response = post_fixture(client, "02_low_stock.json")
    assert response.headers["X-Request-ID"] == "corr_5002"
    plain = client.get("/health/live")
    assert len(plain.headers["X-Request-ID"]) == 36  # generated uuid


def test_logs_are_json_lines_with_correlation_fields(client, json_logs):
    request_id = post_fixture(client, "02_low_stock.json").json()["replenishment_request"]["id"]
    accepted = next(line for line in json_logs.lines if line["msg"] == "inventory event accepted")
    assert accepted["correlation_id"] == "corr_5002"
    assert accepted["event_id"] == "evt_1002"
    assert accepted["replenishment_id"] == request_id
    assert "request_id" not in accepted  # X-Request-ID is the correlation id, not this
    assert accepted["decision"] == "request_created"
    access = next(line for line in json_logs.lines if line["msg"] == "http request")
    assert (access["correlation_id"], access["status"]) == ("corr_5002", 201)
    assert {"ts", "level", "logger"} <= set(accepted)


def test_models_match_the_migration(database_url):
    engine = make_engine(database_url)
    with engine.connect() as conn:
        diff = compare_metadata(MigrationContext.configure(conn), Base.metadata)
    engine.dispose()
    assert diff == []


@postgres_only
def test_postgres_runs_every_transaction_at_read_committed(database_url):
    """The inbound path needs READ COMMITTED (see make_engine)."""
    url = make_url(database_url).update_query_dict(
        {"options": "-c default_transaction_isolation=repeatable\\ read"}
    )
    engine = make_engine(url.render_as_string(hide_password=False))
    try:
        with engine.connect() as conn:
            default = conn.exec_driver_sql("SHOW default_transaction_isolation").scalar()
            used = conn.exec_driver_sql("SHOW transaction_isolation").scalar()
    finally:
        engine.dispose()
    assert (default, used) == ("repeatable read", "read committed")


def test_room_only_history_reads_an_index_in_order(client, app):
    """The brief's form of the history query (room_id, no facility_id) seeks
    ix_observations_room_sku_order and reads it in order:"""
    post_fixture(client, "02_low_stock.json")
    queries: list[tuple[str, Any]] = []

    def capture(conn, cursor, statement, parameters, context, executemany) -> None:
        if "FROM observations" in statement and "ORDER BY" in statement:
            queries.append((statement, parameters))

    event.listen(app.state.engine, "before_cursor_execute", capture)
    try:
        assert client.get(HISTORY, params={"room_id": "ROOM-3A"}).status_code == 200
    finally:
        event.remove(app.state.engine, "before_cursor_execute", capture)
    [(statement, parameters)] = queries
    with app.state.engine.begin() as conn:
        if conn.dialect.name == "postgresql":
            conn.exec_driver_sql("SET LOCAL enable_seqscan = off")  # else a tiny table is scanned
            # A bitmap scan of the index plus a sort can also win on a tiny table: it did when
            conn.exec_driver_sql("SET LOCAL enable_bitmapscan = off")
            plan = conn.exec_driver_sql(f"EXPLAIN {statement}", parameters).scalars().all()
        else:
            rows = conn.exec_driver_sql(f"EXPLAIN QUERY PLAN {statement}", parameters)
            plan = [row[-1] for row in rows]
    shown = "\n".join(plan)
    assert "ix_observations_room_sku_order" in shown, shown
    assert "TEMP B-TREE" not in shown and "Sort" not in shown, shown


def test_the_newer_request_lookup_reads_an_index(client, app):
    """The failed list looks for each failed request's newer one (superseded_by), and the page,
    which refreshes every 1.5 seconds, reads that list each time."""
    client.post("/mock-erp/_control", json={"scenarios": ["permanent_rejection"]})
    post_fixture(client, "02_low_stock.json")
    assert deliver_due(app, client) == 1
    queries: list[tuple[str, Any]] = []

    def capture(conn, cursor, statement, parameters, context, executemany) -> None:
        if "FROM outbound_attempts" in statement:
            queries.append((statement, parameters))

    event.listen(app.state.engine, "before_cursor_execute", capture)
    try:
        params = {"status": "failed"}
        assert client.get("/api/v1/outbound-attempts", params=params).status_code == 200
    finally:
        event.remove(app.state.engine, "before_cursor_execute", capture)
    [(statement, parameters)] = queries
    with app.state.engine.begin() as conn:
        if conn.dialect.name == "postgresql":
            conn.exec_driver_sql("SET LOCAL enable_seqscan = off")  # else a tiny table is scanned
            plan = conn.exec_driver_sql(f"EXPLAIN {statement}", parameters).scalars().all()
        else:
            rows = conn.exec_driver_sql(f"EXPLAIN QUERY PLAN {statement}", parameters)
            plan = [row[-1] for row in rows]
    shown = "\n".join(plan)
    assert "ix_replenishment_requests_facility_id_room_id_sku_created_at" in shown, shown


def test_check_constraints_match_the_models(database_url):
    """compare_metadata ignores CHECK constraints, so I compare them here:"""
    engine = make_engine(database_url)
    try:
        inspector = inspect(engine)
        for table in Base.metadata.sorted_tables:
            expected = {
                c.name: str(c.sqltext) for c in table.constraints if isinstance(c, CheckConstraint)
            }
            found = {c["name"]: c["sqltext"] for c in inspector.get_check_constraints(table.name)}
            if engine.dialect.name == "sqlite":
                assert found == expected, table.name
            else:
                assert set(found) == set(expected), table.name
    finally:
        engine.dispose()


@postgres_only
def test_postgres_schema_matches_the_models(database_url):
    """What PostgreSQL holds for the migrated tables (CHECK definitions from pg_constraint, index
    definitions with their WHERE clauses from pg_indexes, column collations) must equal what it
    holds for the models created in a scratch schema."""
    engine = make_engine(database_url)
    try:
        with engine.connect() as conn:
            schema = conn.execute(text("SELECT current_schema()")).scalar()
            migrated = _postgres_schema(conn, schema)
            conn.execute(text("CREATE SCHEMA models_copy"))
            copy = conn.execution_options(schema_translate_map={None: "models_copy"})
            Base.metadata.create_all(copy)
            from_models = _postgres_schema(conn, "models_copy")
            conn.rollback()
    finally:
        engine.dispose()
    assert migrated == from_models
    open_item = migrated["indexes"]["replenishment_requests.uq_replenishment_requests_open_item"]
    assert open_item.endswith(
        "WHERE ((status)::text = ANY ((ARRAY['pending'::character varying, "
        "'in_flight'::character varying])::text[]))"
    )
    assert migrated["collations"] == {
        "inventory_state.last_event_id": "C",
        "observations.event_id": "C",
        "replenishment_requests.triggering_event_id": "C",
    }


def _postgres_schema(conn: Connection, schema: str) -> dict[str, dict[str, str]]:
    checks = conn.execute(
        text(
            "SELECT rel.relname || '.' || con.conname, pg_get_constraintdef(con.oid) "
            "FROM pg_constraint con JOIN pg_class rel ON rel.oid = con.conrelid "
            "JOIN pg_namespace ns ON ns.oid = rel.relnamespace "
            "WHERE con.contype = 'c' AND ns.nspname = :schema"
        ),
        {"schema": schema},
    )
    indexes = conn.execute(
        text(
            "SELECT tablename || '.' || indexname, replace(indexdef, :qualified, ' ON ') "
            "FROM pg_indexes WHERE schemaname = :schema AND tablename <> 'alembic_version'"
        ),
        {"schema": schema, "qualified": f" ON {schema}."},
    )
    collations = conn.execute(
        text(
            "SELECT table_name || '.' || column_name, collation_name "
            "FROM information_schema.columns "
            "WHERE table_schema = :schema AND collation_name IS NOT NULL"
        ),
        {"schema": schema},
    )
    return {
        "checks": dict(checks.all()),
        "indexes": dict(indexes.all()),
        "collations": dict(collations.all()),
    }


@pytest.mark.parametrize(
    ("table", "values"),
    [
        ("inventory_state", {"current_quantity": -1}),
        ("inventory_state", {"reorder_point": 13}),  # above par_level 12
        ("inventory_state", {"par_level": -1, "reorder_point": -1}),
        ("replenishment_requests", {"observed_quantity": -1}),
        ("replenishment_requests", {"reorder_point": 13}),
        ("replenishment_requests", {"par_level": -1, "reorder_point": -1}),
        ("replenishment_requests", {"attempt_count": -1}),
        ("replenishment_requests", {"status": "cancelled"}),
        ("observations", {"confidence": 2}),
    ],
)
def test_database_refuses_impossible_rows(client, app, table, values):
    """The CHECK constraints refuse what the application must never write, even by a bug."""
    post_fixture(client, "02_low_stock.json")  # one state row and one request (par 12, point 5)
    assignments = ", ".join(f"{column} = :{column}" for column in values)
    with pytest.raises(IntegrityError), app.state.sessions.begin() as session:
        session.execute(text(f"UPDATE {table} SET {assignments}"), values)
