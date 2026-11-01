"""The HTTP API: webhook authentication, the webhook, the read API, manual retry and health.

    POST /api/v1/inventory-events                       receive_inventory_event
    GET  /api/v1/rooms/{room_id}/inventory              room_inventory
    GET  /api/v1/items/{sku}/history                    item_history
    GET  /api/v1/outbound-attempts                      outbound_attempts
    POST /api/v1/outbound-attempts/{attempt_id}/retry   retry_attempt
    GET  /health/live, /health/ready                    live, ready

Every error answers {"error": code, "message": text}; a 422 adds "errors", one per problem.
The page (page.py) reuses inventory_items, list_attempts, list_requests and newer_request_id.
"""

import asyncio
import base64
import binascii
import hashlib
import hmac
import json
import logging
import time
import uuid
from contextlib import aclosing
from datetime import UTC, datetime
from typing import Annotated, Any, Literal, NoReturn

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import JSONResponse
from pydantic import AfterValidator, ValidationError
from sqlalchemy import ColumnElement, Row, ScalarSelect, and_, case, or_, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, aliased

from restock import runtime
from restock.inbound import ingest
from restock.runtime import read_only_connection, schema_revisions, utcnow
from restock.schemas import (
    OBSERVED_YEARS,
    OPEN_STATUSES,
    AttemptListOut,
    AttemptOut,
    ErrorOut,
    EventResponse,
    HistoryOut,
    InventoryEvent,
    InventoryItemOut,
    InventoryOut,
    InventoryState,
    Observation,
    ObservationOut,
    ObservedAtLimit,
    OutboundAttempt,
    ReadyOut,
    ReplenishmentRequest,
    RequestOut,
    RetryOut,
    StateOut,
    ValidationErrorOut,
    format_errors,
    is_ascii_digits,
    is_utf8,
    no_nul,
)

log = logging.getLogger("restock.api")
router = APIRouter()

# Unix seconds as plain digits; 12 digits reach the year 33658, so a longer value is a mistake
# (milliseconds, padding). Bounded before int() sees it: int() raises past 4300 digits.
TIMESTAMP_MAX_DIGITS = 12
# Readiness waits for the database this long: a little above the connect timeout, so an
# unreachable database is reported with the driver's own error.
READY_TIMEOUT_SECONDS = 6.0


# --- errors -----------------------------------------------------------------------------------


class ApiError(Exception):
    """Raised from a route; main.py answers it with the error envelope."""

    def __init__(self, status_code: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message


def error_body(code: str, message: str, **extra: Any) -> dict[str, Any]:
    return {"error": code, "message": message, **extra}


def validation_error_response(errors: list[dict[str, Any]]) -> JSONResponse:
    body = error_body("validation_failed", "request is invalid", errors=errors)
    return JSONResponse(body, status_code=422)


# Declared per route for /openapi.json, so FastAPI does not advertise its own 422 body.
ERROR_RESPONSES: dict[int, dict[str, Any]] = {
    400: {"model": ErrorOut, "description": "Malformed parameter (invalid_cursor)"},
    401: {"model": ErrorOut, "description": "Webhook authentication failed"},
    404: {"model": ErrorOut, "description": "No such resource"},
    409: {"model": ErrorOut, "description": "A newer request exists for the item (superseded)"},
    413: {"model": ErrorOut, "description": "Body larger than WEBHOOK_MAX_BODY_BYTES"},
    422: {"model": ValidationErrorOut, "description": "Invalid input; every problem is listed"},
    503: {"model": ReadyOut, "description": "Not ready; the checks name what failed"},
}


def error_responses(*codes: int) -> dict[int | str, dict[str, Any]]:
    return {code: ERROR_RESPONSES[code] for code in codes}


# --- webhook authentication ------------------------------------------------------------------
# X-Webhook-Signature: v1=<hex HMAC-SHA256 over "<X-Webhook-Timestamp>.<raw body>">, keyed with
# WEBHOOK_SECRET; the timestamp within WEBHOOK_REPLAY_WINDOW_SECONDS of server time. A replay
# inside the window is harmless: its event_id is stored already.


class AuthError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def check_headers(timestamp: str | None, signature: str | None) -> tuple[str, str]:
    """The checks that need no body, so the route runs them before reading any of it.
    Returns the timestamp text and the digest after "v1="."""
    if not timestamp or not signature:
        raise AuthError(
            "missing_signature", "X-Webhook-Timestamp and X-Webhook-Signature are required"
        )
    # Taken as received, no strip(): the HMAC covers that text.
    if not is_ascii_digits(timestamp) or len(timestamp) > TIMESTAMP_MAX_DIGITS:
        raise AuthError(
            "invalid_timestamp",
            f"X-Webhook-Timestamp must be unix seconds: 1 to {TIMESTAMP_MAX_DIGITS} digits, "
            "nothing else (no spaces)",
        )
    scheme, _, digest = signature.strip().partition("=")
    if scheme != "v1" or not digest:
        raise AuthError("invalid_signature", "X-Webhook-Signature must look like v1=<hex>")
    return timestamp, digest


def sign(secret: bytes, timestamp: str, body: bytes) -> str:
    return hmac.new(secret, timestamp.encode("ascii") + b"." + body, hashlib.sha256).hexdigest()


def verify(
    *,
    timestamp: str | None,
    signature: str | None,
    body: bytes,
    secret: bytes,
    window_seconds: int,
    now: float,
) -> None:
    """The whole check; raises AuthError with the code of the first failed check."""
    timestamp, digest = check_headers(timestamp, signature)
    # Signature before age, so a forged request is told invalid_signature and only a genuine
    # late one stale_timestamp. compare_digest: constant time; bytes, since a header can carry
    # non-ASCII that compare_digest refuses in a str.
    received = digest.lower().encode("utf-8", "replace")
    if not hmac.compare_digest(sign(secret, timestamp, body).encode(), received):
        raise AuthError("invalid_signature", "signature does not match the request body")
    skew = int(now) - int(timestamp)
    if abs(skew) > window_seconds:
        raise AuthError(
            "stale_timestamp",
            f"timestamp is {skew} s from server time; the replay window is {window_seconds} s",
        )


# --- the webhook -------------------------------------------------------------------------------


async def verified_body(request: Request) -> bytes:
    """Authenticate the raw body before anything parses it: headers first, then at most
    WEBHOOK_MAX_BODY_BYTES are read (413 beyond), then the HMAC and the age."""
    settings = request.app.state.settings
    limit = settings.webhook_max_body_bytes
    timestamp = request.headers.get("X-Webhook-Timestamp")
    signature = request.headers.get("X-Webhook-Signature")
    try:
        check_headers(timestamp, signature)
        declared = request.headers.get("Content-Length", "")
        if is_ascii_digits(declared) and (len(declared) > 18 or int(declared) > limit):
            raise ApiError(413, "body_too_large", f"request body exceeds {limit} bytes")
        chunks: list[bytes] = []
        size = 0
        async with aclosing(request.stream()) as stream:  # a chunked upload declares no length
            async for chunk in stream:
                size += len(chunk)
                if size > limit:
                    raise ApiError(413, "body_too_large", f"request body exceeds {limit} bytes")
                chunks.append(chunk)
        body = b"".join(chunks)
        verify(
            timestamp=timestamp,
            signature=signature,
            body=body,
            secret=settings.webhook_secret.get_secret_value().encode(),
            window_seconds=settings.webhook_replay_window_seconds,
            now=time.time(),
        )
    except AuthError as exc:
        log.warning("webhook authentication failed", extra={"reason": exc.code})
        raise ApiError(401, exc.code, exc.message) from None
    except ApiError as exc:
        log.warning("webhook body over the size limit", extra={"limit": limit})
        raise exc
    return body


def _reject_constant(name: str) -> NoReturn:
    raise ValueError(f"{name} is not valid JSON")  # NaN, Infinity: not JSON


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    # json.loads keeps the last duplicate key; I-JSON (RFC 7493) forbids duplicates.
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate key {key[:100]!r}")
        result[key] = value
    return result


def _rejected(errors: list[dict[str, Any]]) -> JSONResponse:
    log.warning("inventory event rejected", extra={"errors": [e["field"] for e in errors]})
    return validation_error_response(errors)


@router.post(
    "/api/v1/inventory-events",
    status_code=201,
    response_model=EventResponse,
    responses={
        200: {"model": EventResponse, "description": "Redelivery of a stored event_id"},
        **error_responses(401, 413, 422),
    },
    tags=["inbound"],
)
def receive_inventory_event(
    request: Request, body: Annotated[bytes, Depends(verified_body)]
) -> JSONResponse:
    # 201 for a new event, 200 for a redelivery, 422 (nothing stored) for a bad body.
    try:
        payload = json.loads(body, parse_constant=_reject_constant, object_pairs_hook=_unique_keys)
    except ValueError as exc:
        return _rejected(
            [
                {
                    "field": "(body)",
                    "type": "json_invalid",
                    "message": f"body is not valid JSON: {exc}",
                }
            ]
        )
    except RecursionError:
        return _rejected(
            [{"field": "(body)", "type": "json_invalid", "message": "body is nested too deeply"}]
        )
    if not isinstance(payload, dict):
        return _rejected(
            [{"field": "(body)", "type": "object_type", "message": "body must be a JSON object"}]
        )
    if isinstance(payload.get("event_id"), str):
        runtime.update(event_id=payload["event_id"][:128])  # traceable even when rejected

    settings = request.app.state.settings
    limit = ObservedAtLimit(
        now=utcnow(), max_future_seconds=settings.observed_at_max_future_seconds
    )
    try:
        event = InventoryEvent.model_validate(payload, context=limit)
    except ValidationError as exc:
        return _rejected(format_errors(exc.errors()))

    # The gateway's own correlation id is used only when the caller sent no usable X-Request-ID.
    if not request.state.correlation_from_header and event.correlation_hint() is not None:
        runtime.update(correlation_id=event.correlation_hint())
    result = ingest(request.app.state.sessions, event, runtime.current()["correlation_id"])
    status_code = 200 if result.duplicate else 201
    return JSONResponse(result.response.model_dump(mode="json"), status_code=status_code)


# --- the read API ----------------------------------------------------------------------------

# A NUL in a path or query id would fail the query on PostgreSQL: a 422 like any bad parameter.
IdParam = Annotated[str, AfterValidator(no_nul)]
OptionalIdParam = Annotated[str | None, AfterValidator(no_nul)]
AttemptStatusFilter = Literal["failed", "all"]
RequestStatusFilter = Literal["open", "pending", "in_flight", "delivered", "failed", "all"]


@router.get(
    "/api/v1/rooms/{room_id}/inventory",
    response_model=InventoryOut,
    responses=error_responses(422),
    tags=["read"],
)
def room_inventory(
    room_id: IdParam, request: Request, facility_id: OptionalIdParam = None
) -> InventoryOut:
    with request.app.state.sessions() as session:
        items = inventory_items(session, room_id=room_id, facility_id=facility_id)
    return InventoryOut(room_id=room_id, facility_id=facility_id, items=items)


def inventory_items(
    session: Session, room_id: str | None = None, facility_id: str | None = None
) -> list[InventoryItemOut]:
    """Every item's state with its last observation and its open request, if any. The outer
    join never repeats an item: the partial unique index allows one open request per item."""
    stmt = (
        select(InventoryState, Observation, ReplenishmentRequest)
        .join(Observation, Observation.event_id == InventoryState.last_event_id)
        .outerjoin(
            ReplenishmentRequest,
            and_(
                ReplenishmentRequest.facility_id == InventoryState.facility_id,
                ReplenishmentRequest.room_id == InventoryState.room_id,
                ReplenishmentRequest.sku == InventoryState.sku,
                ReplenishmentRequest.status.in_(OPEN_STATUSES),
            ),
        )
        .order_by(InventoryState.facility_id, InventoryState.room_id, InventoryState.sku)
    )
    if room_id is not None:
        stmt = stmt.where(InventoryState.room_id == room_id)
    if facility_id is not None:
        stmt = stmt.where(InventoryState.facility_id == facility_id)
    return [
        InventoryItemOut(
            **StateOut.model_validate(state).model_dump(),
            last_observation=ObservationOut.model_validate(observation),
            open_request=RequestOut.model_validate(open_request) if open_request else None,
        )
        for state, observation, open_request in session.execute(stmt)
    ]


@router.get(
    "/api/v1/items/{sku}/history",
    response_model=HistoryOut,
    responses=error_responses(400, 422),
    tags=["read"],
)
def item_history(
    sku: IdParam,
    request: Request,
    room_id: IdParam,
    facility_id: OptionalIdParam = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    cursor: str | None = None,
) -> HistoryOut:
    """Newest first by (observed_at desc, event_id desc), the key that orders state. The cursor
    is that key of the last row returned, so pages never skip or repeat rows."""
    stmt = select(Observation).where(Observation.sku == sku, Observation.room_id == room_id)
    if facility_id is not None:
        stmt = stmt.where(Observation.facility_id == facility_id)
    if cursor:
        # The cursor is the key as a JSON array in unpadded URL-safe base64. Anything this
        # API could not have returned is a 400, never a 500.
        try:
            raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
            observed_at_text, event_id = json.loads(raw)
            observed_at = datetime.fromisoformat(observed_at_text)
            if observed_at.tzinfo is None:
                raise ValueError("no UTC offset")
            observed_at = observed_at.astimezone(UTC)
            if observed_at.year not in OBSERVED_YEARS:
                raise ValueError("outside OBSERVED_YEARS")
            if not isinstance(event_id, str) or "\x00" in event_id or not is_utf8(event_id):
                raise ValueError("not a stored event_id")
        except (binascii.Error, ValueError, TypeError, OverflowError, RecursionError) as exc:
            raise ApiError(400, "invalid_cursor", "cursor is not one this API returned") from exc
        stmt = stmt.where(
            or_(
                Observation.observed_at < observed_at,
                and_(Observation.observed_at == observed_at, Observation.event_id < event_id),
            )
        )
    stmt = stmt.order_by(Observation.observed_at.desc(), Observation.event_id.desc())
    with request.app.state.sessions() as session:
        rows = session.scalars(stmt.limit(limit + 1)).all()  # one extra: is there a next page?
    page = rows[:limit]
    next_cursor = None
    if len(rows) > limit:
        key = json.dumps([page[-1].observed_at.isoformat(), page[-1].event_id]).encode()
        next_cursor = base64.urlsafe_b64encode(key).decode().rstrip("=")
    return HistoryOut(
        sku=sku,
        room_id=room_id,
        facility_id=facility_id,
        items=[ObservationOut.model_validate(row) for row in page],
        next_cursor=next_cursor,
    )


def newer_request_id() -> ScalarSelect[uuid.UUID]:
    """SQL for the first request for the same item created after the enclosing statement's
    request, or NULL. A failed request with a newer one is superseded: retrying it would order
    the bin twice."""
    newer = aliased(ReplenishmentRequest)
    return (
        select(newer.id)
        .where(
            newer.facility_id == ReplenishmentRequest.facility_id,
            newer.room_id == ReplenishmentRequest.room_id,
            newer.sku == ReplenishmentRequest.sku,
            newer.created_at > ReplenishmentRequest.created_at,
        )
        .order_by(newer.created_at, newer.id)
        .limit(1)
        .scalar_subquery()
    )


def superseded_by() -> ColumnElement[uuid.UUID | None]:
    # Only a failed request is superseded: a delivered one was done, an open one is the newest.
    return case((ReplenishmentRequest.status == "failed", newer_request_id())).label(
        "superseded_by"
    )


def _request_out(row: ReplenishmentRequest, superseded_by_id: uuid.UUID | None) -> RequestOut:
    return RequestOut.model_validate(row).model_copy(update={"superseded_by": superseded_by_id})


@router.get(
    "/api/v1/outbound-attempts",
    response_model=AttemptListOut,
    responses=error_responses(422),
    tags=["outbound"],
)
def outbound_attempts(
    request: Request,
    status: AttemptStatusFilter = "failed",
    request_id: uuid.UUID | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> AttemptListOut:
    """status=failed: for every request that is failed now, the attempt that failed it.
    status=all: every attempt, newest first. Each attempt carries its request, with
    superseded_by set when a newer request for the item has replaced a failed one."""
    with request.app.state.sessions() as session:
        items = list_attempts(session, status=status, request_id=request_id, limit=limit)
    return AttemptListOut(status=status, items=items)


def list_attempts(
    session: Session,
    status: AttemptStatusFilter = "failed",
    request_id: uuid.UUID | None = None,
    limit: int = 100,
    include_superseded: bool = True,
) -> list[AttemptOut]:
    stmt = select(OutboundAttempt, ReplenishmentRequest, superseded_by()).join(
        ReplenishmentRequest, ReplenishmentRequest.id == OutboundAttempt.request_id
    )
    if status == "failed":
        # A failed request's latest attempt (numbered attempt_count) is the one that failed it.
        stmt = stmt.where(
            ReplenishmentRequest.status == "failed",
            OutboundAttempt.attempt_no == ReplenishmentRequest.attempt_count,
        )
    if request_id is not None:
        stmt = stmt.where(OutboundAttempt.request_id == request_id)
    if not include_superseded:  # the page lists only what a person can still retry
        stmt = stmt.where(newer_request_id().is_(None))
    stmt = stmt.order_by(OutboundAttempt.started_at.desc(), OutboundAttempt.id.desc()).limit(limit)
    columns = [name for name in AttemptOut.model_fields if name != "request"]
    return [
        AttemptOut(**{c: getattr(attempt, c) for c in columns}, request=_request_out(req, newer))
        for attempt, req, newer in session.execute(stmt)
    ]


def list_requests(
    session: Session, status: RequestStatusFilter = "all", limit: int = 100
) -> list[RequestOut]:
    """Requests newest first, for the page; "open" means pending or in flight."""
    stmt = select(ReplenishmentRequest, superseded_by())
    if status == "open":
        stmt = stmt.where(ReplenishmentRequest.status.in_(OPEN_STATUSES))
    elif status != "all":
        stmt = stmt.where(ReplenishmentRequest.status == status)
    stmt = stmt.order_by(ReplenishmentRequest.created_at.desc(), ReplenishmentRequest.id)
    return [_request_out(row, newer) for row, newer in session.execute(stmt.limit(limit))]


# --- manual retry ----------------------------------------------------------------------------


def _request_id_of(attempt_id: int) -> ScalarSelect[uuid.UUID]:
    return (
        select(OutboundAttempt.request_id).where(OutboundAttempt.id == attempt_id).scalar_subquery()
    )


def _request_of_attempt(
    session: Session, attempt_id: int
) -> Row[tuple[uuid.UUID, str, uuid.UUID | None]] | None:
    stmt = select(ReplenishmentRequest.id, ReplenishmentRequest.status, superseded_by()).where(
        ReplenishmentRequest.id == _request_id_of(attempt_id)
    )
    return session.execute(stmt).first()


@router.post(
    "/api/v1/outbound-attempts/{attempt_id}/retry",
    response_model=RetryOut,
    responses={202: {"model": RetryOut}, **error_responses(404, 409, 422)},
    tags=["outbound"],
)
def retry_attempt(attempt_id: int, request: Request) -> JSONResponse:
    """Put a failed request back in the queue: 202 scheduled, or 200 already_scheduled /
    already_delivered on later clicks. Every retry reuses the request's Idempotency-Key, so the
    ERP sees one logical order. 409 superseded once a newer request exists for the item."""
    if not 0 < attempt_id < 2**63:  # anything else cannot exist (and overflows the driver)
        raise ApiError(404, "not_found", f"outbound attempt {attempt_id} does not exist")
    now = utcnow()
    operator_correlation_id = runtime.current().get("correlation_id")
    try:
        with request.app.state.sessions.begin() as session:
            # Write first, read after: the UPDATE waits for a concurrent click to commit, so
            # the read reports the status after it. One conditional UPDATE moves failed to
            # pending, and only while no newer request exists for the item.
            moved = session.execute(
                update(ReplenishmentRequest)
                .where(
                    ReplenishmentRequest.id == _request_id_of(attempt_id),
                    ReplenishmentRequest.status == "failed",
                    newer_request_id().is_(None),
                )
                .values(
                    status="pending",
                    next_attempt_at=now,
                    manual_retry_at=now,  # the worker records the attempt as manual
                    manual_retry_correlation_id=operator_correlation_id,
                    updated_at=now,
                )
                .execution_options(synchronize_session=False)
            ).rowcount
            scheduled = moved == 1
            found = _request_of_attempt(session, attempt_id)
    except IntegrityError:
        # The partial unique index refused to reopen it: a new low event created a request
        # for the item at this very moment (this route takes no item lock). Re-read, and
        # answer the usual 409.
        with request.app.state.sessions() as session:
            found = _request_of_attempt(session, attempt_id)
        if found is None or found.superseded_by is None:
            raise
        log.warning("a newer request for the item was created at the same moment; not retried")
        scheduled = False
    if found is None:
        raise ApiError(404, "not_found", f"outbound attempt {attempt_id} does not exist")
    request_id, request_status, newer_id = found

    if scheduled:
        result, status_code = "scheduled", 202
    elif request_status == "delivered":
        result, status_code = "already_delivered", 200
    elif newer_id is not None:
        result, status_code = "superseded", 409
    else:
        result, status_code = "already_scheduled", 200
    log.info(
        "manual retry requested",
        extra={"replenishment_id": str(request_id), "attempt_id": attempt_id, "result": result},
    )
    if result == "superseded":
        message = f"a newer replenishment request exists for this item: {newer_id}"
        raise ApiError(409, "superseded", message)
    body = RetryOut(
        result=result, attempt_id=attempt_id, request_id=request_id, request_status=request_status
    )
    return JSONResponse(body.model_dump(mode="json"), status_code=status_code)


# --- health -----------------------------------------------------------------------------------
# Both probes are async: a sync route waits for a worker thread, and with every thread stuck
# on a dead database liveness stopped answering too. The ERP is left out of readiness on
# purpose: an ERP outage must not take the webhook out of service.


@router.get("/health/live", tags=["health"])
async def live() -> dict[str, str]:
    return {"status": "ok"}


@router.get(
    "/health/ready", response_model=ReadyOut, responses=error_responses(503), tags=["health"]
)
async def ready(request: Request) -> JSONResponse:
    state = request.app.state

    def database_checks() -> dict[str, str]:
        # Both checks on one connection, so a dead database costs one connect timeout.
        checks = {"database": "unknown", "migrations": "unknown"}
        try:
            with read_only_connection(state.engine) as conn:
                conn.execute(text("SELECT 1"))
                checks["database"] = "ok"
                current = schema_revisions(conn)
        except Exception as exc:
            failed = "migrations" if checks["database"] == "ok" else "database"
            checks[failed] = f"error: {type(exc).__name__}"
            log.warning("readiness check failed", extra={"check": failed, "error": repr(exc)})
            return checks
        if set(current) == set(state.migration_heads):
            checks["migrations"] = "ok"
        else:
            checks["migrations"] = f"at {list(current)}, head is {list(state.migration_heads)}"
            log.warning(
                "readiness check failed",
                extra={"check": "migrations", "error": checks["migrations"]},
            )
        return checks

    try:
        checks = await asyncio.wait_for(asyncio.to_thread(database_checks), READY_TIMEOUT_SECONDS)
    except TimeoutError:
        error = f"no answer within {READY_TIMEOUT_SECONDS:g}s"
        log.warning("readiness check failed", extra={"check": "database", "error": error})
        checks = {"database": f"error: {error}", "migrations": "unknown"}
    # "disabled" (WORKER_ENABLED=false) still counts as ready.
    checks["worker"] = "disabled" if state.worker is None else state.worker.status()
    ok = all(value in ("ok", "disabled") for value in checks.values())
    body = {"status": "ready" if ok else "not_ready", "checks": checks}
    return JSONResponse(body, status_code=200 if ok else 503)
