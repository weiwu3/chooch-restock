"""The data: the four tables (SQLAlchemy), the inbound event contract and the API's JSON
shapes (pydantic).

    observations            one row per accepted event: the history
    inventory_state         one row per item (facility + room + SKU): its current state
    replenishment_requests  one row per logical order
    outbound_attempts       one row per ERP call

The schema is created only by the Alembic migration; tests fail if these classes drift from it.
"""

import math
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Annotated, Any, Self

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)
from sqlalchemy import (
    JSON,
    BigInteger,
    CheckConstraint,
    DateTime,
    Double,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Text,
    TypeDecorator,
    Uuid,
    text,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

INT_MAX = 2_147_483_647  # the quantity columns are 32-bit
# Years in UTC that both databases store and convert without overflow; the history cursor
# uses the same range.
OBSERVED_YEARS = range(1970, 9999)
MAX_JSON_DEPTH = 64  # deeper JSON parses but overflows the stack when stored or read back


# --- text checks shared by the webhook, the read API and the mock ERP -------------------------


def is_utf8(text: str) -> bool:
    """False if the text holds an unpaired surrogate (JSON can carry one as "\\ud800")."""
    try:
        text.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


def is_ascii_digits(text: str) -> bool:
    """Digits 0-9 only: isdigit() alone also takes "²", which int() cannot read."""
    return text.isascii() and text.isdigit()


def no_nul(value: str | None) -> str | None:
    if value is not None and "\x00" in value:
        raise ValueError("must not contain NUL characters")
    return value


def unstorable(value: Any, depth: int = 1) -> str | None:
    """Why this parsed JSON must not be stored, or None: nesting past MAX_JSON_DEPTH, and what
    PostgreSQL's JSONB refuses (NUL, unpaired surrogates, non-finite numbers), refused on both
    databases so they accept the same events. Iterative, so no depth overflows the stack."""
    pending = [(value, depth)]
    while pending:
        item, level = pending.pop()
        if isinstance(item, dict | list) and level > MAX_JSON_DEPTH:
            return f"must not nest more than {MAX_JSON_DEPTH} levels of objects and arrays"
        if isinstance(item, dict):
            pending.extend((key, level) for key in item)
            pending.extend((child, level + 1) for child in item.values())
        elif isinstance(item, list):
            pending.extend((child, level + 1) for child in item)
        elif isinstance(item, str) and "\x00" in item:
            return "must not contain NUL characters"
        elif isinstance(item, str) and not is_utf8(item):
            return "must not contain unpaired surrogates"
        elif isinstance(item, float) and not math.isfinite(item):
            return "numbers must be finite"
    return None


# --- the tables -------------------------------------------------------------------------------

# One naming convention for the models, the migration and both databases, so the drift tests
# can compare constraints by name.
NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}
# What inbound.decide() concluded for an event (its docstring has the table).
DECISIONS = (
    "none",
    "recovered",
    "request_created",
    "blocked_open_request",
    "blocked_not_armed",
    "stale",
)
REQUEST_STATUSES = ("pending", "in_flight", "delivered", "failed")
# Only an open request blocks a new one: a failed one waits for a Retry without stalling its item.
OPEN_STATUSES = ("pending", "in_flight")
# in_progress: written with the claim, before the call; lost: the claim expired without a result.
CLASSIFICATIONS = ("success", "retryable", "non_retryable", "ambiguous", "in_progress", "lost")
TRIGGERS = ("worker", "manual")


class UTCDateTime(TypeDecorator):
    """Aware UTC on both databases: timestamptz on PostgreSQL, naive UTC stored on SQLite."""

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError("naive datetime; use timezone-aware UTC")
        value = value.astimezone(UTC)
        return value.replace(tzinfo=None) if dialect.name == "sqlite" else value

    def process_result_value(self, value: datetime | None, dialect) -> datetime | None:
        if value is None:
            return None
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


BigId = BigInteger().with_variant(Integer(), "sqlite")  # SQLite autoincrements INTEGER only
JsonType = JSON().with_variant(postgresql.JSONB(), "postgresql")
# The tie-breaker compares event ids as plain strings; COLLATE "C" keeps ORDER BY and the
# history cursor in that order on PostgreSQL (a locale collation would differ).
EventId = String(128).with_variant(postgresql.VARCHAR(128, collation="C"), "postgresql")


def _in(column: str, values: tuple[str, ...]) -> str:
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class Observation(Base):
    """One accepted event, stale ones included. UNIQUE(event_id) is the idempotency gate: a
    redelivery, even a concurrent one, hits it and is answered as a duplicate."""

    __tablename__ = "observations"
    __table_args__ = (
        Index(
            "ix_observations_item_order", "facility_id", "room_id", "sku", "observed_at", "event_id"
        ),
        # The brief's history query names a room and SKU but no facility.
        Index("ix_observations_room_sku_order", "room_id", "sku", "observed_at", "event_id"),
        CheckConstraint("observed_quantity >= 0", name="quantity_non_negative"),
        CheckConstraint("par_level >= reorder_point AND reorder_point >= 0", name="thresholds"),
        CheckConstraint("confidence >= 0 AND confidence <= 1", name="confidence_range"),
        CheckConstraint(_in("decision", DECISIONS), name="decision_valid"),
    )

    id: Mapped[int] = mapped_column(BigId, primary_key=True)
    event_id: Mapped[str] = mapped_column(EventId, unique=True)
    facility_id: Mapped[str] = mapped_column(String(64))
    room_id: Mapped[str] = mapped_column(String(64))
    sku: Mapped[str] = mapped_column(String(64))
    observed_at: Mapped[datetime] = mapped_column(UTCDateTime)
    observed_quantity: Mapped[int] = mapped_column(Integer)
    par_level: Mapped[int] = mapped_column(Integer)
    reorder_point: Mapped[int] = mapped_column(Integer)
    confidence: Mapped[float] = mapped_column(Double)
    source: Mapped[str | None] = mapped_column(String(128))
    camera_id: Mapped[str | None] = mapped_column(String(128))
    # "metadata" is reserved on declarative classes, hence the attribute name.
    meta: Mapped[dict[str, Any] | None] = mapped_column("metadata", JsonType)
    received_at: Mapped[datetime] = mapped_column(UTCDateTime)
    correlation_id: Mapped[str] = mapped_column(String(128))
    applied: Mapped[bool]  # it became the item's current state
    decision: Mapped[str] = mapped_column(String(32))


class InventoryState(Base):
    """One item's current state: the newest applied event's values plus the armed flag. A new
    item starts armed, a request disarms it, an applied event above the reorder point re-arms
    it; so after a request the next one needs a recovery and then a later low reading."""

    __tablename__ = "inventory_state"
    __table_args__ = (
        CheckConstraint("status IN ('ok', 'low')", name="status_valid"),
        CheckConstraint("current_quantity >= 0", name="quantity_non_negative"),
        CheckConstraint("par_level >= reorder_point AND reorder_point >= 0", name="thresholds"),
    )

    facility_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    room_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    sku: Mapped[str] = mapped_column(String(64), primary_key=True)
    current_quantity: Mapped[int] = mapped_column(Integer)
    par_level: Mapped[int] = mapped_column(Integer)
    reorder_point: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(8))  # "low" at or below the reorder point
    last_event_id: Mapped[str] = mapped_column(EventId, ForeignKey("observations.event_id"))
    last_observed_at: Mapped[datetime] = mapped_column(UTCDateTime)
    armed: Mapped[bool] = mapped_column(default=True, server_default=text("true"))
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime)


class ReplenishmentRequest(Base):
    """One logical order. The UUID is made before the insert, so the idempotency key
    "replenishment:<id>" is stored with the row and every attempt sends the same key and body.

    status: pending (due at next_attempt_at), in_flight (a worker is calling the ERP),
    delivered (a 2xx named the order; final), failed (refused for good, or out of attempts:
    last_error "exhausted"; a manual retry moves it back to pending).

    The partial unique index uq_replenishment_requests_open_item allows one open (pending or
    in_flight) request per item: the database's own backstop for the inbound check and for a
    retry racing a new low event."""

    __tablename__ = "replenishment_requests"
    __table_args__ = (
        Index(
            "uq_replenishment_requests_open_item",
            "facility_id",
            "room_id",
            "sku",
            unique=True,
            postgresql_where=text(_in("status", OPEN_STATUSES)),
            sqlite_where=text(_in("status", OPEN_STATUSES)),
        ),
        Index(None, "status", "next_attempt_at"),  # the worker's due list
        Index(None, "facility_id", "room_id", "sku", "created_at"),  # newer-request lookup
        CheckConstraint("requested_quantity > 0", name="quantity_positive"),
        CheckConstraint("observed_quantity >= 0", name="observed_quantity_non_negative"),
        CheckConstraint("par_level >= reorder_point AND reorder_point >= 0", name="thresholds"),
        CheckConstraint("attempt_count >= 0", name="attempt_count_non_negative"),
        CheckConstraint(_in("status", REQUEST_STATUSES), name="status_valid"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    facility_id: Mapped[str] = mapped_column(String(64))
    room_id: Mapped[str] = mapped_column(String(64))
    sku: Mapped[str] = mapped_column(String(64))
    triggering_event_id: Mapped[str] = mapped_column(
        EventId, ForeignKey("observations.event_id"), unique=True
    )
    requested_quantity: Mapped[int] = mapped_column(Integer)
    observed_quantity: Mapped[int] = mapped_column(Integer)
    par_level: Mapped[int] = mapped_column(Integer)
    reorder_point: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(16))
    idempotency_key: Mapped[str] = mapped_column(String(64), unique=True)
    erp_order_id: Mapped[str | None] = mapped_column(String(128))
    # Each claim adds one and uses the new value as its attempt_no, which fences late results.
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, server_default=text("0"))
    next_attempt_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    last_error: Mapped[str | None] = mapped_column(Text)
    # Set by the retry endpoint so the next attempt row says triggered_by="manual".
    manual_retry_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    manual_retry_correlation_id: Mapped[str | None] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime)  # the claim time while in_flight


class OutboundAttempt(Base):
    """One ERP call. Inserted as in_progress with the claim, before the call, so a process
    that dies mid-call still leaves a row; completed with the result afterwards."""

    __tablename__ = "outbound_attempts"
    __table_args__ = (
        Index(None, "request_id", "attempt_no"),
        CheckConstraint(_in("classification", CLASSIFICATIONS), name="classification_valid"),
        CheckConstraint(_in("triggered_by", TRIGGERS), name="triggered_by_valid"),
    )

    id: Mapped[int] = mapped_column(BigId, primary_key=True)
    request_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("replenishment_requests.id"))
    attempt_no: Mapped[int] = mapped_column(Integer)
    idempotency_key: Mapped[str] = mapped_column(String(64))
    request_body: Mapped[dict[str, Any]] = mapped_column(JsonType)
    http_status: Mapped[int | None] = mapped_column(Integer)
    response_body: Mapped[Any | None] = mapped_column(JsonType)
    classification: Mapped[str] = mapped_column(String(16))
    error: Mapped[str | None] = mapped_column(Text)
    triggered_by: Mapped[str] = mapped_column(String(8))
    correlation_id: Mapped[str | None] = mapped_column(String(128))
    manual_retry_correlation_id: Mapped[str | None] = mapped_column(String(128))
    started_at: Mapped[datetime] = mapped_column(UTCDateTime)
    finished_at: Mapped[datetime] = mapped_column(UTCDateTime)  # equals started_at until done
    duration_ms: Mapped[int] = mapped_column(Integer)


# --- the inbound event contract -------------------------------------------------------------
# strict=True: "4", 4.0 and true are not integers. Ids are opaque, compared exactly as sent.

Count = Annotated[int, Field(strict=True, ge=0, le=INT_MAX)]
Ident = Annotated[str, Field(strict=True, min_length=1, max_length=64), AfterValidator(no_nul)]
OptionalLabel = Annotated[str | None, Field(strict=True, max_length=128), AfterValidator(no_nul)]
_RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(\.\d+)?([Zz]|[+-]\d{2}:\d{2})$")
# A correlation id goes into HTTP headers and logs, so only this safe token is used as one.
SAFE_CORRELATION_ID = re.compile(r"[A-Za-z0-9._:\-]{1,128}")


@dataclass(frozen=True)
class ObservedAtLimit:
    """The validation context the webhook passes: server time and OBSERVED_AT_MAX_FUTURE_SECONDS."""

    now: datetime
    max_future_seconds: int


class InventoryEvent(BaseModel):
    """The body a gateway posts (fixtures/inbound/). The nine fields of the brief's example are
    required; unknown top-level fields are kept (model_extra) and stored under metadata._extra.
    Pydantic reports every field failure together, so fixture 07 gets all five in one 422."""

    model_config = ConfigDict(extra="allow")

    event_id: Annotated[
        str, Field(strict=True, min_length=1, max_length=128), AfterValidator(no_nul)
    ]
    facility_id: Ident
    room_id: Ident
    sku: Ident
    observed_at: AwareDatetime
    observed_quantity: Count
    par_level: Count  # declared before reorder_point, so its validator can see it
    reorder_point: Count
    confidence: Annotated[float, Field(strict=True, ge=0, le=1)]
    source: OptionalLabel = None
    camera_id: OptionalLabel = None
    metadata: dict[str, Any] | None = None

    @field_validator("observed_at", mode="before")
    @classmethod
    def _rfc3339_with_offset(cls, value: Any) -> datetime:
        """An RFC 3339 string with an offset (pydantic alone would also take unix numbers),
        naming a real moment within OBSERVED_YEARS; returned in UTC."""
        if not isinstance(value, str) or not _RFC3339.match(value):
            raise ValueError("must be an RFC 3339 timestamp with offset, e.g. 2026-08-18T19:22:10Z")
        try:
            parsed = datetime.fromisoformat(value.upper())  # upper(): RFC 3339 allows "z"
        except ValueError as exc:
            raise ValueError(f"not a valid date/time ({exc})") from None
        out_of_range = (
            f"must fall in the years {OBSERVED_YEARS.start}-{OBSERVED_YEARS.stop - 1} in UTC"
        )
        try:
            utc = parsed.astimezone(UTC)
        except OverflowError:  # year 1 or 9999 pushed off the calendar by its offset
            raise ValueError(out_of_range) from None
        if utc.year not in OBSERVED_YEARS:
            raise ValueError(out_of_range)
        return utc

    @field_validator("observed_at")
    @classmethod
    def _not_too_far_ahead(cls, value: datetime, info: ValidationInfo) -> datetime:
        """A far-future reading would become the current state and make every real reading
        after it stale, so one wrong gateway clock could freeze a bin for good."""
        limit = info.context
        if not isinstance(limit, ObservedAtLimit):
            raise TypeError("validate InventoryEvent with an ObservedAtLimit as its context")
        ahead = (value - limit.now).total_seconds()
        if ahead > limit.max_future_seconds:
            raise ValueError(
                f"observed_at is {math.ceil(ahead)} s in the future; "
                f"at most {limit.max_future_seconds} s is accepted"
            )
        return value

    @field_validator("reorder_point")
    @classmethod
    def _reorder_point_within_par(cls, value: int, info: ValidationInfo) -> int:
        # A field validator, not a model validator: pydantic skips model validators once any
        # field failed, and 07 must list all five problems.
        par_level = info.data.get("par_level")
        if par_level is not None and value > par_level:
            raise ValueError(f"reorder_point ({value}) must not exceed par_level ({par_level})")
        return value

    @field_validator("metadata")
    @classmethod
    def _metadata_storable(cls, value: dict[str, Any] | None) -> dict[str, Any] | None:
        problem = unstorable(value)
        if problem is not None:
            raise ValueError(problem)
        return value

    @model_validator(mode="after")
    def _unknown_fields_storable(self) -> Self:
        problem = unstorable(self.model_extra, depth=0)
        if problem is not None:
            raise ValueError(f"unknown fields: {problem}")
        return self

    def correlation_hint(self) -> str | None:
        """metadata.correlation_id, if it is a safe token; the webhook uses it when the caller
        sent no usable X-Request-ID."""
        value = (self.metadata or {}).get("correlation_id")
        if isinstance(value, str) and SAFE_CORRELATION_ID.fullmatch(value):
            return value
        return None


def format_errors(errors: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Pydantic's errors as a 422's "errors" list: {field, type, message, input}."""
    formatted = []
    for error in errors:
        entry: dict[str, Any] = {
            "field": ".".join(str(part) for part in error["loc"]) or "(body)",
            "type": error["type"],
            "message": error["msg"].removeprefix("Value error, "),
        }
        value = error.get("input")
        # Echo only short scalars that strict JSON can carry (not inf, not a lone surrogate).
        echo = isinstance(value, (str, int, float, bool)) and len(str(value)) <= 100
        echo = echo and error["type"] != "missing"
        if isinstance(value, float):
            echo = echo and math.isfinite(value)
        if isinstance(value, str):
            echo = echo and is_utf8(value)
        if echo:
            entry["input"] = value
        formatted.append(entry)
    return formatted


# --- response models ------------------------------------------------------------------------
# from_attributes=True builds a model straight from a database row. Comments, not docstrings:
# pydantic copies a docstring into /openapi.json.


class ApiModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class ErrorOut(ApiModel):  # every error: {"error": code, "message": text}
    error: str
    message: str


class FieldErrorOut(ApiModel):
    field: str
    type: str
    message: str
    input: Any | None = None


class ValidationErrorOut(ApiModel):
    error: str = "validation_failed"
    message: str
    errors: list[FieldErrorOut]


class StateOut(ApiModel):
    facility_id: str
    room_id: str
    sku: str
    current_quantity: int
    par_level: int
    reorder_point: int
    status: str
    armed: bool
    last_event_id: str
    last_observed_at: datetime
    updated_at: datetime


class RequestSummary(ApiModel):
    id: uuid.UUID
    quantity: int
    status: str


class EventResponse(ApiModel):  # POST /api/v1/inventory-events
    status: str  # "accepted" (201) or "duplicate" (200)
    event_id: str
    applied: bool
    decision: str
    current: StateOut
    replenishment_request: RequestSummary | None


class ObservationOut(ApiModel):
    event_id: str
    facility_id: str
    room_id: str
    sku: str
    observed_at: datetime
    observed_quantity: int
    par_level: int
    reorder_point: int
    confidence: float
    source: str | None
    camera_id: str | None
    received_at: datetime
    correlation_id: str
    applied: bool
    decision: str


class RequestOut(ApiModel):
    id: uuid.UUID
    facility_id: str
    room_id: str
    sku: str
    status: str
    requested_quantity: int
    observed_quantity: int
    par_level: int
    reorder_point: int
    triggering_event_id: str
    idempotency_key: str
    erp_order_id: str | None
    attempt_count: int
    next_attempt_at: datetime | None
    last_error: str | None
    created_at: datetime
    updated_at: datetime
    # For a failed request: the newer request for its item that replaced it (api.superseded_by).
    superseded_by: uuid.UUID | None = None


class InventoryItemOut(StateOut):
    last_observation: ObservationOut
    open_request: RequestOut | None


class InventoryOut(ApiModel):  # GET /api/v1/rooms/{room_id}/inventory
    room_id: str
    facility_id: str | None
    items: list[InventoryItemOut]


class HistoryOut(ApiModel):  # GET /api/v1/items/{sku}/history
    sku: str
    room_id: str
    facility_id: str | None
    items: list[ObservationOut]
    next_cursor: str | None


class AttemptOut(ApiModel):
    id: int
    request_id: uuid.UUID
    attempt_no: int
    idempotency_key: str
    triggered_by: str
    classification: str
    http_status: int | None
    response_body: Any | None
    error: str | None
    correlation_id: str | None
    manual_retry_correlation_id: str | None
    started_at: datetime
    finished_at: datetime
    duration_ms: int
    request: RequestOut


class AttemptListOut(ApiModel):  # GET /api/v1/outbound-attempts
    status: str
    items: list[AttemptOut]


class ReadyOut(ApiModel):  # GET /health/ready
    status: str  # "ready" (200) or "not_ready" (503)
    checks: dict[str, str]


class RetryOut(ApiModel):  # POST /api/v1/outbound-attempts/{id}/retry
    result: str  # scheduled | already_scheduled | already_delivered
    attempt_id: int
    request_id: uuid.UUID
    request_status: str
