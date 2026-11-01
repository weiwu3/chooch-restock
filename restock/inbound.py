"""The inbound path: the replenishment rules (decide) and ingest(), which stores one
validated event and acts on it in one database transaction.

The webhook route (api.py) has already authenticated and validated the event. ingest():

    1. Insert the observation; UNIQUE(event_id) makes a redelivery insert nothing, and it is
       answered from the stored row (200 duplicate).
    2. Create the item's state row if this is its first event, then lock it, so one item's
       events are decided one at a time.
    3. decide(): applied or stale, the decision word, the armed flag after it, units to order.
    4. Insert the request, if any, in a savepoint; the partial unique index is the backstop.
    5. Apply a newer event to the state row and store the decision. Commit.
"""

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from restock.runtime import dialect_insert, is_postgres, utcnow
from restock.schemas import (
    OPEN_STATUSES,
    EventResponse,
    InventoryEvent,
    InventoryState,
    Observation,
    ReplenishmentRequest,
    RequestSummary,
    StateOut,
)

log = logging.getLogger("restock.inbound")


# --- the rules ------------------------------------------------------------------------------


@dataclass(frozen=True, order=True)
class OrderKey:
    """Where an event sits in time: observed_at, then event_id as a plain string on a tie."""

    observed_at: datetime
    event_id: str


def is_newer(candidate: OrderKey, current: OrderKey | None) -> bool:
    return current is None or candidate > current


def is_low(quantity: int, reorder_point: int) -> bool:
    return quantity <= reorder_point


def quantity_needed(par_level: int, quantity: int) -> int:
    return max(par_level - quantity, 0)


@dataclass(frozen=True)
class Outcome:
    applied: bool  # the event becomes the item's current state
    decision: str  # one of schemas.DECISIONS
    armed: bool  # the item's armed flag after this event
    request_quantity: int | None = None  # set only for request_created


def decide(
    *,
    event: OrderKey,
    current: OrderKey | None,
    armed: bool,
    quantity: int,
    par_level: int,
    reorder_point: int,
    open_request: bool,
) -> Outcome:
    """The rules, top row first:

    not newer than the current state        stale (changes nothing)
    above the reorder point, armed          none
    above the reorder point, not armed      recovered (re-arms the item)
    low, nothing needed                     none
    low, not armed                          blocked_not_armed
    low, armed, an open request exists      blocked_open_request (stays armed)
    low, armed, no open request             request_created (disarms the item)
    """
    if not is_newer(event, current):
        return Outcome(applied=False, decision="stale", armed=armed)
    if not is_low(quantity, reorder_point):
        return Outcome(applied=True, decision="none" if armed else "recovered", armed=True)
    needed = quantity_needed(par_level, quantity)
    if needed == 0:  # quantity == par_level == reorder_point
        return Outcome(applied=True, decision="none", armed=armed)
    if not armed:
        return Outcome(applied=True, decision="blocked_not_armed", armed=False)
    if open_request:
        return Outcome(applied=True, decision="blocked_open_request", armed=True)
    return Outcome(applied=True, decision="request_created", armed=False, request_quantity=needed)


# --- one event, one transaction ---------------------------------------------------------------

# A redelivery should repeat these exactly; if not, the stored event wins and I log a warning.
_COMPARED_FIELDS = (
    "facility_id",
    "room_id",
    "sku",
    "observed_at",
    "observed_quantity",
    "par_level",
    "reorder_point",
)


@dataclass(frozen=True)
class IngestResult:
    duplicate: bool  # answered 200 instead of 201
    response: EventResponse


def ingest(
    sessions: sessionmaker[Session], event: InventoryEvent, correlation_id: str
) -> IngestResult:
    """Store one validated event and act on it, in one transaction (the module docstring)."""
    now = utcnow()
    status = "low" if is_low(event.observed_quantity, event.reorder_point) else "ok"
    with sessions.begin() as session:
        # 1. Insert the observation. ON CONFLICT DO NOTHING ... RETURNING: no row back means
        # the event_id is stored already. (A plain unique violation would abort the whole
        # PostgreSQL transaction.) A concurrent redelivery waits here for the first one.
        insert = dialect_insert(session)
        metadata = dict(event.metadata or {})
        if event.model_extra:
            key = "_extra"
            while key in metadata:  # never overwrite the gateway's own "_extra"
                key = "_" + key
            metadata[key] = event.model_extra
        inserted = session.execute(
            insert(Observation.__table__)
            .values(
                event_id=event.event_id,
                facility_id=event.facility_id,
                room_id=event.room_id,
                sku=event.sku,
                observed_at=event.observed_at,
                observed_quantity=event.observed_quantity,
                par_level=event.par_level,
                reorder_point=event.reorder_point,
                confidence=event.confidence,
                source=event.source,
                camera_id=event.camera_id,
                metadata=metadata or None,
                received_at=now,
                correlation_id=correlation_id,
                applied=False,  # placeholders; step 5 writes the decision
                decision="none",
            )
            .on_conflict_do_nothing(index_elements=["event_id"])
            .returning(Observation.__table__.c.id)
        ).first()
        if inserted is None:
            # A redelivery: repeat what the first delivery decided, with the state now.
            stored = session.scalars(
                select(Observation).where(Observation.event_id == event.event_id)
            ).one()
            if any(getattr(stored, f) != getattr(event, f) for f in _COMPARED_FIELDS):
                log.warning("duplicate event_id with a different payload; the stored event wins")
            state = session.get_one(
                InventoryState, (stored.facility_id, stored.room_id, stored.sku)
            )
            request = session.scalars(
                select(ReplenishmentRequest).where(
                    ReplenishmentRequest.triggering_event_id == stored.event_id
                )
            ).first()
            log.info("duplicate inventory event", extra={"decision": stored.decision})
            response = _response(
                "duplicate", stored.event_id, stored.applied, stored.decision, state, request
            )
            return IngestResult(duplicate=True, response=response)

        # 2. Create the state row if the item is new (two first events cannot both create it),
        # then lock it: SELECT ... FOR UPDATE on PostgreSQL; SQLite already holds its single
        # write lock from BEGIN IMMEDIATE (runtime.make_engine).
        created = session.execute(
            insert(InventoryState.__table__)
            .values(
                facility_id=event.facility_id,
                room_id=event.room_id,
                sku=event.sku,
                current_quantity=event.observed_quantity,
                par_level=event.par_level,
                reorder_point=event.reorder_point,
                status=status,
                last_event_id=event.event_id,
                last_observed_at=event.observed_at,
                armed=True,  # a new item's first low reading may order
                updated_at=now,
            )
            .on_conflict_do_nothing(index_elements=["facility_id", "room_id", "sku"])
            .returning(InventoryState.__table__.c.facility_id)
        ).first()
        stmt = select(InventoryState).where(
            InventoryState.facility_id == event.facility_id,
            InventoryState.room_id == event.room_id,
            InventoryState.sku == event.sku,
        )
        state = session.scalars(stmt.with_for_update() if is_postgres(session) else stmt).one()

        # 3. Decide. A state row just created holds this very event: nothing older to compare.
        current = None
        if created is None:
            current = OrderKey(state.last_observed_at, state.last_event_id)
        outcome = decide(
            event=OrderKey(event.observed_at, event.event_id),
            current=current,
            armed=state.armed,
            quantity=event.observed_quantity,
            par_level=event.par_level,
            reorder_point=event.reorder_point,
            open_request=_open_request(session, event) is not None,
        )

        # 4. Create the request, due now, in a savepoint. The id is made here so the
        # idempotency key is stored in the same insert.
        request = None
        if outcome.request_quantity is not None:
            request_id = uuid.uuid4()
            request = ReplenishmentRequest(
                id=request_id,
                facility_id=event.facility_id,
                room_id=event.room_id,
                sku=event.sku,
                triggering_event_id=event.event_id,
                requested_quantity=outcome.request_quantity,
                observed_quantity=event.observed_quantity,
                par_level=event.par_level,
                reorder_point=event.reorder_point,
                status="pending",
                idempotency_key=f"replenishment:{request_id}",
                attempt_count=0,
                next_attempt_at=now,
                created_at=now,
                updated_at=now,
            )
            try:
                with session.begin_nested():
                    session.add(request)
            except IntegrityError:
                # The partial unique index refused it: an open request exists after all (a
                # manual retry reopened one at this moment; retries take no item lock).
                if _open_request(session, event) is None:
                    raise  # some other failure, which is a bug
                log.warning("open request already exists for the item; not creating another")
                request = None
                outcome = Outcome(applied=True, decision="blocked_open_request", armed=True)

        # 5. Apply a newer event to the state (thresholds too: each event's own are
        # authoritative) and store the decision on the observation.
        if outcome.applied:
            state.current_quantity = event.observed_quantity
            state.par_level = event.par_level
            state.reorder_point = event.reorder_point
            state.status = status
            state.last_event_id = event.event_id
            state.last_observed_at = event.observed_at
            state.armed = outcome.armed
            state.updated_at = utcnow()
        session.execute(
            update(Observation)
            .where(Observation.event_id == event.event_id)
            .values(applied=outcome.applied, decision=outcome.decision)
            .execution_options(synchronize_session=False)
        )
        response = _response(
            "accepted", event.event_id, outcome.applied, outcome.decision, state, request
        )

    log.info(
        "inventory event accepted",
        extra={
            "decision": outcome.decision,
            "applied": outcome.applied,
            "replenishment_id": str(request.id) if request is not None else None,
        },
    )
    return IngestResult(duplicate=False, response=response)


def _open_request(session: Session, event: InventoryEvent) -> ReplenishmentRequest | None:
    """The item's pending or in-flight request, if any. A failed one does not count."""
    stmt = select(ReplenishmentRequest).where(
        ReplenishmentRequest.facility_id == event.facility_id,
        ReplenishmentRequest.room_id == event.room_id,
        ReplenishmentRequest.sku == event.sku,
        ReplenishmentRequest.status.in_(OPEN_STATUSES),
    )
    return session.scalars(stmt).first()


def _response(
    status: str,
    event_id: str,
    applied: bool,
    decision: str,
    state: InventoryState,
    request: ReplenishmentRequest | None,
) -> EventResponse:
    summary = None
    if request is not None:
        summary = RequestSummary(
            id=request.id, quantity=request.requested_quantity, status=request.status
        )
    return EventResponse(
        status=status,
        event_id=event_id,
        applied=applied,
        decision=decision,
        current=StateOut.model_validate(state),
        replenishment_request=summary,
    )
