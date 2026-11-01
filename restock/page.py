"""The operations page at GET /: three numbers, the orders that need a person (with Retry),
the orders on their way, and one card per bin with a stock chart and a short history.

The page's words map to the data: a "bin" is an item, a "count" an observation, an "order" a
replenishment request, a "try" an outbound attempt. Every chip the page shows is in CHIPS, and
the glossary prints that table, so it explains every chip.
"""

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.orm import Session

from restock.api import inventory_items, list_attempts, list_requests, newer_request_id
from restock.runtime import REPO_ROOT, utcnow
from restock.schemas import (
    AttemptOut,
    InventoryItemOut,
    Observation,
    OutboundAttempt,
    ReplenishmentRequest,
    RequestOut,
)

router = APIRouter()
templates = Jinja2Templates(directory=str(REPO_ROOT / "templates"))
# A tab left open reloads itself when this build of the page changes.
PAGE_VERSION = hashlib.sha256((REPO_ROOT / "templates" / "index.html").read_bytes()).hexdigest()[
    :12
]
ROW_LIMIT = 200  # rows per order list, newest first

# --- the words -------------------------------------------------------------------------------

GOOD = "good"  # green: nothing to do
WAITING = "waiting"  # amber: low, or an order on its way
ATTENTION = "attention"  # red: a person has to act
QUIET = "quiet"  # grey: no longer matters
COLOURS = [GOOD, WAITING, ATTENTION, QUIET]


@dataclass(frozen=True)
class Chip:
    text: str
    colour: str
    meaning: str  # what the glossary says


OK = Chip("OK", GOOD, "Nothing to do.")
LOW = Chip("Low", WAITING, "At or below its refill level; the next count orders more.")
LOW_ORDERED = Chip("Low, ordered", WAITING, "An order is on its way to the ERP, or the ERP has it.")
NEEDS_A_PERSON = Chip("Needs a person", ATTENTION, "Its order failed; see Needs a person.")
REFUSED_CHIP = Chip("Refused", ATTENTION, "The ERP refused the order. Fix the cause, then Retry.")
OUT_OF_TRIES = Chip(
    "Out of tries", ATTENTION, "The ERP stayed busy, down or silent through every try."
)
RETRY_FAILED = Chip("Manual retry failed", ATTENTION, "A person's Retry did not go through either.")
QUEUED = Chip("Queued", WAITING, "New; sent within seconds.")
SENDING = Chip("Sending", WAITING, "Waiting for the ERP's answer.")
RETRYING = Chip(
    "Retrying", WAITING, "The last try did not go through; the next is at the time shown."
)
CHIPS = [
    OK,
    LOW,
    LOW_ORDERED,
    NEEDS_A_PERSON,
    REFUSED_CHIP,
    OUT_OF_TRIES,
    RETRY_FAILED,
    QUEUED,
    SENDING,
    RETRYING,
]


@dataclass(frozen=True, eq=False)  # eq=False: same words can be different situations
class Situation:
    chip: Chip
    note: str  # the card's line about its order; "{units}" is filled in
    low: bool  # counted among the low bins


IN_STOCK = Situation(OK, "", low=False)
FULL = Situation(OK, "", low=False)  # low by the numbers, but nothing is missing
LOW_ORDER_ON_ITS_WAY = Situation(LOW_ORDERED, "Order for {units} on its way", low=True)
LOW_WAITING_FOR_REFILL = Situation(LOW_ORDERED, "Waiting for the refill", low=True)
LOW_NEXT_COUNT_ORDERS = Situation(LOW, "Next count orders more", low=True)
LOW_ORDER_FAILED = Situation(NEEDS_A_PERSON, "Order for {units} failed", low=True)
IN_STOCK_ORDER_ON_ITS_WAY = Situation(OK, "Order for {units} on its way", low=False)
IN_STOCK_ORDER_FAILED = Situation(NEEDS_A_PERSON, "Earlier order for {units} failed", low=False)
SITUATIONS = [
    IN_STOCK,
    FULL,
    LOW_ORDER_ON_ITS_WAY,
    LOW_WAITING_FOR_REFILL,
    LOW_NEXT_COUNT_ORDERS,
    LOW_ORDER_FAILED,
    IN_STOCK_ORDER_ON_ITS_WAY,
    IN_STOCK_ORDER_FAILED,
]

NO_ANSWER = "No answer from the ERP"
ERP_BUSY = "The ERP was busy"  # 429
ERP_UNAVAILABLE = "The ERP was unavailable"  # 503
ERP_PROBLEM = "The ERP had a problem on its side"  # other 5xx
ERP_REFUSED = "The ERP refused the order"  # 4xx
ERP_UNEXPECTED = "The ERP gave an unexpected answer"  # 1xx, 3xx, or a 2xx naming no order
ERP_ANSWERS = [NO_ANSWER, ERP_BUSY, ERP_UNAVAILABLE, ERP_PROBLEM, ERP_REFUSED, ERP_UNEXPECTED]

REFUSED = "Refused by the ERP"
UNEXPECTED = "Unexpected ERP answer"
NOT_CONFIRMED = "Not confirmed after {tries} tries"
FIX_THE_CAUSE = (
    "Ask whoever looks after the ERP to fix the cause, then press Retry. The ERP's message, "
    "if it sent one, says what was wrong."
)
CHECK_THE_ERP = "Ask IT to check that the ERP is working again, then press Retry."

# --- rows and cards -------------------------------------------------------------------------


@dataclass(frozen=True)
class FailedOrderRow:
    attempt_id: int  # Retry posts to /api/v1/outbound-attempts/{attempt_id}/retry
    sku: str
    room: str
    units: int
    chip: Chip
    reason: str
    failed_at: datetime
    details_id: str  # the HTML id of its "more", so a refresh can open it again
    details: list[tuple[str, str]]


@dataclass(frozen=True)
class OrderRow:
    sku: str
    room: str
    units: int
    chip: Chip
    when: str  # "Try 1 soon", "Try 2 now", or "Try 3 at" with next_try_at after it
    next_try_at: datetime | None
    details_id: str
    details: list[tuple[str, str]]


@dataclass(frozen=True)
class BinCard:
    sku: str
    room: str
    stock_now: int
    should_hold: int
    refill_at: int
    situation: Situation
    note: str
    last_counted: datetime
    details_id: str
    details: list[tuple[str, str]]
    history: "BinHistory"


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
def index(request: Request) -> HTMLResponse:
    now = utcnow()
    with request.app.state.sessions() as session:
        # A failed order replaced by a newer one needs nobody (the retry API refuses it).
        failed_tries = list_attempts(
            session, status="failed", limit=ROW_LIMIT, include_superseded=False
        )
        on_their_way = list_requests(session, status="open", limit=ROW_LIMIT)
        latest_tries = last_attempts(session, [order.id for order in on_their_way])
        items = inventory_items(session)
        failed_last = failed_last_orders(session)
        histories = [bin_history(session, item, now) for item in items]
    several = len({item.facility_id for item in items}) > 1  # name facilities only then
    bins = [
        bin_card(
            item, failed_last.get((item.facility_id, item.room_id, item.sku)), several, history
        )
        for item, history in zip(items, histories, strict=True)
    ]
    failed_orders = [failed_order_row(attempt, several) for attempt in failed_tries]
    context = {
        "updated_at": now,
        "failed_orders": failed_orders,
        "orders": [order_row(o, latest_tries.get(o.id), several) for o in on_their_way],
        "bins": bins,
        "needing_a_person": len(failed_orders),
        "low_bins": sum(1 for card in bins if card.situation.low),
        "chips": CHIPS,
        "page_version": PAGE_VERSION,
    }
    return templates.TemplateResponse(request, "index.html", context)


def failed_order_row(attempt: AttemptOut, several: bool) -> FailedOrderRow:
    request = attempt.request
    details = [("What happened", failure_sentence(attempt))]
    message = erp_message(attempt.response_body)
    if message is not None:
        details.append(("The ERP said", f'"{message}"'))
    details.append(("What to do", next_step(attempt)))
    details.extend(order_details(request, attempt))
    return FailedOrderRow(
        attempt_id=attempt.id,
        sku=request.sku,
        room=room_label(request.room_id, request.facility_id, several),
        units=request.requested_quantity,
        chip=failure_chip(attempt),
        reason=failure_reason(attempt),
        failed_at=attempt.finished_at,
        details_id=f"more-{request.id}",
        details=details,
    )


def order_row(request: RequestOut, last_try: OutboundAttempt | None, several: bool) -> OrderRow:
    chip = order_stage(request, last_try)
    next_try_at = None
    details = []
    if chip is RETRYING:
        # Shown even once the time has passed: a time in the past shows nothing is sending.
        when = f"Try {request.attempt_count + 1} at"
        next_try_at = request.next_attempt_at
    elif chip is SENDING:
        when = f"Try {request.attempt_count} now"
    else:
        when = "Try 1 soon"
    if last_try is not None and chip is not SENDING:
        details.append(("Last answer", erp_answer(last_try.http_status)))
    details.append(("Tries so far", str(request.attempt_count)))
    details.extend(order_details(request, last_try))
    return OrderRow(
        sku=request.sku,
        room=room_label(request.room_id, request.facility_id, several),
        units=request.requested_quantity,
        chip=chip,
        when=when,
        next_try_at=next_try_at,
        details_id=f"more-{request.id}",
        details=details,
    )


def bin_card(
    item: InventoryItemOut,
    failed_order: tuple[uuid.UUID, int] | None,
    several: bool,
    history: "BinHistory",
) -> BinCard:
    """failed_order is (request id, units) of the bin's newest order when that order failed."""
    situation = bin_situation(item, failed_order is not None)
    units = None
    details = [("Facility", item.facility_id), ("Last event", item.last_event_id)]
    if item.open_request is not None:
        units = item.open_request.requested_quantity
        details.append(("Order on its way", str(item.open_request.id)))
    elif failed_order is not None:
        request_id, units = failed_order
        details.append(("Failed order", str(request_id)))
    return BinCard(
        sku=item.sku,
        room=room_label(item.room_id, item.facility_id, several),
        stock_now=item.current_quantity,
        should_hold=item.par_level,
        refill_at=item.reorder_point,
        situation=situation,
        note=situation.note.format(units=units),
        last_counted=item.last_observed_at,
        details_id=bin_details_id(item.facility_id, item.room_id, item.sku),
        details=details,
        history=history,
    )


# --- picking the words -----------------------------------------------------------------------


def bin_situation(item: InventoryItemOut, last_order_failed: bool) -> Situation:
    """open request, newest order failed, status, armed, stock >= par -> situation:
    yes - ok/low -> IN_STOCK/LOW _ORDER_ON_ITS_WAY; none yes ok/low -> *_ORDER_FAILED;
    none no ok -> IN_STOCK; none no low: not armed -> LOW_WAITING_FOR_REFILL, armed and
    full -> FULL, else LOW_NEXT_COUNT_ORDERS."""
    if item.open_request is not None:
        return IN_STOCK_ORDER_ON_ITS_WAY if item.status == "ok" else LOW_ORDER_ON_ITS_WAY
    if last_order_failed:
        return IN_STOCK_ORDER_FAILED if item.status == "ok" else LOW_ORDER_FAILED
    if item.status == "ok":
        return IN_STOCK
    if not item.armed:  # no recovery since its last (accepted) order
        return LOW_WAITING_FOR_REFILL
    if item.current_quantity >= item.par_level:
        return FULL
    # Armed, low, no open order: the low count arrived while an earlier order was open.
    return LOW_NEXT_COUNT_ORDERS


def order_stage(request: RequestOut, last_try: OutboundAttempt | None) -> Chip:
    if request.status == "in_flight":
        return SENDING
    if last_try is None or request.next_attempt_at is None:
        return QUEUED
    return RETRYING


def erp_answer(http_status: int | None) -> str:
    """What one try got back, by HTTP status; None means no answer (timeout, lost)."""
    if http_status is None:
        return NO_ANSWER
    if http_status == 429:
        return ERP_BUSY
    if http_status == 503:
        return ERP_UNAVAILABLE
    if 500 <= http_status < 600:
        return ERP_PROBLEM
    if 400 <= http_status < 500:
        return ERP_REFUSED
    return ERP_UNEXPECTED


def failure_chip(attempt: AttemptOut) -> Chip:
    if attempt.triggered_by == "manual":
        return RETRY_FAILED
    if attempt.request.last_error == "exhausted":
        return OUT_OF_TRIES
    return REFUSED_CHIP


def failure_reason(attempt: AttemptOut) -> str:
    if attempt.request.last_error == "exhausted":
        return NOT_CONFIRMED.format(tries=attempt.request.attempt_count)
    if attempt.http_status is not None and 400 <= attempt.http_status < 500:
        return REFUSED
    return UNEXPECTED


def failure_sentence(attempt: AttemptOut) -> str:
    answer = erp_answer(attempt.http_status)
    if attempt.request.last_error == "exhausted":
        return f"Stopped after {attempt.request.attempt_count} tries. Last answer: {answer}."
    return f"{answer}."


def next_step(attempt: AttemptOut) -> str:
    refused = attempt.http_status is not None and 400 <= attempt.http_status < 500
    if attempt.request.last_error != "exhausted" and refused:
        return FIX_THE_CAUSE
    return CHECK_THE_ERP


def erp_message(response_body: Any) -> str | None:
    message = response_body.get("message") if isinstance(response_body, dict) else None
    if not isinstance(message, str):
        return None
    # A stored lone surrogate (SQLite) made the whole page a 500; "replace" turns it into "?".
    return message.encode("utf-8", "replace").decode("utf-8")


def order_details(
    request: RequestOut, last_try: OutboundAttempt | AttemptOut | None
) -> list[tuple[str, str]]:
    """The ids an engineer searches the API and logs with, and the last try as stored."""
    details = [
        ("Request id", str(request.id)),
        ("Idempotency-Key", request.idempotency_key),
        ("Facility", request.facility_id),
        ("Created by event", request.triggering_event_id),
        ("Request status", request.status),
    ]
    if last_try is None:
        details.append(("Last try", "none yet"))
        return details
    details.append(
        (
            "Last try",
            f"try {last_try.attempt_no} (attempt id {last_try.id}, the id the Retry API takes): "
            f"{last_try.classification}",
        )
    )
    details.append(("Last error", last_try.error or "none"))
    details.append(("Last raw response", json.dumps(last_try.response_body)))  # ASCII-escaped
    return details


# --- data and labels ---------------------------------------------------------------------------


def last_attempts(
    session: Session, request_ids: list[uuid.UUID]
) -> dict[uuid.UUID, OutboundAttempt]:
    """The latest attempt of each request."""
    if not request_ids:
        return {}
    stmt = (
        select(OutboundAttempt)
        .where(OutboundAttempt.request_id.in_(request_ids))
        .order_by(OutboundAttempt.attempt_no, OutboundAttempt.id)
    )
    return {attempt.request_id: attempt for attempt in session.scalars(stmt)}


def failed_last_orders(session: Session) -> dict[tuple[str, str, str], tuple[uuid.UUID, int]]:
    """Bins whose newest request has failed, mapped to that request's (id, units)."""
    stmt = select(
        ReplenishmentRequest.facility_id,
        ReplenishmentRequest.room_id,
        ReplenishmentRequest.sku,
        ReplenishmentRequest.id,
        ReplenishmentRequest.requested_quantity,
    ).where(ReplenishmentRequest.status == "failed", newer_request_id().is_(None))
    return {
        (row.facility_id, row.room_id, row.sku): (row.id, row.requested_quantity)
        for row in session.execute(stmt)
    }


def room_label(room_id: str, facility_id: str, several: bool) -> str:
    return f"{room_id} · {facility_id}" if several else room_id


def bin_details_id(facility_id: str, room_id: str, sku: str) -> str:
    """A digest, since the ids may hold any character; stable from one refresh to the next."""
    key = "\x00".join((facility_id, room_id, sku)).encode("utf-8", "surrogatepass")
    return "more-bin-" + hashlib.sha256(key).hexdigest()[:16]


# --- a bin's recent history ----------------------------------------------------------------

CHART_WINDOW = timedelta(minutes=5)  # short enough that each count is a readable stair
HISTORY_WINDOW = timedelta(minutes=30)
HISTORY_COUNTS = 400  # counts read per bin (one every 10 s is 180)
HISTORY_LINES = 8
CHART_WIDTH = 240  # SVG units; the template scales it to the card
CHART_HEIGHT = 72
CHART_PAD = 4

# A plain count ("none") is in the chart only; the list keeps to what changed something.
COUNT_LINES = {
    "request_created": ("Counted {quantity}; ordered {units}", WAITING),
    "recovered": ("Counted {quantity}; refilled", GOOD),
    "blocked_open_request": ("Counted {quantity}; an order is already open", QUIET),
    "blocked_not_armed": ("Counted {quantity}; waiting for the refill", QUIET),
    "stale": ("Late count of {quantity}; ignored", QUIET),
}


@dataclass(frozen=True)
class Mark:  # a dot on the chart: an order, a refill, or (hollow) a late count
    x: float
    y: float
    colour: str
    hollow: bool = False


@dataclass(frozen=True)
class Tick:  # an ERP answer: a stroke under the chart
    x: float
    colour: str


@dataclass(frozen=True)
class HistoryLine:
    at: datetime
    text: str
    colour: str


@dataclass(frozen=True)
class BinHistory:
    minutes: int
    start: datetime
    end: datetime
    line: str  # SVG path of the stock as stairs; "" when there is nothing to draw
    area: str  # the same, closed down to zero, for the fill
    par_y: float
    refill_y: float
    marks: list[Mark]
    ticks: list[Tick]
    lines: list[HistoryLine]  # newest first
    details_id: str


def bin_history(session: Session, item: InventoryItemOut, now: datetime) -> BinHistory:
    """The last CHART_WINDOW of counts as a chart and the last HISTORY_WINDOW as a list. Both
    end now, or at the bin's last count when that is older than the window."""
    last = item.last_observed_at
    end = max(now, last) if last >= now - HISTORY_WINDOW else last
    start = end - HISTORY_WINDOW
    chart_start = end - CHART_WINDOW
    same_bin = (
        Observation.facility_id == item.facility_id,
        Observation.room_id == item.room_id,
        Observation.sku == item.sku,
    )
    counts = session.execute(
        select(Observation, ReplenishmentRequest.requested_quantity)
        .outerjoin(
            ReplenishmentRequest, ReplenishmentRequest.triggering_event_id == Observation.event_id
        )
        .where(*same_bin, Observation.observed_at >= start, Observation.observed_at <= end)
        .order_by(Observation.observed_at.desc(), Observation.event_id.desc())
        .limit(HISTORY_COUNTS)
    ).all()
    counts.reverse()
    # The stock at the chart's left edge: the newest applied count before it.
    before = None
    for count, _ in counts:
        if count.observed_at < chart_start and count.applied:
            before = count.observed_quantity
    if before is None and len(counts) < HISTORY_COUNTS:
        before = session.scalar(
            select(Observation.observed_quantity)
            .where(*same_bin, Observation.applied, Observation.observed_at < start)
            .order_by(Observation.observed_at.desc(), Observation.event_id.desc())
            .limit(1)
        )
    answers = session.execute(
        select(OutboundAttempt, ReplenishmentRequest)
        .join(ReplenishmentRequest, OutboundAttempt.request_id == ReplenishmentRequest.id)
        .where(
            ReplenishmentRequest.facility_id == item.facility_id,
            ReplenishmentRequest.room_id == item.room_id,
            ReplenishmentRequest.sku == item.sku,
            OutboundAttempt.classification != "in_progress",
            OutboundAttempt.finished_at >= start,
            OutboundAttempt.finished_at <= end,
        )
        .order_by(OutboundAttempt.finished_at, OutboundAttempt.id)
    ).all()

    in_chart = [(count, units) for count, units in counts if count.observed_at >= chart_start]
    quantities = [count.observed_quantity for count, _ in in_chart]
    top = max([item.par_level, 1, *quantities, *([before] if before is not None else [])])

    def x_of(moment: datetime) -> float:
        share = (moment - chart_start) / CHART_WINDOW
        return round(CHART_WIDTH * min(max(share, 0.0), 1.0), 1)

    def y_of(quantity: int) -> float:
        return round(CHART_PAD + (CHART_HEIGHT - 2 * CHART_PAD) * (1 - quantity / top), 1)

    steps = [(0.0, y_of(before))] if before is not None else []
    marks = []
    for count, _ in in_chart:
        x, y = x_of(count.observed_at), y_of(count.observed_quantity)
        if count.applied:
            steps.append((x, y))
        if count.decision == "request_created":
            marks.append(Mark(x, y, WAITING))
        elif count.decision == "recovered":
            marks.append(Mark(x, y, GOOD))
        elif count.decision == "stale":
            marks.append(Mark(x, y, QUIET, hollow=True))
    line = area = ""
    if steps:
        x0, y0 = steps[0]
        # A tread to each next count, a riser to it, and the last count holds to the edge.
        line = f"M{x0} {y0}" + "".join(f"H{x}V{y}" for x, y in steps[1:]) + f"H{CHART_WIDTH}"
        area = f"{line}V{y_of(0)}H{x0}Z"

    lines = []
    for count, units in counts:
        if count.decision in COUNT_LINES:
            text, colour = COUNT_LINES[count.decision]
            words = text.format(quantity=count.observed_quantity, units=units or "more")
            lines.append(HistoryLine(count.observed_at, words, colour))
    ticks = []
    for attempt, request in answers:
        prefix = "Retry: " if attempt.triggered_by == "manual" else ""
        if attempt.classification == "success":
            order = f" ({request.erp_order_id})" if request.erp_order_id else ""
            text, colour = f"{prefix}ERP took the order{order}", GOOD
        elif attempt.classification == "lost":
            text, colour = f"{prefix}Try {attempt.attempt_no} lost; sent again", QUIET
        else:
            answer = erp_answer(attempt.http_status)
            refused = attempt.classification == "non_retryable"
            gave_up = request.status == "failed" and attempt.attempt_no == request.attempt_count
            if refused or gave_up:
                text, colour = f"{prefix}{answer}; order failed", ATTENTION
            else:
                text, colour = f"{prefix}{answer}; will try again", WAITING
        lines.append(HistoryLine(attempt.finished_at, text, colour))
        if attempt.finished_at >= chart_start:
            ticks.append(Tick(x_of(attempt.finished_at), colour))
    lines.sort(key=lambda entry: entry.at, reverse=True)

    return BinHistory(
        minutes=int(CHART_WINDOW.total_seconds() // 60),
        start=chart_start,
        end=end,
        line=line,
        area=area,
        par_y=y_of(item.par_level),
        refill_y=y_of(item.reorder_point),
        marks=marks,
        ticks=ticks,
        lines=lines[:HISTORY_LINES],
        details_id=bin_details_id(item.facility_id, item.room_id, item.sku).replace(
            "more-bin-", "history-bin-"
        ),
    )
