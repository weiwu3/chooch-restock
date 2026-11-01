"""The delivery worker: a thread that sends replenishment orders to the ERP.

One pass (run_once): requeue claims that outlived their lease, then for each due request:
claim it (one conditional UPDATE, plus the attempt row written before the call), POST the
order with Idempotency-Key: replenishment:<request_id>, classify the answer, and record it
behind a fence so a late answer cannot overwrite a newer attempt's state.

    ERP answer                                   classification   the request becomes
    2xx whose JSON names the order (erp_order_id) success          delivered
    any other 2xx (no body, HTML, no usable id)   ambiguous        pending, retried, same key
    429 or 5xx                                    retryable        pending, retried after a wait
    timeout or transport error                    ambiguous        pending, retried, same key
    other status (4xx, 1xx, 3xx)                  non_retryable    failed at once

A retryable or ambiguous result on attempt ERP_MAX_ATTEMPTS fails the request ("exhausted").
"""

import logging
import random
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any

import httpx
from sqlalchemy import and_, select, update
from sqlalchemy.orm import Session, sessionmaker

from restock import runtime
from restock.runtime import Settings, utcnow
from restock.schemas import (
    Observation,
    OutboundAttempt,
    ReplenishmentRequest,
    is_ascii_digits,
    is_utf8,
)

log = logging.getLogger("restock.outbound")

POLL_SECONDS = 0.5
BATCH_SIZE = 10  # requests claimed per pass
STOP_GRACE_SECONDS = 1.0  # stop() waits one ERP timeout plus this for a call in progress
STALL_SECONDS = max(5.0, 4 * POLL_SECONDS)  # readiness says "stalled" after this without progress
ERP_ORDER_ID_MAX = 128  # the column's width
RAW_BODY_MAX = 2000  # characters kept of a non-JSON answer
STORED_TEXT_MAX = 500
LOST_ERROR = "claim expired before a result was recorded"
NO_ORDER_ID_ERROR = "2xx without erp_order_id"


def _erp_order_id(body: Any) -> str | None:
    """The answer's erp_order_id, if it is one the database can store as proof of the order."""
    if not isinstance(body, dict):
        return None
    value = body.get("erp_order_id")
    if not isinstance(value, str) or not value or len(value) > ERP_ORDER_ID_MAX:
        return None
    if "\x00" in value or not is_utf8(value):
        return None
    return value


def classify(status_code: int, body: Any = None) -> str:
    if 200 <= status_code < 300:
        # A 2xx proves the ERP holds the order only when it names it; so a misrouted ERP_URL
        # (a proxy's 200 page) cannot mark orders delivered.
        return "success" if _erp_order_id(body) is not None else "ambiguous"
    if status_code == 429 or 500 <= status_code < 600:
        return "retryable"
    return "non_retryable"


def parse_retry_after(value: str | None, now: datetime) -> float | None:
    """Seconds a Retry-After header asks for (a number or an HTTP-date); None if unreadable."""
    if not value:
        return None
    value = value.strip()
    if is_ascii_digits(value):
        return float(value)  # a huge value becomes inf; retry_delay caps it
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return max(0.0, (when - now).total_seconds())


def backoff_delay(
    attempt_no: int, base: float, cap: float, rand: Callable[[], float] = random.random
) -> float:
    """base * 2^(n-1), capped, with equal jitter: half fixed, half random."""
    longest = min(cap, base * 2 ** (attempt_no - 1))
    return longest / 2 + rand() * longest / 2


def retry_delay(attempt_no: int, retry_after: float | None, settings: Settings) -> float:
    """The backoff, unless the ERP's Retry-After is longer; bounded both ways: "0" would burn
    every attempt in milliseconds, and one "86400" would park the order for a day."""
    delay = backoff_delay(
        attempt_no, settings.erp_backoff_base_seconds, settings.erp_backoff_cap_seconds
    )
    if retry_after is not None:
        delay = max(delay, min(retry_after, settings.erp_retry_after_max_seconds))
    return delay


def order_body(request: ReplenishmentRequest) -> dict[str, Any]:
    """Built from stored columns only, so every attempt sends byte-for-byte the same order."""
    return {
        "request_id": str(request.id),
        "facility_id": request.facility_id,
        "room_id": request.room_id,
        "sku": request.sku,
        "quantity": request.requested_quantity,
        "par_level": request.par_level,
        "reorder_point": request.reorder_point,
        "observed_quantity": request.observed_quantity,
        "triggering_event_id": request.triggering_event_id,
        "requested_at": request.created_at.isoformat().replace("+00:00", "Z"),
    }


def _storable_text(text: str) -> str:
    """No NUL, no unpaired surrogates, at most STORED_TEXT_MAX characters."""
    return text.replace("\x00", "").encode("utf-8", "replace").decode("utf-8")[:STORED_TEXT_MAX]


@dataclass(frozen=True)
class Claim:
    """A won claim: what the call and its record need, read in the claim's transaction."""

    request_id: uuid.UUID
    attempt_id: int
    attempt_no: int  # fences the result (see _record)
    idempotency_key: str
    body: dict[str, Any]
    correlation_id: str
    event_id: str
    triggered_by: str  # "manual" when an operator's Retry asked for this attempt
    manual_retry_correlation_id: str | None


@dataclass(frozen=True)
class AttemptResult:
    classification: str
    http_status: int | None = None
    response_body: Any = None
    error: str | None = None
    retry_after: float | None = None
    erp_order_id: str | None = None


class OutboundWorker:
    """Sends due requests one at a time from a background thread. Several workers may share the
    database: the claim UPDATE takes a request only while it is pending and due, and the fence in
    _record keeps a late result from deciding for a newer claim."""

    def __init__(
        self,
        sessions: sessionmaker[Session],
        settings: Settings,
        http_client: httpx.Client | None = None,
    ) -> None:
        self._sessions = sessions
        self._settings = settings
        self._http = http_client or httpx.Client(timeout=settings.erp_timeout_seconds)
        self._owns_http = http_client is None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_tick_error: str | None = None
        self._heartbeat = time.monotonic()  # readiness reads it
        self._in_call = False
        # How long a claim may hold its request: one httpx timeout for each of the four parts
        # of a call, plus 30 s for the database writes around it (SQLite's busy timeout).
        self.lease_seconds = 4 * settings.erp_timeout_seconds + 30

    def start(self) -> None:
        self._stop.clear()
        self._beat()
        self._thread = threading.Thread(target=self._run, name="outbound-worker", daemon=True)
        self._thread.start()
        log.info("outbound worker started", extra={"erp_url": self._settings.erp_url})

    def stop(self) -> bool:
        """Stop claiming and wait for a call in progress for one ERP timeout plus a second.
        False if it was still running then; the lease takes care of it after a restart."""
        self._stop.set()
        if self._thread is None:
            if self._owns_http:
                self._http.close()
            return True
        self._thread.join(timeout=self._settings.erp_timeout_seconds + STOP_GRACE_SECONDS)
        if self._thread.is_alive():
            log.warning("outbound worker stopped waiting for an ERP call still in progress")
            return False
        log.info("outbound worker stopped")
        return True

    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def status(self) -> str:
        """For readiness: stopped, stalled (no progress for STALL_SECONDS; a call in progress
        counts as progress until its lease runs out), or ok."""
        if not self.is_alive():
            return "stopped"
        allowed = self.lease_seconds if self._in_call else STALL_SECONDS
        return "ok" if time.monotonic() - self._heartbeat <= allowed else "stalled"

    def _beat(self, in_call: bool = False) -> None:
        self._heartbeat = time.monotonic()
        self._in_call = in_call

    def _run(self) -> None:
        delay = POLL_SECONDS  # the first wait also lets uvicorn bind the in-app mock's port
        try:
            while not self._stop.wait(delay):
                delay = 0 if self._tick() else POLL_SECONDS  # no sleep while there is work
        finally:
            if self._owns_http:  # closed by the thread, never under a running call
                self._http.close()

    def _tick(self) -> bool:
        """One pass that outlives a database hiccup; True if it attempted anything. Only a
        completed pass refreshes the heartbeat."""
        try:
            busy = self.run_once() > 0
        except Exception as exc:
            if repr(exc) != self._last_tick_error:  # each distinct error logged once
                log.exception("outbound worker tick failed")
                self._last_tick_error = repr(exc)
            return False
        self._last_tick_error = None
        self._beat()
        return busy

    def run_once(self) -> int:
        """One pass; returns the number of attempts made. Tests call it directly."""
        self._requeue_expired_claims()
        attempted = 0
        for request_id in self._due_request_ids():
            if self._stop.is_set():
                break
            claim = self._claim(request_id)
            if claim is not None:
                self._deliver(claim)
                attempted += 1
        return attempted

    def _due_request_ids(self) -> list[uuid.UUID]:
        """Up to BATCH_SIZE pending requests that are due, most overdue first; _claim checks
        each again before anything is sent."""
        with self._sessions() as session:
            return list(
                session.scalars(
                    select(ReplenishmentRequest.id)
                    .where(
                        ReplenishmentRequest.status == "pending",
                        ReplenishmentRequest.next_attempt_at <= utcnow(),
                    )
                    .order_by(ReplenishmentRequest.next_attempt_at)
                    .limit(BATCH_SIZE)
                )
            )

    def _requeue_expired_claims(self) -> None:
        """A request still in_flight after the lease has no recorded result (process died,
        database down, or the call still running): back to pending, due now, sent again with
        the same key; its attempt row becomes lost. A lost attempt never fails a request."""
        now = utcnow()
        cutoff = now - timedelta(seconds=self.lease_seconds)
        with self._sessions.begin() as session:
            # An in_flight request's updated_at is its claim time.
            requeued = set(
                session.scalars(
                    update(ReplenishmentRequest)
                    .where(
                        ReplenishmentRequest.status == "in_flight",
                        ReplenishmentRequest.updated_at < cutoff,
                    )
                    .values(status="pending", next_attempt_at=now, updated_at=now)
                    .returning(ReplenishmentRequest.id)
                    .execution_options(synchronize_session=False)
                )
            )
            open_rows = session.execute(
                select(
                    OutboundAttempt.id,
                    OutboundAttempt.request_id,
                    OutboundAttempt.attempt_no,
                    OutboundAttempt.correlation_id,
                    OutboundAttempt.started_at,
                    ReplenishmentRequest.triggering_event_id,
                )
                .join(ReplenishmentRequest, ReplenishmentRequest.id == OutboundAttempt.request_id)
                .where(
                    OutboundAttempt.classification == "in_progress",
                    OutboundAttempt.started_at < cutoff,
                )
            ).all()
            lost = []
            for row in open_rows:
                # The in_progress condition leaves alone a row whose answer arrived meanwhile.
                changed = session.execute(
                    update(OutboundAttempt)
                    .where(
                        OutboundAttempt.id == row.id,
                        OutboundAttempt.classification == "in_progress",
                    )
                    .values(
                        classification="lost",
                        error=LOST_ERROR,
                        finished_at=now,
                        duration_ms=int((now - row.started_at).total_seconds() * 1000),
                    )
                    .execution_options(synchronize_session=False)
                ).rowcount
                if changed == 1:
                    lost.append(row)
        for row in lost:  # logged after the commit
            log.warning(
                "claim expired before a result was recorded; attempt marked lost",
                extra={
                    "replenishment_id": str(row.request_id),
                    "correlation_id": row.correlation_id,
                    "event_id": row.triggering_event_id,
                    "attempt_no": row.attempt_no,
                    "requeued": row.request_id in requeued,  # False if a late success delivered it
                },
            )

    def _claim(self, request_id: uuid.UUID) -> Claim | None:
        """Take one due request and write its attempt row, in one transaction. None if it is
        no longer pending and due (another worker took it, or a 429 pushed it back)."""
        now = utcnow()
        with self._sessions.begin() as session:
            # Of two workers racing, only one UPDATE matches. attempt_count + 1 is this
            # claim's attempt number; updated_at is the claim time the lease counts from.
            matched = session.execute(
                update(ReplenishmentRequest)
                .where(
                    ReplenishmentRequest.id == request_id,
                    ReplenishmentRequest.status == "pending",
                    ReplenishmentRequest.next_attempt_at <= now,
                )
                .values(
                    status="in_flight",
                    attempt_count=ReplenishmentRequest.attempt_count + 1,
                    updated_at=now,
                )
                .execution_options(synchronize_session=False)
            ).rowcount
            if matched != 1:
                return None
            request = session.get_one(ReplenishmentRequest, request_id)
            correlation_id = session.scalars(
                select(Observation.correlation_id).where(
                    Observation.event_id == request.triggering_event_id
                )
            ).one()
            body = order_body(request)
            # The row is written before the call, so a process that dies mid-call leaves one.
            attempt = OutboundAttempt(
                request_id=request.id,
                attempt_no=request.attempt_count,
                idempotency_key=request.idempotency_key,
                request_body=body,
                classification="in_progress",
                triggered_by="manual" if request.manual_retry_at else "worker",
                correlation_id=correlation_id,
                manual_retry_correlation_id=request.manual_retry_correlation_id,
                started_at=now,
                finished_at=now,  # NOT NULL; completed by _record
                duration_ms=0,
            )
            session.add(attempt)
            session.flush()
            return Claim(
                request_id=request.id,
                attempt_id=attempt.id,
                attempt_no=attempt.attempt_no,
                idempotency_key=attempt.idempotency_key,
                body=body,
                correlation_id=correlation_id,
                event_id=request.triggering_event_id,
                triggered_by=attempt.triggered_by,
                manual_retry_correlation_id=attempt.manual_retry_correlation_id,
            )

    def _deliver(self, claim: Claim) -> None:
        """Send one claimed attempt and record its result. Nothing raises out of here."""
        token = runtime.bind(
            correlation_id=claim.correlation_id,
            event_id=claim.event_id,
            replenishment_id=str(claim.request_id),
            attempt_no=claim.attempt_no,
            manual_retry_correlation_id=claim.manual_retry_correlation_id,
        )
        try:
            started = time.monotonic()
            self._beat(in_call=True)
            result = self._send(claim)
            self._beat()
            duration_ms = int((time.monotonic() - started) * 1000)
            try:
                self._record(claim, result, duration_ms)
            except Exception as exc:
                # Usually the database refused something the ERP sent (NUL, NaN, a lone
                # surrogate): record the same outcome without the ERP's data.
                log.exception("could not record the attempt; recording it without the response")
                driver_error = getattr(exc, "orig", None)
                note = f"response not stored ({type(driver_error or exc).__name__})"
                order_id = result.erp_order_id
                if order_id is not None and _storable_text(order_id) != order_id:
                    order_id = None
                stripped = AttemptResult(
                    classification=result.classification,
                    http_status=result.http_status,
                    error=f"{_storable_text(result.error)}; {note}" if result.error else note,
                    erp_order_id=order_id,
                )
                self._record(claim, stripped, duration_ms)
        except Exception:
            # The database is down: the request stays in_flight until the lease marks this
            # attempt lost and the request is sent again.
            log.exception(
                "could not record the attempt; it is marked lost and resent when the claim expires"
            )
        finally:
            runtime.reset(token)

    def _send(self, claim: Claim) -> AttemptResult:
        """POST the order and classify the answer. Never raises."""
        url = f"{self._settings.erp_url.rstrip('/')}/orders"
        headers = {
            "Idempotency-Key": claim.idempotency_key,
            "X-Correlation-ID": claim.correlation_id,
        }
        try:
            response = self._http.post(url, json=claim.body, headers=headers)
        except httpx.TimeoutException as exc:
            # The ERP may have accepted the order anyway: ambiguous, retried with the same key.
            timeout = self._settings.erp_timeout_seconds
            return AttemptResult(
                "ambiguous", error=f"timeout after {timeout:g}s ({type(exc).__name__})"
            )
        except httpx.HTTPError as exc:
            return AttemptResult("ambiguous", error=f"{type(exc).__name__}: {exc}")
        except Exception as exc:
            log.exception("unexpected error while calling the ERP")
            return AttemptResult("ambiguous", error=f"{type(exc).__name__}: {exc}")
        try:
            try:
                body = response.json()
            except (ValueError, RecursionError):
                body = {"raw": response.text[:RAW_BODY_MAX]} if response.text else None
            classification = classify(response.status_code, body)
            if classification == "success":
                error = None
            elif 200 <= response.status_code < 300:
                error = NO_ORDER_ID_ERROR
            else:
                error = f"HTTP {response.status_code}"
                detail = body.get("error") if isinstance(body, dict) else None
                if detail:
                    error += f": {detail}"
            return AttemptResult(
                classification=classification,
                http_status=response.status_code,
                response_body=body,
                error=error,
                retry_after=parse_retry_after(response.headers.get("Retry-After"), utcnow()),
                erp_order_id=_erp_order_id(body),
            )
        except Exception as exc:  # the ERP did answer, so the status code still classifies it
            log.exception("could not read the ERP response")
            return AttemptResult(
                classification=classify(response.status_code),
                http_status=response.status_code,
                error=f"HTTP {response.status_code}: unreadable response ({type(exc).__name__})",
            )

    def _record(self, claim: Claim, result: AttemptResult, duration_ms: int) -> None:
        """Complete the attempt row and, if the fence allows, write the request's next state,
        in one transaction. Raises if the database refuses the write."""
        now = utcnow()
        values: dict[str, Any] = {
            "manual_retry_at": None,  # an operator's Retry markers were for this attempt
            "manual_retry_correlation_id": None,
            "updated_at": now,
        }
        next_in = None
        if result.classification == "success":
            values |= {
                "status": "delivered",
                "erp_order_id": result.erp_order_id,
                "next_attempt_at": None,
                "last_error": None,
            }
        elif result.classification == "non_retryable":
            values |= {"status": "failed", "next_attempt_at": None, "last_error": result.error}
        elif claim.attempt_no >= self._settings.erp_max_attempts:
            values |= {"status": "failed", "next_attempt_at": None, "last_error": "exhausted"}
        else:
            next_in = retry_delay(claim.attempt_no, result.retry_after, self._settings)
            values |= {
                "status": "pending",
                "next_attempt_at": now + timedelta(seconds=next_in),
                "last_error": result.error,
            }

        # The fence. A success always delivers the request (every claim sent the same order
        # under the same key, so a 2xx naming it means the ERP holds it). Any other result
        # decides only while this claim still holds the request: in_flight with this attempt
        # number. A claim can lose its request when a slow call outlives the lease.
        if result.classification == "success":
            fence = ReplenishmentRequest.status != "delivered"
        else:
            fence = and_(
                ReplenishmentRequest.status == "in_flight",
                ReplenishmentRequest.attempt_count == claim.attempt_no,
            )
        refused_state = None
        with self._sessions.begin() as session:
            session.execute(  # even if the row was marked lost meanwhile: the answer is worth more
                update(OutboundAttempt)
                .where(OutboundAttempt.id == claim.attempt_id)
                .values(
                    http_status=result.http_status,
                    response_body=result.response_body,
                    classification=result.classification,
                    error=result.error,
                    finished_at=now,
                    duration_ms=duration_ms,
                )
                .execution_options(synchronize_session=False)
            )
            passed = (
                session.execute(
                    update(ReplenishmentRequest)
                    .where(ReplenishmentRequest.id == claim.request_id, fence)
                    .values(**values)
                    .execution_options(synchronize_session=False)
                ).rowcount
                == 1
            )
            if not passed:
                refused_state = session.execute(
                    select(ReplenishmentRequest.status, ReplenishmentRequest.attempt_count).where(
                        ReplenishmentRequest.id == claim.request_id
                    )
                ).one()
        if refused_state is not None:  # logged after the commit
            if refused_state.status == "delivered":
                log.info("the request was already delivered; recorded only")
            elif refused_state.attempt_count != claim.attempt_no:
                log.warning("the request was claimed again during this attempt; recorded only")
            else:
                log.warning("the claim expired during this attempt and was requeued; recorded only")
        log.info(
            "erp attempt finished",
            extra={
                "classification": result.classification,
                "http_status": result.http_status,
                "duration_ms": duration_ms,
                "request_status": values["status"] if passed else None,
                "next_attempt_in_s": round(next_in, 3) if next_in is not None else None,
                "triggered_by": claim.triggered_by,
            },
        )
