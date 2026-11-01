"""The mock ERP, mounted in the app at /mock-erp (the default ERP_URL), and the demo's ERP
simulator: the same mock alone in its own app, with a fault schedule and a status page.

Each POST /mock-erp/orders answers as one scenario of fixtures/outbound/erp_scenarios.json
says: the X-Mock-Scenario header if sent, else the next name queued by POST /mock-erp/_control,
else unqueued_scenario() ("success"; the simulator draws from FAULTS instead). Error scenarios
answer before the key lookup, like a rate limiter in front of the ERP. Accepted orders are
remembered by Idempotency-Key in memory (ERP-7001, ERP-7002, ...): a repeated key gets the
same id back with "duplicate": true.
"""

import asyncio
import html
import json
import logging
import os
import random
import threading
from collections import deque
from pathlib import Path
from typing import Any

from fastapi import APIRouter, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from starlette.exceptions import HTTPException as StarletteHTTPException

from restock.runtime import REPO_ROOT, utcnow
from restock.schemas import unstorable

log = logging.getLogger("restock.erp_sim")

SCENARIOS_PATH = REPO_ROOT / "fixtures" / "outbound" / "erp_scenarios.json"
ACCEPTING = {"success", "duplicate_idempotency_key", "timeout_after_accept"}
RECEIVED_MAX = 200  # calls kept in the received list
RECEIVED_BODY_MAX = 16_384  # bytes; a larger body is kept as null

router = APIRouter(prefix="/mock-erp", tags=["mock-erp"])


class MockErp:
    """The mock's state; one lock, since the orders route runs on the event loop and the
    control routes in worker threads."""

    def __init__(self, scenarios_path: Path, client_timeout_seconds: float) -> None:
        loaded = json.loads(scenarios_path.read_text())["scenarios"]
        self.scenarios: dict[str, dict[str, Any]] = {s["name"]: s for s in loaded}
        self.client_timeout_seconds = client_timeout_seconds  # the worker's ERP timeout
        self._lock = threading.Lock()
        self.queue: deque[str] = deque()
        self.orders: dict[str, str] = {}  # Idempotency-Key -> erp_order_id
        self.received: deque[dict[str, Any]] = deque(maxlen=RECEIVED_MAX)
        self._next_order_no = 7001

    def set_queue(self, names: list[str]) -> None:
        with self._lock:
            self.queue = deque(names)

    def next_scenario(self, header: str | None) -> str:
        with self._lock:
            if header:
                return header
            if self.queue:
                return self.queue.popleft()
            return self.unqueued_scenario()

    def unqueued_scenario(self) -> str:
        return "success"

    def accept(self, key: str) -> tuple[str, bool]:
        """(erp_order_id, was_already_known) for an order under this key."""
        with self._lock:
            if key in self.orders:
                return self.orders[key], True
            order_id = f"ERP-{self._next_order_no}"
            self._next_order_no += 1
            self.orders[key] = order_id
            return order_id, False

    def record(self, entry: dict[str, Any]) -> None:
        with self._lock:
            self.received.append(entry)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "queue": list(self.queue),
                "orders": dict(self.orders),
                "received": list(self.received),
            }


class ControlIn(BaseModel):
    scenarios: list[str]


@router.post("/orders")
async def create_order(request: Request) -> JSONResponse:
    # Async: timeout_after_accept awaits asyncio.sleep without holding a worker thread.
    mock: MockErp = request.app.state.mock_erp
    key = request.headers.get("Idempotency-Key")
    name = mock.next_scenario(request.headers.get("X-Mock-Scenario"))
    raw = await request.body()
    try:
        order = json.loads(raw)
        valid_json = True
    except ValueError:
        order = None
        valid_json = False
    # Keep only what GET /_control can encode: a lone surrogate or JSON nested hundreds deep
    # made every later read of the control view a 500.
    if len(raw) > RECEIVED_BODY_MAX:
        kept = None
    elif (problem := unstorable(order)) is not None:
        kept = {"_unstorable": problem}
    else:
        kept = order
    mock.record(
        {
            "at": utcnow().isoformat(),
            "scenario": name,
            "idempotency_key": key,
            "correlation_id": request.headers.get("X-Correlation-ID"),
            "body": kept,
        }
    )
    scenario = mock.scenarios.get(name)
    if scenario is None:
        return JSONResponse({"error": "unknown_scenario", "scenario": name}, status_code=400)
    if not key:
        return JSONResponse({"error": "missing_idempotency_key"}, status_code=400)
    if not valid_json:
        return JSONResponse({"error": "invalid_json"}, status_code=400)
    if name not in ACCEPTING:
        return JSONResponse(
            scenario["response_body"],
            status_code=scenario["http_status"],
            headers=scenario.get("response_headers"),
        )
    order_id, known = mock.accept(key)
    if name == "timeout_after_accept":
        # The order is stored; the answer is held past the client's timeout.
        await asyncio.sleep(mock.client_timeout_seconds + 1)
        return JSONResponse({"erp_order_id": order_id, "status": "accepted", "duplicate": known})
    template = mock.scenarios[
        "duplicate_idempotency_key" if known or name == "duplicate_idempotency_key" else "success"
    ]
    body = dict(template["response_body"])
    body["erp_order_id"] = order_id  # the fixture's id is only a sample
    return JSONResponse(body, status_code=template["http_status"])


@router.post("/_control")
def set_control(payload: ControlIn, request: Request) -> JSONResponse:
    """Replace the queue of scenarios for the next calls; an unknown name is a 422."""
    mock: MockErp = request.app.state.mock_erp
    unknown = [name for name in payload.scenarios if name not in mock.scenarios]
    if unknown:
        body = {"error": "unknown_scenario", "unknown": unknown, "known": sorted(mock.scenarios)}
        return JSONResponse(body, status_code=422)
    mock.set_queue(payload.scenarios)
    return JSONResponse({"queue": payload.scenarios})


@router.get("/_control")
def get_control(request: Request) -> dict[str, Any]:
    return request.app.state.mock_erp.snapshot()


# --- the demo's ERP simulator: `restock erp-sim --port 8090` -----------------------------------
# The service runs with ERP_MOCK_ENABLED=false and ERP_URL pointing here, so orders go to a
# separate process like a real ERP. Unqueued calls draw from FAULTS: success with probability
# 1 - rate, else the faults split the rate 4:3:2:1.

DEFAULT_FAULT_RATE = 0.2
FAULTS = {
    "rate_limited": 0.4,
    "temporary_failure": 0.3,
    "timeout_after_accept": 0.2,
    "permanent_rejection": 0.1,
}
STATUS_ROWS = 50
ANSWERS = {
    "success": ("Took the order", 201),
    "duplicate_idempotency_key": ("Took the order (answered as a repeat)", 200),
    "rate_limited": ("Too busy: asked to try again in 2 seconds", 429),
    "temporary_failure": ("Down for a moment", 503),
    "timeout_after_accept": ("Took the order but answered too late", None),
    "permanent_rejection": ("Refused the order", 422),
}
KNOWN = ("Already had this order: sent the same order number back", 200)
KNOWN_LATE = ("Already had this order but answered too late", None)


def draw_scenario(rng: random.Random, fault_rate: float) -> str:
    names = ["success", *FAULTS]
    weights = [1 - fault_rate, *(share * fault_rate for share in FAULTS.values())]
    return rng.choices(names, weights)[0]


def fault_rate_from_env() -> float:
    raw = os.environ.get("ERP_SIM_FAULT_RATE")
    return DEFAULT_FAULT_RATE if raw in (None, "") else float(raw)


class SimErp(MockErp):
    """The mock with the fault schedule, and a note on each received call of what it answered."""

    def __init__(
        self, client_timeout_seconds: float, fault_rate: float, rng: random.Random
    ) -> None:
        if not 0 <= fault_rate <= 1:
            raise ValueError(f"the fault rate must be between 0 and 1, not {fault_rate}")
        super().__init__(SCENARIOS_PATH, client_timeout_seconds)
        self.fault_rate = fault_rate
        self._rng = rng

    def unqueued_scenario(self) -> str:
        return draw_scenario(self._rng, self.fault_rate)

    def record(self, entry: dict[str, Any]) -> None:
        scenario, key = entry["scenario"], entry["idempotency_key"]
        known = key in self.orders  # recorded before the answer, so the key is not stored yet
        if scenario not in ANSWERS:
            answer, status = "Unknown test case", 400
        elif not key:
            answer, status = "Refused: the order had no order key", 400
        elif known and scenario in ("success", "duplicate_idempotency_key"):
            answer, status = KNOWN
        elif known and scenario == "timeout_after_accept":
            answer, status = KNOWN_LATE
        else:
            answer, status = ANSWERS[scenario]
        log.info(
            "erp sim call",
            extra={
                "scenario": scenario,
                "http_status": status,
                "idempotency_key": key,
                "correlation_id": entry["correlation_id"],
            },
        )
        super().record({**entry, "answer": answer, "http_status": status})


def create_erp_sim_app(
    fault_rate: float = DEFAULT_FAULT_RATE,
    client_timeout_seconds: float = 5.0,
    seed: int | None = None,
) -> FastAPI:
    from restock.main import _http_error, _request_validation_error  # the service's error envelope

    app = FastAPI(title="restock erp-sim", version="0.1.0", summary="The demo's ERP.")
    app.state.mock_erp = SimErp(client_timeout_seconds, fault_rate, random.Random(seed))
    app.add_exception_handler(RequestValidationError, _request_validation_error)
    app.add_exception_handler(StarletteHTTPException, _http_error)
    app.include_router(router)
    app.add_api_route("/", status_page, methods=["GET"], response_class=HTMLResponse)
    return app


def status_page(request: Request) -> HTMLResponse:
    """GET /: the orders held, the fault rate and the last 50 calls; reloads every 2 s."""
    mock: SimErp = request.app.state.mock_erp
    state = mock.snapshot()
    e = html.escape
    rows = []
    for call in reversed(state["received"][-STATUS_ROWS:]):
        body = call["body"] if isinstance(call["body"], dict) else {}
        what = " ".join(str(body.get(k, "")) for k in ("room_id", "sku")).strip() or "?"
        if body.get("quantity") is not None:
            what += f", {body['quantity']} units"
        took = call["scenario"] in ACCEPTING
        number = state["orders"].get(call["idempotency_key"] or "", "") if took else ""
        status = call.get("http_status")
        rows.append(
            "<tr>"
            f"<td>{e(call['at'][11:19])}</td><td>{e(what)}</td>"
            f"<td>{e(call.get('answer', call['scenario']))}</td>"
            f"<td>{status if status is not None else 'none'}</td><td>{e(number)}</td>"
            "</tr>"
        )
    page = STATUS_TEMPLATE.format(
        held=len(state["orders"]),
        rate=round(mock.fault_rate * 100),
        queue=e(", ".join(state["queue"]) or "none"),
        rows="\n".join(rows) or '<tr><td colspan="5">No orders yet.</td></tr>',
    )
    return HTMLResponse(page)


STATUS_TEMPLATE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="2">
<title>Simulated ERP</title>
<style>
body {{ font: 15px/1.4 system-ui, sans-serif; margin: 16px; color: #1d2733; background: #fff; }}
table {{ border-collapse: collapse; width: 100%; max-width: 980px; }}
th, td {{ text-align: left; padding: 4px 8px; border-bottom: 1px solid #e3e7ec; }}
th {{ font-weight: 600; }}
.numbers {{ display: flex; gap: 32px; margin: 12px 0 20px; flex-wrap: wrap; }}
.numbers b {{ font-size: 24px; display: block; }}
</style></head>
<body>
<h1>Simulated ERP</h1>
<p>This stands in for the company's ordering system. The restock service sends its refill
orders here. Now and then it answers badly on purpose, so you can watch the service cope.</p>
<div class="numbers">
<div><b>{held}</b>orders held</div>
<div><b>{rate}%</b>of calls answered badly on purpose</div>
<div><b>{queue}</b>test answers waiting</div>
</div>
<h2>Last 50 calls, newest first (times in UTC)</h2>
<table><thead><tr><th>Time</th><th>Order</th><th>What the ERP did</th><th>Code</th>
<th>Order number</th></tr></thead>
<tbody>
{rows}
</tbody></table>
</body></html>
"""
