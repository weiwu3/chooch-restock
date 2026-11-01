#!/usr/bin/env python3
"""Play a facility gateway for the living demo: bins whose stock goes down, a signed count for
each bin every round, and now and then a deliberate mistake. Standard library only.

    export WEBHOOK_SECRET=assessment-secret
    python scripts/gateway_sim.py --url http://127.0.0.1:8000 --port 8081

It only talks HTTP to the service: it posts counts to POST /api/v1/inventory-events like the
fixtures, and reads GET /api/v1/outbound-attempts to learn when an order it caused was
delivered; the refill then reaches the room one or two rounds later. A failed order waits for a
person to press Retry. Misbehaviour (--misbehave per round): a duplicate (the last event again),
a late count (dated a minute earlier), a broken event (must be refused) or a race (two counts of
one bin at the same moment). The control page on --port shows the bins, the last 50 posts and
buttons that only ever produce signed posts.
"""

import argparse
import hashlib
import hmac
import html
import itertools
import json
import os
import random
import signal
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

SOURCE = "vision-room-gateway-sim"
FACILITY = "FAC-101"
EVENTS_PATH = "/api/v1/inventory-events"
ATTEMPTS_PATH = "/api/v1/outbound-attempts?status=all&limit=500"
LOG_ROWS = 50
MISBEHAVIOURS = ("duplicate", "late", "broken", "race")
# room, item, par, reorder point, units used per round
DEFAULT_BINS = [
    ("ROOM-5A", "MED-50101", 12, 4, 2),
    ("ROOM-5A", "MED-50102", 20, 6, 3),
    ("ROOM-5B", "MED-51201", 10, 3, 1),
    ("ROOM-5B", "MED-51202", 16, 5, 2),
    ("ROOM-6C", "MED-62301", 8, 2, 1),
    ("ROOM-6C", "MED-62302", 14, 4, 3),
]
DECISION_WORDS = {
    "none": "Noted",
    "recovered": "Noted: back above the refill level",
    "request_created": "Ordered a refill",
    "blocked_open_request": "Noted: a refill is already on its way",
    "blocked_not_armed": "Noted: already ordered, waiting for the refill",
    "stale": "Ignored: older than the last count",
}
KIND_WORDS = {
    "count": "count",
    "restock": "restocked",
    "duplicate": "duplicate",
    "late": "late count",
    "broken": "broken event",
    "race": "two at once",
}
BUTTONS = [
    ("low", "Count low now"),
    ("restock", "Restock now"),
    ("duplicate", "Send duplicate"),
    ("late", "Send a late count"),
    ("broken", "Send a broken event"),
]
# (method, path, body, headers) -> (status, decoded JSON); status None when unreachable.
Transport = Callable[[str, str, bytes | None, dict[str, str]], tuple[int | None, Any]]


@dataclass
class Bin:
    room_id: str
    sku: str
    par: int
    reorder: int
    rate: int  # units used per round, give or take one
    stock: int
    facility_id: str = FACILITY
    order_id: str | None = None  # the refill order the bin waits for
    order_status: str | None = None
    restock_round: int | None = None  # when the refill reaches the room
    last_body: bytes | None = None  # for a duplicate
    last_observed_at: datetime | None = None  # for a late count

    @property
    def name(self) -> str:
        return f"{self.room_id} {self.sku}"


def make_bins(count: int = len(DEFAULT_BINS)) -> list[Bin]:
    bins = []
    for index in range(count):
        room, sku, par, reorder, rate = DEFAULT_BINS[index % len(DEFAULT_BINS)]
        if index >= len(DEFAULT_BINS):
            sku = f"{sku}-{index // len(DEFAULT_BINS) + 1}"
        bins.append(Bin(room, sku, par, reorder, rate, stock=par))
    return bins


def sign(body: bytes, secret: str, timestamp: int) -> dict[str, str]:
    """The webhook headers: HMAC-SHA256 over b"<timestamp>." + the raw bytes."""
    digest = hmac.new(
        secret.encode(), str(timestamp).encode() + b"." + body, hashlib.sha256
    ).hexdigest()
    return {
        "Content-Type": "application/json",
        "X-Webhook-Timestamp": str(timestamp),
        "X-Webhook-Signature": f"v1={digest}",
    }


def iso(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class Gateway:
    """run_round() is one automatic round; act() is one button. Both hold one lock."""

    def __init__(
        self,
        bins: list[Bin],
        transport: Transport,
        secret: str,
        *,
        rng: random.Random | None = None,
        misbehave: float = 0.1,
        restock_delay: tuple[int, int] = (1, 2),
        echo: bool = True,
    ) -> None:
        self.bins = {b.name: b for b in bins}
        self.transport = transport
        self.secret = secret
        self.rng = rng or random.Random()
        self.misbehave = misbehave
        self.restock_delay = restock_delay  # rounds from delivery to the refill
        self.echo = echo  # print each post as a JSON line
        self.log: deque[dict[str, Any]] = deque(maxlen=LOG_ROWS)
        self.round_no = 0
        self.paused = False
        self.lock = threading.Lock()
        # Event ids: this run's start time, then a zero-padded counter, so a restarted
        # simulator never reuses an id and the string order matches the sending order.
        self._run = f"{int(time.time() * 1000):x}"
        self._ids = itertools.count(1)

    def new_event(self, b: Bin, quantity: int, observed_at: datetime | None = None) -> dict:
        snapshot = next(self._ids)
        return {
            "event_id": f"evt_gw_{self._run}_{snapshot:06d}",
            "source": SOURCE,
            "facility_id": b.facility_id,
            "room_id": b.room_id,
            "sku": b.sku,
            "observed_at": iso(observed_at or datetime.now(UTC)),
            "observed_quantity": quantity,
            "par_level": b.par,
            "reorder_point": b.reorder,
            "confidence": round(self.rng.uniform(0.9, 0.99), 2),
            "camera_id": f"CAM-{b.room_id.removeprefix('ROOM-')}-01",
            "metadata": {
                "snapshot_id": f"snap_{self._run}_{snapshot:06d}",
                "correlation_id": f"corr_gw_{self._run}_{snapshot:06d}",
            },
        }

    def post(self, b: Bin, event: dict | bytes, kind: str) -> dict[str, Any]:
        """Sign and post one event (a dict, or the exact bytes of an earlier one)."""
        body = event if isinstance(event, bytes) else json.dumps(event).encode()
        status, answer = self.transport(
            "POST", EVENTS_PATH, body, sign(body, self.secret, int(time.time()))
        )
        self._note_order(b, status, answer)
        return self._record(b, body, kind, status, answer)

    def _note_order(self, b: Bin, status: int | None, answer: Any) -> None:
        if (
            status == 201 and answer.get("decision") == "request_created"
        ):  # a duplicate repeats an old answer
            b.order_id = answer["replenishment_request"]["id"]
            b.order_status = answer["replenishment_request"]["status"]
            b.restock_round = None

    def _record(
        self, b: Bin, body: bytes, kind: str, status: int | None, answer: Any
    ) -> dict[str, Any]:
        try:
            sent = json.loads(body)
        except ValueError:
            sent = {}
        answer = answer if isinstance(answer, dict) else {}
        entry = {
            "ts": iso(datetime.now(UTC)),
            "bin": b.name,
            "event_id": sent.get("event_id"),
            "kind": kind,
            "quantity": sent.get("observed_quantity"),
            "status": status,
            "answer": answer.get("status"),  # accepted or duplicate
            "decision": answer.get("decision") or answer.get("error"),
        }
        self.log.append(entry)
        if self.echo:
            print(json.dumps(entry), flush=True)
        return entry

    def run_round(self) -> None:
        """Learn what happened to the open orders, count every bin, maybe misbehave once."""
        with self.lock:
            self.round_no += 1
            waiting = [b for b in self.bins.values() if b.order_id and b.restock_round is None]
            if waiting:
                status, answer = self.transport("GET", ATTEMPTS_PATH, None, {})
                if status == 200:  # else ask again next round
                    # An order with no attempt yet is not listed; it keeps the status it had.
                    statuses = {a["request"]["id"]: a["request"]["status"] for a in answer["items"]}
                    for b in waiting:
                        b.order_status = statuses.get(b.order_id, b.order_status)
                        if b.order_status == "delivered":
                            b.restock_round = self.round_no + self.rng.randint(*self.restock_delay)
            for b in self.bins.values():
                if b.restock_round is not None and self.round_no >= b.restock_round:
                    self._restock(b)
                else:
                    b.stock = max(0, b.stock - max(0, b.rate + self.rng.choice((-1, 0, 0, 1))))
                    self._count(b, "count")
            if self.rng.random() < self.misbehave:
                self._misbehave(
                    self.rng.choice(MISBEHAVIOURS), self.rng.choice(list(self.bins.values()))
                )

    def _count(self, b: Bin, kind: str) -> dict[str, Any]:
        event = self.new_event(b, b.stock)
        entry = self.post(b, event, kind)
        b.last_body = json.dumps(event).encode()
        b.last_observed_at = datetime.fromisoformat(event["observed_at"])
        return entry

    def _restock(self, b: Bin) -> dict[str, Any]:
        b.stock = b.par
        b.order_id = b.order_status = None
        b.restock_round = None
        return self._count(b, "restock")

    def _misbehave(self, kind: str, b: Bin) -> list[dict[str, Any]]:
        if kind == "race":
            # Two cameras report the bin at the same moment, from two threads released
            # together; the service must make at most one order of it.
            moment = datetime.now(UTC)
            bodies = [
                json.dumps(self.new_event(b, b.stock, observed_at=moment)).encode()
                for _ in range(2)
            ]
            results: list[tuple[int | None, Any]] = [(None, None), (None, None)]
            barrier = threading.Barrier(2)

            def send(index: int) -> None:
                barrier.wait()
                headers = sign(bodies[index], self.secret, int(time.time()))
                results[index] = self.transport("POST", EVENTS_PATH, bodies[index], headers)

            threads = [threading.Thread(target=send, args=(i,)) for i in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            entries = []
            for body, (status, answer) in zip(bodies, results, strict=True):
                self._note_order(b, status, answer)
                entries.append(self._record(b, body, "race", status, answer))
            b.last_body = bodies[1]
            b.last_observed_at = moment
            return entries
        if b.last_body is None:  # nothing to repeat or be late against yet
            self._count(b, "count")
        if kind == "duplicate":
            assert b.last_body is not None
            return [self.post(b, b.last_body, "duplicate")]
        if kind == "late":
            assert b.last_observed_at is not None
            earlier = b.last_observed_at - timedelta(minutes=1)
            event = self.new_event(b, min(b.par, b.stock + b.rate), observed_at=earlier)
            return [self.post(b, event, "late")]
        if kind == "broken":
            event = self.new_event(b, -3)
            del event["room_id"]
            event["confidence"] = 1.4
            return [self.post(b, event, "broken")]
        raise ValueError(f"unknown misbehaviour {kind!r}")

    def act(self, bin_name: str, action: str) -> list[dict[str, Any]]:
        """One button of the control page."""
        with self.lock:
            b = self.bins[bin_name]
            if action == "low":
                b.stock = min(b.stock, b.reorder)
                return [self._count(b, "count")]
            if action == "restock":
                return [self._restock(b)]
            if action in MISBEHAVIOURS:
                return self._misbehave(action, b)
            raise ValueError(f"unknown action {action!r}")


def http_transport(base_url: str, timeout: float = 5.0) -> Transport:
    root = base_url.rstrip("/")

    def decode(raw: bytes) -> Any:
        try:
            return json.loads(raw) if raw else None
        except ValueError:
            return None

    def call(
        method: str, path: str, body: bytes | None, headers: dict[str, str]
    ) -> tuple[int | None, Any]:
        request = urllib.request.Request(root + path, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.status, decode(response.read())
        except urllib.error.HTTPError as exc:
            return exc.code, decode(exc.read())
        except OSError as exc:
            return None, {"error": f"could not reach {root}: {exc}"}

    return call


def make_handler(gateway: Gateway, interval: float) -> type[BaseHTTPRequestHandler]:
    """GET / (the page), GET /state (the same as JSON), POST /act, /pause, /resume."""

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path == "/state":
                state = {
                    "round": gateway.round_no,
                    "paused": gateway.paused,
                    "bins": [
                        {
                            "name": b.name,
                            "stock": b.stock,
                            "par": b.par,
                            "reorder": b.reorder,
                            "order_id": b.order_id,
                            "order_status": b.order_status,
                            "restock_round": b.restock_round,
                        }
                        for b in gateway.bins.values()
                    ],
                    "log": list(gateway.log),
                }
                self._send(200, "application/json", json.dumps(state, default=str))
                return
            if self.path != "/":
                self._send(404, "text/plain", "not found")
                return
            # Read without the lock: a slow round must not stop the page from loading.
            e = html.escape
            buttons = "".join(
                f'<button name="action" value="{a}">{label}</button>' for a, label in BUTTONS
            )
            bin_rows = []
            for b in list(gateway.bins.values()):
                if b.restock_round is not None:
                    rounds = max(b.restock_round - gateway.round_no, 0)
                    words = f"Refill delivered; it reaches the room in {rounds} round(s)"
                elif b.order_status == "failed":
                    words = "Refill order failed: someone has to press Retry on the operations page"
                elif b.order_id:
                    words = "Refill ordered; waiting for the delivery"
                else:
                    words = "Low" if b.stock <= b.reorder else "In stock"
                bin_rows.append(
                    f"<tr><td>{e(b.room_id)}</td><td>{e(b.sku)}</td>"
                    f"<td><b>{b.stock}</b> of {b.par}</td><td>{b.reorder}</td><td>{e(words)}</td>"
                    f'<td class="buttons"><form method="post" action="/act">'
                    f'<input type="hidden" name="bin" value="{e(b.name)}">'
                    f"{buttons}</form></td></tr>"
                )
            log_rows = []
            for entry in reversed(list(gateway.log)):
                status = entry["status"]
                if status is None:
                    answer = "Could not reach the service"
                elif entry["answer"] == "duplicate":
                    answer = "Already had this count; nothing changed"
                elif status == 422:
                    answer = "Refused: the event was broken"
                elif status == 401:
                    answer = "Refused: the signature did not match"
                elif status >= 400:
                    answer = f"Refused ({entry['decision']})"
                else:
                    answer = DECISION_WORDS.get(entry["decision"], str(entry["decision"]))
                log_rows.append(
                    f"<tr><td>{e(entry['ts'][11:19])}</td><td>{e(entry['bin'])}</td>"
                    f"<td>{e(KIND_WORDS.get(entry['kind'], entry['kind']))}</td>"
                    f"<td>{'' if entry['quantity'] is None else entry['quantity']}</td>"
                    f"<td>{status if status is not None else 'none'}</td><td>{e(answer)}</td></tr>"
                )
            toggle = "resume" if gateway.paused else "pause"
            page = PAGE_TEMPLATE.format(
                running="paused" if gateway.paused else f"running, every {interval:g} seconds",
                toggle=toggle,
                toggle_label=toggle.capitalize(),
                round_no=gateway.round_no,
                bins="\n".join(bin_rows),
                log="\n".join(log_rows) or '<tr><td colspan="6">Nothing sent yet.</td></tr>',
            )
            self._send(200, "text/html; charset=utf-8", page)

        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            form = urllib.parse.parse_qs(self.rfile.read(length).decode("utf-8"))
            if self.path in ("/pause", "/resume"):
                gateway.paused = self.path == "/pause"
            elif self.path == "/act":
                try:
                    gateway.act(form.get("bin", [""])[0], form.get("action", [""])[0])
                except (KeyError, ValueError):
                    self._send(400, "text/plain", "unknown bin or button")
                    return
            else:
                self._send(404, "text/plain", "not found")
                return
            self.send_response(303)  # back to the page, so a reload does not post again
            self.send_header("Location", "/")
            self.end_headers()

        def _send(self, status: int, content_type: str, text: str) -> None:
            data = text.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, format: str, *args: Any) -> None:
            pass  # stdout carries the posts as JSON lines

    return Handler


PAGE_TEMPLATE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="2">
<title>Simulated gateway</title>
<style>
body {{ font: 15px/1.4 system-ui, sans-serif; margin: 16px; color: #1d2733; background: #fff; }}
table {{ border-collapse: collapse; margin-bottom: 20px; }}
th, td {{ text-align: left; padding: 4px 8px; border-bottom: 1px solid #e3e7ec; }}
td {{ white-space: nowrap; }}
td.buttons {{ white-space: normal; min-width: 300px; }}
th {{ font-weight: 600; }}
td b {{ font-size: 18px; }}
button {{ margin: 2px; font: inherit; font-size: 13px; }}
.wrap {{ overflow-x: auto; }}
</style></head>
<body>
<h1>Simulated gateway</h1>
<p>This page plays the cameras in the supply rooms. Every round it counts each bin and sends
the count to the restock service, signed the way the real cameras sign it. The buttons send
one count, or one deliberate mistake, for a bin.</p>
<form method="post" action="/{toggle}">Automatic counts: {running} (round {round_no}).
<button>{toggle_label}</button></form>
<h2>Bins</h2>
<div class="wrap"><table><thead><tr><th>Room</th><th>Item</th><th>On the shelf</th>
<th>Refill at</th><th>What is happening</th><th></th></tr></thead>
<tbody>
{bins}
</tbody></table></div>
<h2>Last 50 counts sent, newest first (times in UTC)</h2>
<div class="wrap"><table><thead><tr><th>Time</th><th>Bin</th><th>Sent</th><th>Units</th>
<th>Code</th><th>What the service did</th></tr></thead>
<tbody>
{log}
</tbody></table></div>
</body></html>
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--url", default=os.environ.get("RESTOCK_URL", "http://127.0.0.1:8000"))
    parser.add_argument("--secret", default=os.environ.get("WEBHOOK_SECRET"))
    parser.add_argument("--interval", type=float, default=10.0, help="seconds between rounds")
    parser.add_argument("--bins", type=int, default=6)
    parser.add_argument("--host", default="127.0.0.1", help="address of the control page")
    parser.add_argument("--port", type=int, default=8081)
    parser.add_argument("--seed", type=int, help="repeat the same randomness")
    parser.add_argument(
        "--misbehave", type=float, default=0.1, help="chance per round of one mistake"
    )
    args = parser.parse_args()
    if not args.secret:
        parser.error("set WEBHOOK_SECRET or pass --secret (the demo uses assessment-secret)")
    if args.bins < 1 or args.interval <= 0 or not 0 <= args.misbehave <= 1:
        parser.error("--bins must be at least 1, --interval above 0, --misbehave from 0 to 1")

    gateway = Gateway(
        make_bins(args.bins),
        http_transport(args.url),
        args.secret,
        rng=random.Random(args.seed),
        misbehave=args.misbehave,
    )
    server = ThreadingHTTPServer((args.host, args.port), make_handler(gateway, args.interval))
    stop = threading.Event()

    def rounds() -> None:
        while not stop.is_set():
            if not gateway.paused:
                try:
                    gateway.run_round()
                except Exception as exc:  # a bad answer must not end the demo
                    print(
                        json.dumps({"ts": iso(datetime.now(UTC)), "error": repr(exc)}), flush=True
                    )
            stop.wait(args.interval)

    threading.Thread(target=rounds, daemon=True).start()
    # As PID 1 in a container SIGTERM would otherwise be ignored.
    signal.signal(signal.SIGTERM, lambda signum, frame: sys.exit(0))
    try:
        server.serve_forever()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        stop.set()
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
