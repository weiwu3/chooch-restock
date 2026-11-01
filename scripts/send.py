#!/usr/bin/env python3
"""Talk to the service by hand, signed the way the gateway signs. Standard library only.

    export WEBHOOK_SECRET=assessment-secret          # RESTOCK_URL defaults to http://127.0.0.1:8000
    python scripts/send.py send fixtures/inbound/01_normal_stock.json
    python scripts/send.py send --concurrent fixtures/inbound/08_concurrent_a.json \\
        fixtures/inbound/09_concurrent_b.json
    python scripts/send.py count ROOM-7D MED-70001 2       # one fresh count for a bin
    python scripts/send.py seed                   # the FIXTURE_GUIDE sequence, step by step

send prints each answer and exits 1 on a 5xx or no answer (4xx is expected for 07). seed
needs a fresh database for the README's outcomes; a second run shows every event as a duplicate.
"""

import argparse
import hashlib
import hmac
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "fixtures" / "inbound"


def call(
    url: str, method: str = "GET", body: bytes | None = None, headers: dict | None = None
) -> tuple[int | None, Any]:
    """(status, decoded body); status None when the service could not be reached."""
    request = urllib.request.Request(url, data=body, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read()
            status = response.status
    except urllib.error.HTTPError as exc:  # urllib raises for every 4xx and 5xx
        raw, status = exc.read(), exc.code
    except urllib.error.URLError as exc:
        return None, {"error": f"could not reach {url}: {exc.reason}"}
    try:
        return status, json.loads(raw) if raw else None
    except ValueError:
        return status, raw.decode("utf-8", "replace")


def post_event(
    base: str, secret: str, body: bytes, timestamp: int | None = None, request_id: str | None = None
) -> tuple[int | None, Any]:
    """Sign the exact bytes like tools/sign_payload.py (HMAC-SHA256 over "<ts>.<body>") and POST."""
    ts = str(int(time.time()) if timestamp is None else timestamp)
    digest = hmac.new(secret.encode(), ts.encode() + b"." + body, hashlib.sha256).hexdigest()
    headers = {
        "Content-Type": "application/json",
        "X-Webhook-Timestamp": ts,
        "X-Webhook-Signature": f"v1={digest}",
    }
    if request_id:
        headers["X-Request-ID"] = request_id
    return call(f"{base}/api/v1/inventory-events", "POST", body, headers)


def send(args: argparse.Namespace) -> int:
    def one(path: Path) -> tuple[int | None, Any]:
        return post_event(args.url, args.secret, path.read_bytes(), args.timestamp, args.request_id)

    if args.concurrent:
        # One thread per file, all released together, the way a gateway delivers 08 and 09.
        results: list[tuple[int | None, Any]] = [(None, None)] * len(args.files)
        barrier = threading.Barrier(len(args.files))

        def worker(index: int) -> None:
            barrier.wait()
            results[index] = one(args.files[index])

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(len(args.files))]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    else:
        results = [one(path) for path in args.files]
    failed = False
    for path, (status, body) in zip(args.files, results, strict=True):
        print(f"{path.name} -> HTTP {status if status is not None else 'error'}")
        print(json.dumps(body, indent=2))
        failed = failed or status is None or status >= 500
    return 1 if failed else 0


def count(args: argparse.Namespace) -> int:
    event = {
        "event_id": f"evt_{uuid.uuid4().hex[:12]}",
        "source": "vision-room-gateway",
        "facility_id": args.facility,
        "room_id": args.room,
        "sku": args.sku,
        "observed_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "observed_quantity": args.quantity,
        "par_level": args.par,
        "reorder_point": args.reorder,
        "confidence": 0.95,
        "camera_id": f"CAM-{args.room}",
        "metadata": {"snapshot_id": f"snap_{int(time.time())}", "correlation_id": "corr_manual"},
    }
    status, body = post_event(args.url, args.secret, json.dumps(event).encode())
    if status is None:
        print(body["error"], file=sys.stderr)
        return 1
    decision = body.get("decision") if isinstance(body, dict) else None
    request = body.get("replenishment_request") if isinstance(body, dict) else None
    print(f"HTTP {status}: {args.quantity} units in {args.room} {args.sku} -> {decision}")
    if request:
        print(f"  refill order for {request['quantity']} units, status {request['status']}")
    elif status >= 400:
        print(json.dumps(body, indent=2))
    return 0 if status < 500 else 1


STEPS = [
    ("1", "01_normal_stock.json", "observation and state stored; no replenishment request"),
    ("2", "02_low_stock.json", "current quantity 4; one request for 8 units"),
    (
        "3",
        "03_duplicate_low_stock.json",
        "duplicate of step 2: no new effect, request or ERP order",
    ),
    ("4", "04_out_of_order.json", "kept in history (decision stale); state and requests unchanged"),
    ("5", "05_restocked.json", "current quantity 12; recovery recorded (armed=true)"),
    (None, None, None),  # wait for the step-2 request to be delivered
    ("6", "06_new_depletion.json", "current quantity 3; a new cycle: one request for 9 units"),
    ("7", "07_invalid.json", "422 listing each invalid field; nothing stored, no outbound work"),
    (
        "8",
        "08_concurrent_a.json + 09_concurrent_b.json, concurrently",
        "both stored; evt_2002 current; exactly one request for 8 units",
    ),
]


def seed(args: argparse.Namespace) -> int:
    base = args.url
    deadline = time.monotonic() + 15
    while call(f"{base}/health/ready")[0] != 200:  # the server may still be starting
        if time.monotonic() > deadline:
            sys.exit(
                f"server at {base} is not ready; start it with 'make migrate' and 'make run', "
                "or 'make compose-up'"
            )
        time.sleep(1)
    # An empty mock queue, so the step-2 request is delivered before step 6.
    status, _ = call(
        f"{base}/mock-erp/_control",
        "POST",
        b'{"scenarios": []}',
        {"Content-Type": "application/json"},
    )
    if status == 404:
        print("note: the mock ERP is not mounted (ERP_MOCK_ENABLED=false); orders go to ERP_URL")

    def orders() -> dict[str, Any]:
        _, body = call(f"{base}/api/v1/outbound-attempts?status=all")
        return {a["request"]["triggering_event_id"]: a["request"] for a in reversed(body["items"])}

    for number, name, expected in STEPS:
        if number is None:
            print("\n--- waiting for the step-2 request to be delivered by the worker")
            deadline = time.monotonic() + 20
            while (first := orders().get("evt_1002")) is None or first["status"] != "delivered":
                if time.monotonic() > deadline:
                    sys.exit(f"request for evt_1002 not delivered in time: {first}")
                time.sleep(0.25)
            print(
                f"request {first['id']} delivered as {first['erp_order_id']} "
                f"after {first['attempt_count']} attempt(s)"
            )
            continue
        print(f"\n=== Step {number}: {name}\n    expected: {expected}")
        files = [
            FIXTURES / f
            for f in (["08_concurrent_a.json", "09_concurrent_b.json"] if number == "8" else [name])
        ]
        send(
            argparse.Namespace(
                url=base,
                secret=args.secret,
                files=files,
                concurrent=number == "8",
                timestamp=None,
                request_id=None,
            )
        )

    print("\n=== Result\ncurrent inventory:")
    for room in ("ROOM-3A", "ROOM-4B"):
        for item in call(f"{base}/api/v1/rooms/{room}/inventory")[1]["items"]:
            req = item["open_request"]
            open_req = f"{req['requested_quantity']} units {req['status']}" if req else "none"
            print(
                f"  {item['room_id']} {item['sku']}: qty={item['current_quantity']} "
                f"status={item['status']} "
                f"armed={item['armed']} last_event={item['last_event_id']} open_request={open_req}"
            )
    print("orders sent to the ERP (oldest first):")
    for r in orders().values():
        print(
            f"  {r['triggering_event_id']} {r['room_id']} {r['sku']}: "
            f"qty={r['requested_quantity']} "
            f"status={r['status']} attempts={r['attempt_count']} erp_order={r['erp_order_id']}"
        )
    print("history of MED-30224 in ROOM-3A (newest first):")
    for o in call(f"{base}/api/v1/items/MED-30224/history?room_id=ROOM-3A")[1]["items"]:
        print(
            f"  {o['event_id']} observed_at={o['observed_at']} qty={o['observed_quantity']} "
            f"applied={o['applied']} decision={o['decision']}"
        )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--url", default=os.environ.get("RESTOCK_URL", "http://127.0.0.1:8000"))
    parser.add_argument(
        "--secret", default=os.environ.get("WEBHOOK_SECRET"), help="default: $WEBHOOK_SECRET"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("send", help="sign and POST fixture files")
    p.add_argument("files", nargs="+", type=Path)
    p.add_argument("--concurrent", action="store_true", help="all files at the same moment")
    p.add_argument("--timestamp", type=int, help="override X-Webhook-Timestamp")
    p.add_argument("--request-id", help="send this X-Request-ID")
    p = commands.add_parser("count", help="one fresh count for a bin")
    p.add_argument("room")
    p.add_argument("sku")
    p.add_argument("quantity", type=int)
    p.add_argument("--par", type=int, default=10)
    p.add_argument("--reorder", type=int, default=3)
    p.add_argument("--facility", default="FAC-101")
    commands.add_parser("seed", help="replay the FIXTURE_GUIDE sequence")
    args = parser.parse_args()
    args.url = args.url.rstrip("/")
    if not args.secret and (ROOT / ".env").is_file():
        for line in (ROOT / ".env").read_text().splitlines():
            if line.startswith("WEBHOOK_SECRET="):
                args.secret = line.split("=", 1)[1].strip()
    if not args.secret:
        parser.error("set WEBHOOK_SECRET or pass --secret (the fixtures use assessment-secret)")
    return {"send": send, "count": count, "seed": seed}[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
