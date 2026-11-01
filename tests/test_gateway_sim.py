"""The gateway simulator (scripts/gateway_sim.py) against the real webhook:"""

import importlib.util
import random
import sys
import threading
import urllib.parse
import urllib.request
from http.server import ThreadingHTTPServer
from typing import Any

from fastapi.testclient import TestClient

from restock.runtime import REPO_ROOT
from restock.schemas import ReplenishmentRequest

from .helpers import SECRET, count_rows, deliver_due, free_port, requests_now

_spec = importlib.util.spec_from_file_location(
    "gateway_sim", REPO_ROOT / "scripts" / "gateway_sim.py"
)
assert _spec is not None and _spec.loader is not None
gateway_sim = importlib.util.module_from_spec(_spec)
sys.modules["gateway_sim"] = gateway_sim  # dataclasses look the module up by name
_spec.loader.exec_module(gateway_sim)


class Recorder:
    """A gateway transport that calls the app through the TestClient and keeps each answer."""

    def __init__(self, client: TestClient) -> None:
        self.client = client
        self.answers: list[tuple[int, Any]] = []

    def __call__(
        self, method: str, path: str, body: bytes | None, headers: dict[str, str]
    ) -> tuple[int, Any]:
        response = self.client.request(method, path, content=body, headers=headers)
        answer = response.json() if response.content else None
        self.answers.append((response.status_code, answer))
        return response.status_code, answer


def one_bin(transport: Any, **bin_fields: Any) -> tuple[Any, Any]:
    """A gateway playing one bin (par 10, refill at 3, 2 a round unless given), no random mistakes,
    the refill one round after delivery."""
    fields = {"par": 10, "reorder": 3, "rate": 2, "stock": 10} | bin_fields
    b = gateway_sim.Bin("ROOM-SIM", "MED-99001", **fields)
    gateway = gateway_sim.Gateway(
        [b],
        transport,
        SECRET,
        rng=random.Random(1),
        misbehave=0,
        restock_delay=(1, 1),
        echo=False,
    )
    return gateway, b


def test_built_events_are_accepted_by_the_webhook(client):
    recorder = Recorder(client)
    gateway, b = one_bin(recorder)
    event = gateway.new_event(b, 7)
    assert set(event) == {
        "event_id",
        "source",
        "facility_id",
        "room_id",
        "sku",
        "observed_at",
        "observed_quantity",
        "par_level",
        "reorder_point",
        "confidence",
        "camera_id",
        "metadata",
    }
    entry = gateway.post(b, event, "count")
    assert (entry["status"], entry["answer"], entry["decision"]) == (201, "accepted", "none")
    assert set(entry) >= {"ts", "bin", "event_id", "status", "decision"}
    history = client.get(f"/api/v1/items/{b.sku}/history", params={"room_id": b.room_id})
    stored = history.json()["items"][0]
    assert (stored["event_id"], stored["source"]) == (event["event_id"], "vision-room-gateway-sim")
    assert stored["observed_quantity"] == 7


def test_misbehaviours_get_the_decisions_they_provoke(client):
    recorder = Recorder(client)
    gateway, b = one_bin(recorder)
    gateway.act(b.name, "restock")  # a first count to repeat and to be late against

    [duplicate] = gateway.act(b.name, "duplicate")
    assert (duplicate["status"], duplicate["answer"]) == (200, "duplicate")

    [late] = gateway.act(b.name, "late")
    assert (late["status"], late["decision"]) == (201, "stale")
    current = recorder.answers[-1][1]["current"]
    assert current["current_quantity"] == 10  # the late count changed nothing

    [broken] = gateway.act(b.name, "broken")
    assert (broken["status"], broken["decision"]) == (422, "validation_failed")
    fields = {error["field"] for error in recorder.answers[-1][1]["errors"]}
    assert {"room_id", "observed_quantity", "confidence"} <= fields
    assert count_rows(client.app, ReplenishmentRequest) == 0


def test_a_race_makes_one_order(live_server):
    live = live_server()
    gateway, b = one_bin(gateway_sim.http_transport(live.url), stock=2)
    entries = gateway.act(b.name, "race")
    assert [entry["status"] for entry in entries] == [201, 201]
    decisions = sorted(entry["decision"] for entry in entries)
    assert decisions.count("request_created") == 1
    assert count_rows(live.app, ReplenishmentRequest) == 1
    assert b.order_id is not None


def test_three_rounds_order_then_restock_a_bin(client, app):
    recorder = Recorder(client)
    gateway, b = one_bin(recorder, reorder=9)  # the first count (7 to 9) is already low

    gateway.run_round()
    assert gateway.log[-1]["decision"] == "request_created"
    order_id = b.order_id
    assert deliver_due(app, client) == 1  # the service's worker, one pass

    gateway.run_round()  # learns of the delivery; the refill arrives next round
    assert b.order_status == "delivered" and b.restock_round == 3

    gateway.run_round()
    assert (gateway.log[-1]["kind"], gateway.log[-1]["decision"]) == ("restock", "recovered")
    assert b.stock == b.par and b.order_id is None
    requests = requests_now(client)
    assert [(r["id"], r["status"]) for r in requests] == [(order_id, "delivered")]
    items = client.get(f"/api/v1/rooms/{b.room_id}/inventory").json()["items"]
    assert (items[0]["current_quantity"], items[0]["armed"]) == (10, True)


def test_the_control_page_buttons_send_counts(client):
    gateway, b = one_bin(Recorder(client))
    port = free_port()
    server = ThreadingHTTPServer(("127.0.0.1", port), gateway_sim.make_handler(gateway, 3))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{port}"
    try:
        form = urllib.parse.urlencode({"bin": b.name, "action": "low"}).encode()
        with urllib.request.urlopen(base + "/act", data=form, timeout=10) as response:
            page = response.read().decode()  # urllib follows the 303 back to the page
        assert "Ordered a refill" in page and "Count low now" in page
        urllib.request.urlopen(base + "/pause", data=b"", timeout=10).close()
        assert gateway.paused
        with urllib.request.urlopen(base + "/", timeout=10) as response:
            assert "Automatic counts: paused" in response.read().decode()
    finally:
        server.shutdown()
        server.server_close()
