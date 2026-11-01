"""The ERP simulator (restock/erp_mock.py):"""

import random
import time
from collections import Counter
from collections.abc import Callable, Iterator

import httpx
import pytest
from fastapi.testclient import TestClient

from restock.erp_mock import FAULTS, SimErp, create_erp_sim_app, draw_scenario
from restock.main import create_app
from restock.outbound import OutboundWorker

from .helpers import event_body, make_settings, post_event, requests_now


@pytest.fixture
def make_sim() -> Iterator[Callable[..., TestClient]]:
    """Start the standalone ERP at a fault rate, behind a TestClient."""
    started: list[TestClient] = []

    def start(rate: float = 0.0, seed: int = 7) -> TestClient:
        client = TestClient(create_erp_sim_app(rate, client_timeout_seconds=0.01, seed=seed))
        started.append(client.__enter__())
        return client

    yield start
    for client in started:
        client.__exit__(None, None, None)


def order(sim: TestClient, key: str) -> httpx.Response:
    return sim.post("/mock-erp/orders", json={"sku": "MED-1"}, headers={"Idempotency-Key": key})


def test_the_fixture_scenarios_on_demand(make_sim):
    sim = make_sim()
    names = ["rate_limited", "temporary_failure", "permanent_rejection", "success"]
    assert sim.post("/mock-erp/_control", json={"scenarios": names}).status_code == 200
    answers = [order(sim, "k1") for _ in names]
    assert [a.status_code for a in answers] == [429, 503, 422, 201]
    assert answers[0].headers["Retry-After"] == "2"
    again = sim.post(
        "/mock-erp/orders",
        json={"sku": "MED-1"},
        headers={"Idempotency-Key": "k1", "X-Mock-Scenario": "duplicate_idempotency_key"},
    )
    assert again.status_code == 200 and again.json()["duplicate"] is True
    assert again.json()["erp_order_id"] == answers[3].json()["erp_order_id"] == "ERP-7001"
    refused = sim.post("/mock-erp/_control", json={"scenarios": ["nope"]})
    assert refused.status_code == 422 and refused.json()["error"] == "unknown_scenario"
    received = sim.get("/mock-erp/_control").json()["received"]
    assert [r["http_status"] for r in received] == [429, 503, 422, 201, 200]
    page = sim.get("/")
    assert page.status_code == 200 and "Simulated ERP" in page.text and "ERP-7001" in page.text


def test_rate_zero_always_accepts(make_sim):
    sim = make_sim()
    rng = random.Random(1)
    assert {draw_scenario(rng, 0.0) for _ in range(2000)} == {"success"}
    assert {order(sim, f"k{n}").status_code for n in range(30)} == {201}


def test_the_schedule_splits_the_faults_8_6_4_2():
    rng = random.Random(2)
    drawn = Counter(draw_scenario(rng, 0.2) for _ in range(20_000))
    assert abs(drawn["success"] / 20_000 - 0.8) < 0.015
    for name, share in FAULTS.items():
        assert abs(drawn[name] / 20_000 - 0.2 * share) < 0.01, name


def test_rate_one_never_gives_a_plain_accept(make_sim):
    sim = make_sim(1.0, seed=4)  # its first 8 calls draw all four faults
    rng = random.Random(3)
    drawn = Counter(draw_scenario(rng, 1.0) for _ in range(2000))
    assert "success" not in drawn and set(drawn) == set(FAULTS)
    for n in range(8):
        started = time.monotonic()
        answer = order(sim, f"k{n}")
        late = time.monotonic() - started >= 1.0  # accept-then-timeout: past any caller
        assert answer.status_code in (429, 503, 422) or late, answer.status_code
    received = sim.get("/mock-erp/_control").json()["received"]
    assert {r["scenario"] for r in received} == set(FAULTS)


def test_queued_scenarios_win_over_the_schedule(make_sim):
    sim = make_sim(1.0)
    sim.post("/mock-erp/_control", json={"scenarios": ["success", "success"]})
    assert [order(sim, "a").status_code, order(sim, "b").status_code] == [201, 201]
    order(sim, "c")  # the queue is empty again: the schedule answers
    scenarios = [r["scenario"] for r in sim.get("/mock-erp/_control").json()["received"]]
    assert scenarios[:2] == ["success", "success"] and scenarios[2] in FAULTS


def test_a_rate_outside_0_to_1_is_refused():
    with pytest.raises(ValueError):
        SimErp(1.0, 1.5, random.Random())


def test_the_service_worker_delivers_through_the_sim(database_url, make_sim):
    sim = make_sim()
    # The service as the demo runs it: no mock of its own, ERP_URL pointing at the simulator
    # (http://testserver is the TestClient's host for both apps).
    settings = make_settings(database_url, erp_mock_enabled=False)
    with TestClient(create_app(settings)) as service:
        assert service.get("/mock-erp/_control").status_code == 404
        created = post_event(service, event_body()).json()["replenishment_request"]
        worker = OutboundWorker(service.app.state.sessions, settings, http_client=sim)
        assert worker.run_once() == 1
        [delivered] = requests_now(service)
    assert (delivered["id"], delivered["status"]) == (created["id"], "delivered")
    held = sim.get("/mock-erp/_control").json()["orders"]
    assert held == {f"replenishment:{created['id']}": delivered["erp_order_id"]}
