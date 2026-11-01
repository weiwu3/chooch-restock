"""The pure decision function, as a table."""

from datetime import UTC, datetime

import pytest

from restock.inbound import OrderKey, decide, is_low, is_newer, quantity_needed

T0 = datetime(2026, 8, 18, 19, 22, 10, tzinfo=UTC)
T1 = datetime(2026, 8, 18, 20, 0, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("candidate", "current", "newer"),
    [
        (OrderKey(T1, "evt_1"), None, True),  # first event for an item
        (OrderKey(T1, "evt_1"), OrderKey(T0, "evt_9"), True),  # later observed_at wins
        (OrderKey(T0, "evt_9"), OrderKey(T1, "evt_1"), False),  # arrival order is irrelevant
        (OrderKey(T0, "evt_b"), OrderKey(T0, "evt_a"), True),  # tie: larger event_id wins
        (OrderKey(T0, "evt_a"), OrderKey(T0, "evt_b"), False),
        (OrderKey(T0, "evt_2002"), OrderKey(T0, "evt_2001"), True),
        (OrderKey(T0, "evt_a"), OrderKey(T0, "evt_a"), False),  # equal is not newer
        (OrderKey(T0, "EVT_Z"), OrderKey(T0, "evt_a"), False),  # plain code-point order
    ],
)
def test_ordering_and_tie_break(candidate, current, newer):
    assert is_newer(candidate, current) is newer


@pytest.mark.parametrize(
    ("par", "quantity", "needed"),
    [(12, 4, 8), (12, 3, 9), (10, 2, 8), (5, 5, 0), (5, 7, 0), (12, 0, 12)],
)
def test_quantity_needed(par, quantity, needed):
    assert quantity_needed(par, quantity) == needed


def test_low_means_at_or_below_reorder_point():
    assert is_low(5, 5) and is_low(4, 5) and not is_low(6, 5)


NEWER = {"event": OrderKey(T1, "evt_new"), "current": OrderKey(T0, "evt_old")}
PAR_12_REORDER_5 = {"par_level": 12, "reorder_point": 5}


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        pytest.param(
            dict(
                event=OrderKey(T0, "evt_old"),
                current=OrderKey(T1, "evt_new"),
                armed=True,
                quantity=2,
                open_request=False,
                **PAR_12_REORDER_5,
            ),
            (False, "stale", True, None),
            id="stale event changes nothing and keeps armed as it was",
        ),
        pytest.param(
            dict(NEWER, armed=False, quantity=12, open_request=True, **PAR_12_REORDER_5),
            (True, "recovered", True, None),
            id="recovery above the reorder point arms and is recorded",
        ),
        pytest.param(
            dict(NEWER, armed=True, quantity=12, open_request=False, **PAR_12_REORDER_5),
            (True, "none", True, None),
            id="above the reorder point while already armed is a plain reading",
        ),
        pytest.param(
            dict(NEWER, armed=False, quantity=6, open_request=False, **PAR_12_REORDER_5),
            (True, "recovered", True, None),
            id="one unit above the reorder point is a recovery",
        ),
        pytest.param(
            dict(
                event=OrderKey(T0, "evt_old"),
                current=OrderKey(T1, "evt_new"),
                armed=False,
                quantity=12,
                open_request=False,
                **PAR_12_REORDER_5,
            ),
            (False, "stale", False, None),
            id="a late recovery is stale and does not arm",
        ),
        pytest.param(
            dict(NEWER, armed=True, quantity=4, open_request=False, **PAR_12_REORDER_5),
            (True, "request_created", False, 8),
            id="armed, low, no open request: request par - qty",
        ),
        pytest.param(
            dict(NEWER, armed=False, quantity=3, open_request=False, **PAR_12_REORDER_5),
            (True, "blocked_not_armed", False, None),
            id="low but no recovery since the last request",
        ),
        pytest.param(
            dict(NEWER, armed=True, quantity=3, open_request=True, **PAR_12_REORDER_5),
            (True, "blocked_open_request", True, None),
            id="armed but a request is still open: blocked, stays armed",
        ),
        pytest.param(
            dict(
                event=OrderKey(T0, "evt_2001"),
                current=None,
                armed=True,
                quantity=2,
                open_request=False,
                par_level=10,
                reorder_point=3,
            ),
            (True, "request_created", False, 8),
            id="first event for an item is armed",
        ),
    ],
)
def test_decide(kwargs, expected):
    outcome = decide(**kwargs)
    assert (outcome.applied, outcome.decision, outcome.armed, outcome.request_quantity) == expected


def test_low_with_nothing_needed_creates_nothing():
    # quantity == par_level == reorder_point is "low" but max(par - qty, 0) == 0.
    outcome = decide(
        **NEWER, armed=True, quantity=5, par_level=5, reorder_point=5, open_request=False
    )
    assert (outcome.applied, outcome.decision, outcome.armed) == (True, "none", True)
