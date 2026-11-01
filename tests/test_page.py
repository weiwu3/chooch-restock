"""The operations page (GET /):"""

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from html.parser import HTMLParser
from types import SimpleNamespace

import httpx
from sqlalchemy import update

from restock.outbound import OutboundWorker
from restock.page import (
    ATTENTION,
    CHECK_THE_ERP,
    CHIPS,
    COLOURS,
    ERP_ANSWERS,
    FIX_THE_CAUSE,
    FULL,
    GOOD,
    IN_STOCK,
    IN_STOCK_ORDER_FAILED,
    IN_STOCK_ORDER_ON_ITS_WAY,
    LOW_NEXT_COUNT_ORDERS,
    LOW_ORDER_FAILED,
    LOW_ORDER_ON_ITS_WAY,
    LOW_WAITING_FOR_REFILL,
    OUT_OF_TRIES,
    QUEUED,
    REFUSED_CHIP,
    RETRY_FAILED,
    RETRYING,
    SENDING,
    SITUATIONS,
    WAITING,
    bin_situation,
    erp_answer,
    failure_chip,
    failure_reason,
    failure_sentence,
    next_step,
    order_stage,
)
from restock.runtime import REPO_ROOT, utcnow
from restock.schemas import ReplenishmentRequest

from .helpers import deliver_due, event_body, post_event, post_fixture, requests_now

REJECTED = "MED-30224 ROOM-3A"  # the bin whose order the ERP refuses, as the rows name it
RETRIED = "MED-40230 ROOM-4B"  # the bin whose order is still being retried
UTC_DATE_TIME = r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d UTC"  # the text a <time> has before the script
WORD_BUDGET = 120  # the page's own words, glossary closed
GLOSSARY_BUDGET = 150

# Elements that have no end tag.
VOID_TAGS = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "wbr"}
# The classes that name the parts of a row or card that the tests read.
FIELDS = ("chip", "bin", "units", "what", "when", "act", "title", "stock", "refill", "note")


@dataclass
class Element:
    """One HTML element: its tag, its attributes, and its children (elements and text)."""

    tag: str
    attrs: dict[str, str | None]
    children: list["Element | str"] = field(default_factory=list)

    @property
    def classes(self) -> list[str]:
        return (self.attrs.get("class") or "").split()

    def all(self, tag: str | None = None, cls: str | None = None) -> list["Element"]:
        """Every element inside this one with that tag and class, in document order."""
        found = []
        for child in self.children:
            if isinstance(child, Element):
                if tag in (None, child.tag) and (cls is None or cls in child.classes):
                    found.append(child)
                found.extend(child.all(tag, cls))
        return found

    def text(self, *, leave_out: tuple[str, ...] = ()) -> str:
        """The text inside, with every run of whitespace made one space."""
        return " ".join(_pieces(self, leave_out, visible_only=False).split())

    def visible_words(self, *, leave_out: tuple[str, ...] = ()) -> int:
        """How many words a person sees here with every <details> closed:"""
        words = _pieces(self, leave_out, visible_only=True).split()
        return sum(1 for word in words if any(char.isalnum() for char in word))


def _pieces(element: Element, leave_out: tuple[str, ...], visible_only: bool) -> str:
    pieces = []
    for child in element.children:
        if isinstance(child, str):
            pieces.append(child)
        elif child.tag in leave_out or (visible_only and _unseen(child)):
            continue
        elif visible_only and child.tag == "details" and "open" not in child.attrs:
            summary = child.all("summary")[0]
            pieces.append(f" {_pieces(summary, leave_out, visible_only)} ")
        else:
            pieces.append(f" {_pieces(child, leave_out, visible_only)} ")
    return "".join(pieces)


def _unseen(element: Element) -> bool:
    """True for an element whose text never shows on the page."""
    return element.tag in ("head", "script", "style") or "hidden" in element.attrs


class _TreeBuilder(HTMLParser):
    """Builds the Element tree of a page."""

    def __init__(self) -> None:
        super().__init__()
        self.root = Element("#document", {})
        self._open = [self.root]

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        element = Element(tag, dict(attrs))
        self._open[-1].children.append(element)
        if tag not in VOID_TAGS:
            self._open.append(element)

    def handle_endtag(self, tag: str) -> None:
        # Close the innermost open element with this tag, and whatever is still open in it.
        for depth in range(len(self._open) - 1, 0, -1):
            if self._open[depth].tag == tag:
                del self._open[depth:]
                return

    def handle_data(self, data: str) -> None:
        self._open[-1].children.append(data)


@dataclass
class Row:
    """One row of a list, or one bin card:"""

    fields: dict[str, str]
    more: dict[str, str]


class Page:
    """What the tests read from the page."""

    def __init__(self, html: str) -> None:
        builder = _TreeBuilder()
        builder.feed(html)
        builder.close()
        self.root = builder.root
        self.ids = {str(e.attrs["id"]): e for e in self.root.all() if e.attrs.get("id")}
        self.script = "".join(str(text) for text in self.root.all("script")[0].children)

    def rows(self, section: str) -> list[Row]:
        """The rows of "needs-a-person" or "on-their-way"."""
        return [_row(element) for element in self.ids[section].all("li", "row")]

    def cards(self) -> list[Row]:
        return [_row(element) for element in self.ids["bins"].all("article", "card")]

    def section_text(self, section: str) -> str:
        return self.ids[section].text()

    def tiles(self) -> list[tuple[str, str]]:
        """The three numbers at the top: each tile's text and the section it links to."""
        return [(tile.text(), str(tile.attrs["href"])) for tile in self.ids["summary"].all("a")]

    def retry_buttons(self) -> list[tuple[str, str]]:
        """Each Retry button's attempt id and the label a screen reader reads out."""
        buttons = [b for b in self.root.all("button") if "data-attempt" in b.attrs]
        return [(str(b.attrs["data-attempt"]), str(b.attrs["aria-label"])) for b in buttons]


def _row(element: Element) -> Row:
    fields = {}
    for name in FIELDS:
        for part in element.all(cls=name):
            fields[name] = part.text(leave_out=("details",))
    [more] = element.all("details", "more")
    labels = [dt.text() for dt in more.all("dt")]
    values = [dd.text() for dd in more.all("dd")]
    return Row(fields, dict(zip(labels, values, strict=True)))


def get_page(client) -> Page:
    response = client.get("/")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    return Page(response.text)


def retry_messages(page: Page) -> dict[str, str]:
    """The script's MESSAGES: what the page says for each answer of the retry endpoint."""
    block = re.search(r"const MESSAGES = \{(.*?)\n  \}", page.script, re.DOTALL)
    assert block
    return dict(re.findall(r'(\w+): "([^"]+)"', block.group(1)))


def attempt_bin(attempt: dict) -> str:
    """The bin of an attempt from the attempts list, named the way the rows name it."""
    return f"{attempt['request']['sku']} {attempt['request']['room_id']}"


def reject_one(client, app) -> int:
    """02's request is rejected by the ERP (422, failed). Returns the failed attempt's id."""
    client.post("/mock-erp/_control", json={"scenarios": ["permanent_rejection"]})
    post_fixture(client, "01_normal_stock.json")
    post_fixture(client, "02_low_stock.json")
    assert deliver_due(app, client) == 1
    failed = client.get("/api/v1/outbound-attempts", params={"status": "failed"}).json()["items"]
    assert len(failed) == 1
    return failed[0]["id"]


def reject_one_and_retry_another(client, app) -> int:
    """As reject_one, then 08's request gets a 429 and, on its second attempt, a 503:"""
    attempt_id = reject_one(client, app)
    client.post("/mock-erp/_control", json={"scenarios": ["rate_limited", "temporary_failure"]})
    post_fixture(client, "08_concurrent_a.json")
    assert deliver_due(app, client) == 1
    due_now = update(ReplenishmentRequest).values(next_attempt_at=utcnow())
    with app.state.sessions.begin() as session:  # do not wait out the 429's Retry-After: 2
        session.execute(due_now.where(ReplenishmentRequest.status == "pending"))
    assert deliver_due(app, client) == 1
    return attempt_id


def test_page_shows_what_needs_a_person_what_is_on_its_way_and_every_bin(client, app):
    attempt_id = reject_one_and_retry_another(client, app)
    page = get_page(client)

    # The three numbers, each linking to its section.
    assert page.tiles() == [
        ("1 Needs a person", "#needs-a-person"),
        ("1 On their way", "#on-their-way"),
        ("2 Low bins", "#bins"),
    ]

    # Needs a person: the rejected order with its chip, bin and units, what went wrong and
    [failed] = page.rows("needs-a-person")
    assert re.fullmatch(f"Refused by the ERP · {UTC_DATE_TIME}", failed.fields.pop("what"))
    assert failed.fields == {
        "chip": "Refused",
        "bin": REJECTED,
        "units": "8 units",
        "act": "Retry",
    }
    assert page.retry_buttons() == [(str(attempt_id), "Retry the order for MED-30224 in ROOM-3A")]
    assert page.ids[f"result-{attempt_id}"].text() == ""
    assert failed.more["What happened"] == "The ERP refused the order."
    assert failed.more["The ERP said"] == '"The ERP does not recognize the supplied SKU."'
    assert failed.more["What to do"] == FIX_THE_CAUSE
    assert failed.more["Idempotency-Key"].startswith("replenishment:")
    assert failed.more["Last try"] == (
        f"try 1 (attempt id {attempt_id}, the id the Retry API takes): non_retryable"
    )
    assert failed.more["Last error"] == "HTTP 422: unknown_sku"
    assert '"error": "unknown_sku"' in failed.more["Last raw response"]

    # On their way: only the order still being retried, since a failed order is listed once,
    pending = {"items": requests_now(client, "pending")}
    next_at = datetime.fromisoformat(pending["items"][0]["next_attempt_at"])
    [retrying] = page.rows("on-their-way")
    assert retrying.fields == {
        "chip": "Retrying",
        "bin": RETRIED,
        "units": "8 units",
        "when": f"Try 3 at {next_at:%H:%M:%S} UTC",
    }
    assert retrying.more["Last answer"] == "The ERP was unavailable"
    assert retrying.more["Tries so far"] == "2"
    assert retrying.more["Request status"] == "pending"
    assert retrying.more["Last try"].endswith(": retryable")

    # Bins: a card each, with the item and room, the chip, the stock of what the bin should
    cards = page.cards()
    assert [card.fields for card in cards] == [
        {
            "title": REJECTED,
            "chip": "Needs a person",
            "stock": "4 of 12",
            "refill": "refill at 5",
            "note": "Order for 8 failed",
        },
        {
            "title": RETRIED,
            "chip": "Low, ordered",
            "stock": "2 of 10",
            "refill": "refill at 3",
            "note": "Order for 8 on its way",
        },
    ]
    assert [card.more["Last counted"] for card in cards] == [
        "2026-08-18 19:22:10 UTC",
        "2026-08-18 20:15:00 UTC",
    ]
    assert [card.more["Facility"] for card in cards] == ["FAC-101", "FAC-101"]


def test_with_several_facilities_every_room_names_its_facility(client, app):
    reject_one(client, app)
    other = event_body(event_id="evt_9001", facility_id="FAC-202", observed_quantity=9)
    assert post_event(client, other).status_code == 201
    page = get_page(client)
    assert [card.fields["title"] for card in page.cards()] == [
        "MED-30224 ROOM-3A · FAC-101",
        "MED-30224 ROOM-3A · FAC-202",
    ]
    assert [card.fields["chip"] for card in page.cards()] == ["Needs a person", "OK"]
    [failed] = page.rows("needs-a-person")
    assert failed.fields["bin"] == "MED-30224 ROOM-3A · FAC-101"


def test_retry_button_reaches_the_retry_endpoint_and_every_answer_has_a_message(client, app):
    reject_one(client, app)
    page = get_page(client)
    # The button runs fetch(<prefix> + id + <suffix>, { method: <method> }) and shows
    # MESSAGES[result], or else the endpoint's message; I call exactly what the script calls.
    call = re.search(r'fetch\("([^"]+)" \+ id \+ "([^"]+)", \{ method: "(\w+)" \}\)', page.script)
    assert call, "the Retry handler no longer calls fetch(prefix + id + suffix, {method})"
    prefix, suffix, method = call.groups()
    messages = retry_messages(page)
    assert messages == {
        "scheduled": "Retry queued.",
        "already_scheduled": "Already queued.",
        "already_delivered": "Already delivered.",
        "superseded": "Superseded by a newer order.",
    }

    def click(attempt_id: str) -> tuple[int, str]:
        response = client.request(method, prefix + attempt_id + suffix)
        body = response.json()
        assert body.get("result") in messages or body.get("message"), body
        return response.status_code, body.get("result", body.get("error"))

    [(attempt_id, _)] = page.retry_buttons()
    assert click(attempt_id) == (202, "scheduled")
    assert click(attempt_id) == (200, "already_scheduled")
    assert deliver_due(app, client) == 1  # the queue is empty now, so the ERP accepts it
    assert click(attempt_id) == (200, "already_delivered")
    assert click("999999") == (404, "not_found")

    # The order has moved on: nothing needs a person or is on its way, and the bin's card
    # says the refill is ordered.
    after = get_page(client)
    assert after.retry_buttons() == []
    assert after.section_text("needs-a-person") == "Needs a person None"
    assert after.section_text("on-their-way") == "On their way None"
    assert [card.fields["note"] for card in after.cards()] == ["Waiting for the refill"]
    assert [count for count, _ in after.tiles()] == [
        "0 Needs a person",
        "0 On their way",
        "1 Low bins",
    ]


def test_a_superseded_order_leaves_needs_a_person(client, app):
    """Both orders are refused, ROOM-4B's first."""
    client.post("/mock-erp/_control", json={"scenarios": ["permanent_rejection"] * 2})
    post_fixture(client, "08_concurrent_a.json")
    assert deliver_due(app, client) == 1
    post_fixture(client, "01_normal_stock.json")
    post_fixture(client, "02_low_stock.json")
    assert deliver_due(app, client) == 1
    post_fixture(client, "05_restocked.json")
    newer = post_fixture(client, "06_new_depletion.json").json()["replenishment_request"]
    page = get_page(client)

    [row] = page.rows("needs-a-person")
    assert (row.fields["bin"], row.fields["chip"], row.fields["act"]) == (
        RETRIED,
        "Refused",
        "Retry",
    )
    assert row.more["What to do"] == FIX_THE_CAUSE
    failed = client.get("/api/v1/outbound-attempts", params={"status": "failed"}).json()["items"]
    attempt_of = {attempt_bin(a): a["id"] for a in failed}
    superseded_by = {attempt_bin(a): a["request"]["superseded_by"] for a in failed}
    assert superseded_by == {RETRIED: None, REJECTED: newer["id"]}
    assert page.retry_buttons() == [
        (str(attempt_of[RETRIED]), "Retry the order for MED-40230 in ROOM-4B")
    ]
    assert page.tiles()[0] == ("1 Needs a person", "#needs-a-person")
    # The newer order waits for its first try.
    [newest] = page.rows("on-their-way")
    assert (newest.fields["bin"], newest.fields["chip"], newest.fields["when"]) == (
        REJECTED,
        "Queued",
        "Try 1 soon",
    )
    assert [(card.fields["chip"], card.fields["note"]) for card in page.cards()] == [
        ("Low, ordered", "Order for 9 on its way"),  # ROOM-3A: the newer order
        ("Needs a person", "Order for 8 failed"),  # ROOM-4B
    ]

    stale_click = client.post(f"/api/v1/outbound-attempts/{attempt_of[REJECTED]}/retry")
    assert stale_click.status_code == 409
    assert stale_click.json()["error"] in retry_messages(page)


def test_page_shows_an_erp_message_with_an_unpaired_surrogate(client, app):
    """The ERP refuses an order with a message that holds an unpaired surrogate ("\ud800" in its
    JSON)."""
    post_fixture(client, "02_low_stock.json")
    answer = b'{"error": "unknown_sku", "message": "bad \\ud800 sku"}'
    erp = httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(422, content=answer)))
    worker = OutboundWorker(app.state.sessions, app.state.settings, http_client=erp)
    assert worker.run_once() == 1

    page = get_page(client)  # asserts a 200
    [failed] = page.rows("needs-a-person")
    assert failed.fields["bin"] == REJECTED
    if app.state.engine.dialect.name == "sqlite":
        # The answer was stored, so "more" shows the ERP's message with "?" for the
        # surrogate, and the raw answer with the surrogate escaped.
        assert failed.more["The ERP said"] == '"bad ? sku"'
        assert '"bad \\ud800 sku"' in failed.more["Last raw response"]


def test_page_renders_an_empty_database(client):
    page = get_page(client)
    assert page.section_text("needs-a-person") == "Needs a person None"
    assert page.section_text("on-their-way") == "On their way None"
    assert page.section_text("bins") == "Bins No bins counted yet."
    assert page.tiles() == [
        ("0 Needs a person", "#needs-a-person"),
        ("0 On their way", "#on-their-way"),
        ("0 Low bins", "#bins"),
    ]


def test_auto_refresh_finds_every_section_and_has_a_hidden_stale_banner(client, app):
    """The page refreshes every 1.5 seconds:"""
    reject_one_and_retry_another(client, app)
    page = get_page(client)
    listed = re.search(r"const SECTIONS = \[(.*?)\]", page.script)
    assert listed
    sections = re.findall(r'"([\w-]+)"', listed.group(1))
    assert sections == ["updated", "summary", "needs-a-person", "on-their-way", "bins"]
    assert "const REFRESH_MS = 1500\n" in page.script
    assert "setInterval(refresh, REFRESH_MS)" in page.script
    assert "const CLICK_REFRESH_MS = 1000\n" in page.script
    assert "setTimeout(refresh, CLICK_REFRESH_MS)" in page.script
    assert "until: Date.now() + ANSWER_MS" in page.script
    assert not re.search(r"set(Interval|Timeout)\(refresh, \d", page.script)  # no stray number
    assert all(section in page.ids for section in sections)
    banner = page.ids["stale"]
    assert "hidden" in banner.attrs and banner.attrs["role"] == "alert"
    more_ids = [str(details.attrs.get("id")) for details in page.root.all("details", "more")]
    assert len(more_ids) == 4 and len(set(more_ids)) == 4  # two rows and two cards
    for attempt_id, _ in page.retry_buttons():
        assert page.ids[f"result-{attempt_id}"].attrs["role"] == "status"


def test_every_situation_and_stage_has_its_chip_and_a_colour():
    """page.py turns stored states into the chips and lines an operator reads."""

    def bin_state(status, *, armed=True, quantity=2, par_level=10, request=None):
        return SimpleNamespace(
            status=status,
            armed=armed,
            current_quantity=quantity,
            par_level=par_level,
            open_request=request,
        )

    def request(status, next_attempt_at=None):
        return SimpleNamespace(status=status, requested_quantity=8, next_attempt_at=next_attempt_at)

    on_its_way = ("Low, ordered", WAITING, "Order for {units} on its way")
    order_failed = ("Needs a person", ATTENTION, "Order for {units} failed")
    # (the bin, whether its newest order failed, the situation, its chip, colour and line)
    situations = [
        (bin_state("ok"), False, IN_STOCK, ("OK", GOOD, "")),
        (
            bin_state("ok", request=request("pending")),
            False,
            IN_STOCK_ORDER_ON_ITS_WAY,
            ("OK", GOOD, "Order for {units} on its way"),
        ),
        (
            bin_state("ok"),
            True,
            IN_STOCK_ORDER_FAILED,
            ("Needs a person", ATTENTION, "Earlier order for {units} failed"),
        ),
        (
            bin_state("low", armed=False, request=request("pending")),
            False,
            LOW_ORDER_ON_ITS_WAY,
            on_its_way,
        ),
        (
            bin_state("low", armed=False, request=request("in_flight")),
            False,
            LOW_ORDER_ON_ITS_WAY,
            on_its_way,
        ),
        (bin_state("low", armed=False), True, LOW_ORDER_FAILED, order_failed),
        (bin_state("low", armed=True), True, LOW_ORDER_FAILED, order_failed),
        (
            bin_state("low", armed=False),
            False,
            LOW_WAITING_FOR_REFILL,
            ("Low, ordered", WAITING, "Waiting for the refill"),
        ),
        (bin_state("low", quantity=5, par_level=5), False, FULL, ("OK", GOOD, "")),
        (
            bin_state("low"),
            False,
            LOW_NEXT_COUNT_ORDERS,
            ("Low", WAITING, "Next count orders more"),
        ),
    ]
    for state, last_order_failed, expected, words in situations:
        situation = bin_situation(state, last_order_failed)
        assert situation is expected
        assert (situation.chip.text, situation.chip.colour, situation.note) == words
        assert situation.chip in CHIPS
    # The summary counts a bin as low exactly when its stock is at or below its refill level
    # and something is missing: not FULL, and not a bin in stock.
    assert [s for s in SITUATIONS if s.low] == [
        LOW_ORDER_ON_ITS_WAY,
        LOW_WAITING_FOR_REFILL,
        LOW_NEXT_COUNT_ORDERS,
        LOW_ORDER_FAILED,
    ]

    due = datetime(2026, 8, 18, 6, 29, 10, tzinfo=UTC)
    stages = [
        (request("pending", due), None, QUEUED, "Queued"),
        (request("in_flight"), SimpleNamespace(http_status=None), SENDING, "Sending"),
        (request("pending", due), SimpleNamespace(http_status=429), RETRYING, "Retrying"),
    ]
    for state, last_try, expected, text in stages:
        chip = order_stage(state, last_try)
        assert chip is expected and (chip.text, chip.colour) == (text, WAITING)
        assert chip in CHIPS

    # A 2xx reaches erp_answer only from a try that did not name the order.
    answers = [erp_answer(code) for code in (None, 429, 503, 500, 422, 302, 200)]
    assert answers == [
        "No answer from the ERP",
        "The ERP was busy",
        "The ERP was unavailable",
        "The ERP had a problem on its side",
        "The ERP refused the order",
        "The ERP gave an unexpected answer",
        "The ERP gave an unexpected answer",
    ]
    assert all(answer in ERP_ANSWERS for answer in answers)

    # A failed order: what went wrong in a few words (its row), in a sentence (its "more"),
    # and what a person should do about it.
    def failed(http_status, last_error, attempt_count=1, triggered_by="worker"):
        request = SimpleNamespace(last_error=last_error, attempt_count=attempt_count)
        return SimpleNamespace(http_status=http_status, request=request, triggered_by=triggered_by)

    # The row's chip says why: refused, gave up, or a person's Retry failed too.
    assert failure_chip(failed(422, "HTTP 422: unknown_sku")) is REFUSED_CHIP
    assert failure_chip(failed(302, "HTTP 302")) is REFUSED_CHIP
    assert failure_chip(failed(503, "exhausted", attempt_count=6)) is OUT_OF_TRIES
    assert failure_chip(failed(422, "HTTP 422: unknown_sku", 2, "manual")) is RETRY_FAILED
    assert failure_chip(failed(503, "exhausted", 7, "manual")) is RETRY_FAILED
    assert {REFUSED_CHIP, OUT_OF_TRIES, RETRY_FAILED} <= set(CHIPS)

    refused = failed(422, "HTTP 422: unknown_sku")
    assert failure_reason(refused) == "Refused by the ERP"
    assert failure_sentence(refused) == "The ERP refused the order."
    assert next_step(refused) == FIX_THE_CAUSE
    gave_up = failed(503, "exhausted", attempt_count=6)
    assert failure_reason(gave_up) == "Not confirmed after 6 tries"
    gave_up_sentence = "Stopped after 6 tries. Last answer: The ERP was unavailable."
    assert failure_sentence(gave_up) == gave_up_sentence
    assert next_step(gave_up) == CHECK_THE_ERP
    busy_to_the_end = failed(429, "exhausted", attempt_count=6)  # a 4xx, but one it retried
    assert failure_reason(busy_to_the_end) == "Not confirmed after 6 tries"
    assert next_step(busy_to_the_end) == CHECK_THE_ERP
    unexpected = failed(302, "HTTP 302")
    assert failure_reason(unexpected) == "Unexpected ERP answer"
    assert failure_sentence(unexpected) == "The ERP gave an unexpected answer."
    assert next_step(unexpected) == CHECK_THE_ERP

    # Every chip has one to three words, and every colour a CSS rule.
    assert [chip.text for chip in CHIPS if not 1 <= len(chip.text.split()) <= 3] == []
    html = (REPO_ROOT / "templates" / "index.html").read_text()
    style = html[html.index("<style>") : html.index("</style>")]
    assert COLOURS == [GOOD, WAITING, ATTENTION, "quiet"]
    assert [colour for colour in COLOURS if not re.search(rf"\.chip\.{colour}\b", style)] == []


def test_the_glossary_is_closed_short_and_explains_every_chip(client):
    """The glossary prints page.py's CHIPS, each chip with its meaning; the test above checks that
    every chip page.py picks is in CHIPS."""
    glossary = get_page(client).ids["glossary"]
    assert glossary.tag == "details" and "open" not in glossary.attrs
    assert glossary.all("summary")[0].text() == "What do these words mean?"
    text = glossary.text()
    for chip in CHIPS:
        assert f"{chip.text} {chip.meaning}" in text, chip.text
    assert "3 of 12 3 in the bin now; it should hold 12." in text
    assert "refill at 5 At 5 or fewer the bin is low" in text
    assert len(text.split()) <= GLOSSARY_BUDGET


def test_the_page_keeps_to_its_word_budget_and_puts_no_paragraph_under_a_title(client, app):
    """The page's own words (title, subtitle line, the three numbers, section titles, empty-state
    lines, footer, and the glossary's label while it is closed) stay within WORD_BUDGET, on an
    empty database and on a full one; the words inside rows and cards are the data."""
    empty = get_page(client)
    assert empty.root.visible_words() <= WORD_BUDGET
    reject_one_and_retry_another(client, app)
    full = get_page(client)
    assert full.root.visible_words(leave_out=("li", "article")) <= WORD_BUDGET
    for page in (empty, full):
        for section in page.root.all("section"):
            parts = [(e.tag, e.classes) for e in section.children if isinstance(e, Element)]
            assert parts[0] == ("h2", []), section.attrs["id"]
            assert parts[1:] in ([("ul", ["rows"])], [("div", ["cards"])], [("p", ["none"])])


def bin_history_parts(page: Page) -> list[tuple[Element | None, list[tuple[str, str]]]]:
    """Each card's chart (its <svg>, or None) and its "history" lines as (colour, text), the text
    without its time."""
    found = []
    for card in page.ids["bins"].all("article", "card"):
        [history] = card.all("details", "history")
        lines = []
        for li in history.all("li"):
            spans = li.all("span")
            lines.append((li.classes[0], spans[-1].text()))
        svgs = card.all("svg")
        found.append((svgs[0] if svgs else None, lines))
    return found


def test_each_card_charts_its_last_5_minutes_as_stairs_and_lists_what_happened(client, app):
    """A card draws the bin's counts over the last 5 minutes as stairs (each count holds until the
    next), starting from the count before the chart, over a fill down to zero, with a dot where
    an order was placed, a hollow dot for a late count that changed nothing, and a stroke for
    each ERP answer."""
    now = utcnow().replace(microsecond=0)

    def ago(seconds: int) -> str:
        return (now - timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")

    client.post("/mock-erp/_control", json={"scenarios": ["permanent_rejection"]})
    for event_id, seconds, quantity in [
        ("evt_h0", 45 * 60, 9),  # before both windows
        ("evt_h1", 20 * 60, 10),  # in the history, before the chart: where the stairs start
        ("evt_h2", 240, 12),  # a plain count
        ("evt_h3", 120, 4),  # low: orders 12 - 4 = 8
        ("evt_h4", 180, 2),  # older than evt_h3, so it is late and ignored
    ]:
        body = event_body(event_id=event_id, observed_at=ago(seconds), observed_quantity=quantity)
        assert post_event(client, body).status_code < 300
    assert deliver_due(app, client) == 1  # the ERP refuses the order

    page = get_page(client)
    [(svg, lines)] = bin_history_parts(page)
    assert lines == [
        ("attention", "The ERP refused the order; order failed"),
        ("waiting", "Counted 4; ordered 8"),
        ("quiet", "Late count of 2; ignored"),
    ]
    assert svg is not None
    [line] = svg.all("path", "stock-line")
    d = str(line.attrs["d"])
    # Par 12 is the top (y 4.0), zero the bottom (y 68.0): from the left edge at 10 (y 14.7),
    # up to 12, down to 4 (y 46.7), then held to the right edge.
    assert re.fullmatch(r"M0\.0 14\.7H[\d.]+V4\.0H[\d.]+V46\.7H240", d), d
    [area] = svg.all("path", "stock-area")
    assert area.attrs["d"] == f"{d}V68.0H0.0Z"
    [par] = svg.all("line", "par")
    assert par.attrs["y1"] == "4.0"
    [card] = page.ids["bins"].all("article", "card")
    dots = [dot.attrs["class"] for dot in card.all("span", "dot")]
    assert dots == ["dot quiet hollow", "dot waiting"]  # in time order: evt_h4, then evt_h3
    assert [t.attrs["class"] for t in svg.all("line", "tick")] == ["tick attention"]
    history = page.root.all("details", "history")[0]
    assert "open" not in history.attrs and str(history.attrs["id"]).startswith("history-bin-")


def test_a_bin_counted_long_ago_charts_the_minutes_before_its_last_count(client, app):
    """The supplied fixtures are dated 2026-08-18."""
    reject_one(client, app)
    page = get_page(client)
    [(svg, lines)] = bin_history_parts(page)
    assert svg is not None
    assert lines == [("waiting", "Counted 4; ordered 8")]
    [card] = page.ids["bins"].all("article", "card")
    [caption] = card.all("figcaption")
    # The chart's first and last time, in UTC until the script shows them in local time.
    assert caption.text() == "19:17:10 UTC last 5 min 19:22:10 UTC"


def test_a_refresh_keeps_the_readers_place(client, app):
    """A refresh would move everything under a row that appears or a list that grows."""
    reject_one_and_retry_another(client, app)
    first, second = get_page(client), get_page(client)

    def more_ids(page: Page) -> list[str]:
        return [str(d.attrs["id"]) for d in page.root.all("details", "more")]

    assert more_ids(first) == more_ids(second)
    assert "const place = topAnchor()" in first.script
    assert "keepPlace(place)" in first.script
    assert "overflow-anchor: none" in "".join(str(t) for t in first.root.all("style")[0].children)


def test_a_tab_left_open_reloads_when_the_page_changes(client):
    """The refresh swaps in sections only, so a tab left open would keep an old script and old
    styles after a deploy."""
    page = get_page(client)
    [meta] = [m for m in page.root.all("meta") if m.attrs.get("name") == "page-version"]
    assert re.fullmatch(r"[0-9a-f]{12}", str(meta.attrs["content"]))
    assert "version.content !== PAGE_VERSION" in page.script
    assert "location.reload()" in page.script


def test_a_bin_with_only_plain_counts_has_a_chart_and_says_so_in_its_history(client, app):
    """A count that changed nothing but the stock is drawn, not listed."""
    body = event_body(event_id="evt_e1", observed_at="2026-08-18T10:00:00Z", observed_quantity=9)
    assert post_event(client, body).status_code == 201
    page = get_page(client)
    [(svg, lines)] = bin_history_parts(page)
    assert svg is not None
    assert lines == [("quiet", "Only counts in the last 30 minutes")]
