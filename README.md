# restock

A small service that receives signed inventory observations from a facility gateway, keeps
the full history plus a correct current state per facility + room + SKU, and turns low stock
into exactly one logical replenishment order for an unreliable ERP.

- **API + operations page + delivery worker** in one FastAPI process (`restock run`).
- **Relational database with migrations**: SQLite by default, PostgreSQL 16 via Docker
  Compose. The schema exists only through Alembic (`restock migrate`).
- **Mock ERP** mounted in the same app (`/mock-erp`), driven by
  `fixtures/outbound/erp_scenarios.json`; `ERP_MOCK_ENABLED=false` leaves it out wherever
  `ERP_URL` is a real ERP.

Solution report (architecture, API, evidence, next steps): [docs/REPORT.md](docs/REPORT.md).
Design notes written before the code (architecture, API, schema): [docs/DESIGN.md](docs/DESIGN.md).

To play the camera gateway by hand against a running server, send one count for a bin:
`WEBHOOK_SECRET=assessment-secret .venv/bin/python scripts/send.py count ROOM-7D MED-70001 2`
(a count at or below the reorder point, 3 by default, orders a refill; `--help` lists the options).

## Setting up on a fresh machine

What a clean Linux or WSL (Ubuntu) machine needs, in order:

```bash
sudo apt update && sudo apt install -y git make curl python3.12 python3.12-venv   # or use uv, below
git clone git@github.com:weiwu3/chooch-restock.git restock && cd restock        # private: an SSH key on the GitHub account, or clone over HTTPS with a token
```

The Quick start below then runs the service on SQLite with no Docker at all. For the
PostgreSQL stack and the living demo you also need Docker with the Compose plugin: on WSL
that is Docker Desktop for Windows with "WSL integration" switched on for your distro, or
Docker Engine installed inside the distro; `docker compose version` must answer. The
published ports (8000, 8081, 8090, 5432) are then reachable from the Windows browser at
`http://localhost:<port>`.

## Quick start (SQLite, no Docker)

Requires Python 3.12 with its venv module (`make install PYTHON=python3` if your 3.12 is
called `python3`; on Debian/Ubuntu the module is a separate package:
`sudo apt install python3.12-venv`), and curl for the examples below (the scripts need only
Python). `make install` does not activate the venv: the Makefile calls `.venv/bin/...`
itself, and without make you call `.venv/bin/restock` (or run `source .venv/bin/activate`
first). Without the venv module, uv makes the same venv (the same 32 pinned packages) in
place of `make install`, the first line of the block after this one:

```bash
uv venv --python 3.12 .venv && uv pip install --python .venv/bin/python -r requirements-dev.txt -e .
```

```bash
make install                 # .venv: pinned dependencies + test tools (requirements-dev.txt)
cp .env.example .env         # then edit: WEBHOOK_SECRET=assessment-secret to replay the fixtures
make migrate                 # alembic upgrade head -> ./restock.db
make run                     # http://127.0.0.1:8000 ; page at /, OpenAPI docs at /docs
```

In a second terminal:

```bash
make seed                    # replays the FIXTURE_GUIDE sequence and prints every response
```

Without a `.env` file, pass the secret on the command line instead:

```bash
make migrate
WEBHOOK_SECRET=assessment-secret make run
WEBHOOK_SECRET=assessment-secret make seed      # second terminal
```

`assessment-secret` is the synthetic secret the supplied fixtures and `tools/sign_payload.py`
examples use. `.env.example` holds placeholders only (`WEBHOOK_SECRET=change-me`); the app
refuses to start without a secret, and also while it is still `change-me`, since anyone who
has read the repository could sign events with it.

## Docker Compose (PostgreSQL 16)

```bash
WEBHOOK_SECRET=assessment-secret make compose-up   # db + app; returns once both are healthy
make compose-seed                                  # the fixture sequence, inside the app container
make compose-down                                  # stops and deletes the database volume
```

`make compose-up` builds the image, starts PostgreSQL and the app, and waits until both
healthchecks pass (`pg_isready` over TCP, then the app's `/health/ready`). The app migrates
on every start, and both containers restart unless you stop them. The image starts the
server with `exec`, so it is the container's main process and receives the SIGTERM of
`docker compose stop`: the lifespan shutdown then runs, and the worker stops claiming and
records a call in progress if it ends within one ERP timeout plus a second. Without `exec`
the signal stopped at `sh`, and Docker killed the app after its 10 s grace (10,239 ms
measured; with `exec` the same stop took 635 ms, and 5.3 s while an ERP call was in flight). The secret has no default: without
`WEBHOOK_SECRET`, Compose refuses to start. `make compose-seed` runs `scripts/send.py seed`
inside the app container, so the host needs neither Python nor curl;
`WEBHOOK_SECRET=assessment-secret make seed` from the host works as well and needs only a
Python 3 (the venv from `make install` if there is one, else `python3`). `make test-pg` runs
on the host and needs `make install`.

The image installs the runtime dependencies only (`requirements.txt`; the test and lint
tools are in `requirements-dev.txt`) and runs as an unprivileged user (uid 10001), which
cannot write to the app directory, so `DATABASE_URL` must point at PostgreSQL there. The
compose Postgres publishes port 5432 on the host and also creates a `restock_test`
database for `make test-pg`. Docker is not installed on the machine I built this on, so I
ran the Compose path on a second one (Docker Compose v2.29.7): healthy in 24 s, the
in-container seed gave every expected outcome, `make test-pg` passed against the compose
PostgreSQL and `make test` on SQLite.

## Living demo

The plain stack waits for someone to send events. The living demo adds the two parties the
service talks to, each as a process of its own, so the operations page has something to watch:
a gateway simulator that counts six bins in three rooms and posts signed counts to the
webhook, and an ERP simulator (the mock ERP alone) that now and then answers badly on purpose.
Every event comes from one of them; the page only watches.

```bash
WEBHOOK_SECRET=assessment-secret make demo-up   # db + app + ERP simulator + gateway simulator
make demo-down                                   # stops all four and deletes the database volume
```

| URL | What it is |
| --- | --- |
| http://127.0.0.1:8000 | the operations page |
| http://127.0.0.1:8081 | the gateway control page: the bins, the last 50 counts sent, and buttons per bin ("Count low now", "Restock now", "Send duplicate", "Send a late count", "Send a broken event") plus pause and resume |
| http://127.0.0.1:8090 | the ERP status page: the orders it holds, how often it answers badly, the last 50 calls and what it answered |

What to watch on the operations page: every 10 seconds each bin's count drops. A bin that
reaches its refill level turns "Low, ordered" and its order appears under "On their way".
About one ERP call in five is answered badly (`ERP_SIM_FAULT_RATE`, default 0.2): a 429 or a
503 shows as a retry with its try number, an answer held back past the timeout is retried
with the same key and the ERP answers that it already has the order, and about one call in
fifty is refused for good, which puts the order under "Needs a person" with a Retry button.
Once an order is delivered the gateway brings the refill one or two rounds later, and the bin
is back to "OK". About one round in ten the gateway also misbehaves once (a duplicate, a late
count, a broken event or two counts at the same moment), and its control page shows how the
service answered. `ERP_SIM_FAULT_RATE=0.5 WEBHOOK_SECRET=assessment-secret make demo-up`
gives a rougher ERP.

The plain stack (`make compose-up`, `docker compose up`) never starts the simulators: they
are in the compose profile `demo`, and the app keeps its in-app mock ERP unless `make demo-up`
points it at the simulator (`ERP_MOCK_ENABLED=false`, `ERP_URL=http://erp:8090/mock-erp`).
Docker is not installed on the machine I built this on; the demo profile ran on a second
machine (Docker Compose v2.29.7): the four containers were healthy 20 s after `make demo-up`,
and 45 s later the service had placed and delivered three orders from the simulated gateway
through the simulated ERP. The three processes also run without Docker, as below.

Without Docker, in three terminals (here on ports 8780, 8790 and 8781):

```bash
export WEBHOOK_SECRET=assessment-secret
.venv/bin/restock erp-sim --port 8790 --fault-rate 0.3
ERP_MOCK_ENABLED=false ERP_URL=http://127.0.0.1:8790/mock-erp .venv/bin/restock run --port 8780
.venv/bin/python scripts/gateway_sim.py --url http://127.0.0.1:8780 --port 8781 --interval 3
```

`scripts/gateway_sim.py --help` lists its options (`--bins`, `--seed`, `--misbehave`, ...).
It talks to the service over HTTP only and prints every post as a JSON line (`ts`, `bin`,
`event_id`, `status`, `decision`). After about 90 seconds of that run (fault rate 0.3), the
service had placed 23 orders and delivered 21, after 28 ERP calls: one 429, four 503s, one
answer held back past the timeout (the retry got the same order back) and one 422, which left
that order under "Needs a person"; the gateway had brought 19 refills, each recorded as
`recovered`.

The screenshots below are from the Compose demo profile after a few rounds, with the gateway
paused: one order the ERP refused (it needs a person), two bins low with their orders
delivered and waiting for the refill, the rest in stock.

![The operations page during the living demo](docs/screenshots/page-living-demo.png)

![The gateway control page](docs/screenshots/gateway-sim.png)

![The ERP status page](docs/screenshots/erp-sim.png)

## Tests

```bash
make test        # whole suite on a throwaway SQLite file (about 35 s)
make test-pg     # the same suite on PostgreSQL; defaults to the compose restock_test database
TEST_DATABASE_URL=postgresql+psycopg://USER:PASS@HOST:5432/DBNAME make test-pg   # any other
make lint        # ruff check + ruff format --check
```

`make test-pg` starts by migrating the target database down to base (dropping the tables),
so point it at a dedicated database. The suite covers authentication, validation, the fixture
sequence, ordering and tie-break, eligibility edge cases, the partial unique index, ERP
classification and retries, manual retry, read endpoints, health, logs, the operations page,
and migration-vs-model drift checks (columns and indexes on both databases, CHECK constraints
by name on both and by text on SQLite, and on PostgreSQL the CHECK definitions, the partial
index and the collations). The concurrency and delivery tests start the real app under
uvicorn in a thread and send parallel HTTP requests from threads. The five tests in
`tests/test_concurrency.py` also assert that at least half of each batch was in the server at
once; the live delivery tests, the ten simultaneous Retry clicks included, do not.

The two targets check different locks. On SQLite every write transaction starts with
`BEGIN IMMEDIATE`, which already serialises them, so `make test` cannot notice if the
PostgreSQL row lock (`SELECT ... FOR UPDATE` on the item's state row) is removed: with it
removed, the concurrency tests still pass on SQLite and fail on PostgreSQL. Run
`make test-pg` for any change to locking or transactions. `make test` skips the two tests
that only mean something on PostgreSQL (the isolation level and the schema as PostgreSQL
reports it).

The screenshots in `docs/screenshots/` are from the demo profile (`make demo-up`), taken with headless Chrome.

## Configuration

All settings come from environment variables (or `.env`):

| Variable | Default | Meaning |
| --- | --- | --- |
| `DATABASE_URL` | `sqlite:///./restock.db` | SQLAlchemy URL; Postgres: `postgresql+psycopg://user:pass@host:5432/db` |
| `WEBHOOK_SECRET` | (required) | HMAC secret shared with the gateway |
| `WEBHOOK_REPLAY_WINDOW_SECONDS` | `300` | max distance between `X-Webhook-Timestamp` and server time, either way |
| `WEBHOOK_MAX_BODY_BYTES` | `65536` | largest webhook body accepted; a larger one is 413 `body_too_large`, refused before it is read |
| `OBSERVED_AT_MAX_FUTURE_SECONDS` | `300` | how far an event's `observed_at` may be ahead of server time; a later one is a 422 on `observed_at` and nothing is stored |
| `ERP_URL` | `http://127.0.0.1:8000/mock-erp` | orders are POSTed to `{ERP_URL}/orders` |
| `ERP_TIMEOUT_SECONDS` | `5` | httpx timeout for each step of an ERP call (connect, send, each read), not for the whole call: an answer that trickles in can take longer |
| `ERP_MAX_ATTEMPTS` | `6` | automatic attempts before a request is marked failed (`exhausted`) |
| `ERP_BACKOFF_BASE_SECONDS` | `1` | backoff = base * 2^(attempt-1), capped, with jitter |
| `ERP_BACKOFF_CAP_SECONDS` | `30` | backoff cap |
| `ERP_RETRY_AFTER_MAX_SECONDS` | `300` | longest wait a `Retry-After` header can impose |
| `ERP_MOCK_ENABLED` | `true` | mount the mock ERP and its control endpoint at `/mock-erp`; set `false` wherever `ERP_URL` is a real ERP (every `/mock-erp` route then answers 404) |
| `LOG_LEVEL` | `INFO` | JSON logs on stdout |
| `WORKER_ENABLED` | `true` | run the delivery worker thread in the web process |

To run on another port, give make the port: `make run PORT=8001` and `make seed PORT=8001`.
`make run` also points `ERP_URL` at the mock ERP on that port unless `ERP_URL` is set in the
environment; an `ERP_URL` in `.env` does not count there, because the environment wins over
`.env` and `make run` always sets it. Without make, move `ERP_URL` yourself:
`ERP_URL=http://127.0.0.1:8001/mock-erp .venv/bin/restock run --port 8001`, then
`RESTOCK_URL=http://127.0.0.1:8001 make seed`.

## API

| Method and path | What it does |
| --- | --- |
| `POST /api/v1/inventory-events` | signed webhook: 201 `accepted`, 200 `duplicate`, 401, 413, 422 |
| `GET /api/v1/rooms/{room_id}/inventory[?facility_id=]` | current state per SKU, thresholds, status, last observation, open request |
| `GET /api/v1/items/{sku}/history?room_id=[&facility_id=&limit=&cursor=]` | observations newest first, cursor pagination |
| `GET /api/v1/outbound-attempts?status=failed\|all[&request_id=&limit=]` | `failed`: the attempt that failed each request, with the ERP's answer; `all`: every attempt |
| `POST /api/v1/outbound-attempts/{id}/retry` | safe manual retry: 202 on the click that queued it, 200 after, 409 `superseded` once the item has a newer request |
| `GET /health/live`, `GET /health/ready` | process up; database, schema head and worker progress (503 within 6 s). The ERP is not checked on purpose |
| `GET /` | the operations page: three counts, "Needs a person" with Retry, "On their way", one card per bin with its last five minutes of stock as a chart; refreshes every 1.5 s |

Errors are `{"error": "<code>", "message": "..."}`; a 422 adds `errors`, one entry per
problem. `/docs` declares every response. What each endpoint does, does not do and how it
fails is in [docs/DESIGN.md](docs/DESIGN.md); the decision words (`none`, `recovered`,
`request_created`, `blocked_open_request`, `blocked_not_armed`, `stale`) and the rules
behind them are in the [report](docs/REPORT.md).

Send fixtures by hand:

```bash
export WEBHOOK_SECRET=assessment-secret
.venv/bin/python scripts/send.py send fixtures/inbound/01_normal_stock.json
.venv/bin/python scripts/send.py send --concurrent fixtures/inbound/08_concurrent_a.json fixtures/inbound/09_concurrent_b.json
.venv/bin/python scripts/send.py count ROOM-9Z MED-90001 2 --par 12 --reorder 4   # one new count, observed now
```

Or with curl and the supplied tool (`--data-binary` keeps the body byte-for-byte):

```bash
TS=$(date +%s)
SIG=$(python3 tools/sign_payload.py fixtures/inbound/02_low_stock.json assessment-secret \
      --timestamp "$TS" | sed -n 's/^X-Webhook-Signature: //p')
curl -sS -X POST http://127.0.0.1:8000/api/v1/inventory-events \
  -H 'Content-Type: application/json' -H "X-Webhook-Timestamp: $TS" -H "X-Webhook-Signature: $SIG" \
  --data-binary @fixtures/inbound/02_low_stock.json
```

## Driving the mock ERP

The mock answers from `fixtures/outbound/erp_scenarios.json`: `success`,
`duplicate_idempotency_key`, `rate_limited` (429, `Retry-After: 2`), `temporary_failure`
(503), `permanent_rejection` (422) and `timeout_after_accept` (stores the order, then holds
the answer past `ERP_TIMEOUT_SECONDS`). A repeated Idempotency-Key gets 200 `duplicate: true`
with the stored order id; the error scenarios answer before the key lookup. Orders are
numbered ERP-7001, ERP-7002, ... and kept in memory, so a restart forgets them.

Queue scenarios for the worker's next calls (consumed in order, then `success`; the queue is
global, so wait until "On their way" is empty first), then send a new count that creates a
request:

```bash
curl -sS -X POST http://127.0.0.1:8000/mock-erp/_control -H 'Content-Type: application/json' \
  -d '{"scenarios": ["permanent_rejection"]}'
WEBHOOK_SECRET=assessment-secret .venv/bin/python scripts/send.py count ROOM-9Z MED-90001 2 --par 12 --reorder 4
curl -sS 'http://127.0.0.1:8000/api/v1/outbound-attempts?status=failed'
```

The request fails with 422 `unknown_sku` and appears under "Needs a person"; press Retry and
it is delivered under the same key. `GET /mock-erp/_control` shows the queue, the orders by
key and the received calls; `-d '{"scenarios": []}'` clears the queue (`make seed` does the
same). In the demo profile the control endpoint is on the ERP simulator (port 8090) instead.

## Logs

One JSON object per line on stdout: `ts`, `level`, `logger`, `msg`, plus `correlation_id`,
`event_id`, `replenishment_id` and `attempt_no` when known; an attempt caused by a Retry
click also carries that click's id as `manual_retry_correlation_id`. The correlation id is
the caller's `X-Request-ID`, else the event's `metadata.correlation_id`, else a new uuid
(either of the first two only if it is a safe token, since it goes into headers). It is
echoed as `X-Request-ID`, stored on the observation and sent to the ERP as
`X-Correlation-ID`, so one id traces event, request and ERP calls.

## Troubleshooting

| Symptom | Cause and fix |
| --- | --- |
| `ensurepip is not available` from `make install` | the system Python has no venv module: on Debian/Ubuntu `sudo apt install python3.12-venv`, then `rm -rf .venv` and `make install` again; or make the venv with uv (Quick start) |
| `RuntimeError: WEBHOOK_SECRET is not set` | set it in `.env` or the environment |
| `RuntimeError: WEBHOOK_SECRET is still the .env.example placeholder 'change-me'` | set your own secret in `.env` or the environment (`assessment-secret` to replay the supplied fixtures) |
| `required variable WEBHOOK_SECRET is missing a value` from `make compose-up` | Compose has no default for the secret: `WEBHOOK_SECRET=assessment-secret make compose-up` |
| 401 `invalid_signature` from the scripts | the server and the script use different secrets; use the same `WEBHOOK_SECRET` for both |
| 401 `stale_timestamp` | headers older than 300 s (re-run `tools/sign_payload.py` right before sending) or clock skew |
| `/health/ready` is 503 with `migrations: at [] ...` | run `make migrate` against the same `DATABASE_URL` the server uses |
| `/health/ready` is 503 with `database: error: ...` | the database is down or unreachable from the app. The log line `readiness check failed` carries the driver's error (for example `connection timeout expired`); check `DATABASE_URL` and that the database is running. A PostgreSQL connection attempt gives up after 5 s and a probe after 6 s (`no answer within 6s`) |
| `/health/ready` is 503 with `worker: stalled` | the worker thread is running but has not completed a polling pass for 5 s, or one ERP call has run longer than the claim lease (`4 * ERP_TIMEOUT_SECONDS + 30 s`). Look in the log for `outbound worker tick failed` (usually the database; the `database` and `migrations` checks then say more) or for an ERP call that never finishes. It clears by itself once a pass completes |
| `/health/ready` is 503 with `worker: stopped` | the worker thread is not running: the service is shutting down, or the thread died on an error its loop could not catch (Python prints that traceback on stderr, outside the JSON log). Restart the service |
| `make seed` says `server at ... is not ready` | it waited 15 s for `/health/ready`: start the server (`make migrate`, then `make run`) or `make compose-up`; with Compose, see `docker compose logs app` |
| `make seed` stops after step 5: `request for evt_1002 not delivered in time` | the worker did not deliver the step-2 request within 20 s: `WORKER_ENABLED=false`, `ERP_MOCK_ENABLED=false` while `ERP_URL` still points at the mock (the seed prints a note), or `ERP_URL` does not reach the mock (next rows); see `GET /api/v1/outbound-attempts?status=all`. Fix it and restart the server (press Retry on the page if that request already failed), then run `make seed` again: steps 1-5 come back as duplicates and the rest runs |
| Step 6 says `blocked_open_request` when you send the fixtures by hand | the step-2 request was still pending or in flight when 06 arrived (`make seed` waits for its delivery); check `open_request` in `GET /api/v1/rooms/ROOM-3A/inventory`. The item stays armed, so the next newer low event after the request is delivered or fails starts the new cycle |
| Requests end `failed` (`last_error: exhausted`) with `ConnectError` or `ConnectTimeout` attempts | `ERP_URL` does not reach a running ERP/mock (e.g. a different port). Each attempt is `ambiguous` and retried; with the defaults the request fails after 6 attempts (I measured 26 s for a host that does not resolve, 55 s when every connect times out) and shows on the page under "Needs a person" ("Not confirmed after 6 tries"). Fix `ERP_URL`, restart, then press Retry |
| Requests end `failed` (`last_error: exhausted`) with attempts that got HTTP 200 or 201 and the error `2xx without erp_order_id` | `ERP_URL` reaches something that answers 2xx but is not the ERP's order endpoint (a proxy, a web server, a wrong path), so no answer names an order. Fix `ERP_URL`, restart, then press Retry; the same Idempotency-Key goes to the ERP |
| The page says `No contact with the server since ... Tell IT if this lasts.` and is greyed out | its refresh (the page refreshes every 1.5 seconds) failed: the server stopped or cannot be reached from the browser. Point at the line for the technical reason; the page recovers by itself on the next refresh that works |
| Seed shows only duplicates | the database already has the fixtures: stop the server, `rm restock.db`, `make migrate`, start it again (a running server keeps writing to the deleted file) |
| `address already in use` | another server on 8000: stop it, or use another port: `make run PORT=8001` and `make seed PORT=8001` (see Configuration) |
| `make compose-up` fails with `address already in use` for 5432 | a local PostgreSQL holds the port: stop it, or change the host side of `"5432:5432"` in `docker-compose.yml` (and `PG_TEST_URL` for `make test-pg`) |
| `make test-pg` cannot connect | start Postgres: `WEBHOOK_SECRET=assessment-secret make compose-up`, or the database alone with `WEBHOOK_SECRET=unused docker compose up -d db` (Compose reads the whole file, which requires the secret, for every command); or set `TEST_DATABASE_URL` |
| `no such column: replenishment_requests.manual_retry_correlation_id` (SQLite), `column ... does not exist` (PostgreSQL), a CHECK constraint error on a recovery event, or a 500 for a low event that follows a failed request | the database was created by an earlier copy of migration 0001, which I changed in place before release (its open-request index still counts a failed request): stop the server, delete the database (`rm restock.db`, or `make compose-down`), `make migrate`, start it again |

## Layout

```text
restock/        main (app factory + CLI), api (routes + webhook auth), inbound (rules + ingest),
                outbound (worker), schemas (tables + API shapes), runtime (settings, db, logging),
                erp_mock (mock ERP + the demo's ERP simulator), page (operations page)
alembic/        the single migration (0001) ; alembic.ini
templates/      index.html (the operations page)
tests/          pytest suite (TestClient + live uvicorn server)
scripts/        send.py (send fixtures, one count by hand, or seed the FIXTURE_GUIDE sequence) ;
                gateway_sim.py (the living demo's gateway)
docs/           REPORT.md ; DESIGN.md ; screenshots/
docker/         postgres-init.sql (the compose database for make test-pg)
fixtures/ tools/ examples/   the supplied assessment files, unchanged
```
