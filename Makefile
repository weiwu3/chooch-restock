PYTHON ?= python3.12
VENV := .venv
BIN := $(VENV)/bin
# Port for `make run` and `make seed`, e.g. `make run PORT=8001` and `make seed PORT=8001`.
PORT ?= 8000
# The compose Postgres (make compose-up) creates this database for the test suite.
PG_TEST_URL ?= postgresql+psycopg://restock:restock@localhost:5432/restock_test
# Compose reads the whole file for every command, and the file requires WEBHOOK_SECRET.
# Commands that do not start the app get a stand-in when it is not set.
COMPOSE_NO_SECRET = WEBHOOK_SECRET="$${WEBHOOK_SECRET:-unused}" docker compose

.PHONY: install migrate run seed test test-pg compose-up compose-seed compose-down demo-up \
	demo-down lint

install:  ## create .venv and install the pinned dependencies, test and lint tools included
	$(PYTHON) -m venv $(VENV)
	$(BIN)/pip install --upgrade pip
	$(BIN)/pip install -r requirements-dev.txt
	$(BIN)/pip install --no-deps -e .

migrate:  ## apply migrations to DATABASE_URL (alembic upgrade head)
	$(BIN)/restock migrate

run:  ## start the API, page and delivery worker on http://127.0.0.1:$(PORT)
	ERP_URL="$${ERP_URL:-http://127.0.0.1:$(PORT)/mock-erp}" $(BIN)/restock run --port $(PORT)

seed:  ## replay the FIXTURE_GUIDE sequence against the server on $(PORT)
	RESTOCK_URL="$${RESTOCK_URL:-http://127.0.0.1:$(PORT)}" $(BIN)/python scripts/send.py seed

test:  ## run the test suite on a throwaway SQLite database
	env -u TEST_DATABASE_URL $(BIN)/pytest

test-pg:  ## run the same suite against PostgreSQL (TEST_DATABASE_URL or the compose db)
	TEST_DATABASE_URL=$${TEST_DATABASE_URL:-$(PG_TEST_URL)} $(BIN)/pytest

compose-up:  ## start Postgres and the app in Docker; returns once both are healthy (port 8000)
	docker compose up -d --build --wait

compose-seed:  ## replay the FIXTURE_GUIDE sequence inside the app container (no host Python)
	$(COMPOSE_NO_SECRET) exec -T app env RESTOCK_URL=http://127.0.0.1:8000 python scripts/send.py seed

compose-down:  ## stop the containers and delete the Postgres volume
	$(COMPOSE_NO_SECRET) down -v

demo-up:  ## the living demo: db + app + ERP simulator (8090) + gateway simulator (8081)
	DEMO_ERP_URL=http://erp:8090/mock-erp DEMO_ERP_MOCK_ENABLED=false \
		docker compose --profile demo up -d --build --wait

demo-down:  ## stop the living demo and delete the Postgres volume
	$(COMPOSE_NO_SECRET) --profile demo down -v

lint:  ## ruff lint and format check
	$(BIN)/ruff check .
	$(BIN)/ruff format --check .
