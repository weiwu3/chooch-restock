"""Fixtures."""

import logging
import os
import threading
from collections.abc import Callable, Iterator
from typing import Any

import pytest
import uvicorn
from alembic import command
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import text

from restock.main import create_app
from restock.runtime import Settings, alembic_config, make_engine

from .helpers import (
    TABLES,
    InFlightProbe,
    ListHandler,
    LiveServer,
    free_port,
    make_settings,
    wait_for,
)


@pytest.fixture(scope="session")
def database_url(tmp_path_factory: pytest.TempPathFactory) -> str:
    """TEST_DATABASE_URL (PostgreSQL) migrated down to base and back up, or a new SQLite file
    migrated up; either way at the head revision, once for the whole run."""
    url = os.environ.get("TEST_DATABASE_URL")
    if url:
        config = alembic_config(url)
        command.downgrade(config, "base")  # also exercises the downgrade path
        command.upgrade(config, "head")
        return url
    url = f"sqlite:///{tmp_path_factory.mktemp('db') / 'restock-test.db'}"
    command.upgrade(alembic_config(url), "head")
    return url


@pytest.fixture(autouse=True)
def clean_database(database_url: str) -> None:
    """Every test starts with empty tables; TABLES lists children before parents."""
    engine = make_engine(database_url)
    with engine.begin() as conn:
        for table in TABLES:
            conn.execute(text(f"DELETE FROM {table}"))
    engine.dispose()


@pytest.fixture
def settings(database_url: str) -> Settings:
    return make_settings(database_url)


@pytest.fixture
def app(settings: Settings) -> FastAPI:
    return create_app(settings)


@pytest.fixture
def client(app: FastAPI) -> Iterator[TestClient]:
    # Used as a context manager, TestClient also runs the app's startup and shutdown
    # (lifespan) around the test.
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def live_server(database_url: str) -> Iterator[Callable[..., LiveServer]]:
    """Start the real app under uvicorn in a thread, worker on, ERP_URL pointing at its own mock
    ERP, so delivery goes over real sockets with real timeouts."""
    started: list[LiveServer] = []

    def start(**overrides: Any) -> LiveServer:
        # The ERP URL is this server's own mock ERP; a test may override any setting.
        port = free_port()
        url = f"http://127.0.0.1:{port}"
        settings = make_settings(
            database_url, **{"erp_url": f"{url}/mock-erp", "worker_enabled": True, **overrides}
        )
        app = create_app(settings)
        probe = InFlightProbe(app)  # wraps the app to count requests in progress at once
        config = uvicorn.Config(
            probe, host="127.0.0.1", port=port, log_config=None, access_log=False
        )
        server = uvicorn.Server(config)
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        wait_for(lambda: server.started, timeout=10, what="uvicorn to start")
        live = LiveServer(url, app, server, thread, probe)
        started.append(live)
        return live

    yield start
    for live in started:
        live.stop()


@pytest.fixture
def json_logs() -> Iterator[ListHandler]:
    """A handler on the root logger for the length of the test; its .lines holds every log line
    written meanwhile, parsed from JSON."""
    handler = ListHandler()
    logging.getLogger().addHandler(handler)
    yield handler
    logging.getLogger().removeHandler(handler)
