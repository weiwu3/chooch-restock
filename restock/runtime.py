"""Settings, the database engine and sessions, and JSON logging with a correlation context."""

import contextvars
import json
import logging
import sqlite3
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import Connection, Engine, create_engine, event
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.orm import Session, sessionmaker

# The checkout root: the app reads alembic/, templates/ and fixtures/ from it.
REPO_ROOT = Path(__file__).resolve().parent.parent
# psycopg's default connect timeout is 130 s, which held startup and readiness that long when
# the database host dropped packets.
CONNECT_TIMEOUT_SECONDS = 5


# --- settings --------------------------------------------------------------------------------


class Settings(BaseSettings):
    """Read from the environment, else .env, else these defaults. .env.example lists them."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "sqlite:///./restock.db"
    # Optional here so `restock migrate` runs without it; create_app() refuses to start
    # without it or with the "change-me" placeholder.
    webhook_secret: SecretStr | None = None
    webhook_replay_window_seconds: int = Field(300, ge=1)
    webhook_max_body_bytes: int = Field(65536, ge=1)
    # An observed_at further ahead of server time is a 422: it would freeze the bin's state.
    observed_at_max_future_seconds: int = Field(300, ge=0)
    erp_url: str = "http://127.0.0.1:8000/mock-erp"
    # The httpx timeout for each part of an ERP call (connect, send, each read), not the whole.
    erp_timeout_seconds: float = Field(5.0, gt=0)
    erp_max_attempts: int = Field(6, ge=1)
    erp_backoff_base_seconds: float = Field(1.0, ge=0)
    erp_backoff_cap_seconds: float = Field(30.0, ge=0)
    erp_retry_after_max_seconds: float = Field(300.0, ge=0)
    erp_mock_enabled: bool = True
    log_level: str = "INFO"
    worker_enabled: bool = True


# --- database ---------------------------------------------------------------------------------


def utcnow() -> datetime:
    return datetime.now(UTC)


def make_engine(url: str) -> Engine:
    """SQLite: every transaction starts with BEGIN IMMEDIATE, so writers queue instead of
    deadlocking on a read-then-write upgrade (SQLite has no row locks). PostgreSQL: READ
    COMMITTED pinned, because the inbound path must see rows committed after its transaction
    began (ON CONFLICT insert, then a SELECT of the stored or locked row)."""
    if not url.startswith("sqlite"):
        return create_engine(
            url,
            pool_pre_ping=True,
            isolation_level="READ COMMITTED",
            connect_args={"connect_timeout": CONNECT_TIMEOUT_SECONDS},
        )
    engine = create_engine(url, connect_args={"check_same_thread": False, "timeout": 30})

    @event.listens_for(engine, "connect")
    def on_connect(dbapi_conn: sqlite3.Connection, _record: Any) -> None:
        dbapi_conn.isolation_level = None  # I issue BEGIN myself (below)
        dbapi_conn.execute("PRAGMA foreign_keys = ON")

    @event.listens_for(engine, "begin")
    def on_begin(conn: Connection) -> None:
        # A read_only_connection reads alongside a writer instead of queueing behind it.
        read_only = conn.get_execution_options().get("read_only")
        conn.exec_driver_sql("BEGIN" if read_only else "BEGIN IMMEDIATE")

    return engine


def make_sessionmaker(engine: Engine) -> sessionmaker[Session]:
    # expire_on_commit=False: objects stay readable after the commit.
    return sessionmaker(engine, expire_on_commit=False)


def read_only_connection(engine: Engine) -> Connection:
    return engine.connect().execution_options(read_only=True)


def is_postgres(session: Session) -> bool:
    return session.get_bind().dialect.name == "postgresql"


def dialect_insert(session: Session) -> Callable[..., postgresql.Insert | sqlite.Insert]:
    """The dialect's insert(); only those offer on_conflict_do_nothing()."""
    return postgresql.insert if is_postgres(session) else sqlite.insert


def alembic_config(database_url: str) -> Config:
    config = Config(str(REPO_ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(REPO_ROOT / "alembic"))
    config.attributes["database_url"] = database_url
    return config


def migration_heads() -> tuple[str, ...]:
    """The newest revision(s) in alembic/versions; reads files only."""
    return tuple(ScriptDirectory.from_config(alembic_config("")).get_heads())


def current_revisions(engine: Engine) -> tuple[str, ...]:
    with read_only_connection(engine) as conn:
        return schema_revisions(conn)


def schema_revisions(conn: Connection) -> tuple[str, ...]:
    return tuple(MigrationContext.configure(conn).get_current_heads())


# --- logging ---------------------------------------------------------------------------------
#
# One JSON object per line. A log context (a dict in a ContextVar) is bound per HTTP request
# (main.py) and per delivery attempt (outbound.py); JsonFormatter copies its fields onto every
# line written meanwhile, so one correlation id joins the webhook call, its request and every
# ERP attempt for it.

CONTEXT_FIELDS = (
    "correlation_id",
    "event_id",
    "replenishment_id",
    "attempt_no",
    "manual_retry_correlation_id",
)
_context: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "restock_log_context", default=None
)
_STANDARD_ATTRS = set(vars(logging.makeLogRecord({}))) | {"message", "asctime", "color_message"}


def bind(**fields: Any) -> contextvars.Token:
    return _context.set({k: v for k, v in fields.items() if v is not None})


def update(**fields: Any) -> None:
    # Mutates the bound dict: FastAPI runs sync routes in a copied context, and the middleware
    # must still see what the route added.
    ctx = _context.get()
    if ctx is not None:
        ctx.update({k: v for k, v in fields.items() if v is not None})


def current() -> dict[str, Any]:
    return _context.get() or {}


def reset(token: contextvars.Token) -> None:
    _context.reset(token)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds")
        out: dict[str, Any] = {
            "ts": ts.replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        ctx = current()
        for key in CONTEXT_FIELDS:
            value = record.__dict__.get(key, ctx.get(key))
            if value is not None:
                out[key] = value
        # Everything the call site passed in extra= is a plain attribute on the record.
        for key, value in record.__dict__.items():
            from_extra = key not in _STANDARD_ATTRS and not key.startswith("_")
            if from_extra and key not in out and value is not None:
                out[key] = value
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)
        return json.dumps(out, default=str)


def configure_logging(level: str = "INFO") -> None:
    root = logging.getLogger()
    root.handlers = [h for h in root.handlers if not getattr(h, "_restock", False)]
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    handler._restock = True  # type: ignore[attr-defined]
    root.addHandler(handler)
    root.setLevel(level.upper())
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logging.getLogger(name).handlers.clear()
        logging.getLogger(name).propagate = True
    logging.getLogger("uvicorn.access").disabled = True  # the middleware writes the access line
    logging.getLogger("httpx").setLevel(logging.WARNING)
