"""The app factory, the request middleware and the `restock` command line."""

import argparse
import asyncio
import logging
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from http.client import responses as http_phrases

import uvicorn
from alembic import command
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.datastructures import MutableHeaders
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from restock import api, erp_mock, page, runtime
from restock.erp_mock import MockErp
from restock.outbound import OutboundWorker
from restock.runtime import (
    Settings,
    alembic_config,
    current_revisions,
    make_engine,
    make_sessionmaker,
    migration_heads,
)
from restock.schemas import SAFE_CORRELATION_ID, format_errors

log = logging.getLogger("restock")
access_log = logging.getLogger("restock.http")


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    secret = settings.webhook_secret.get_secret_value() if settings.webhook_secret else ""
    if not secret:
        raise RuntimeError("WEBHOOK_SECRET is not set; see .env.example")
    if secret == "change-me":  # anyone who read the repository could sign events with it
        raise RuntimeError(
            "WEBHOOK_SECRET is still the .env.example placeholder 'change-me'; set your own "
            "secret (the supplied fixtures are signed with assessment-secret)"
        )
    runtime.configure_logging(settings.log_level)
    logging.getLogger("alembic").setLevel(logging.WARNING)  # readiness reads the schema version

    engine = make_engine(settings.database_url)
    sessions = make_sessionmaker(engine)
    app = FastAPI(
        title="restock",
        version="0.1.0",
        summary="Inventory observations in, one logical replenishment order out.",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.engine = engine
    app.state.sessions = sessions
    app.state.worker = OutboundWorker(sessions, settings) if settings.worker_enabled else None
    app.state.migration_heads = migration_heads()
    if settings.erp_mock_enabled:
        app.state.mock_erp = MockErp(erp_mock.SCENARIOS_PATH, settings.erp_timeout_seconds)

    app.add_middleware(CorrelationMiddleware)
    app.add_exception_handler(api.ApiError, _api_error)
    app.add_exception_handler(RequestValidationError, _request_validation_error)
    app.add_exception_handler(StarletteHTTPException, _http_error)
    app.include_router(api.router)
    if settings.erp_mock_enabled:
        app.include_router(erp_mock.router)
    app.include_router(page.router)
    return app


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    state = app.state
    try:
        current = current_revisions(state.engine)
    except Exception as exc:
        log.error("cannot read the schema version", extra={"error": repr(exc)})
    else:
        if set(current) != set(state.migration_heads):
            log.error(
                "database schema is not at head; /health/ready will report 503 until you run "
                "`restock migrate`",
                extra={"current": list(current), "head": list(state.migration_heads)},
            )
    if state.worker is not None:
        state.worker.start()
    yield
    if state.worker is not None:
        await asyncio.to_thread(state.worker.stop)  # may wait for an ERP call; off the loop
    state.engine.dispose()


class CorrelationMiddleware:
    """Gives each request a correlation id (the caller's X-Request-ID when it is a safe token,
    else a new uuid), binds it to the log context, echoes it in X-Request-ID, answers an
    unexpected error with a 500 that carries it, and writes one access line. A plain ASGI
    class, so it sees only the first message of each response."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        incoming = dict(scope["headers"]).get(b"x-request-id", b"").decode("latin-1")
        from_header = bool(SAFE_CORRELATION_ID.fullmatch(incoming))
        scope.setdefault("state", {})["correlation_from_header"] = from_header
        token = runtime.bind(correlation_id=incoming if from_header else str(uuid.uuid4()))
        context = runtime.current()  # the webhook may swap in the event's own id; same dict
        started = time.monotonic()
        status = 500
        response_started = False

        async def send_with_id(message: Message) -> None:
            nonlocal status, response_started
            if message["type"] == "http.response.start":
                status = message["status"]
                MutableHeaders(scope=message)["X-Request-ID"] = context["correlation_id"]
                response_started = True
            await send(message)

        try:
            await self.app(scope, receive, send_with_id)
        except Exception:
            # Answered here, not by an app-level handler: Starlette runs that outside the
            # middleware, after the context is reset, so its 500 had no correlation id.
            log.exception("unhandled error")
            if response_started:
                raise
            body = api.error_body(
                "internal_error",
                "unexpected server error; see the logs for this correlation id",
                correlation_id=context["correlation_id"],
            )
            await JSONResponse(body, status_code=500)(scope, receive, send_with_id)
        finally:
            access_log.info(
                "http request",
                extra={
                    "method": scope["method"],
                    "path": scope["path"],
                    "status": status,
                    "duration_ms": int((time.monotonic() - started) * 1000),
                },
            )
            runtime.reset(token)


# The exception handlers: the error envelope for errors that leave a route as exceptions.


async def _api_error(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, api.ApiError)
    return JSONResponse(api.error_body(exc.code, exc.message), status_code=exc.status_code)


async def _request_validation_error(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, RequestValidationError)  # a bad path or query parameter
    return api.validation_error_response(format_errors(list(exc.errors())))


async def _http_error(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, StarletteHTTPException)  # 404 unknown path, 405 wrong method
    code = http_phrases.get(exc.status_code, "HTTP error").lower().replace(" ", "_")
    return JSONResponse(
        api.error_body(code, str(exc.detail)), status_code=exc.status_code, headers=exc.headers
    )


def cli(argv: list[str] | None = None) -> None:
    """restock migrate | run [--host --port] | erp-sim [--host --port --fault-rate --seed]"""
    parser = argparse.ArgumentParser(prog="restock")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("migrate", help="apply database migrations (alembic upgrade head)")
    run = commands.add_parser("run", help="start the HTTP service and the delivery worker")
    run.add_argument("--host", default="127.0.0.1")
    run.add_argument("--port", type=int, default=8000)
    sim = commands.add_parser("erp-sim", help="start the ERP simulator of the living demo")
    sim.add_argument("--host", default="127.0.0.1")
    sim.add_argument("--port", type=int, default=8090)
    sim.add_argument(
        "--fault-rate", type=float, help="share of calls answered badly (ERP_SIM_FAULT_RATE, 0.2)"
    )
    sim.add_argument("--seed", type=int, help="repeat the same fault schedule")
    args = parser.parse_args(argv)

    settings = Settings()
    runtime.configure_logging(settings.log_level)
    if args.command == "migrate":
        command.upgrade(alembic_config(settings.database_url), "head")
        engine = make_engine(settings.database_url)
        log.info("database is at head", extra={"revision": list(current_revisions(engine))})
        engine.dispose()
        return
    if args.command == "erp-sim":
        rate = erp_mock.fault_rate_from_env() if args.fault_rate is None else args.fault_rate
        app = erp_mock.create_erp_sim_app(rate, settings.erp_timeout_seconds, args.seed)
    else:
        app = create_app(settings)
    # log_config=None keeps the JSON logging; the middleware writes the access line.
    uvicorn.run(app, host=args.host, port=args.port, log_config=None, access_log=False)
