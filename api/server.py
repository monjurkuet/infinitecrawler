"""FastAPI app creation with lifespan, CORS, auth middleware."""

import logging

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.dependencies import _API_TOKEN
from api.routers import configs, leads, monitor, search, system, tasks
from api.services import pg_service, redis_service, task_runner

log = logging.getLogger("api.server")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Server startup/shutdown."""
    log.info("Starting infinitecrawler API server...")

    # Initialize PG pool
    try:
        pg_pool = await pg_service.create_pool()
        await pg_service.ensure_tasks_table()
        log.info("PostgreSQL pool ready")
    except Exception as e:
        log.error(f"Failed to connect to PostgreSQL: {e}")
        log.warning("API will start but PG queries will fail")

    # Initialize Redis
    try:
        await redis_service.create_client()
        log.info("Redis client ready")
    except Exception as e:
        log.error(f"Failed to connect to Redis: {e}")
        log.warning("API will start but Redis queries will fail")

    # Restore running tasks as failed
    try:
        await task_runner.restore_tasks()
    except Exception as e:
        log.warning(f"Task restore skipped: {e}")

    api_token = _API_TOKEN if _API_TOKEN != "changeme" else "[NOT SET — authentication disabled]"
    log.info(f"API token: {api_token}")

    yield  # Server runs here

    # Shutdown
    log.info("Shutting down API server...")
    await task_runner.kill_all()
    await pg_service.close_pool()
    await redis_service.close_client()


def create_app() -> FastAPI:
    app = FastAPI(
        title="InfiniteCrawler API",
        description="REST API for the BD lead pipeline — manage scraping tasks, monitor crawlers, query leads.",
        version="1.0.0",
        lifespan=lifespan,
    )

    # CORS — allow all origins (LAN/VPN only by default)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Register routers
    app.include_router(tasks.router)
    app.include_router(leads.router)
    app.include_router(search.router)
    app.include_router(monitor.router)
    app.include_router(configs.router)
    app.include_router(system.router)

    # Root
    @app.get("/")
    async def root():
        return {
            "service": "InfiniteCrawler API",
            "version": "1.0.0",
            "docs": "/docs",
            "openapi": "/openapi.json",
        }

    # Global error handler
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        log.error(f"Unhandled error: {exc}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"detail": f"Internal server error: {exc}"},
        )

    return app


app = create_app()
