"""Premium API server (port 8016) — own app, own auth (JWT), shares PG+Redis services."""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.services import pg_service  # same pool service as internal API
from api_premium.auth.router import router as auth_router
from api_premium.premium.router import router as premium_router

log = logging.getLogger("api_premium.server")


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Premium API starting...")
    try:
        await pg_service.create_pool()
        log.info("PG pool ready")
    except Exception:
        log.exception("PG pool failed")
    yield
    log.info("Premium API shutdown")
    try:
        await pg_service.close_pool()
    except Exception:
        log.exception("PG pool close failed")


def create_app() -> FastAPI:
    app = FastAPI(
        title="InfiniteCrawler Premium API",
        version="1.0.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["Content-Disposition"],
    )

    app.include_router(auth_router)
    app.include_router(premium_router)

    @app.get("/")
    async def root():
        return {
            "service": "infinitecrawler-premium-api",
            "docs": "/docs",
            "auth": ["/auth/register", "/auth/login", "/auth/me"],
            "premium": ["/premium/leads", "/premium/leads/{id}", "/premium/export.csv", "/premium/stats"],
        }

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app


app = create_app()
