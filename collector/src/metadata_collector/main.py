"""FastAPI application entry point."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pythonjsonlogger.jsonlogger import JsonFormatter

from metadata_collector.api import connection_router, lineage_router, search_router
from metadata_collector.db import close_pool, get_pool, init_pool
from metadata_collector.settings import get_settings


def setup_logging(level: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s"
    ))
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level.upper())


@asynccontextmanager
async def lifespan(_app: FastAPI):
    settings = get_settings()
    setup_logging(settings.log_level)
    logger = logging.getLogger(__name__)
    logger.info("Starting metadata-collector", extra={"version": "0.1.0"})

    await init_pool(
        dsn=settings.metadata_db_url,
        min_size=settings.db_pool_min_size,
        max_size=settings.db_pool_max_size,
    )
    try:
        yield
    finally:
        await close_pool()
        logger.info("Stopped metadata-collector")


app = FastAPI(
    title="Metadata Collector",
    version="0.1.0",
    description=(
        "OpenLineage-native metadata collector with Postgres backend. "
        "Supports lineage ingestion and search by connection / dataset."
    ),
    lifespan=lifespan,
)

settings = get_settings()
app.include_router(lineage_router.router, prefix=settings.api_prefix)
app.include_router(connection_router.router, prefix=settings.api_prefix)
app.include_router(search_router.router, prefix=settings.api_prefix)


@app.get("/health", tags=["health"])
async def health() -> dict[str, str]:
    """Liveness + readiness probe."""
    try:
        pool = get_pool()
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"status": "ok"}
    except Exception as e:
        return {"status": "degraded", "error": str(e)}


@app.get("/", tags=["health"])
async def root() -> dict[str, str]:
    return {
        "service": "metadata-collector",
        "version": "0.1.0",
        "docs": "/docs",
    }
