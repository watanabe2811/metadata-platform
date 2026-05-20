"""Postgres async connection pool."""
from __future__ import annotations

import json
import logging
from typing import AsyncIterator

import asyncpg

logger = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None


async def _setup_codecs(conn: asyncpg.Connection) -> None:
    """Register codecs so JSONB is returned as dict, not str."""
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads,
        schema="pg_catalog", format="text",
    )
    await conn.set_type_codec(
        "json", encoder=json.dumps, decoder=json.loads,
        schema="pg_catalog", format="text",
    )


async def init_pool(dsn: str, min_size: int = 2, max_size: int = 10) -> asyncpg.Pool:
    """Initialize the global connection pool."""
    global _pool
    if _pool is not None:
        return _pool

    clean_dsn = dsn.replace("postgresql+psycopg2://", "postgresql://")
    clean_dsn = clean_dsn.replace("postgresql+asyncpg://", "postgresql://")

    logger.info("Initializing Postgres pool (min=%d, max=%d)", min_size, max_size)
    _pool = await asyncpg.create_pool(
        dsn=clean_dsn,
        min_size=min_size,
        max_size=max_size,
        command_timeout=30,
        init=_setup_codecs,
    )
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("Postgres pool closed")


def get_pool() -> asyncpg.Pool:
    """Return the initialized pool. Raises if not initialized."""
    if _pool is None:
        raise RuntimeError("DB pool not initialized; call init_pool() at startup")
    return _pool


async def acquire() -> AsyncIterator[asyncpg.Connection]:
    """FastAPI dependency yielding a connection from the pool."""
    pool = get_pool()
    async with pool.acquire() as conn:
        yield conn
