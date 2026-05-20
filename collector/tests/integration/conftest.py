"""Integration test fixtures — require a live Postgres.

Each test gets an asyncpg connection with an open transaction that is rolled
back after the test, so tests are fully isolated without truncating tables.

Set TEST_DB_URL to override the default:
    TEST_DB_URL=postgresql://metadata:metadata@localhost:5432/metadata pytest
"""
from __future__ import annotations

import json
import os

import asyncpg
import pytest
import pytest_asyncio

TEST_DB_URL = os.getenv(
    "TEST_DB_URL",
    "postgresql://metadata:metadata@localhost:5432/metadata",
)


@pytest.fixture(scope="session")
def db_pool():
    """Sentinel fixture: checks Postgres connectivity at session start.

    Returns the DSN string so dependent fixtures can open their own connections.
    Skips all integration tests if Postgres is unreachable.
    """
    import socket
    from urllib.parse import urlparse

    parsed = urlparse(TEST_DB_URL)
    host = parsed.hostname or "localhost"
    port = parsed.port or 5432
    try:
        s = socket.create_connection((host, port), timeout=3)
        s.close()
    except OSError as e:
        pytest.skip(f"Postgres not available at {TEST_DB_URL}: {e}")
    return TEST_DB_URL


@pytest_asyncio.fixture
async def db_conn(db_pool: str):
    """Per-test connection with a rolled-back transaction for test isolation."""
    conn = await asyncpg.connect(dsn=db_pool, command_timeout=30)
    await _setup_codecs(conn)
    tr = conn.transaction()
    await tr.start()
    yield conn
    await tr.rollback()
    await conn.close()


async def _setup_codecs(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads,
        schema="pg_catalog", format="text",
    )
    await conn.set_type_codec(
        "json", encoder=json.dumps, decoder=json.loads,
        schema="pg_catalog", format="text",
    )


# ---------------------------------------------------------------------------
# Shared factory helpers
# ---------------------------------------------------------------------------

async def make_connection(
    conn: asyncpg.Connection,
    *,
    logical_name: str = "test-oracle",
    platform: str = "oracle",
    host: str | None = "db.test.local",
    port: int | None = 1521,
    classification: str | None = "internal",
    owner_team: str | None = "test-team",
) -> dict:
    from metadata_collector.repositories.connection_repo import ConnectionRepository
    repo = ConnectionRepository()
    cid = await repo.create(
        conn,
        logical_name=logical_name,
        platform=platform,
        host=host,
        port=port,
        classification=classification,
        owner_team=owner_team,
    )
    return await repo.get_by_logical_name(conn, logical_name)  # type: ignore[return-value]


async def make_job(
    conn: asyncpg.Connection,
    *,
    namespace: str = "test.ns",
    name: str = "test-job",
    job_type: str = "python",
) -> dict:
    from metadata_collector.repositories.job_repo import JobRepository
    repo = JobRepository()
    jid = await repo.upsert(conn, namespace=namespace, name=name, job_type=job_type)
    row = await conn.fetchrow("SELECT * FROM job WHERE id = $1", jid)
    return dict(row)  # type: ignore[arg-type]


async def make_dataset(
    conn: asyncpg.Connection,
    *,
    connection_id,
    name: str = "test_table",
    fqn: str | None = None,
    dataset_type: str = "table",
) -> dict:
    from metadata_collector.repositories.dataset_repo import DatasetRepository
    repo = DatasetRepository()
    did = await repo.upsert(
        conn,
        connection_id=connection_id,
        name=name,
        fqn=fqn or f"test-oracle.{name}",
        dataset_type=dataset_type,
    )
    row = await conn.fetchrow("SELECT * FROM dataset WHERE id = $1", did)
    return dict(row)  # type: ignore[arg-type]
