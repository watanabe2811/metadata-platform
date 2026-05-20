"""Integration tests for ConnectionRepository — requires live Postgres."""
from __future__ import annotations

import pytest
import pytest_asyncio
import asyncpg

from metadata_collector.repositories.connection_repo import ConnectionRepository
from .conftest import make_connection

pytestmark = pytest.mark.integration

repo = ConnectionRepository()


class TestConnectionCRUD:
    async def test_create_and_get(self, db_conn):
        c = await make_connection(db_conn, logical_name="ora-test-crud")
        assert c["logical_name"] == "ora-test-crud"
        assert c["platform"] == "oracle"
        assert c["host"] == "db.test.local"

    async def test_get_nonexistent_returns_none(self, db_conn):
        result = await repo.get_by_logical_name(db_conn, "does-not-exist")
        assert result is None

    async def test_duplicate_logical_name_raises(self, db_conn):
        await make_connection(db_conn, logical_name="ora-dup")
        with pytest.raises(asyncpg.UniqueViolationError):
            await repo.create(db_conn, logical_name="ora-dup", platform="oracle")

    async def test_list_all_excludes_deleted(self, db_conn):
        await make_connection(db_conn, logical_name="ora-visible")
        await make_connection(db_conn, logical_name="ora-hidden")
        await repo.soft_delete(db_conn, "ora-hidden")

        results = await repo.list_all(db_conn, limit=100)
        names = {r["logical_name"] for r in results}
        assert "ora-visible" in names
        assert "ora-hidden" not in names

    async def test_soft_delete_sets_deleted_at(self, db_conn):
        await make_connection(db_conn, logical_name="ora-del")
        ok = await repo.soft_delete(db_conn, "ora-del")
        assert ok is True
        result = await repo.get_by_logical_name(db_conn, "ora-del")
        assert result is None  # get_by_logical_name filters deleted_at IS NULL

    async def test_soft_delete_nonexistent_returns_false(self, db_conn):
        ok = await repo.soft_delete(db_conn, "ghost")
        assert ok is False

    async def test_update_host(self, db_conn):
        await make_connection(db_conn, logical_name="ora-upd", host="old-host")
        updated = await repo.update(db_conn, "ora-upd", {"host": "new-host.local"})
        assert updated is not None
        assert updated["host"] == "new-host.local"

    async def test_update_ignores_unknown_columns(self, db_conn):
        await make_connection(db_conn, logical_name="ora-safe-upd")
        result = await repo.update(db_conn, "ora-safe-upd", {"nonexistent_col": "val"})
        # Should return current row unchanged
        assert result is not None
        assert result["logical_name"] == "ora-safe-upd"


class TestUpsertMinimal:
    async def test_creates_stub(self, db_conn):
        cid = await repo.upsert_minimal(db_conn, logical_name="stub-conn")
        result = await repo.get_by_logical_name(db_conn, "stub-conn")
        assert result is not None
        assert result["platform"] == "unknown"

    async def test_idempotent_on_conflict(self, db_conn):
        id1 = await repo.upsert_minimal(db_conn, logical_name="idem-conn")
        id2 = await repo.upsert_minimal(db_conn, logical_name="idem-conn")
        assert id1 == id2

    async def test_does_not_overwrite_real_connection(self, db_conn):
        await make_connection(db_conn, logical_name="real-conn", platform="oracle")
        await repo.upsert_minimal(db_conn, logical_name="real-conn")
        result = await repo.get_by_logical_name(db_conn, "real-conn")
        assert result is not None
        assert result["platform"] == "oracle"  # not overwritten to "unknown"


class TestSearch:
    async def _seed(self, db_conn):
        await make_connection(db_conn, logical_name="kafka-prod-01",  platform="kafka",
                              host=None, port=None, classification="internal",
                              owner_team="streaming-team")
        await make_connection(db_conn, logical_name="t24-core-prod", platform="oracle",
                              host="10.0.1.5", port=1521, classification="confidential",
                              owner_team="core-banking")
        await make_connection(db_conn, logical_name="pg-analytics",  platform="postgresql",
                              host="10.0.2.10", port=5432, classification="internal",
                              owner_team="data-platform")

    async def test_search_by_q_logical_name(self, db_conn):
        await self._seed(db_conn)
        results = await repo.search(db_conn, q="t24")
        names = [r["logical_name"] for r in results]
        assert "t24-core-prod" in names

    async def test_search_by_q_host(self, db_conn):
        await self._seed(db_conn)
        results = await repo.search(db_conn, q="10.0.1")
        names = [r["logical_name"] for r in results]
        assert "t24-core-prod" in names

    async def test_search_by_host_partial(self, db_conn):
        await self._seed(db_conn)
        results = await repo.search(db_conn, host="10.0.2")
        names = [r["logical_name"] for r in results]
        assert "pg-analytics" in names
        assert "t24-core-prod" not in names

    async def test_search_by_platform(self, db_conn):
        await self._seed(db_conn)
        results = await repo.search(db_conn, platform="kafka")
        names = [r["logical_name"] for r in results]
        assert "kafka-prod-01" in names
        assert all(r["platform"] == "kafka" for r in results)

    async def test_search_by_classification(self, db_conn):
        await self._seed(db_conn)
        results = await repo.search(db_conn, classification="confidential")
        names = [r["logical_name"] for r in results]
        assert "t24-core-prod" in names
        assert "kafka-prod-01" not in names

    async def test_search_by_owner_team_partial(self, db_conn):
        await self._seed(db_conn)
        results = await repo.search(db_conn, owner_team="banking")
        names = [r["logical_name"] for r in results]
        assert "t24-core-prod" in names

    async def test_search_combined_filters(self, db_conn):
        await self._seed(db_conn)
        results = await repo.search(db_conn, platform="oracle", classification="confidential")
        names = [r["logical_name"] for r in results]
        assert names == ["t24-core-prod"]

    async def test_search_excludes_deleted(self, db_conn):
        await self._seed(db_conn)
        await repo.soft_delete(db_conn, "pg-analytics")
        results = await repo.search(db_conn, q="pg")
        names = [r["logical_name"] for r in results]
        assert "pg-analytics" not in names

    async def test_search_limit(self, db_conn):
        await self._seed(db_conn)
        results = await repo.search(db_conn, q="prod", limit=1)
        assert len(results) <= 1
