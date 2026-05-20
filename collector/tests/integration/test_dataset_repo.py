"""Integration tests for DatasetRepository — requires live Postgres."""
from __future__ import annotations

import pytest
from uuid import UUID

from metadata_collector.repositories.dataset_repo import DatasetRepository
from .conftest import make_connection, make_dataset

pytestmark = pytest.mark.integration

repo = DatasetRepository()


class TestDatasetUpsert:
    async def test_creates_dataset(self, db_conn):
        conn_row = await make_connection(db_conn, logical_name="ds-test-conn")
        did = await repo.upsert(
            db_conn,
            connection_id=conn_row["id"],
            name="STMT",
            fqn="ds-test-conn.STMT",
            dataset_type="table",
        )
        assert isinstance(did, UUID)

    async def test_upsert_is_idempotent(self, db_conn):
        conn_row = await make_connection(db_conn, logical_name="ds-idem-conn")
        id1 = await repo.upsert(
            db_conn, connection_id=conn_row["id"],
            name="TBL", fqn="ds-idem-conn.TBL", dataset_type="table",
        )
        id2 = await repo.upsert(
            db_conn, connection_id=conn_row["id"],
            name="TBL", fqn="ds-idem-conn.TBL", dataset_type="table",
        )
        assert id1 == id2

    async def test_get_by_fqn(self, db_conn):
        conn_row = await make_connection(db_conn, logical_name="ds-fqn-conn")
        await repo.upsert(
            db_conn, connection_id=conn_row["id"],
            name="ACCT", fqn="ds-fqn-conn.ACCT", dataset_type="table",
        )
        result = await repo.get_by_fqn(db_conn, "ds-fqn-conn.ACCT")
        assert result is not None
        assert result["name"] == "ACCT"

    async def test_get_by_fqn_not_found(self, db_conn):
        result = await repo.get_by_fqn(db_conn, "nonexistent.TABLE")
        assert result is None


class TestDatasetSearch:
    async def _seed(self, db_conn):
        ora = await make_connection(db_conn, logical_name="ds-ora-conn",
                                    platform="oracle", classification="confidential")
        kafka = await make_connection(db_conn, logical_name="ds-kafka-conn",
                                      platform="kafka", classification="internal")
        ice = await make_connection(db_conn, logical_name="ds-iceberg-conn",
                                    platform="iceberg", classification="internal")

        await repo.upsert(db_conn, connection_id=ora["id"],
                          name="STMT", fqn="ds-ora-conn.STMT", dataset_type="table")
        await repo.upsert(db_conn, connection_id=ora["id"],
                          name="ACCOUNT", fqn="ds-ora-conn.ACCOUNT", dataset_type="table")
        await repo.upsert(db_conn, connection_id=kafka["id"],
                          name="txn-events", fqn="ds-kafka-conn.txn-events", dataset_type="topic")
        await repo.upsert(db_conn, connection_id=ice["id"],
                          name="fact_stmt", fqn="ds-iceberg-conn.fact_stmt",
                          dataset_type="iceberg_table")
        return ora, kafka, ice

    async def test_search_by_q_name(self, db_conn):
        await self._seed(db_conn)
        results = await repo.search(db_conn, q="stmt")
        fqns = [r["fqn"] for r in results]
        assert "ds-ora-conn.STMT" in fqns
        assert "ds-iceberg-conn.fact_stmt" in fqns

    async def test_search_by_connection(self, db_conn):
        await self._seed(db_conn)
        results = await repo.search(db_conn, connection="ds-ora-conn")
        fqns = {r["fqn"] for r in results}
        assert fqns == {"ds-ora-conn.STMT", "ds-ora-conn.ACCOUNT"}

    async def test_search_by_dataset_type(self, db_conn):
        await self._seed(db_conn)
        results = await repo.search(db_conn, dataset_type="topic")
        fqns = [r["fqn"] for r in results]
        assert "ds-kafka-conn.txn-events" in fqns
        assert all(r["dataset_type"] == "topic" for r in results)

    async def test_search_by_dataset_type_iceberg(self, db_conn):
        await self._seed(db_conn)
        results = await repo.search(db_conn, dataset_type="iceberg_table")
        fqns = [r["fqn"] for r in results]
        assert "ds-iceberg-conn.fact_stmt" in fqns

    async def test_search_combined_q_and_connection(self, db_conn):
        await self._seed(db_conn)
        results = await repo.search(db_conn, q="stmt", connection="ds-ora-conn")
        fqns = [r["fqn"] for r in results]
        assert "ds-ora-conn.STMT" in fqns
        assert "ds-iceberg-conn.fact_stmt" not in fqns

    async def test_search_returns_platform_from_connection(self, db_conn):
        await self._seed(db_conn)
        results = await repo.search(db_conn, connection="ds-kafka-conn")
        assert all(r["platform"] == "kafka" for r in results)

    async def test_search_no_results(self, db_conn):
        await self._seed(db_conn)
        results = await repo.search(db_conn, q="xyzzy-no-match-ever")
        assert results == []
