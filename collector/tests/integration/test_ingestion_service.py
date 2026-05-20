"""Integration tests for LineageIngestionService — requires live Postgres."""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from metadata_collector.services.ingestion_service import LineageIngestionService

pytestmark = pytest.mark.integration

svc = LineageIngestionService()


def _event(**overrides) -> dict:
    base = {
        "eventType": "COMPLETE",
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "run": {"runId": str(uuid4()), "facets": {}},
        "job": {"namespace": "svc-test.ns", "name": "svc-job", "facets": {}},
        "inputs":  [{"namespace": "svc-src",  "name": "SRC_TBL",  "facets": {}}],
        "outputs": [{"namespace": "svc-dst",  "name": "DST_TBL",  "facets": {}}],
        "producer": "test-producer/1.0",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
    }
    base.update(overrides)
    return base


class TestIngest:
    async def test_ingest_creates_job(self, db_conn):
        await svc.ingest(db_conn, _event(
            **{"job": {"namespace": "svc-job-ns", "name": "create-job", "facets": {}}}
        ))
        row = await db_conn.fetchrow(
            "SELECT * FROM job WHERE namespace = $1 AND name = $2",
            "svc-job-ns", "create-job",
        )
        assert row is not None

    async def test_ingest_creates_job_run(self, db_conn):
        run_id = str(uuid4())
        result = await svc.ingest(db_conn, _event(
            run={"runId": run_id, "facets": {}},
        ))
        row = await db_conn.fetchrow(
            "SELECT * FROM job_run WHERE run_id = $1", run_id
        )
        assert row is not None
        assert row["status"] == "completed"

    async def test_ingest_creates_stub_connections(self, db_conn):
        await svc.ingest(db_conn, _event())
        src = await db_conn.fetchrow(
            "SELECT platform FROM connection WHERE logical_name = 'svc-src' AND deleted_at IS NULL"
        )
        dst = await db_conn.fetchrow(
            "SELECT platform FROM connection WHERE logical_name = 'svc-dst' AND deleted_at IS NULL"
        )
        assert src is not None
        assert dst is not None

    async def test_ingest_creates_datasets(self, db_conn):
        await svc.ingest(db_conn, _event())
        row = await db_conn.fetchrow(
            "SELECT fqn FROM dataset WHERE fqn = 'svc-src.SRC_TBL' AND deleted_at IS NULL"
        )
        assert row is not None

    async def test_ingest_creates_lineage_edges(self, db_conn):
        await svc.ingest(db_conn, _event())
        edges = await db_conn.fetch(
            """
            SELECT le.direction
            FROM lineage_edge le
            JOIN job j ON j.id = le.job_id
            WHERE j.name = 'svc-job' AND j.namespace = 'svc-test.ns'
            """
        )
        directions = {r["direction"] for r in edges}
        assert "input" in directions
        assert "output" in directions

    async def test_ingest_appends_outbox(self, db_conn):
        run_id = str(uuid4())
        await svc.ingest(db_conn, _event(run={"runId": run_id, "facets": {}}))
        row = await db_conn.fetchrow(
            "SELECT event_type FROM outbox WHERE aggregate_type = 'job_run' "
            "ORDER BY id DESC LIMIT 1"
        )
        assert row is not None
        assert row["event_type"] == "ol.complete"

    async def test_ingest_idempotent_on_same_run_id(self, db_conn):
        ev = _event()
        run_id = ev["run"]["runId"]
        await svc.ingest(db_conn, ev)
        await svc.ingest(db_conn, ev)
        count = await db_conn.fetchval(
            "SELECT COUNT(*) FROM job_run WHERE run_id = $1", run_id
        )
        assert count == 1

    async def test_ingest_fail_event_status(self, db_conn):
        run_id = str(uuid4())
        await svc.ingest(db_conn, _event(
            eventType="FAIL",
            run={"runId": run_id, "facets": {}},
        ))
        row = await db_conn.fetchrow(
            "SELECT status FROM job_run WHERE run_id = $1", run_id
        )
        assert row["status"] == "failed"

    async def test_ingest_start_then_complete(self, db_conn):
        run_id = str(uuid4())
        await svc.ingest(db_conn, _event(
            eventType="START",
            run={"runId": run_id, "facets": {}},
        ))
        await svc.ingest(db_conn, _event(
            eventType="COMPLETE",
            run={"runId": run_id, "facets": {}},
        ))
        row = await db_conn.fetchrow(
            "SELECT status, started_at, ended_at FROM job_run WHERE run_id = $1", run_id
        )
        assert row["status"] == "completed"
        assert row["started_at"] is not None
        assert row["ended_at"] is not None

    async def test_ingest_with_column_lineage(self, db_conn):
        ev = _event(outputs=[{
            "namespace": "svc-dst",
            "name": "DST_COL",
            "facets": {
                "columnLineage": {
                    "fields": {
                        "balance": {"inputFields": [
                            {"namespace": "svc-src", "name": "SRC_TBL", "field": "BAL"}
                        ]}
                    }
                }
            },
        }])
        await svc.ingest(db_conn, ev)
        row = await db_conn.fetchrow(
            """
            SELECT le.column_mapping
            FROM lineage_edge le
            JOIN dataset d ON d.id = le.dataset_id
            WHERE d.fqn = 'svc-dst.DST_COL' AND le.direction = 'output'
            """
        )
        assert row is not None
        assert row["column_mapping"][0]["target"] == "balance"
