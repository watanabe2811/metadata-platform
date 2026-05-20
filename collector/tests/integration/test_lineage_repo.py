"""Integration tests for LineageRepository — requires live Postgres."""
from __future__ import annotations

import pytest

from metadata_collector.repositories.lineage_repo import LineageRepository
from .conftest import make_connection, make_job, make_dataset

pytestmark = pytest.mark.integration

repo = LineageRepository()


async def _wire(db_conn, *, job_id, dataset_id, direction: str):
    return await repo.upsert_edge(
        db_conn, job_id=job_id, dataset_id=dataset_id, direction=direction
    )


class TestEdgeUpsert:
    async def test_upsert_creates_edge(self, db_conn):
        conn_row = await make_connection(db_conn, logical_name="edge-conn")
        job = await make_job(db_conn, namespace="test.ns", name="edge-job")
        ds = await make_dataset(db_conn, connection_id=conn_row["id"], name="tbl")
        eid = await _wire(db_conn, job_id=job["id"], dataset_id=ds["id"], direction="input")
        assert eid is not None

    async def test_upsert_is_idempotent(self, db_conn):
        conn_row = await make_connection(db_conn, logical_name="idem-edge-conn")
        job = await make_job(db_conn, namespace="test.ns", name="idem-edge-job")
        ds = await make_dataset(db_conn, connection_id=conn_row["id"], name="tbl2")
        id1 = await _wire(db_conn, job_id=job["id"], dataset_id=ds["id"], direction="output")
        id2 = await _wire(db_conn, job_id=job["id"], dataset_id=ds["id"], direction="output")
        assert id1 == id2

    async def test_input_and_output_are_separate_edges(self, db_conn):
        conn_row = await make_connection(db_conn, logical_name="dir-conn")
        job = await make_job(db_conn, namespace="test.ns", name="dir-job")
        ds = await make_dataset(db_conn, connection_id=conn_row["id"], name="tbl3")
        id_in = await _wire(db_conn, job_id=job["id"], dataset_id=ds["id"], direction="input")
        id_out = await _wire(db_conn, job_id=job["id"], dataset_id=ds["id"], direction="output")
        assert id_in != id_out

    async def test_column_mapping_stored(self, db_conn):
        conn_row = await make_connection(db_conn, logical_name="col-conn")
        job = await make_job(db_conn, namespace="test.ns", name="col-job")
        ds = await make_dataset(db_conn, connection_id=conn_row["id"], name="col_tbl")
        mapping = [{"target": "balance", "sources": [{"field": "BAL"}]}]
        eid = await repo.upsert_edge(
            db_conn, job_id=job["id"], dataset_id=ds["id"],
            direction="output", column_mapping=mapping,
        )
        row = await db_conn.fetchrow("SELECT column_mapping FROM lineage_edge WHERE id = $1", eid)
        assert row["column_mapping"][0]["target"] == "balance"


class TestJobsForConnection:
    """FR-1: jobs_touching_connection."""

    async def _build_graph(self, db_conn):
        conn_a = await make_connection(db_conn, logical_name="fr1-conn-a")
        conn_b = await make_connection(db_conn, logical_name="fr1-conn-b")
        job1 = await make_job(db_conn, namespace="ns", name="fr1-job-reader")
        job2 = await make_job(db_conn, namespace="ns", name="fr1-job-writer")
        ds_a1 = await make_dataset(db_conn, connection_id=conn_a["id"], name="DS_A1",
                                   fqn="fr1-conn-a.DS_A1")
        ds_b1 = await make_dataset(db_conn, connection_id=conn_b["id"], name="DS_B1",
                                   fqn="fr1-conn-b.DS_B1")
        await _wire(db_conn, job_id=job1["id"], dataset_id=ds_a1["id"], direction="input")
        await _wire(db_conn, job_id=job2["id"], dataset_id=ds_a1["id"], direction="output")
        return conn_a, job1, job2

    async def test_returns_readers_and_writers(self, db_conn):
        conn_a, job1, job2 = await self._build_graph(db_conn)
        jobs = await repo.jobs_touching_connection(db_conn, "fr1-conn-a")
        names = {j["name"] for j in jobs}
        assert "fr1-job-reader" in names
        assert "fr1-job-writer" in names

    async def test_role_reader(self, db_conn):
        await self._build_graph(db_conn)
        jobs = await repo.jobs_touching_connection(db_conn, "fr1-conn-a")
        reader = next(j for j in jobs if j["name"] == "fr1-job-reader")
        assert reader["role"] == "reader"

    async def test_role_writer(self, db_conn):
        await self._build_graph(db_conn)
        jobs = await repo.jobs_touching_connection(db_conn, "fr1-conn-a")
        writer = next(j for j in jobs if j["name"] == "fr1-job-writer")
        assert writer["role"] == "writer"

    async def test_unrelated_connection_returns_empty(self, db_conn):
        await self._build_graph(db_conn)
        jobs = await repo.jobs_touching_connection(db_conn, "fr1-conn-b")
        assert jobs == []


class TestJobsForDataset:
    """FR-2: jobs_for_dataset."""

    async def test_reader_and_writer_detected(self, db_conn):
        conn = await make_connection(db_conn, logical_name="fr2-conn")
        ds = await make_dataset(db_conn, connection_id=conn["id"],
                                name="FACT", fqn="fr2-conn.FACT")
        job_r = await make_job(db_conn, namespace="ns", name="fr2-reader")
        job_w = await make_job(db_conn, namespace="ns", name="fr2-writer")
        await _wire(db_conn, job_id=job_r["id"], dataset_id=ds["id"], direction="input")
        await _wire(db_conn, job_id=job_w["id"], dataset_id=ds["id"], direction="output")

        jobs = await repo.jobs_for_dataset(db_conn, "fr2-conn.FACT")
        roles = {j["name"]: j["role"] for j in jobs}
        assert roles["fr2-reader"] == "reader"
        assert roles["fr2-writer"] == "writer"

    async def test_both_role_when_read_and_write(self, db_conn):
        conn = await make_connection(db_conn, logical_name="fr2-rw-conn")
        ds = await make_dataset(db_conn, connection_id=conn["id"],
                                name="RW_TBL", fqn="fr2-rw-conn.RW_TBL")
        job = await make_job(db_conn, namespace="ns", name="fr2-rw-job")
        await _wire(db_conn, job_id=job["id"], dataset_id=ds["id"], direction="input")
        await _wire(db_conn, job_id=job["id"], dataset_id=ds["id"], direction="output")

        jobs = await repo.jobs_for_dataset(db_conn, "fr2-rw-conn.RW_TBL")
        assert len(jobs) == 1
        assert jobs[0]["role"] == "both"


class TestLineageTraversal:
    """Upstream / downstream recursive CTE."""

    async def _build_chain(self, db_conn):
        """Build: src.RAW → [job-load] → mid.FACT → [job-agg] → dst.DM"""
        src = await make_connection(db_conn, logical_name="chain-src")
        mid = await make_connection(db_conn, logical_name="chain-mid")
        dst = await make_connection(db_conn, logical_name="chain-dst")

        ds_raw  = await make_dataset(db_conn, connection_id=src["id"],
                                     name="RAW", fqn="chain-src.RAW")
        ds_fact = await make_dataset(db_conn, connection_id=mid["id"],
                                     name="FACT", fqn="chain-mid.FACT")
        ds_dm   = await make_dataset(db_conn, connection_id=dst["id"],
                                     name="DM", fqn="chain-dst.DM")

        job_load = await make_job(db_conn, namespace="ns", name="chain-job-load")
        job_agg  = await make_job(db_conn, namespace="ns", name="chain-job-agg")

        await _wire(db_conn, job_id=job_load["id"], dataset_id=ds_raw["id"],  direction="input")
        await _wire(db_conn, job_id=job_load["id"], dataset_id=ds_fact["id"], direction="output")
        await _wire(db_conn, job_id=job_agg["id"],  dataset_id=ds_fact["id"], direction="input")
        await _wire(db_conn, job_id=job_agg["id"],  dataset_id=ds_dm["id"],   direction="output")

        return ds_raw, ds_fact, ds_dm

    async def test_downstream_from_raw(self, db_conn):
        ds_raw, ds_fact, ds_dm = await self._build_chain(db_conn)
        nodes = await repo.downstream(db_conn, "chain-src.RAW", max_depth=5)
        fqns = {n["dataset_fqn"] for n in nodes}
        assert "chain-mid.FACT" in fqns
        assert "chain-dst.DM" in fqns

    async def test_downstream_depth_limits(self, db_conn):
        await self._build_chain(db_conn)
        nodes = await repo.downstream(db_conn, "chain-src.RAW", max_depth=1)
        fqns = {n["dataset_fqn"] for n in nodes}
        assert "chain-mid.FACT" in fqns
        assert "chain-dst.DM" not in fqns  # depth 2, cut off

    async def test_upstream_from_dm(self, db_conn):
        ds_raw, ds_fact, ds_dm = await self._build_chain(db_conn)
        nodes = await repo.upstream(db_conn, "chain-dst.DM", max_depth=5)
        fqns = {n["dataset_fqn"] for n in nodes}
        assert "chain-mid.FACT" in fqns
        assert "chain-src.RAW" in fqns

    async def test_no_upstream_for_source(self, db_conn):
        await self._build_chain(db_conn)
        nodes = await repo.upstream(db_conn, "chain-src.RAW", max_depth=5)
        assert nodes == []

    async def test_via_job_name_populated(self, db_conn):
        await self._build_chain(db_conn)
        nodes = await repo.downstream(db_conn, "chain-src.RAW", max_depth=1)
        fact_node = next(n for n in nodes if n["dataset_fqn"] == "chain-mid.FACT")
        assert "chain-job-load" in fact_node["via_job_name"]

    async def test_cycle_guard(self, db_conn):
        """A dataset that is both input and output of the same job must not loop."""
        conn = await make_connection(db_conn, logical_name="cycle-conn")
        ds = await make_dataset(db_conn, connection_id=conn["id"],
                                name="SELF", fqn="cycle-conn.SELF")
        job = await make_job(db_conn, namespace="ns", name="cycle-job")
        await _wire(db_conn, job_id=job["id"], dataset_id=ds["id"], direction="input")
        await _wire(db_conn, job_id=job["id"], dataset_id=ds["id"], direction="output")

        nodes = await repo.downstream(db_conn, "cycle-conn.SELF", max_depth=10)
        # Should return empty (self-reference detected via path guard)
        assert all(n["dataset_fqn"] != "cycle-conn.SELF" for n in nodes)
