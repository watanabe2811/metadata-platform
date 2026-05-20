"""API-level integration tests using FastAPI AsyncClient — requires live Postgres.

The app's DB pool is replaced per-test with a fresh asyncpg pool so that every
request runs against the live database. Tests use a run-unique prefix (RUN_ID)
to avoid name collisions across repeated test runs (no rollback isolation).
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

from metadata_collector import db as db_module
from metadata_collector.main import app

pytestmark = pytest.mark.integration

HEADERS = {"Authorization": "Bearer dev-token-change-me"}

# Short unique prefix so every test run uses different logical_names.
RUN_ID = uuid4().hex[:8]

TEST_DB_URL = os.getenv(
    "TEST_DB_URL",
    "postgresql://metadata:metadata@localhost:5432/metadata",
)


async def _setup_codecs(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads,
                              schema="pg_catalog", format="text")
    await conn.set_type_codec("json", encoder=json.dumps, decoder=json.loads,
                              schema="pg_catalog", format="text")


@pytest_asyncio.fixture
async def api_client():
    """AsyncClient backed by a fresh asyncpg pool injected into the app."""
    pool = await asyncpg.create_pool(
        dsn=TEST_DB_URL, min_size=1, max_size=3,
        command_timeout=30, init=_setup_codecs,
    )
    db_module._pool = pool
    client = AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
    yield client
    await client.aclose()
    await pool.close()
    db_module._pool = None


class TestHealthEndpoint:
    async def test_health_ok(self, api_client):
        r = await api_client.get("/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"


class TestLineageIngest:
    def _event(self, **overrides) -> dict:
        ev = {
            "eventType": "COMPLETE",
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "run": {"runId": str(uuid4()), "facets": {}},
            "job": {"namespace": "api-test.ns", "name": "api-job", "facets": {}},
            "inputs":  [{"namespace": "api-src", "name": "SRC", "facets": {}}],
            "outputs": [{"namespace": "api-dst", "name": "DST", "facets": {}}],
            "producer": "test/1.0",
            "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
        }
        ev.update(overrides)
        return ev

    async def test_ingest_returns_202(self, api_client):
        r = await api_client.post("/api/v1/lineage", json=self._event(), headers=HEADERS)
        assert r.status_code == 202
        assert "run_id" in r.json()

    async def test_ingest_invalid_event_type_returns_422(self, api_client):
        r = await api_client.post(
            "/api/v1/lineage",
            json=self._event(eventType="INVALID"),
            headers=HEADERS,
        )
        assert r.status_code == 422

    async def test_ingest_missing_producer_returns_422(self, api_client):
        ev = self._event()
        del ev["producer"]
        r = await api_client.post("/api/v1/lineage", json=ev, headers=HEADERS)
        assert r.status_code == 422


class TestConnectionAPI:
    def _payload(self, suffix: str = "01") -> dict:
        name = f"api-conn-{RUN_ID}-{suffix}"
        return {
            "logical_name": name,
            "platform": "oracle",
            "host": f"ora-{suffix}.test",
            "port": 1521,
            "properties": {"service_name": "ORCLPDB"},
        }

    async def test_create_connection_201(self, api_client):
        r = await api_client.post("/api/v1/connections", json=self._payload("c1"),
                                  headers=HEADERS)
        assert r.status_code == 201
        assert r.json()["logical_name"] == f"api-conn-{RUN_ID}-c1"

    async def test_create_duplicate_returns_409(self, api_client):
        await api_client.post("/api/v1/connections", json=self._payload("c2"), headers=HEADERS)
        r = await api_client.post("/api/v1/connections", json=self._payload("c2"),
                                  headers=HEADERS)
        assert r.status_code == 409

    async def test_get_connection_200(self, api_client):
        await api_client.post("/api/v1/connections", json=self._payload("c3"), headers=HEADERS)
        name = f"api-conn-{RUN_ID}-c3"
        r = await api_client.get(f"/api/v1/connections/{name}", headers=HEADERS)
        assert r.status_code == 200
        assert r.json()["platform"] == "oracle"

    async def test_get_nonexistent_returns_404(self, api_client):
        r = await api_client.get("/api/v1/connections/ghost-conn-xyz", headers=HEADERS)
        assert r.status_code == 404

    async def test_update_host(self, api_client):
        await api_client.post("/api/v1/connections", json=self._payload("c4"), headers=HEADERS)
        name = f"api-conn-{RUN_ID}-c4"
        r = await api_client.put(
            f"/api/v1/connections/{name}",
            json={"host": "new-host.test"},
            headers=HEADERS,
        )
        assert r.status_code == 200
        assert r.json()["host"] == "new-host.test"

    async def test_delete_returns_204(self, api_client):
        await api_client.post("/api/v1/connections", json=self._payload("c5"), headers=HEADERS)
        name = f"api-conn-{RUN_ID}-c5"
        r = await api_client.delete(f"/api/v1/connections/{name}", headers=HEADERS)
        assert r.status_code == 204

    async def test_kafka_missing_bootstrap_servers_returns_422(self, api_client):
        r = await api_client.post(
            "/api/v1/connections",
            json={"logical_name": f"api-bad-kafka-{RUN_ID}", "platform": "kafka",
                  "properties": {}},
            headers=HEADERS,
        )
        assert r.status_code == 422


class TestSearchAPI:
    @property
    def _ora(self) -> str:
        return f"api-s-ora-{RUN_ID}"

    @property
    def _kafka(self) -> str:
        return f"api-s-kfk-{RUN_ID}"

    @property
    def _pg(self) -> str:
        return f"api-s-pg-{RUN_ID}"

    async def _seed(self, client: AsyncClient):
        for name, platform, host in [
            (self._ora,   "oracle",     "10.10.1.5"),
            (self._kafka, "kafka",      None),
            (self._pg,    "postgresql", "10.10.2.5"),
        ]:
            payload: dict = {"logical_name": name, "platform": platform, "properties": {}}
            if host:
                payload["host"] = host
            if platform == "kafka":
                payload["properties"] = {"bootstrap_servers": "b:9092"}
            if platform == "oracle":
                payload["properties"] = {"service_name": "ORCLPDB"}
            await client.post("/api/v1/connections", json=payload, headers=HEADERS)

        for conn_name, tbl in [
            (self._ora,   "ACCT"),
            (self._kafka, "txn-events"),
            (self._pg,    "fact_balance"),
        ]:
            ev = {
                "eventType": "COMPLETE",
                "eventTime": datetime.now(timezone.utc).isoformat(),
                "run": {"runId": str(uuid4()), "facets": {}},
                "job": {"namespace": "api-test.ns", "name": f"job-{tbl}-{RUN_ID}",
                        "facets": {}},
                "inputs":  [{"namespace": conn_name, "name": tbl, "facets": {}}],
                "outputs": [],
                "producer": "test/1.0",
                "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
            }
            await client.post("/api/v1/lineage", json=ev, headers=HEADERS)

    async def test_search_connections_by_platform(self, api_client):
        await self._seed(api_client)
        r = await api_client.get(f"/api/v1/search/connections?platform=kafka", headers=HEADERS)
        assert r.status_code == 200
        names = [c["logical_name"] for c in r.json()]
        assert self._kafka in names
        assert self._ora not in names

    async def test_search_connections_by_host(self, api_client):
        await self._seed(api_client)
        r = await api_client.get("/api/v1/search/connections?host=10.10.1", headers=HEADERS)
        assert r.status_code == 200
        names = [c["logical_name"] for c in r.json()]
        assert self._ora in names

    async def test_search_connections_no_params_returns_400(self, api_client):
        r = await api_client.get("/api/v1/search/connections", headers=HEADERS)
        assert r.status_code == 400

    async def test_search_datasets_by_q(self, api_client):
        await self._seed(api_client)
        r = await api_client.get("/api/v1/search/datasets?q=acct", headers=HEADERS)
        assert r.status_code == 200
        fqns = [d["fqn"] for d in r.json()]
        assert any("ACCT" in f for f in fqns)

    async def test_search_datasets_by_connection(self, api_client):
        await self._seed(api_client)
        r = await api_client.get(
            f"/api/v1/search/datasets?connection={self._kafka}", headers=HEADERS
        )
        assert r.status_code == 200
        assert all(d["connection"] == self._kafka for d in r.json())

    async def test_search_datasets_by_type(self, api_client):
        await self._seed(api_client)
        r = await api_client.get("/api/v1/search/datasets?dataset_type=topic", headers=HEADERS)
        assert r.status_code == 200
        assert all(d["dataset_type"] == "topic" for d in r.json())

    async def test_search_datasets_no_params_returns_400(self, api_client):
        r = await api_client.get("/api/v1/search/datasets", headers=HEADERS)
        assert r.status_code == 400

    async def test_fr1_jobs_touching_connection(self, api_client):
        await self._seed(api_client)
        r = await api_client.get(
            f"/api/v1/search/connections/{self._ora}/jobs", headers=HEADERS
        )
        assert r.status_code == 200
        assert "jobs" in r.json()

    async def test_fr2_jobs_for_dataset(self, api_client):
        await self._seed(api_client)
        fqn = f"{self._ora}.ACCT"
        r = await api_client.get(
            f"/api/v1/search/datasets/{fqn}/jobs", headers=HEADERS
        )
        assert r.status_code == 200
        data = r.json()
        assert "readers" in data or "writers" in data
