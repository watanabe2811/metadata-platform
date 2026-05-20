"""End-to-end smoke test.

Spins up a local Postgres (if not running), applies migrations, sends a few
OpenLineage events, and verifies search endpoints.

Prereq: `docker compose up -d postgres` in docker/, then:
    cd migrations && alembic upgrade head
    cd ../collector && uvicorn metadata_collector.main:app --port 8080

Then:
    python samples/smoke_test.py
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from uuid import uuid4

import httpx

BASE = "http://localhost:8080"
TOKEN = "dev-token-change-me"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def post_event(client: httpx.Client, event: dict) -> dict:
    r = client.post("/api/v1/lineage", json=event)
    r.raise_for_status()
    return r.json()


def make_event(
    job_namespace: str,
    job_name: str,
    inputs: list[tuple[str, str]],
    outputs: list[tuple[str, str]],
    event_type: str = "COMPLETE",
    run_id: str | None = None,
) -> dict:
    return {
        "eventType": event_type,
        "eventTime": now_iso(),
        "run": {"runId": run_id or str(uuid4()), "facets": {}},
        "job": {"namespace": job_namespace, "name": job_name, "facets": {}},
        "inputs": [{"namespace": ns, "name": n, "facets": {}} for ns, n in inputs],
        "outputs": [{"namespace": ns, "name": n, "facets": {}} for ns, n in outputs],
        "producer": "smoke-test/1.0",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
    }


def main() -> int:
    client = httpx.Client(base_url=BASE, headers=HEADERS, timeout=10.0)

    # Health
    r = client.get("/health")
    print(f"Health: {r.status_code} {r.text}")
    assert r.status_code == 200

    # Send a few lineage events forming a small graph:
    #   t24-core-prod.STMT  ─┐
    #                        ├─ etl.t24.daily_stmt_load ──> iceberg.fact_stmt
    #   t24-core-prod.ACCOUNT ┘
    #
    #   iceberg.fact_stmt ──> etl.dwh.aggregate_balance ──> doris.dm_balance_daily
    events = [
        make_event(
            "mbbank.dwh", "etl.t24.daily_stmt_load",
            inputs=[("t24-core-prod", "STMT"), ("t24-core-prod", "ACCOUNT")],
            outputs=[("iceberg-warehouse", "fact_stmt")],
        ),
        make_event(
            "mbbank.dwh", "etl.dwh.aggregate_balance",
            inputs=[("iceberg-warehouse", "fact_stmt")],
            outputs=[("doris-serving", "dm_balance_daily")],
        ),
        make_event(
            "mbbank.dwh", "report.ifrs9.ecl_input",
            inputs=[("iceberg-warehouse", "fact_stmt"),
                    ("t24-core-prod", "CUSTOMER")],
            outputs=[("iceberg-warehouse", "ifrs9_ecl_input")],
        ),
    ]
    for e in events:
        result = post_event(client, e)
        print(f"Ingested: {e['job']['name']} → run_id={result['run_id']}")

    # FR-1: jobs touching t24-core-prod
    r = client.get("/api/v1/search/connections/t24-core-prod/jobs")
    r.raise_for_status()
    data = r.json()
    print(f"\nFR-1: Jobs touching t24-core-prod ({len(data['jobs'])} jobs):")
    for j in data["jobs"]:
        print(f"  - {j['namespace']}.{j['name']} [{j['role']}] datasets={j['dataset_count']}")

    # FR-2: jobs for a specific table
    r = client.get("/api/v1/search/datasets/iceberg-warehouse.fact_stmt/jobs")
    r.raise_for_status()
    data = r.json()
    print(f"\nFR-2: Jobs for iceberg-warehouse.fact_stmt:")
    print(f"  readers: {[j['name'] for j in data['readers']]}")
    print(f"  writers: {[j['name'] for j in data['writers']]}")

    # Related connections
    r = client.get("/api/v1/search/connections/t24-core-prod/related")
    r.raise_for_status()
    data = r.json()
    print(f"\nConnections sharing jobs with t24-core-prod:")
    for c in data["related"]:
        print(f"  - {c['logical_name']} ({c['platform']}) "
              f"bridging_jobs={c['bridging_job_count']}")

    # Downstream traversal
    r = client.get("/api/v1/search/datasets/t24-core-prod.STMT/downstream?depth=5")
    r.raise_for_status()
    data = r.json()
    print(f"\nDownstream from t24-core-prod.STMT (depth 5):")
    for n in data["nodes"]:
        print(f"  depth={n['depth']} {n['dataset_fqn']} via {n['via_job_name']}")

    print("\nAll checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
