"""Sample ETL: read from `t24-core-prod`, transform, write to `iceberg-warehouse`.

This demonstrates the full lineage flow:
1. Resolve logical connection names → real DB endpoints (via collector + Vault).
2. Record read/write datasets.
3. On clean exit, COMPLETE OpenLineage event is emitted.

Run locally:
    BANK_CONN_COLLECTOR_URL=http://localhost:8080 \
    BANK_CONN_EMIT_LINEAGE=true \
    python samples/sample_etl.py
"""
from __future__ import annotations

import logging

from bank_conn import Connection, configure

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    configure(
        collector_url="http://localhost:8080",
        # In production these come from env / K8s secrets, not hardcoded
        collector_token="dev-token-change-me",
        default_job_namespace="mbbank.dwh",
    )

    with Connection(
        "t24-core-prod",
        job_name="etl.t24.daily_stmt_load",
    ) as conn:
        # Record what this job touches
        conn.record_read("STMT")
        conn.record_read("ACCOUNT")
        conn.record_read("CUSTOMER")

        conn.record_write(
            "fact_stmt_daily",
            connection="iceberg-warehouse",
            column_lineage={
                "balance_amt": [
                    {"namespace": "t24-core-prod", "name": "STMT", "field": "BAL_AMT"},
                ],
                "account_id": [
                    {"namespace": "t24-core-prod", "name": "STMT", "field": "ACCT_ID"},
                    {"namespace": "t24-core-prod", "name": "ACCOUNT", "field": "ID"},
                ],
                "customer_full_name": [
                    {"namespace": "t24-core-prod", "name": "CUSTOMER", "field": "FIRST_NM"},
                    {"namespace": "t24-core-prod", "name": "CUSTOMER", "field": "LAST_NM"},
                ],
            },
        )

        # Actual work would happen here. We skip the real DB call so this
        # sample runs without an Oracle endpoint.
        logger.info("Resolved connection: platform=%s host=%s",
                    conn.resolved.platform, conn.resolved.host)
        logger.info("(sample) Would now query STMT/ACCOUNT/CUSTOMER and write fact_stmt_daily")


if __name__ == "__main__":
    main()
