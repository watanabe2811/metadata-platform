"""Lineage ingestion service.

Parses an OpenLineage RunEvent and persists derived entities atomically:
- Job (upserted from job.namespace + job.name)
- JobRun (upserted from run.runId; status from eventType)
- Dataset for each input/output (auto-creates Connection stub if missing)
- LineageEdge for each input/output
- Outbox event (for future downstream sync)
"""
from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

import asyncpg

from metadata_collector.repositories.connection_repo import ConnectionRepository
from metadata_collector.repositories.dataset_repo import DatasetRepository
from metadata_collector.repositories.job_repo import JobRepository
from metadata_collector.repositories.lineage_repo import LineageRepository
from metadata_collector.repositories.outbox_repo import OutboxRepository

logger = logging.getLogger(__name__)


class LineageIngestionService:
    def __init__(self) -> None:
        self.conn_repo = ConnectionRepository()
        self.dataset_repo = DatasetRepository()
        self.job_repo = JobRepository()
        self.lineage_repo = LineageRepository()
        self.outbox_repo = OutboxRepository()

    async def ingest(
        self, conn: asyncpg.Connection, event: dict[str, Any]
    ) -> UUID:
        """Persist an OpenLineage RunEvent. Returns the JobRun id.

        Must be called within an active transaction (caller's responsibility),
        so partial writes are rolled back on error.
        """
        async with conn.transaction():
            # 1. Job
            job_id = await self.job_repo.upsert(
                conn,
                namespace=event["job"]["namespace"],
                name=event["job"]["name"],
                job_type=self._infer_job_type(event),
                facets=event["job"].get("facets", {}),
            )

            # 2. Job run
            run_id = await self.job_repo.upsert_run(
                conn,
                job_id=job_id,
                run_uuid=event["run"]["runId"],
                event_type=event["eventType"],
                event_time=event["eventTime"],
                facets=event["run"].get("facets", {}),
            )

            # 3. Inputs
            for ds in event.get("inputs", []):
                dataset_id = await self._upsert_dataset(conn, ds)
                await self.lineage_repo.upsert_edge(
                    conn,
                    job_id=job_id,
                    dataset_id=dataset_id,
                    direction="input",
                )

            # 4. Outputs (with column-level lineage if present)
            for ds in event.get("outputs", []):
                dataset_id = await self._upsert_dataset(conn, ds)
                column_mapping = self._extract_column_lineage(ds)
                await self.lineage_repo.upsert_edge(
                    conn,
                    job_id=job_id,
                    dataset_id=dataset_id,
                    direction="output",
                    column_mapping=column_mapping,
                )

            # 5. Outbox event for future downstream sync
            await self.outbox_repo.append(
                conn,
                aggregate_type="job_run",
                aggregate_id=run_id,
                event_type=f"ol.{event['eventType'].lower()}",
                payload=event,
            )

            return run_id

    async def _upsert_dataset(
        self, conn: asyncpg.Connection, ol_dataset: dict[str, Any]
    ) -> UUID:
        """Resolve dataset by namespace (logical connection name) + name.

        If the connection doesn't exist yet, create a stub. Admins enrich later.
        """
        namespace = ol_dataset["namespace"]
        name = ol_dataset["name"]

        connection_id = await self.conn_repo.upsert_minimal(
            conn, logical_name=namespace, platform="unknown"
        )

        fqn = f"{namespace}.{name}"
        dataset_type = self._infer_dataset_type(namespace, name, ol_dataset.get("facets", {}))

        return await self.dataset_repo.upsert(
            conn,
            connection_id=connection_id,
            name=name,
            fqn=fqn,
            dataset_type=dataset_type,
            facets=ol_dataset.get("facets", {}),
        )

    @staticmethod
    def _infer_job_type(event: dict[str, Any]) -> str:
        producer = event.get("producer", "").lower()
        if "spark" in producer:
            return "spark"
        if "flink" in producer:
            return "flink"
        if "airflow" in producer:
            return "airflow_task"
        if "fastapi" in producer:
            return "fastapi"
        if "trino" in producer:
            return "trino_query"
        if "python" in producer:
            return "python"
        return "unknown"

    @staticmethod
    def _infer_dataset_type(
        namespace: str, name: str, facets: dict[str, Any]
    ) -> str:
        ns_lower = namespace.lower()
        if "kafka" in ns_lower:
            return "topic"
        if "iceberg" in ns_lower or "iceberg" in facets.get("storage", {}).get("storageLayer", "").lower():
            return "iceberg_table"
        if ns_lower.startswith("s3://") or ns_lower.startswith("file://"):
            return "file"
        return "table"

    @staticmethod
    def _extract_column_lineage(
        ol_dataset: dict[str, Any],
    ) -> list[dict[str, Any]] | None:
        """Extract OpenLineage columnLineage facet → simplified list.

        Spec: https://openlineage.io/docs/spec/facets/dataset-facets/column_lineage_facet
        """
        facets = ol_dataset.get("facets", {})
        col_lineage = facets.get("columnLineage")
        if not col_lineage or "fields" not in col_lineage:
            return None
        return [
            {"target": target, "sources": spec.get("inputFields", [])}
            for target, spec in col_lineage["fields"].items()
        ]
