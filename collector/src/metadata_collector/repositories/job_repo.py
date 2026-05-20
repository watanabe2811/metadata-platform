"""Job + JobRun repository."""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg


class JobRepository:
    async def upsert(
        self,
        conn: asyncpg.Connection,
        *,
        namespace: str,
        name: str,
        job_type: str = "unknown",
        facets: dict[str, Any] | None = None,
    ) -> UUID:
        row = await conn.fetchrow(
            """
            INSERT INTO job (namespace, name, job_type, properties)
            VALUES ($1, $2, $3, $4::jsonb)
            ON CONFLICT (namespace, name) DO UPDATE
              SET job_type = CASE
                    WHEN job.job_type = 'unknown' THEN EXCLUDED.job_type
                    ELSE job.job_type
                  END,
                  properties = job.properties || EXCLUDED.properties,
                  updated_at = now(),
                  deleted_at = NULL
            RETURNING id
            """,
            namespace, name, job_type, json.dumps(facets or {}),
        )
        assert row is not None
        return row["id"]

    async def upsert_run(
        self,
        conn: asyncpg.Connection,
        *,
        job_id: UUID,
        run_uuid: str,
        event_type: str,
        event_time: str,
        facets: dict[str, Any] | None = None,
    ) -> UUID:
        status_map = {
            "START": "started",
            "RUNNING": "running",
            "COMPLETE": "completed",
            "FAIL": "failed",
            "ABORT": "aborted",
            "OTHER": "other",
        }
        status = status_map.get(event_type, "other")
        is_terminal = event_type in ("COMPLETE", "FAIL", "ABORT")

        # asyncpg requires native datetime for timestamptz (not ISO strings).
        # OpenLineage spec sends ISO 8601 with timezone (e.g. "2026-05-19T...+00:00").
        event_dt = datetime.fromisoformat(event_time)

        # On START: set started_at; on terminal: set ended_at.
        # Use COALESCE to never overwrite earlier started_at when later events arrive.
        row = await conn.fetchrow(
            """
            INSERT INTO job_run (job_id, run_id, started_at, ended_at, status, facets)
            VALUES (
                $1, $2,
                CASE WHEN $3 = 'started' THEN $4::timestamptz ELSE NULL::timestamptz END,
                CASE WHEN $5 THEN $4::timestamptz ELSE NULL::timestamptz END,
                $3, $6::jsonb
            )
            ON CONFLICT (job_id, run_id) DO UPDATE
              SET started_at = COALESCE(job_run.started_at,
                                        CASE WHEN $3 = 'started' THEN $4::timestamptz
                                             ELSE NULL::timestamptz END),
                  ended_at = CASE WHEN $5 THEN $4::timestamptz ELSE job_run.ended_at END,
                  status = CASE WHEN $5 OR job_run.status IS NULL THEN $3 ELSE job_run.status END,
                  facets = job_run.facets || EXCLUDED.facets
            RETURNING id
            """,
            job_id, run_uuid, status, event_dt, is_terminal,
            json.dumps(facets or {}),
        )
        assert row is not None
        return row["id"]

    async def latest_run_status(
        self, conn: asyncpg.Connection, job_id: UUID
    ) -> str | None:
        row = await conn.fetchrow(
            """
            SELECT status FROM job_run
            WHERE job_id = $1
            ORDER BY started_at DESC NULLS LAST
            LIMIT 1
            """,
            job_id,
        )
        return row["status"] if row else None
