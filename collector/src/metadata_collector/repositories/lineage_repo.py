"""Lineage edge repository — includes recursive CTE traversal queries."""
from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

import asyncpg


Direction = Literal["input", "output"]


class LineageRepository:
    # ---------------- Write ----------------

    async def upsert_edge(
        self,
        conn: asyncpg.Connection,
        *,
        job_id: UUID,
        dataset_id: UUID,
        direction: Direction,
        column_mapping: list[dict[str, Any]] | None = None,
    ) -> UUID:
        row = await conn.fetchrow(
            """
            INSERT INTO lineage_edge (
                job_id, dataset_id, direction, column_mapping
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (job_id, dataset_id, direction) DO UPDATE
              SET column_mapping = COALESCE(EXCLUDED.column_mapping, lineage_edge.column_mapping),
                  last_seen_at = now()
            RETURNING id
            """,
            job_id, dataset_id, direction, column_mapping,
        )
        assert row is not None
        return row["id"]

    # ---------------- Search: FR-1 ----------------

    async def jobs_touching_connection(
        self, conn: asyncpg.Connection, logical_name: str
    ) -> list[dict[str, Any]]:
        rows = await conn.fetch(
            """
            SELECT
                j.id, j.namespace, j.name, j.job_type, j.source_repo, j.owner_team,
                array_agg(DISTINCT le.direction) AS directions,
                count(DISTINCT le.dataset_id) AS dataset_count,
                max(le.last_seen_at) AS last_seen_at
            FROM connection c
            JOIN dataset d ON d.connection_id = c.id AND d.deleted_at IS NULL
            JOIN lineage_edge le ON le.dataset_id = d.id
            JOIN job j ON j.id = le.job_id AND j.deleted_at IS NULL
            WHERE c.logical_name = $1 AND c.deleted_at IS NULL
            GROUP BY j.id, j.namespace, j.name, j.job_type, j.source_repo, j.owner_team
            ORDER BY last_seen_at DESC NULLS LAST
            """,
            logical_name,
        )
        return [self._format_job_row(r) for r in rows]

    async def connection_impact_summary(
        self, conn: asyncpg.Connection, logical_name: str
    ) -> dict[str, Any] | None:
        row = await conn.fetchrow(
            """
            SELECT
                c.logical_name,
                count(DISTINCT d.id) AS affected_datasets,
                count(DISTINCT j.id) AS affected_jobs
            FROM connection c
            LEFT JOIN dataset d ON d.connection_id = c.id AND d.deleted_at IS NULL
            LEFT JOIN lineage_edge le ON le.dataset_id = d.id
            LEFT JOIN job j ON j.id = le.job_id AND j.deleted_at IS NULL
            WHERE c.logical_name = $1 AND c.deleted_at IS NULL
            GROUP BY c.logical_name
            """,
            logical_name,
        )
        return dict(row) if row else None

    async def related_connections(
        self, conn: asyncpg.Connection, logical_name: str
    ) -> list[dict[str, Any]]:
        """Connections that share at least one job with the given connection.

        Useful for understanding cross-system impact ("if I change X, what other
        systems are touched by the same jobs?").
        """
        rows = await conn.fetch(
            """
            WITH target_jobs AS (
                SELECT DISTINCT j.id
                FROM connection c
                JOIN dataset d ON d.connection_id = c.id AND d.deleted_at IS NULL
                JOIN lineage_edge le ON le.dataset_id = d.id
                JOIN job j ON j.id = le.job_id AND j.deleted_at IS NULL
                WHERE c.logical_name = $1 AND c.deleted_at IS NULL
            )
            SELECT
                c2.logical_name,
                c2.platform,
                c2.classification,
                count(DISTINCT tj.id) AS bridging_job_count
            FROM target_jobs tj
            JOIN lineage_edge le ON le.job_id = tj.id
            JOIN dataset d2 ON d2.id = le.dataset_id AND d2.deleted_at IS NULL
            JOIN connection c2 ON c2.id = d2.connection_id AND c2.deleted_at IS NULL
            WHERE c2.logical_name != $1
            GROUP BY c2.logical_name, c2.platform, c2.classification
            ORDER BY bridging_job_count DESC
            """,
            logical_name,
        )
        return [dict(r) for r in rows]

    # ---------------- Search: FR-2 ----------------

    async def jobs_for_dataset(
        self, conn: asyncpg.Connection, fqn: str
    ) -> list[dict[str, Any]]:
        """Return jobs that read from OR write to the given dataset.

        Each job appears once with role = 'reader'|'writer'|'both'.
        """
        rows = await conn.fetch(
            """
            WITH edges AS (
                SELECT j.id, j.namespace, j.name, j.job_type, j.source_repo,
                       j.owner_team, le.direction, le.last_seen_at
                FROM dataset d
                JOIN lineage_edge le ON le.dataset_id = d.id
                JOIN job j ON j.id = le.job_id AND j.deleted_at IS NULL
                WHERE d.fqn = $1 AND d.deleted_at IS NULL
            )
            SELECT
                id, namespace, name, job_type, source_repo, owner_team,
                CASE
                    WHEN bool_or(direction = 'input')
                         AND bool_or(direction = 'output') THEN 'both'
                    WHEN bool_or(direction = 'input') THEN 'reader'
                    ELSE 'writer'
                END AS role,
                max(last_seen_at) AS last_seen_at,
                count(*) AS edge_count
            FROM edges
            GROUP BY id, namespace, name, job_type, source_repo, owner_team
            ORDER BY last_seen_at DESC NULLS LAST
            """,
            fqn,
        )
        return [dict(r) for r in rows]

    # ---------------- Search: Multi-hop traversal ----------------

    async def upstream(
        self, conn: asyncpg.Connection, fqn: str, max_depth: int
    ) -> list[dict[str, Any]]:
        """Recursive CTE: walk upstream through (dataset)→(output edge)→(job)→(input edge)→(dataset)."""
        rows = await conn.fetch(
            """
            WITH RECURSIVE upstream AS (
                SELECT d.id AS dataset_id, d.fqn AS dataset_fqn,
                       NULL::uuid AS via_job_id, NULL::text AS via_job_name,
                       0 AS depth, ARRAY[d.id] AS path
                FROM dataset d
                WHERE d.fqn = $1 AND d.deleted_at IS NULL

                UNION ALL

                SELECT
                    d_in.id, d_in.fqn,
                    j.id, j.namespace || '.' || j.name,
                    us.depth + 1, us.path || d_in.id
                FROM upstream us
                JOIN lineage_edge le_out
                    ON le_out.dataset_id = us.dataset_id AND le_out.direction = 'output'
                JOIN job j ON j.id = le_out.job_id AND j.deleted_at IS NULL
                JOIN lineage_edge le_in
                    ON le_in.job_id = j.id AND le_in.direction = 'input'
                JOIN dataset d_in
                    ON d_in.id = le_in.dataset_id AND d_in.deleted_at IS NULL
                WHERE us.depth < $2 AND d_in.id <> ALL(us.path)
            )
            SELECT depth, dataset_id, dataset_fqn, via_job_id, via_job_name
            FROM upstream
            WHERE depth > 0
            ORDER BY depth, dataset_fqn
            """,
            fqn, max_depth,
        )
        return [dict(r) for r in rows]

    async def downstream(
        self, conn: asyncpg.Connection, fqn: str, max_depth: int
    ) -> list[dict[str, Any]]:
        rows = await conn.fetch(
            """
            WITH RECURSIVE downstream AS (
                SELECT d.id AS dataset_id, d.fqn AS dataset_fqn,
                       NULL::uuid AS via_job_id, NULL::text AS via_job_name,
                       0 AS depth, ARRAY[d.id] AS path
                FROM dataset d
                WHERE d.fqn = $1 AND d.deleted_at IS NULL

                UNION ALL

                SELECT
                    d_out.id, d_out.fqn,
                    j.id, j.namespace || '.' || j.name,
                    ds.depth + 1, ds.path || d_out.id
                FROM downstream ds
                JOIN lineage_edge le_in
                    ON le_in.dataset_id = ds.dataset_id AND le_in.direction = 'input'
                JOIN job j ON j.id = le_in.job_id AND j.deleted_at IS NULL
                JOIN lineage_edge le_out
                    ON le_out.job_id = j.id AND le_out.direction = 'output'
                JOIN dataset d_out
                    ON d_out.id = le_out.dataset_id AND d_out.deleted_at IS NULL
                WHERE ds.depth < $2 AND d_out.id <> ALL(ds.path)
            )
            SELECT depth, dataset_id, dataset_fqn, via_job_id, via_job_name
            FROM downstream
            WHERE depth > 0
            ORDER BY depth, dataset_fqn
            """,
            fqn, max_depth,
        )
        return [dict(r) for r in rows]

    # ---------------- helpers ----------------

    @staticmethod
    def _format_job_row(row: asyncpg.Record) -> dict[str, Any]:
        d = dict(row)
        directions = d.pop("directions", [])
        if "input" in directions and "output" in directions:
            d["role"] = "both"
        elif "input" in directions:
            d["role"] = "reader"
        elif "output" in directions:
            d["role"] = "writer"
        else:
            d["role"] = "unknown"
        return d
