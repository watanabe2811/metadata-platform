"""Dataset repository."""
from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg


class DatasetRepository:
    async def upsert(
        self,
        conn: asyncpg.Connection,
        *,
        connection_id: UUID,
        name: str,
        fqn: str,
        dataset_type: str = "table",
        facets: dict[str, Any] | None = None,
    ) -> UUID:
        """Upsert a dataset by (connection_id, fqn)."""
        row = await conn.fetchrow(
            """
            INSERT INTO dataset (
                connection_id, name, fqn, dataset_type, properties
            )
            VALUES ($1, $2, $3, $4, $5::jsonb)
            ON CONFLICT (connection_id, fqn) DO UPDATE
              SET name = EXCLUDED.name,
                  properties = dataset.properties || EXCLUDED.properties,
                  updated_at = now(),
                  deleted_at = NULL
            RETURNING id
            """,
            connection_id, name, fqn, dataset_type,
            json.dumps(facets or {}),
        )
        assert row is not None
        return row["id"]

    async def get_by_fqn(
        self, conn: asyncpg.Connection, fqn: str
    ) -> dict[str, Any] | None:
        row = await conn.fetchrow(
            """
            SELECT id, connection_id, fqn, name, dataset_type, classification,
                   properties, created_at, updated_at
            FROM dataset
            WHERE fqn = $1 AND deleted_at IS NULL
            """,
            fqn,
        )
        return dict(row) if row else None

    async def search(
        self,
        conn: asyncpg.Connection,
        *,
        q: str | None = None,
        connection: str | None = None,
        dataset_type: str | None = None,
        classification: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Search datasets with optional filters.

        q              — fuzzy match on fqn and name
        connection     — filter by connection logical_name (exact)
        dataset_type   — exact match (table / view / topic / ...)
        classification — exact match
        """
        conditions = ["d.deleted_at IS NULL", "c.deleted_at IS NULL"]
        values: list[Any] = []

        if q:
            i = len(values) + 1
            conditions.append(
                f"(d.fqn ILIKE '%' || ${i} || '%'"
                f" OR d.name ILIKE '%' || ${i} || '%')"
            )
            values.append(q)

        if connection:
            i = len(values) + 1
            conditions.append(f"c.logical_name = ${i}")
            values.append(connection)

        if dataset_type:
            i = len(values) + 1
            conditions.append(f"d.dataset_type = ${i}")
            values.append(dataset_type)

        if classification:
            i = len(values) + 1
            conditions.append(f"d.classification = ${i}")
            values.append(classification)

        limit_i = len(values) + 1
        values.append(limit)

        score_expr = f"similarity(d.fqn, $1)" if q else "0"

        query = f"""
            SELECT d.fqn, d.name, d.dataset_type, d.classification,
                   c.logical_name AS connection, c.platform,
                   d.created_at, d.updated_at,
                   {score_expr} AS score
            FROM dataset d
            JOIN connection c ON c.id = d.connection_id
            WHERE {" AND ".join(conditions)}
            ORDER BY {score_expr + " DESC, " if q else ""}d.fqn
            LIMIT ${limit_i}
        """
        rows = await conn.fetch(query, *values)
        return [dict(r) for r in rows]

    async def list_by_connection(
        self,
        conn: asyncpg.Connection,
        connection_id: UUID,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        rows = await conn.fetch(
            """
            SELECT id, fqn, name, dataset_type, classification, updated_at
            FROM dataset
            WHERE connection_id = $1 AND deleted_at IS NULL
            ORDER BY fqn
            LIMIT $2 OFFSET $3
            """,
            connection_id, limit, offset,
        )
        return [dict(r) for r in rows]
