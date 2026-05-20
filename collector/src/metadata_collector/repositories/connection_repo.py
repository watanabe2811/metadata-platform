"""Connection repository."""
from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg


class ConnectionRepository:
    async def get_by_logical_name(
        self, conn: asyncpg.Connection, logical_name: str
    ) -> dict[str, Any] | None:
        row = await conn.fetchrow(
            """
            SELECT id, logical_name, platform, host, port, service_name,
                   vault_path, classification, owner_team, description,
                   properties, created_at, updated_at
            FROM connection
            WHERE logical_name = $1 AND deleted_at IS NULL
            """,
            logical_name,
        )
        return dict(row) if row else None

    async def list_all(
        self, conn: asyncpg.Connection, limit: int = 100, offset: int = 0
    ) -> list[dict[str, Any]]:
        rows = await conn.fetch(
            """
            SELECT id, logical_name, platform, host, port, service_name,
                   vault_path, classification, owner_team, description,
                   properties, created_at, updated_at
            FROM connection
            WHERE deleted_at IS NULL
            ORDER BY logical_name
            LIMIT $1 OFFSET $2
            """,
            limit, offset,
        )
        return [dict(r) for r in rows]

    async def create(
        self,
        conn: asyncpg.Connection,
        *,
        logical_name: str,
        platform: str,
        host: str | None = None,
        port: int | None = None,
        service_name: str | None = None,
        vault_path: str | None = None,
        classification: str | None = None,
        owner_team: str | None = None,
        description: str | None = None,
        properties: dict[str, Any] | None = None,
    ) -> UUID:
        row = await conn.fetchrow(
            """
            INSERT INTO connection (
                logical_name, platform, host, port, service_name,
                vault_path, classification, owner_team, description, properties
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            RETURNING id
            """,
            logical_name, platform, host, port, service_name,
            vault_path, classification, owner_team, description,
            properties or {},
        )
        assert row is not None
        return row["id"]

    async def upsert_minimal(
        self,
        conn: asyncpg.Connection,
        *,
        logical_name: str,
        platform: str = "unknown",
    ) -> UUID:
        """Used during lineage ingestion when a referenced connection doesn't exist yet.

        Creates a stub connection that should be enriched later by an admin.
        """
        row = await conn.fetchrow(
            """
            INSERT INTO connection (logical_name, platform, description)
            VALUES ($1, $2, $3)
            ON CONFLICT (logical_name) DO UPDATE
              SET updated_at = now()
            RETURNING id
            """,
            logical_name, platform,
            "Auto-created by lineage ingestion; please enrich",
        )
        assert row is not None
        return row["id"]

    async def update(
        self,
        conn: asyncpg.Connection,
        logical_name: str,
        updates: dict[str, Any],
    ) -> dict[str, Any] | None:
        if not updates:
            return await self.get_by_logical_name(conn, logical_name)

        # Build dynamic SET clause; whitelist allowed columns
        allowed = {
            "platform", "host", "port", "service_name", "vault_path",
            "classification", "owner_team", "description", "properties",
        }
        cols = [k for k in updates if k in allowed]
        if not cols:
            return await self.get_by_logical_name(conn, logical_name)

        set_clauses = []
        values: list[Any] = []
        for i, col in enumerate(cols, start=2):
            if col == "properties":
                set_clauses.append(f"{col} = ${i}")
                values.append(updates[col])
            else:
                set_clauses.append(f"{col} = ${i}")
                values.append(updates[col])

        query = f"""
            UPDATE connection
            SET {', '.join(set_clauses)}
            WHERE logical_name = $1 AND deleted_at IS NULL
            RETURNING id, logical_name, platform, host, port, service_name,
                      vault_path, classification, owner_team, description,
                      properties, created_at, updated_at
        """
        row = await conn.fetchrow(query, logical_name, *values)
        return dict(row) if row else None

    async def search(
        self,
        conn: asyncpg.Connection,
        *,
        q: str | None = None,
        host: str | None = None,
        platform: str | None = None,
        classification: str | None = None,
        owner_team: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Flexible connection search.

        q         — fuzzy match across logical_name, host, description
        host      — partial IP/hostname match (ILIKE)
        platform  — exact match
        classification — exact match
        owner_team — partial match (ILIKE)
        """
        conditions = ["deleted_at IS NULL"]
        values: list[Any] = []

        if q:
            i = len(values) + 1
            conditions.append(
                f"(logical_name ILIKE '%' || ${i} || '%'"
                f" OR host ILIKE '%' || ${i} || '%'"
                f" OR description ILIKE '%' || ${i} || '%')"
            )
            values.append(q)

        if host:
            i = len(values) + 1
            conditions.append(f"host ILIKE '%' || ${i} || '%'")
            values.append(host)

        if platform:
            i = len(values) + 1
            conditions.append(f"platform = ${i}")
            values.append(platform)

        if classification:
            i = len(values) + 1
            conditions.append(f"classification = ${i}")
            values.append(classification)

        if owner_team:
            i = len(values) + 1
            conditions.append(f"owner_team ILIKE '%' || ${i} || '%'")
            values.append(owner_team)

        limit_i = len(values) + 1
        values.append(limit)

        score_expr = (
            "GREATEST("
            "  similarity(logical_name, $1),"
            "  COALESCE(similarity(host, $1), 0)"
            ")" if q else "0"
        )

        query = f"""
            SELECT id, logical_name, platform, host, port, service_name,
                   vault_path, classification, owner_team, description,
                   properties, created_at, updated_at,
                   {score_expr} AS score
            FROM connection
            WHERE {" AND ".join(conditions)}
            ORDER BY {score_expr + " DESC, " if q else ""}logical_name
            LIMIT ${limit_i}
        """
        rows = await conn.fetch(query, *values)
        return [dict(r) for r in rows]

    async def soft_delete(
        self, conn: asyncpg.Connection, logical_name: str
    ) -> bool:
        result = await conn.execute(
            """
            UPDATE connection SET deleted_at = now()
            WHERE logical_name = $1 AND deleted_at IS NULL
            """,
            logical_name,
        )
        return result.endswith("UPDATE 1")
