"""Outbox + audit log repositories — append-only writers."""
from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg


class OutboxRepository:
    async def append(
        self,
        conn: asyncpg.Connection,
        *,
        aggregate_type: str,
        aggregate_id: UUID | None,
        event_type: str,
        payload: dict[str, Any],
    ) -> int:
        row = await conn.fetchrow(
            """
            INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
            VALUES ($1, $2, $3, $4::jsonb)
            RETURNING id
            """,
            aggregate_type, aggregate_id, event_type, json.dumps(payload),
        )
        assert row is not None
        return row["id"]


class AuditRepository:
    async def log(
        self,
        conn: asyncpg.Connection,
        *,
        actor: str,
        action: str,
        entity_type: str,
        entity_id: UUID | None = None,
        before_state: dict[str, Any] | None = None,
        after_state: dict[str, Any] | None = None,
    ) -> int:
        row = await conn.fetchrow(
            """
            INSERT INTO audit_log (
                actor, action, entity_type, entity_id, before_state, after_state
            )
            VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb)
            RETURNING id
            """,
            actor, action, entity_type, entity_id,
            json.dumps(before_state) if before_state else None,
            json.dumps(after_state) if after_state else None,
        )
        assert row is not None
        return row["id"]
