"""Lineage ingestion endpoint — accepts OpenLineage RunEvents."""
from __future__ import annotations

import logging

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, status

from metadata_collector.api.deps import require_token
from metadata_collector.db import get_pool
from metadata_collector.schemas import OpenLineageRunEvent
from metadata_collector.services.ingestion_service import LineageIngestionService

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/lineage", tags=["lineage"])

_service = LineageIngestionService()


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest an OpenLineage RunEvent",
)
async def ingest_lineage_event(
    event: OpenLineageRunEvent,
    actor: str = Depends(require_token),
) -> dict[str, str]:
    pool = get_pool()
    payload = event.model_dump()
    try:
        async with pool.acquire() as conn:
            run_id = await _service.ingest(conn, payload)
        return {"status": "accepted", "run_id": str(run_id)}
    except asyncpg.PostgresError as e:
        logger.exception("Postgres error ingesting lineage event")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}",
        )
    except (KeyError, ValueError) as e:
        logger.warning("Invalid lineage event: %s", e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid event: {e}",
        )
