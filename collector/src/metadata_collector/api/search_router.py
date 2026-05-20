"""Search router — FR-1, FR-2, graph traversal, and flexible search."""
from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status

from metadata_collector.api.deps import require_token
from metadata_collector.db import get_pool
from metadata_collector.repositories.connection_repo import ConnectionRepository
from metadata_collector.repositories.dataset_repo import DatasetRepository
from metadata_collector.repositories.lineage_repo import LineageRepository
from metadata_collector.schemas import ConnectionSearchResult, DatasetSearchResult
from metadata_collector.settings import get_settings

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/search", tags=["search"])

_conn_repo = ConnectionRepository()
_dataset_repo = DatasetRepository()
_lineage_repo = LineageRepository()


@router.get(
    "/connections/{logical_name}/jobs",
    summary="FR-1: Jobs touching this connection",
)
async def jobs_touching_connection(
    logical_name: str,
    _: str = Depends(require_token),
) -> dict:
    pool = get_pool()
    async with pool.acquire() as conn:
        connection = await _conn_repo.get_by_logical_name(conn, logical_name)
        if not connection:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"Connection '{logical_name}' not found")
        jobs = await _lineage_repo.jobs_touching_connection(conn, logical_name)
        summary = await _lineage_repo.connection_impact_summary(conn, logical_name)

    return {
        "connection": logical_name,
        "summary": summary,
        "jobs": jobs,
    }


@router.get(
    "/connections/{logical_name}/related",
    summary="Connections sharing jobs with this connection",
)
async def related_connections(
    logical_name: str,
    _: str = Depends(require_token),
) -> dict:
    pool = get_pool()
    async with pool.acquire() as conn:
        connection = await _conn_repo.get_by_logical_name(conn, logical_name)
        if not connection:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
        related = await _lineage_repo.related_connections(conn, logical_name)
    return {"connection": logical_name, "related": related}


@router.get(
    "/datasets/{fqn:path}/jobs",
    summary="FR-2: Jobs reading from or writing to this dataset",
)
async def jobs_for_dataset(
    fqn: str,
    _: str = Depends(require_token),
) -> dict:
    pool = get_pool()
    async with pool.acquire() as conn:
        dataset = await _dataset_repo.get_by_fqn(conn, fqn)
        if not dataset:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dataset '{fqn}' not found",
            )
        jobs = await _lineage_repo.jobs_for_dataset(conn, fqn)
    return {
        "dataset_fqn": fqn,
        "readers": [j for j in jobs if j["role"] in ("reader", "both")],
        "writers": [j for j in jobs if j["role"] in ("writer", "both")],
        "all_jobs": jobs,
    }


@router.get(
    "/datasets/{fqn:path}/upstream",
    summary="Recursive upstream lineage",
)
async def dataset_upstream(
    fqn: str,
    depth: Annotated[int, Query(ge=1, le=20)] | None = None,
    _: str = Depends(require_token),
) -> dict:
    settings = get_settings()
    actual_depth = min(depth or settings.default_lineage_depth, settings.max_lineage_depth)
    pool = get_pool()
    async with pool.acquire() as conn:
        nodes = await _lineage_repo.upstream(conn, fqn, actual_depth)
    return {
        "root_fqn": fqn,
        "direction": "upstream",
        "max_depth": actual_depth,
        "nodes": nodes,
    }


@router.get(
    "/datasets/{fqn:path}/downstream",
    summary="Recursive downstream lineage",
)
async def dataset_downstream(
    fqn: str,
    depth: Annotated[int, Query(ge=1, le=20)] | None = None,
    _: str = Depends(require_token),
) -> dict:
    settings = get_settings()
    actual_depth = min(depth or settings.default_lineage_depth, settings.max_lineage_depth)
    pool = get_pool()
    async with pool.acquire() as conn:
        nodes = await _lineage_repo.downstream(conn, fqn, actual_depth)
    return {
        "root_fqn": fqn,
        "direction": "downstream",
        "max_depth": actual_depth,
        "nodes": nodes,
    }


@router.get(
    "/connections",
    summary="Search connections by name, host/IP, platform, classification, or team",
    response_model=list[ConnectionSearchResult],
)
async def search_connections(
    q: Annotated[str | None, Query(description="Fuzzy match on logical_name, host, description")] = None,
    host: Annotated[str | None, Query(description="Partial IP or hostname match")] = None,
    platform: Annotated[str | None, Query(description="Exact platform filter (oracle, kafka, ...)")] = None,
    classification: Annotated[str | None, Query(description="public | internal | confidential")] = None,
    owner_team: Annotated[str | None, Query(description="Partial team name match")] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    _: str = Depends(require_token),
) -> list[dict]:
    if not any([q, host, platform, classification, owner_team]):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least one search parameter is required: q, host, platform, classification, owner_team",
        )
    pool = get_pool()
    async with pool.acquire() as conn:
        return await _conn_repo.search(
            conn,
            q=q,
            host=host,
            platform=platform,
            classification=classification,
            owner_team=owner_team,
            limit=limit,
        )


@router.get(
    "/datasets",
    summary="Search datasets by name, connection, type, or classification",
    response_model=list[DatasetSearchResult],
)
async def search_datasets(
    q: Annotated[str | None, Query(description="Fuzzy match on fqn and name")] = None,
    connection: Annotated[str | None, Query(description="Filter by connection logical_name (exact)")] = None,
    dataset_type: Annotated[str | None, Query(description="table | view | topic | file | iceberg_table | materialized_view")] = None,
    classification: Annotated[str | None, Query(description="public | internal | confidential")] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    _: str = Depends(require_token),
) -> list[dict]:
    if not any([q, connection, dataset_type, classification]):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least one search parameter is required: q, connection, dataset_type, classification",
        )
    pool = get_pool()
    async with pool.acquire() as conn:
        return await _dataset_repo.search(
            conn,
            q=q,
            connection=connection,
            dataset_type=dataset_type,
            classification=classification,
            limit=limit,
        )
