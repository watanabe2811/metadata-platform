"""Connection registry CRUD router."""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Response, status

from metadata_collector.api.deps import require_token
from metadata_collector.db import get_pool
from metadata_collector.repositories.connection_repo import ConnectionRepository
from metadata_collector.repositories.outbox_repo import AuditRepository
from metadata_collector.schemas import ConnectionCreate, ConnectionOut, ConnectionUpdate

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/connections", tags=["connections"])

_repo = ConnectionRepository()
_audit = AuditRepository()


@router.get("", response_model=list[ConnectionOut])
async def list_connections(
    limit: int = 100,
    offset: int = 0,
    _: str = Depends(require_token),
) -> list[dict]:
    pool = get_pool()
    async with pool.acquire() as conn:
        return await _repo.list_all(conn, limit=limit, offset=offset)


@router.get("/{logical_name}", response_model=ConnectionOut)
async def get_connection(
    logical_name: str,
    _: str = Depends(require_token),
) -> dict:
    pool = get_pool()
    async with pool.acquire() as conn:
        result = await _repo.get_by_logical_name(conn, logical_name)
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    return result


@router.post(
    "",
    response_model=ConnectionOut,
    status_code=status.HTTP_201_CREATED,
)
async def create_connection(
    payload: ConnectionCreate,
    actor: str = Depends(require_token),
) -> dict:
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            existing = await _repo.get_by_logical_name(conn, payload.logical_name)
            if existing and existing.get("platform") != "unknown":
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Connection '{payload.logical_name}' already exists",
                )
            # If a stub exists (created by lineage auto-resolve), upgrade it
            if existing:
                updated = await _repo.update(conn, payload.logical_name,
                                             payload.model_dump(exclude={"logical_name"}))
                await _audit.log(
                    conn, actor=actor, action="upgrade_stub",
                    entity_type="connection", entity_id=existing["id"],
                    before_state=_strip_dates(existing),
                    after_state=_strip_dates(updated) if updated else None,
                )
                assert updated is not None
                return updated

            new_id = await _repo.create(conn, **payload.model_dump())
            result = await _repo.get_by_logical_name(conn, payload.logical_name)
            assert result is not None
            await _audit.log(
                conn, actor=actor, action="create",
                entity_type="connection", entity_id=new_id,
                after_state=_strip_dates(result),
            )
            return result


@router.put("/{logical_name}", response_model=ConnectionOut)
async def update_connection(
    logical_name: str,
    payload: ConnectionUpdate,
    actor: str = Depends(require_token),
) -> dict:
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            before = await _repo.get_by_logical_name(conn, logical_name)
            if not before:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
            updates = {k: v for k, v in payload.model_dump().items() if v is not None}
            after = await _repo.update(conn, logical_name, updates)
            assert after is not None
            await _audit.log(
                conn, actor=actor, action="update",
                entity_type="connection", entity_id=before["id"],
                before_state=_strip_dates(before),
                after_state=_strip_dates(after),
            )
            return after


@router.delete(
    "/{logical_name}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
async def delete_connection(
    logical_name: str,
    actor: str = Depends(require_token),
) -> Response:
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            before = await _repo.get_by_logical_name(conn, logical_name)
            if not before:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
            ok = await _repo.soft_delete(conn, logical_name)
            if not ok:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
            await _audit.log(
                conn, actor=actor, action="delete",
                entity_type="connection", entity_id=before["id"],
                before_state=_strip_dates(before),
            )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


def _strip_dates(d: dict | None) -> dict | None:
    """Remove non-serializable fields for audit JSON payload."""
    if d is None:
        return None
    out = {}
    for k, v in d.items():
        if k in ("created_at", "updated_at", "deleted_at"):
            out[k] = v.isoformat() if v else None
        elif hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        else:
            out[k] = str(v) if hasattr(v, "hex") else v   # UUID → str
    return out
