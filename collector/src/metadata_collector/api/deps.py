"""Simple service-token auth.

V1: shared bearer token in env. V2: replace with Keycloak JWT verification.
"""
from fastapi import Header, HTTPException, status

from metadata_collector.settings import get_settings


async def require_token(authorization: str | None = Header(default=None)) -> str:
    """Extract bearer token from Authorization header.

    Returns the authenticated actor name (for audit logging).
    In V1 this is just the token holder; in V2 it would be the JWT subject.
    """
    settings = get_settings()
    if not settings.auth_required:
        return "anonymous"

    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid Authorization header",
        )

    token = authorization.removeprefix("Bearer ").strip()
    if token != settings.auth_service_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid service token",
        )

    return "service-account"
