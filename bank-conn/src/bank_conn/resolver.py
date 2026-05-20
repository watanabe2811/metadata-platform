"""Connection metadata + secrets resolver.

Resolution order:
1. Metadata from the metadata-collector API (host, port, service_name, vault_path).
2. Secrets from Vault at vault_path.
3. Local cache with TTL.

Failures degrade gracefully:
- If collector is down but cache has fresh entry: use cache.
- If both fail: raise ConnectionResolutionError.
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any

import httpx
import hvac

from bank_conn.config import get_config

logger = logging.getLogger(__name__)


class ConnectionResolutionError(Exception):
    """Raised when a logical connection name cannot be resolved."""


@dataclass(frozen=True)
class ResolvedConnection:
    """Fully resolved connection: metadata + credentials."""

    logical_name: str
    platform: str
    host: str | None
    port: int | None
    service_name: str | None
    username: str | None
    password: str | None
    properties: dict[str, Any]


@dataclass
class _CacheEntry:
    value: ResolvedConnection
    expires_at: float


class ConnectionResolver:
    """Thread-safe resolver with TTL cache."""

    def __init__(self) -> None:
        self._cache: dict[str, _CacheEntry] = {}
        self._lock = threading.Lock()

    def resolve(self, logical_name: str) -> ResolvedConnection:
        cfg = get_config()
        now = time.monotonic()

        # Cache check
        with self._lock:
            entry = self._cache.get(logical_name)
            if entry and entry.expires_at > now:
                return entry.value

        # 1. Fetch metadata from collector
        metadata = self._fetch_metadata(logical_name)

        # 2. Fetch secrets from Vault (if vault_path set)
        username, password = self._fetch_secrets(metadata.get("vault_path"))

        resolved = ResolvedConnection(
            logical_name=logical_name,
            platform=metadata["platform"],
            host=metadata.get("host"),
            port=metadata.get("port"),
            service_name=metadata.get("service_name"),
            username=username,
            password=password,
            properties=metadata.get("properties") or {},
        )

        with self._lock:
            self._cache[logical_name] = _CacheEntry(
                value=resolved,
                expires_at=now + cfg.cache_ttl_seconds,
            )
        return resolved

    def invalidate(self, logical_name: str | None = None) -> None:
        """Clear cache for one or all connections (e.g. on credential rotation)."""
        with self._lock:
            if logical_name is None:
                self._cache.clear()
            else:
                self._cache.pop(logical_name, None)

    def _fetch_metadata(self, logical_name: str) -> dict[str, Any]:
        cfg = get_config()
        url = f"{cfg.collector_url.rstrip('/')}/api/v1/connections/{logical_name}"
        headers = {"Authorization": f"Bearer {cfg.collector_token}"}
        try:
            resp = httpx.get(url, headers=headers, timeout=10.0)
        except httpx.HTTPError as e:
            raise ConnectionResolutionError(
                f"Failed to reach metadata collector for '{logical_name}': {e}"
            ) from e
        if resp.status_code == 404:
            raise ConnectionResolutionError(
                f"Connection '{logical_name}' not registered in metadata collector"
            )
        if resp.status_code >= 400:
            raise ConnectionResolutionError(
                f"Collector returned {resp.status_code}: {resp.text}"
            )
        return resp.json()

    def _fetch_secrets(self, vault_path: str | None) -> tuple[str | None, str | None]:
        if not vault_path:
            return (None, None)
        cfg = get_config()
        if not cfg.vault_token:
            logger.warning("vault_path set but VAULT_TOKEN missing; skipping secrets fetch")
            return (None, None)
        try:
            client = hvac.Client(
                url=cfg.vault_url,
                token=cfg.vault_token,
                namespace=cfg.vault_namespace,
            )
            # KV v2: path format is "secret/data/<path>", but hvac handles it
            mount_point, _, path = vault_path.partition("/")
            secret = client.secrets.kv.v2.read_secret_version(
                path=path, mount_point=mount_point, raise_on_deleted_version=True
            )
            data = secret["data"]["data"]
            return (data.get("username"), data.get("password"))
        except Exception as e:
            logger.exception("Vault secret fetch failed for path=%s", vault_path)
            raise ConnectionResolutionError(f"Vault fetch failed: {e}") from e


_global_resolver = ConnectionResolver()


def get_resolver() -> ConnectionResolver:
    return _global_resolver
