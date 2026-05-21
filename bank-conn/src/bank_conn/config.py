"""Global configuration for bank-conn.

Loading priority (highest → lowest):
  1. Environment variables
  2. config/config.yaml  (path overridable via BANK_CONN_CONFIG_FILE env var)
  3. Hardcoded defaults

Configure once at process startup, then use Connection() throughout the job.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        import yaml  # type: ignore[import-untyped]
        with open(path) as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def _yaml() -> dict[str, Any]:
    override = os.getenv("BANK_CONN_CONFIG_FILE")
    path = Path(override) if override else Path("config/config.yaml")
    return _load_yaml(path)


# Parsed once at module import; used as the YAML layer in default_factory lambdas.
_Y: dict[str, Any] = _yaml()


def _s(key: str, yaml_val: Any, default: str) -> str:
    """env var → yaml → default (string)."""
    v = os.getenv(key)
    if v is not None:
        return v
    if yaml_val is not None:
        return str(yaml_val)
    return default


def _b(key: str, yaml_val: Any, default: bool) -> bool:
    """env var → yaml → default (bool)."""
    v = os.getenv(key)
    if v is not None:
        return v.lower() in ("1", "true", "yes")
    if yaml_val is not None:
        return bool(yaml_val)
    return default


def _i(key: str, yaml_val: Any, default: int) -> int:
    """env var → yaml → default (int)."""
    v = os.getenv(key)
    if v is not None:
        return int(v)
    if yaml_val is not None:
        return int(yaml_val)
    return default


@dataclass
class BankConnConfig:
    """Global library configuration.

    Instantiate without arguments to pick up env vars + config/config.yaml.
    Override individual keys at runtime with configure().
    """

    collector_url: str = field(
        default_factory=lambda: _s(
            "BANK_CONN_COLLECTOR_URL",
            (_Y.get("collector") or {}).get("url"),
            "http://localhost:8080",
        )
    )
    collector_token: str = field(
        default_factory=lambda: _s(
            "BANK_CONN_COLLECTOR_TOKEN",
            (_Y.get("collector") or {}).get("token"),
            "dev-token-change-me",
        )
    )
    vault_url: str = field(
        default_factory=lambda: _s(
            "VAULT_ADDR",
            (_Y.get("vault") or {}).get("addr"),
            "http://localhost:8200",
        )
    )
    vault_token: str = field(
        default_factory=lambda: _s(
            "VAULT_TOKEN",
            (_Y.get("vault") or {}).get("token") or None,
            "",
        )
    )
    vault_namespace: str | None = field(
        default_factory=lambda: _s(
            "VAULT_NAMESPACE",
            (_Y.get("vault") or {}).get("namespace"),
            None,  # type: ignore[arg-type]
        ) or None
    )
    cache_ttl_seconds: int = field(
        default_factory=lambda: _i(
            "BANK_CONN_CACHE_TTL",
            (_Y.get("cache") or {}).get("ttl_seconds"),
            300,
        )
    )
    emit_lineage: bool = field(
        default_factory=lambda: _b(
            "BANK_CONN_EMIT_LINEAGE",
            (_Y.get("lineage") or {}).get("emit"),
            True,
        )
    )
    openlineage_producer: str = field(
        default_factory=lambda: _s(
            "BANK_CONN_OL_PRODUCER",
            (_Y.get("lineage") or {}).get("producer"),
            "bank-conn/0.1.0",
        )
    )
    default_job_namespace: str = field(
        default_factory=lambda: _s(
            "BANK_CONN_JOB_NAMESPACE",
            (_Y.get("job") or {}).get("namespace"),
            "mbbank.default",
        )
    )


_config = BankConnConfig()


def configure(**kwargs: Any) -> BankConnConfig:
    """Override individual config values at runtime.

    Example:
        configure(collector_url="https://metadata.bank.local", cache_ttl_seconds=60)
    """
    global _config
    for k, v in kwargs.items():
        if hasattr(_config, k):
            setattr(_config, k, v)
        else:
            raise ValueError(f"Unknown config key: {k!r}")
    return _config


def get_config() -> BankConnConfig:
    return _config
