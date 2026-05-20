"""Global configuration for bank-conn.

Configure once at process startup (or via env vars), then use Connection().
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class BankConnConfig:
    """Global library configuration."""

    # Metadata collector API (for connection metadata + lineage emission)
    collector_url: str = field(default_factory=lambda: os.getenv(
        "BANK_CONN_COLLECTOR_URL", "http://localhost:8080"
    ))
    collector_token: str = field(default_factory=lambda: os.getenv(
        "BANK_CONN_COLLECTOR_TOKEN", "dev-token-change-me"
    ))

    # Vault
    vault_url: str = field(default_factory=lambda: os.getenv(
        "VAULT_ADDR", "http://localhost:8200"
    ))
    vault_token: str = field(default_factory=lambda: os.getenv(
        "VAULT_TOKEN", ""
    ))
    vault_namespace: str | None = field(default_factory=lambda: os.getenv(
        "VAULT_NAMESPACE"
    ))

    # Cache TTL for connection metadata (seconds)
    cache_ttl_seconds: int = 300

    # Lineage emission
    emit_lineage: bool = field(default_factory=lambda: os.getenv(
        "BANK_CONN_EMIT_LINEAGE", "true"
    ).lower() in ("1", "true", "yes"))

    # OpenLineage producer string for emitted events
    openlineage_producer: str = "bank-conn/0.1.0"

    # Default job namespace if not provided
    default_job_namespace: str = field(default_factory=lambda: os.getenv(
        "BANK_CONN_JOB_NAMESPACE", "mbbank.default"
    ))


_config = BankConnConfig()


def configure(**kwargs) -> BankConnConfig:
    """Override defaults at runtime.

    Example:
        configure(collector_url="https://metadata.bank.local",
                  cache_ttl_seconds=60)
    """
    global _config
    for k, v in kwargs.items():
        if hasattr(_config, k):
            setattr(_config, k, v)
        else:
            raise ValueError(f"Unknown config key: {k}")
    return _config


def get_config() -> BankConnConfig:
    return _config
