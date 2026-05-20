"""Unit tests for BankConnConfig and configure()."""
from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from bank_conn.config import BankConnConfig, configure, get_config


class TestBankConnConfig:
    def test_defaults(self):
        cfg = BankConnConfig()
        assert cfg.collector_url == "http://localhost:8080"
        assert cfg.collector_token == "dev-token-change-me"
        assert cfg.vault_url == "http://localhost:8200"
        assert cfg.vault_token == ""
        assert cfg.vault_namespace is None
        assert cfg.cache_ttl_seconds == 300
        assert cfg.emit_lineage is True
        assert cfg.openlineage_producer == "bank-conn/0.1.0"
        assert cfg.default_job_namespace == "mbbank.default"

    def test_env_overrides_collector_url(self):
        with patch.dict(os.environ, {"BANK_CONN_COLLECTOR_URL": "http://prod-collector"}):
            cfg = BankConnConfig()
        assert cfg.collector_url == "http://prod-collector"

    def test_env_overrides_collector_token(self):
        with patch.dict(os.environ, {"BANK_CONN_COLLECTOR_TOKEN": "prod-token"}):
            cfg = BankConnConfig()
        assert cfg.collector_token == "prod-token"

    def test_emit_lineage_false_via_env(self):
        for val in ("false", "0", "no"):
            with patch.dict(os.environ, {"BANK_CONN_EMIT_LINEAGE": val}):
                cfg = BankConnConfig()
            assert cfg.emit_lineage is False, f"expected False for BANK_CONN_EMIT_LINEAGE={val}"

    def test_emit_lineage_true_via_env(self):
        for val in ("true", "1", "yes"):
            with patch.dict(os.environ, {"BANK_CONN_EMIT_LINEAGE": val}):
                cfg = BankConnConfig()
            assert cfg.emit_lineage is True, f"expected True for BANK_CONN_EMIT_LINEAGE={val}"

    def test_vault_namespace_from_env(self):
        with patch.dict(os.environ, {"VAULT_NAMESPACE": "mbbank/team-a"}):
            cfg = BankConnConfig()
        assert cfg.vault_namespace == "mbbank/team-a"

    def test_default_job_namespace_from_env(self):
        with patch.dict(os.environ, {"BANK_CONN_JOB_NAMESPACE": "mbbank.dwh"}):
            cfg = BankConnConfig()
        assert cfg.default_job_namespace == "mbbank.dwh"


class TestConfigure:
    def test_configure_updates_global_config(self):
        original_url = get_config().collector_url
        configure(collector_url="http://new-collector")
        assert get_config().collector_url == "http://new-collector"
        configure(collector_url=original_url)  # restore

    def test_configure_unknown_key_raises(self):
        with pytest.raises(ValueError, match="Unknown config key"):
            configure(nonexistent_key="value")

    def test_configure_multiple_keys(self):
        original_url = get_config().collector_url
        original_ttl = get_config().cache_ttl_seconds
        configure(collector_url="http://test", cache_ttl_seconds=60)
        assert get_config().collector_url == "http://test"
        assert get_config().cache_ttl_seconds == 60
        configure(collector_url=original_url, cache_ttl_seconds=original_ttl)

    def test_get_config_returns_same_instance(self):
        assert get_config() is get_config()
