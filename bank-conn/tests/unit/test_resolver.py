"""Unit tests for ConnectionResolver — HTTP and Vault calls are mocked."""
from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import httpx
import pytest

from bank_conn.config import BankConnConfig
from bank_conn.resolver import (
    ConnectionResolutionError,
    ConnectionResolver,
    ResolvedConnection,
)


def _make_resolver(cfg: BankConnConfig | None = None) -> ConnectionResolver:
    if cfg is None:
        cfg = BankConnConfig(
            collector_url="http://mock-collector",
            collector_token="test-token",
            cache_ttl_seconds=300,
        )
    resolver = ConnectionResolver()
    with patch("bank_conn.resolver.get_config", return_value=cfg):
        pass
    return resolver


def _mock_metadata_response(data: dict, status_code: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = data
    resp.text = str(data)
    return resp


_ORA_METADATA = {
    "platform": "oracle",
    "host": "ora-prod.bank.local",
    "port": 1521,
    "service_name": "ORCLPDB",
    "vault_path": None,
    "properties": {"service_name": "ORCLPDB"},
}


class TestCacheHit:
    def test_second_call_uses_cache(self):
        resolver = ConnectionResolver()
        cfg = BankConnConfig(
            collector_url="http://mock", collector_token="tok", cache_ttl_seconds=300
        )
        with patch("bank_conn.resolver.get_config", return_value=cfg), \
             patch("bank_conn.resolver.httpx.get") as mock_get:
            mock_get.return_value = _mock_metadata_response(_ORA_METADATA)
            resolver.resolve("ora-prod")
            resolver.resolve("ora-prod")
        assert mock_get.call_count == 1

    def test_expired_cache_refetches(self):
        resolver = ConnectionResolver()
        cfg = BankConnConfig(
            collector_url="http://mock", collector_token="tok", cache_ttl_seconds=0
        )
        with patch("bank_conn.resolver.get_config", return_value=cfg), \
             patch("bank_conn.resolver.httpx.get") as mock_get:
            mock_get.return_value = _mock_metadata_response(_ORA_METADATA)
            resolver.resolve("ora-prod")
            time.sleep(0.01)  # ensure TTL=0 expires
            resolver.resolve("ora-prod")
        assert mock_get.call_count == 2

    def test_cache_miss_for_different_names(self):
        resolver = ConnectionResolver()
        cfg = BankConnConfig(
            collector_url="http://mock", collector_token="tok", cache_ttl_seconds=300
        )
        meta_b = {**_ORA_METADATA, "host": "ora-backup.bank.local"}
        with patch("bank_conn.resolver.get_config", return_value=cfg), \
             patch("bank_conn.resolver.httpx.get") as mock_get:
            mock_get.side_effect = [
                _mock_metadata_response(_ORA_METADATA),
                _mock_metadata_response(meta_b),
            ]
            r1 = resolver.resolve("ora-prod")
            r2 = resolver.resolve("ora-backup")
        assert r1.host == "ora-prod.bank.local"
        assert r2.host == "ora-backup.bank.local"
        assert mock_get.call_count == 2


class TestInvalidate:
    def test_invalidate_single_name_clears_entry(self):
        resolver = ConnectionResolver()
        cfg = BankConnConfig(
            collector_url="http://mock", collector_token="tok", cache_ttl_seconds=300
        )
        with patch("bank_conn.resolver.get_config", return_value=cfg), \
             patch("bank_conn.resolver.httpx.get") as mock_get:
            mock_get.return_value = _mock_metadata_response(_ORA_METADATA)
            resolver.resolve("ora-prod")
            resolver.invalidate("ora-prod")
            resolver.resolve("ora-prod")
        assert mock_get.call_count == 2

    def test_invalidate_all_clears_entire_cache(self):
        resolver = ConnectionResolver()
        cfg = BankConnConfig(
            collector_url="http://mock", collector_token="tok", cache_ttl_seconds=300
        )
        meta_b = {**_ORA_METADATA, "host": "b"}
        with patch("bank_conn.resolver.get_config", return_value=cfg), \
             patch("bank_conn.resolver.httpx.get") as mock_get:
            mock_get.side_effect = [
                _mock_metadata_response(_ORA_METADATA),
                _mock_metadata_response(meta_b),
                _mock_metadata_response(_ORA_METADATA),
                _mock_metadata_response(meta_b),
            ]
            resolver.resolve("ora-prod")
            resolver.resolve("ora-backup")
            resolver.invalidate()  # clear all
            resolver.resolve("ora-prod")
            resolver.resolve("ora-backup")
        assert mock_get.call_count == 4

    def test_invalidate_nonexistent_does_not_raise(self):
        resolver = ConnectionResolver()
        resolver.invalidate("ghost")  # must not raise


class TestFetchMetadata:
    def test_404_raises_connection_resolution_error(self):
        resolver = ConnectionResolver()
        cfg = BankConnConfig(collector_url="http://mock", collector_token="tok")
        not_found = _mock_metadata_response({}, status_code=404)
        with patch("bank_conn.resolver.get_config", return_value=cfg), \
             patch("bank_conn.resolver.httpx.get", return_value=not_found):
            with pytest.raises(ConnectionResolutionError, match="not registered"):
                resolver.resolve("ghost-conn")

    def test_5xx_raises_connection_resolution_error(self):
        resolver = ConnectionResolver()
        cfg = BankConnConfig(collector_url="http://mock", collector_token="tok")
        server_err = _mock_metadata_response({}, status_code=500)
        server_err.text = "internal error"
        with patch("bank_conn.resolver.get_config", return_value=cfg), \
             patch("bank_conn.resolver.httpx.get", return_value=server_err):
            with pytest.raises(ConnectionResolutionError, match="500"):
                resolver.resolve("bad-conn")

    def test_connect_error_raises_connection_resolution_error(self):
        resolver = ConnectionResolver()
        cfg = BankConnConfig(collector_url="http://mock", collector_token="tok")
        with patch("bank_conn.resolver.get_config", return_value=cfg), \
             patch("bank_conn.resolver.httpx.get",
                   side_effect=httpx.ConnectError("refused")):
            with pytest.raises(ConnectionResolutionError, match="Failed to reach"):
                resolver.resolve("unreachable-conn")

    def test_resolved_fields_populated(self):
        resolver = ConnectionResolver()
        cfg = BankConnConfig(collector_url="http://mock", collector_token="tok")
        with patch("bank_conn.resolver.get_config", return_value=cfg), \
             patch("bank_conn.resolver.httpx.get",
                   return_value=_mock_metadata_response(_ORA_METADATA)):
            result = resolver.resolve("ora-prod")
        assert isinstance(result, ResolvedConnection)
        assert result.platform == "oracle"
        assert result.host == "ora-prod.bank.local"
        assert result.port == 1521
        assert result.service_name == "ORCLPDB"
        assert result.username is None
        assert result.password is None

    def test_authorization_header_sent(self):
        resolver = ConnectionResolver()
        cfg = BankConnConfig(
            collector_url="http://mock", collector_token="secret-token"
        )
        with patch("bank_conn.resolver.get_config", return_value=cfg), \
             patch("bank_conn.resolver.httpx.get") as mock_get:
            mock_get.return_value = _mock_metadata_response(_ORA_METADATA)
            resolver.resolve("ora-prod")
        call_kwargs = mock_get.call_args
        headers = call_kwargs.kwargs.get("headers") or call_kwargs.args[1]
        assert headers["Authorization"] == "Bearer secret-token"


class TestFetchSecrets:
    def test_no_vault_path_returns_none_credentials(self):
        resolver = ConnectionResolver()
        cfg = BankConnConfig(collector_url="http://mock", collector_token="tok")
        meta = {**_ORA_METADATA, "vault_path": None}
        with patch("bank_conn.resolver.get_config", return_value=cfg), \
             patch("bank_conn.resolver.httpx.get",
                   return_value=_mock_metadata_response(meta)):
            result = resolver.resolve("ora-prod")
        assert result.username is None
        assert result.password is None

    def test_vault_token_missing_skips_fetch(self):
        resolver = ConnectionResolver()
        cfg = BankConnConfig(
            collector_url="http://mock", collector_token="tok", vault_token=""
        )
        meta = {**_ORA_METADATA, "vault_path": "secret/ora-prod"}
        with patch("bank_conn.resolver.get_config", return_value=cfg), \
             patch("bank_conn.resolver.httpx.get",
                   return_value=_mock_metadata_response(meta)):
            result = resolver.resolve("ora-prod")
        assert result.username is None
        assert result.password is None

    def test_vault_fetch_returns_credentials(self):
        resolver = ConnectionResolver()
        cfg = BankConnConfig(
            collector_url="http://mock",
            collector_token="tok",
            vault_url="http://vault",
            vault_token="vault-root",
        )
        meta = {**_ORA_METADATA, "vault_path": "secret/ora-prod"}
        mock_hvac = MagicMock()
        mock_hvac.secrets.kv.v2.read_secret_version.return_value = {
            "data": {"data": {"username": "dbuser", "password": "s3cr3t"}}
        }
        with patch("bank_conn.resolver.get_config", return_value=cfg), \
             patch("bank_conn.resolver.httpx.get",
                   return_value=_mock_metadata_response(meta)), \
             patch("bank_conn.resolver.hvac.Client", return_value=mock_hvac):
            result = resolver.resolve("ora-prod")
        assert result.username == "dbuser"
        assert result.password == "s3cr3t"

    def test_vault_failure_raises_connection_resolution_error(self):
        resolver = ConnectionResolver()
        cfg = BankConnConfig(
            collector_url="http://mock",
            collector_token="tok",
            vault_url="http://vault",
            vault_token="vault-root",
        )
        meta = {**_ORA_METADATA, "vault_path": "secret/ora-prod"}
        mock_hvac = MagicMock()
        mock_hvac.secrets.kv.v2.read_secret_version.side_effect = Exception("Vault down")
        with patch("bank_conn.resolver.get_config", return_value=cfg), \
             patch("bank_conn.resolver.httpx.get",
                   return_value=_mock_metadata_response(meta)), \
             patch("bank_conn.resolver.hvac.Client", return_value=mock_hvac):
            with pytest.raises(ConnectionResolutionError, match="Vault fetch failed"):
                resolver.resolve("ora-prod")
