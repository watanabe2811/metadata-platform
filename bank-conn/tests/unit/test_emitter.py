"""Unit tests for LineageEmitter — HTTP calls are mocked."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from bank_conn.config import BankConnConfig, _config
from bank_conn.emitter import LineageEmitter


def _make_emitter(emit_lineage: bool = True) -> LineageEmitter:
    with patch("bank_conn.emitter.get_config") as mock_cfg:
        mock_cfg.return_value = BankConnConfig(
            collector_url="http://mock-collector",
            collector_token="test-token",
            emit_lineage=emit_lineage,
        )
        return LineageEmitter()


class TestEmitterPayload:
    def _capture_post(self, emitter: LineageEmitter, status_code: int = 202):
        mock_resp = MagicMock()
        mock_resp.status_code = status_code
        emitter._client = MagicMock()
        emitter._client.post.return_value = mock_resp
        return emitter._client

    def test_emit_start_calls_post(self):
        emitter = _make_emitter()
        client = self._capture_post(emitter)

        emitter.emit_start(
            run_id="run-123",
            job_namespace="ns.test",
            job_name="test-job",
            inputs=[{"namespace": "src", "name": "TBL", "facets": {}}],
            outputs=[],
        )
        client.post.assert_called_once()
        call_kwargs = client.post.call_args
        payload = call_kwargs.kwargs.get("json") or call_kwargs.args[1]
        assert payload["eventType"] == "START"
        assert payload["run"]["runId"] == "run-123"
        assert payload["job"]["namespace"] == "ns.test"
        assert payload["job"]["name"] == "test-job"
        assert len(payload["inputs"]) == 1

    def test_emit_complete_event_type(self):
        emitter = _make_emitter()
        client = self._capture_post(emitter)
        emitter.emit_complete(
            run_id="run-456",
            job_namespace="ns",
            job_name="job",
            inputs=[],
            outputs=[],
        )
        payload = client.post.call_args.kwargs.get("json") or client.post.call_args.args[1]
        assert payload["eventType"] == "COMPLETE"

    def test_emit_fail_includes_error_facet(self):
        emitter = _make_emitter()
        client = self._capture_post(emitter)
        emitter.emit_fail(
            run_id="run-789",
            job_namespace="ns",
            job_name="job",
            inputs=[],
            outputs=[],
            error="ORA-01403: no data found",
        )
        payload = client.post.call_args.kwargs.get("json") or client.post.call_args.args[1]
        assert payload["eventType"] == "FAIL"
        assert "errorMessage" in payload["run"]["facets"]
        assert "ORA-01403" in payload["run"]["facets"]["errorMessage"]["message"]

    def test_emit_fail_without_error_no_facet(self):
        emitter = _make_emitter()
        client = self._capture_post(emitter)
        emitter.emit_fail(
            run_id="run-000",
            job_namespace="ns",
            job_name="job",
            inputs=[],
            outputs=[],
        )
        payload = client.post.call_args.kwargs.get("json") or client.post.call_args.args[1]
        assert "errorMessage" not in payload["run"]["facets"]

    def test_schema_url_is_set(self):
        emitter = _make_emitter()
        client = self._capture_post(emitter)
        emitter.emit_complete(run_id="r", job_namespace="ns", job_name="j",
                              inputs=[], outputs=[])
        payload = client.post.call_args.kwargs.get("json") or client.post.call_args.args[1]
        assert "openlineage.io/spec" in payload["schemaURL"]


class TestEmitterErrorSuppression:
    def _make_failing_emitter(self, exc) -> LineageEmitter:
        emitter = _make_emitter()
        mock_client = MagicMock()
        mock_client.post.side_effect = exc
        emitter._client = mock_client
        return emitter

    def test_http_error_does_not_raise(self):
        emitter = self._make_failing_emitter(httpx.ConnectError("refused"))
        emitter.emit_complete(run_id="r", job_namespace="ns", job_name="j",
                              inputs=[], outputs=[])

    def test_timeout_does_not_raise(self):
        emitter = self._make_failing_emitter(httpx.TimeoutException("timeout"))
        emitter.emit_complete(run_id="r", job_namespace="ns", job_name="j",
                              inputs=[], outputs=[])

    def test_4xx_response_does_not_raise(self):
        emitter = _make_emitter()
        mock_resp = MagicMock()
        mock_resp.status_code = 400
        mock_resp.text = "bad request"
        emitter._client = MagicMock()
        emitter._client.post.return_value = mock_resp
        emitter.emit_complete(run_id="r", job_namespace="ns", job_name="j",
                              inputs=[], outputs=[])


class TestEmitLineageDisabled:
    def test_disabled_skips_http_call(self):
        emitter = _make_emitter(emit_lineage=False)
        with patch("bank_conn.emitter.get_config") as mock_cfg:
            mock_cfg.return_value = BankConnConfig(emit_lineage=False)
            mock_client = MagicMock()
            emitter._client = mock_client
            emitter._emit("COMPLETE", "r", "ns", "j", [], [], None)
            mock_client.post.assert_not_called()
