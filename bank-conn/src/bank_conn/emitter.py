"""OpenLineage event emitter.

Wraps the openlineage-python client. Events are POSTed to the collector's
/api/v1/lineage endpoint with bearer-token auth.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import httpx

from bank_conn.config import get_config

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_run_id() -> str:
    """Generate an OpenLineage run UUID for the current process.

    For Airflow / Spark, the framework already provides a run id; reuse it.
    """
    return str(uuid4())


class LineageEmitter:
    """Emits OpenLineage RunEvent JSON to the collector."""

    SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json"

    def __init__(self) -> None:
        cfg = get_config()
        self._client = httpx.Client(
            timeout=10.0,
            base_url=cfg.collector_url.rstrip("/"),
            headers={"Authorization": f"Bearer {cfg.collector_token}"},
        )

    def emit_start(
        self,
        *,
        run_id: str,
        job_namespace: str,
        job_name: str,
        inputs: list[dict[str, Any]],
        outputs: list[dict[str, Any]],
        producer: str | None = None,
    ) -> None:
        self._emit("START", run_id, job_namespace, job_name, inputs, outputs, producer)

    def emit_complete(
        self,
        *,
        run_id: str,
        job_namespace: str,
        job_name: str,
        inputs: list[dict[str, Any]],
        outputs: list[dict[str, Any]],
        producer: str | None = None,
    ) -> None:
        self._emit("COMPLETE", run_id, job_namespace, job_name, inputs, outputs, producer)

    def emit_fail(
        self,
        *,
        run_id: str,
        job_namespace: str,
        job_name: str,
        inputs: list[dict[str, Any]],
        outputs: list[dict[str, Any]],
        error: str | None = None,
        producer: str | None = None,
    ) -> None:
        run_facets: dict[str, Any] = {}
        if error:
            run_facets["errorMessage"] = {
                "_producer": producer or get_config().openlineage_producer,
                "_schemaURL": self.SCHEMA_URL,
                "message": error,
                "programmingLanguage": "PYTHON",
            }
        self._emit("FAIL", run_id, job_namespace, job_name, inputs, outputs,
                   producer, run_facets=run_facets)

    def _emit(
        self,
        event_type: str,
        run_id: str,
        job_namespace: str,
        job_name: str,
        inputs: list[dict[str, Any]],
        outputs: list[dict[str, Any]],
        producer: str | None = None,
        run_facets: dict[str, Any] | None = None,
    ) -> None:
        cfg = get_config()
        if not cfg.emit_lineage:
            logger.debug("Lineage emission disabled; skipping event_type=%s", event_type)
            return

        event = {
            "eventType": event_type,
            "eventTime": _now_iso(),
            "run": {
                "runId": run_id,
                "facets": run_facets or {},
            },
            "job": {
                "namespace": job_namespace,
                "name": job_name,
                "facets": {},
            },
            "inputs": inputs,
            "outputs": outputs,
            "producer": producer or cfg.openlineage_producer,
            "schemaURL": self.SCHEMA_URL,
        }
        try:
            resp = self._client.post("/api/v1/lineage", json=event)
            if resp.status_code >= 400:
                logger.warning(
                    "Lineage emit failed: %s %s", resp.status_code, resp.text[:200]
                )
        except httpx.HTTPError as e:
            # Never let lineage emission failure break the actual job
            logger.warning("Lineage emit error (suppressed): %s", e)


_global_emitter: LineageEmitter | None = None


def get_emitter() -> LineageEmitter:
    global _global_emitter
    if _global_emitter is None:
        _global_emitter = LineageEmitter()
    return _global_emitter
