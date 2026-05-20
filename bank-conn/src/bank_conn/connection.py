"""Connection — primary user-facing API.

Usage:
    from bank_conn import Connection

    with Connection("t24-core-prod") as c:
        engine = c.sqlalchemy_engine()
        df = pd.read_sql("SELECT * FROM STMT WHERE ROWNUM < 100", engine)

    # Or, manually track read/write datasets for lineage:
    with Connection("t24-core-prod") as c:
        c.record_read("STMT")
        c.record_read("ACCOUNT")
        # ... do work ...
        c.record_write("iceberg-warehouse.fact_stmt")
        # On exit, COMPLETE event emitted with all recorded datasets
"""
from __future__ import annotations

import logging
import urllib.parse
from types import TracebackType
from typing import Any

from sqlalchemy import Engine, create_engine

from bank_conn.config import get_config
from bank_conn.emitter import get_emitter, make_run_id
from bank_conn.resolver import (
    ConnectionResolutionError,
    ResolvedConnection,
    get_resolver,
)

logger = logging.getLogger(__name__)


class Connection:
    """A logical, lineage-aware database connection."""

    def __init__(
        self,
        logical_name: str,
        *,
        job_namespace: str | None = None,
        job_name: str | None = None,
        run_id: str | None = None,
    ) -> None:
        self.logical_name = logical_name
        self._resolved: ResolvedConnection | None = None
        self._engine: Engine | None = None

        cfg = get_config()
        self.job_namespace = job_namespace or cfg.default_job_namespace
        self.job_name = job_name or _infer_job_name()
        self.run_id = run_id or make_run_id()

        # Lineage accumulators (populated via record_read / record_write)
        self._inputs: list[dict[str, Any]] = []
        self._outputs: list[dict[str, Any]] = []
        self._started = False

    # ---------------- resolution ----------------

    @property
    def resolved(self) -> ResolvedConnection:
        if self._resolved is None:
            self._resolved = get_resolver().resolve(self.logical_name)
        return self._resolved

    # ---------------- adapters ----------------

    def sqlalchemy_url(self) -> str:
        """Build a SQLAlchemy URL from resolved metadata + credentials."""
        r = self.resolved
        if not r.host:
            raise ConnectionResolutionError(
                f"Connection '{self.logical_name}' has no host configured"
            )
        user = urllib.parse.quote_plus(r.username or "")
        pwd = urllib.parse.quote_plus(r.password or "")
        auth = f"{user}:{pwd}@" if user else ""

        platform = r.platform.lower()
        if platform == "oracle":
            return (
                f"oracle+oracledb://{auth}{r.host}:{r.port or 1521}"
                f"/?service_name={r.service_name}"
            )
        if platform == "postgres":
            return f"postgresql+psycopg2://{auth}{r.host}:{r.port or 5432}/{r.service_name}"
        if platform == "mysql":
            return f"mysql+pymysql://{auth}{r.host}:{r.port or 3306}/{r.service_name}"
        raise ConnectionResolutionError(
            f"Platform '{r.platform}' has no SQLAlchemy URL builder; "
            f"use jdbc_options() or extend bank-conn"
        )

    def sqlalchemy_engine(self, **kwargs: Any) -> Engine:
        """Return a cached SQLAlchemy Engine for this connection."""
        if self._engine is None:
            self._engine = create_engine(self.sqlalchemy_url(), **kwargs)
        return self._engine

    def jdbc_options(self) -> dict[str, Any]:
        """Return JDBC options (for Spark / Flink JDBC connectors)."""
        r = self.resolved
        if not r.host:
            raise ConnectionResolutionError("No host configured")
        platform = r.platform.lower()
        if platform == "oracle":
            url = f"jdbc:oracle:thin:@//{r.host}:{r.port or 1521}/{r.service_name}"
            driver = "oracle.jdbc.OracleDriver"
        elif platform == "postgres":
            url = f"jdbc:postgresql://{r.host}:{r.port or 5432}/{r.service_name}"
            driver = "org.postgresql.Driver"
        elif platform == "mysql":
            url = f"jdbc:mysql://{r.host}:{r.port or 3306}/{r.service_name}"
            driver = "com.mysql.cj.jdbc.Driver"
        else:
            raise ConnectionResolutionError(
                f"Platform '{r.platform}' JDBC options not implemented"
            )
        return {
            "url": url,
            "user": r.username,
            "password": r.password,
            "driver": driver,
        }

    # ---------------- lineage recording ----------------

    def record_read(self, table_or_topic: str) -> None:
        """Record that this job reads from a dataset on this connection."""
        self._inputs.append({
            "namespace": self.logical_name,
            "name": table_or_topic,
            "facets": {},
        })

    def record_write(
        self,
        target: str,
        *,
        connection: str | None = None,
        column_lineage: dict[str, list[dict[str, str]]] | None = None,
    ) -> None:
        """Record that this job writes to a dataset.

        Args:
            target: dataset name (table / topic / file path).
            connection: optional logical_name if the write target is on a different
                        connection than this one.
            column_lineage: optional dict {target_col: [{namespace, name, field}, ...]}
        """
        facets: dict[str, Any] = {}
        if column_lineage:
            facets["columnLineage"] = {
                "_producer": "bank-conn",
                "_schemaURL": (
                    "https://openlineage.io/spec/facets/1-0-2/ColumnLineageDatasetFacet.json"
                ),
                "fields": {
                    col: {"inputFields": sources}
                    for col, sources in column_lineage.items()
                },
            }
        self._outputs.append({
            "namespace": connection or self.logical_name,
            "name": target,
            "facets": facets,
        })

    # ---------------- context manager ----------------

    def __enter__(self) -> "Connection":
        emitter = get_emitter()
        emitter.emit_start(
            run_id=self.run_id,
            job_namespace=self.job_namespace,
            job_name=self.job_name,
            inputs=self._inputs,
            outputs=self._outputs,
        )
        self._started = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if not self._started:
            return
        emitter = get_emitter()
        if exc_type is None:
            emitter.emit_complete(
                run_id=self.run_id,
                job_namespace=self.job_namespace,
                job_name=self.job_name,
                inputs=self._inputs,
                outputs=self._outputs,
            )
        else:
            emitter.emit_fail(
                run_id=self.run_id,
                job_namespace=self.job_namespace,
                job_name=self.job_name,
                inputs=self._inputs,
                outputs=self._outputs,
                error=f"{exc_type.__name__}: {exc}",
            )
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None


def _infer_job_name() -> str:
    """Best-effort job-name inference from sys.argv / env."""
    import os
    import sys
    name = os.getenv("BANK_CONN_JOB_NAME")
    if name:
        return name
    if sys.argv and sys.argv[0]:
        return os.path.basename(sys.argv[0]).removesuffix(".py")
    return "unknown-job"
