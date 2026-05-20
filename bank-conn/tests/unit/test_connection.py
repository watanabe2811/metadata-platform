"""Unit tests for Connection class — resolver and emitter are mocked."""
from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest

from bank_conn.config import BankConnConfig
from bank_conn.connection import Connection, _infer_job_name
from bank_conn.resolver import ConnectionResolutionError, ResolvedConnection


def _resolved(
    platform: str = "oracle",
    host: str = "ora.test",
    port: int = 1521,
    service_name: str = "ORCLPDB",
    username: str | None = "user",
    password: str | None = "pass",
    properties: dict | None = None,
) -> ResolvedConnection:
    return ResolvedConnection(
        logical_name="test-conn",
        platform=platform,
        host=host,
        port=port,
        service_name=service_name,
        username=username,
        password=password,
        properties=properties or {},
    )


def _make_connection(platform: str = "oracle", **kwargs) -> tuple[Connection, MagicMock]:
    """Return (Connection, mock_emitter) with resolver pre-patched."""
    mock_emitter = MagicMock()
    resolved = _resolved(platform=platform, **kwargs)
    cfg = BankConnConfig(
        collector_url="http://mock",
        collector_token="tok",
        default_job_namespace="mbbank.test",
    )
    with patch("bank_conn.connection.get_config", return_value=cfg), \
         patch("bank_conn.connection.get_resolver") as mock_resolver_fn, \
         patch("bank_conn.connection.get_emitter", return_value=mock_emitter), \
         patch("bank_conn.connection.make_run_id", return_value="run-fixed"):
        mock_resolver_fn.return_value.resolve.return_value = resolved
        conn = Connection("test-conn", job_namespace="mbbank.test", job_name="test-job")
        # inject same mock for __enter__/__exit__
        conn._resolved = resolved
    return conn, mock_emitter


class TestSQLAlchemyURL:
    def test_oracle_url(self):
        conn, _ = _make_connection(platform="oracle")
        url = conn.sqlalchemy_url()
        assert url.startswith("oracle+oracledb://")
        assert "service_name=ORCLPDB" in url
        assert "ora.test:1521" in url

    def test_postgres_url(self):
        conn, _ = _make_connection(
            platform="postgres", host="pg.test", port=5432, service_name="mydb"
        )
        url = conn.sqlalchemy_url()
        assert url.startswith("postgresql+psycopg2://")
        assert "pg.test:5432/mydb" in url

    def test_mysql_url(self):
        conn, _ = _make_connection(
            platform="mysql", host="mysql.test", port=3306, service_name="bank"
        )
        url = conn.sqlalchemy_url()
        assert url.startswith("mysql+pymysql://")
        assert "mysql.test:3306/bank" in url

    def test_unknown_platform_raises(self):
        conn, _ = _make_connection(platform="cassandra")
        with pytest.raises(ConnectionResolutionError, match="has no SQLAlchemy URL builder"):
            conn.sqlalchemy_url()

    def test_no_host_raises(self):
        conn, _ = _make_connection(host=None)
        conn._resolved = _resolved(host=None)
        with pytest.raises(ConnectionResolutionError, match="no host"):
            conn.sqlalchemy_url()

    def test_credentials_url_encoded(self):
        conn, _ = _make_connection(username="user@bank", password="p@ss/word")
        conn._resolved = _resolved(username="user@bank", password="p@ss/word")
        url = conn.sqlalchemy_url()
        assert "user%40bank" in url
        assert "p%40ss%2Fword" in url

    def test_no_credentials_omits_auth(self):
        conn, _ = _make_connection(username=None, password=None)
        conn._resolved = _resolved(username=None, password=None)
        url = conn.sqlalchemy_url()
        assert "@" not in url.split("://", 1)[1]


class TestJDBCOptions:
    def test_oracle_jdbc(self):
        conn, _ = _make_connection(platform="oracle")
        opts = conn.jdbc_options()
        assert opts["url"].startswith("jdbc:oracle:thin:@//")
        assert opts["driver"] == "oracle.jdbc.OracleDriver"
        assert opts["user"] == "user"

    def test_postgres_jdbc(self):
        conn, _ = _make_connection(platform="postgres", port=5432, service_name="mydb")
        conn._resolved = _resolved(platform="postgres", port=5432, service_name="mydb")
        opts = conn.jdbc_options()
        assert opts["url"].startswith("jdbc:postgresql://")
        assert opts["driver"] == "org.postgresql.Driver"

    def test_mysql_jdbc(self):
        conn, _ = _make_connection(platform="mysql", port=3306, service_name="bank")
        conn._resolved = _resolved(platform="mysql", port=3306, service_name="bank")
        opts = conn.jdbc_options()
        assert opts["url"].startswith("jdbc:mysql://")
        assert opts["driver"] == "com.mysql.cj.jdbc.Driver"

    def test_unknown_platform_raises(self):
        conn, _ = _make_connection(platform="snowflake")
        conn._resolved = _resolved(platform="snowflake")
        with pytest.raises(ConnectionResolutionError, match="JDBC options not implemented"):
            conn.jdbc_options()


class TestRecordReadWrite:
    def test_record_read_accumulates(self):
        conn, _ = _make_connection()
        conn.record_read("STMT")
        conn.record_read("ACCOUNT")
        assert len(conn._inputs) == 2
        assert conn._inputs[0] == {
            "namespace": "test-conn", "name": "STMT", "facets": {}
        }

    def test_record_write_accumulates(self):
        conn, _ = _make_connection()
        conn.record_write("fact_stmt")
        assert len(conn._outputs) == 1
        assert conn._outputs[0]["namespace"] == "test-conn"
        assert conn._outputs[0]["name"] == "fact_stmt"

    def test_record_write_with_different_connection(self):
        conn, _ = _make_connection()
        conn.record_write("fact_stmt", connection="iceberg-warehouse")
        assert conn._outputs[0]["namespace"] == "iceberg-warehouse"

    def test_record_write_with_column_lineage(self):
        conn, _ = _make_connection()
        conn.record_write("fact_stmt", column_lineage={
            "balance": [{"namespace": "test-conn", "name": "STMT", "field": "BAL"}]
        })
        facets = conn._outputs[0]["facets"]
        assert "columnLineage" in facets
        assert "balance" in facets["columnLineage"]["fields"]

    def test_record_write_no_column_lineage_empty_facets(self):
        conn, _ = _make_connection()
        conn.record_write("fact_stmt")
        assert conn._outputs[0]["facets"] == {}


class TestContextManager:
    def test_enter_emits_start(self):
        conn, mock_emitter = _make_connection()
        with patch("bank_conn.connection.get_emitter", return_value=mock_emitter):
            conn.__enter__()
        mock_emitter.emit_start.assert_called_once_with(
            run_id=conn.run_id,
            job_namespace=conn.job_namespace,
            job_name=conn.job_name,
            inputs=[],
            outputs=[],
        )
        conn._started = False  # prevent __exit__ from emitting

    def test_exit_without_exception_emits_complete(self):
        conn, mock_emitter = _make_connection()
        conn._started = True
        with patch("bank_conn.connection.get_emitter", return_value=mock_emitter):
            conn.__exit__(None, None, None)
        mock_emitter.emit_complete.assert_called_once()
        mock_emitter.emit_fail.assert_not_called()

    def test_exit_with_exception_emits_fail(self):
        conn, mock_emitter = _make_connection()
        conn._started = True
        with patch("bank_conn.connection.get_emitter", return_value=mock_emitter):
            conn.__exit__(ValueError, ValueError("oops"), None)
        mock_emitter.emit_fail.assert_called_once()
        call_kwargs = mock_emitter.emit_fail.call_args.kwargs
        assert "ValueError" in call_kwargs["error"]
        assert "oops" in call_kwargs["error"]
        mock_emitter.emit_complete.assert_not_called()

    def test_context_manager_complete_flow(self):
        conn, mock_emitter = _make_connection()
        conn.record_read("STMT")
        conn.record_write("fact_stmt")
        with patch("bank_conn.connection.get_emitter", return_value=mock_emitter):
            conn.__enter__()
            conn.__exit__(None, None, None)
        mock_emitter.emit_start.assert_called_once()
        mock_emitter.emit_complete.assert_called_once()
        start_inputs = mock_emitter.emit_start.call_args.kwargs["inputs"]
        complete_inputs = mock_emitter.emit_complete.call_args.kwargs["inputs"]
        assert len(start_inputs) == 1  # STMT recorded before __enter__
        assert len(complete_inputs) == 1

    def test_exit_without_enter_does_nothing(self):
        conn, mock_emitter = _make_connection()
        with patch("bank_conn.connection.get_emitter", return_value=mock_emitter):
            conn.__exit__(None, None, None)
        mock_emitter.emit_complete.assert_not_called()
        mock_emitter.emit_fail.assert_not_called()

    def test_engine_disposed_on_exit(self):
        conn, mock_emitter = _make_connection()
        mock_engine = MagicMock()
        conn._engine = mock_engine
        conn._started = True
        with patch("bank_conn.connection.get_emitter", return_value=mock_emitter):
            conn.__exit__(None, None, None)
        mock_engine.dispose.assert_called_once()
        assert conn._engine is None


class TestInferJobName:
    def test_env_var_takes_precedence(self):
        with patch.dict("os.environ", {"BANK_CONN_JOB_NAME": "my-etl-job"}):
            assert _infer_job_name() == "my-etl-job"

    def test_argv_basename_without_extension(self):
        with patch.dict("os.environ", {}, clear=True), \
             patch("sys.argv", ["/opt/jobs/load_stmt.py"]):
            # BANK_CONN_JOB_NAME not set
            import os
            os.environ.pop("BANK_CONN_JOB_NAME", None)
            name = _infer_job_name()
        assert name == "load_stmt"

    def test_no_argv_returns_unknown(self):
        with patch.dict("os.environ", {}, clear=True), \
             patch("sys.argv", []):
            import os
            os.environ.pop("BANK_CONN_JOB_NAME", None)
            name = _infer_job_name()
        assert name == "unknown-job"
