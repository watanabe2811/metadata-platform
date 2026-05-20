"""Metadata scanner.

Scans a project directory and extracts:
- Database/broker connections (from .env, config files, source code)
- Job definitions (Airflow DAGs, Spark scripts, Python ETL)
- Inferred lineage edges (input → job → output)

Output: JSON written to stdout, errors to stderr.

Usage:
    python tools/metadata_scanner.py <project-dir> [options]

Options:
    --namespace TEXT   OpenLineage job namespace  [default: project dir name]
    --platform TEXT    Force platform override for all connections
    --out FILE         Write JSON to file instead of stdout
"""
from __future__ import annotations

import argparse
import ast
import json
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import uuid4

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class DetectedConnection:
    logical_name: str
    platform: str
    host: str | None = None
    port: int | None = None
    service_name: str | None = None
    vault_path: str | None = None
    classification: str | None = None
    owner_team: str | None = None
    description: str | None = None
    properties: dict[str, Any] = field(default_factory=dict)
    source_file: str = ""

    def to_api_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "logical_name": self.logical_name,
            "platform": self.platform,
            "properties": self.properties,
        }
        for attr in ("host", "port", "service_name", "vault_path",
                     "classification", "owner_team", "description"):
            v = getattr(self, attr)
            if v is not None:
                payload[attr] = v
        return payload


@dataclass
class DetectedJob:
    namespace: str
    name: str
    job_type: str
    inputs: list[tuple[str, str]] = field(default_factory=list)   # (namespace, name)
    outputs: list[tuple[str, str]] = field(default_factory=list)  # (namespace, name)
    source_file: str = ""
    owner_team: str | None = None

    def to_lineage_event(self) -> dict[str, Any]:
        return {
            "eventType": "COMPLETE",
            "eventTime": _now_iso(),
            "run": {"runId": str(uuid4()), "facets": {}},
            "job": {
                "namespace": self.namespace,
                "name": self.name,
                "facets": {},
            },
            "inputs":  [{"namespace": ns, "name": n, "facets": {}} for ns, n in self.inputs],
            "outputs": [{"namespace": ns, "name": n, "facets": {}} for ns, n in self.outputs],
            "producer": "metadata-scanner/1.0",
            "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
        }


@dataclass
class ScanResult:
    project_dir: str
    namespace: str
    connections: list[DetectedConnection] = field(default_factory=list)
    jobs: list[DetectedJob] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def _slugify(s: str) -> str:
    """Convert arbitrary string to valid logical_name."""
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9._-]", "-", s)
    s = re.sub(r"-{2,}", "-", s)
    return s.strip("-") or "unknown"


def _infer_platform(value: str) -> str:
    v = value.lower()
    if any(k in v for k in ("postgresql", "postgres", "psql")):
        return "postgresql"
    if any(k in v for k in ("mysql", "mariadb")):
        return "mysql"
    if any(k in v for k in ("oracle", "oracledb", "cx_oracle")):
        return "oracle"
    if any(k in v for k in ("kafka", "bootstrap")):
        return "kafka"
    if any(k in v for k in ("redis",)):
        return "redis"
    if any(k in v for k in ("mongodb", "mongo")):
        return "mongodb"
    if any(k in v for k in ("s3://", "minio")):
        return "s3"
    if any(k in v for k in ("iceberg",)):
        return "iceberg"
    if any(k in v for k in ("trino", "presto")):
        return "trino"
    if any(k in v for k in ("clickhouse",)):
        return "clickhouse"
    return "unknown"


# URL patterns: postgresql://user:pass@host:port/db
_URL_RE = re.compile(
    r"(?P<scheme>[a-z+]+)://(?:[^@\s]+@)?(?P<host>[^:/\s]+)(?::(?P<port>\d+))?/(?P<db>[^\s?\"']+)?",
    re.IGNORECASE,
)

# Key patterns in env files: DB_HOST, DATABASE_URL, KAFKA_BROKERS, etc.
_ENV_PATTERNS = [
    (re.compile(r"^(?P<prefix>[A-Z0-9_]*?)(?:DATABASE|DB|POSTGRES|MYSQL|ORACLE|MONGO|REDIS|KAFKA|TRINO)_URL\s*=\s*(?P<val>.+)$", re.I), "url"),
    (re.compile(r"^(?P<prefix>[A-Z0-9_]*?)(?:BOOTSTRAP_SERVERS|KAFKA_BROKERS)\s*=\s*(?P<val>.+)$", re.I), "kafka_brokers"),
    (re.compile(r"^(?P<prefix>[A-Z0-9_]*?)(?:DB|DATABASE)_HOST\s*=\s*(?P<val>.+)$", re.I), "host"),
]


# ---------------------------------------------------------------------------
# Scanners
# ---------------------------------------------------------------------------

class EnvScanner:
    """Extract connection info from .env, .env.*, docker-compose env sections."""

    def scan(self, root: Path, result: ScanResult) -> None:
        env_files = list(root.rglob(".env")) + list(root.rglob(".env.*"))
        env_files = [f for f in env_files if not any(
            p in str(f) for p in (".git", "node_modules", "__pycache__", ".venv", "venv")
        )]
        for ef in env_files:
            self._parse_env_file(ef, result)

    def _parse_env_file(self, path: Path, result: ScanResult) -> None:
        try:
            text = path.read_text(errors="replace")
        except OSError:
            return

        urls_found: dict[str, str] = {}
        hosts_found: dict[str, str] = {}
        brokers_found: dict[str, str] = {}

        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            for pattern, kind in _ENV_PATTERNS:
                m = pattern.match(line)
                if not m:
                    continue
                prefix = m.group("prefix").lower().strip("_") or "default"
                val = m.group("val").strip().strip('"').strip("'")
                if kind == "url":
                    urls_found[prefix] = val
                elif kind == "kafka_brokers":
                    brokers_found[prefix] = val
                elif kind == "host":
                    hosts_found[prefix] = val

        for prefix, url in urls_found.items():
            conn = self._url_to_connection(url, prefix, str(path))
            if conn:
                result.connections.append(conn)

        for prefix, brokers in brokers_found.items():
            name = _slugify(f"kafka-{prefix}") if prefix != "default" else "kafka-default"
            result.connections.append(DetectedConnection(
                logical_name=name,
                platform="kafka",
                properties={"bootstrap_servers": brokers},
                source_file=str(path),
            ))

    def _url_to_connection(
        self, url: str, prefix: str, source: str
    ) -> DetectedConnection | None:
        m = _URL_RE.match(url)
        if not m:
            return None
        scheme = m.group("scheme")
        host = m.group("host")
        port_str = m.group("port")
        db = m.group("db")
        platform = _infer_platform(scheme)
        logical = _slugify(f"{platform}-{prefix}" if prefix != "default" else f"{platform}-default")
        return DetectedConnection(
            logical_name=logical,
            platform=platform,
            host=host,
            port=int(port_str) if port_str else None,
            service_name=db,
            source_file=source,
        )


class DockerComposeScanner:
    """Extract connection info from docker-compose.yml / docker-compose.yaml."""

    def scan(self, root: Path, result: ScanResult) -> None:
        for fname in ("docker-compose.yml", "docker-compose.yaml",
                      "compose.yml", "compose.yaml"):
            for p in root.rglob(fname):
                if ".git" in str(p):
                    continue
                self._parse_compose(p, result)

    def _parse_compose(self, path: Path, result: ScanResult) -> None:
        try:
            import yaml  # type: ignore[import]
            data = yaml.safe_load(path.read_text())
        except Exception:
            result.warnings.append(f"Could not parse {path} (install PyYAML for better coverage)")
            return

        services = (data or {}).get("services", {})
        for svc_name, svc in services.items():
            image: str = svc.get("image", "")
            platform = _infer_platform(image or svc_name)
            if platform == "unknown":
                continue

            env: dict[str, str] = {}
            raw_env = svc.get("environment", {})
            if isinstance(raw_env, dict):
                env = {k: str(v) for k, v in raw_env.items()}
            elif isinstance(raw_env, list):
                for item in raw_env:
                    if "=" in item:
                        k, _, v = item.partition("=")
                        env[k] = v

            host = env.get("POSTGRES_HOST") or env.get("MYSQL_HOST") or svc_name
            port_str = svc.get("ports", [""])[0].split(":")[0] if svc.get("ports") else None
            logical = _slugify(svc_name)
            result.connections.append(DetectedConnection(
                logical_name=logical,
                platform=platform,
                host=host,
                port=int(port_str) if port_str and port_str.isdigit() else None,
                source_file=str(path),
            ))


class PythonSourceScanner:
    """Extract job definitions from Python ETL scripts and Airflow DAGs."""

    _IMPORT_RE = re.compile(
        r"(?:from|import)\s+(airflow|pyspark|flink|kafka|psycopg2|asyncpg|sqlalchemy|hvac|httpx)"
    )
    _SQL_TABLE_RE = re.compile(
        r"(?:FROM|JOIN|INTO|TABLE)\s+[`'\"]?(?P<schema>\w+\.)?(?P<table>\w+)[`'\"]?",
        re.IGNORECASE,
    )
    _READ_RE = re.compile(r"\.read(?:_csv|_parquet|_json|_table|_sql|_orc)?\(", re.IGNORECASE)
    _WRITE_RE = re.compile(r"\.(?:write|to_csv|to_parquet|to_sql|saveAsTable|insertInto)\(", re.IGNORECASE)

    def scan(self, root: Path, result: ScanResult) -> None:
        py_files = [
            f for f in root.rglob("*.py")
            if not any(p in str(f) for p in (
                ".git", "__pycache__", ".venv", "venv", "node_modules",
                "test_", "_test.py", "conftest",
            ))
        ]
        for py_file in py_files:
            self._scan_file(py_file, result)

    def _scan_file(self, path: Path, result: ScanResult) -> None:
        try:
            text = path.read_text(errors="replace")
        except OSError:
            return

        if not self._import_re_match(text):
            return

        job_type = self._infer_job_type(text)
        job_name = _slugify(path.stem)
        inputs, outputs = self._infer_io(text, result)

        if inputs or outputs:
            result.jobs.append(DetectedJob(
                namespace=result.namespace,
                name=job_name,
                job_type=job_type,
                inputs=inputs,
                outputs=outputs,
                source_file=str(path),
            ))

    def _import_re_match(self, text: str) -> bool:
        return bool(self._IMPORT_RE.search(text))

    def _infer_job_type(self, text: str) -> str:
        if "airflow" in text.lower():
            return "airflow_task"
        if "pyspark" in text.lower() or "SparkSession" in text:
            return "spark"
        if "flink" in text.lower():
            return "flink"
        return "python"

    def _infer_io(
        self, text: str, result: ScanResult
    ) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
        inputs: list[tuple[str, str]] = []
        outputs: list[tuple[str, str]] = []

        tables = self._SQL_TABLE_RE.findall(text)
        known_logical_names = {c.logical_name for c in result.connections}

        for schema_dot, table in tables:
            schema = schema_dot.rstrip(".") if schema_dot else None
            ns = schema if schema and schema in known_logical_names else result.namespace
            entry = (ns, table.lower())
            if self._READ_RE.search(text) and entry not in inputs:
                inputs.append(entry)
            elif self._WRITE_RE.search(text) and entry not in outputs:
                outputs.append(entry)

        return inputs, outputs


class AirflowDagScanner:
    """Extract task-level lineage from Airflow DAG files."""

    _DAG_RE = re.compile(r"dag_id\s*=\s*['\"](?P<dag>[^'\"]+)['\"]")
    _TASK_ID_RE = re.compile(r"task_id\s*=\s*['\"](?P<task>[^'\"]+)['\"]")
    _SQL_RE = re.compile(r"sql\s*=\s*['\"](?P<sql>[^'\"]{10,})['\"]", re.DOTALL)

    def scan(self, root: Path, result: ScanResult) -> None:
        dag_dirs = list(root.rglob("dags")) + [root]
        seen_files: set[Path] = set()
        for d in dag_dirs:
            for py_file in d.glob("*.py"):
                if py_file in seen_files:
                    continue
                seen_files.add(py_file)
                self._scan_dag_file(py_file, result)

    def _scan_dag_file(self, path: Path, result: ScanResult) -> None:
        try:
            text = path.read_text(errors="replace")
        except OSError:
            return

        if "DAG" not in text and "airflow" not in text.lower():
            return

        dag_match = self._DAG_RE.search(text)
        if not dag_match:
            return

        dag_id = dag_match.group("dag")
        tables = self._extract_sql_tables(text)

        if tables:
            inputs = [(result.namespace, t) for t in tables if "src" in t or "raw" in t or "stg" in t]
            outputs = [(result.namespace, t) for t in tables if t not in [i[1] for i in inputs]]
            result.jobs.append(DetectedJob(
                namespace=result.namespace,
                name=dag_id,
                job_type="airflow_task",
                inputs=inputs,
                outputs=outputs,
                source_file=str(path),
            ))

    def _extract_sql_tables(self, text: str) -> list[str]:
        tables: list[str] = []
        for m in re.finditer(
            r"(?:FROM|JOIN|INTO)\s+[`'\"]?(\w+(?:\.\w+)?)[`'\"]?",
            text, re.IGNORECASE
        ):
            t = m.group(1).split(".")[-1].lower()
            if t not in ("select", "where", "and", "or", "not") and t not in tables:
                tables.append(t)
        return tables


# ---------------------------------------------------------------------------
# Main scanner
# ---------------------------------------------------------------------------

def scan(project_dir: str, namespace: str | None = None) -> ScanResult:
    root = Path(project_dir).resolve()
    if not root.exists():
        raise FileNotFoundError(f"Project directory not found: {root}")

    ns = namespace or _slugify(root.name)
    result = ScanResult(project_dir=str(root), namespace=ns)

    scanners = [
        EnvScanner(),
        DockerComposeScanner(),
        PythonSourceScanner(),
        AirflowDagScanner(),
    ]
    for scanner in scanners:
        try:
            scanner.scan(root, result)
        except Exception as e:
            result.warnings.append(f"{scanner.__class__.__name__} failed: {e}")

    # Deduplicate connections by logical_name (keep first)
    seen: set[str] = set()
    unique: list[DetectedConnection] = []
    for c in result.connections:
        if c.logical_name not in seen:
            seen.add(c.logical_name)
            unique.append(c)
    result.connections = unique

    return result


def to_dict(result: ScanResult) -> dict[str, Any]:
    return {
        "project_dir": result.project_dir,
        "namespace": result.namespace,
        "warnings": result.warnings,
        "connections": [
            {**c.to_api_payload(), "_source_file": c.source_file}
            for c in result.connections
        ],
        "jobs": [
            {
                "namespace": j.namespace,
                "name": j.name,
                "job_type": j.job_type,
                "source_file": j.source_file,
                "lineage_event": j.to_lineage_event(),
            }
            for j in result.jobs
        ],
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("project_dir", help="Path to the project to scan")
    parser.add_argument("--namespace", help="OpenLineage job namespace (default: project dir name)")
    parser.add_argument("--out", help="Write JSON to file instead of stdout")
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")

    try:
        result = scan(args.project_dir, namespace=args.namespace)
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    data = to_dict(result)
    output = json.dumps(data, indent=2, ensure_ascii=False)

    if args.out:
        Path(args.out).write_text(output)
        print(f"Scan result written to {args.out}", file=sys.stderr)
    else:
        print(output)

    if result.warnings:
        for w in result.warnings:
            print(f"WARNING: {w}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
