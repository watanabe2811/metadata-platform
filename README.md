# Metadata Platform

Lightweight, OpenLineage-native metadata collector backed by Postgres.
Designed as the foundation layer that can later sync to OpenMetadata or DataHub
without rewriting producers.

## Architecture

```
Producers (FastAPI / Python ETL / PySpark / Flink / Airflow)
         │ OpenLineage RunEvent (HTTP POST)
         ▼
metadata-collector (FastAPI)
         │
         ▼
Postgres 15+
         │ (outbox table)
         ▼
Future: sync workers → OpenMetadata / DataHub
```

## Components

| Path | Purpose |
|---|---|
| `collector/` | FastAPI service: ingestion + search APIs |
| `bank-conn/` | Python library: logical-name → connection resolver + OpenLineage emission |
| `migrations/` | Plain SQL migrations (no ORM) |
| `samples/` | Example producers demonstrating usage |
| `docker/` | Docker Compose for local development |

---

## Quick start — Docker Compose (recommended for first run)

```bash
cd docker
docker compose up -d
```

This starts Postgres, applies migrations, and launches the collector on port 8080.

API docs: http://localhost:8080/docs

---

## Manual setup — when Postgres is already running

Use this path if you have an existing Postgres instance and do not want Docker.

### Prerequisites

- Python 3.11+
- `psql` on PATH (for migrations)
- A Postgres 15+ database (local or remote)

### 1. Create the database and user

```sql
-- run as a Postgres superuser
CREATE USER metadata WITH PASSWORD 'metadata';
CREATE DATABASE metadata OWNER metadata;
\c metadata
CREATE EXTENSION IF NOT EXISTS pg_trgm;   -- needed for fuzzy search
CREATE EXTENSION IF NOT EXISTS pgcrypto;  -- needed for gen_random_uuid()
```

### 2. Apply migrations

```bash
DATABASE_URL="postgresql://metadata:metadata@localhost:5432/metadata" \
  ./migrations/migrate.sh
```

The script is idempotent — safe to run multiple times. It applies only
unapplied migration files in `migrations/sql/` in version order.

### 3. Install the collector

```bash
cd collector
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

### 4. Configure

Copy and edit the default config file:

```bash
cp collector/config/config.yaml collector/config/config.local.yaml
# then edit config.local.yaml and point METADATA_CONFIG_FILE at it:
export METADATA_CONFIG_FILE=config/config.local.yaml
```

Key settings in `collector/config/config.yaml`:

| Key | Default | Description |
|---|---|---|
| `metadata_db_url` | `postgresql://metadata:metadata@localhost:5432/metadata` | Postgres connection string |
| `api_port` | `8080` | Port the collector listens on |
| `auth_required` | `false` | Set `true` in production (Keycloak JWT) |
| `auth_service_token` | `dev-token-change-me` | Dev bearer token |
| `max_lineage_depth` | `10` | Max recursive CTE depth for lineage traversal |
| `log_level` | `INFO` | `DEBUG` / `INFO` / `WARNING` |

Any setting can also be set via environment variable (env vars take precedence
over the YAML file):

```bash
export METADATA_DB_URL="postgresql://metadata:metadata@db.prod:5432/metadata"
export AUTH_REQUIRED=true
```

### 5. Run the collector

```bash
cd collector
source .venv/bin/activate
uvicorn metadata_collector.main:app --host 0.0.0.0 --port 8080 --reload
```

Or without `--reload` for a stable process:

```bash
uvicorn metadata_collector.main:app --host 0.0.0.0 --port 8080 --workers 2
```

### 6. Verify

```bash
curl http://localhost:8080/health
# {"status":"ok"}
```

### 7. (Optional) Install bank-conn for Python producers

```bash
cd bank-conn
pip install -e ".[dev]"
```

Configure `bank-conn/config/config.yaml`:

```yaml
collector:
  url: http://localhost:8080
  token: dev-token-change-me
```

Or via env vars:

```bash
export BANK_CONN_COLLECTOR_URL=http://localhost:8080
export BANK_CONN_COLLECTOR_TOKEN=dev-token-change-me
```

### 8. Run the smoke test

```bash
python samples/smoke_test.py
```

Expected output: ingests 3 OpenLineage events and exercises FR-1, FR-2,
related-connections, and downstream traversal.

---

## Configuration reference

### collector/config/config.yaml

```yaml
metadata_db_url: postgresql://user:pass@host:5432/metadata
db_pool_min_size: 2
db_pool_max_size: 10

api_host: "0.0.0.0"
api_port: 8080
api_prefix: /api/v1

auth_required: false
auth_service_token: dev-token-change-me

log_level: INFO

max_lineage_depth: 10
default_lineage_depth: 3
```

### bank-conn/config/config.yaml

```yaml
collector:
  url: http://localhost:8080
  token: dev-token-change-me

vault:
  addr: http://localhost:8200
  token: ""
  namespace: null

cache:
  ttl_seconds: 300

lineage:
  emit: true
  producer: bank-conn/0.1.0

job:
  namespace: mbbank.default
```

### Environment variable override table

| Env var | Config field |
|---|---|
| `METADATA_DB_URL` | `metadata_db_url` |
| `AUTH_REQUIRED` | `auth_required` |
| `AUTH_SERVICE_TOKEN` | `auth_service_token` |
| `LOG_LEVEL` | `log_level` |
| `METADATA_CONFIG_FILE` | path to collector YAML file |
| `BANK_CONN_COLLECTOR_URL` | `collector.url` |
| `BANK_CONN_COLLECTOR_TOKEN` | `collector.token` |
| `VAULT_ADDR` | `vault.addr` |
| `VAULT_TOKEN` | `vault.token` |
| `BANK_CONN_EMIT_LINEAGE` | `lineage.emit` |
| `BANK_CONN_JOB_NAMESPACE` | `job.namespace` |
| `BANK_CONN_CONFIG_FILE` | path to bank-conn YAML file |

---

## Running tests

```bash
# Unit tests only (no Postgres required)
cd collector && pytest -m unit
cd bank-conn  && pytest -m unit

# Integration tests (requires Postgres)
cd collector
TEST_DB_URL="postgresql://metadata:metadata@localhost:5432/metadata" pytest -m integration

# Full suite
cd collector && pytest
```

---

## Key API endpoints

### Ingestion
- `POST /api/v1/lineage` — OpenLineage RunEvent
- `POST /api/v1/connections` — register connection
- `PUT  /api/v1/connections/{logical_name}` — update connection (FR-3)

### Search
- `GET /api/v1/search/connections/{logical_name}/jobs` — FR-1: jobs touching connection
- `GET /api/v1/search/datasets/{fqn}/jobs` — FR-2: readers + writers of a table
- `GET /api/v1/search/connections/{logical_name}/related` — connections sharing jobs
- `GET /api/v1/lineage/dataset/{fqn}/upstream?depth=3`
- `GET /api/v1/lineage/dataset/{fqn}/downstream?depth=3`

---

## Compliance notes

- All entity changes are audit-logged (`audit_log` table) for TT09 traceability.
- Credentials never stored in Postgres; only Vault paths.
- Soft-delete (`deleted_at`) preserves lineage history for BCBS 239.

---

## Status

- [x] Postgres schema + migrations
- [x] Collector FastAPI app (ingestion + search)
- [x] `bank-conn` Python library (SQLAlchemy adapter)
- [x] YAML + env var configuration for both services
- [x] Sample Python ETL producer
- [x] Docker Compose
- [x] Test suite (125 tests: 48 unit + 77 integration)
- [ ] PySpark adapter in `bank-conn`
- [ ] Flink adapter
- [ ] Outbox sync worker
- [ ] Keycloak JWT auth (stub exists in `api/deps.py`)
