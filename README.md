# Metadata Platform — Option A scaffold

Lightweight, OpenLineage-native metadata collector backed by Postgres.

Designed as **the foundation layer** that can later sync to OpenMetadata or DataHub
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
| `migrations/` | Alembic schema migrations for Postgres |
| `samples/` | Example producers demonstrating usage |
| `docker/` | Docker Compose for local development |

## Quick start (local dev)

```bash
cd docker
docker compose up -d postgres
cd ../migrations
pip install -r requirements.txt
alembic upgrade head
cd ../collector
pip install -e .
uvicorn metadata_collector.main:app --reload --port 8080
```

API docs: http://localhost:8080/docs

## Key API endpoints

### Ingestion

- `POST /api/v1/lineage` — OpenLineage RunEvent
- `POST /api/v1/connections` — register/update connection
- `PUT  /api/v1/connections/{logical_name}` — update connection

### Search (FR-1, FR-2)

- `GET /api/v1/search/connections/{logical_name}/jobs` — FR-1: jobs touching this connection
- `GET /api/v1/search/datasets/{fqn}/jobs` — FR-2: readers + writers of a table
- `GET /api/v1/search/connections/{logical_name}/related` — connections sharing jobs
- `GET /api/v1/lineage/dataset/{fqn}/upstream?depth=3`
- `GET /api/v1/lineage/dataset/{fqn}/downstream?depth=3`

## Compliance notes

- All entity changes are audit-logged (`audit_log` table) for TT09 traceability.
- Sensitive credentials never stored in Postgres; only Vault paths.
- Soft-delete (`deleted_at`) preserves lineage history for BCBS 239.

## Status

- [x] Postgres schema + migrations
- [x] Collector FastAPI app (ingestion + search)
- [x] `bank-conn` Python library (SQLAlchemy adapter)
- [x] Sample Python ETL producer
- [x] Docker Compose
- [ ] PySpark adapter in `bank-conn` (next iteration)
- [ ] Flink adapter (next iteration)
- [ ] Outbox sync worker (next iteration)
- [ ] Internal search UI (next iteration)
