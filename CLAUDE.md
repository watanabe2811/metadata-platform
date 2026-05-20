# CLAUDE.md

Guidance for Claude Code when working in this repository.

## What this project is

Lightweight, OpenLineage-native **metadata collector** for MBBank's data
platform. Postgres-backed. Designed as a first-stage metadata store that
can later sync to OpenMetadata / DataHub via the outbox pattern.

Solves three concrete requirements:

1. **FR-1**: Given a database connection (logical name), find all jobs that touch it.
2. **FR-2**: Given a table FQN, find all jobs that read from / write to it.
3. **FR-3**: Change a database connection's physical address without touching job code.

The codebase is **not** a re-implementation of OpenMetadata or DataHub.
It is a thin collector + connection registry + search layer, sized for the
first ~12 months. Anything that would require building a full metadata
governance UI, glossary engine, or business-term framework belongs in
OpenMetadata/DataHub, not here.

## Repository layout

```
metadata-platform/
├── collector/                          FastAPI ingestion + search service
│   └── src/metadata_collector/
│       ├── main.py                     App entry + lifespan
│       ├── settings.py                 Env-driven config (pydantic-settings)
│       ├── db.py                       asyncpg pool + JSONB codec
│       ├── schemas.py                  Pydantic I/O models (incl. OpenLineage)
│       ├── api/                        FastAPI routers
│       ├── repositories/               Thin per-aggregate DB access
│       └── services/                   Transactional orchestration
├── bank-conn/                          Producer library (logical-name resolver + OL emitter)
│   └── src/bank_conn/
├── migrations/                         Plain SQL migrations
│   ├── sql/                            Numbered SQL files (V0001__*.sql, ...)
│   └── migrate.sh                      Runner script (uses psql)
├── samples/                            Demo producers + smoke test
└── docker/                             Local-dev Compose
```

## Architecture invariants — do not break these

1. **OpenLineage is the wire format.** Never invent a parallel ingestion API.
   New producers emit OL RunEvents to `POST /api/v1/lineage`. This is what
   makes future migration to OpenMetadata/DataHub free.

2. **The outbox table is the only sync channel out.** Future sinks
   (OpenMetadata, DataHub) read from `outbox`; they do not call the
   collector or DB directly. Every entity-mutating service must append
   to `outbox` in the same transaction. Bypassing it = silent drift.

3. **Logical connection names are immutable in lineage.** `dataset.fqn` is
   `<connection.logical_name>.<dataset_name>`. Renaming a connection breaks
   historical lineage. Physical attributes (host, port, vault_path) are
   mutable; logical_name is not.

4. **Soft-delete only.** Use `deleted_at` everywhere; never `DELETE FROM`.
   Lineage history must remain queryable for BCBS 239 / TT09 audit.

5. **No secrets in Postgres.** `connection.vault_path` is a reference;
   actual credentials live in Vault. If a code change introduces a
   password column anywhere, that's a regression — flag it.

6. **Postgres is the only store.** No Neo4j, no Elasticsearch, no separate
   graph DB. Recursive CTEs are the answer up to ~5M edges. If genuinely
   blocked at scale, the next step is migrating to OpenMetadata, not
   adding a graph DB.

## Tech stack — locked choices

| Layer | Choice | Why |
|---|---|---|
| Language | Python 3.11+ | Async stdlib, type generics |
| API | FastAPI 0.111 | Pydantic v2 native |
| DB driver | asyncpg | Async, fast, no SQLAlchemy ORM overhead |
| Schema migrations | Plain SQL + migrate.sh | No Python deps, runs anywhere psql is available |
| Postgres | 15+ | JSONB ops, `gen_random_uuid()` via pgcrypto |
| Validation | Pydantic v2 | Strict v2 — no v1 compat shims |
| Logging | python-json-logger | Structured logs for ELK/Loki |

**Do not introduce:**
- SQLAlchemy ORM (we use SQLAlchemy only in `bank-conn` for client-side query convenience)
- Celery / Dramatiq (the outbox worker, when built, should use plain asyncio loop)
- Pydantic v1
- A second database
- ORMs in repositories

## Conventions

### Code

- **No pseudo-code.** Every code change must be runnable. Include imports.
- **Strict typing.** `from __future__ import annotations` at the top of every module. Type all function signatures.
- **Repositories return `dict` / `list[dict]`**, not Pydantic models. Pydantic happens at the API boundary, not in the data layer.
- **Services own transactions.** Routers never start transactions; services do (`async with conn.transaction():`).
- **No bare `except:`.** Catch specific exceptions; re-raise as `HTTPException` only at the API layer.
- **No prints.** Use `logger = logging.getLogger(__name__)` and structured logs.

### SQL

- **Recursive CTEs for graph traversal.** See `lineage_repo.py::upstream/downstream`. Always cap by `max_depth` (settings: `MAX_LINEAGE_DEPTH`, default 10) and use `<> ALL(path)` to prevent cycles.
- **Parameter casts when types are ambiguous.** Postgres can't infer types through `CASE … ELSE NULL`. Always cast: `NULL::timestamptz`, `$4::timestamptz`, `$5::jsonb`.
- **JSONB writes use `json.dumps(d) + ::jsonb` cast.** JSONB reads return `dict` automatically (codec registered in `db.py::_setup_codecs`).
- **Index any column used in a WHERE that filters lineage queries.** New columns added to `dataset` / `job` / `lineage_edge` need an index review.

### Naming

- Logical connection names: lowercase, `[a-z0-9._-]`, no uppercase, no spaces.
  Pattern enforced by `ConnectionCreate.logical_name`.
- Dataset FQN: `<connection.logical_name>.<dataset_name>`. Built in `ingestion_service._upsert_dataset`.
- Job FQN (for display): `<job.namespace>.<job.name>`. `namespace` is typically the OpenLineage producer namespace (e.g. `mbbank.dwh`).

### API design

- **Mutations are audit-logged.** Every POST/PUT/DELETE on `connection` writes an `audit_log` entry with actor + before/after state.
- **Read endpoints are unauthenticated in dev** (`AUTH_REQUIRED=false`) but **must be JWT-protected in prod**. The placeholder `require_token` in `api/deps.py` is a stub for Keycloak integration.
- **404 on missing entities, 409 on conflicts.** Never 500 for expected business cases.
- **Path-parameter FQNs use `{fqn:path}`** because FQNs contain `.` and `/`. Search endpoints depend on this — don't change route declarations.

## How to extend

### Add a new producer type

1. In `bank-conn`, add an adapter to `connection.py` if the framework needs a special connection object (e.g. PySpark JDBC reader, Flink JDBC connector).
2. Configure the framework's OpenLineage integration to POST to the collector's `/api/v1/lineage` endpoint.
3. Producer must include `producer` field in events; `ingestion_service._infer_job_type` will map it to a known `job_type`. Add the new producer string there if needed.

### Add a new search endpoint

1. Add SQL query to the relevant repository. Use recursive CTEs for graph traversal. Always cap depth.
2. Add Pydantic response model to `schemas.py`.
3. Add router endpoint in `api/search_router.py`. Use `require_token` dependency.
4. Test with `samples/smoke_test.py` — extend it; don't write a new test file.

### Add a new entity type (e.g. dashboard, ML model)

1. Add table in a new Alembic migration. Follow the soft-delete + JSONB-properties + audit pattern from existing tables.
2. Add repository in `repositories/`. Mirror the structure of `dataset_repo.py`.
3. If the entity participates in lineage, extend `lineage_edge` (it's already entity-agnostic via `dataset_id`; rename to `entity_id` only if necessary — that's a breaking migration).
4. Update `ingestion_service.py` to handle the new entity in OL events.

### Add a sync target (e.g. OpenMetadata)

The outbox worker pattern is documented but not yet implemented. When building:

1. New worker process reads `outbox` where `published_at IS NULL` AND `'<target>' != ALL(published_targets)`.
2. Per event, call the target's API.
3. On success, append `<target>` to `published_targets`. Set `published_at = now()` only when `published_targets` covers all configured sinks.
4. Failures: log and retry next loop. Idempotency comes from upstream's natural keys (OL runId, FQN).

**Do not** modify the collector to write directly to OpenMetadata. The outbox is the only path out.

## Compliance constraints (MBBank context)

- **NHNN TT09/2020**: All entity mutations are audit-logged with actor + timestamp + before/after.
- **BCBS 239**: Lineage history is never hard-deleted (soft-delete only).
- **Three-zone data classification** (public/internal/confidential): connection-level `classification` propagates to datasets unless overridden. The propagation logic is **not yet implemented** — currently a static field. Flag this if a feature request needs cascading classification.
- **Credential rotation**: physical credentials live in Vault; the collector never sees passwords. Rotation = update Vault path; nothing in this codebase changes.

## Running locally

```bash
# DB only
cd docker && docker compose up -d postgres

# Apply migrations (requires psql on PATH)
DATABASE_URL="postgresql://metadata:metadata@localhost:5432/metadata" \
  ./migrations/migrate.sh

# Install + run
cd collector && pip install -e .
cd ../bank-conn && pip install -e .

METADATA_DB_URL="postgresql://metadata:metadata@localhost:5432/metadata" \
AUTH_REQUIRED=false \
uvicorn metadata_collector.main:app --reload --port 8080

# Smoke test
python samples/smoke_test.py
```

## Adding a new migration

1. Create `migrations/sql/V<next>__<description>.sql` (e.g. `V0002__add_tag_table.sql`).
2. Write idempotent DDL (`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`).
3. Run `./migrations/migrate.sh` — it skips already-applied versions and applies only the new file.
4. Never edit an already-applied migration file; always add a new one.

## Smoke test is the contract

`samples/smoke_test.py` is the canonical end-to-end test. It:
- Ingests 3 OpenLineage events
- Exercises FR-1, FR-2, related-connections, downstream traversal
- Expects specific job/dataset/edge counts

**Any change that breaks the smoke test breaks production.** Run it after
every non-trivial change before opening an MR.

If a change intentionally alters behavior, update the smoke test in the
same commit and explain the change in the MR description.

## Testing

### Test structure

```
collector/
└── tests/
    ├── conftest.py                     # pytest markers registration
    ├── unit/
    │   ├── test_schemas.py             # Pydantic model validation (no DB)
    │   └── test_ingestion_static.py    # Pure logic: infer_job_type, infer_dataset_type, column lineage
    └── integration/
        ├── conftest.py                 # db_pool + db_conn fixtures (rollback isolation)
        ├── test_connection_repo.py     # ConnectionRepository CRUD + search
        ├── test_dataset_repo.py        # DatasetRepository CRUD + search
        ├── test_lineage_repo.py        # LineageRepository edges, FR-1, FR-2, traversal, cycle guard
        ├── test_ingestion_service.py   # LineageIngestionService full ingest flow
        └── test_search_api.py          # API-level tests via httpx.AsyncClient + ASGITransport

bank-conn/
└── tests/
    └── unit/
        ├── test_config.py              # BankConnConfig env overrides, configure()
        ├── test_resolver.py            # ConnectionResolver cache, 404, Vault
        ├── test_connection.py          # Connection adapters, record_read/write, context manager
        └── test_emitter.py             # LineageEmitter payload + error suppression
```

### Running tests

```bash
# Unit tests only (no Postgres required)
cd collector && pytest -m unit
cd bank-conn && pytest -m unit

# Integration tests (requires Postgres)
cd docker && docker compose up -d postgres
cd collector && TEST_DB_URL="postgresql://metadata:metadata@localhost:5432/metadata" pytest -m integration

# All collector tests
cd collector && pytest

# All bank-conn tests
cd bank-conn && pytest
```

### Key conventions

- **`TEST_DB_URL`** env var overrides the default `postgresql://metadata:metadata@localhost:5432/metadata`.
- **Integration test isolation**: every test runs inside a transaction that is rolled back in the fixture teardown. No truncate needed, no test ordering dependencies.
- **`pytest-asyncio` in auto mode**: all `async def test_*` functions are automatically detected. No `@pytest.mark.asyncio` needed.
- **API tests inject the test pool**: `db_module._pool = db_pool` before each test so the app uses the rolled-back transaction pool, not a real connection pool.
- **Unit tests mock everything external**: HTTP calls (httpx), Vault (hvac), SQLAlchemy engine creation. Never make real network calls in unit tests.

## What is NOT in scope

Do not add to this codebase:

- Business glossary / term management → belongs in OpenMetadata
- Tag propagation engine → OpenMetadata
- Access policies / ABAC → Keycloak + OpenMetadata
- Data quality test framework → Great Expectations or OpenMetadata DQ
- BI dashboard ingestion → OpenMetadata or DataHub native connectors
- Profiling / statistics → OpenMetadata profiler workflow

If asked to build any of these here, push back. The right answer is to
finish the outbox sync to OpenMetadata, then let OpenMetadata own those
concerns.

## Known gaps to address (priority order)

1. **Keycloak JWT auth** — `api/deps.py::require_token` is a stub. Replace with JWT verification against Keycloak OIDC discovery.
2. **Outbox worker** — table exists; worker process does not. P6 in the original plan.
3. **PySpark + Flink adapters in `bank-conn`** — only SQLAlchemy / JDBC string is implemented. P3 + P5.
4. **Vault KV v2 mount-point parsing** in `bank-conn/resolver.py::_fetch_secrets` — assumes `<mount>/<path>`; needs validation against real Vault layout.
5. **Column-level lineage propagation** through recursive CTEs — current `upstream`/`downstream` queries traverse dataset edges only. A `col_upstream` query exists conceptually (in the design doc) but not in `lineage_repo.py` yet.
6. **Tag propagation through lineage** — `classification` is a static column today.

## Common pitfalls (learned the hard way during P0)

- **asyncpg + `timestamptz`**: pass `datetime` objects, not ISO strings. `datetime.fromisoformat(event["eventTime"])` before passing to repos.
- **asyncpg + Postgres CASE expressions**: explicit type casts on every branch. `CASE WHEN ... THEN $4::timestamptz ELSE NULL::timestamptz END`. Without these, Postgres infers `text` and the bind fails.
- **JSONB columns**: ensure `_setup_codecs` runs as pool init. Without it, JSONB reads come back as strings and Pydantic dict-validation fails with a 500.
- **FastAPI 0.111 + 204 endpoints**: declare `response_class=Response` and return `Response(status_code=204)` explicitly. Returning `None` raises an assertion at app-build time.
- **OpenLineage namespace = our `connection.logical_name`**. Producers that use random namespaces will auto-create stub connections. Engineers need to know this convention or stub connections will proliferate.

## Style for responses when asked to design/extend

- Follow the 5-step SDLC: Requirements → Solutions (≥3 options for non-trivial work) → Architecture → Plan → Execute.
- State confidence level (High/Medium/Low) on recommendations.
- Cite sources for benchmarks/performance claims, or say no source exists.
- No flattery, no filler sign-offs, no apologies for being an AI.
- Vietnamese input → translate to English at top of response, answer in English unless user explicitly requests Vietnamese.

## Where to find things

- Schema: `migrations/sql/V0001__initial_schema.sql` (single source of truth)
- API contract: live at `http://localhost:8080/docs` when running, or read the routers under `collector/src/metadata_collector/api/`
- Recursive lineage queries: `collector/src/metadata_collector/repositories/lineage_repo.py`
- Architecture decisions: this file + the chat history that produced it (not yet exported to ADRs — TODO)
