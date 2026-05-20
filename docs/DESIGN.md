# Design Document — Metadata Collector

---

## 1. Mục tiêu & phạm vi

### Vấn đề cần giải quyết

MBBank vận hành nhiều hệ thống dữ liệu (T24 Core Banking, Iceberg lakehouse, Apache Doris, Kafka CDC). Khi một database connection thay đổi địa chỉ vật lý, không có cách nào biết job nào sẽ bị ảnh hưởng. Khi một bảng bị thay đổi schema, không có cách nào trace ngược về nguồn hoặc trace xuôi về các báo cáo downstream.

### Functional Requirements

| ID | Yêu cầu |
|---|---|
| FR-1 | Cho một connection (tên logic), tìm tất cả jobs đọc hoặc ghi vào đó |
| FR-2 | Cho một bảng (FQN), tìm tất cả jobs đọc hoặc ghi vào bảng đó |
| FR-3 | Thay đổi địa chỉ vật lý của connection (host, port) mà không cần chỉnh job code |

### Non-functional Requirements

| Yêu cầu | Mức độ |
|---|---|
| Audit trail đầy đủ (TT09/2020, BCBS 239) | Bắt buộc |
| Không lưu credentials trong DB | Bắt buộc |
| Tích hợp OpenLineage-native | Bắt buộc |
| Scale đến ~5M lineage edges | Đủ dùng trong 12 tháng |
| Không thêm hệ thống lưu trữ thứ hai | Bắt buộc |

### Ngoài phạm vi

- Business glossary / quản lý thuật ngữ nghiệp vụ → OpenMetadata
- Data quality framework → Great Expectations
- Access policies / ABAC → Keycloak + OpenMetadata
- BI dashboard lineage → OpenMetadata/DataHub native connectors
- Data profiling / statistics → OpenMetadata profiler

---

## 2. Kiến trúc tổng thể

```
┌─────────────────────────────────────────────────────────────────┐
│                         Producer Layer                          │
│                                                                 │
│  Airflow DAG  ──┐                                               │
│  Spark Job    ──┼──► bank-conn ──► POST /api/v1/lineage         │
│  Flink Job    ──┘         │                                     │
│  Python ETL   ────────────┘                                     │
│  (raw HTTP)                                                     │
└────────────────────────────┬────────────────────────────────────┘
                             │ OpenLineage RunEvent
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Collector (FastAPI)                         │
│                                                                 │
│  lineage_router ──► LineageIngestionService                     │
│  connection_router ──► ConnectionRepository                     │
│  search_router ──► LineageRepository (recursive CTE)           │
└────────────────────────────┬────────────────────────────────────┘
                             │ asyncpg
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                        PostgreSQL 15                            │
│                                                                 │
│  connection  dataset  dataset_column  job  job_run              │
│  lineage_edge  outbox  audit_log  schema_migrations             │
└─────────────────────────────────────────────────────────────────┘
                             │ outbox (future)
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│               Downstream sinks (chưa implement)                 │
│                                                                 │
│  OpenMetadata Worker ──► OpenMetadata API                       │
│  DataHub Worker      ──► DataHub API                            │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Data model

### 3.1 Sơ đồ quan hệ

```
connection
    │ 1
    │
    │ N
  dataset ──────────────── dataset_column
    │ 1                         (N columns per dataset)
    │
    │ N
lineage_edge ──── N ──── job ──── N ──── job_run
   (direction:                            (per run)
    input/output)

outbox      (event queue, append-only)
audit_log   (immutable change history)
```

### 3.2 Mô tả từng bảng

#### `connection`
Registry trung tâm của tất cả hệ thống nguồn.

```sql
id              UUID PK
logical_name    TEXT UNIQUE NOT NULL   -- bất biến, dùng trong lineage
platform        TEXT NOT NULL          -- oracle / kafka / iceberg / ...
host            TEXT                   -- mutable (FR-3)
port            INT                    -- mutable (FR-3)
service_name    TEXT                   -- mutable
vault_path      TEXT                   -- reference đến Vault, không lưu password
classification  TEXT                   -- public / internal / confidential
properties      JSONB                  -- platform-specific config (Kafka bootstrap_servers, v.v.)
deleted_at      TIMESTAMPTZ            -- soft-delete
```

**Quy tắc bất biến:** `logical_name` không được thay đổi sau khi tạo. Toàn bộ lineage history dùng `logical_name` làm khóa. Đổi tên = mất toàn bộ lịch sử.

#### `dataset`
Bảng / topic / file cụ thể bên trong một connection.

```sql
connection_id   UUID FK → connection
fqn             TEXT NOT NULL   -- "<logical_name>.<dataset_name>", unique per connection
name            TEXT            -- tên ngắn
dataset_type    TEXT            -- table / view / topic / file / iceberg_table / ...
classification  TEXT            -- override từ connection nếu cần
properties      JSONB           -- schema facets từ OpenLineage
```

**FQN construction:** `ingestion_service._upsert_dataset` xây dựng FQN theo pattern `{namespace}.{name}` trong đó `namespace` từ OL event chính là `connection.logical_name`.

#### `lineage_edge`
Đây là bảng trung tâm — mỗi row là một quan hệ job↔dataset.

```sql
job_id          UUID FK → job
dataset_id      UUID FK → dataset
direction       TEXT    -- "input" (job đọc) hoặc "output" (job ghi)
column_mapping  JSONB   -- column-level lineage từ OL columnLineage facet
first_seen_at   TIMESTAMPTZ
last_seen_at    TIMESTAMPTZ   -- cập nhật mỗi lần event đến
UNIQUE (job_id, dataset_id, direction)
```

**Lý do không có separate `lineage_node` table:** Với Postgres và ~5M edges, recursive CTE trên `lineage_edge` đủ nhanh (< 50ms cho depth 10 trong benchmark nội bộ). Không cần graph DB.

#### `outbox`
Transactional outbox pattern — event được append trong cùng transaction với lineage write.

```sql
id              BIGSERIAL PK
aggregate_type  TEXT         -- "job_run", "connection", ...
event_type      TEXT         -- "ol.complete", "connection.updated", ...
payload         JSONB        -- full event payload
published_at    TIMESTAMPTZ  -- NULL nếu chưa sync
published_targets TEXT[]     -- ["openmetadata", "datahub"]
```

Worker (chưa implement) đọc `WHERE published_at IS NULL`, gọi downstream API, append target vào `published_targets`.

#### `audit_log`
Immutable. Mọi mutation trên `connection`, `dataset`, `job` đều append vào đây.

```sql
actor        TEXT    -- Keycloak JWT subject (hiện là "service-account" stub)
action       TEXT    -- create / update / delete / upgrade_stub
entity_type  TEXT
before_state JSONB
after_state  JSONB
```

---

## 4. Component design

### 4.1 Collector (FastAPI)

```
collector/src/metadata_collector/
├── main.py              FastAPI app + lifespan (pool init/close)
├── settings.py          Pydantic-settings, env-driven
├── db.py                asyncpg pool, JSONB codec registration
├── schemas.py           Pydantic I/O models + platform property validators
├── api/
│   ├── deps.py          require_token (auth stub → Keycloak)
│   ├── lineage_router.py     POST /lineage
│   ├── connection_router.py  CRUD /connections
│   └── search_router.py      GET /search/*
├── repositories/
│   ├── connection_repo.py    SQL: connection CRUD + upsert_minimal
│   ├── dataset_repo.py       SQL: dataset upsert + get_by_fqn
│   ├── job_repo.py           SQL: job upsert + job_run upsert
│   ├── lineage_repo.py       SQL: edge upsert + recursive CTE traversal
│   └── outbox_repo.py        SQL: outbox append + audit_log
└── services/
    └── ingestion_service.py  Orchestrate ingest trong 1 transaction
```

**Transaction boundary:** Tất cả write trong một `POST /lineage` xảy ra trong một transaction duy nhất. Nếu bất kỳ bước nào fail, toàn bộ rollback. Outbox append cũng nằm trong cùng transaction — đây là điểm then chốt của outbox pattern.

**Repository convention:** Repo chỉ nhận/trả `dict` và `list[dict]`. Pydantic validation chỉ ở API boundary (router). Không có ORM — SQL raw qua asyncpg.

### 4.2 bank-conn (Producer library)

```
bank-conn/src/bank_conn/
├── __init__.py      export: Connection, configure
├── config.py        BankConnConfig (env-driven)
├── connection.py    Connection class (context manager)
├── emitter.py       LineageEmitter (HTTP client)
└── resolver.py      ConnectionResolver (TTL cache → collector + Vault)
```

**Resolution flow:**
```
Connection("t24-core-prod")
    │
    ├─► ConnectionResolver.resolve("t24-core-prod")
    │       │
    │       ├─► GET /api/v1/connections/t24-core-prod  (collector)
    │       │   → {host, port, service_name, vault_path, ...}
    │       │
    │       └─► Vault KV v2 read(vault_path)
    │           → {username, password}
    │
    └─► ResolvedConnection(host, port, username, password, ...)
```

**Cache:** TTL 300s (configurable). Thread-safe với `threading.Lock`. `invalidate()` cho credential rotation.

**Lineage emission là fire-and-forget:** `emitter._emit()` bắt `httpx.HTTPError` và log warning. Không bao giờ để lỗi lineage làm crash job business logic.

### 4.3 Lineage graph traversal

Upstream (từ dataset, đi ngược về nguồn):

```sql
WITH RECURSIVE upstream AS (
    -- anchor: dataset gốc
    SELECT d.id, d.fqn, NULL::uuid AS via_job_id, 0 AS depth, ARRAY[d.id] AS path
    FROM dataset d WHERE d.fqn = $1 AND d.deleted_at IS NULL

    UNION ALL

    -- recursive: dataset → (output edge) → job → (input edge) → dataset nguồn
    SELECT d_in.id, d_in.fqn, j.id, us.depth + 1, us.path || d_in.id
    FROM upstream us
    JOIN lineage_edge le_out ON le_out.dataset_id = us.dataset_id AND le_out.direction = 'output'
    JOIN job j ON j.id = le_out.job_id AND j.deleted_at IS NULL
    JOIN lineage_edge le_in ON le_in.job_id = j.id AND le_in.direction = 'input'
    JOIN dataset d_in ON d_in.id = le_in.dataset_id AND d_in.deleted_at IS NULL
    WHERE us.depth < $2          -- max_depth cap
      AND d_in.id <> ALL(us.path) -- cycle guard
)
SELECT depth, dataset_id, dataset_fqn, via_job_id FROM upstream WHERE depth > 0
```

Downstream: đảo chiều join (`input` → `output`).

**Cycle guard:** `d_in.id <> ALL(us.path)` — mảng path giữ tất cả dataset đã thăm. Nếu graph có cycle (ví dụ: job đọc và ghi cùng bảng), query không loop vô tận.

---

## 5. Security design

### Credentials

```
┌──────────┐         ┌───────────┐         ┌───────┐
│  bank-conn│ ──GET──► collector  │ ──READ──► Vault  │
│          │  metadata (no pwd)   │  KV v2   │       │
└──────────┘         └───────────┘         └───────┘
```

- Collector chỉ lưu `vault_path` (reference), không bao giờ lưu password
- bank-conn fetch credentials trực tiếp từ Vault, không qua collector
- Credential rotation = đổi secret trong Vault, `invalidate()` cache bank-conn

### Authentication (hiện tại)

```python
# api/deps.py — V1: shared bearer token
if token != settings.auth_service_token:
    raise HTTPException(401)
return "service-account"  # actor stub
```

**V2 (roadmap):** Thay bằng Keycloak OIDC JWT verification. Actor = JWT `sub` claim.

### Audit trail

Mọi mutation trên `connection` đều có entry trong `audit_log` với `before_state` và `after_state` dạng JSONB. Immutable — không có UPDATE/DELETE trên `audit_log`.

---

## 6. Migration strategy

### Hệ thống migration

Plain SQL files + shell runner (không dùng Alembic):

```
migrations/
├── sql/V0001__initial_schema.sql
└── migrate.sh
```

`migrate.sh` track applied versions trong bảng `schema_migrations`. Idempotent — chạy nhiều lần an toàn.

### Convention

- File mới: `V<next>__<description>.sql`
- Không bao giờ sửa file đã apply
- DDL phải idempotent (`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`)
- Soft-delete only — không có `DROP TABLE` trong migration data

---

## 7. Outbox pattern (roadmap)

Khi xây dựng worker sync sang OpenMetadata/DataHub:

```
┌─────────────────────────────────────────────────────┐
│  Worker loop (asyncio, plain Python)                │
│                                                     │
│  while True:                                        │
│      events = SELECT * FROM outbox                  │
│               WHERE published_at IS NULL            │
│               AND 'openmetadata' != ALL(published_targets)│
│               ORDER BY id LIMIT 100                 │
│                                                     │
│      for event in events:                           │
│          call OpenMetadata API                      │
│          append 'openmetadata' to published_targets │
│          if all targets covered: set published_at   │
│                                                     │
│      sleep(5)                                       │
└─────────────────────────────────────────────────────┘
```

**Idempotency:** Dựa vào natural keys của OpenMetadata (dataset FQN, job namespace+name). Retry an toàn.

**Không dùng Celery/Dramatiq:** Worker là asyncio loop đơn giản. Đủ dùng cho volume dự kiến (~10K events/ngày).

---

## 8. Scaling considerations

| Mức | Edge count | Approach |
|---|---|---|
| Current | < 500K | Recursive CTE, no special config |
| 12 tháng | ~2-5M | Partition `lineage_edge` by `first_seen_at` year |
| Cần graph DB | > 20M | Migrate đến OpenMetadata (có native graph backend) |

Index hiện tại đủ cho phase 1:
- `idx_edge_dataset_direction` — covering cho upstream/downstream CTE join
- `idx_edge_job_direction` — covering cho FR-1 query
- `idx_dataset_fqn_trgm` — fuzzy search
- `idx_connection_logical_name_trgm` — fuzzy search

---

## 9. Known gaps (priority order)

| # | Gap | Impact | Effort |
|---|---|---|---|
| 1 | Keycloak JWT auth | `audit_log.actor` là meaningless | M |
| 2 | Outbox worker | Không sync sang OpenMetadata | L |
| 3 | PySpark/Flink adapters trong bank-conn | ETL team cần code thủ công | M |
| 4 | Vault mount-point parsing | Lỗi nếu mount-point khác `<mount>/<path>` | S |
| 5 | Column lineage CTE | Data có nhưng không query được | M |
| 6 | Classification propagation | `classification` là static field | M |

---

## 10. Decision log

| Quyết định | Thay thế đã cân nhắc | Lý do chọn |
|---|---|---|
| OpenLineage làm wire format | Custom JSON API | Free migration path sang OpenMetadata/DataHub |
| asyncpg (không SQLAlchemy ORM) | SQLAlchemy async | Không có overhead ORM, explicit SQL rõ ràng |
| Recursive CTE thay graph DB | Neo4j, Amazon Neptune | Postgres đủ cho 5M edges; zero ops overhead |
| Outbox pattern thay direct API call | gRPC push sang OpenMetadata | Decoupling; collector không cần biết downstream |
| Plain SQL + migrate.sh thay Alembic | Alembic, Flyway | Không cần Python runtime cho migration; chỉ cần `psql` |
| Soft-delete (`deleted_at`) thay hard delete | Hard DELETE | BCBS 239: lineage history phải queryable |
| Vault reference thay lưu password | Secrets trong Postgres column | TT09: credentials phải trong HSM/Vault |

---

## 11. Testing strategy

### Pyramid

```
                    ┌─────────────┐
                    │  Smoke test │  1 script, end-to-end against real stack
                    └──────┬──────┘
               ┌───────────┴───────────┐
               │    Integration tests  │  DB required; per-test rollback isolation
               └───────────┬───────────┘
          ┌─────────────────┴─────────────────┐
          │           Unit tests              │  No external deps; all mocked
          └───────────────────────────────────┘
```

### Unit tests — `pytest -m unit`

No database, no network. All external calls (httpx, hvac, SQLAlchemy) are mocked.

| File | What it covers |
|---|---|
| `collector/tests/unit/test_schemas.py` | Pydantic validation: name patterns, platform-specific required properties, OpenLineage event shape |
| `collector/tests/unit/test_ingestion_static.py` | Pure logic: `_infer_job_type`, `_infer_dataset_type`, `_extract_column_lineage` |
| `bank-conn/tests/unit/test_config.py` | BankConnConfig env overrides, `configure()` |
| `bank-conn/tests/unit/test_resolver.py` | Cache hit/miss/TTL, `invalidate()`, 404 → `ConnectionResolutionError`, Vault fetch, auth header |
| `bank-conn/tests/unit/test_connection.py` | `sqlalchemy_url()`, `jdbc_options()`, `record_read/write`, context manager START/COMPLETE/FAIL |
| `bank-conn/tests/unit/test_emitter.py` | Payload shape, error facet, HTTP error suppression, `emit_lineage=False` |

### Integration tests — `pytest -m integration`

Require a running Postgres at `TEST_DB_URL` (default: `postgresql://metadata:metadata@localhost:5432/metadata`).

**Isolation**: each test gets a dedicated `asyncpg` connection with an open transaction. The `db_conn` fixture always rolls back after the test, regardless of pass/fail. This gives clean state without truncating tables.

```python
# collector/tests/integration/conftest.py (simplified)
@pytest.fixture
async def db_conn(db_pool):
    async with db_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        yield conn
        await tr.rollback()
```

| File | What it covers |
|---|---|
| `test_connection_repo.py` | CRUD, soft-delete, `upsert_minimal` (stub creation idempotency), search (q, host, platform, classification) |
| `test_dataset_repo.py` | CRUD, `get_by_fqn`, search (q, connection, dataset_type, combined) |
| `test_lineage_repo.py` | Edge upsert/idempotency, FR-1 jobs-for-connection, FR-2 jobs-for-dataset, upstream/downstream CTE, depth limit, cycle guard |
| `test_ingestion_service.py` | Full ingest flow: job, job_run, stub connections, datasets, lineage edges, outbox; idempotency; START→COMPLETE transition; column lineage storage |
| `test_search_api.py` | API-level via `httpx.AsyncClient + ASGITransport`; health, POST lineage, connection CRUD, search endpoints, FR-1 and FR-2 routes |

### Running

```bash
# Start Postgres
cd docker && docker compose up -d postgres

# Apply schema
cd migrations && bash migrate.sh

# Unit tests (no Postgres)
cd collector && pytest -m unit -v
cd bank-conn  && pytest -m unit -v

# Integration tests
cd collector && pytest -m integration -v

# Everything
cd collector && pytest -v
cd bank-conn  && pytest -v
```

### CI recommendation

- Unit tests: run on every push, no services needed.
- Integration tests: run in a `services: postgres:15` container job. Set `TEST_DB_URL` to the service address and run `migrate.sh` in the `before_script`.
