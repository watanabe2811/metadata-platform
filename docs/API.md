# Metadata Collector — API Reference

Base URL: `http://<host>:8080`  
API prefix: `/api/v1`  
Interactive docs: `http://<host>:8080/docs`

---

## Authentication

All `/api/v1/*` endpoints require a Bearer token in the `Authorization` header.

```
Authorization: Bearer <token>
```

In development (`AUTH_REQUIRED=false`), the header is optional.  
In production, the token must match `AUTH_SERVICE_TOKEN` env var (placeholder until Keycloak JWT is wired).

---

## Health

### `GET /health`

Liveness + readiness probe. Executes `SELECT 1` against Postgres.

**Response 200**
```json
{ "status": "ok" }
```

**Response 200 (degraded)**
```json
{ "status": "degraded", "error": "connection refused" }
```

---

## Lineage Ingestion

### `POST /api/v1/lineage`

Ingest một OpenLineage RunEvent. Đây là endpoint chính — toàn bộ job/dataset/lineage đều đi qua đây.

**Request body** — OpenLineage RunEvent (spec v2.0.2)

| Field | Type | Bắt buộc | Mô tả |
|---|---|---|---|
| `eventType` | string | ✓ | `START` \| `RUNNING` \| `COMPLETE` \| `FAIL` \| `ABORT` \| `OTHER` |
| `eventTime` | string (ISO 8601) | ✓ | Thời điểm sự kiện, có timezone |
| `run.runId` | string (UUID) | ✓ | UUID duy nhất của lần chạy này |
| `run.facets` | object | — | Metadata bổ sung cho run |
| `job.namespace` | string | ✓ | **Phải trùng với `logical_name` của Connection** |
| `job.name` | string | ✓ | Tên job, VD: `etl.t24.daily_stmt_load` |
| `job.facets` | object | — | Metadata bổ sung cho job |
| `inputs` | array[Dataset] | — | Danh sách dataset job đọc vào |
| `outputs` | array[Dataset] | — | Danh sách dataset job ghi ra |
| `producer` | string | ✓ | Identifier của producer, VD: `airflow/2.9.1` |
| `schemaURL` | string | ✓ | URL spec, dùng `https://openlineage.io/spec/2-0-2/OpenLineage.json` |

**Dataset object** (trong `inputs` / `outputs`):

| Field | Type | Bắt buộc | Mô tả |
|---|---|---|---|
| `namespace` | string | ✓ | `logical_name` của Connection chứa dataset này |
| `name` | string | ✓ | Tên bảng / topic / file |
| `facets` | object | — | Xem phần Facets bên dưới |

**Response 202**
```json
{
  "status": "accepted",
  "run_id": "a1b2c3d4-0000-0000-0000-000000000000"
}
```

**Errors**

| HTTP | Nguyên nhân |
|---|---|
| 400 | Payload không đúng format OpenLineage |
| 401 | Token thiếu hoặc sai (khi AUTH_REQUIRED=true) |
| 500 | Lỗi Postgres |

---

### Ví dụ đầy đủ — Job đọc từ T24, ghi vào Iceberg

```bash
curl -X POST http://localhost:8080/api/v1/lineage \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer dev-token-change-me" \
  -d '{
    "eventType": "COMPLETE",
    "eventTime": "2026-05-20T08:30:00+07:00",
    "run": {
      "runId": "550e8400-e29b-41d4-a716-446655440000",
      "facets": {}
    },
    "job": {
      "namespace": "mbbank.dwh",
      "name": "etl.t24.daily_stmt_load",
      "facets": {}
    },
    "inputs": [
      { "namespace": "t24-core-prod", "name": "STMT","facets": {} },
      { "namespace": "t24-core-prod", "name": "ACCOUNT", "facets": {} }
    ],
    "outputs": [
      { "namespace": "iceberg-warehouse", "name": "fact_stmt", "facets": {} }
    ],
    "producer": "airflow/2.9.1",
    "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json"
  }'
```

---

### Facets thường dùng

#### Schema facet — mô tả cấu trúc bảng

Đặt trong `inputs[].facets` hoặc `outputs[].facets`:

```json
"schema": {
  "_producer": "airflow/2.9.1",
  "_schemaURL": "https://openlineage.io/spec/facets/2-0-2/SchemaDatasetFacet.json",
  "fields": [
    { "name": "STMT_ID",   "type": "NUMBER" },
    { "name": "ACCT_NO",  "type": "VARCHAR2" },
    { "name": "AMOUNT",   "type": "NUMBER",  "description": "Transaction amount in VND" }
  ]
}
```

#### Column lineage facet — lineage cấp cột

Đặt trong `outputs[].facets` để ghi lại cột nào từ bảng nào tạo ra cột output:

```json
"columnLineage": {
  "_producer": "airflow/2.9.1",
  "_schemaURL": "https://openlineage.io/spec/facets/2-0-2/ColumnLineageDatasetFacet.json",
  "fields": {
    "total_balance": {
      "inputFields": [
        { "namespace": "t24-core-prod", "name": "ACCOUNT", "field": "CURR_BAL" },
        { "namespace": "t24-core-prod", "name": "ACCOUNT", "field": "AVAIL_BAL" }
      ]
    }
  }
}
```

#### Error facet — khi eventType = FAIL

Đặt trong `run.facets`:

```json
"errorMessage": {
  "_producer": "airflow/2.9.1",
  "_schemaURL": "https://openlineage.io/spec/facets/2-0-2/ErrorMessageRunFacet.json",
  "message": "java.sql.SQLException: ORA-01403: no data found",
  "programmingLanguage": "JAVA",
  "stackTrace": "..."
}
```

---

### Vòng đời một run (START → COMPLETE)

Một job chạy thường emit 2 event:

```
START event    → tạo job_run với status=started
COMPLETE event → cập nhật job_run status=completed, ghi lineage edges
```

Cả hai event phải dùng cùng `run.runId`. Nếu chỉ emit một event `COMPLETE`, vẫn hoạt động (job_run được tạo với status=completed ngay).

```json
// Event 1 — khi job bắt đầu
{ "eventType": "START", "run": { "runId": "abc-123" }, ... }

// Event 2 — khi job kết thúc
{ "eventType": "COMPLETE", "run": { "runId": "abc-123" }, ... }
```

---

## Connection Registry

### `GET /api/v1/connections`

List tất cả connections (không bao gồm đã soft-delete).

**Query params**

| Param | Default | Mô tả |
|---|---|---|
| `limit` | 100 | Số records tối đa |
| `offset` | 0 | Phân trang |

**Response 200** — array of [ConnectionOut](#connectionout-object)

---

### `GET /api/v1/connections/{logical_name}`

Lấy thông tin một connection theo `logical_name`.

**Response 200** — [ConnectionOut](#connectionout-object)  
**Response 404** — connection không tồn tại hoặc đã bị xóa

---

### `POST /api/v1/connections`

Đăng ký một connection mới. Nếu connection đã tồn tại dưới dạng stub (auto-created bởi lineage ingestion), endpoint này sẽ enrich stub đó thay vì tạo mới.

**Request body**

| Field | Type | Bắt buộc | Mô tả |
|---|---|---|---|
| `logical_name` | string | ✓ | Chỉ `[a-z0-9._-]`, không uppercase, không space. Bất biến sau khi tạo. |
| `platform` | string | ✓ | `oracle` \| `postgresql` \| `kafka` \| `iceberg` \| `trino` \| ... |
| `host` | string | — | Hostname / IP. Với multi-broker (Kafka) để null, dùng `properties` |
| `port` | integer | — | Port số |
| `service_name` | string | — | Oracle service name / SID |
| `vault_path` | string | — | Path trong Vault KV v2, VD: `database/prod/t24` |
| `classification` | string | — | `public` \| `internal` \| `confidential` |
| `owner_team` | string | — | Team chịu trách nhiệm |
| `description` | string | — | Mô tả tự do |
| `properties` | object | — | Metadata mở rộng, bắt buộc với một số platform (xem bên dưới) |

**`properties` bắt buộc theo platform**

| Platform | Bắt buộc | Tuỳ chọn |
|---|---|---|
| `kafka` | `bootstrap_servers` | `security_protocol`, `sasl_mechanism`, `schema_registry_url`, `replication_factor` |
| `oracle` | `service_name` | `sid` |
| `trino` | `catalog` | `schema` |
| `iceberg` | `warehouse` | `catalog_type` |
| Khác | — | Tự do |

**Response 201** — [ConnectionOut](#connectionout-object)  
**Response 409** — `logical_name` đã tồn tại (và không phải stub)

---

**Ví dụ — Oracle T24**

```bash
curl -X POST http://localhost:8080/api/v1/connections \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer dev-token-change-me" \
  -d '{
    "logical_name": "t24-core-prod",
    "platform": "oracle",
    "host": "oracle-t24.internal",
    "port": 1521,
    "vault_path": "database/prod/t24",
    "classification": "confidential",
    "owner_team": "core-banking",
    "description": "T24 Core Banking production schema",
    "properties": {
      "service_name": "T24PROD"
    }
  }'
```

**Ví dụ — Kafka multi-broker**

```bash
curl -X POST http://localhost:8080/api/v1/connections \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer dev-token-change-me" \
  -d '{
    "logical_name": "kafka-mbbank-prod",
    "platform": "kafka",
    "vault_path": "kafka/prod/sasl",
    "classification": "internal",
    "owner_team": "platform-engineering",
    "properties": {
      "bootstrap_servers": "kafka-1.internal:9092,kafka-2.internal:9092,kafka-3.internal:9092",
      "security_protocol": "SASL_SSL",
      "sasl_mechanism": "GSSAPI",
      "schema_registry_url": "https://schema-registry.internal:8081"
    }
  }'
```

**Ví dụ — Iceberg / S3**

```bash
curl -X POST http://localhost:8080/api/v1/connections \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer dev-token-change-me" \
  -d '{
    "logical_name": "iceberg-warehouse",
    "platform": "iceberg",
    "classification": "internal",
    "owner_team": "data-platform",
    "properties": {
      "warehouse": "s3://mbbank-datalake/warehouse",
      "catalog_type": "glue"
    }
  }'
```

---

### `PUT /api/v1/connections/{logical_name}`

Cập nhật thông tin vật lý của connection (FR-3). `logical_name` không thể thay đổi.

**Request body** — các field của [ConnectionCreate](#post-apiv1connections) ngoại trừ `logical_name`, tất cả đều tuỳ chọn.

> Lưu ý: Nếu truyền cả `platform` lẫn `properties`, validator sẽ kiểm tra `properties` theo platform mới.

**Response 200** — [ConnectionOut](#connectionout-object)  
**Response 404** — connection không tồn tại

**Ví dụ — đổi host sau khi migrate T24**

```bash
curl -X PUT http://localhost:8080/api/v1/connections/t24-core-prod \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer dev-token-change-me" \
  -d '{
    "host": "oracle-t24-new.internal",
    "port": 1521
  }'
```

---

### `DELETE /api/v1/connections/{logical_name}`

Soft-delete connection (đặt `deleted_at`). Lineage history vẫn còn, chỉ ẩn khỏi listing.

**Response 204** — thành công, không có body  
**Response 404** — connection không tồn tại

---

## Search

### `GET /api/v1/search/connections/{logical_name}/jobs`

**FR-1**: Tìm tất cả jobs có đọc hoặc ghi vào bất kỳ dataset nào của connection này.

**Response 200**
```json
{
  "connection": "t24-core-prod",
  "summary": {
    "logical_name": "t24-core-prod",
    "affected_datasets": 3,
    "affected_jobs": 2
  },
  "jobs": [
    {
      "id": "uuid",
      "namespace": "mbbank.dwh",
      "name": "etl.t24.daily_stmt_load",
      "job_type": "airflow_task",
      "source_repo": null,
      "owner_team": null,
      "role": "reader",
      "dataset_count": 2,
      "last_seen_at": "2026-05-20T08:30:00+00:00"
    }
  ]
}
```

`role`: `reader` (chỉ đọc) | `writer` (chỉ ghi) | `both` (đọc và ghi)

---

### `GET /api/v1/search/connections/{logical_name}/related`

Tìm các connections khác mà cùng ít nhất một job với connection này. Hữu ích để đánh giá impact khi thay đổi.

**Response 200**
```json
{
  "connection": "t24-core-prod",
  "related": [
    {
      "logical_name": "iceberg-warehouse",
      "platform": "iceberg",
      "classification": "internal",
      "bridging_job_count": 2
    }
  ]
}
```

---

### `GET /api/v1/search/datasets/{fqn}/jobs`

**FR-2**: Tìm tất cả jobs đọc từ hoặc ghi vào dataset theo FQN.

FQN format: `<logical_name>.<dataset_name>`, VD: `iceberg-warehouse.fact_stmt`

> **Lưu ý:** FQN có thể chứa `.` nên URL phải encode đúng hoặc dùng path param trực tiếp.

```bash
curl "http://localhost:8080/api/v1/search/datasets/iceberg-warehouse.fact_stmt/jobs" \
  -H "Authorization: Bearer dev-token-change-me"
```

**Response 200**
```json
{
  "dataset_fqn": "iceberg-warehouse.fact_stmt",
  "readers": [
    {
      "id": "uuid",
      "namespace": "mbbank.dwh",
      "name": "etl.dwh.aggregate_balance",
      "job_type": "airflow_task",
      "role": "reader",
      "last_seen_at": "2026-05-20T09:00:00+00:00"
    }
  ],
  "writers": [
    {
      "id": "uuid",
      "namespace": "mbbank.dwh",
      "name": "etl.t24.daily_stmt_load",
      "job_type": "airflow_task",
      "role": "writer",
      "last_seen_at": "2026-05-20T08:30:00+00:00"
    }
  ],
  "all_jobs": [ ... ]
}
```

---

### `GET /api/v1/search/datasets/{fqn}/upstream`

Duyệt đồ thị lineage ngược chiều (tìm nguồn dữ liệu của dataset).

**Query params**

| Param | Default | Max | Mô tả |
|---|---|---|---|
| `depth` | 3 | 20 | Số hop tối đa |

```bash
curl "http://localhost:8080/api/v1/search/datasets/iceberg-warehouse.fact_stmt/upstream?depth=5" \
  -H "Authorization: Bearer dev-token-change-me"
```

**Response 200**
```json
{
  "root_fqn": "iceberg-warehouse.fact_stmt",
  "direction": "upstream",
  "max_depth": 5,
  "nodes": [
    {
      "dataset_id": "uuid",
      "dataset_fqn": "t24-core-prod.STMT",
      "depth": 1,
      "via_job_id": "uuid",
      "via_job_name": "mbbank.dwh.etl.t24.daily_stmt_load"
    },
    {
      "dataset_id": "uuid",
      "dataset_fqn": "t24-core-prod.ACCOUNT",
      "depth": 1,
      "via_job_id": "uuid",
      "via_job_name": "mbbank.dwh.etl.t24.daily_stmt_load"
    }
  ]
}
```

---

### `GET /api/v1/search/datasets/{fqn}/downstream`

Duyệt đồ thị lineage xuôi chiều (tìm các dataset phụ thuộc vào dataset này).

Cùng params và response format với `/upstream`, `direction` = `"downstream"`.

```bash
curl "http://localhost:8080/api/v1/search/datasets/t24-core-prod.STMT/downstream?depth=3" \
  -H "Authorization: Bearer dev-token-change-me"
```

---

### `GET /api/v1/search/connections`

Tìm kiếm connections theo nhiều tiêu chí. Ít nhất một query param phải được truyền.

**Query params**

| Param | Mô tả |
|---|---|
| `q` | Fuzzy match trên `logical_name`, `host`, `description` |
| `host` | Partial match trên IP/hostname (ILIKE) |
| `platform` | Exact match: `oracle`, `kafka`, `iceberg`, `trino`, `postgresql`, ... |
| `classification` | `public` \| `internal` \| `confidential` |
| `owner_team` | Partial match trên tên team |
| `limit` | Mặc định 50, tối đa 200 |

**Ví dụ — tìm connection theo IP**
```bash
curl "http://localhost:8080/api/v1/search/connections?host=192.168.1" \
  -H "Authorization: Bearer dev-token-change-me"
```

**Ví dụ — tìm tất cả Kafka connections**
```bash
curl "http://localhost:8080/api/v1/search/connections?platform=kafka" \
  -H "Authorization: Bearer dev-token-change-me"
```

**Ví dụ — tìm theo tên + classification**
```bash
curl "http://localhost:8080/api/v1/search/connections?q=t24&classification=confidential" \
  -H "Authorization: Bearer dev-token-change-me"
```

**Response 200** — array
```json
[
  {
    "id": "uuid",
    "logical_name": "t24-core-prod",
    "platform": "oracle",
    "host": "oracle-t24.internal",
    "port": 1521,
    "classification": "confidential",
    "owner_team": "core-banking",
    "description": "T24 Core Banking production schema",
    "score": 0.8
  }
]
```

`score`: trigram similarity score (0–1). Chỉ có ý nghĩa khi dùng `q`. Kết quả được sort theo `score DESC`.

**Response 400** — khi không có param nào:
```json
{ "detail": "At least one search parameter is required: q, host, platform, classification, owner_team" }
```

---

### `GET /api/v1/search/datasets`

Tìm kiếm dataset theo tên bảng, connection, loại, hoặc classification. Ít nhất một query param phải được truyền.

**Query params**

| Param | Mô tả |
|---|---|
| `q` | Fuzzy match trên `fqn` và `name` |
| `connection` | Exact match trên `connection.logical_name` — lấy tất cả bảng của một connection |
| `dataset_type` | `table` \| `view` \| `topic` \| `file` \| `iceberg_table` \| `materialized_view` |
| `classification` | `public` \| `internal` \| `confidential` |
| `limit` | Mặc định 50, tối đa 200 |

**Ví dụ — tìm bảng theo tên**
```bash
curl "http://localhost:8080/api/v1/search/datasets?q=stmt" \
  -H "Authorization: Bearer dev-token-change-me"
```

**Ví dụ — lấy tất cả bảng của một connection**
```bash
curl "http://localhost:8080/api/v1/search/datasets?connection=t24-core-prod" \
  -H "Authorization: Bearer dev-token-change-me"
```

**Ví dụ — tìm tất cả Kafka topics**
```bash
curl "http://localhost:8080/api/v1/search/datasets?dataset_type=topic" \
  -H "Authorization: Bearer dev-token-change-me"
```

**Ví dụ — kết hợp nhiều filter**
```bash
curl "http://localhost:8080/api/v1/search/datasets?q=fact&connection=iceberg-warehouse&dataset_type=iceberg_table" \
  -H "Authorization: Bearer dev-token-change-me"
```

**Response 200** — array
```json
[
  {
    "fqn": "t24-core-prod.STMT",
    "name": "STMT",
    "dataset_type": "table",
    "classification": "confidential",
    "connection": "t24-core-prod",
    "platform": "oracle",
    "score": 0.6
  },
  {
    "fqn": "iceberg-warehouse.fact_stmt",
    "name": "fact_stmt",
    "dataset_type": "iceberg_table",
    "classification": "internal",
    "connection": "iceberg-warehouse",
    "platform": "iceberg",
    "score": 0.5
  }
]
```

---

## Shared Objects

### ConnectionOut object

```json
{
  "id": "uuid",
  "logical_name": "t24-core-prod",
  "platform": "oracle",
  "host": "oracle-t24.internal",
  "port": 1521,
  "service_name": null,
  "vault_path": "database/prod/t24",
  "classification": "confidential",
  "owner_team": "core-banking",
  "description": "T24 Core Banking production schema",
  "properties": { "service_name": "T24PROD" },
  "created_at": "2026-05-20T00:00:00+00:00",
  "updated_at": "2026-05-20T00:00:00+00:00"
}
```

---

## Quy ước quan trọng

### `job.namespace` = `connection.logical_name`

Khi producer emit OpenLineage event, `job.namespace` trong event phải trùng với `logical_name` của Connection đã đăng ký. Đây là cách hệ thống biết job thuộc về connection nào.

```
job.namespace = "mbbank.dwh"
→ Connection với logical_name = "mbbank.dwh" phải tồn tại (hoặc sẽ được tạo stub tự động)
```

### Dataset FQN

```
<connection.logical_name>.<dataset_name>
→ "t24-core-prod.STMT"
→ "iceberg-warehouse.fact_stmt"
→ "kafka-mbbank-prod.txn-events"
```

### Stub connection

Nếu lineage event tham chiếu đến một `namespace` chưa được đăng ký, hệ thống tự tạo stub connection với `platform = "unknown"`. Admin cần enrich stub đó sau bằng `POST /api/v1/connections` (endpoint sẽ upgrade stub thay vì báo lỗi 409).

### HTTP status codes

| Code | Ý nghĩa |
|---|---|
| 200 | Đọc thành công |
| 201 | Tạo mới thành công |
| 202 | Lineage event được nhận (xử lý async) |
| 204 | Xóa thành công |
| 400 | Request body không hợp lệ |
| 401 | Token sai hoặc thiếu |
| 404 | Resource không tồn tại |
| 409 | Conflict (logical_name đã tồn tại) |
| 422 | Validation error (Pydantic) |
| 500 | Lỗi server / database |
