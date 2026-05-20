# Hướng dẫn sử dụng Skills — Metadata Collector

Skills là các slash commands chạy trong Claude Code, tự động hóa việc scan code và đẩy metadata lên collector API.

---

## Cài đặt

Skills có sẵn khi mở project `metadata-platform` trong Claude Code. Không cần cài thêm.

Kiểm tra:
```
/help
```
Sẽ thấy `scan-metadata`, `review-metadata`, `push-metadata` trong danh sách commands.

---

## Tổng quan 3 bước

```
/scan-metadata <project-dir>
        │
        ▼  .metadata_scan.json
/review-metadata
        │
        ▼  .metadata_scan.json (đã chỉnh sửa)
/push-metadata
        │
        ▼  connections + lineage events → collector API
```

Mỗi bước đọc/ghi file `.metadata_scan.json`. Có thể dừng giữa chừng và tiếp tục sau.

---

## `/scan-metadata` — Bước 1: Quét project

### Cú pháp

```
/scan-metadata <project-dir> [--namespace NS] [--out FILE]
```

| Argument | Default | Mô tả |
|---|---|---|
| `<project-dir>` | `.` (thư mục hiện tại) | Đường dẫn project cần scan |
| `--namespace NS` | tên thư mục project | OpenLineage namespace cho các job phát hiện được |
| `--out FILE` | `.metadata_scan.json` trong project-dir | File đầu ra |

### Ví dụ

```
# Scan project hiện tại
/scan-metadata

# Scan project ETL của team T24
/scan-metadata /opt/projects/t24-etl --namespace mbbank.t24

# Scan và lưu ra file khác
/scan-metadata ~/projects/dwh-jobs --out /tmp/dwh_metadata.json
```

### Scanner tìm gì

| Nguồn | Thông tin |
|---|---|
| `.env`, `.env.*` | `DATABASE_URL`, `DB_HOST`, `KAFKA_BROKERS`, `BOOTSTRAP_SERVERS` |
| `docker-compose.yml` | Service image → platform, env vars → host/port |
| Python source (`.py`) | Import `airflow/pyspark/flink/psycopg2/asyncpg` → job type + SQL tables |
| Airflow DAG files | `dag_id`, SQL trong tasks → input/output tables |

**Lưu ý:** Scanner là static analysis, không chạy code. Kết quả là best-effort — bước review (bước 2) để sửa sai sót.

### Output

```
Scan complete: /opt/projects/t24-etl
Namespace   : mbbank.t24

Connections found: 3
  - t24-core-prod  [oracle]  oracle-t24.bank.local  (from .env)
  - iceberg-warehouse  [iceberg]  null  (from docker-compose.yml)
  - kafka-cdc-prod  [kafka]  null  (from .env)

Jobs found: 5
  - etl_stmt_load  [airflow_task]  t24-core-prod.STMT → iceberg-warehouse.fact_stmt
  - daily_balance  [python]  t24-core-prod.ACCOUNT → doris-serving.dm_balance
  ...

Warnings: 1
  - Could not parse docker/compose.yml (install PyYAML for better coverage)

Output saved to: /opt/projects/t24-etl/.metadata_scan.json

Next step: /review-metadata /opt/projects/t24-etl/.metadata_scan.json
```

### Cài thêm PyYAML để scan docker-compose

Nếu thấy warning về docker-compose:
```bash
pip install pyyaml
```
Sau đó chạy lại `/scan-metadata`.

---

## `/review-metadata` — Bước 2: Kiểm tra & chỉnh sửa

### Cú pháp

```
/review-metadata [<scan-file>]
```

| Argument | Default | Mô tả |
|---|---|---|
| `<scan-file>` | `.metadata_scan.json` | File từ bước scan |

### Ví dụ

```
# Dùng file mặc định trong thư mục hiện tại
/review-metadata

# Chỉ định file cụ thể
/review-metadata /opt/projects/t24-etl/.metadata_scan.json
```

### Skill làm gì

**1. Validate tự động**

Skill kiểm tra và flag ⚠️ các vấn đề:

| Kiểm tra | Ví dụ lỗi |
|---|---|
| `platform = "unknown"` | Scanner không detect được platform từ URL |
| Required properties thiếu | Kafka connection thiếu `bootstrap_servers` |
| `logical_name` không hợp lệ | Chứa ký tự uppercase hoặc khoảng trắng |
| `job_type` không hợp lệ | Không thuộc `airflow_task / spark / flink / python / ...` |
| Namespace của job không match connection | Job dùng namespace `mbbank-t24` nhưng connection là `mbbank.t24` |

**2. Hỏi về những gì muốn giữ/bỏ**

Skill hỏi:
- Connection nào muốn **xóa khỏi danh sách push** (VD: connection của local dev không cần đăng ký)
- Job nào muốn **xóa khỏi danh sách push**
- Có muốn **sửa thông tin** nào không (VD: thêm `host`, đổi `platform`)

**3. Ví dụ sửa thông tin**

Khi skill hỏi "Do you want to edit any connection's details?" và bạn chọn "Yes":

```
Bạn mô tả: "connection iceberg-warehouse cần thêm properties warehouse = s3://mbbank-datalake/warehouse"

Skill áp dụng và hiển thị lại:
{
  "logical_name": "iceberg-warehouse",
  "platform": "iceberg",
  "properties": {
    "warehouse": "s3://mbbank-datalake/warehouse"
  }
}
Xác nhận? [Yes/No]
```

**4. Lưu lại file**

Sau khi confirm, skill ghi lại `.metadata_scan.json` với các thay đổi. File này là input cho bước 3.

---

## `/push-metadata` — Bước 3: Đẩy lên API

### Cú pháp

```
/push-metadata [<scan-file>] [--collector-url URL] [--token TOKEN] [--dry-run]
```

| Argument | Default | Mô tả |
|---|---|---|
| `<scan-file>` | `.metadata_scan.json` | File từ bước review |
| `--collector-url URL` | _(hỏi khi chạy)_ | Base URL của collector |
| `--token TOKEN` | _(hỏi khi chạy)_ | Bearer token |
| `--dry-run` | false | In payload, không gọi API thật |

### Ví dụ

```
# Hỏi URL và token khi chạy
/push-metadata

# Truyền sẵn
/push-metadata --collector-url http://metadata:8080 --token my-token

# Xem trước payload trước khi push thật
/push-metadata --dry-run

# Dùng file cụ thể và push lên prod
/push-metadata /tmp/dwh_metadata.json \
  --collector-url https://metadata.bank.internal \
  --token $PROD_TOKEN
```

### Skill làm gì

**1. Hỏi API credentials** (nếu chưa truyền qua flag)

Skill đưa ra các lựa chọn phổ biến:
- `http://localhost:8080` (local dev)
- `http://metadata:8080` (Docker internal)
- Custom URL

**2. Health check**

```bash
GET <collector-url>/health
→ {"status": "ok"}
```

Nếu collector không reachable, dừng và hướng dẫn:
```
Collector không reachable tại http://localhost:8080
→ Kiểm tra: docker compose up -d collector
→ Hoặc: uvicorn metadata_collector.main:app --port 8080
```

**3. Push connections** (tuần tự)

Mỗi connection trong file được push qua `POST /api/v1/connections`:

```
[OK]   t24-core-prod — created (HTTP 201)
[OK]   iceberg-warehouse — stub upgraded (HTTP 200)
[SKIP] kafka-cdc-prod — already registered (HTTP 409)
[FAIL] doris-serving — platform 'doris' missing properties (HTTP 422)
```

Lỗi 409 (đã tồn tại) **không phải lỗi** — skip và tiếp tục.

**4. Push lineage events** (tuần tự)

Mỗi job trong file được push qua `POST /api/v1/lineage`:

```
[OK]   etl.t24.daily_stmt_load (HTTP 202)
[OK]   etl.dwh.aggregate_balance (HTTP 202)
[FAIL] report.unknown_job — invalid namespace (HTTP 422)
```

**5. Báo cáo cuối**

```
=== push-metadata complete ===

Collector : http://metadata:8080
File      : .metadata_scan.json
Mode      : LIVE

Connections
  Created  : 3
  Upgraded : 1  (stub → full)
  Skipped  : 2  (already existed)
  Failed   : 1

Lineage events
  Accepted : 4
  Failed   : 1

Failed items:
  [connection] doris-serving: platform 'doris' properties missing
  [job]        report.unknown_job: invalid namespace format

Next steps:
  - Enrich stub connections at http://metadata:8080/docs
  - Fix failed items and run /push-metadata again
```

### Dry-run mode

Dry-run in toàn bộ payload mà không gọi API. Dùng để kiểm tra trước khi push production:

```
/push-metadata --dry-run

[DRY-RUN] POST /api/v1/connections
{
  "logical_name": "t24-core-prod",
  "platform": "oracle",
  "host": "oracle-t24.bank.local",
  ...
}

[DRY-RUN] POST /api/v1/lineage
{
  "eventType": "COMPLETE",
  "job": { "namespace": "mbbank.t24", "name": "etl_stmt_load" },
  ...
}

=== push-metadata complete (DRY-RUN — nothing was pushed) ===
```

---

## Workflow hoàn chỉnh — Ví dụ thực tế

### Onboard project ETL mới

```bash
# 1. Scan project
/scan-metadata /opt/projects/t24-etl --namespace mbbank.t24

# 2. Review: bỏ connection local dev, sửa properties kafka
/review-metadata /opt/projects/t24-etl/.metadata_scan.json

# 3. Xem trước rồi push
/push-metadata /opt/projects/t24-etl/.metadata_scan.json --dry-run
/push-metadata /opt/projects/t24-etl/.metadata_scan.json \
  --collector-url http://metadata:8080 \
  --token $METADATA_TOKEN
```

### Cập nhật sau khi thêm job mới

Không cần scan lại toàn bộ — chỉ emit lineage thủ công cho job mới qua `POST /api/v1/lineage`, hoặc chạy lại 3 bước (push sẽ skip các connection/job đã có).

### Push lên nhiều môi trường

```bash
# Dev
/push-metadata --collector-url http://localhost:8080 --token dev-token-change-me

# Staging
/push-metadata --collector-url http://metadata-staging:8080 --token $STAGING_TOKEN

# Prod (dry-run trước)
/push-metadata --collector-url https://metadata.bank.internal --token $PROD_TOKEN --dry-run
/push-metadata --collector-url https://metadata.bank.internal --token $PROD_TOKEN
```

---

## Troubleshooting

| Vấn đề | Giải pháp |
|---|---|
| "File not found: .metadata_scan.json" | Chạy `/scan-metadata` trước |
| Scanner không tìm được connection | Kiểm tra `.env` file có `DATABASE_URL` / `KAFKA_BROKERS` không |
| Kafka connection thiếu `bootstrap_servers` | Sửa ở bước review, hoặc thêm vào `.env`: `KAFKA_BOOTSTRAP_SERVERS=...` |
| HTTP 422 khi push | Platform-specific properties thiếu — xem [INTEGRATION.md](INTEGRATION.md) phần properties |
| Collector unreachable | `docker compose up -d` hoặc kiểm tra `--collector-url` |
| Job không có input/output | Scanner chỉ detect SQL table name trong code; Airflow DAG đơn giản có thể bị bỏ qua |

---

## File `.metadata_scan.json` — Format tham khảo

```json
{
  "project_dir": "/opt/projects/t24-etl",
  "namespace": "mbbank.t24",
  "warnings": [],
  "connections": [
    {
      "logical_name": "t24-core-prod",
      "platform": "oracle",
      "host": "oracle-t24.bank.local",
      "port": 1521,
      "vault_path": "kv/bank/db/t24",
      "classification": "confidential",
      "properties": { "service_name": "T24PROD" },
      "_source_file": "/opt/projects/t24-etl/.env"
    }
  ],
  "jobs": [
    {
      "namespace": "mbbank.t24",
      "name": "etl_stmt_load",
      "job_type": "airflow_task",
      "source_file": "/opt/projects/t24-etl/dags/stmt_load.py",
      "lineage_event": {
        "eventType": "COMPLETE",
        "eventTime": "2026-05-20T08:00:00+07:00",
        "run": { "runId": "uuid", "facets": {} },
        "job": { "namespace": "mbbank.t24", "name": "etl_stmt_load", "facets": {} },
        "inputs":  [{ "namespace": "t24-core-prod", "name": "stmt", "facets": {} }],
        "outputs": [{ "namespace": "iceberg-warehouse", "name": "fact_stmt", "facets": {} }],
        "producer": "metadata-scanner/1.0",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json"
      }
    }
  ]
}
```

Field `_source_file` chỉ dùng để hiển thị trong review — không được gửi lên API.
