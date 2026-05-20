# Integration Guide — Metadata Collector

Hướng dẫn tích hợp các hệ thống producer vào metadata collector.

---

## Tổng quan

Tất cả producer đều tích hợp qua một endpoint duy nhất:

```
POST /api/v1/lineage
```

Payload là một **OpenLineage RunEvent** chuẩn. Không có SDK riêng — bất kỳ HTTP client nào cũng dùng được.

Luồng tích hợp gồm 2 bước:

```
1. Đăng ký connections (một lần, do admin)
   POST /api/v1/connections

2. Emit lineage events (mỗi lần job chạy, do producer)
   POST /api/v1/lineage
```

---

## Bước 1 — Đăng ký connections

Trước khi bất kỳ job nào emit event, các connection phải được đăng ký. Nếu bỏ qua bước này, collector vẫn tự tạo stub connection, nhưng thiếu thông tin (host, vault_path, classification).

```bash
curl -X POST http://metadata:8080/api/v1/connections \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $METADATA_TOKEN" \
  -d '{
    "logical_name": "t24-core-prod",
    "platform": "oracle",
    "host": "oracle-t24.bank.local",
    "port": 1521,
    "vault_path": "kv/bank/db/t24-core-prod",
    "classification": "confidential",
    "owner_team": "core-banking",
    "properties": { "service_name": "T24PROD" }
  }'
```

Xem [API.md](API.md) để biết đầy đủ các field và ví dụ cho từng platform.

---

## Bước 2 — Emit lineage events

### 2.1 Python thuần (không dùng bank-conn)

Phù hợp cho script đơn giản, không cần resolve credentials.

```python
import httpx
from datetime import datetime, timezone
from uuid import uuid4

COLLECTOR_URL = "http://metadata:8080"
TOKEN = "your-token"

def emit_lineage(job_name, inputs, outputs, event_type="COMPLETE"):
    """
    inputs/outputs: list of (namespace, table_name)
    namespace phải trùng với logical_name của Connection đã đăng ký.
    """
    event = {
        "eventType": event_type,
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "run": {"runId": str(uuid4()), "facets": {}},
        "job": {
            "namespace": "mbbank.dwh",
            "name": job_name,
            "facets": {},
        },
        "inputs":  [{"namespace": ns, "name": tbl, "facets": {}} for ns, tbl in inputs],
        "outputs": [{"namespace": ns, "name": tbl, "facets": {}} for ns, tbl in outputs],
        "producer": "python-etl/1.0",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
    }
    resp = httpx.post(
        f"{COLLECTOR_URL}/api/v1/lineage",
        json=event,
        headers={"Authorization": f"Bearer {TOKEN}"},
        timeout=10,
    )
    resp.raise_for_status()


# Sử dụng
emit_lineage(
    job_name="etl.report.daily_balance",
    inputs=[
        ("t24-core-prod", "ACCOUNT"),
        ("t24-core-prod", "STMT"),
    ],
    outputs=[
        ("doris-serving", "dm_balance_daily"),
    ],
)
```

### 2.2 bank-conn (Python với credential resolve + auto lineage)

`bank-conn` là thư viện nội bộ giải quyết 3 việc cùng lúc:
- Resolve logical name → host/port/credentials (qua collector API + Vault)
- Build SQLAlchemy engine / JDBC options
- Tự động emit START/COMPLETE/FAIL event

**Cài đặt:**
```bash
pip install -e /path/to/metadata-platform/bank-conn
```

**Cấu hình môi trường:**
```bash
export BANK_CONN_COLLECTOR_URL=http://metadata:8080
export BANK_CONN_COLLECTOR_TOKEN=your-token
export VAULT_ADDR=https://vault.bank.local
export VAULT_TOKEN=your-vault-token
export BANK_CONN_JOB_NAMESPACE=mbbank.dwh
```

**Pattern cơ bản:**
```python
from bank_conn import Connection

with Connection("t24-core-prod", job_name="etl.t24.daily_stmt_load") as conn:
    conn.record_read("STMT")
    conn.record_read("ACCOUNT")

    engine = conn.sqlalchemy_engine()
    df = pd.read_sql("SELECT * FROM STMT WHERE TRUNC(STMT_DATE) = TRUNC(SYSDATE)", engine)

    conn.record_write("fact_stmt_daily", connection="iceberg-warehouse")
    # ... ghi vào Iceberg ...

# Khi thoát khỏi `with`: tự động emit COMPLETE event
# Nếu exception: tự động emit FAIL event
```

**Với column-level lineage:**
```python
from bank_conn import Connection

with Connection("t24-core-prod", job_name="etl.t24.enrich_stmt") as conn:
    conn.record_read("STMT")
    conn.record_read("CUSTOMER")

    conn.record_write(
        "fact_stmt_enriched",
        connection="iceberg-warehouse",
        column_lineage={
            "balance_amt": [
                {"namespace": "t24-core-prod", "name": "STMT", "field": "BAL_AMT"},
            ],
            "customer_name": [
                {"namespace": "t24-core-prod", "name": "CUSTOMER", "field": "FIRST_NM"},
                {"namespace": "t24-core-prod", "name": "CUSTOMER", "field": "LAST_NM"},
            ],
        },
    )
```

**JDBC options cho Spark:**
```python
from bank_conn import Connection

conn = Connection("t24-core-prod", job_name="spark.t24.batch_load")
jdbc = conn.jdbc_options()

df = (
    spark.read.format("jdbc")
    .option("url",      jdbc["url"])
    .option("dbtable",  "T24PROD.STMT")
    .option("user",     jdbc["user"])
    .option("password", jdbc["password"])
    .option("driver",   jdbc["driver"])
    .load()
)
```

### 2.3 Apache Airflow

Dùng `OpenLineageSparkIntegration` hoặc emit thủ công từ PythonOperator.

**Option A — PythonOperator emit thủ công:**

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from bank_conn import Connection

def load_stmt(**context):
    run_id = context["run_id"]   # Airflow run_id

    with Connection(
        "t24-core-prod",
        job_namespace="mbbank.dwh",
        job_name="dag.t24.daily_stmt_load",
        run_id=run_id,
    ) as conn:
        conn.record_read("STMT")
        conn.record_read("ACCOUNT")
        conn.record_write("fact_stmt", connection="iceberg-warehouse")
        # ... logic ETL ...

with DAG("t24_daily_stmt_load", start_date=datetime(2026, 1, 1), schedule="0 2 * * *") as dag:
    PythonOperator(task_id="load_stmt", python_callable=load_stmt)
```

**Option B — OpenLineage Airflow provider (tự động, không cần code):**

Cài provider và trỏ transport về collector:

```bash
pip install apache-airflow-providers-openlineage
```

```ini
# airflow.cfg
[openlineage]
transport = {"type": "http", "url": "http://metadata:8080", "endpoint": "api/v1/lineage", "auth": {"type": "api_key", "apiKey": "your-token"}}
namespace = mbbank.dwh
```

Sau khi cấu hình, mọi DAG Airflow sẽ tự động emit lineage mà không cần thêm code.

### 2.4 Apache Spark

**Option A — OpenLineage Spark plugin (khuyến nghị):**

Thêm vào Spark config:

```properties
spark.jars.packages          io.openlineage:openlineage-spark_2.12:1.18.0
spark.extraListeners         io.openlineage.spark.agent.OpenLineageSparkListener
spark.openlineage.transport.type     http
spark.openlineage.transport.url      http://metadata:8080
spark.openlineage.transport.endpoint /api/v1/lineage
spark.openlineage.transport.auth.type api_key
spark.openlineage.transport.auth.apiKey your-token
spark.openlineage.namespace  mbbank.dwh
```

Không cần thay đổi code Spark — plugin tự theo dõi DataFrame read/write.

**Option B — Emit thủ công từ Spark driver:**

```python
from pyspark.sql import SparkSession
from bank_conn import Connection, configure
from bank_conn.emitter import LineageEmitter, make_run_id

configure(collector_url="http://metadata:8080")

spark = SparkSession.builder.appName("spark.t24.daily_load").getOrCreate()
run_id = make_run_id()
emitter = LineageEmitter()

emitter.emit_start(
    run_id=run_id,
    job_namespace="mbbank.dwh",
    job_name="spark.t24.daily_load",
    inputs=[{"namespace": "t24-core-prod", "name": "STMT", "facets": {}}],
    outputs=[{"namespace": "iceberg-warehouse", "name": "fact_stmt", "facets": {}}],
)
try:
    df = spark.read.format("jdbc") \
        .option("url", "jdbc:oracle:thin:@//oracle-t24.bank.local:1521/T24PROD") \
        .option("dbtable", "T24PROD.STMT") \
        .load()
    df.write.format("iceberg").mode("append").save("warehouse.fact_stmt")
    emitter.emit_complete(
        run_id=run_id,
        job_namespace="mbbank.dwh",
        job_name="spark.t24.daily_load",
        inputs=[{"namespace": "t24-core-prod", "name": "STMT", "facets": {}}],
        outputs=[{"namespace": "iceberg-warehouse", "name": "fact_stmt", "facets": {}}],
    )
except Exception as e:
    emitter.emit_fail(
        run_id=run_id,
        job_namespace="mbbank.dwh",
        job_name="spark.t24.daily_load",
        inputs=[],
        outputs=[],
        error=str(e),
    )
    raise
```

### 2.5 Flink

Dùng OpenLineage Flink connector:

```xml
<!-- pom.xml -->
<dependency>
  <groupId>io.openlineage</groupId>
  <artifactId>openlineage-flink</artifactId>
  <version>1.18.0</version>
</dependency>
```

```java
// openlineage.yml (classpath)
transport:
  type: http
  url: http://metadata:8080
  endpoint: /api/v1/lineage
  auth:
    type: api_key
    apiKey: "your-token"
namespace: mbbank.dwh
```

```java
OpenLineageFlinkJobListener listener = OpenLineageFlinkJobListener.builder()
    .executionEnvironment(env)
    .jobName("flink.cdc.t24_to_iceberg")
    .build();

env.registerJobListener(listener);
```

### 2.6 Trino / DBeaver (query tracking)

Với Trino, emit event ngay sau khi query chạy:

```python
import httpx
from datetime import datetime, timezone
from uuid import uuid4

def track_trino_query(query: str, source_table: str, target_table: str):
    event = {
        "eventType": "COMPLETE",
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "run": {"runId": str(uuid4()), "facets": {}},
        "job": {
            "namespace": "mbbank.trino",
            "name": f"trino.{source_table}_to_{target_table}",
            "facets": {},
        },
        "inputs":  [{"namespace": "iceberg-warehouse", "name": source_table, "facets": {}}],
        "outputs": [{"namespace": "doris-serving",     "name": target_table, "facets": {}}],
        "producer": "trino/432",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
    }
    httpx.post("http://metadata:8080/api/v1/lineage",
               json=event,
               headers={"Authorization": "Bearer your-token"})
```

---

## Quy ước bắt buộc

### `job.namespace` = `connection.logical_name`

Đây là quy ước quan trọng nhất. Khi producer dùng `job.namespace = "mbbank.dwh"`, collector hiểu rằng job này thuộc namespace `mbbank.dwh` — và phải có Connection với `logical_name = "mbbank.dwh"`.

| Namespace trong event | Ý nghĩa |
|---|---|
| `mbbank.dwh` | Job thuộc namespace DWH team |
| `t24-core-prod` | Dataset đọc từ connection T24 prod |
| `iceberg-warehouse` | Dataset ghi vào Iceberg warehouse |

### Dataset FQN

```
<connection.logical_name>.<dataset_name>
```

Ví dụ: `t24-core-prod.STMT`, `iceberg-warehouse.fact_stmt`

### Namespace format

Chỉ dùng `[a-z0-9._-]`. Không uppercase, không dấu cách.

---

## Kiểm tra tích hợp

Sau khi tích hợp, verify bằng API:

```bash
# FR-1: job nào đang đọc/ghi vào t24-core-prod?
curl http://metadata:8080/api/v1/search/connections/t24-core-prod/jobs \
  -H "Authorization: Bearer $TOKEN"

# FR-2: ai đang đọc/ghi vào bảng fact_stmt?
curl http://metadata:8080/api/v1/search/datasets/iceberg-warehouse.fact_stmt/jobs \
  -H "Authorization: Bearer $TOKEN"

# Downstream: nếu STMT thay đổi, những dataset nào bị ảnh hưởng?
curl "http://metadata:8080/api/v1/search/datasets/t24-core-prod.STMT/downstream?depth=5" \
  -H "Authorization: Bearer $TOKEN"
```

---

## Troubleshooting

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| HTTP 422 trên `POST /lineage` | `eventType` sai (phải UPPERCASE) hoặc thiếu `producer` | Kiểm tra payload theo [API.md](API.md) |
| Stub connection tự sinh (`platform=unknown`) | `namespace` trong event chưa được đăng ký | Chạy `POST /api/v1/connections` trước |
| Lineage edge không xuất hiện | Event `RUNNING` không tạo edge; chỉ `COMPLETE`/`FAIL` tạo | Dùng `eventType: COMPLETE` |
| `ConnectionResolutionError` trong bank-conn | Collector không reachable hoặc token sai | Kiểm tra `BANK_CONN_COLLECTOR_URL` và `BANK_CONN_COLLECTOR_TOKEN` |
| Column lineage không có | `columnLineage` facet đặt sai chỗ | Facet phải trong `outputs[].facets`, không phải `run.facets` |
