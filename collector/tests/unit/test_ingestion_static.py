"""Unit tests for LineageIngestionService static methods — no DB required."""
from __future__ import annotations

import pytest

from metadata_collector.services.ingestion_service import LineageIngestionService


class TestInferJobType:
    def _infer(self, producer: str) -> str:
        return LineageIngestionService._infer_job_type({"producer": producer})

    def test_spark(self):
        assert self._infer("openlineage-spark/1.0") == "spark"

    def test_airflow(self):
        assert self._infer("apache-airflow/2.9.1") == "airflow_task"

    def test_flink(self):
        assert self._infer("openlineage-flink/1.0") == "flink"

    def test_fastapi(self):
        assert self._infer("fastapi-producer/1.0") == "fastapi"

    def test_trino(self):
        assert self._infer("trino/432") == "trino_query"

    def test_python(self):
        assert self._infer("python-etl/1.0") == "python"

    def test_unknown(self):
        assert self._infer("custom-tool/1.0") == "unknown"

    def test_empty_producer(self):
        assert self._infer("") == "unknown"

    def test_case_insensitive(self):
        assert self._infer("Apache-Airflow/2.9") == "airflow_task"


class TestInferDatasetType:
    def _infer(self, namespace: str, name: str = "tbl", facets: dict | None = None) -> str:
        return LineageIngestionService._infer_dataset_type(namespace, name, facets or {})

    def test_kafka_namespace(self):
        assert self._infer("kafka-prod") == "topic"

    def test_iceberg_namespace(self):
        assert self._infer("iceberg-warehouse") == "iceberg_table"

    def test_iceberg_facet(self):
        assert self._infer(
            "my-store", facets={"storage": {"storageLayer": "iceberg"}}
        ) == "iceberg_table"

    def test_s3_namespace(self):
        assert self._infer("s3://mybucket") == "file"

    def test_file_namespace(self):
        assert self._infer("file:///data/path") == "file"

    def test_default_table(self):
        assert self._infer("t24-core-prod") == "table"


class TestExtractColumnLineage:
    def test_returns_none_when_no_facet(self):
        result = LineageIngestionService._extract_column_lineage({"facets": {}})
        assert result is None

    def test_returns_none_when_no_fields_key(self):
        result = LineageIngestionService._extract_column_lineage(
            {"facets": {"columnLineage": {}}}
        )
        assert result is None

    def test_extracts_column_mapping(self):
        ol_dataset = {
            "facets": {
                "columnLineage": {
                    "fields": {
                        "balance_amt": {
                            "inputFields": [
                                {"namespace": "t24", "name": "STMT", "field": "BAL_AMT"}
                            ]
                        }
                    }
                }
            }
        }
        result = LineageIngestionService._extract_column_lineage(ol_dataset)
        assert result is not None
        assert len(result) == 1
        assert result[0]["target"] == "balance_amt"
        assert result[0]["sources"][0]["field"] == "BAL_AMT"

    def test_multiple_columns(self):
        ol_dataset = {
            "facets": {
                "columnLineage": {
                    "fields": {
                        "col_a": {"inputFields": [{"namespace": "ns", "name": "t", "field": "a"}]},
                        "col_b": {"inputFields": [{"namespace": "ns", "name": "t", "field": "b"}]},
                    }
                }
            }
        }
        result = LineageIngestionService._extract_column_lineage(ol_dataset)
        assert result is not None
        targets = {r["target"] for r in result}
        assert targets == {"col_a", "col_b"}
