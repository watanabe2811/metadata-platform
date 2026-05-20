"""Unit tests for Pydantic schema validation — no DB required."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from metadata_collector.schemas import (
    ConnectionCreate,
    ConnectionUpdate,
    OpenLineageRunEvent,
)


# ---------------------------------------------------------------------------
# ConnectionCreate — logical_name validation
# ---------------------------------------------------------------------------

class TestConnectionCreateName:
    def test_valid_simple(self):
        c = ConnectionCreate(logical_name="t24-core-prod", platform="postgresql")
        assert c.logical_name == "t24-core-prod"

    def test_valid_with_dots(self):
        c = ConnectionCreate(logical_name="mbbank.dwh.prod", platform="postgresql")
        assert c.logical_name == "mbbank.dwh.prod"

    def test_valid_numbers(self):
        ConnectionCreate(logical_name="kafka01", platform="kafka",
                         properties={"bootstrap_servers": "b:9092"})

    @pytest.mark.parametrize("bad_name", [
        "T24-Core",        # uppercase
        "t24 core",        # space
        "-starts-dash",    # starts with dash
        "",                # empty
        "a" * 129,         # too long
    ])
    def test_invalid_names(self, bad_name):
        with pytest.raises(ValidationError):
            ConnectionCreate(logical_name=bad_name, platform="oracle")


# ---------------------------------------------------------------------------
# Platform-specific properties validation
# ---------------------------------------------------------------------------

class TestPlatformProperties:
    def test_kafka_valid(self):
        c = ConnectionCreate(
            logical_name="kafka-prod",
            platform="kafka",
            properties={"bootstrap_servers": "b1:9092,b2:9092"},
        )
        assert c.properties["bootstrap_servers"] == "b1:9092,b2:9092"

    def test_kafka_missing_bootstrap_servers(self):
        with pytest.raises(ValidationError, match="bootstrap_servers"):
            ConnectionCreate(logical_name="kafka-prod", platform="kafka", properties={})

    def test_oracle_valid(self):
        ConnectionCreate(
            logical_name="ora-prod",
            platform="oracle",
            properties={"service_name": "ORCLPDB"},
        )

    def test_oracle_missing_service_name(self):
        with pytest.raises(ValidationError, match="service_name"):
            ConnectionCreate(logical_name="ora-prod", platform="oracle", properties={})

    def test_trino_valid(self):
        ConnectionCreate(
            logical_name="trino-prod",
            platform="trino",
            properties={"catalog": "hive"},
        )

    def test_trino_missing_catalog(self):
        with pytest.raises(ValidationError, match="catalog"):
            ConnectionCreate(logical_name="trino-prod", platform="trino", properties={})

    def test_iceberg_valid(self):
        ConnectionCreate(
            logical_name="iceberg-wh",
            platform="iceberg",
            properties={"warehouse": "s3://bucket/wh"},
        )

    def test_iceberg_missing_warehouse(self):
        with pytest.raises(ValidationError, match="warehouse"):
            ConnectionCreate(logical_name="iceberg-wh", platform="iceberg", properties={})

    def test_unknown_platform_allows_any_properties(self):
        c = ConnectionCreate(
            logical_name="custom-db",
            platform="doris",
            properties={"any_key": "any_value"},
        )
        assert c.properties["any_key"] == "any_value"

    def test_empty_properties_allowed_for_unknown_platform(self):
        ConnectionCreate(logical_name="my-db", platform="postgresql")


# ---------------------------------------------------------------------------
# ConnectionUpdate — partial update, conditional platform validation
# ---------------------------------------------------------------------------

class TestConnectionUpdate:
    def test_all_none_is_valid(self):
        u = ConnectionUpdate()
        assert u.platform is None

    def test_platform_with_valid_properties(self):
        u = ConnectionUpdate(
            platform="kafka",
            properties={"bootstrap_servers": "b:9092"},
        )
        assert u.platform == "kafka"

    def test_platform_with_invalid_properties(self):
        with pytest.raises(ValidationError):
            ConnectionUpdate(platform="kafka", properties={})

    def test_properties_without_platform_skips_validation(self):
        # Cannot validate properties without knowing the platform
        u = ConnectionUpdate(properties={"bootstrap_servers": "b:9092"})
        assert u.properties is not None

    def test_host_update_only(self):
        u = ConnectionUpdate(host="new-host.local", port=5432)
        assert u.host == "new-host.local"


# ---------------------------------------------------------------------------
# OpenLineageRunEvent parsing
# ---------------------------------------------------------------------------

class TestOpenLineageRunEvent:
    def _base_event(self, **overrides) -> dict:
        event = {
            "eventType": "COMPLETE",
            "eventTime": "2026-05-20T08:00:00+07:00",
            "run": {"runId": "550e8400-e29b-41d4-a716-446655440000", "facets": {}},
            "job": {"namespace": "mbbank.dwh", "name": "etl.job", "facets": {}},
            "inputs": [],
            "outputs": [],
            "producer": "airflow/2.9",
            "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
        }
        event.update(overrides)
        return event

    def test_valid_complete_event(self):
        e = OpenLineageRunEvent(**self._base_event())
        assert e.eventType == "COMPLETE"

    def test_valid_start_event(self):
        e = OpenLineageRunEvent(**self._base_event(eventType="START"))
        assert e.eventType == "START"

    def test_invalid_event_type(self):
        with pytest.raises(ValidationError):
            OpenLineageRunEvent(**self._base_event(eventType="UNKNOWN"))

    def test_inputs_outputs_parsed(self):
        e = OpenLineageRunEvent(**self._base_event(
            inputs=[{"namespace": "t24-core-prod", "name": "STMT", "facets": {}}],
            outputs=[{"namespace": "iceberg-wh", "name": "fact_stmt", "facets": {}}],
        ))
        assert len(e.inputs) == 1
        assert e.inputs[0].namespace == "t24-core-prod"
        assert len(e.outputs) == 1

    def test_extra_fields_preserved(self):
        e = OpenLineageRunEvent(**self._base_event(customField="value"))
        assert e.model_extra["customField"] == "value"

    def test_missing_producer_raises(self):
        ev = self._base_event()
        del ev["producer"]
        with pytest.raises(ValidationError):
            OpenLineageRunEvent(**ev)
