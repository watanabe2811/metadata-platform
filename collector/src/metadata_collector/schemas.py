"""Pydantic models for API I/O.

Two families of models:
- OpenLineage event models (passthrough, JSON-faithful)
- Domain models (Connection, Dataset, Job, Lineage views)
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator


# --------------------------------------------------------------------
# OpenLineage models — minimal validation, JSON passthrough
# Spec: https://openlineage.io/apidocs/openapi/
# --------------------------------------------------------------------


class OpenLineageDataset(BaseModel):
    model_config = ConfigDict(extra="allow")
    namespace: str
    name: str
    facets: dict[str, Any] = Field(default_factory=dict)


class OpenLineageJob(BaseModel):
    model_config = ConfigDict(extra="allow")
    namespace: str
    name: str
    facets: dict[str, Any] = Field(default_factory=dict)


class OpenLineageRun(BaseModel):
    model_config = ConfigDict(extra="allow")
    runId: str
    facets: dict[str, Any] = Field(default_factory=dict)


class OpenLineageRunEvent(BaseModel):
    """Accept an OpenLineage RunEvent. Extra fields preserved."""
    model_config = ConfigDict(extra="allow")

    eventType: Literal["START", "RUNNING", "COMPLETE", "FAIL", "ABORT", "OTHER"]
    eventTime: str
    run: OpenLineageRun
    job: OpenLineageJob
    inputs: list[OpenLineageDataset] = Field(default_factory=list)
    outputs: list[OpenLineageDataset] = Field(default_factory=list)
    producer: str
    schemaURL: str


# --------------------------------------------------------------------
# Platform-specific properties models
# --------------------------------------------------------------------


class KafkaProperties(BaseModel):
    bootstrap_servers: str
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str | None = None
    schema_registry_url: str | None = None
    replication_factor: int | None = None


class OracleProperties(BaseModel):
    service_name: str
    sid: str | None = None


class TrinoProperties(BaseModel):
    catalog: str
    schema_: str | None = Field(None, alias="schema")


class IcebergProperties(BaseModel):
    warehouse: str
    catalog_type: str = "glue"


_PLATFORM_PROPERTIES: dict[str, type[BaseModel]] = {
    "kafka": KafkaProperties,
    "oracle": OracleProperties,
    "trino": TrinoProperties,
    "iceberg": IcebergProperties,
}


def _validate_platform_properties(platform: str, properties: dict[str, Any]) -> None:
    model_cls = _PLATFORM_PROPERTIES.get(platform)
    if model_cls is not None:
        model_cls(**properties)


# --------------------------------------------------------------------
# Domain models — Connection
# --------------------------------------------------------------------


Classification = Literal["public", "internal", "confidential"]


class ConnectionCreate(BaseModel):
    logical_name: str = Field(min_length=1, max_length=128, pattern=r"^[a-z0-9][a-z0-9._-]*$")
    platform: str
    host: str | None = None
    port: int | None = None
    service_name: str | None = None
    vault_path: str | None = None
    classification: Classification | None = None
    owner_team: str | None = None
    description: str | None = None
    properties: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_properties_for_platform(self) -> ConnectionCreate:
        _validate_platform_properties(self.platform, self.properties)
        return self


class ConnectionUpdate(BaseModel):
    platform: str | None = None
    host: str | None = None
    port: int | None = None
    service_name: str | None = None
    vault_path: str | None = None
    classification: Classification | None = None
    owner_team: str | None = None
    description: str | None = None
    properties: dict[str, Any] | None = None

    @model_validator(mode="after")
    def validate_properties_for_platform(self) -> ConnectionUpdate:
        if self.platform is not None and self.properties is not None:
            _validate_platform_properties(self.platform, self.properties)
        return self


class ConnectionOut(BaseModel):
    id: UUID
    logical_name: str
    platform: str
    host: str | None
    port: int | None
    service_name: str | None
    vault_path: str | None
    classification: Classification | None
    owner_team: str | None
    description: str | None
    properties: dict[str, Any]
    created_at: datetime
    updated_at: datetime


# --------------------------------------------------------------------
# Search response models
# --------------------------------------------------------------------


class JobSummary(BaseModel):
    id: UUID
    namespace: str
    name: str
    job_type: str
    source_repo: str | None = None
    owner_team: str | None = None


class JobWithRole(JobSummary):
    role: Literal["reader", "writer", "both"]
    dataset_count: int
    last_seen_at: datetime
    latest_status: str | None = None


class LineageNode(BaseModel):
    dataset_id: UUID
    dataset_fqn: str
    depth: int
    via_job_id: UUID | None = None
    via_job_name: str | None = None


class LineageGraph(BaseModel):
    root_fqn: str
    direction: Literal["upstream", "downstream"]
    max_depth: int
    nodes: list[LineageNode]


class RelatedConnection(BaseModel):
    logical_name: str
    platform: str
    classification: Classification | None
    bridging_job_count: int


class ConnectionImpact(BaseModel):
    connection: str
    affected_datasets: int
    affected_jobs: int
    sample_jobs: list[JobSummary]


class ConnectionSearchResult(BaseModel):
    id: UUID
    logical_name: str
    platform: str
    host: str | None
    port: int | None
    classification: Classification | None
    owner_team: str | None
    description: str | None
    score: float


class DatasetSearchResult(BaseModel):
    fqn: str
    name: str
    dataset_type: str
    classification: Classification | None
    connection: str
    platform: str
    score: float
