-- V0001: Initial schema
-- Extensions
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- ---------------- connection ----------------
CREATE TABLE connection (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    logical_name    TEXT NOT NULL UNIQUE,
    platform        TEXT NOT NULL,
    host            TEXT,
    port            INT,
    service_name    TEXT,
    vault_path      TEXT,
    classification  TEXT CHECK (classification IN ('public','internal','confidential')),
    owner_team      TEXT,
    description     TEXT,
    properties      JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at      TIMESTAMPTZ
);

CREATE INDEX idx_connection_platform
    ON connection(platform) WHERE deleted_at IS NULL;

CREATE INDEX idx_connection_classification
    ON connection(classification) WHERE deleted_at IS NULL;

CREATE INDEX idx_connection_logical_name_trgm
    ON connection USING GIN (logical_name gin_trgm_ops);

CREATE INDEX idx_connection_properties_gin
    ON connection USING GIN (properties);

-- ---------------- dataset ----------------
CREATE TABLE dataset (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    connection_id   UUID NOT NULL REFERENCES connection(id) ON DELETE RESTRICT,
    fqn             TEXT NOT NULL,
    name            TEXT NOT NULL,
    schema_name     TEXT,
    database_name   TEXT,
    dataset_type    TEXT NOT NULL CHECK (dataset_type IN
        ('table','view','topic','file','iceberg_table','materialized_view','unknown')),
    classification  TEXT CHECK (classification IN ('public','internal','confidential')),
    properties      JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at      TIMESTAMPTZ,
    UNIQUE (connection_id, fqn)
);

CREATE INDEX idx_dataset_connection
    ON dataset(connection_id) WHERE deleted_at IS NULL;

CREATE INDEX idx_dataset_name_trgm
    ON dataset USING GIN (name gin_trgm_ops);

CREATE INDEX idx_dataset_fqn_trgm
    ON dataset USING GIN (fqn gin_trgm_ops);

CREATE INDEX idx_dataset_classification
    ON dataset(classification) WHERE deleted_at IS NULL;

-- ---------------- dataset_column ----------------
CREATE TABLE dataset_column (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id      UUID NOT NULL REFERENCES dataset(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    data_type       TEXT,
    ordinal         INT,
    classification  TEXT,
    description     TEXT,
    properties      JSONB NOT NULL DEFAULT '{}'::JSONB,
    UNIQUE (dataset_id, name)
);

CREATE INDEX idx_dataset_column_dataset ON dataset_column(dataset_id);

-- ---------------- job ----------------
CREATE TABLE job (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    namespace       TEXT NOT NULL,
    name            TEXT NOT NULL,
    job_type        TEXT NOT NULL CHECK (job_type IN
        ('airflow_task','spark','flink','python','fastapi','trino_query','unknown')),
    source_repo     TEXT,
    source_path     TEXT,
    owner_team      TEXT,
    properties      JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at      TIMESTAMPTZ,
    UNIQUE (namespace, name)
);

CREATE INDEX idx_job_namespace ON job(namespace) WHERE deleted_at IS NULL;
CREATE INDEX idx_job_name_trgm ON job USING GIN (name gin_trgm_ops);
CREATE INDEX idx_job_type ON job(job_type) WHERE deleted_at IS NULL;

-- ---------------- job_run ----------------
CREATE TABLE job_run (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id          UUID NOT NULL REFERENCES job(id) ON DELETE CASCADE,
    run_id          TEXT NOT NULL,
    started_at      TIMESTAMPTZ,
    ended_at        TIMESTAMPTZ,
    status          TEXT CHECK (status IN
        ('started','running','completed','failed','aborted','other')),
    facets          JSONB NOT NULL DEFAULT '{}'::JSONB,
    UNIQUE (job_id, run_id)
);

CREATE INDEX idx_job_run_started ON job_run(started_at DESC NULLS LAST);
CREATE INDEX idx_job_run_job_started
    ON job_run(job_id, started_at DESC NULLS LAST);

-- ---------------- lineage_edge ----------------
CREATE TABLE lineage_edge (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id          UUID NOT NULL REFERENCES job(id) ON DELETE CASCADE,
    dataset_id      UUID NOT NULL REFERENCES dataset(id) ON DELETE CASCADE,
    direction       TEXT NOT NULL CHECK (direction IN ('input','output')),
    column_mapping  JSONB,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (job_id, dataset_id, direction)
);

CREATE INDEX idx_edge_dataset_direction
    ON lineage_edge(dataset_id, direction);

CREATE INDEX idx_edge_job_direction
    ON lineage_edge(job_id, direction);

-- ---------------- outbox ----------------
CREATE TABLE outbox (
    id              BIGSERIAL PRIMARY KEY,
    aggregate_type  TEXT NOT NULL,
    aggregate_id    UUID,
    event_type      TEXT NOT NULL,
    payload         JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ,
    published_targets TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[]
);

CREATE INDEX idx_outbox_unpublished
    ON outbox(created_at) WHERE published_at IS NULL;

-- ---------------- audit_log ----------------
CREATE TABLE audit_log (
    id              BIGSERIAL PRIMARY KEY,
    actor           TEXT NOT NULL,
    action          TEXT NOT NULL,
    entity_type     TEXT NOT NULL,
    entity_id       UUID,
    before_state    JSONB,
    after_state     JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_audit_log_entity
    ON audit_log(entity_type, entity_id);

CREATE INDEX idx_audit_log_created ON audit_log(created_at DESC);

-- ---------------- updated_at trigger ----------------
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_connection_updated_at
    BEFORE UPDATE ON connection
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_dataset_updated_at
    BEFORE UPDATE ON dataset
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_job_updated_at
    BEFORE UPDATE ON job
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
