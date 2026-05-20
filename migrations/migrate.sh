#!/usr/bin/env bash
# Run all pending SQL migrations against Postgres.
#
# Usage:
#   export DATABASE_URL="postgresql://metadata:metadata@localhost:5432/metadata"
#   ./migrations/migrate.sh
#
# Or pass the DSN directly:
#   ./migrations/migrate.sh "postgresql://metadata:metadata@localhost:5432/metadata"
#
# Each SQL file in sql/ is applied exactly once; applied versions are
# recorded in the schema_migrations table.

set -euo pipefail

DSN="${1:-${DATABASE_URL:-}}"
if [[ -z "$DSN" ]]; then
    echo "ERROR: provide a DSN via DATABASE_URL env var or as first argument" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_DIR="$SCRIPT_DIR/sql"

psql "$DSN" -v ON_ERROR_STOP=1 -c "
    CREATE TABLE IF NOT EXISTS schema_migrations (
        version     TEXT PRIMARY KEY,
        applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    );
"

for sql_file in "$SQL_DIR"/V*.sql; do
    version="$(basename "$sql_file" .sql)"

    applied=$(psql "$DSN" -t -A -c \
        "SELECT COUNT(*) FROM schema_migrations WHERE version = '$version';")

    if [[ "$applied" -eq 1 ]]; then
        echo "  skip  $version (already applied)"
        continue
    fi

    echo "  apply $version ..."
    psql "$DSN" -v ON_ERROR_STOP=1 -f "$sql_file"
    psql "$DSN" -v ON_ERROR_STOP=1 -c \
        "INSERT INTO schema_migrations (version) VALUES ('$version');"
    echo "  done  $version"
done

echo ""
echo "Migrations complete."
