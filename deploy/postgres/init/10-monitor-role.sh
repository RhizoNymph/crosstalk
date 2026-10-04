#!/usr/bin/env bash
# Read-only role for postgres-exporter. Runs once at first initialisation;
# the password comes from POSTGRES_MONITOR_PASSWORD (deploy/.env).
set -euo pipefail
psql -v ON_ERROR_STOP=1 -v pw="$POSTGRES_MONITOR_PASSWORD" \
    --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
CREATE ROLE crosstalk_monitor LOGIN PASSWORD :'pw';
GRANT pg_monitor TO crosstalk_monitor;
SQL
