#!/usr/bin/env bash
# Create role + database from .env (requires a running local PostgreSQL and a superuser).
set -euo pipefail
set -a; source "$(dirname "$0")/../.env"; set +a
SU="${PG_SUPERUSER:-postgres}"
psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$SU" -v ON_ERROR_STOP=1 \
  -v u="$POSTGRES_USER" -v p="$POSTGRES_PASSWORD" -v d="$POSTGRES_DB" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'u', :'p')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'u') \gexec
SELECT format('CREATE DATABASE %I OWNER %I', :'d', :'u')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'d') \gexec
SQL
python "$(dirname "$0")/migrate.py"
