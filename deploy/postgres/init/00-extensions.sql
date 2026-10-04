-- Runs once, when the data volume is first initialised. The store harness
-- also creates vector and pg_trgm itself (CREATE EXTENSION IF NOT EXISTS);
-- creating them here keeps a fresh database ready for psql exploration.
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
-- Query statistics for postgres-exporter's stat_statements collector.
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
