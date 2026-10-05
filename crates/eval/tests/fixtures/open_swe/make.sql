-- Rebuilds the Parquet shards from the synthetic rows.jsonl beside them,
-- with the column types of Open-SWE-Traces. Run from this directory:
--   duckdb < make.sql
CREATE MACRO shard(src) AS TABLE
SELECT instance_id, repo, license, language, trajectory_id, messages, tools, resolved
FROM read_json(src, format = 'newline_delimited', columns = {
    instance_id: 'VARCHAR', repo: 'VARCHAR', license: 'VARCHAR', language: 'VARCHAR',
    trajectory_id: 'VARCHAR',
    messages: 'STRUCT("role" VARCHAR, "content" VARCHAR, reasoning_content VARCHAR, tool_calls STRUCT("function" STRUCT(arguments VARCHAR, "name" VARCHAR), id VARCHAR, "type" VARCHAR)[])[]',
    tools: 'VARCHAR[]', resolved: 'INTEGER'
});
COPY (FROM shard('data/openhands/fixture_model/fixture-set/rows.jsonl'))
    TO 'data/openhands/fixture_model/fixture-set/train-00000-of-00001.parquet' (FORMAT parquet, COMPRESSION snappy);
COPY (FROM shard('data/sweagent/fixture_model/fixture-set/rows.jsonl'))
    TO 'data/sweagent/fixture_model/fixture-set/train-00000-of-00001.parquet' (FORMAT parquet, COMPRESSION snappy);
COPY (FROM shard('data/minisweagent/fixture_model/fixture-set/rows.jsonl'))
    TO 'data/minisweagent/fixture_model/fixture-set/train-00000-of-00001.parquet' (FORMAT parquet, COMPRESSION snappy);
