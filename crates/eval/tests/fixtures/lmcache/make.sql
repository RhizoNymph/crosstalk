-- Rebuilds the Parquet files from the synthetic JSONL beside them, with the
-- column types of the LMCache agentic traces; `g` is the row group a row
-- goes to (each UNION ALL branch is written as its own row group). Run
-- from this directory:
--   duckdb < make.sql
SET preserve_insertion_order = true;
CREATE MACRO rows(src, grp) AS TABLE
SELECT session_id, model, input, output_length, pre_gap
FROM read_json(src, format = 'newline_delimited', columns = {
    session_id: 'VARCHAR', model: 'VARCHAR',
    input: 'STRUCT("role" VARCHAR, "content" VARCHAR, tool_calls STRUCT(id VARCHAR, "type" VARCHAR, "function" STRUCT("name" VARCHAR, arguments VARCHAR))[], tool_call_id VARCHAR, "name" VARCHAR)[]',
    output_length: 'BIGINT', pre_gap: 'DOUBLE', g: 'INTEGER'
})
WHERE g = grp;
COPY (FROM rows('data/train-00000-of-00002.jsonl', 0) UNION ALL FROM rows('data/train-00000-of-00002.jsonl', 1))
    TO 'data/train-00000-of-00002.parquet' (FORMAT parquet, COMPRESSION snappy, ROW_GROUP_SIZE 1);
COPY (FROM rows('data/train-00001-of-00002.jsonl', 0))
    TO 'data/train-00001-of-00002.parquet' (FORMAT parquet, COMPRESSION snappy);
