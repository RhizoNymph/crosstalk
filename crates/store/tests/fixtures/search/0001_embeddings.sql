-- Fixture layer migration (search): uses both required extensions, which
-- live in `public` and resolve through the runner's search_path.
CREATE TABLE embeddings (
    id bigint PRIMARY KEY,
    label text NOT NULL,
    embedding vector(3) NOT NULL
);
CREATE INDEX embeddings_label_trgm ON embeddings USING gin (label gin_trgm_ops);
