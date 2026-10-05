-- L6 search index (crosstalk_analysis::search): the indexed confirmed
-- transmissions, their embeddings per model, the index's copy of each
-- transmission's current verdict, and the embedding model queries use.
-- Runs in schema "analysis" (crosstalk_store::migrate); runtime queries
-- qualify every table.
--
-- Ids are ULID text in COLLATE "C" columns (byte order is id order), spec
-- values with a wire form are their wire JSON (text), times are
-- microseconds since the epoch.

CREATE TABLE search_docs (
    transmission text COLLATE "C" PRIMARY KEY,
    -- Sender and reader as attributed; resolved through AgentDirectory at
    -- query time.
    from_agent text COLLATE "C" NOT NULL,
    to_agent text COLLATE "C" NOT NULL,
    -- Route's wire JSON, its channel as stored; resolved at query time.
    route text NOT NULL,
    -- Confirmed::at.
    confirmed_at bigint NOT NULL,
    -- The matched content, as the embedder saw it.
    body text NOT NULL,
    -- Full-text terms of the body's first 65,536 characters (the 'simple'
    -- configuration: lower-cased words, no stemming or stop words).
    terms tsvector GENERATED ALWAYS AS (to_tsvector('simple'::regconfig, left(body, 65536))) STORED
);

CREATE INDEX search_docs_terms ON search_docs USING gin (terms);
CREATE INDEX search_docs_confirmed_at ON search_docs (confirmed_at);

-- At most one embedding per (transmission, model). A model is its name and
-- dimension (EmbeddingModel); the vector's dimension is the model's.
CREATE TABLE search_embeddings (
    transmission text COLLATE "C" NOT NULL REFERENCES search_docs (transmission) ON DELETE CASCADE,
    model_name text NOT NULL,
    model_dimension integer NOT NULL CHECK (model_dimension > 0),
    embedding vector NOT NULL CHECK (vector_dims(embedding) = model_dimension),
    PRIMARY KEY (transmission, model_name, model_dimension)
);

CREATE INDEX search_embeddings_model ON search_embeddings (model_name, model_dimension);

-- SearchCorpus::judge's copy (CurrentVerdict), whether or not the
-- transmission is indexed.
CREATE TABLE search_verdicts (
    transmission text COLLATE "C" PRIMARY KEY,
    -- Verdict's wire JSON, NULL for a withdrawal.
    verdict text,
    revision integer NOT NULL CHECK (revision > 0)
);

-- The model queries must be embedded with (one row), and the models whose
-- vectors were dropped.
CREATE TABLE search_model (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    model_name text NOT NULL,
    model_dimension integer NOT NULL CHECK (model_dimension > 0)
);

CREATE TABLE search_dropped_models (
    model_name text NOT NULL,
    model_dimension integer NOT NULL,
    PRIMARY KEY (model_name, model_dimension)
);

-- The events L6's stores publish, appended in the transaction that makes
-- the change and deleted once the sink took them.
CREATE TABLE outbox (
    seq bigserial PRIMARY KEY,
    -- BusEvent's wire JSON.
    event text NOT NULL
);
