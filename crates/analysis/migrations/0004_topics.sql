-- L6 topic catalog (crosstalk_analysis::topics, PgTopicCatalog): the
-- topic-model version history, every version's topics, the lineage from
-- each version to its successor, the topic assignments and the all-time
-- sizes frozen when retention drops a version.
--
-- Spec values are their wire JSON (text), decoded through their checked
-- constructors; ids are ULID text in COLLATE "C" columns; times are
-- microseconds since the epoch.

-- The catalog's one control row: the number the next fit gets. A counter
-- row rather than a sequence, so a fit refused or rolled back (a retried
-- serializable transaction included) never consumes a number, and a
-- failed fit's number is never given again.
CREATE TABLE topic_catalog (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    next_version bigint NOT NULL CHECK (next_version > 0)
);

-- One row per version in the history (a failed fit leaves none).
CREATE TABLE topic_versions (
    version bigint PRIMARY KEY CHECK (version >= 0),
    -- TopicVersionInfo's wire JSON: status and retention (pin, drop).
    info text NOT NULL,
    -- The status kind and retention, for reading without decoding.
    state text NOT NULL CHECK (state IN ('fitting', 'ready', 'active', 'superseded')),
    retained boolean NOT NULL,
    -- When a fitting version's fit returned (complete_fit), until it is
    -- marked ready.
    fit_returned_at bigint CHECK (fit_returned_at >= 0),
    -- TopicSizes' wire JSON: the all-time sizes frozen when the version was
    -- dropped.
    frozen_sizes text,
    CHECK (fit_returned_at IS NULL OR state = 'fitting'),
    CHECK ((frozen_sizes IS NOT NULL) = (NOT retained))
);

-- Every topic of every version whose fit returned. A topic id belongs to
-- one version.
CREATE TABLE topics (
    topic text COLLATE "C" PRIMARY KEY,
    version bigint NOT NULL REFERENCES topic_versions (version),
    -- Topic's wire JSON (label, terms, centroid, fit time).
    topic_row text NOT NULL
);
CREATE INDEX topics_by_version ON topics (version, topic);

-- The lineage from a version to its successor, keyed by the older one.
CREATE TABLE topic_lineage (
    from_version bigint PRIMARY KEY REFERENCES topic_versions (version),
    to_version bigint NOT NULL REFERENCES topic_versions (version),
    -- TopicLineage's wire JSON.
    lineage text NOT NULL,
    CHECK (from_version < to_version)
);
CREATE INDEX topic_lineage_to ON topic_lineage (to_version);

-- At most one assignment per (version, transmission). Deleted when the
-- version is dropped (after its sizes are frozen) or its fit fails.
CREATE TABLE topic_assignments (
    version bigint NOT NULL REFERENCES topic_versions (version),
    transmission text COLLATE "C" NOT NULL,
    -- NULL for an outlier.
    topic text COLLATE "C" REFERENCES topics (topic),
    confirmed_at bigint NOT NULL,
    matched_bytes bigint NOT NULL CHECK (matched_bytes > 0),
    from_agent text COLLATE "C" NOT NULL,
    to_agent text COLLATE "C" NOT NULL,
    PRIMARY KEY (version, transmission)
);
