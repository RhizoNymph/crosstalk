-- L7 topology: edge and access buckets, the contributions and accesses
-- behind them, versions, the watermark, the verdict copy, drill-down
-- cursors and the outbox. Plain Postgres (decision D3): the bucket tables
-- are range-partitioned on bucket_start; partitions are created on demand
-- by the store (crates/topology/src/store/partition.rs).
--
-- Ids are ULID text (26 characters, which sorts as the ULID does), times
-- are Unix microseconds, routes are the spec's `Route` as serde JSON text.

-- The store's one control row: the version queries read and the exposed
-- watermark. Writes take it FOR SHARE (apply) or FOR UPDATE (activate,
-- drop, advance), which serializes every control change with the applies
-- it would make late or refuse.
CREATE TABLE state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    active_version bigint NOT NULL DEFAULT 0 CHECK (active_version >= 0),
    watermark_micros bigint NOT NULL DEFAULT 0 CHECK (watermark_micros >= 0)
);
INSERT INTO state DEFAULT VALUES;

-- Every topic-model version the store has heard of. Version 0 is active
-- (activated) from the start.
CREATE TABLE versions (
    version bigint PRIMARY KEY CHECK (version >= 0),
    -- TopicVersionReady's count; the first one received is kept.
    ready_count bigint CHECK (ready_count >= 0),
    activated boolean NOT NULL DEFAULT false,
    dropped boolean NOT NULL DEFAULT false
);
INSERT INTO versions (version, activated) VALUES (0, true);

-- One row per applied (version, transmission): what the drill-down lists
-- and what an `Exclude` query subtracts.
CREATE TABLE contributions (
    version bigint NOT NULL,
    transmission text NOT NULL,
    from_agent text NOT NULL,
    to_agent text NOT NULL,
    route text NOT NULL,
    topic text,
    at_micros bigint NOT NULL CHECK (at_micros >= 0),
    matched_bytes bigint NOT NULL CHECK (matched_bytes > 0),
    PRIMARY KEY (version, transmission)
);
CREATE INDEX contributions_by_time ON contributions (version, at_micros);
CREATE INDEX contributions_by_transmission ON contributions (transmission);

-- The distinct transmissions classified with cause Refit processed under
-- each version (applied, already applied, or rejected as a self-edge).
CREATE TABLE refit_processed (
    version bigint NOT NULL,
    transmission text NOT NULL,
    PRIMARY KEY (version, transmission)
);

-- Edge buckets: per version, bucket, sender, reader, route and topic ('' for
-- an outlier), as attributed and routed (resolved at read time).
CREATE TABLE edge_buckets (
    version bigint NOT NULL,
    bucket_start bigint NOT NULL CHECK (bucket_start >= 0),
    from_agent text NOT NULL,
    to_agent text NOT NULL,
    route text NOT NULL,
    topic text NOT NULL,
    transmissions bigint NOT NULL CHECK (transmissions > 0),
    matched_bytes bigint NOT NULL CHECK (matched_bytes > 0),
    PRIMARY KEY (version, bucket_start, from_agent, to_agent, route, topic)
) PARTITION BY RANGE (bucket_start);

-- Every applied access, once per id.
CREATE TABLE accesses (
    access text PRIMARY KEY,
    agent text NOT NULL,
    resource text NOT NULL,
    op smallint NOT NULL CHECK (op IN (0, 1)),
    at_micros bigint NOT NULL CHECK (at_micros >= 0)
);

-- Access buckets: per bucket, agent, resource and op (0 write, 1 read), as
-- recorded (the resource resolves to its channel at read time).
CREATE TABLE access_buckets (
    bucket_start bigint NOT NULL CHECK (bucket_start >= 0),
    agent text NOT NULL,
    resource text NOT NULL,
    op smallint NOT NULL CHECK (op IN (0, 1)),
    accesses bigint NOT NULL CHECK (accesses > 0),
    PRIMARY KEY (bucket_start, agent, resource, op)
) PARTITION BY RANGE (bucket_start);

-- The store's copy of each transmission's current verdict
-- (CurrentVerdict): verdict NULL (withdrawn), 0 Genuine, 1 FalseDetection.
CREATE TABLE verdicts (
    transmission text PRIMARY KEY,
    verdict smallint CHECK (verdict IN (0, 1)),
    revision bigint NOT NULL CHECK (revision > 0)
);
CREATE INDEX verdicts_false_detection ON verdicts (transmission) WHERE verdict = 1;

-- Drill-down cursors: what each issued token is bound to and resumes after.
CREATE TABLE cursors (
    token text PRIMARY KEY,
    binding text NOT NULL,
    version bigint NOT NULL,
    confirmed_at bigint NOT NULL,
    transmission text NOT NULL
);

-- Events committed with the change that decided them, relayed to the bus
-- after commit in seq order: a bus event (JSON), or a traffic change (the
-- bucket window it touched), which the relay coalesces.
CREATE TABLE outbox (
    seq bigserial PRIMARY KEY,
    event text,
    traffic_start bigint,
    traffic_end bigint,
    CHECK ((event IS NULL) <> (traffic_start IS NULL)),
    CHECK ((traffic_start IS NULL) = (traffic_end IS NULL)),
    CHECK (traffic_start < traffic_end)
);
