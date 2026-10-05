-- L5 flow stores (crates/flow/src/store): the channel registry, accesses,
-- the recorded traffic of channels, the transmission store with its verdict
-- logs, the event outbox, the cursor book and the correlator shards' tick
-- checkpoints. Runs in schema "flow" (crosstalk-store's per-layer runner).
--
-- Conventions:
--   * Entity ids are their ULID text (26 upper-case Crockford characters).
--     Columns use the "C" collation, so text order is id order.
--   * Times are microseconds since the epoch (Timestamp::as_micros).
--   * Spec values are stored as the JSON text of their wire form in TEXT
--     columns (never JSONB, which refuses \u0000 and rewrites the text);
--     the scalar columns beside them are derived from the value by the store
--     in the same statement, for indexes and constraints.

-- Every stored channel. `origin` and `policy` are the JSON of
-- ChannelOrigin and Policy; `kind`, `created_at` and `superseded_by` are
-- derived from `origin`.
CREATE TABLE channels (
    id            TEXT COLLATE "C" PRIMARY KEY CHECK (length(id) = 26),
    kind          TEXT NOT NULL CHECK (kind IN ('declared', 'discovered', 'superseded')),
    origin        TEXT NOT NULL,
    created_at    BIGINT NOT NULL,
    superseded_by TEXT COLLATE "C" REFERENCES channels (id),
    policy        TEXT NOT NULL,
    CHECK ((kind = 'superseded') = (superseded_by IS NOT NULL)),
    CHECK (superseded_by IS DISTINCT FROM id)
);

-- ChannelReads::channels: newest created first, ties by id.
CREATE INDEX channels_by_created ON channels (created_at DESC, id DESC);
-- The members of a canonical channel, and the directory.
CREATE INDEX channels_by_superseder ON channels (superseded_by) WHERE superseded_by IS NOT NULL;
-- Lookups and overlap checks read the declared patterns.
CREATE INDEX channels_declared ON channels (id) WHERE kind = 'declared';

-- A channel's own resources are listed in the order they joined it.
CREATE SEQUENCE resource_listing;

-- Every stored resource, on a channel or on none. `locator_key` is the
-- JSON text of its Locator: one resource per locator. `listed_seq` orders
-- Channel::resources; a seed resource and a resource on no channel have
-- none.
CREATE TABLE resources (
    id          TEXT COLLATE "C" PRIMARY KEY CHECK (length(id) = 26),
    locator_key TEXT NOT NULL UNIQUE,
    resource    TEXT NOT NULL,
    channel_id  TEXT COLLATE "C" REFERENCES channels (id),
    listed_seq  BIGINT,
    CHECK (listed_seq IS NULL OR channel_id IS NOT NULL)
);

CREATE INDEX resources_by_channel ON resources (channel_id, listed_seq) WHERE channel_id IS NOT NULL;

-- Every recorded access. `write_outcome` is the outcome of a write
-- (delivered, rejected or unknown) once the spec's Access carries one; a
-- read has none.
CREATE TABLE accesses (
    id            TEXT COLLATE "C" PRIMARY KEY CHECK (length(id) = 26),
    resource_id   TEXT COLLATE "C" NOT NULL REFERENCES resources (id),
    agent         TEXT COLLATE "C" NOT NULL,
    at            BIGINT NOT NULL,
    kind          TEXT NOT NULL CHECK (kind IN ('write', 'read')),
    write_outcome TEXT CHECK (write_outcome IN ('delivered', 'rejected', 'unknown')),
    access        TEXT NOT NULL,
    CHECK (kind = 'write' OR write_outcome IS NULL)
);

-- ChannelRegistry::resource_use: a resource's accesses in a window.
CREATE INDEX accesses_by_resource ON accesses (resource_id, at);

-- Every policy decision recorded for a channel. A history reads in
-- (at, seq) order: equal times keep the order they were recorded in, which
-- is where PolicyHistory::record puts a later arrival.
CREATE TABLE policy_decisions (
    channel_id TEXT COLLATE "C" NOT NULL REFERENCES channels (id),
    seq        BIGINT GENERATED ALWAYS AS IDENTITY,
    at         BIGINT NOT NULL,
    decision   TEXT NOT NULL,
    PRIMARY KEY (channel_id, seq)
);

CREATE INDEX policy_decisions_in_order ON policy_decisions (channel_id, at, seq);

-- The registry's record of every channel transmission, as last recorded
-- (ChannelTraffic::record_transmission): a channel's cross-agent traffic is
-- tallied from it at the read. `channel_id` is the route's channel as
-- stored, never rewritten by a supersession.
CREATE TABLE channel_traffic (
    transmission_id TEXT COLLATE "C" PRIMARY KEY CHECK (length(transmission_id) = 26),
    channel_id      TEXT COLLATE "C" NOT NULL REFERENCES channels (id),
    opened_at       BIGINT NOT NULL,
    confirmed       BOOLEAN NOT NULL,
    transmission    TEXT NOT NULL
);

-- ChannelReads::transmissions: newest opened first, ties by id.
CREATE INDEX channel_traffic_by_channel
    ON channel_traffic (channel_id, opened_at DESC, transmission_id DESC);

-- The transmission store: every transmission as last saved, beside its
-- verdict log. `channel_id` is a channel route's channel (no foreign key:
-- the store keeps any route). Indexed by (state, channel, opened_at) for
-- the transmission list.
CREATE TABLE transmissions (
    id           TEXT COLLATE "C" PRIMARY KEY CHECK (length(id) = 26),
    state        TEXT NOT NULL CHECK (state IN ('detected', 'awaiting_content', 'suspected',
                                                'confirmed', 'classified', 'aggregated',
                                                'discarded')),
    route        TEXT NOT NULL CHECK (route IN ('channel', 'delegation', 'direct', 'unobserved')),
    channel_id   TEXT COLLATE "C",
    opened_at    BIGINT NOT NULL,
    transmission TEXT NOT NULL,
    CHECK ((route = 'channel') = (channel_id IS NOT NULL))
);

CREATE INDEX transmissions_by_state_channel
    ON transmissions (state, channel_id, opened_at DESC, id DESC);
CREATE INDEX transmissions_by_opened ON transmissions (opened_at, id);

-- Every verdict record, append only: revision n is the log's nth record.
CREATE TABLE verdicts (
    transmission_id TEXT COLLATE "C" NOT NULL REFERENCES transmissions (id),
    revision        INTEGER NOT NULL CHECK (revision >= 1),
    verdict         TEXT CHECK (verdict IN ('genuine', 'false_detection')),
    record          TEXT NOT NULL,
    PRIMARY KEY (transmission_id, revision)
);

-- The transactional outbox: the events a write publishes, staged in the
-- write's transaction and relayed to the bus after it commits.
CREATE TABLE outbox (
    seq       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event     TEXT NOT NULL,
    staged_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The cursors the registry's lists issued: a token resolves only with the
-- list and binding it was issued for.
CREATE SEQUENCE cursor_tokens;

CREATE TABLE cursors (
    token     TEXT PRIMARY KEY,
    list      TEXT NOT NULL,
    binding   TEXT NOT NULL,
    after_key TEXT NOT NULL,
    issued_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX cursors_by_issued ON cursors (issued_at);

-- The last tick each correlator shard processed (PipelineFrontier's
-- ticked_through is the earliest of them). Only ever moves forward.
CREATE TABLE shard_ticks (
    shard          INTEGER PRIMARY KEY CHECK (shard >= 0),
    ticked_through BIGINT NOT NULL
);
