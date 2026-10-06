-- The durable event bus (PgBus): an append-only log of envelopes, the
-- consumer groups reading it, each group's unacked deliveries, and dead
-- letters. Schema "transport" (crosstalk_store::migrate).
--
-- Times are microseconds since the epoch. Bus delays (available_at) are
-- read from the bus's injected clock, never from now().

-- The log. `seq` orders it: publishes take a transaction-scoped advisory
-- lock, so rows commit in seq order and a reader that sees seq n has seen
-- every committed seq below it.
CREATE TABLE events (
    seq      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id       text COLLATE "C" NOT NULL UNIQUE CHECK (length(id) = 26), -- Envelope::id, ULID text
    subject  text NOT NULL,                                            -- Subject wire name
    at       bigint NOT NULL CHECK (at >= 0),                          -- Envelope::at
    -- false only for an envelope stored because a dead letter was put for
    -- it directly (DeadLetterStore::put) and never published: groups never
    -- admit it, but a replay can deliver it.
    routed   boolean NOT NULL DEFAULT true,
    envelope text NOT NULL                                             -- Envelope wire JSON
);
CREATE INDEX events_by_subject ON events (subject, seq);
CREATE INDEX events_by_at ON events (at);

CREATE TABLE groups (
    name                   text COLLATE "C" PRIMARY KEY,
    subjects               text[] NOT NULL,          -- sorted, distinct wire names
    max_attempts           integer NOT NULL CHECK (max_attempts > 0),
    initial_backoff_micros bigint NOT NULL CHECK (initial_backoff_micros > 0),
    max_backoff_micros     bigint NOT NULL CHECK (max_backoff_micros >= initial_backoff_micros),
    admitted_through       bigint NOT NULL CHECK (admitted_through >= 0) -- last events.seq admitted
);

-- One row per admitted, unacked envelope of a group. An ack deletes it.
-- `attempt` counts deliveries so far; a held row's attempt is the one its
-- holder was given.
CREATE TABLE deliveries (
    group_name   text COLLATE "C" NOT NULL REFERENCES groups (name),
    seq          bigint NOT NULL REFERENCES events (seq),
    at           bigint NOT NULL,              -- the envelope's time (frontier)
    attempt      integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    state        text NOT NULL CHECK (state IN ('ready', 'held', 'delayed')),
    available_at bigint,                       -- 'delayed': bus-clock micros it becomes ready
    last_error   text,
    PRIMARY KEY (group_name, seq),
    CHECK ((state = 'delayed') = (available_at IS NOT NULL))
);
CREATE INDEX deliveries_ready ON deliveries (group_name, seq) WHERE state = 'ready';
CREATE INDEX deliveries_delayed ON deliveries (group_name, available_at) WHERE state = 'delayed';
CREATE INDEX deliveries_oldest ON deliveries (group_name, at);
CREATE INDEX deliveries_by_seq ON deliveries (seq);

-- Deliveries that exhausted their retries. Never dropped automatically;
-- the events they name are kept by prune.
CREATE TABLE dead_letters (
    group_name text COLLATE "C" NOT NULL,
    event_id   text COLLATE "C" NOT NULL CHECK (length(event_id) = 26),
    seq        bigint NOT NULL,
    at         bigint NOT NULL,
    envelope   text NOT NULL,
    attempts   integer NOT NULL CHECK (attempts > 0),
    last_error text NOT NULL,
    PRIMARY KEY (group_name, event_id)
);
-- DeadLetterStore::list: newest envelope first, ties by group.
CREATE INDEX dead_letters_listing ON dead_letters (event_id DESC, group_name DESC);
CREATE INDEX dead_letters_oldest ON dead_letters (group_name, at);
CREATE INDEX dead_letters_by_seq ON dead_letters (seq);
