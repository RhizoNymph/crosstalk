-- L4 provenance, schema "provenance" (the runner sets the search path).
--
-- No message text anywhere: ids, content hashes, locations, states, times
-- and fingerprints only. Ids are 16-byte ULIDs, hashes 32-byte BLAKE3
-- digests, times microseconds since the Unix epoch, fingerprints the u64
-- bit pattern in a bigint.

-- Exchanges from ExchangeCaptured, and each one's scan status.
CREATE TABLE exchanges (
    exchange        bytea PRIMARY KEY CHECK (length(exchange) = 16),
    started_at      bigint NOT NULL,
    output          bytea CHECK (output IS NULL OR length(output) = 32),
    status          text NOT NULL CHECK (status IN ('pending', 'scanned', 'indexed', 'failed')),
    status_at       bigint,
    failure_kind    text CHECK (failure_kind IN ('body_missing', 'body_undecodable', 'inconsistent')),
    failure_message bytea,
    failure_reason  text,
    CHECK ((status = 'pending') = (status_at IS NULL)),
    CHECK ((status = 'failed') = (failure_kind IS NOT NULL))
);
CREATE INDEX exchanges_started ON exchanges (started_at);

-- The request's message hashes, kept until pruned after retention.
CREATE TABLE exchange_requests (
    exchange bytea PRIMARY KEY REFERENCES exchanges (exchange) ON DELETE CASCADE,
    request  bytea[] NOT NULL
);

-- Which messages each exchange's delta listed: per-message scan status.
CREATE TABLE scanned_messages (
    message    bytea NOT NULL CHECK (length(message) = 32),
    exchange   bytea NOT NULL REFERENCES exchanges (exchange) ON DELETE CASCADE,
    scanned_as text NOT NULL CHECK (scanned_as IN ('input', 'system', 'output')),
    PRIMARY KEY (message, exchange, scanned_as)
);
CREATE INDEX scanned_messages_exchange ON scanned_messages (exchange);

-- Spans. The primary key serves a span's location by id.
CREATE SEQUENCE index_seq;
CREATE TABLE spans (
    span          bytea PRIMARY KEY CHECK (length(span) = 16),
    agent         bytea NOT NULL,
    exchange      bytea NOT NULL REFERENCES exchanges (exchange),
    message       bytea NOT NULL CHECK (length(message) = 32),
    part          integer NOT NULL CHECK (part BETWEEN 0 AND 65535),
    range_start   bigint NOT NULL CHECK (range_start >= 0),
    range_end     bigint NOT NULL CHECK (range_end > range_start AND range_end <= 4294967295),
    ordinal       integer NOT NULL CHECK (ordinal >= 0),
    state         text NOT NULL CHECK (state IN ('common', 'relayed', 'originated', 'indexed', 'propagated', 'expired')),
    relay_span    bytea,
    relay_message bytea,
    indexed_at    bigint,
    first_hit_at  bigint,
    hits          bigint CHECK (hits IS NULL OR hits > 0),
    expired_at    bigint,
    index_seq     bigint UNIQUE,
    CHECK ((state = 'relayed') = ((relay_span IS NULL) <> (relay_message IS NULL))),
    CHECK ((state IN ('indexed', 'propagated', 'expired')) = (indexed_at IS NOT NULL)),
    CHECK (state <> 'propagated' OR (first_hit_at IS NOT NULL AND hits IS NOT NULL)),
    CHECK ((state = 'expired') = (expired_at IS NOT NULL))
);
CREATE INDEX spans_exchange ON spans (exchange, ordinal);
CREATE INDEX spans_message ON spans (message, part, range_start);
CREATE INDEX spans_live ON spans (indexed_at) WHERE state IN ('indexed', 'propagated');

-- Content matches, listed by reader message, by origin span and by reader
-- exchange. The id is the ContentMatched envelope's.
CREATE TABLE matches (
    id              bytea PRIMARY KEY CHECK (length(id) = 16),
    reader_exchange bytea NOT NULL REFERENCES exchanges (exchange),
    ordinal         integer NOT NULL CHECK (ordinal >= 0),
    at              bigint NOT NULL,
    origin          bytea NOT NULL REFERENCES spans (span),
    origin_agent    bytea NOT NULL,
    reader          bytea NOT NULL,
    read_message    bytea NOT NULL CHECK (length(read_message) = 32),
    read_part       integer NOT NULL CHECK (read_part BETWEEN 0 AND 65535),
    read_start      bigint NOT NULL CHECK (read_start >= 0),
    read_end        bigint NOT NULL CHECK (read_end > read_start),
    carrier         text NOT NULL,
    kind            text NOT NULL,
    matched_bytes   bigint NOT NULL CHECK (matched_bytes > 0)
);
CREATE INDEX matches_reader_message ON matches (read_message, read_part, read_start);
CREATE INDEX matches_origin ON matches (origin, at);
CREATE INDEX matches_reader_exchange ON matches (reader_exchange, ordinal);

-- The fingerprint index: postings of originated spans, and one
-- observation per scanned text. Fingerprints and ids only
-- (provenance.index.no-text).
CREATE TABLE postings (
    fingerprint bigint NOT NULL,
    span        bytea NOT NULL,
    span_offset bigint NOT NULL CHECK (span_offset >= 0),
    PRIMARY KEY (fingerprint, span, span_offset)
);
CREATE INDEX postings_span ON postings (span);

CREATE TABLE observations (
    observation bigserial PRIMARY KEY,
    at          bigint NOT NULL
);
CREATE INDEX observations_at ON observations (at);

CREATE TABLE observed (
    fingerprint bigint NOT NULL,
    observation bigint NOT NULL REFERENCES observations (observation) ON DELETE CASCADE,
    at          bigint NOT NULL,
    PRIMARY KEY (fingerprint, observation)
);
CREATE INDEX observed_observation ON observed (observation);
CREATE INDEX observed_at ON observed (fingerprint, at);
