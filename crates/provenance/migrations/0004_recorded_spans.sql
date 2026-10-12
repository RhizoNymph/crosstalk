-- Span-index records of spans no scan committed (SpanIndex::record,
-- provenance.span-index.spans-as-recorded): where such a span sits and who
-- wrote it, kept once per id. A scan's spans are in `spans`, which needs
-- the span's exchange and its place in the output; a span recorded through
-- the index alone (the world seed's origin spans) has neither, so it is
-- kept here and is in no exchange's spans. Ids and hashes only
-- (provenance.index.no-text).
CREATE TABLE recorded_spans (
    span        bytea PRIMARY KEY CHECK (length(span) = 16),
    agent       bytea NOT NULL CHECK (length(agent) = 16),
    exchange    bytea NOT NULL CHECK (length(exchange) = 16),
    message     bytea NOT NULL CHECK (length(message) = 32),
    part        integer NOT NULL CHECK (part BETWEEN 0 AND 65535),
    range_start bigint NOT NULL CHECK (range_start >= 0),
    range_end   bigint NOT NULL CHECK (range_end > range_start AND range_end <= 4294967295)
);
