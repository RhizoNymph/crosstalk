-- Coincident template stretches (provenance.span.coincident-template-originated):
-- an output stretch matching another agent's indexed span with no token
-- rare for that span's holders is the writer's own Originated text, and
-- still a copy of the earlier span for the spread rule
-- (provenance.match.cross-agent-spread). One row per (span, source):
-- `span` the writer's Originated span holding the stretch, `source` the
-- earlier span it coincides with. ProvenanceStore::relays reads these rows
-- with the spans relayed from a source.
CREATE TABLE span_coincidences (
    span   bytea NOT NULL REFERENCES spans (span),
    source bytea NOT NULL CHECK (length(source) = 16),
    PRIMARY KEY (span, source),
    CHECK (span <> source)
);
CREATE INDEX span_coincidences_source ON span_coincidences (source);
