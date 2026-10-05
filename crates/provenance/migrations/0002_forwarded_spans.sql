-- Forwarded spans (provenance.index.forwarded-indexed): a span relayed from
-- one of its agent's inputs keeps state 'relayed' and is also indexed under
-- that agent. Its indexing is recorded here, the forwarded counterpart of
-- indexed_at and expired_at: pending while forward_indexed_at is NULL,
-- indexed once it is set, expired once forward_expired_at is set too.
ALTER TABLE spans
    ADD COLUMN forward_indexed_at bigint,
    ADD COLUMN forward_expired_at bigint;
ALTER TABLE spans
    ADD CONSTRAINT spans_forward_is_relayed_input
        CHECK (forward_indexed_at IS NULL OR (state = 'relayed' AND relay_message IS NOT NULL)),
    ADD CONSTRAINT spans_forward_expired_after_indexed
        CHECK (forward_expired_at IS NULL OR forward_indexed_at IS NOT NULL);
CREATE INDEX spans_forward_live ON spans (forward_indexed_at)
    WHERE forward_indexed_at IS NOT NULL AND forward_expired_at IS NULL;
