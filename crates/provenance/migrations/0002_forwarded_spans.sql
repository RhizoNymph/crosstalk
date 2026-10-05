-- Forwarded spans (provenance.index.forwarded-indexed): with forwarding on,
-- a span relayed from one of its agent's inputs keeps state 'relayed' and is
-- also indexed under that agent. forwarded marks such a span; its indexing
-- is the forwarded counterpart of indexed_at and expired_at: pending while
-- forward_indexed_at is NULL, indexed once it is set, expired once
-- forward_expired_at is set too.
ALTER TABLE spans
    ADD COLUMN forwarded boolean NOT NULL DEFAULT false,
    ADD COLUMN forward_indexed_at bigint,
    ADD COLUMN forward_expired_at bigint;
ALTER TABLE spans
    ADD CONSTRAINT spans_forwarded_is_relayed_input
        CHECK (NOT forwarded OR (state = 'relayed' AND relay_message IS NOT NULL)),
    ADD CONSTRAINT spans_forward_indexed_is_forwarded
        CHECK (forward_indexed_at IS NULL OR forwarded),
    ADD CONSTRAINT spans_forward_expired_after_indexed
        CHECK (forward_expired_at IS NULL OR forward_indexed_at IS NOT NULL);
CREATE INDEX spans_forward_live ON spans (forward_indexed_at)
    WHERE forward_indexed_at IS NOT NULL AND forward_expired_at IS NULL;

-- The spread rule (provenance.match.cross-agent-spread) counts the copies of
-- a span (spans relayed from it) as originations.
CREATE INDEX spans_relay_span ON spans (relay_span) WHERE relay_span IS NOT NULL;
