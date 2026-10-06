-- Stable envelope ids for the outbox (analysis.outbox.stable-envelope-id).
-- The relay stamps each row with its envelope's id and time in a committed
-- transaction before the row's first publish; every later publish of the
-- row reuses them, so a relay that crashes after publishing and before
-- deleting republishes the same id, which the bus deduplicates. A row is
-- either unstamped (both NULL) or stamped (both set).

ALTER TABLE outbox
    ADD COLUMN envelope_id text COLLATE "C" CHECK (length(envelope_id) = 26),
    ADD COLUMN at bigint CHECK (at >= 0),
    ADD CONSTRAINT outbox_stamped CHECK ((envelope_id IS NULL) = (at IS NULL));
