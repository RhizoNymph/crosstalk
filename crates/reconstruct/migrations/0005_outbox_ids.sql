-- Stable outbox envelope ids (reconstruct.outbox.stable-envelope-id,
-- INV-1211). The relay stamps each staged row with the envelope id and
-- time it is published under, in a committed transaction of its own before
-- the row's first publish; every later publish of the row reuses them, so a
-- bus idempotent on ids holds the event once. Rows are staged unstamped.
--
-- envelope_id: the envelope's ULID text; at: its time, microseconds since
-- the epoch. Both or neither.

ALTER TABLE outbox
    ADD COLUMN envelope_id text COLLATE "C" CHECK (length(envelope_id) = 26),
    ADD COLUMN at bigint;

ALTER TABLE outbox
    ADD CONSTRAINT outbox_stamped CHECK ((envelope_id IS NULL) = (at IS NULL));
