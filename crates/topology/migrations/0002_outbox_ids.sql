-- Stable envelope ids for the outbox (topology.outbox.stable-envelope-id).
--
-- The relay stamps each row with its envelope id (ULID text) and time
-- (Unix microseconds) in a transaction of its own, committed before the
-- row's first publish. Every later publish of the row, after a failed or
-- interrupted drain, reuses the stamp, so a durable bus that already holds
-- the id adds nothing. Rows staged before this migration are unstamped and
-- get their stamp from the next drain.
--
-- A traffic row is stamped when a drain coalesces the batch's unstamped
-- traffic rows into it (their hull), so a stamped traffic row names one
-- fixed notification.

ALTER TABLE outbox
    ADD COLUMN envelope_id text COLLATE "C" CHECK (length(envelope_id) = 26),
    ADD COLUMN at bigint CHECK (at >= 0);

ALTER TABLE outbox
    ADD CONSTRAINT outbox_stamped CHECK ((envelope_id IS NULL) = (at IS NULL));
