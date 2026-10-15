-- Order-tolerant threading (reconstruct.thread.order-independent): what the
-- store holds for a set of exchanges is what threading them in turn-key
-- order (when each ended, then its id) leaves, whatever order they arrived
-- in. An exchange that arrives after turns of its cluster with greater keys
-- undoes them, newest first, is threaded, and threads them again from their
-- kept calls.
--
-- - conversation_turns: each turn's place in the order the store took it
--   (seq), its key's end time, and the conversation's last system message
--   once it was threaded (restored when a later turn is undone).
-- - conversations: the latest conversation of a cluster is the one whose
--   last turn has the greatest key (last_ended_at, last_exchange), not the
--   one written last; `updated` goes.
-- - responses and seen_messages: the exchange that filed or saw each row,
--   so undoing a turn removes exactly its rows. A sighting is kept per
--   exchange.
-- - thread_calls: each threading call's input, kept for the reorder window
--   to be threaded again.
--
-- Rows from before this migration have no kept call, so a late exchange
-- never undoes them; their keys and sequence are backfilled from their
-- starts.

ALTER SEQUENCE conversation_updates RENAME TO turn_seq;

ALTER TABLE conversation_turns
    ADD COLUMN seq bigint,
    -- Microseconds since the epoch: when the exchange ended.
    ADD COLUMN ended_at bigint,
    ADD COLUMN last_system bytea;

UPDATE conversation_turns t
SET seq = o.seq, ended_at = t.started_at
FROM (SELECT exchange,
             row_number() OVER (ORDER BY started_at, exchange COLLATE "C") AS seq
      FROM conversation_turns) o
WHERE o.exchange = t.exchange;

UPDATE conversation_turns t
SET last_system = c.last_system
FROM conversations c
WHERE c.id = t.conversation AND t.turn = c.turns - 1;

SELECT setval('turn_seq', greatest((SELECT coalesce(max(seq), 0) FROM conversation_turns), 1));

ALTER TABLE conversation_turns
    ALTER COLUMN seq SET NOT NULL,
    ALTER COLUMN ended_at SET NOT NULL,
    ADD CONSTRAINT conversation_turns_seq UNIQUE (seq);

CREATE INDEX conversation_turns_key
    ON conversation_turns (agent, ended_at, exchange COLLATE "C");
CREATE INDEX conversation_turns_taken ON conversation_turns (agent, seq);

DROP INDEX conversations_agent;

ALTER TABLE conversations
    DROP COLUMN updated,
    -- The last turn's key.
    ADD COLUMN last_ended_at bigint NOT NULL DEFAULT 0,
    ADD COLUMN last_exchange text COLLATE "C" NOT NULL DEFAULT '';

UPDATE conversations c
SET last_ended_at = t.ended_at, last_exchange = t.exchange
FROM conversation_turns t
WHERE t.conversation = c.id AND t.turn = c.turns - 1;

CREATE INDEX conversations_latest ON conversations (agent, last_ended_at, last_exchange);

ALTER TABLE responses
    -- The exchange that filed it; NULL for rows filed before this migration.
    ADD COLUMN exchange text;

CREATE INDEX responses_exchange ON responses (exchange) WHERE exchange IS NOT NULL;

ALTER TABLE seen_messages
    -- The exchange that saw it; '' for rows seen before this migration.
    ADD COLUMN exchange text NOT NULL DEFAULT '';

ALTER TABLE seen_messages DROP CONSTRAINT seen_messages_pkey;
ALTER TABLE seen_messages ADD PRIMARY KEY (agent, message, conversation, exchange);

CREATE INDEX seen_messages_exchange ON seen_messages (exchange);

CREATE TABLE thread_calls (
    exchange text PRIMARY KEY,
    -- The agent the exchange was attributed to.
    agent text NOT NULL,
    -- Microseconds since the epoch: the exchange's start and end.
    started_at bigint NOT NULL,
    ended_at bigint NOT NULL,
    -- The request's messages and their roles, in order.
    request_messages bytea[] NOT NULL,
    request_roles text[] NOT NULL,
    output bytea,
    summary bytea,
    -- The id a conversation the call creates takes.
    conversation text NOT NULL,
    -- The previous response an increment continues, the response it files
    -- and its traffic source, as JSON.
    call text NOT NULL,
    CHECK (cardinality(request_messages) = cardinality(request_roles))
);

CREATE INDEX thread_calls_ended ON thread_calls (agent, ended_at);
