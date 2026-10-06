-- L3 conversation reads (ConversationReads): each conversation's traffic
-- source, origin kind and source conversation, first and last turn times;
-- each transcript entry's carried-over flag; and one row per turn, so a
-- window of turns reads without the whole transcript.

ALTER TABLE conversations
    -- TrafficSource, as JSON; replay_corpus is its corpus (NULL when live).
    ADD COLUMN source text NOT NULL DEFAULT '{"type":"live"}',
    ADD COLUMN replay_corpus text,
    -- ConversationOrigin's kind, and the parent or predecessor it names.
    ADD COLUMN origin_kind text NOT NULL DEFAULT 'root'
        CHECK (origin_kind IN ('root', 'fork', 'compaction')),
    ADD COLUMN origin_of text,
    -- Microseconds since the epoch: the first and last turn's start.
    ADD COLUMN started_at bigint NOT NULL DEFAULT 0,
    ADD COLUMN last_turn_at bigint NOT NULL DEFAULT 0,
    -- Turns threaded.
    ADD COLUMN turns integer NOT NULL DEFAULT 0 CHECK (turns >= 0);

UPDATE conversations
SET origin_kind = origin::jsonb->>'type',
    origin_of = coalesce(origin::jsonb->'data'->>'parent', origin::jsonb->'data'->>'predecessor');

ALTER TABLE conversations
    ADD CONSTRAINT conversations_origin_of CHECK ((origin_kind = 'root') = (origin_of IS NULL));

CREATE INDEX conversations_successors ON conversations (origin_of) WHERE origin_of IS NOT NULL;

ALTER TABLE conversation_entries
    -- A compaction's turn 0: the hash is in the predecessor's history.
    ADD COLUMN carried_over boolean NOT NULL DEFAULT false;

CREATE TABLE conversation_turns (
    conversation  text NOT NULL REFERENCES conversations (id),
    -- 0, 1, 2, ... in threading order.
    turn          integer NOT NULL CHECK (turn >= 0),
    -- An exchange is threaded once (thread_records), so it is one turn.
    exchange      text NOT NULL UNIQUE,
    -- Its entries: ordinals first_ordinal .. first_ordinal + entries.
    first_ordinal integer NOT NULL CHECK (first_ordinal >= 0),
    entries       integer NOT NULL CHECK (entries >= 0),
    -- The agent the exchange was attributed to.
    agent         text NOT NULL,
    -- Microseconds since the epoch: the exchange's start.
    started_at    bigint NOT NULL,
    outcome       text NOT NULL CHECK (outcome IN ('starts', 'extends', 'forks', 'compacts')),
    -- Non-system history length once the turn was threaded.
    history_end   integer NOT NULL CHECK (history_end >= 0),
    PRIMARY KEY (conversation, turn)
);

-- Conversations threaded before this migration: their turns from the
-- thread records, ordered by where each exchange's entries start (an
-- exchange that appended nothing goes after them, by id); times unknown (0).
INSERT INTO conversation_turns
    (conversation, turn, exchange, first_ordinal, entries, agent, started_at, outcome, history_end)
SELECT r.conversation,
       (row_number() OVER (PARTITION BY r.conversation
                           ORDER BY coalesce(f.first, 2147483647), r.exchange) - 1)::integer,
       r.exchange,
       coalesce(f.first, (SELECT count(*) FROM conversation_entries e
                          WHERE e.conversation = r.conversation))::integer,
       coalesce(f.n, 0)::integer,
       r.outcome::jsonb->'data'->'delta'->>'agent',
       0,
       r.outcome::jsonb->>'type',
       (SELECT count(*) FROM conversation_entries e
        WHERE e.conversation = r.conversation AND e.history_index IS NOT NULL
          AND e.ordinal < coalesce(f.first + f.n, 2147483647))::integer
FROM thread_records r
LEFT JOIN (SELECT conversation, exchange, min(ordinal) AS first, count(*) AS n
           FROM conversation_entries GROUP BY conversation, exchange) f
    ON f.conversation = r.conversation AND f.exchange = r.exchange;

UPDATE conversations c
SET turns = (SELECT count(*) FROM conversation_turns t WHERE t.conversation = c.id);

-- Compactions threaded before this migration: turn 0's request messages
-- whose hash is in the predecessor's stored history.
UPDATE conversation_entries e
SET carried_over = true
FROM conversations c, conversation_turns t
WHERE c.id = e.conversation
  AND c.origin_kind = 'compaction'
  AND t.conversation = c.id AND t.turn = 0
  AND e.ordinal >= t.first_ordinal AND e.ordinal < t.first_ordinal + t.entries
  AND e.history_index IS NOT NULL AND NOT e.output
  AND EXISTS (SELECT 1 FROM conversation_entries p
              WHERE p.conversation = c.origin_of AND p.message = e.message
                AND p.history_index IS NOT NULL);
