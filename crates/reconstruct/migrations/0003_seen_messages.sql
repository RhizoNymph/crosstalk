-- L3 seen messages: per attributed agent, each message it saw (received in
-- a request or produced as an output) and, per conversation, the latest
-- time it was seen there. A delta's new inputs leave out messages the
-- agent's cluster saw in another conversation within the retention
-- (reconstruct.delta.excludes-seen-elsewhere).

CREATE TABLE seen_messages (
    agent text NOT NULL,
    message bytea NOT NULL,
    conversation text NOT NULL REFERENCES conversations (id),
    -- Microseconds since the epoch: the latest exchange start that saw it.
    seen_at bigint NOT NULL,
    PRIMARY KEY (agent, message, conversation)
);

CREATE INDEX seen_messages_age ON seen_messages (agent, seen_at);
