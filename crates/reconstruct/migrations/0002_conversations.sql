-- L3 conversations: threaded conversations, every message of each in
-- order (system turns included), the outcome of each threaded exchange and
-- the responses an increment can continue.

CREATE SEQUENCE conversation_updates;

CREATE TABLE conversations (
    id text PRIMARY KEY,
    -- The agent its first exchange was attributed to.
    agent text NOT NULL,
    -- ConversationOrigin, as JSON.
    origin text NOT NULL,
    -- Non-system messages in the stored history.
    history_len integer NOT NULL CHECK (history_len >= 0),
    -- The chain hash of the whole non-system history; NULL when empty.
    head bytea,
    -- The system message of the latest exchange, if it had one.
    last_system bytea,
    -- From conversation_updates on every change: the latest is the most
    -- recently threaded conversation.
    updated bigint NOT NULL,
    CHECK ((head IS NULL) = (history_len = 0))
);

CREATE INDEX conversations_head ON conversations (head) WHERE head IS NOT NULL;
CREATE INDEX conversations_agent ON conversations (agent, updated);

CREATE TABLE conversation_entries (
    conversation text NOT NULL REFERENCES conversations (id),
    -- 0, 1, 2, ... over every message of the conversation, in order.
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    message bytea NOT NULL,
    -- system, user, assistant or tool.
    role text NOT NULL,
    -- The exchange that added the message.
    exchange text NOT NULL,
    -- The message's place in the non-system history, and the chain hash of
    -- the history through it; NULL for a system message.
    history_index integer CHECK (history_index >= 0),
    chain bytea,
    -- The exchange's output (its response, or a failed one's partial).
    output boolean NOT NULL,
    PRIMARY KEY (conversation, ordinal),
    UNIQUE (conversation, history_index),
    CHECK ((history_index IS NULL) = (chain IS NULL)),
    CHECK ((history_index IS NULL) = (role = 'system'))
);

CREATE INDEX conversation_entries_chain ON conversation_entries (chain) WHERE chain IS NOT NULL;
CREATE INDEX conversation_entries_message ON conversation_entries (message);

CREATE TABLE thread_records (
    exchange text PRIMARY KEY,
    conversation text NOT NULL REFERENCES conversations (id),
    -- The ThreadOutcome, as JSON.
    outcome text NOT NULL
);

CREATE TABLE responses (
    upstream text NOT NULL,
    -- IdentityScope's wire JSON.
    scope text NOT NULL,
    response text NOT NULL,
    conversation text NOT NULL REFERENCES conversations (id),
    -- Non-system history length through the response.
    history_len integer NOT NULL CHECK (history_len > 0),
    PRIMARY KEY (upstream, scope, response)
);
