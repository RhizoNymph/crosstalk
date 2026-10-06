-- The extraction step's ledger (crates/flow/src/extract/step, the Postgres
-- side in crates/flow/src/store/ledger.rs): what the step remembers between
-- deltas. One delta's changes and its extract_done row commit in one
-- transaction. Times are microseconds; ids ULID text; a tool call is the
-- canonical encoding of an assistant message holding just that call.

-- Each agent's context in each conversation (ConversationContext, JSON),
-- stamped with the start of the exchange that last set it.
CREATE TABLE extract_contexts (
    agent        TEXT COLLATE "C" NOT NULL CHECK (length(agent) = 26),
    conversation TEXT COLLATE "C" NOT NULL CHECK (length(conversation) = 26),
    context      TEXT NOT NULL,
    updated_at   BIGINT NOT NULL,
    PRIMARY KEY (agent, conversation)
);

CREATE INDEX extract_contexts_age ON extract_contexts (updated_at);

-- The order calls with one (agent, call id) were made in.
CREATE SEQUENCE extract_pending_order;

-- Calls made in an output, waiting for their result: one per conversation
-- that made a call with that id, listed by made_seq. `context` is the
-- context the call was extracted in; `writes` its held writes,
-- [(Locator, AccessId)] JSON.
CREATE TABLE extract_pending (
    agent        TEXT COLLATE "C" NOT NULL CHECK (length(agent) = 26),
    call_id      TEXT NOT NULL,
    conversation TEXT COLLATE "C" NOT NULL CHECK (length(conversation) = 26),
    made_seq     BIGINT NOT NULL DEFAULT nextval('extract_pending_order'),
    context      TEXT NOT NULL,
    call         TEXT NOT NULL,
    writes       TEXT NOT NULL,
    PRIMARY KEY (agent, call_id, conversation)
);

CREATE INDEX extract_pending_listing ON extract_pending (agent, call_id, made_seq);

-- Calls of known tools seen only among a conversation's new inputs.
CREATE TABLE extract_history (
    conversation TEXT COLLATE "C" NOT NULL CHECK (length(conversation) = 26),
    call_id      TEXT NOT NULL,
    call         TEXT NOT NULL,
    PRIMARY KEY (conversation, call_id)
);

-- The results each agent was delivered (a digest of call and result),
-- stamped with the latest delivery's exchange start.
CREATE TABLE extract_delivered (
    agent TEXT COLLATE "C" NOT NULL CHECK (length(agent) = 26),
    key   BYTEA NOT NULL CHECK (length(key) = 32),
    at    BIGINT NOT NULL,
    PRIMARY KEY (agent, key)
);

CREATE INDEX extract_delivered_age ON extract_delivered (at);

-- The deltas (by exchange) whose extraction committed.
CREATE TABLE extract_done (
    exchange TEXT COLLATE "C" PRIMARY KEY CHECK (length(exchange) = 26),
    at       BIGINT NOT NULL
);

CREATE INDEX extract_done_age ON extract_done (at);
