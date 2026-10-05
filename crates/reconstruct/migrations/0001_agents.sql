-- L3 agents: the agent table, each agent's evidence, the merge log, merge
-- vetoes, harness claims and activity per attributed agent, and the outbox
-- the agent store publishes its events from. Runs in schema "reconstruct"
-- (crosstalk_store::migrate); runtime queries qualify every table.
--
-- Ids are ULID text, spec values with a wire form are their wire JSON
-- (text), times are microseconds since the epoch.

CREATE TABLE agents (
    id text PRIMARY KEY,
    -- The spawning agent as stored; it need not be stored itself.
    parent text,
    -- AgentState's wire JSON.
    state text NOT NULL,
    -- MergedInto::into while merged, else NULL: the merge table the
    -- directory and every read resolve through. Written with `state`.
    merged_into text REFERENCES agents (id),
    -- AgentLabel's text.
    label text,
    CHECK (merged_into IS NULL OR merged_into <> id)
);

CREATE INDEX agents_merged_into ON agents (merged_into) WHERE merged_into IS NOT NULL;
CREATE INDEX agents_parent ON agents (parent) WHERE parent IS NOT NULL;

CREATE TABLE agent_evidence (
    agent text NOT NULL REFERENCES agents (id),
    -- Order the evidence was attached in.
    position integer NOT NULL,
    -- IdentityEvidence's wire JSON: equal evidence has equal text, so this
    -- is the lookup key `resolve` matches on. Not unique per agent: a new
    -- agent keeps the evidence it was created with as given, and
    -- attach_evidence refuses a repeat itself.
    item text NOT NULL,
    -- The evidence variant's tag (`harness_agent`, `harness_session`, ...).
    kind text NOT NULL,
    PRIMARY KEY (agent, position)
);

CREATE INDEX agent_evidence_item ON agent_evidence (item);

CREATE TABLE merges (
    id text PRIMARY KEY,
    source text NOT NULL REFERENCES agents (id),
    target text NOT NULL REFERENCES agents (id),
    -- MergeRecord's wire JSON, its reversal included once reverted.
    record text NOT NULL,
    reverted boolean NOT NULL,
    CHECK (source <> target)
);

-- At most one unreverted record names an agent as its source.
CREATE UNIQUE INDEX merges_one_open_per_source ON merges (source) WHERE NOT reverted;

CREATE TABLE vetoes (
    a text NOT NULL,
    b text NOT NULL,
    -- MergeVeto's wire JSON.
    veto text NOT NULL,
    PRIMARY KEY (a, b),
    CHECK (a < b)
);

-- Claims and activity are kept per attributed agent, whatever it merges
-- into later, and may name an agent the store does not hold yet.
CREATE TABLE claims (
    agent text NOT NULL,
    -- HarnessClaim's wire JSON.
    claim text NOT NULL,
    last_seen bigint NOT NULL,
    PRIMARY KEY (agent, claim)
);

CREATE TABLE activity (
    agent text PRIMARY KEY,
    last_seen bigint NOT NULL
);

-- Events a committed write publishes, until the sink took them.
CREATE TABLE outbox (
    seq bigserial PRIMARY KEY,
    -- BusEvent's wire JSON.
    event text NOT NULL
);
