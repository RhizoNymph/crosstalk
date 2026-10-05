-- L6 alert store (crosstalk_analysis::alerts): rules, alerts and triage's
-- copy of each transmission's current verdict, in one schema so every
-- AlertRuleStore, AlertTriage, AlertRuleMaintenance and AlertActions call
-- is one transaction over all three.

CREATE TABLE alert_rules (
    id text COLLATE "C" PRIMARY KEY,
    -- The built-in rule's index in BuiltinRule::ALL, NULL for a user rule:
    -- the rules list sorts on it.
    builtin smallint UNIQUE CHECK (builtin BETWEEN 0 AND 4),
    -- AlertRuleDef's wire JSON (decoded through its checked constructors).
    definition text NOT NULL,
    -- RuleRevision: 1 when created, one more per stored change.
    revision integer NOT NULL CHECK (revision > 0)
);

-- What the alerts consumer last made current (one row): the topic-model
-- version a watched-topic rule must name and its topics, and the embedding
-- model it last saw.
CREATE TABLE alert_rule_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    topic_version bigint NOT NULL CHECK (topic_version >= 0),
    -- The version's TopicIds' wire JSON (an array).
    topics text NOT NULL,
    -- EmbeddingModel's wire JSON.
    model text NOT NULL
);

CREATE TABLE alerts (
    id text COLLATE "C" PRIMARY KEY,
    rule text COLLATE "C" NOT NULL REFERENCES alert_rules (id),
    -- AlertSubject's wire JSON: the dedup key with `rule`.
    subject text NOT NULL,
    -- The subject's kind (channel, transmission, agent) and id, for the
    -- suppressions and the channel filter.
    subject_kind text NOT NULL CHECK (subject_kind IN ('channel', 'transmission', 'agent')),
    subject_id text COLLATE "C" NOT NULL,
    -- AlertStateKind's wire text.
    state text NOT NULL CHECK (state IN ('open', 'acknowledged', 'resolved', 'suppressed')),
    -- Alert's wire JSON.
    alert text NOT NULL,
    -- AlertRevision: 1 when opened, one more per stored change.
    revision integer NOT NULL CHECK (revision > 0)
);

-- At most one active (open or acknowledged) alert per (rule, subject),
-- however drafts are triaged concurrently.
CREATE UNIQUE INDEX alerts_one_active_per_key ON alerts (rule, subject)
    WHERE state IN ('open', 'acknowledged');
CREATE INDEX alerts_active_subject ON alerts (subject_kind, subject_id)
    WHERE state IN ('open', 'acknowledged');
CREATE INDEX alerts_active_rule ON alerts (rule)
    WHERE state IN ('open', 'acknowledged');

-- Triage's copy of each transmission's current verdict (CurrentVerdict).
CREATE TABLE alert_verdicts (
    transmission text COLLATE "C" PRIMARY KEY,
    -- Verdict's wire JSON, NULL for a withdrawal.
    verdict text,
    revision integer NOT NULL CHECK (revision > 0)
);
