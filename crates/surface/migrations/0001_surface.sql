-- L8 surface stores (schema "surface"): the audit log and its write-ahead
-- intents, the operator directory and the configured alert sinks.
--
-- Ids are ULID text in COLLATE "C" columns (byte order is id order), times
-- are microseconds since the epoch, and spec values are their wire JSON in
-- text. Every runtime query names the schema.

-- The audit log. Append-only: the trigger below refuses every UPDATE and
-- DELETE (surface.audit.append-only), whatever the role's grants.
CREATE TABLE audit (
    seq      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id       text COLLATE "C" NOT NULL UNIQUE CHECK (length(id) = 26),
    at       bigint NOT NULL CHECK (at >= 0),
    kind     text NOT NULL CHECK (kind IN ('operator', 'config', 'export')),
    -- The author (AuditEntry::by): the operator's id, NULL for config.
    by_operator text COLLATE "C" CHECK (length(by_operator) = 26),
    entry    text NOT NULL,                 -- AuditEntry wire JSON
    CHECK ((kind = 'config') = (by_operator IS NULL))
);
-- AuditLog::query: newest first by (at, id).
CREATE INDEX audit_by_at ON audit (at DESC, id DESC);
CREATE INDEX audit_by_operator ON audit (by_operator, at DESC, id DESC) WHERE by_operator IS NOT NULL;

-- AuditEntry::subjects, for the subject filter.
CREATE TABLE audit_subjects (
    audit_seq bigint NOT NULL REFERENCES audit (seq),
    subject   text NOT NULL,                -- AuditSubject wire JSON
    PRIMARY KEY (subject, audit_seq)
);

CREATE FUNCTION refuse_audit_change() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'the audit log is append-only: % on %.% refused',
        TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME
        USING ERRCODE = 'insufficient_privilege';
END;
$$;

CREATE TRIGGER audit_append_only
    BEFORE UPDATE OR DELETE ON audit
    FOR EACH ROW EXECUTE FUNCTION refuse_audit_change();
CREATE TRIGGER audit_subjects_append_only
    BEFORE UPDATE OR DELETE ON audit_subjects
    FOR EACH ROW EXECUTE FUNCTION refuse_audit_change();

-- Write-ahead records of operator action calls in progress (decision Q3,
-- surface.audit.no-silent-effect): inserted before the effect, deleted in
-- the transaction that appends the call's entry, or turned into an
-- Interrupted entry at start.
CREATE TABLE action_intents (
    id     text COLLATE "C" PRIMARY KEY CHECK (length(id) = 26),
    at     bigint NOT NULL CHECK (at >= 0),
    intent text NOT NULL                    -- AuditIntent wire JSON
);
CREATE INDEX action_intents_oldest ON action_intents (at, id);

-- The one stored operator directory: its mode and every operator, current
-- and former (a former one has no permissions).
CREATE TABLE operator_directory (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    directory text NOT NULL                 -- {"mode", "operators": [Operator]} JSON
);

-- The sinks config defines, each with its last delivery.
CREATE TABLE sinks (
    id   text COLLATE "C" PRIMARY KEY CHECK (length(id) = 26),
    info text NOT NULL                      -- SinkInfo wire JSON
);
