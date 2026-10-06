-- Restart durability for L5 (docs/features/postgres_stores.md, "L5: flow
-- checkpoint and restore"): stable outbox envelope ids, the order accesses
-- and tool calls were recorded in, writes held for their result, and the
-- correlator shards' checkpoints.

-- Stable envelope ids (flow.outbox.stable-envelope-id, INV-1212): the relay
-- stamps each row once, in a committed transaction, before its first
-- publish, and every publish of the row carries that stamp.
ALTER TABLE outbox ADD COLUMN envelope_id TEXT COLLATE "C" CHECK (length(envelope_id) = 26),
                   ADD COLUMN at BIGINT,
                   ADD CONSTRAINT outbox_stamped CHECK ((envelope_id IS NULL) = (at IS NULL));

-- The order the flow consumer recorded its inputs in: accesses and tool
-- calls share it, so a restore re-feeds exactly the inputs its checkpoint
-- has not seen, in the order they were first taken. Rows recorded before
-- this migration predate every checkpoint and get their number here.
CREATE SEQUENCE recording;

ALTER TABLE accesses ADD COLUMN recorded_seq BIGINT NOT NULL UNIQUE DEFAULT nextval('recording');

-- The channel the flow consumer resolved an access's resource to when it
-- recorded it (`resolved`, with `resolved_channel` NULL for no channel),
-- so a restore re-feeds the access into the medium it was first
-- correlated in, whatever channel the resource is on since. Written by
-- the consumer right after the access; `resolved` false until then.
ALTER TABLE accesses ADD COLUMN resolved BOOLEAN NOT NULL DEFAULT false,
                     ADD COLUMN resolved_channel TEXT COLLATE "C" CHECK (length(resolved_channel) = 26),
                     ADD CONSTRAINT accesses_resolved CHECK (resolved OR resolved_channel IS NULL);

-- Every tool call the extraction step handed the consumer: the name a
-- Direct(ToolResult) transmission for its result carries. Rows a
-- checkpoint covers are deleted with it.
CREATE TABLE tool_calls (
    recorded_seq BIGINT PRIMARY KEY DEFAULT nextval('recording'),
    agent        TEXT COLLATE "C" NOT NULL CHECK (length(agent) = 26),
    call_id      TEXT NOT NULL,
    name         TEXT NOT NULL,
    at           BIGINT NOT NULL
);

-- Writes held until their outcome is final (Extracted::Write with no
-- outcome): inserted when held, deleted once the released write's access
-- is recorded (its result, or the settle tick).
CREATE TABLE held_writes (
    access_id  TEXT COLLATE "C" PRIMARY KEY CHECK (length(access_id) = 26),
    settles_at BIGINT NOT NULL,
    write      TEXT NOT NULL              -- Observed<WriteCall>, JSON
);

CREATE INDEX held_writes_settling ON held_writes (settles_at);

-- One snapshot per correlator shard, written in the transaction that
-- writes its shard_ticks row (flow.checkpoint.ticks-with-state, INV-1216).
-- `format` is the snapshot encoding's version; `recorded_through` the last
-- recording number the snapshot covers.
CREATE TABLE checkpoints (
    shard            INTEGER PRIMARY KEY CHECK (shard >= 0),
    format           INTEGER NOT NULL CHECK (format > 0),
    shards           INTEGER NOT NULL CHECK (shards > shard),
    ticked_through   BIGINT,
    recorded_through BIGINT NOT NULL CHECK (recorded_through >= 0),
    taken_at         BIGINT NOT NULL,
    state            BYTEA NOT NULL
);
