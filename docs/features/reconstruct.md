# Reconstruct (L3)

`crosstalk-reconstruct` (`crates/reconstruct`), roadmap item P4.1: who sent
each captured exchange, and which conversation it continues. It
implements `crosstalk_spec::interfaces::l3_reconstruction` and is a layer
crate: it depends on `crosstalk-spec` and `crosstalk-store` only;
`crosstalk-memory`, `crosstalk-testkit`, `crosstalk-sim` and
`crosstalk-transport` are dev-dependencies.

## Scope

- Evidence derivation (`EvidenceDeriver`): `ApiKeyEvidence` (account,
  credential by stability), `HeaderEvidence` (harness agent and session
  ids, scoped), `PromptFingerprintEvidence` (system prompt plus first user
  turn), `ChainEvidence` (all of them, most specific first), and the one
  place an exchange's identity scope and caller evidence are decided
  (`evidence::scope`).
- `PgAgents`: every L3 agent store trait on Postgres (`AgentDirectory`,
  `IdentityResolver`, `AgentLifecycle`, `ClaimStore`, `ActivityStore`,
  `AgentReads`), with its migrations, outbox relay (stable envelope ids,
  INV-1211) and directory cache.
- Threading (`Threader`): `ConversationThreader` over a
  `ConversationStore` (`MemoryConversations`, `PgConversations`): prefix
  matching, forks and retries, compaction, WebSocket increment resolution,
  system turns anywhere in a request, rethreading idempotence, and the
  per-agent seen-message set that keeps replayed history out of a delta's
  new inputs (`ThreadConfig`).
  `MemoryConversations` also implements the spec's `ExchangePlacements`
  (an exchange's agent and conversation, as its recorded outcome placed
  it; `reconstruct.placement.as-threaded`), which `Live` exposes for
  eval. `PgConversations` implements it too, from `thread_records`.
- List cursor keys derived from the deployment secret
  (`ids::cursor_key`, `KeyedHasher::derive_key` under
  `crosstalk.cursor.v1.agents` and `crosstalk.cursor.v1.conversations`;
  `PgAgents::open_with_secret`, `with_cursor_secret` on both conversation
  stores), so list cursors survive a restart (decision Q4 of
  [postgres_stores](postgres_stores.md)).
- The L3 bus consumer (`consumer`): `ExchangeCaptured` in; attribution,
  claims and activity, threading; `ConversationDelta` out. `AgentSeen` is
  the agent store's: staged in the outbox by the `create` (from traffic)
  or `attach_evidence` that attributes the evidence.

## Non-scope

- Wiring: the consumer is not added to `crosstalk_gateway::pipeline` here.
  A later step subscribes `consumer::subjects()` in `consumer::group()`
  and spawns `consumer::run`.
- A `ConversationReads` surface: conversations are stored in order (every
  message under an ordinal, system turns included, `transcript`) so one
  can page them later without threading again.
- Replay corpora (`IngressMode::Replay`): not on this base. See
  [Replay isolation](#replay-isolation).
- Multi-node coherence of the directory cache beyond `apply_event`.

## Data and control flow

```text
ExchangeCaptured(exchange) ── consumer::ReconstructConsumer::handle
  1. opening = request messages through the first user message (blob store, MessageReader)
  2. evidence = ChainEvidence.derive(meta, opening)       (none: Unattributed, ack)
  3. attribute: IdentityResolver::resolve(evidence)
       New        → mint agent id at started_at, derive_parent, AgentLifecycle::create (Provisional,
                    activity in the same write); every item is newly seen
       Known      → attach the evidence the agent lacks; Registered → FirstTraffic;
                    Provisional with two variants (one strong) → Establish
       Conflict   → strong deciding evidence: Resolver merges of every other candidate into the
                    lowest id, then Known; weak evidence or a refused merge (veto): Review (no delta)
  4. ActivityStore::record, ClaimStore::record (the harness claim, never evidence)
  5. Threader::thread(exchange, attributed agent)
  6. publish ConversationDelta under an envelope id derived from the exchange id
     (ids::derived_event_id): a redelivery republishes the same envelope. AgentSeen per newly
     attributed item was staged by step 3's create / attach_evidence in its own transaction
     and leaves through the store's outbox relay
run(subscription, consumer): ack on success, nack (200 ms) on failure → bus retries / dead letters
```

### Identity

- Scope (`evidence::scope::scope_of`): the account, else a stable
  credential, else the upstream; during a rotation overlap also the
  previous digest's scope. Caller evidence (`caller_evidence`): accounts
  and credentials by stability (stable, rotating; shared and missing give
  none), current and previous digests.
- `resolve` (Postgres, `agents::resolve`): one read-only snapshot; only
  the most specific items decide; an agent holds an item when one of its
  evidence rows has the item's exact wire JSON (so harness ids under
  different scopes never meet); a session counts only for agents with no
  `HarnessAgent` evidence; holders resolve through `agents.merged_into`.
- Parents (`consumer::derive_parent`): the holder of the harness parent
  agent id in the exchange's scope; else, for a sub-agent (harness agent id
  or `Subagent` class), the session's main agent; else none. Parents are
  always agents stored before the child, so links never cycle.

### The agent store on Postgres

Tables in schema `reconstruct` (`migrations/0001_agents.sql`): `agents`
(state as wire JSON, `merged_into` mirrored for queries), `agent_evidence`
(one row per item, the wire JSON as lookup key, its variant tag),
`merges` (record JSON, one open record per source), `vetoes`, `claims`
(per attributed agent and claim, latest time by `GREATEST`), `activity`,
`outbox` (with `envelope_id` and `at` since `0005_outbox_ids.sql`, both
or neither: `outbox_stamped`).

- Merges and unmerges run `SERIALIZABLE` (`retry_serializable`): the
  agent table, merge log and vetoes are read in one statement into an
  `agents::table::Table`, the decision is made there (the merge log's
  procedure from the spec), and exactly what its `Diff` names is written,
  with the events appended to `outbox` in the same transaction.
- Lifecycle writes (`create`, `advance`, `attach_evidence`) are targeted
  `SERIALIZABLE` transactions; `create` from traffic and `FirstTraffic`
  record activity in the same transaction.
- After commit: the directory cache is repointed, then the write's outbox
  rows are relayed (`agents::outbox::relay`, INV-1211):
  1. a transaction of its own takes the rows (`FOR UPDATE SKIP LOCKED`, in
     `seq` order), stamps each row without an envelope id with
     `EventSink::stamp` (an id and time from the injected clock and a ULID
     generator in `BusSink`), and commits;
  2. each row is published as an `Envelope` under its stamp, in `seq`
     order, awaiting `EventSink::publish` (`BusSink`: `EventBus::publish`);
  3. the published rows are deleted.
  A failure or a stop anywhere leaves the rows, stamped once step 1
  committed; `flush_outbox` (run at start, before the stage subscribes)
  relays every row left, in batches of 256, under the ids they carry. A
  bus idempotent on ids (`PgBus`) therefore holds each event once; an
  uncommitted row is never seen by a relay.
- Reads (`list`, `cluster`, `names`) take one snapshot statement of every
  table (JSON aggregates, one round trip) and build the read models with
  the spec's checked constructors. List cursors are `<last id>_<tag>`, the
  tag a keyed BLAKE3 of the store's cursor key, the filter JSON and the id.
- `canonical` reads the in-process directory cache, loaded at `open` and
  updated before any event of a committed merge or unmerge is published;
  another node's merges are folded in with `apply_event`.

### Threading

A request's messages are read from the blob store (`MessageReader`, a
bounded per-hash cache of each body's role and whether it is a summary
turn). System messages, wherever they are, are left out of the history;
the request's system message for `new_system` is its last one. The
non-system history is hashed as a chain (`chain(k) = BLAKE3(chain(k-1) ||
hash_k)`), and every stored conversation keeps the chain of each history
position and of its whole history (its head), so all prefix lookups are
equality lookups.

The decision (`thread::plan`), inside the store's atomic step:

1. An exchange already threaded returns its recorded outcome.
2. An increment whose previous response is filed under the exchange's
   upstream and scope, in a cluster conversation, becomes that history
   through the response plus the increment; otherwise it `Starts` holding
   only the increment.
3. `Extends` the cluster conversation with the longest stored history that
   is a prefix of the request (ties: most recently threaded).
4. `Compacts` when the request carries content evidence: a summary turn
   (a user message opening with a known harness preamble, or, with the
   `Compaction` hint, mentioning a summary) that no cluster conversation
   holds yet, or a stored output echoed as its first message. The
   predecessor is the most recently threaded cluster conversation holding
   any of the request's other messages, else the most recently threaded
   one. New inputs exclude messages in the predecessor's history.
5. `Forks` the cluster conversation sharing the longest common prefix that
   holds an assistant message.
6. Otherwise `Starts`.

Whatever the outcome, the delta's new inputs then leave out every message
an agent of the cluster saw in another conversation within the seen-message
retention (`reconstruct.delta.excludes-seen-elsewhere`, INV-1100), in
order. This is what keeps a restarted episode carrying its earlier
transcript (SALT's `main` and `memory_length` conditions; eval finding 6),
a fork's unchanged tail after a rewritten message, a re-run opening turn
and the agent's own earlier outputs pasted back as history from being
scanned by L4 as freshly received. Within one conversation the rule stays
positional: a message repeated there is new again. The delta's output is
never withheld.

### The seen-message set

- What counts as seen in a conversation: every non-system message a write
  stores there from the request, and the output. For a new conversation
  that is its whole request (a fork's base included: the request carried
  it); for an extension, the appended messages. It is recorded under the
  attributed agent with the exchange's start (`ThreadInput::at`), keeping
  the latest time per (agent, message, conversation); a lookup covers every
  member of the cluster (`ThreadReads::seen_elsewhere`), so merged agents
  share the set and different agents never do. Under replay each agent
  belongs to one corpus (INV-973), so the set is per corpus as well.
- Retention (`ThreadConfig::seen_retention`, a checked `SeenRetention`,
  30 days by default, L4's index retention; JSON
  `{"seen_retention_secs": 2592000}`): a sighting counts while it is no
  older than the retention before the exchange's start. The time is when
  the message was stored in that conversation; later turns re-sending it as
  history do not refresh it (that would cost an upsert per history message
  per call).
- Bounding: `MemoryConversations` forgets, after each write, every sighting
  older than the retention behind the newest exchange it has threaded (an
  ordered index makes this a pop from the front). `PgConversations`
  deletes the threaded cluster's expired rows in the threading transaction
  and offers `forget_seen(now)` to sweep agents that stopped calling. A
  store may therefore forget a sighting an exchange arriving more than the
  retention out of order would still have counted.
- Configuration: `MemoryConversations::with_config`,
  `PgConversations::with_config`; the gateway's `LiveConfig::threading`.

The store then records the conversation (`Write`): new conversations,
fork bases copied from the parent's transcript through the shared prefix,
appended entries with ordinals, the outcome per exchange, the response
for later increments, and the write's seen messages. `MemoryConversations` does this under one
`tokio::sync::Mutex`; `PgConversations` in one `SERIALIZABLE` transaction
(`migrations/0002_conversations.sql`; the seen set in
`migrations/0003_seen_messages.sql`, table `seen_messages` keyed by agent,
message and conversation, indexed by agent and time).

### Replay isolation

Exchanges replayed from datasets are the demo's main input. Scoping lives
in one place, `evidence::scope` (`scope_of`, `caller_evidence`), and
increment resolution files responses under `scope_of(..).current`. When
`IngressMode::Replay { corpus }` lands, a replayed exchange's credential
and account digests are re-keyed by the corpus there (a corpus-tagged
digest), and its upstream scope gains the corpus, so a replayed credential
never meets a live agent or another corpus's agents; the resolver and the
store need no change. The AI Village fixture replays with a synthetic,
stable per-corpus API key.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `crates/reconstruct/src/lib.rs` | Crate root | modules |
| `src/evidence/mod.rs` | Evidence derivers | `ApiKeyEvidence`, `HeaderEvidence`, `PromptFingerprintEvidence`, `ChainEvidence`, `parent_agent_evidence`, `session_evidence` |
| `src/evidence/scope.rs` | Identity scope and caller evidence | `CallerScope`, `scope_of`, `caller_evidence` |
| `src/ids.rs` | Id sources, derived envelope ids, cursor keys from the deployment secret | `IdSource`, `UlidSource`, `derived_event_id`, `cursor_key`, `AGENTS_CURSOR_LABEL`, `CONVERSATIONS_CURSOR_LABEL` |
| `src/publish.rs` | Event sinks: stamp, then publish awaited | `EventSink` (`stamp`, `publish`), `Stamp`, `BusSink`, `SinkError` |
| `src/error.rs` | Storage failures to spec errors | `StorageFailure`, `StoreReason` |
| `src/agents/mod.rs` | The Postgres agent store | `PgAgents` (`open`, `open_with_secret`, `with_retry`, `reload_directory`, `apply_event`, `members`, `flush_outbox`), `MIGRATIONS`, `run_migrations` |
| `src/agents/table.rs` | Merge-log decisions and read models over loaded rows | `Table`, `Diff`, `Applied` (crate) |
| `src/agents/{load,writes,resolve,reads,cache,codec}.rs` | Snapshot loading; writes; `resolve`; `AgentReads`; directory cache; column codecs | — |
| `src/agents/outbox.rs` | Staging and the relay (stamp, publish, delete) | `stage`, `relay`, `Rows` (crate) |
| `src/thread/mod.rs` | The threader | `ConversationThreader`, `ClusterMembers`, `ReadsMembers`, `outcome_kind` |
| `src/thread/{history,plan,store}.rs` | Chain hashes and request analysis; the decision (including the seen-elsewhere filter); the store trait and its input | `Entry`, `ChainHash`, `ConversationStore`, `ThreadInput` (with `at`), `RequestKind`, `ResponseKey`, `TranscriptEntry` |
| `src/thread/config.rs` | Typed threading configuration | `ThreadConfig`, `SeenRetention`, `ThreadConfigError`, `DEFAULT_SEEN_RETENTION` |
| `src/thread/{memory,pg}.rs` | Conversation stores, each with its seen-message set | `MemoryConversations` (`new`, `with_config`, `with_cursor_key`, `with_cursor_secret`), `PgConversations` (`new`, `with_retry`, `with_config`, `with_cursor_key`, `with_cursor_secret`, `forget_seen`); both `ExchangePlacements` |
| `src/thread/messages.rs` | Message bodies and facts | `MessageReader`, `Facts`, `DEFAULT_SUMMARY_PREAMBLES` |
| `src/consumer/{mod,attribute}.rs` | The bus consumer | `ReconstructConsumer`, `ConsumerParts`, `Handled`, `ConsumeError`, `run`, `group`, `subjects`, `GROUP`, `attribute`, `derive_parent`, `corroborated` |
| `crates/reconstruct/migrations/000{1,2,3,4,5}_*.sql` | Schema `reconstruct`: agents, conversations, seen messages, conversation reads, outbox stamps | — |
| `src/tests/` | Tests by area (paths below) | — |
| `src/tests/refresh.rs` | A rotating (OAuth) credential refreshed inside one harness session keeps the agent and its conversation; two sessions on one token are two agents (INV-1157, [claude_code_oauth](claude_code_oauth.md)) | — |

## Tests

- `tests::threading`: outcomes, origins, failures, increments, compaction
  evidence, system turns, and the fixture regressions (interleaved re-runs,
  a rewritten early tool result forking at the rewrite, a resumed session
  and a compact boundary).
- `tests::seen`: the seen-message set on both stores (Postgres gated):
  a restart replaying earlier history, a later replayed message, separate
  agents, a repeat inside one conversation, a replayed own output, a merged
  cluster, expiry and the Postgres sweep, and the config's checks.
- `tests::thread_props`: generated harness scripts (continuations,
  retries, branches, rewrites, system changes and mid-array system turns,
  failures, compactions, a two-agent cluster) with an oracle checking every
  step; metamorphic properties for increments, scopes, system changes and
  compactions.
- `tests::evidence`, `tests::consumer`, `tests::consumer_props`,
  `tests::table`: derivation, attribution, parents, conflicts, merges and
  conversations, the store's decisions without a database.
- `tests::dst`: `crosstalk-sim` schedules of duplicate and reordered
  deliveries, a secret rotation, and concurrent threaders; and
  `redelivery_republishes_the_same_envelope_ids` (INV-1202): publishes
  failing at seeded points, the delivery redelivered until handled; the
  same deliveries with every publish landing give exactly the same bus log
  (each id with its one event) and exactly the same store events,
  `AgentSeen` included.
- `tests::cursor_keys`: cursor keys stable per secret and label; a
  conversation list cursor resolves on a store handle keyed from the same
  secret and is refused under another.
- `tests::pg_outbox` (gated): the relay's crash points (failure before and
  after publishing, a dropped relay, a store opened anew) publish each
  staged event once under its stamped id (INV-1211); an uncommitted row is
  never relayed; stamps increase; an agents list cursor keyed from the
  secret resolves after a reopen and is refused under another secret.
- `tests::pg_placement` (gated): `ExchangePlacements` on `PgConversations`
  against `MemoryConversations` on generated scripts, every third exchange
  unthreaded.
- `tests::pg_agents`, `tests::pg_props`, `tests::pg_threads` (gated on
  `TEST_DATABASE_URL`, at most three databases at a time): the model test
  against `crosstalk-memory`'s reference (`check_agent_store_with`),
  integration tests of the merge log, resolution properties, and the
  Postgres conversation store against the in-memory one.
- `tests::fixtures` (`#[ignore]`, local data): AI Village's Claude Code
  stream replayed through the consumer (68 561 calls: 1 start, 940
  compactions, every other call an extension, no fork), and lmcache's
  interleaved WildClaw re-runs (2 starts, 31 extensions, 1 fork).

## Invariants and constraints

- Time is always an argument; no clock is read (ids are minted at the
  exchange's or merge's time; `BusSink` reads only the clock it is given).
- A refused write changes nothing and publishes nothing; events are
  published only after their transaction commits.
- Records keep the attributed agent: no write rewrites an agent id after
  a merge; deltas carry the attributed agent.
- Harness claims and labels are never read by `resolve`.
- One threading call is atomic; a second call for an exchange returns the
  first outcome.
- A delta's new inputs never list a message the cluster saw in another
  conversation within the retention; its output is never withheld
  (INV-1100). The stored history keeps every request message in place, so
  a conversation's history is its deltas' request suffixes, not their
  new inputs alone (INV-152, 390).
- Evidence paths: `crosstalk_reconstruct::tests::<area>::<fn>`.
- Every outbox event is published under the envelope id stamped on its
  row before its first publish, and only after the staging transaction
  committed (INV-1211). Envelopes the consumer publishes have ids derived
  from the exchange (INV-1202).
- `AgentSeen` commits with the write that attributes the evidence
  (`create` from traffic, `attach_evidence`, in memory and on Postgres),
  so a failed publish followed by a redelivery loses no announcement.
- Not yet evidenced (still `agent = "false"`): the store-level `dst`
  invariants INV-141, 144, 161, 508, 540, 608, 612; INV-1211 and
  INV-1202 (their reconstruct tests exist; the Postgres test has not run
  here, and INV-1202's `dst` flag covers five consumers).

## Conversation reads

Both conversation stores implement the spec's `ConversationReads`
([conversation_reads.md](conversation_reads.md)): each threading call that
records an outcome also records one turn (exchange, first ordinal and
entry count, agent, start, outcome kind, history length after it), a new
conversation records its traffic source (`ThreadInput::source`), and a
compaction's turn-0 entries in the predecessor's history are flagged
`carried_over` by the plan. `TranscriptEntry` is the spec's type. Postgres:
migration `0004_conversation_reads` (`conversation_turns`, conversation
columns, `carried_over`, best-effort backfill with time 0). Files:
`thread/reads.rs`, `thread/memory/reads.rs`, `thread/pg/reads.rs`; tests
`tests/conversation_reads.rs`, `tests/pg_conversation_reads.rs`.
INV-1001..1008, 1010, 1011, 1016, 1022.
