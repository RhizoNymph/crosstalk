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
  system turns anywhere in a request, rethreading idempotence, the
  per-agent seen-message set that keeps replayed history out of a delta's
  new inputs (`ThreadConfig`), and delivery-order tolerance: what a store
  holds for a set of exchanges is what threading them in turn-key order
  leaves, whatever order they arrived in (`thread::order`,
  `reconstruct.thread.order-independent`).
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

- Wiring: the gateway composes the consumer as its L3 stage
  (`crosstalk_gateway::live::layers::l3::Reconstruct` over `MemoryAgents`
  and `MemoryConversations`; `live::pg::l3::PgReconstruct` over
  `PgAgentStore` and `PgConversations` in Postgres mode), keeping each
  `ExchangeCaptured` in L1's exchange store before threading it. See
  [gateway](gateway.md).
- The HTTP side of conversation reads (`crosstalk-surface`, `crosstalk-api`);
  this crate implements the store trait only (see
  [Conversation reads](#conversation-reads)).
- Replay corpora (`IngressMode::Replay`): not on this base. See
  [Replay isolation](#replay-isolation).
- Multi-node coherence of the directory cache beyond `apply_event`.
- Order tolerance outside threading: an agent's id, `first_seen` and
  state times come from the first exchange L3 handled for it, a
  conversation's id is minted when its opening exchange first arrives, and
  a rewound exchange's `ConversationDelta` is not published again (see
  [Delivery order](#delivery-order)).

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
   is a prefix of the request (ties: the latest in threading order, the
   conversation whose last turn has the greatest turn key).
4. `Compacts` when the request carries content evidence: a summary turn
   (a user message opening with a known harness preamble, or, with the
   `Compaction` hint, mentioning a summary) that no cluster conversation
   holds yet, or a stored output echoed as its first message. The
   predecessor is the most recently threaded cluster conversation holding
   any of the request's other messages, else the latest one (ties and
   "latest" are always by last turn key). New inputs exclude messages in
   the predecessor's history.
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

### Delivery order

L2 promises no order (`transport.ordering.unconstrained`): a retry, a
nack, a second take, or a delivery cut by an outage and redelivered after
later ones (the node0 forensic run: a session's first exchange
redelivered after its second, which then opened the conversation and left
an extra one) hands the store an exchange after exchanges that ended after
it. Every step of the decision reads what is stored, so the result is
defined by an order of the exchanges' own: the **turn key**
(`order::TurnKey`), when the exchange ended (`finished_at`, or
`failed_at`), then its id. A request carries only outputs of exchanges
that ended before it started, so continuations always come after what
they continue; with no fault it is the order capture publishes in, so
in-order traffic threads exactly as before.

`order::thread`, inside the store's atomic step:

```text
recorded?                         → the recorded outcome
later = the cluster's turns taken at or after the first one whose key > x's
  (each turn numbered by when the store took it: seq)
empty                             → plan and apply x (thread_now)
arrival(later, turn counts, kept calls):
  Late(Rewind)                    → unthread each later turn, newest seq first;
                                    thread x; thread each kept call again in key order
  Stranded(Forgotten | Interleaved) → warn; thread x where it arrived
```

- `Rewind` exists only when it can be carried out: every later turn's
  call is kept and the later turns are the newest turns of each
  conversation they touch. Undoing a turn (`unthread`) then removes
  exactly what its write added: the conversation it opened (with a fork's
  copied base), or its transcript entries from its first ordinal; its
  turn; its thread record; the response it filed; the sightings it
  recorded; and points the conversation at its previous turn again (history
  length, head, last system message, last key and time).
- Every threading call is kept (`ThreadCall`: the input less the cluster)
  for the **reorder window** (`ReorderWindow`, a day by default,
  `with_reorder_window` on both stores), then pruned with the cluster's
  calls that ended more than the window before a newer call. An exchange
  arriving after one that ended more than the window after it can find a
  call pruned (`Stranded::Forgotten`) and is threaded where it arrived;
  so is one whose rewind would undo turns of agents outside the cluster
  (after an unmerge, `Stranded::Interleaved`).
- The outcome returned (and published) is the late exchange's. The
  rethreaded exchanges' recorded outcomes, turns and transcript entries
  change with them (`ExchangePlacements`, `ConversationReads`); their
  deltas were published when they arrived and are not published again, so
  L4 and L5 keep what those deltas said.
- Ids are not re-derived: a conversation keeps the id minted when its
  opening exchange first arrived (a late opening exchange's id carries the
  later millisecond the monotonic generator was at), and agent ids,
  `first_seen` and state times come from attribution, which happens on
  arrival.
- With the seen-message set kept per exchange, and pruned only past the
  retention plus the window, a rethreaded turn sees exactly the sightings
  of the turns before it in key order.

### Known limitations (deferred until after the post)

- **Ids depend on arrival order.** A new agent's id, `first_seen` and
  state times come from the first of its exchanges L3 handles, not the
  earliest; a conversation keeps the id minted when its opening exchange
  first arrived, and the monotonic generator gives a late exchange's id
  the later millisecond. In the node0 forensic case B's id carries the
  fourth exchange's millisecond.
  `crosstalk_e2e::reorder::exact_answers_need_agent_ids_from_the_earliest_exchange`
  (ignored) demonstrates it; re-anchoring needs downstream support,
  since the later exchange's delta has already published the id.
- **INV-187 wording.** `reconstruct.thread.rethread-idempotent` says a
  rethread returns "the outcome of its first threading". It returns the
  recorded outcome, which a late earlier exchange can change; the
  statement holds when first arrivals are in turn-key order. INV-187 is
  left as is.

### The seen-message set

- What counts as seen in a conversation: every non-system message a write
  stores there from the request, and the output. For a new conversation
  that is its whole request (a fork's base included: the request carried
  it); for an extension, the appended messages. It is recorded under the
  attributed agent with the exchange's start (`ThreadInput::at`), one
  sighting per (agent, message, conversation, exchange), so undoing a turn
  removes exactly its sightings; a lookup covers every
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
  older than the retention and the reorder window behind the newest
  exchange it has threaded (an ordered index makes this a pop from the
  front). `PgConversations` deletes the threaded cluster's rows older than
  the retention and the window behind the exchange in the threading
  transaction and offers `forget_seen(now)` to sweep agents that stopped
  calling (an unbatched retention delete, so it raises the transaction to
  the store's longer retention statement bound, INV-1240). A store may therefore forget a sighting an exchange arriving
  more than the retention and the window out of order would still have
  counted.
- Configuration: `MemoryConversations::with_config`,
  `PgConversations::with_config`; the gateway's `LiveConfig::threading`.

The store then records the conversation (`Write`): new conversations,
fork bases copied from the parent's transcript through the shared prefix,
appended entries with ordinals, the outcome per exchange, the response
for later increments (with the exchange that filed it), the write's seen
messages, the turn with its `seq`, key and the conversation's last system
message, and the call. `MemoryConversations` does this under one
`tokio::sync::Mutex`; `PgConversations` in one `SERIALIZABLE` transaction
(`migrations/0002_conversations.sql`; the seen set in
`migrations/0003_seen_messages.sql`, table `seen_messages`; the order in
`migrations/0006_threading_order.sql`: `conversation_turns.seq`,
`ended_at`, `last_system`, the sequence `turn_seq`;
`conversations.last_ended_at`, `last_exchange` in place of `updated`;
`responses.exchange`; `seen_messages.exchange` in the key; and
`thread_calls`, the kept calls, the request as `bytea[]` and `text[]`).

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
| `src/thread/{history,plan,store}.rs` | Chain hashes and request analysis; the decision (including the seen-elsewhere filter); the store trait and its input | `Entry`, `ChainHash`, `ConversationStore`, `ThreadInput` (with `at`, `ended_at`, `key`), `RequestKind`, `ResponseKey`, `TranscriptEntry`, `TurnKey` |
| `src/thread/order.rs` | Delivery-order tolerance: the turn key, the reorder window, the kept call, the rewind plan and the step every store runs | `TurnKey`, `ReorderWindow`, `DEFAULT_REORDER_WINDOW`; crate: `ThreadCall`, `LaterTurn`, `Rewind`, `Arrival`, `Stranded`, `arrival`, `OrderedStore`, `thread` |
| `src/thread/memory/rewind.rs`, `src/thread/pg/rewind.rs` | Each store's `OrderedStore`: later turns, undoing one, threading one call | `Step` (module) |
| `src/thread/pg/calls.rs` | Kept calls on Postgres (`thread_calls`): keep and prune, read | `keep`, `calls` (module) |
| `src/thread/config.rs` | Typed threading configuration | `ThreadConfig`, `SeenRetention`, `ThreadConfigError` (also the reorder window's refusals), `DEFAULT_SEEN_RETENTION` |
| `src/thread/{memory,pg}.rs` | Conversation stores, each with its seen-message set | `MemoryConversations` (`new`, `with_config`, `with_reorder_window`, `with_cursor_key`, `with_cursor_secret`), `PgConversations` (`new`, `with_retry`, `with_config`, `with_reorder_window`, `with_cursor_key`, `with_cursor_secret`, `forget_seen`); both `ExchangePlacements` |
| `src/thread/messages.rs` | Message bodies and facts | `MessageReader`, `Facts`, `DEFAULT_SUMMARY_PREAMBLES` |
| `src/consumer/{mod,attribute}.rs` | The bus consumer | `ReconstructConsumer` (`new`, `handle`, `stats`, `agents`, `threader`), `ConsumerParts`, `ConsumerStats`, `ConsumerCounts`, `Handled`, `ConsumeError`, `run`, `group`, `subjects`, `GROUP`, `attribute`, `derive_parent`, `corroborated` |
| `crates/reconstruct/migrations/000{1,2,3,4,5,6}_*.sql` | Schema `reconstruct`: agents, conversations, seen messages, conversation reads, outbox stamps, threading order and kept calls | — |
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
- `tests::order_props` (`reconstruct.thread.order-independent`): generated
  scripts (general, fork-heavy, compaction-heavy) each delivered in three
  seeded permutations store exactly what in-order delivery stores
  (conversations by opening exchange, transcripts, turns, recorded
  outcomes; `tests::order_view`); the node0 forensic case (a session's
  first exchange after its second) on the threader and through the whole
  consumer; a late middle turn joining between its neighbours.
  `tests::order_arrival`: the rewind plan (undo newest first, replay by
  key; stranded by a forgotten call or another cluster's turn), the
  window's checks, and a store past the window still threading every
  exchange. `tests::pg_order` (gated): the forensic case and other orders
  of it, and generated scripts reversed and in two seeded orders, on
  Postgres against in-order delivery in memory. End to end,
  `crosstalk_e2e::reorder` feeds the wiki relay with B's first exchange
  after its second (memory and Postgres mode) and gets the in-order
  threads and the uninterrupted answers with agents named by role; the
  exact comparison (agent ids included) is an ignored test until agent
  ids are re-anchored.
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
- `tests::placement`: `ExchangePlacements` on `MemoryConversations` (an
  exchange reads back as the agent and conversation its threading
  recorded). `tests::pg_placement` (gated): `ExchangePlacements` on `PgConversations`
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
- One threading call is atomic; a second call for an exchange returns its
  recorded outcome: the first one, unless an exchange that ended before it
  arrived later and rethreaded it (INV-187's "first threading" holds for
  deliveries whose first arrivals are in turn-key order).
- The stored result for a set of exchanges is the turn-key-order result
  (`reconstruct.thread.order-independent`, INV-1235), within the
  reorder window; before each call the cluster's turns were taken in key
  order except where an arrival was stranded, so the turns a rewind undoes
  are the newest of their conversations.
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
