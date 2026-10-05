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
  `AgentReads`), with its migrations, outbox and directory cache.
- Threading (`Threader`): `ConversationThreader` over a
  `ConversationStore` (`MemoryConversations`, `PgConversations`): prefix
  matching, forks and retries, compaction, WebSocket increment resolution,
  system turns anywhere in a request, rethreading idempotence.
  `MemoryConversations` also implements the spec's `ExchangePlacements`
  (an exchange's agent and conversation, as its recorded outcome placed
  it; `reconstruct.placement.as-threaded`), which `Live` exposes for
  eval. `PgConversations` does not yet.
- The L3 bus consumer (`consumer`): `ExchangeCaptured` in; attribution,
  claims and activity, threading; `AgentSeen` and `ConversationDelta` out.

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
  6. publish AgentSeen per new item, then ConversationDelta, under envelope ids derived from the
     exchange id (ids::derived_event_id): a redelivery republishes the same envelopes
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
`outbox`.

- Merges and unmerges run `SERIALIZABLE` (`retry_serializable`): the
  agent table, merge log and vetoes are read in one statement into an
  `agents::table::Table`, the decision is made there (the merge log's
  procedure from the spec), and exactly what its `Diff` names is written,
  with the events appended to `outbox` in the same transaction.
- Lifecycle writes (`create`, `advance`, `attach_evidence`) are targeted
  `SERIALIZABLE` transactions; `create` from traffic and `FirstTraffic`
  record activity in the same transaction.
- After commit: the directory cache is repointed, then the events go to
  the `EventSink` and their outbox rows are deleted; a sink failure leaves
  them for `flush_outbox`.
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

The store then records the conversation (`Write`): new conversations,
fork bases copied from the parent's transcript through the shared prefix,
appended entries with ordinals, the outcome per exchange, and the
response for later increments. `MemoryConversations` does this under one
`tokio::sync::Mutex`; `PgConversations` in one `SERIALIZABLE` transaction
(`migrations/0002_conversations.sql`).

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
| `src/ids.rs` | Id sources and derived envelope ids | `IdSource`, `UlidSource`, `derived_event_id` |
| `src/publish.rs` | Event sinks | `EventSink`, `BusSink`, `SinkError` |
| `src/error.rs` | Storage failures to spec errors | `StorageFailure`, `StoreReason` |
| `src/agents/mod.rs` | The Postgres agent store | `PgAgents` (`open`, `with_retry`, `reload_directory`, `apply_event`, `members`, `flush_outbox`), `MIGRATIONS`, `run_migrations` |
| `src/agents/table.rs` | Merge-log decisions and read models over loaded rows | `Table`, `Diff`, `Applied` (crate) |
| `src/agents/{load,writes,resolve,reads,cache,codec}.rs` | Snapshot loading; writes and outbox; `resolve`; `AgentReads`; directory cache; column codecs | — |
| `src/thread/mod.rs` | The threader | `ConversationThreader`, `ClusterMembers`, `ReadsMembers`, `outcome_kind` |
| `src/thread/{history,plan,store}.rs` | Chain hashes and request analysis; the decision; the store trait and its input | `Entry`, `ChainHash`, `ConversationStore`, `ThreadInput`, `RequestKind`, `ResponseKey`, `TranscriptEntry` |
| `src/thread/{memory,pg}.rs` | Conversation stores | `MemoryConversations`, `PgConversations` |
| `src/thread/messages.rs` | Message bodies and facts | `MessageReader`, `Facts`, `DEFAULT_SUMMARY_PREAMBLES` |
| `src/consumer/{mod,attribute}.rs` | The bus consumer | `ReconstructConsumer`, `ConsumerParts`, `Handled`, `ConsumeError`, `run`, `group`, `subjects`, `GROUP`, `attribute`, `derive_parent`, `corroborated` |
| `crates/reconstruct/migrations/000{1,2}_*.sql` | Schema `reconstruct` | — |
| `src/tests/` | Tests by area (paths below) | — |

## Tests

- `tests::threading`: outcomes, origins, failures, increments, compaction
  evidence, system turns, and the fixture regressions (interleaved re-runs,
  a rewritten early tool result forking at the rewrite, a resumed session
  and a compact boundary).
- `tests::thread_props`: generated harness scripts (continuations,
  retries, branches, rewrites, system changes and mid-array system turns,
  failures, compactions, a two-agent cluster) with an oracle checking every
  step; metamorphic properties for increments, scopes, system changes and
  compactions.
- `tests::evidence`, `tests::consumer`, `tests::consumer_props`,
  `tests::table`: derivation, attribution, parents, conflicts, merges and
  conversations, the store's decisions without a database.
- `tests::dst`: `crosstalk-sim` schedules of duplicate and reordered
  deliveries, a secret rotation, and concurrent threaders.
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
- Evidence paths: `crosstalk_reconstruct::tests::<area>::<fn>`.
- Not yet evidenced (still `agent = "false"`): the store-level `dst`
  invariants INV-141, 144, 161, 508, 540, 608, 612.
