# Memory reference stores

`crosstalk-memory` (`crates/memory`) holds an in-memory implementation of
every stateful store trait in the spec, and a model-based property harness
per store. Roadmap item P2.3, principle 3 ("reference models first"). Each
in-memory store is:

- the **reference model** its Postgres implementation is checked against:
  the harness runs random operation sequences on both and requires equal
  observable results;
- the backend for early L8 and UI work;
- the store behind the deterministic simulation.

The crate is a dev-dependency of the layer crates, never a normal one
(`docs/features/workspace.md`, dependency rule 2).

The pipeline half (L3 to L5) and the insight half (L6 to L8) are separate
sections of this page.

## Pipeline stores (L3–L5)

### Scope

| Store | Spec traits | Module |
| --- | --- | --- |
| `MemoryAgents` | `AgentDirectory`, `IdentityResolver` (`merge`, `unmerge`, `rename`; `resolve` as a reference lookup), `ClaimStore`, `ActivityStore`, `AgentReads` | `reconstruct` |
| `MemoryFingerprintIndex` | `FingerprintIndex` | `provenance` |
| `MemoryChannels<D>` | `ChannelRegistry`, `ChannelDirectory` | `flow::registry` |
| `MemoryVerdicts` | `TransmissionVerdicts` (the verdict store) | `flow::verdicts` |

Each store also implements a seeding trait for the writes the spec gives
no trait method, because the layer's consumer makes them (P4.1, P5):
`SeedAgents` (create an agent, move it Registered to Provisional to
Established, attach evidence), `SeedChannels` (discover a channel, add a
resource, record an access, set a detection, apply a confirmation, read
every channel back) and `SeedTransmissions` (put a transmission). The
harnesses drive the store under test through the same seeding traits, so
the Postgres stores implement them too.

### Non-scope

- Computations that hold no state: `Segmenter`, `Decoder`,
  `Fingerprinter`, `ResourceExtractor`, `Correlator`, `Normalizer`.
- `Threader`. It keeps conversations, but it is L3's threading algorithm
  (prefix matching, forks, compaction, increments), not a store; it belongs
  to P4.1.
- `IdentityResolver::resolve`'s algorithm. The chain of resolvers, conflict
  handling and prompt fingerprints are P4.1's. `MemoryAgents::resolve` only
  looks up the evidence an exchange's client context carries (see
  "Resolution" below).
- `SemanticMatcher`: its lookup embeds query text, which needs a model.
- Postgres. The Postgres stores are P4.1, P4.2 and P5; their tests reuse
  the harnesses here.

### Shared building blocks (`pipeline`)

| Item | Role |
| --- | --- |
| `State<T>` | `Arc<std::sync::RwLock<T>>` around a store's plain data. Every operation is a pure function over `T` run in one critical section, which is the store's transaction. A `std` lock, not a `tokio` one, because `AgentDirectory::canonical` and `ChannelDirectory::canonical` are synchronous and called from async tasks; no guard is ever held across an `.await`, so every trait future is `Send`. A poisoned lock is recovered (writes check before they change anything) |
| `Outbox` | The sending half of an unbounded `tokio::sync::mpsc` channel of `BusEvent`s. A store publishes after its critical section ends, so a re-query after any event sees the change. `Outbox::none()` drops events; `drain` empties a receiver |
| `Clock`, `ManualClock` | The "now" a trait does not pass in: `declare`'s declaration time, the fingerprint index's retention |
| `IdSequence` | Deterministic increasing ids (`prefix << 64 \| counter`) for ids a store creates (`MergeId`, declared `ChannelId`). Drawn only for accepted operations |
| `CursorTable<K>` | Page cursors: a token `<list>-<n>` indexes a row holding the request it was issued for and the last key served. An unknown token, or one presented with another request, is `InvalidCursor` |
| `page_after` | Newest-first keyset paging over an already filtered list |
| `harness::{run, same, HarnessConfig}` | The proptest runner every harness shares: fresh current-thread runtime per case, shrinking, a panic naming the first mismatch |

### Data and control flow

```text
caller ── trait call ──▶ store method
                          │  state.write()      (one critical section)
                          │    table op: check everything, then apply
                          │    returns (result, events)
                          │  guard dropped
                          └─ outbox.publish(events) ──▶ receiver (wiring stamps Envelopes)
```

**L3 (`reconstruct`).** `AgentTable` holds agents, the merge log
(`MergeId → MergeRecord`), vetoes (one per ordered pair), claims and
activity per attributed agent.

- `canonical(id)` is the agent's `MergedInto::into` or the id itself.
- `merge` follows `observed::agent::merge`: unknown agents, then
  `MergeRequest::conflict` (`MergeIntoSelf` before `AgentMerged`), then for
  a resolver merge any veto that separates the two clusters (`Vetoed`);
  an operator merge deletes those vetoes. The source is merged away with
  its active state as `prior`, every alias of the source is repointed,
  and the record lists them. Publishes `AgentMerged` and `Changed::Agent`
  for the source, target, repointed agents, every agent whose stored
  parent is one of those, and the source's canonical parent.
- `unmerge` refuses an unknown or reverted record, restores the source to
  its prior state, points back every agent the record repointed that has
  not moved since (`Agent::restore`), records the veto (replacing one for
  the same pair), and publishes `AgentUnmerged` and `Changed::Agent` the
  same way.
- `rename` uses `Agent::rename`: a merged agent is `AgentMerged`;
  `Applied` publishes `AgentRenamed` and `Changed::Agent`; `Unchanged`
  publishes nothing.
- Claims and activity are stored per attributed agent and unioned (or
  maxed) over the cluster at read time, so an unmerge splits them again.
- `AgentReads::list` builds each canonical agent's profile (aliases,
  canonical parent unless it is the agent's own cluster, union of claims,
  latest activity), keeps those `AgentFilter::matches`, newest id first,
  and pages them with a cursor bound to the filter's JSON. `cluster` and
  `names` resolve the id first.

**Resolution.** `resolve` derives evidence from the client context only:
the scope (account, else stable credential, else upstream; plus the same
under the previous digests during a rotation), harness agent and session
ids in that scope, the account and the credential by stability. It takes
the most specific evidence present; the holders of it (for a session, only
agents holding no harness agent id) map to canonical agents: none is
`New`, one is `Known` (with the evidence that agent lacks), more is
`Conflict` (ascending, with the deciding evidence). An exchange with no
such evidence is a `Store` error: its only evidence would be a prompt
fingerprint, which needs a content hash this crate does not compute.

**L4 (`provenance`).** `IndexState` holds postings (`Fingerprint →
{(SpanId, offset)}`) and one observation per scanned text (its time and
distinct fingerprints). `frequency(f)` counts observations containing `f`
with `now - retention <= at`. `insert` stores no posting for a fingerprint
above the cutoff, and `lookup` returns no hit on one, including postings
stored while it was below. `insert` and `lookup` refuse a call holding a
fingerprint of a shard the node does not own, naming the first. Writes
drop aged observations; `evict` drops a span's postings.

**L5 registry (`flow::registry`).** `ChannelTable` holds channels, policy
histories, resources (each on one channel) and accesses.

- `lookup`: `Known` (the canonical channel of the stored resource with
  that locator), else `Declared` (the declared channel whose pattern
  matches), else `New`.
- `declare` refuses a pattern overlapping a declared one, dates the
  declaration by the clock, and records the policy's decision, if any, as
  the history's first entry.
- `set_policy` refuses a superseded channel, records the decision in the
  history (`Current`, `Superseded` or `Duplicate`) and sets the channel's
  policy to the history's current one; anything but `Duplicate` announces
  the channel.
- `promote` runs `promotion::plan` over the registry (ascending id) and
  applies it: the plan's origin, the decision recorded, every planned
  channel superseded; publishes one `ChannelPromoted` then
  `Changed::promotion`. `promotion_coverage` runs `promotion::coverage`
  over the same entries, a channel's held resources being its seed and its
  own resources.
- `resource_use` resolves the channel, gathers the resources of it and of
  every channel it superseded, counts accesses in the window by canonical
  agent (through `D: AgentDirectory`) and kind, and pages newest resource
  first with a cursor bound to (canonical channel, window).
- `confirm` (seeding) advances the canonical channel's detection to
  `Active` (keeping `since` when already active) and leaves a superseded
  channel's frozen.

**L5 verdicts (`flow::verdicts`).** `VerdictTable` holds transmissions and
one `VerdictLog` each. `set` refuses an unknown transmission and one whose
state is not judgeable, appends through `VerdictLog::record`, and on
`Appended(r)` publishes `VerdictSet` with revision `r` and
`Changed::Verdict`. `quality` is `DetectionQuality::tally` over every
stored transmission with its current verdict.

### The property harnesses

Each harness generates operation sequences with proptest (pinned
`proptest = "=1.11.0"`), runs every sequence on the store under test and
on the reference, and after every step requires equal results, equal
published events except `Changed` (the store under test must announce at
least the reference's notifications), and equal observations of the whole
store. Lists are compared page by page, each store following its own
cursors; unordered results (fingerprint hits) as multisets.

| Harness | Store under test is built by | Observes after each step | Also checks on the store under test |
| --- | --- | --- | --- |
| `reconstruct::model::check_agent_store(config, make)` | `make(IdSequence, Outbox) -> S` where `S: AgentStore` (`AgentDirectory + IdentityResolver + ClaimStore + ActivityStore + AgentReads + SeedAgents`) | `canonical`, `claims`, `last_seen`, `cluster` of every id; `names`; the unfiltered list in pages of 2; a foreign cursor | merge chains flat; merged exactly when one unreverted record names the agent; every state change legal |
| `provenance::model::check_fingerprint_index(config, make)` | `make(IndexConfig, ManualClock) -> S` where `S: FingerprintIndex`; run for a single node and for one of two shards | `frequency` and `lookup` of every fingerprint | — |
| `flow::registry::model::check_channel_registry(config, make)` | `make(MemoryAgents, IdSequence, ManualClock, Outbox) -> S` where `S: ChannelStore` (`ChannelRegistry + ChannelDirectory + SeedChannels`) | every channel; `canonical` and `policy_history` of every id; `lookup` of every locator; a full `resource_use` traversal of every channel | declared patterns disjoint; policy is the history's current; supersession one step, to a declared channel |
| `flow::verdicts::model::check_transmission_verdicts(config, make)` | `make(Outbox) -> S` where `S: VerdictStore` (`TransmissionVerdicts + SeedTransmissions + TransmissionReads`) | every log; the all-time quality | `set` never changes the stored transmission |

Ids the store creates come from the `IdSequence` it is given, so the two
stores create equal ids and results compare without translation. Each
harness is proved twice in this crate: the reference passes against
itself, and a deliberately broken store (dropped activity records, no
cutoff, a directory without merges, an outbox that drops everything) is
caught (`#[should_panic]`).

### Invariants and constraints

- Every store is `Send + Sync`; every trait future is `Send`.
- Every write checks before it changes anything: a refusal leaves the
  state (and the outbox) untouched.
- Events are published after the critical section, in the order the
  operation produced them; a refused or unchanged operation publishes
  nothing.
- Ids a store creates are drawn only for accepted operations.
- No store holds text (the fingerprint index stores fingerprints, span ids
  and offsets only).
- The reference tests live under `crosstalk_memory::{reconstruct,
  provenance, flow::registry, flow::verdicts}::tests`. They check the same
  properties as invariants whose implementation evidence names
  `crosstalk_reconstruct::`, `crosstalk_provenance::` or `crosstalk_flow::`
  tests; that evidence belongs to the Postgres stores and is not flipped
  here.

### Files

| File | Role | Key exports |
| --- | --- | --- |
| `crates/memory/src/pipeline/mod.rs` | Shared building blocks | `State`, `Outbox`, `drain`, `Clock`, `ManualClock`, `IdSequence`, `CursorTable` |
| `crates/memory/src/pipeline/harness.rs` | The harness runner | `HarnessConfig`, `run`, `same`, `Mismatch` |
| `crates/memory/src/reconstruct/mod.rs` | The L3 store | `MemoryAgents` |
| `crates/memory/src/reconstruct/table.rs` | L3 state and operations | (crate) `AgentTable` |
| `crates/memory/src/reconstruct/store.rs` | L3 trait impls | — |
| `crates/memory/src/reconstruct/seed.rs` | L3 seeding | `SeedAgents`, `NewAgent`, `AgentOrigin`, `Advance`, `SeedError` |
| `crates/memory/src/reconstruct/resolve.rs` | The reference lookup behind `resolve` | `context_evidence` |
| `crates/memory/src/reconstruct/model.rs` | L3 harness | `check_agent_store`, `AgentStore`, `AgentOp`, `agent_ops`, `traverse` |
| `crates/memory/src/reconstruct/tests/` | L3 reference tests | — |
| `crates/memory/src/provenance/index.rs` | The fingerprint index | `MemoryFingerprintIndex`, `IndexConfig`, `InvalidIndexConfig` |
| `crates/memory/src/provenance/model.rs` | L4 harness | `check_fingerprint_index`, `IndexOp`, `configs` |
| `crates/memory/src/provenance/tests.rs` | L4 reference tests | — |
| `crates/memory/src/flow/registry/mod.rs` | The L5 registry | `MemoryChannels` |
| `crates/memory/src/flow/registry/table.rs` | Registry state and operations | (crate) `ChannelTable` |
| `crates/memory/src/flow/registry/store.rs` | Registry trait impls | — |
| `crates/memory/src/flow/registry/seed.rs` | Registry seeding | `SeedChannels`, `DetectionUpdate`, `SeedError` |
| `crates/memory/src/flow/registry/model.rs` | Registry harness | `check_channel_registry`, `ChannelStore`, `RegistryOp`, `traverse` |
| `crates/memory/src/flow/registry/tests.rs` | Registry reference tests | — |
| `crates/memory/src/flow/verdicts/mod.rs` | The verdict store | `MemoryVerdicts`, `SeedTransmissions` |
| `crates/memory/src/flow/verdicts/model.rs` | Verdict harness | `check_transmission_verdicts`, `VerdictStore`, `TransmissionReads`, `VerdictOp` |
| `crates/memory/src/flow/verdicts/tests.rs` | Verdict reference tests | — |
