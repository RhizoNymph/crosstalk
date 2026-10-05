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

Every store implements spec traits only, write side included (roadmap
P0.6): there are no seeding traits and no inherent write hooks. The
harnesses drive the store under test through the same spec traits, and
what a store reads from another layer's caches (merges and supersessions,
node facts, L7's watermark) is a world of spec read traits (`AgentDirectory`,
`ChannelDirectory`, `NodeFacts`, `WatermarkRead`) the harness hands `make`
and changes itself, for the subject and the reference alike. So a Postgres
store needs nothing crate-specific to be checked.

The pipeline half (L3 to L5) and the insight and surface half (L6 to L8)
are separate sections of this page. They share one set of building blocks
(`support`: `IdSequence`, `Outbox`, `CursorBook` and `page_after`, the
locks) and one harness runner (`model::run` with `HarnessConfig`,
`Divergence` and `ModelMismatch`: a failing harness returns a
`ModelMismatch` with proptest's minimal sequence). `HarnessConfig::default`
is 48 cases of at most 40 operations; the pipeline harnesses' own tests run
64 cases, as they always have. Each case runs on a fresh current-thread
runtime.

No store reads a clock: every store method that depends on the time takes
it as an argument, and so does `TopicModel::fit` (`FakeTopicModel` stamps
`fitted_at` with the `at` it is given). `support::ManualClock` implements
the spec's `Clock` for tests that need one to move.

## Pipeline stores (L3–L5)

### Scope

| Store | Spec traits | Module |
| --- | --- | --- |
| `MemoryAgents` | `AgentDirectory`, `IdentityResolver` (`merge`, `unmerge`, `rename`, `resolve`), `AgentLifecycle`, `ClaimStore`, `ActivityStore`, `AgentReads` | `reconstruct` |
| `MemoryFingerprintIndex` | `FingerprintIndex`, `SpanIndex` (span records written by `record`, read in batches by `spans`, kept through eviction) | `provenance` |
| `MemoryChannels<D>` | `ChannelRegistry`, `ChannelTraffic`, `ChannelReads`, `AccessStore`, `ChannelDirectory` | `flow::registry` |
| `MemoryVerdicts` | `TransmissionStore`, `TransmissionVerdicts` | `flow::verdicts` |

The writes the layer consumers make (P4.1, P5) are spec traits:
`AgentLifecycle` (create an agent, move it Registered to Provisional to
Established, attach evidence), `ChannelTraffic` (store a resource where
its lookup puts it, record an access, discover a channel by a cross-agent
transmission, record each channel transmission's state, set a detection)
with `ChannelReads` (a channel by id and filtered pages of them, each with
its cross-agent traffic; a channel's crossing transmissions), and
`TransmissionStore` (save a transmission, read it back).

### Non-scope

- Computations that hold no state: `Segmenter`, `Decoder`,
  `Fingerprinter`, `ResourceExtractor`, `Correlator`, `Normalizer`.
- `Threader`. It keeps conversations, but it is L3's threading algorithm
  (prefix matching, forks, compaction, increments), not a store; it belongs
  to P4.1.
- Deriving identity evidence (`EvidenceDeriver`), conflict handling and
  prompt fingerprints: P4.1's. `MemoryAgents::resolve` is the store half,
  the lookup of derived evidence (see "Resolution" below);
  `reconstruct::resolve::context_evidence` derives the client context's
  part of it for tests.
- `SemanticMatcher`: its lookup embeds query text, which needs a model.
- Postgres. The Postgres stores are P4.1, P4.2 and P5; their tests reuse
  the harnesses here.

### Shared building blocks (`support`, every store)

| Item | Role |
| --- | --- |
| `State<T>` | `Arc<std::sync::RwLock<T>>` around an L3–L5 store's plain data. Every operation is a pure function over `T` run in one critical section, which is the store's transaction. A `std` lock, not a `tokio` one, because `AgentDirectory::canonical` and `ChannelDirectory::canonical` are synchronous and called from async tasks; no guard is ever held across an `.await`, so every trait future is `Send`. A poisoned lock is recovered (writes check before they change anything). The L6–L8 stores take a `std::sync::Mutex` with `lock`, which recovers the same way |
| `Outbox` | The sending half of an unbounded `tokio::sync::mpsc` channel of `BusEvent`s. The L3–L5 stores publish right after their critical section, the L6–L8 stores from inside it (in commit order); either way a re-query after any event sees the change. `Outbox::none()` drops events; `drain` empties a receiver |
| `IdSequence` | Deterministic increasing ids (`base + 1`, `base + 2`, …; default base `1 << 80`, above the reserved rule ids) for ids a store creates (`MergeId`, declared `ChannelId`, rule and alert ids, config audit ids). Clones share the counter. Drawn only for accepted operations (`peek` and `skip` for an operation that draws several) |
| `CursorBook<B, K>`, `page_after` | Page cursors: a token is a key into the issuing store's book, bound to the request (`B`) and resuming after `K`. An unknown token, one from another store, or one presented with another request is `InvalidCursor`. `page_after` cuts one page of the already filtered items and issues the next cursor only when more follow |
| `ManualClock` | A spec `Clock` a test moves |

The harness runner is `model::run` (`HarnessConfig`, `same`, `holds`,
`Divergence`, `ModelMismatch`), shared by every harness here and by the
merge round-trip property test.

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

**Resolution.** `resolve` takes derived evidence (`NonEmpty`, so "no
evidence" cannot reach it). It takes the most specific evidence present;
the holders of it (for a session, only agents holding no harness agent id)
map to canonical agents: none is `New`, one is `Known` (with the evidence
that agent lacks), more is `Conflict` (ascending, with the deciding
evidence). `context_evidence` derives the client context's evidence for
tests: the scope (account, else stable credential, else upstream; plus
the same under the previous digests during a rotation), harness agent and
session ids in that scope, the account and the credential by stability.

**Agent writes (`AgentLifecycle`).** `create` refuses a taken id; an agent
from traffic starts `Provisional` with its first exchange recorded in the
activity store in the same transaction. `advance` accepts only
`Registered` to `Provisional` (recording activity) and `Provisional` to
`Established`; `attach_evidence` refuses evidence the record holds. Each
publishes `Changed::Agent` (and, for a creation, the canonical parent's).

**L4 (`provenance`).** `IndexState` holds postings (`Fingerprint →
{(SpanId, offset)}`) and one observation per scanned text (its time and
distinct fingerprints). Every call takes `now`. `frequency(f, now)` counts
observations containing `f` with `now - retention <= at`. `insert` stores no posting for a fingerprint
above the cutoff, and `lookup` returns no hit on one, including postings
stored while it was below. `insert` and `lookup` refuse a call holding a
fingerprint of a shard the node does not own, naming the first. Writes
drop aged observations; `evict` drops a span's postings.

**L5 registry (`flow::registry`).** `ChannelTable` holds channels, policy
histories, resources (each on at most one channel, or on none), accesses
and the state of every channel transmission as last recorded. A channel
exists only once a cross-agent transmission goes through it
([channel_semantics.md](channel_semantics.md)); its cross-agent traffic is
never stored but tallied at each read (`CrossTraffic::tally` over the
recorded transmissions routed through it and every channel it superseded,
agents resolved through `D: AgentDirectory`), so a merge hides a
discovered channel and an unmerge lists it again with nothing rewritten.

- `lookup`: `Known` (the canonical channel of the stored resource with
  that locator, when it is on one), else `Declared` (the declared channel
  whose pattern matches, a resource stored on no channel included), else
  `NoChannel`. It creates nothing.
- `declare` refuses a pattern overlapping a declared one, dates the
  declaration by the time it is given, and records the policy's decision,
  if any, as the history's first entry.
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
- `ChannelTraffic` (`flow/registry/traffic.rs`): `add_resource` stores a
  first sighting on the declared channel its lookup names or on no channel
  (`NoChannel`), moves a resource on no channel onto a declaration made
  since, and refuses a resource already on a channel and a second resource
  with a stored locator (`DuplicateLocator`); a discovered channel holds
  only its seed. `record_access` needs a stored resource (on a channel or
  not) and a new access id, and stores a write whatever its
  `WriteOutcome`, so a rejected write counts as a write in `resource_use`
  (`flow.access.rejected-write-recorded`; pairing is the correlator's, in
  `crosstalk-flow`). `discover` creates the discovered channel for
  a resource on no channel whose lookup is `NoChannel` (seed: the
  resource, the transmission and its opening time; `Active` since then;
  `Unreviewed(None)`; an empty history), moves the resource onto it and
  publishes `ChannelDiscovered` and `Changed::Channel`; for a resource
  already on a channel it returns `Existing` and changes nothing, and for
  one a declared pattern now claims it stores the resource there and
  returns `Existing` with that channel. `record_transmission` keeps the
  latest state of a `Route::Channel` transmission (refusing any other
  route) and, for an opened (`AwaitingContent`) or confirmed state,
  advances the canonical channel's detection to `Active` (keeping `since`
  when already active, else since the opening or `Confirmed::at`; a
  declared channel awaiting traffic or unused goes `InUse`), leaving a
  superseded channel's frozen; `Applied` publishes `Changed::Channel` for
  the canonical channel, a state already recorded is `Unchanged`.
  `set_detection` returns `Applied` or `Unchanged` and refuses a frozen
  (superseded) channel and `Unused` off `AwaitingTraffic`. Each refusal is
  a `TrafficError` and changes nothing.
- `ChannelReads`: `channel` returns the stored record with its traffic
  (`ChannelWithTraffic`; a superseded channel as itself, without traffic;
  a hidden channel too); `channels` pages the channels
  `ChannelFilter::keeps` keeps (never a hidden one), newest
  `ChannelOrigin::created_at` first and ties by id descending, its cursor
  bound to the filter; `transmissions` pages the transmissions routed
  through the canonical channel and every channel it superseded that cross
  agents at the read and pass the confirmation filter, newest opened
  first, its cursor bound to the canonical channel and the filter.
- `AccessStore::accesses` reads a batch of recorded accesses with their
  stored resources in one snapshot, leaving out unknown ids.

**L5 transmissions (`flow::verdicts`).** `VerdictTable` holds
transmissions and one `VerdictLog` each. `TransmissionStore::save`
replaces the stored transmission and keeps its log. `set` refuses an unknown transmission and one whose
state is not judgeable, appends through `VerdictLog::record`, and on
`Appended(r)` publishes `VerdictSet` with revision `r` and
`Changed::Verdict`. `quality` is `DetectionQuality::tally` over every
stored transmission with its current verdict, agents resolved through the
directory `MemoryVerdicts::with_agents` was given (none merged for
`new`), so a transmission whose agents have since merged into one is not
counted (`flow.quality.cross-agent-only`). `list` returns the stored
transmissions a `TransmissionQuery` matches, newest id first, channels
resolved through the directory `MemoryVerdicts::with_directories` was
given, the cursor bound to the query
(`flow.transmission-store.list-matches-query`).

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
| `reconstruct::model::check_agent_store(config, make)`, `check_agent_store_with(config, make)` | `make(IdSequence, Outbox) -> S` where `S: AgentStore` (`AgentDirectory + IdentityResolver + AgentLifecycle + ClaimStore + ActivityStore + AgentReads`), or (`_with`) a future of one, built inside the case's runtime so a Postgres store connects there | `canonical`, `claims`, `last_seen`, `cluster` of every id; `names`; the unfiltered list in pages of 2; a foreign cursor | merge chains flat; merged exactly when one unreverted record names the agent; every state change legal |
| `provenance::model::check_fingerprint_index(config, make)` | `make(IndexConfig) -> S` where `S: FingerprintIndex`; run for a single node and for one of two shards, both stores given the same `now` | `frequency` and `lookup` of every fingerprint | — |
| `flow::registry::model::check_channel_registry(config, make)` | `make(MemoryAgents, IdSequence, Outbox) -> S` where `S: ChannelStore` (`ChannelRegistry + ChannelTraffic + ChannelReads + ChannelDirectory`); every resource stored, then discoveries by transmission, then random steps that record channel transmissions sent by agent 1 or by agent 3 (merged into the reader) | every listed channel with its traffic (a full `ChannelReads::channels` traversal); `channel`, `canonical` and `policy_history` of every id; `lookup` of every locator; a full `resource_use` and `transmissions` traversal of every channel | declared patterns disjoint; policy is the history's current; supersession one step, to a declared channel |
| `flow::verdicts::model::check_transmission_verdicts(config, make)` | `make(MemoryAgents, Outbox) -> S` where `S: VerdictStore` (`TransmissionVerdicts + TransmissionStore`); some transmissions are sent by agent 3, merged into the reader | every log and every stored transmission; the all-time quality | `set` never changes the stored transmission |

Ids the store creates come from the `IdSequence` it is given, so the two
stores create equal ids and results compare without translation. Each
harness is proved twice in this crate: the reference passes against
itself, and a deliberately broken store (dropped activity records, no
cutoff, a directory without merges, an outbox that drops everything) is
caught (the harness returns `ModelMismatch::Failed`).

### Invariants and constraints

- Every store is `Send + Sync`; every trait future is `Send`.
- No store reads a clock; every time is an argument.
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
| `crates/memory/src/support/mod.rs` | Shared building blocks of every store | `State`, `lock`, `Outbox`, `drain`, `IdSequence`, `CursorBook`, `page_after`, `PageError`, `ManualClock` |
| `crates/memory/src/reconstruct/mod.rs` | The L3 store | `MemoryAgents` |
| `crates/memory/src/reconstruct/table.rs` | L3 state and operations | (crate) `AgentTable` |
| `crates/memory/src/reconstruct/store.rs` | L3 trait impls, `AgentLifecycle` included | — |
| `crates/memory/src/reconstruct/resolve.rs` | The lookup behind `resolve`; the client context's evidence | `context_evidence` |
| `crates/memory/src/reconstruct/model.rs` | L3 harness | `check_agent_store`, `check_agent_store_with`, `AgentStore`, `AgentOp`, `agent_ops`, `traverse` |
| `crates/memory/src/reconstruct/tests/` | L3 reference tests | — |
| `crates/memory/src/provenance/index.rs` | The fingerprint index; `IndexConfig` exposes its cutoff, retention, shards and owned shards, so a store under test is built with the same settings | `MemoryFingerprintIndex`, `IndexConfig`, `InvalidIndexConfig` |
| `crates/memory/src/provenance/model.rs` | L4 harness | `check_fingerprint_index`, `IndexOp`, `configs` |
| `crates/memory/src/provenance/tests.rs` | L4 reference tests | — |
| `crates/memory/src/flow/registry/mod.rs` | The L5 registry | `MemoryChannels` |
| `crates/memory/src/flow/registry/table.rs` | Registry state and its declaration, policy, promotion and resource-use operations | (crate) `ChannelTable`, `StoredResource` |
| `crates/memory/src/flow/registry/traffic.rs` | `ChannelTraffic` and `ChannelReads` on the table: placing resources, discovery, recorded transmissions, cross-agent traffic and listings | (crate) `ChannelKey`, `TransmissionKey` |
| `crates/memory/src/flow/registry/store.rs` | Registry trait impls: `ChannelRegistry`, `ChannelTraffic`, `ChannelReads`, `ChannelDirectory` | — |
| `crates/memory/src/flow/registry/model.rs` | Registry harness | `check_channel_registry`, `run_case` (one case on a caller-built subject, for stores that need an async, multi-threaded setup), `ChannelStore`, `RegistryOp`, `traverse`, `transmissions`, `all_channels`, `channels_under` |
| `crates/memory/src/flow/registry/tests/mod.rs`, `tests/traffic.rs` | Registry reference tests; `traffic` covers discovery, resources on no channel, listings, merges hiding channels, the list order and a channel's transmissions | — |
| `crates/memory/src/flow/verdicts/mod.rs` | The transmission and verdict store | `MemoryVerdicts` |
| `crates/memory/src/flow/verdicts/model.rs` | Verdict harness, and the transmission fixtures the registry harness shares | `check_transmission_verdicts`, `run_case`, `VerdictStore`, `VerdictOp`, `state`, `state_between`, `co_access_between`, `content_between` |
| `crates/memory/src/flow/verdicts/tests.rs` | Verdict reference tests | — |

## Insight and surface stores (L6–L8)

### Scope

- L6 (`crosstalk_spec::interfaces::l6_analysis`): `TopicCatalog` and
  `TopicLifecycle`, `SearchIndex` (exact search over the stored text and
  embeddings) and `SearchCorpus`, `ProjectionSource`, `ProjectionStore`,
  `AlertRuleStore`, `AlertTriage`, `AlertRuleMaintenance`, `AlertActions`
  and `AlertReads`;
  deterministic `Fake*` doubles of the computational traits (`Embedder`,
  `TopicModel`, `LayoutFitter`) and of `RuleContext`.
- L7 (`l7_topology`): `EdgeStore` (and `WatermarkRead`), with buckets per
  topic version, `version_ready` and activation, the watermark, retention
  and pins (through the catalog), the graph, series, the channel-centred
  graph, totals, the edge drill-down and agent traffic, all computed from
  the stored contributions; a `FrontierSource` the test sets, and
  `StaticNodes`, a `NodeFacts` the test sets.
- L8 (`l8_surface*`): `AuditLog`, `OperatorStore`, `SinkRegistry` and an
  `AlertSink` double.
- The model-based harnesses for all of the above (`model::analysis`,
  `model::topology`, `model::surface`).

### Non-scope

- `QueryApi`, `OperatorActions` and `LiveFeed`: composed over these stores
  by `crosstalk-surface` (P2.6), through the spec traits alone.
- Bus publishing. Each store sends what the spec says it publishes to its
  `Outbox`, from the critical section of the change; the wiring owns the
  receiver and forwards it.
- Embedding, topic fitting and layout. The `Fake*` doubles are stable
  stand-ins, not reference models.

### Data and control flow

```text
            ┌──────────── InMemoryTopicCatalog ◀── TopicLifecycle: begin_fit / complete_fit / mark_ready / mark_active / assign (analyze)
            │   history, topics, lineage, assignments, frozen sizes; pin / unpin / enforce_retention (publishes TopicVersionDropped)
            │        │ TopicVersions (history, version_of, topic_ids)        │ assignment(v, t), retains(v)
            ▼        ▼                                                       ▼
 InMemoryEdgeStore ◀─ Env { catalog, directory, nodes }        InMemorySearchIndex ── InMemoryProjectionSource
   contributions, accesses, verdict copy,                        documents, verdict copy     (+ spec WatermarkRead)
   (nodes: spec NodeFacts)                                       ◀── SearchCorpus (analyze)
   watermark, activated / dropped versions                       │
   every read = the fold over contributions                      └─ search / sample under one resolved version
            │ WatermarkRead
            ▼
 InMemoryProjectionStore (jobs, leases, frames)      InMemoryAlertStore (rule set, alerts, verdict copy)
                                                        ▲ AlertRuleStore: create / update / set_enabled (surface)
                                                        ▲ AlertTriage: triage / sanctioned / disabled / judged (alerts consumer)
                                                        ▲ AlertRuleMaintenance: topic_version_ready / embedding_model_changed
                                                        ▲ AlertActions: acknowledge / resolve (surface); AlertReads
 InMemoryOperatorStore ── OperatorStore::load ──▶ InMemoryAuditLog (config entries, same lock order)
 InMemorySinkRegistry (SinkRegistry: configured sinks, last delivery)
```

- **One lock per store.** Every store keeps its state behind one
  `std::sync::Mutex` in an `Arc` (cloning shares the store) and takes it
  once per call, so each call is one transaction. No lock is held across an
  `.await`: the only awaiting store call, embedding a semantic rule's query,
  runs before the lock is taken. Stores that read another store take their
  own lock first and never the reverse (index → catalog, edge store →
  catalog, directory, nodes; catalog → directory; alerts → directory;
  operators → audit log), so
  there is no lock cycle. Every store is `Send + Sync`.
- **Paging.** Every list pages with a `CursorBook`
  (`support`): a token is a key into the issuing store's book,
  bound to the request (filter, window, edge, query) and, for linked views,
  to the version the first page resolved. A token from another store, an
  unknown one and one presented with another request are all
  `InvalidCursor`. `page_after` cuts a page and issues the next cursor only
  when more items follow.
- **Topic versions.** The catalog keeps the `TopicVersionHistory` and
  rebuilds it through the spec's constructors on every change, so every
  stored history passes `TopicVersionHistory::new`. A fit's lineage from
  its predecessor (the version before it in the history) is computed when
  the fit returns (`lineage_between`): for each older topic, every newer
  topic ranked by centroid similarity, ties to the lower id; the best link
  whatever the floor, the others at or above it (`complete_fit`).
  `mark_active` supersedes every older version not yet superseded, then
  enforces retention, which freezes the all-time sizes, deletes the
  assignments and publishes `TopicVersionDropped` for each dropped
  version: the catalog is the event's one publisher. Sizes count an assignment only when its
  sender and reader resolve to different agents through the directory
  `InMemoryTopicCatalog::with_agents` was given (none merged by default),
  at the read; frozen sizes apply the merges in force at the drop.
- **Edge store.** It stores contributions keyed by (version,
  transmission) and accesses keyed by id (each on its resource, as
  `AccessContribution` names it), never buckets. `apply` checks, in
  order: dropped version, self-edge, already applied (returns the stored
  key), late (activated version, bucket final under the watermark). A
  version activates (`activate` returns `Switched` and publishes
  `TopicVersionActivated`) once `version_ready` has its count and that many
  distinct `Refit` classifications under it were processed (applied,
  already applied or self-edges). Reads take the watermark first, resolve
  the filter's selector against the catalog's history (retained = not
  dropped here), refuse topics outside the version, and run the fold:
  contributions of the version in the window, agents resolved, resolved
  self-edges dropped, routes resolved, the filter admitted (false
  detections from the store's verdict copy). Graph, totals
  (`EdgeTotals::of` the graph), the channel-centred graph (plus access
  buckets, each resource resolved to the canonical channel `NodeFacts`
  holds it on now and kept only when that channel's facts list it as a
  channel, with its confirmation; a channel node is `Confirmed` when a
  transmission edge routes through it), the drill-down, agent traffic (the graph's node counts) and
  series (the fold cut into grid steps) are all built from it, and the
  results pass the spec's checked constructors (`TopologyGraph::new`,
  `BipartiteGraph::new`, `TopologySeries::new`).
- **Search.** Scores depend on the query, the model and the document
  only: text is the fraction of the query's distinct terms (maximal
  alphanumeric runs, lower-cased) the document contains; semantic is the
  cosine similarity; hybrid is their mean. The filter is applied before
  ranking. A cursor pins the first page's version and binds the query (and
  with it the model); a dropped version is `Version(NotRetained)` and a
  changed index model `WrongModel`. Samples take every admitted document
  with an embedding of the spec's model, keep the `limit` smallest seeded
  BLAKE3 keys (ties to the smaller id), and read the watermark from a
  `WatermarkRead`. Each row's route is a `PointRoute`, its channel
  resolved through supersession when the sample is read.
- **Projection jobs.** Jobs move only through the spec's `ProjectionInfo`
  transitions. `claim` takes the oldest queued job by (`requested_at`, id)
  and records a lease; `requeue_lapsed` returns every fitting job whose
  lease ended before `now`, keeping its age; `complete` builds the `Fitted`
  record from the job's start and the frame's header and joins them with
  `Projection::new`; `expire` drops the frames fitted more than the
  retention before `now`.
- **Alerts.** Rules, alerts and triage's verdict copy share one lock.
  Triage re-reads the draft's rule (`AlertRuleDef::evaluates`) and the
  verdict copy, deduplicates on (rule, stored subject) among open and
  acknowledged alerts, or opens one. Disabling a rule suppresses its active
  alerts in the same call. `topic_version_ready` remaps every watched-topic
  rule current on the lineage's `from` with `AlertRuleDef::remap` and makes
  the new version the one rules must name. Every change bumps the rule's
  or alert's revision (refused before any change when exhausted) and
  publishes `AlertRuleChanged`, `AlertOpened` or `AlertChanged` and the
  matching `Changed`.
- **Operators.** `InMemoryOperatorStore::load` runs
  `OperatorDirectory::load` against the stored directory and appends one
  applied config entry per change to the audit log, all or nothing,
  before storing the new directory.

### Model-based harnesses

Each `check_<trait>(HarnessConfig, make)` generates operation sequences
with proptest (ids from small pools, so they collide), builds a fresh
subject with `make` and a fresh reference per case, applies every operation
to both, and compares what both return and then read back. Cursors are
followed to the end, never compared; store-assigned ids (rules, alerts,
audit ids) are mapped by creation order; results whose order the spec
leaves open are compared sorted. Each case runs on its own current-thread
tokio runtime, so `make` may be async (a Postgres pool). A failure is a
`ModelMismatch` with proptest's minimal sequence.

| Harness | Subject | `make` takes |
| --- | --- | --- |
| `model::analysis::check_topic_catalog` | `CatalogSubject`: `TopicCatalog + TopicLifecycle` | `CatalogConfig` |
| `model::analysis::check_search_index` | `SearchSubject`: `SearchIndex + SearchCorpus + ProjectionSource + TopicLifecycle` | `EmbeddingModel`, `SearchWorld` (the `StaticDirectory` the harness merges and supersedes in, the `ManualWatermark` samples are dated by) |
| `model::analysis::check_projection_store` | `ProjectionStore` | `ProjectionConfig` |
| `model::analysis::check_alert_rule_store`, `check_alert_triage` | `AlertStoreSubject`: `AlertRuleStore + AlertTriage + AlertRuleMaintenance + AlertActions + AlertReads` | `AlertWorld` (config, embedder, and the `StaticDirectory` the harness supersedes channels in) |
| `model::topology::check_edge_store` | `EdgeSubject`: `EdgeStore + TopicLifecycle` (the catalog whose history the store reads) | `EdgeStoreConfig`, `EdgeWorld` (the `StaticDirectory` and `StaticNodes` the harness merges, supersedes and parents in) |
| `model::surface::check_audit_log` | `AuditLog` | nothing |
| `model::surface::check_operator_store` | `OperatorStoreSubject`: `OperatorStore + AuditLog` (config entries read back through `AuditLog::query`) | nothing |

Beyond equality, the harnesses keep their own oracles: the catalog's
sizes against a count of the assignments
(`analysis.sizes.match-cross-agent-assignments`; no agent is merged in
that harness, so the reference tests cover merges); the queue bound (`analysis.projection.queue-bounded`); at
most one active alert per (rule, subject); every graph against an
independent fold over the contributions the harness applied, with
the graph's parts re-checked by `TopologyGraph::new` and shares summing to 1, and every series' total
against the graph's (`topology.graph.matches-fold-model`,
`topology.series.total-matches-graph`); audit entries never changing
(`surface.audit.append-only`). Text and hybrid search ranking is the
implementation's, so for those the harness compares only the resolved
version and the errors and checks the traversal's order, page sizes and
window. `model::mutants` plants one bug per harness family and checks the
harness catches it, so none is vacuous.

### Files

| File | Role | Key exports |
| --- | --- | --- |
| `analysis/catalog.rs` | The topic catalog: `TopicCatalog` and `TopicLifecycle` (history, topics, lineage, assignments, retention; sizes of cross-agent assignments) | `InMemoryTopicCatalog` (`with_agents`), `CatalogConfig`, `TopicVersions` |
| `analysis/lineage.rs` | The lineage stored when a fit returns | `lineage_between`, `LineageError` |
| `analysis/search.rs` | Exact search and `SearchCorpus`, the verdict copy, projection sampling | `InMemorySearchIndex`, `InMemoryProjectionSource`, `FixedWatermark`, `ManualWatermark` (spec `WatermarkRead`s), `text_score`, `terms`, `sample_key` |
| `analysis/projection.rs` | Projection jobs, leases and frames (`FrameMismatch` for a frame of another job) | `InMemoryProjectionStore`, `ProjectionConfig`, `plus` |
| `analysis/alerts/mod.rs` | The alert store's state, `AlertReads` and commits; reads transmission routes from a `MemoryVerdicts` | `InMemoryAlertStore`, `AlertStoreConfig`, `CommitRefused`, `is_active`, `state_kind` |
| `analysis/alerts/rules.rs` | `AlertRuleStore`; `AlertRuleMaintenance` (remap on version ready, model changes) | — |
| `analysis/alerts/triage.rs` | `AlertTriage`; `AlertActions` (acknowledge and resolve) | — |
| `analysis/fakes.rs` | Deterministic doubles | `FakeEmbedder`, `FakeTopicModel`, `FakeLayoutFitter`, `FakeRuleContext`, `fake_model` |
| `analysis/aliases.rs` | Merges and supersessions a test sets | `StaticDirectory`, `Directories`, `AliasError` |
| `analysis/support.rs` | The similarity every score uses | `similarity` |
| `topology/store.rs` | The edge store's state and writes (`version_ready`, `activate`, the watermark); `WatermarkRead` | `InMemoryEdgeStore`, `EdgeStoreConfig`, `ManualFrontier`, `bucket_of` |
| `topology/reads.rs` | `EdgeStore` | — |
| `topology/fold.rs` | The fold, edges, nodes, access edges | `edges`, `nodes`, `route_key`, `kind_index` |
| `topology/env.rs` | What the edge store reads from other stores | `TopologyEnv`, `Env`, `StaticNodes` (a spec `NodeFacts`), `agent_facts`, `channel_facts` |
| `surface/audit.rs` | The append-only audit log | `InMemoryAuditLog` |
| `surface/operators.rs` | `OperatorStore`: the directory and its config loads | `InMemoryOperatorStore` |
| `surface/sinks.rs` | `SinkRegistry`, and a sink double | `InMemorySinkRegistry`, `SinkConfig`, `FakeSink` |
| `model/mod.rs` | The harness runner every harness shares | `HarnessConfig`, `ModelMismatch`, `Divergence`, `run`, `same`, `holds` |
| `model/build.rs` | Value builders the harnesses and tests share | id builders, `ts`, `window`, `unit`, `topic`, `catalog`, `timing`, `bucket_width` |
| `model/analysis.rs`, `model/analysis/*.rs` | The L6 harnesses | see the table above, and `ReferenceCatalog` (`reference_catalog`), `ReferenceSearch`, `SearchWorld`, `ReferenceAlerts` (`reference_alerts`), `new_version_in`, `FilterSeed` |
| `model/topology.rs`, `model/topology/*.rs` | The L7 harness | `check_edge_store`, `EdgeSubject`, `EdgeWorld`, `ReferenceEdges`, `catalog_ready`, `edge_config` |
| `model/surface.rs` | The L8 harnesses | `check_audit_log`, `check_operator_store`, `OperatorStoreSubject`, `ReferenceOperators` |

Unit tests are in `analysis/tests/`, `topology/tests/`, `surface/tests.rs`;
the harnesses run on the reference in `model/tests.rs` and against planted
bugs in `model/mutants.rs`.

### Invariants and constraints

- Every stored value goes through the spec's checked constructors and
  transitions, so a reference store never holds a value the spec forbids.
- A refused write changes nothing: lifecycle errors, rule errors, triage
  errors, projection transitions and audit `IdReused` are all checked
  before the first mutation, and multi-step changes (a disable with its
  suppressions, a load with its audit entries) are all or nothing.
- Nothing is published before it is visible: the outbox is appended in the
  critical section of the change.
- Determinism: ids come from per-store sequences (`IdSequence`, above the
  reserved rule range), times from the caller, and every
  output order is fixed (sorted keys), so the same calls give the same
  results.
- The evidence these tests provide for invariants that name
  `crosstalk_analysis::`, `crosstalk_topology::` or `crosstalk_surface::`
  paths is not flipped: those paths belong to the layer crates. The
  equivalent reference tests here are candidates for them.

### Behaviour the spec leaves open, as the reference decides it

- Similarity is the cosine of two unit vectors clamped to `0.0..=1.0`
  (negative cosines score 0).
- A triage draft for an unknown rule is `RuleInactive`.
- Enqueuing a job that is not queued is a `Store` error; enqueuing any job
  under a used id is a no-op.
- `AlertReads::rules` lists rules in `QueryApi::alert_rules`'s order:
  built-in rules first, in `BuiltinRule::ALL` order, then user rules
  newest id first (`analysis::alerts::rule_list_order`; a cursor resumes
  after the last rule served in that order).
- An agent `NodeFacts` has not seen is drawn provisional, top-level,
  unlabelled and without claims; a channel, which only a transmission
  edge can draw, discovered, active, unreviewed and confirmed and
  summarized by its id, and no access to it is drawn
  (`topology.node-facts.unknown-channel-defaults`; `StaticNodes` returns
  `None` for what it was never told, and `StaticNodes::set_resource` sets
  which channel holds a resource).
- `TopicSizes` lists topics in ascending id; graph edges and nodes, access
  edges and grouped series come sorted by key.
- A text hit needs at least one shared term; a semantic or hybrid hit
  needs an embedding of the query's model; a snippet is the first 160
  characters of the text.
- Retention skips a version superseded after the enforcement time; the
  next enforcement drops it.
- In the channel-centred view, a channel's topics count every
  channel-routed contribution in the window that the filter's
  `false_detections` keeps, resolved self-edges included.
