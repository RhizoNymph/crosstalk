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

The pipeline half (L3 to L5) and the insight and surface half (L6 to L8)
are separate sections of this page. Each half keeps its own harness runner
and `HarnessConfig`: `pipeline::harness` (64 cases by default; a failing
harness panics) for L3 to L5, and `model` (48 cases by default; a failing
harness returns a `ModelMismatch`) for L6 to L8. Both draw sequences of at
most 40 operations and run each case on a fresh current-thread runtime.

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

## Insight and surface stores (L6–L8)

### Scope

- L6 (`crosstalk_spec::interfaces::l6_analysis`): `TopicCatalog`,
  `SearchIndex` (exact search over the stored text and embeddings),
  `ProjectionSource`, `ProjectionStore`, `AlertRuleStore` and `AlertTriage`;
  deterministic `Fake*` doubles of the computational traits (`Embedder`,
  `TopicModel`, `LayoutFitter`) and of `RuleContext`.
- L7 (`l7_topology`): `EdgeStore`, with buckets per topic version,
  activation, the watermark, retention and pins (through the catalog), the
  graph, series, the channel-centred graph, totals, the edge drill-down and
  agent traffic, all computed from the stored contributions; and a
  `FrontierSource` the test sets.
- L8 (`l8_surface*`): `AuditLog`, the store behind `OperatorDirectory`, the
  sink registry (`QueryApi::sinks`' data) and an `AlertSink` double.
- The model-based harnesses for all of the above (`model::analysis`,
  `model::topology`, `model::surface`).

### Non-scope

- `QueryApi`, `OperatorActions` and `LiveFeed`: composed over these stores
  by `crosstalk-surface` (P2.6). The stores give it what it needs that the
  spec traits do not (alert listing and acknowledgement, rule listing, the
  operator directory's reads), as inherent methods.
- Bus publishing. Each store appends what the spec says it publishes to an
  outbox, in the same critical section as the change; the consumer drains
  it (`Published`) and forwards it.
- Embedding, topic fitting and layout. The `Fake*` doubles are stable
  stand-ins, not reference models.

### Data and control flow

```text
            ┌──────────── InMemoryTopicCatalog ◀── begin_fit / fit_returned / ready / activated / assign (analyze)
            │   history, topics, lineage, assignments, frozen sizes; pin / unpin / enforce_retention
            │        │ TopicVersions (history, version_of, topic_ids)        │ assignment(v, t), retains(v)
            ▼        ▼                                                       ▼
 InMemoryEdgeStore ◀─ Env { catalog, directory, nodes }        InMemorySearchIndex ── InMemoryProjectionSource
   contributions, accesses, verdict copy,                        documents, verdict copy     (+ WatermarkRead)
   watermark, activated / dropped versions                       │
   every read = the fold over contributions                      └─ search / sample under one resolved version
            │ WatermarkRead
            ▼
 InMemoryProjectionStore (jobs, leases, frames)      InMemoryAlertStore (rule set, alerts, verdict copy)
                                                        ▲ create / update / set_enabled (surface)
                                                        ▲ triage / sanctioned / disabled / judged (alerts consumer)
                                                        ▲ topic_version_ready (lineage) / embedding_model_changed
 InMemoryOperatorStore ── load(config) ──▶ InMemoryAuditLog (config entries, same lock order)
 InMemorySinkRegistry (configured sinks, last delivery)
```

- **One lock per store.** Every store keeps its state behind one
  `std::sync::Mutex` in an `Arc` (cloning shares the store) and takes it
  once per call, so each call is one transaction. No lock is held across an
  `.await`: the only awaiting store call, embedding a semantic rule's query,
  runs before the lock is taken. Stores that read another store take their
  own lock first and never the reverse (index → catalog, edge store →
  catalog, directory, nodes; alerts → directory; operators → audit log), so
  there is no lock cycle. Every store is `Send + Sync`.
- **Paging.** Every list pages with a `CursorBook`
  (`surface::paging`): a token is a key into the issuing store's book,
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
  whatever the floor, the others at or above it. `activated` supersedes
  every older version not yet superseded, then enforces retention, which
  freezes the all-time sizes, deletes the assignments and publishes
  `TopicVersionDropped` for each dropped version.
- **Edge store.** It stores contributions keyed by (version,
  transmission) and accesses keyed by id, never buckets. `apply` checks, in
  order: dropped version, self-edge, already applied (returns the stored
  key), late (activated version, bucket final under the watermark). A
  version activates once `version_ready` has its count and that many
  distinct `Refit` classifications under it were processed (applied,
  already applied or self-edges). Reads take the watermark first, resolve
  the filter's selector against the catalog's history (retained = not
  dropped here), refuse topics outside the version, and run the fold:
  contributions of the version in the window, agents resolved, resolved
  self-edges dropped, routes resolved, the filter admitted (false
  detections from the store's verdict copy). Graph, totals
  (`EdgeTotals::of` the graph), the channel-centred graph (plus access
  buckets), the drill-down, agent traffic (the graph's node counts) and
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
| `model::analysis::check_topic_catalog` | `CatalogSubject`: `TopicCatalog` + the fit lifecycle, assignments, the clock | `CatalogConfig` |
| `model::analysis::check_search_index` | `SearchSubject`: `SearchIndex` + `ProjectionSource` + indexing, verdicts, merges, supersessions, versions, assignments, the watermark | `EmbeddingModel` |
| `model::analysis::check_projection_store` | `ProjectionStore` | `ProjectionConfig` |
| `model::analysis::check_alert_rule_store`, `check_alert_triage` | `AlertStoreSubject`: both alert traits + the consumer's rule changes, acknowledge and resolve, full reads, supersessions, the clock | `AlertWorld` |
| `model::topology::check_edge_store` | `EdgeSubject`: `EdgeStore` + classification causes, `version_ready`, activation, merges, supersessions, parents, the catalog | `EdgeStoreConfig` |
| `model::surface::check_audit_log` | `AuditLog` | nothing |
| `model::surface::check_operator_store` | `OperatorStoreSubject`: load, operators, caller, the config entries | nothing |

Beyond equality, the harnesses keep their own oracles: the catalog's
sizes against a count of the assignments (`analysis.sizes.match-
assignments`); the queue bound (`analysis.projection.queue-bounded`); at
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
| `analysis/catalog.rs` | The topic catalog: history, topics, lineage, assignments, retention | `InMemoryTopicCatalog`, `CatalogConfig`, `TopicVersions`, `StoredAssignment`, `Assigned`, `Activated`, `LifecycleError` |
| `analysis/lineage.rs` | The lineage stored when a fit returns | `lineage_between`, `LineageError` |
| `analysis/search.rs` | Exact search, the verdict copy, projection sampling | `InMemorySearchIndex`, `IndexedTransmission`, `InMemoryProjectionSource`, `WatermarkRead`, `FixedWatermark`, `ManualWatermark`, `text_score`, `terms`, `sample_key` |
| `analysis/projection.rs` | Projection jobs, leases and frames | `InMemoryProjectionStore`, `ProjectionConfig`, `plus` |
| `analysis/alerts/mod.rs` | The alert store's state, reads and commits | `InMemoryAlertStore`, `AlertStoreConfig`, `CommitRefused`, `AlertReadError`, `is_active`, `state_kind` |
| `analysis/alerts/rules.rs` | `AlertRuleStore`; remap on version ready; model changes | `InMemoryAlertStore::{topic_version_ready, embedding_model_changed, start}` |
| `analysis/alerts/triage.rs` | `AlertTriage`; acknowledge and resolve | `AlertActionError`, `InMemoryAlertStore::{acknowledge, resolve}` |
| `analysis/fakes.rs` | Deterministic doubles | `FakeEmbedder`, `FakeTopicModel`, `FakeLayoutFitter`, `FakeRuleContext`, `fake_model` |
| `analysis/aliases.rs` | Merges and supersessions a test sets | `StaticDirectory`, `Directories`, `AliasError` |
| `analysis/support.rs` | Clock, id sequences, outbox, similarity | `Clock`, `ManualClock`, `IdSequence`, `Outbox`, `Published`, `similarity` |
| `topology/store.rs` | The edge store's state and writes | `InMemoryEdgeStore`, `EdgeStoreConfig`, `Activation`, `ManualFrontier`, `bucket_of` |
| `topology/reads.rs` | `EdgeStore` | — |
| `topology/fold.rs` | The fold, edges, nodes, access edges | `edges`, `nodes`, `route_key`, `kind_index` |
| `topology/env.rs` | What the edge store reads from other stores | `TopologyEnv`, `Env`, `NodeDescriptions`, `StaticNodes`, `AgentDescription`, `ChannelDescription` |
| `surface/audit.rs` | The append-only audit log | `InMemoryAuditLog` |
| `surface/operators.rs` | The operator directory's store | `InMemoryOperatorStore`, `LoadError`, `CallerError` |
| `surface/sinks.rs` | The sink registry and a sink double | `InMemorySinkRegistry`, `SinkConfig`, `UnknownSink`, `FakeSink` |
| `surface/paging.rs` | Cursor books and page cutting | `CursorBook`, `page_after`, `PageError` |
| `model/mod.rs` | The harness runner | `HarnessConfig`, `ModelMismatch`, `Divergence` |
| `model/build.rs` | Value builders the harnesses and tests share | id builders, `ts`, `window`, `unit`, `topic`, `catalog`, `timing`, `bucket_width` |
| `model/analysis.rs`, `model/analysis/*.rs` | The L6 harnesses | see the table above, and `ReferenceCatalog`, `ReferenceSearch`, `ReferenceAlerts`, `FilterSeed` |
| `model/topology.rs`, `model/topology/*.rs` | The L7 harness | `check_edge_store`, `EdgeSubject`, `ReferenceEdges`, `edge_config` |
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
  reserved rule range), times from the caller or a `ManualClock`, and every
  output order is fixed (sorted keys), so the same calls give the same
  results.
- The evidence these tests provide for invariants that name
  `crosstalk_analysis::`, `crosstalk_topology::` or `crosstalk_surface::`
  paths is not flipped: those paths belong to the layer crates. The
  equivalent reference tests here are candidates for them.

### Behaviour the spec leaves open, as the reference decides it

- Similarity is the cosine of two unit vectors clamped to `0.0..=1.0`
  (negative cosines score 0).
- `AlertTriage::channel_sanctioned`, `rule_disabled`, `transmission_judged`
  and `TopicCatalog::unpin` take no time; the stores read a `Clock`.
- `EdgeContribution` carries no classification cause, yet activation
  counts `Refit` classifications: the store adds `apply_classified(_,
  cause)` and `version_ready`, and the trait's `apply` counts as a
  confirmation. `EdgeStore::activate` returns `()`; `activate_if_complete`
  says whether it switched.
- A triage draft for an unknown rule is `RuleInactive`.
- Enqueuing a job that is not queued is a `Store` error; enqueuing any job
  under a used id is a no-op. A completed frame that does not belong to
  its job is `Transition(NotAllowed { Fitting → Ready })`.
- Resolving an open alert is refused (`NotAcknowledged`); there is no
  `ConflictKind` for it.
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
