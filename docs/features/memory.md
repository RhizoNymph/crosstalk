# Memory: in-memory reference stores

`crosstalk-memory` (`crates/memory`) holds an in-memory implementation of
every stateful store trait in the spec, and a model-based property harness
per trait. Roadmap item P2.3, principle 3: each reference store is the
model the Postgres store is tested against, the backend for early L8 and UI
work, and the store behind the deterministic simulation. It is a
dev-dependency of the layer crates, never a normal one.

The crate is built in two halves: the pipeline stores (L3–L5) and the
insight and surface stores (L6–L8). Each half has a section below.

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
  results pass the spec's checked constructors (`TopologyGraph::check`,
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
  `WatermarkRead`.
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
`TopologyGraph::check` and shares summing to 1, and every series' total
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
