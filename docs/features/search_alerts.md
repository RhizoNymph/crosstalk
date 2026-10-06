# L6 on Postgres: search, alerts, topics, projections

`crosstalk-analysis` (`crates/analysis`), roadmap item P6.2: the L6 search
index and alert store on Postgres, rule evaluation, and the `alerts` bus
consumer. Implements `crosstalk_spec::interfaces::l6_analysis`. The topic
catalog, projection store, stable outbox ids and the classification step
(P7.3 W5) are in the second half of this page.

## Scope

- `search::PgSearchIndex`: `SearchIndex` and `SearchCorpus`.
- `search::PgProjectionSource`: `ProjectionSource` over the same documents.
- `alerts::PgAlertStore`: `AlertRuleStore`, `AlertTriage`,
  `AlertRuleMaintenance`, `AlertActions`, `AlertReads`, one transaction
  scope over rules, alerts and triage's verdict copy.
- `alerts::eval::RuleEvaluator`: `AlertRuleEval` for every rule kind.
- `alerts::consumer::AlertsStage`: the `alerts` consumer group, plus
  `FlowContext` (the wiring's `RuleContext`).
- `pg`: what the stores share: migrations, column codec, keyed cursors,
  keyset pages, the failure mapping (`pg::tx`), the outbox and its relay
  (stable envelope ids; see `analysis_pg_stores`).

## Non-scope

- The embedder, topic model and layout fitter (P6.3's sidecar; this code
  uses the spec's `Embedder` trait only).
- The Postgres `TopicCatalog` and `ProjectionStore` (`analysis_pg_stores`).
  Search reads topics through the local `TopicAssignments` trait, which
  `PgTopicCatalog` implements; the search tests use the memory catalog.
- Sinks and alert delivery, and wiring into the gateway.

## Data and control flow

```text
analyze ── SearchCorpus::index/remove/judge/set_model/drop_model ──▶ analysis.search_*
surface ── SearchIndex::query ──▶ resolve version (TopicAssignments) ─▶ ranked SQL batches
                                  ─▶ FilterSubject per row (directories, topics, verdict copy)
                                  ─▶ admitted hits until size + 1 ─▶ page + keyed cursor
bus ── alerts group (SUBJECTS) ──▶ AlertsStage::handle
   detections ─▶ AlertReads::rules (evaluating) ─▶ RuleEvaluator::evaluate(envelope, context)
              ─▶ AlertTriage::triage
   PolicyChanged / ChannelPromoted (Sanctioned) ─▶ channel_sanctioned(channel, envelope time)
   VerdictSet ─▶ transmission_judged(…, verdict time)
   TopicVersionReady ─▶ TopicCatalog lineage from the predecessor + topics ─▶ topic_version_ready
   start ─▶ embedding_model_changed(embedder model)
PgAlertStore write: SERIALIZABLE tx (retry_serializable): read rows ─▶ spec transition
   ─▶ write rows + revision CAS ─▶ append events to analysis.outbox ─▶ commit
   ─▶ relay: stamp rows (envelope id + time, committed) ─▶ EventSink::publish
   ─▶ delete outbox rows (leftovers: flush_outbox, same ids)
```

### Search scores

| Mode | Hits | Score |
| --- | --- | --- |
| `Text` | documents sharing a `simple`-configuration lexeme with the query (GIN on `terms`) | fraction of the query's lexemes the document holds |
| `Semantic` | documents with an embedding of the query's model | cosine, `sum(a * b ORDER BY i)` in `real`, clamped to `0..=1` |
| `Hybrid` | documents with an embedding of the query's model | mean of the two |

Embeddings are stored as pgvector `vector`, but the cosine is computed in
ordered `real` arithmetic rather than with pgvector's operators: those use
fused and reordered sums whose last bits differ from the reference's `f32`
sum, and the spec's model check compares semantic scores exactly. Search is
an exact scan (no ANN index), which the spec's full-traversal and stable
score guarantees require anyway.

## Files

| File | Role |
| --- | --- |
| `migrations/0001_search.sql` | search tables, `outbox` |
| `migrations/0002_alerts.sql` | `alert_rules`, `alert_rule_state`, `alerts` (partial unique index: one active alert per rule and subject), `alert_verdicts` |
| `src/pg/mod.rs` | `MIGRATIONS`, `run_migrations`, `StorageFailure` |
| `src/pg/codec.rs` | ids as ULID text (`COLLATE "C"`), micros, wire JSON |
| `src/pg/cursor.rs` | `CursorKey`, `issue`, `resume`: position plus keyed BLAKE3 tag over list and request |
| `src/pg/outbox.rs` | `EventSink` (`stamp`, `publish`), `BusSink`, `ChannelSink`, `append`, `deliver`, `flush`: the relay with stable envelope ids (`analysis_pg_stores`) |
| `src/pg/tx.rs` | `Failure`, `abort`, `fail`, `finish`: storage failures as each spec error's `Store` |
| `src/pg/paging.rs` | `Binding`, `page_of`: keyset pages with keyed cursors |
| `src/search/mod.rs` | `PgSearchIndex`, `TopicAssignments`, `similarity` |
| `src/search/text.rs` | the scoring SQL per mode |
| `src/search/query.rs` | `SearchIndex::query` |
| `src/search/corpus.rs` | `SearchCorpus` |
| `src/search/sample.rs` | `PgProjectionSource`, `sample_key` |
| `src/alerts/mod.rs` | `PgAlertStore`, `AlertStoreConfig`, `AlertStoreParts`, `open` (provisions built-ins) |
| `src/alerts/store.rs` | row loads and saves, revision CAS, `Failure` |
| `src/alerts/rules.rs` | `AlertRuleStore`, `AlertRuleMaintenance` |
| `src/alerts/triage.rs` | `AlertTriage`, `AlertActions` |
| `src/alerts/reads.rs` | `AlertReads`, `rule_list_order` |
| `src/alerts/facts.rs` | `SubjectFacts`, `FlowFacts`, `NoFacts` |
| `src/alerts/eval/mod.rs` | `RuleEvaluator` |
| `src/alerts/consumer.rs` | `GROUP`, `SUBJECTS`, `AlertsStage`, `FlowContext`, `EmbeddingSource` |

## Invariants and constraints

- Every write is one `SERIALIZABLE` transaction; changes to one rule or
  alert are compare-and-set on its revision; an exhausted counter is a
  store failure before anything changes.
- Inside a transaction body a driver error (`StorageFailure::Query`, or
  `OutboxError::Db` from the outbox append) goes back to
  `retry_serializable` as `TxError::Db` (`StorageFailure::into_tx`), so a
  serialization failure or deadlock re-runs the transaction. Only
  non-driver failures (codec, invariant, revision exhaustion, spec
  refusals) abort. A conflict that outlasts the retry budget is the spec
  error's `Store { reason }`.
- At most one active alert per (rule, stored subject), backed by a partial
  unique index. `open_alert` inserts `ON CONFLICT (rule, subject) WHERE
  state IN ('open', 'acknowledged') DO NOTHING`, so a concurrent opener
  whose row the snapshot cannot see raises a serialization failure (not a
  unique violation) and the retry deduplicates into the winner's alert.
- Events are published at least once and only after commit.
- Rules list built-in rules first in `BuiltinRule::ALL` order, then user
  rules newest id first; rule ids are ULIDs minted at `at`, never in the
  reserved range (minted at 1 ms or later).
- The alerts list leaves out subjects `SubjectFacts::shown` says readers do
  not show (`AlertSubject::shown`); the memory reference does not filter.
- Search applies the filter before cutting the page; cursors pin the
  resolved topic version and are bound to the query, window and filter.
- The stage skips envelopes it already handled while running; a
  redelivery after a crash, or after a failure partway through an
  envelope's drafts, counts again as an occurrence.

## Tests

- Model harnesses from `crosstalk-memory`: `check_search_index`,
  `check_alert_rule_store`, `check_alert_triage` (on Postgres, a fresh
  current-thread pool per case on emptied tables).
- Focused Postgres cases per invariant in `search/tests/cases.rs` and
  `alerts/tests/{triage,rules,reads,consumer}.rs`; rule evaluation unit and
  property tests in `alerts/eval/tests.rs` (no database).
- Database tests skip without `TEST_DATABASE_URL`.

# Topic catalog, projections, stable outbox ids, classification (P7.3 W5)

Workstream W5 of [postgres_stores](postgres_stores.md): what L6 needs so a
restarted gateway ends up where an uninterrupted one would.

## Scope (W5)

- `topics::PgTopicCatalog`: the spec's `TopicCatalog` and
  `TopicLifecycle`, and the crate's `TopicAssignments` (what search and
  sampling read), over `migrations/0004_topics.sql`. The one publisher of
  `TopicVersionDropped`.
- `projections::PgProjectionStore`: the spec's `ProjectionStore`, over
  `migrations/0005_projections.sql`.
- `pg::outbox`: the relay stamps each outbox row with its envelope id and
  time in a committed transaction before its first publish
  (`migrations/0003_outbox_ids.sql`, INV-1213
  `analysis.outbox.stable-envelope-id`). The alert store, the catalog and
  the projection store all publish through it.
- `classify::Classifier`: the `analyze` consumer's classification step
  (`TransmissionConfirmed` in, `TransmissionClassified` envelope out),
  idempotent on redelivery, its envelope id derived from the delivery's
  (`EventId::derive`).
- `pg::tx` (failure mapping) and `pg::paging` (keyset pages), shared.

## Non-scope (W5)

- Wiring (W8, `crates/gateway`): building these stores over the gateway's
  pool, `BusSink` over `PgBus`, and replacing the gateway's
  `live::classify::Classifier` with `crosstalk_analysis::classify::Classifier`.
- A topic model in the classification step: a fresh classification is an
  outlier under the active version, as the gateway's minimal classifier
  decides today. Refits and re-classification are not here.

## Data and control flow (W5)

The outbox relay:

```text
store write (SERIALIZABLE): change + append events to analysis.outbox (unstamped) ─▶ COMMIT
relay (deliver: the write's own rows; flush: every row, batches of 256):
  1. txn: SELECT seq, event, envelope_id, at ... ORDER BY seq FOR UPDATE SKIP LOCKED
          unstamped row ─▶ EventSink::stamp() (ULID generator at the injected clock's reading)
          ─▶ UPDATE envelope_id, at ─▶ COMMIT                              (ids now fixed)
  2. per row in seq order: EventSink::publish(Envelope { id, at, event }); stop at a refusal
  3. DELETE the published rows
```

A relay that stops after step 1, during step 2 or before step 3 leaves its
rows stamped; the next relay (`flush`, which every store's `open` runs)
republishes them under the same ids, which `PgBus` deduplicates. A row is
invisible to every relay until its staging transaction commits. Concurrent
relays skip each other's locked rows; a row both relay after its stamp
committed is published twice under one id.

The topic catalog:

```text
write: retry_serializable {
  rows::versions (topic_versions by version ─▶ TopicVersionHistory::new; fit_returned_at)
  ─▶ the spec's transition (TopicVersionInfo::new / with_retention, History::new, pin,
     unpin, RetentionPolicy::to_drop, mark_dropped); a refusal aborts: nothing changes
  ─▶ store the changed versions (save_changed), insert, or remove a failed fit
  ─▶ enforce (mark_active, unpin, enforce_retention), per version to drop:
       count sizes (topics + assignments + AgentDirectory) ─▶ mark_dropped in the history
       ─▶ UPDATE info, retained = false, frozen_sizes ─▶ DELETE its assignments
  ─▶ outbox: Changed::TopicVersion (ready, activation, superseded, pin, unpin, drop),
     TopicVersionDropped
} ─▶ relay
read: one REPEATABLE READ READ ONLY snapshot
```

- `begin_fit` takes the number from the `topic_catalog` counter row, not
  a sequence: a refused or retried call consumes none, and a failed fit's
  number stays taken (restarts included).
- `complete_fit` stores the topics, the lineage from the predecessor
  (`topics::lineage_between`: the reference's ranking over
  `search::similarity`) and `fit_returned_at`; `mark_ready` clears it.
- `assign` is keyed by (version, transmission): the same assignment again
  is `Unchanged`, another `Conflicting`.
- `sizes` of a retained version counts its assignments in Rust at the
  read through the `AgentDirectory`; of a dropped one, the frozen sizes.

The projection store, one serializable transaction per call over
`projection_jobs` (`ProjectionInfo` wire JSON plus `state`,
`requested_at`, `lease_until`, `fitted_at`) and `projection_frames`
(`ProjectionFrame::encode` bytes):

- `enqueue`: a known id is a no-op; otherwise a queued job only, while
  fewer than `MAX_PENDING` are queued or fitting (counted in the
  transaction).
- `claim(at)`: the oldest queued job by (`requested_at`, id), `FOR UPDATE
  SKIP LOCKED`, `start(at)`, lease until `at + lease`.
- `complete`: `Fitting` only; `Projection::new` checks the frame; `Ready`
  and the frame in one transaction.
- `fail`; `requeue_lapsed(now)` (`lease_until < now`); `expire(now)`
  (`fitted_at < now - frame_retention`, frame deleted); in id order.
  `complete`, `fail` and each expiry publish `Changed::Projection`.

The classification step:

```text
TransmissionConfirmed (delivery d) ─▶ Classifier::classify(&d):
  stored = TransmissionStore::transmission(id)
  classification = stored Classified / Aggregated ? its classification
                 : outlier under the active version, and, when stored is Confirmed,
                   save Classified { confirmed, classification } before anything else
  TopicLifecycle::assign(id, classification.version, ..)
      Applied | Unchanged | Conflicting (warn) | VersionNotRetained (warn)
  ─▶ Some(Envelope { id: EventId::derive(d.id, "transmission-classified", 0), at: d.at,
                     TransmissionClassified { cause: Confirmation, .. } })
composer: publish, then ack
```

The gateway's classifier (`crates/gateway/src/live/classify.rs`) assigns,
then saves, then publishes under a fresh id, re-reading the active version
on every delivery. Read against the redelivery cases, it is not
idempotent: a topic-model version activated between two deliveries of one
confirmation makes the redelivery store a second assignment under the new
version, and after a crash between its save and its publish it publishes
the new version while the store holds the old. This step decides once,
saves the decision before any other effect and replays it, so a
redelivery redoes nothing and returns the same envelope. One case cannot
replay exactly: a confirmed transmission the store does not hold (logged
at warn) has nowhere to record its decision.

## Files (W5)

| File | Role | Key exports |
| --- | --- | --- |
| `migrations/0003_outbox_ids.sql` | `envelope_id`, `at` on `outbox` (both or neither) | - |
| `migrations/0004_topics.sql` | `topic_catalog`, `topic_versions`, `topics`, `topic_lineage`, `topic_assignments` | - |
| `migrations/0005_projections.sql` | `projection_jobs` (partial indexes per state), `projection_frames` | - |
| `src/pg/outbox.rs` | the relay | `EventSink` (`stamp`, `publish`), `BusSink`, `ChannelSink`, `Pending`, `append`, `deliver`, `flush` |
| `src/topics/mod.rs` | the catalog | `PgTopicCatalog`, `CatalogConfig`, `CatalogParts`, `lineage_between` |
| `src/topics/{rows,lifecycle,catalog,lineage}.rs` | rows and size counting; `TopicLifecycle`; `TopicCatalog`, `TopicAssignments`, retention; lineage | - |
| `src/projections/{mod,store}.rs` | the projection store | `PgProjectionStore`, `ProjectionStoreConfig`, `ProjectionParts`, `plus` |
| `src/classify/mod.rs` | the classification step | `Classifier`, `ClassifyError`, `CLASSIFIED_LABEL` |
| `src/integration/mod.rs` | outbox relay crash points (tests) | - |

Opening: `PgTopicCatalog::open(pool, CatalogConfig, started_at,
CatalogParts { agents, sink, cursor_key, retry })` records version 0
(active since `started_at`) on the first open only;
`PgProjectionStore::open(pool, ProjectionStoreConfig, ProjectionParts {
sink, cursor_key, retry })`. Both flush the outbox first. The wiring's
sink is `BusSink::new(bus, clock, UlidGenerator)` seeded from entropy
(`surface.ids.unique-across-restart`); the step is
`Classifier::new(catalog, transmissions).classify(&delivery)`.

## Invariants and constraints (W5)

- INV-1213 `analysis.outbox.stable-envelope-id`: each staged event is
  published under one envelope id, stamped in a committed transaction
  before its first publish, never before the staging commit; a stamped
  row is never stamped again.
- Every catalog and projection write is one `SERIALIZABLE` transaction; a
  refusal changes nothing and publishes nothing. Stored histories always
  pass `TopicVersionHistory::new`.
- Frozen sizes are written in the statement that marks a version dropped,
  its assignments deleted after (`analysis.retention.mark-before-delete`);
  `topic_versions` checks frozen sizes exactly when not retained.
- Version numbers are dense and never reused, restarts included.
- At most `MAX_PENDING` projection jobs are queued or fitting under
  concurrent enqueues; a fitting job has a lease, a ready one its fit time.
- No store reads a clock: leases, expiry and version times are arguments;
  envelope ids and times come from the sink's injected clock.
- The classification step's effects are keyed (the saved classification,
  the (version, transmission) assignment), and its envelope id is a
  function of the delivery's (`transport.consumer.derived-envelope-ids`).

## Tests (W5)

- `topics::tests::model::catalog_matches_the_reference` (`check_topic_catalog`);
  `topics::tests::cases`: redelivered and conflicting assignments, a
  restart, a drop (frozen sizes, deleted assignments, events once), pin
  and unpin, concurrent `begin_fit`, sizes under merges.
- `projections::tests`: `check_projection_store`, concurrent enqueues at
  the bound, concurrent claims, a restart with a lapsed claim.
- `integration`: the relay stopping after the stamp, after the publish and
  before the delete; nothing relayed before the commit; concurrent relays.
- `classify::tests`: redelivery after a version change (memory stores and
  `PgTopicCatalog`), a crash after the save, a fresh confirmation, other
  subjects, distinct deliveries.
