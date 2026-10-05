# L6 search and alerts on Postgres

`crosstalk-analysis` (`crates/analysis`), roadmap item P6.2: the L6 search
index and alert store on Postgres, rule evaluation, and the `alerts` bus
consumer. Implements `crosstalk_spec::interfaces::l6_analysis`.

## Scope

- `search::PgSearchIndex`: `SearchIndex` and `SearchCorpus`.
- `search::PgProjectionSource`: `ProjectionSource` over the same documents.
- `alerts::PgAlertStore`: `AlertRuleStore`, `AlertTriage`,
  `AlertRuleMaintenance`, `AlertActions`, `AlertReads`, one transaction
  scope over rules, alerts and triage's verdict copy.
- `alerts::eval::RuleEvaluator`: `AlertRuleEval` for every rule kind.
- `alerts::consumer::AlertsStage`: the `alerts` consumer group, plus
  `FlowContext` (the wiring's `RuleContext`).
- `pg`: what the stores share: migrations, column codec, keyed cursors, the
  outbox and its `EventSink`s.

## Non-scope

- The embedder, topic model and layout fitter (P6.3's sidecar; this code
  uses the spec's `Embedder` trait only).
- A Postgres `TopicCatalog`. Search reads topics through the local
  `TopicAssignments` trait; tests use the memory catalog.
- `ProjectionStore`, sinks and alert delivery, and wiring into the gateway.

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
   ─▶ EventSink::publish ─▶ delete outbox rows (leftovers: flush_outbox)
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
| `src/pg/outbox.rs` | `EventSink`, `BusSink`, `ChannelSink`, `append`, `deliver`, `flush` |
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
