# Evaluation harness (`crosstalk-eval`)

The eval turns public multi-agent datasets into per-agent sequences of the
spec's `NormalizedExchange`s with ground-truth labels. It scores any
detector's transmissions against those labels and ships a deliberately
naive reference matcher. The matcher validates the labels today and gives
the gateway's detection a baseline to beat. The first dataset is SALT-NLP
("Emergent Collusion in Long-Horizon LLM Agent Interaction").

The crate is `crosstalk-eval` (`crates/eval`). Its binary is `ct-eval`. It
is a composer in the workspace's dependency rule
(`crates/gateway/tests/architecture.rs`), so it may depend on
`crosstalk-gateway` and the layer crates; today it uses the gateway's
pipeline and transport's bus and blob store, with sim, testkit and memory
as dev-dependencies.

## Scope

- **Corpus model.** `TraceSource` is a stream of `World`s: sets of agents
  that only talk to each other. Each world holds its exchanges in
  virtual-time order and its truth.
- **Labels.** Expected transmissions, negative controls, exemptions
  (places left unjudged) and agent clusters, with tiers, as JSONL.
- **Predictions.** The eval-side view of a detector's output, converted
  from spec `Transmission`s: their `ContentMatch`es, and the `CoAccess`
  records of suspected and discarded ones, read through the spec's read
  traits (`SpanIndex`, `AccessStore`, `ChannelReads`/`resource_use`).
- **Scoring.** One alignment rule, TP/FP/FN broken down by dataset × route
  kind × carrier × match class × tier, negative-control violations, and a
  bridge to the spec's `DetectionQuality`.
- **Detectors.** A naive reference matcher (span/shingle matching with
  escape-aware normalization and decoding), the gateway pipeline itself
  (`Pipeline::ingest`, unscored: it has no detection consumers), and the
  `LiveBackend` seam that scores the gateway's live composition
  (`crosstalk_gateway::live::Live`, L3–L7) through its adapter,
  `detect::live::gateway`.
- **Reports and gates.** A table, a JSON report (with `overall`, an
  `out_of_reach` summary and an `access_only` recall kept apart from
  it), and regression gates in `gates.toml`, found by `GateSearch`
  (below).
- **The SALT converter.**
- **The AgentDojo and τ²-bench converters** (see their sections below).
- **The demo swarm benchmark** (`ct-eval swarm`): the live gateway scored
  on the demo swarm's traffic against the swarm's own ground truth (see
  its section below).

## Non-scope

- **Gateway detection.** The eval is not L4/L5. The reference matcher is
  not the provenance layer and never will be.
- **Dataset bytes.** None are committed. Datasets are read from a
  configured root; tests use small synthetic fixtures.
- **Statistical thresholds as invariants.** Thresholds are regression gates
  in eval config, not gateway invariants.
- **Semantic matching, identity resolution and channel discovery.**
  `AgentCluster` labels exist for later identity tests; SALT emits none.

## Data and control flow

```text
dataset files ──▶ TraceSource::worlds()          (one World at a time; e.g. SaltSource, one trace file each)
                    │  WorldBuilder: agents (synthetic ClientContext, IngressMode::Replay { corpus }),
                    │  exchanges (ExchangeDraft → spec Exchange → checked NormalizedExchange → CorpusExchange),
                    │  labels, coverage
                    ▼
                  World { agents, exchanges (time order), truth, coverage }
                    │
                    ├──▶ Detector::detect(&World) ─▶ Detection { transmissions, agents: AgentMap, resolved }
                    │        ReferenceDetector, PipelineDetector or LiveDetector (below)
                    │        resolved = Resolved::gather(transmissions, Reads { spans, accesses, channels })
                    │                   SpanIndex::spans · AccessStore::accesses · ChannelResources (ChannelReads +
                    │                   ChannelRegistry::resource_use), by IdBatch
                    │                 │
                    │                 ▼
                    │        predict::from_transmission (via WorldDirectory) ─▶ Vec<Prediction>
                    │          confirmed: one per ContentMatch · suspected/discarded: one per CoAccess
                    ▼                 ▼
                  score::Judge (alignment rule) ─▶ Scorer::add_world ─▶ Score
                                                         │
                                         report::Report (+ gates::Gates::evaluate) ─▶ table, report.json, exit code
```

`pipeline::run` drives this loop. A world that fails to load, detect or
predict is recorded in the run's failures and skipped; the run goes on.
Worlds are processed one at a time and dropped after scoring, so memory is
bounded by the largest world, never the dataset.

### The detector seam (`Detector`)

`pipeline::Detector::detect(&World) -> Detection` is the only place a
detector plugs in. Everything after it is detector-agnostic: predictions,
scoring, reports and gates.

A `Detection` holds the spec `Transmission`s, an `AgentMap` (the
detector's agent ids as corpus agents) and a `Resolved` snapshot: every
span, access and channel the transmissions name, read through the spec's
read traits by `Resolved::gather` (`predict/reads.rs`), in batches of at
most `IdBatch::MAX`. That one code path serves every detector:

- the reference matcher, over eval-owned tables (`predict/memory.rs`:
  `SpanTable: SpanIndex`, `AccessTable: AccessStore`,
  `ChannelTable: ChannelResources`), whose reads never suspend and run
  without a runtime (`reads::ready`);
- crosstalk-memory's `MemoryFingerprintIndex` (`SpanIndex`) and
  `MemoryChannels` (`AccessStore`, and `ChannelReads` +
  `ChannelRegistry::resource_use` through `RegistryResources`), as the
  live test backend uses;
- the gateway's own stores, through `LiveBackend`.

- `ReferenceDetector` runs the reference matcher.
- `gateway::PipelineDetector` runs the gateway's own composition behind
  the proxy, per world, on a fresh in-process bus and memory blob store:
  1. `Pipeline::build(Settings::default(), Deps::stores(MemoryBlobStore,
     MpscBus, SeededRandom::new(seed)), clock)`, where the clock is a
     `CorpusClock` set to each exchange's corpus time before it is
     ingested;
  2. a counting consumer group (`eval-captured`) subscribed to
     `ExchangeCaptured` before the first ingest;
  3. for each `CorpusExchange` in world order,
     `pipeline.ingest(exchange.normalized().clone(), exchange.at())`, then
     the envelope read back and checked: it names the exchange, carries the
     id `ingest` returned and is stamped at the corpus time;
  4. `pipeline.shutdown(deadline)`.
- `Pipeline` alone has no detection consumers, so no transmissions come
  back. The detection says `DetectionStatus::NoConsumers { ingested }`;
  the run counts the world as unscored (`RunSummary::unscored`), and the
  report says "no detector consumers yet" instead of scoring zero. The
  detection layers are scored through `LiveDetector` instead.
- `gateway::ingest_world` is the same loop over any spec `BlobStore` and
  `EventBus`. The smoke test (`tests/pipeline.rs`) runs it under
  crosstalk-sim's clock (its default epoch is the corpus clock's,
  2026-01-01), advancing paused time to each exchange's corpus time.

`ct-eval run --detector pipeline` runs the pipeline path.

### The live seam (`detect::live`)

`LiveDetector<B: LiveBackend>` scores the gateway's live composition. The
seam is two traits with exactly the operations the composition offers
(agreed with the implementation session before `Live` merged; the
differences are under "The adapter" below):

```rust
pub trait LiveBackend {
    type World: LiveWorld;
    /// A fresh composition: empty stores, clock at `start`, correlator under `settings.timing`.
    fn build(&mut self, settings: &LiveSettings, start: Timestamp)
        -> impl Future<Output = Result<Self::World, BackendError>>;
}

pub trait LiveWorld {
    type Spans: SpanIndex + Sync;
    type Accesses: AccessStore + Sync;
    type Channels: ChannelResources + Sync;
    /// Pipeline::ingest(exchange, at), the clock moved to `at` first.
    fn ingest(&mut self, exchange: NormalizedExchange, at: Timestamp) -> impl Future<Output = Result<(), BackendError>>;
    /// Live::settle(until): clock advanced, correlator ticked, drained to a fixpoint.
    fn settle(&mut self, until: Timestamp) -> impl Future<Output = Result<(), BackendError>>;
    /// TransmissionStore::list(TransmissionQuery { window, states: all, channel: None }), every page.
    fn transmissions(&self, window: TimeWindow) -> impl Future<Output = Result<Vec<Transmission>, BackendError>>;
    fn spans(&self) -> &Self::Spans;
    fn accesses(&self) -> &Self::Accesses;
    fn channels(&self) -> &Self::Channels;
    /// L3: the agent and conversation of each exchange.
    fn attribution(&self, exchanges: &IdBatch<ExchangeId>)
        -> impl Future<Output = Result<BTreeMap<ExchangeId, Attribution>, BackendError>>;
    fn shutdown(self) -> impl Future<Output = ()>;
}
```

Per world, on a current-thread runtime:

1. `build(settings, first exchange's time)`: a fresh composition. One is
   never reused across worlds: resources canonicalize by URL or path, so
   two worlds would cross-link through one.
2. `ingest` each exchange in world order at its corpus time.
3. `settle(last exchange + settle_after)`, where `settle_after` is
   `CorrelationTiming::settle_after` (evidence window + suspected TTL).
   The eval's timing (`LiveSettings::short`) is a 60 s correlation
   window, a 10 s evidence window and a 60 s suspected TTL, so a world
   settles 70 s of virtual time after its last exchange and every
   transmission is final. Any still `Detected` or `AwaitingContent` is
   logged and makes no prediction.
4. `transmissions(all_time())`, sorted by id (the final set is
   deterministic; its listing order need not be).
5. `attribution` of every world exchange, in batches, into an `AgentMap`:
   each detector agent stands for the corpus agent whose exchanges it
   holds. Several detector agents for one corpus agent (a split) are fine;
   one detector agent holding two corpus agents' exchanges (a merge) fails
   the world (`AgentMapError::Merged`), since its evidence cannot be told
   apart.
6. `Resolved::gather` over `spans()`, `accesses()` and `channels()`, then
   `shutdown`.

Predictions (`predict::from_transmission`):

- confirmed, classified or aggregated: one per `ContentMatch`, of its match
  class and carrier kind, with `origin_at` from the span's `IndexedSpan`;
- suspected or discarded: one per `CoAccess`, aligned through its two
  accesses: the sender is the write access's agent, the reader the read
  access's agent at the read's exchange, `read_at` the whole tool result
  the read returned (`AccessOp::Read::result`), `origin_at` the whole
  write call; class `suspected` or `discarded`, carrier `tool_result`;
- detected or awaiting content: none.

Each prediction carries its transmission's `QualityMatch`, so the scorer's
transmission rows are the spec's `DetectionQuality` rows (tested), with
verdicts the truth implies (`score::quality`).

### The adapter (`detect::live::gateway`)

`gateway_backend()` returns `GatewayBackend`, the `LiveBackend` over the
gateway's merged `crosstalk_gateway::live::Live`; `ct-eval run --detector
live` runs it. It is wiring only:

```text
build      clock = ManualClock::at(start)
           Live::start(LiveConfig::new(LiveClock::Manual(clock), flow_config(settings.timing), settings.seed))
             memory blobs, Ticking::OnSettle, ProvenanceConfig::default() (k 32, w 16, cutoff 50)
ingest     clock.set(at) (forward only); live.pipeline().ingest(exchange, at)
settle     live.settle(until) -> Settled { at, passes }      logged at debug
read       live.stores().transmissions.list(TransmissionQuery { window, states: None, channel: None }, page)
             every page, PageSize::MAX
           live.layers().provenance        SpanIndex::spans (MemoryProvenanceStore)
           live.stores().channels          AccessStore; RegistryResources over all time for channels
           live.layers().conversations     ExchangePlacements::placement, one exchange at a time
shutdown   live.shutdown(now + 5 s) -> LiveDrained                logged at debug
```

`flow_config` turns `LiveSettings::timing` into L5's `FlowConfig`
(milliseconds), with one shard, so the correlator sees inputs in their
order, and a 1 s `tick_ms` that `Ticking::OnSettle` never uses.

How the merged API differs from what the seam was written against, and
what the eval does about it:

| Agreed | Merged | In the eval |
| --- | --- | --- |
| `Live::settle(until)` | `Live::settle(&self, until) -> Result<Settled, SettleError>` | the `Settled` is logged; a `SettleError` fails the world (`BackendError::Settle`) |
| a `FlowConfig` on `LiveConfig` | `LiveConfig::new(LiveClock, FlowConfig, seed)`, surface defaults, `Ticking::OnSettle` | `gateway::flow_config` |
| `Live::ingest(exchange, at)` | `live.pipeline().ingest(exchange, at)`; the clock is the `ManualClock` inside `LiveClock::Manual` | the adapter keeps a clone and moves it to `at` first |
| `SpanIndex` on the live stores | `live.layers().provenance` (`MemoryProvenanceStore`): only originated spans (`Originated`, `Indexed`, `Propagated`, `Expired`); relayed and common spans are absent | `LiveWorld::Spans = MemoryProvenanceStore` |
| an L3 read of an exchange's agent and conversation | `ExchangePlacements::placement(exchange) -> Option<Placement { agent, conversation }>` on `live.layers().conversations`, one exchange per call | `attribution` keeps its batch shape and loops; an exchange never threaded is absent |
| `TransmissionStore::list` | as agreed | as written |
| `BackendError::Unavailable`, the `Unavailable` backend | not needed | removed; `gateway_backend()` cannot fail |

The eval-side traits (`LiveBackend`, `LiveWorld`) did not change shape.

**Ingest does not yield.** `Pipeline::ingest` publishes to the in-process
bus without suspending, and the eval drives a current-thread runtime, so
every exchange of a world is captured before any stage runs; the stages
handle them all inside `settle`, with the clock already at `until`. The
order they see is the publish order, so runs are reproducible (two runs
give byte-identical reports and predictions,
`tests/live_gateway.rs`), but stages that read the clock (L3's and L4's id
generators, L5's `now`) read the settle time, not the exchange's.
Provenance eviction happens once, at the settle tick: a world is never
evicted mid-replay.

**The correlation window is corpus time.** `LiveSettings::short` keeps
the agreed 60 s correlation window, but the corpus clock steps one
`compose` major (1,000 s) per call on SALT, the SWE corpora and splices, so
a write and a read two calls apart never co-access under it. `ct-eval run
--correlation-window S` (and `--evidence-window`, `--suspected-ttl`)
override the windows; splices need about a day (`86400`) to be reachable.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `src/lib.rs` | crate root | modules |
| `src/keys.rs` | eval-owned keys (spec ids are used as they are) | `DatasetId`, `WorldKey`, `AgentKey` (world + name), `SourceRef` (file + record path) |
| `src/location.rs` | helpers over the spec's `SpanLocation` | `location`, `in_message`, `whole_part`, `sort_key`, `SpanLocationExt` (`overlaps`, `text`, `len`, `message`) |
| `src/ids.rs` | deterministic ids | `agent_id`, `exchange_id`, `span_id`, `transmission_id`, `channel_id`, `digest` |
| `src/corpus/mod.rs` | streaming model | `TraceSource`, `World`, `CorpusAgent`, `Driven`, `Coverage`, `SourceError`, `InMemory` |
| `src/corpus/exchange.rs` | one exchange | `CorpusExchange` (checked), `HashedMessage`, `Fidelity`, `normalized` (the checked `NormalizedExchange` builder), `CorpusError` |
| `src/corpus/builder.rs` | how converters build worlds | `WorldBuilder`, `ExchangeDraft` |
| `src/corpus/clock.rs` | the virtual clock | `compose(major, minor, sub)`, `ordinal`, `EPOCH_MICROS` |
| `src/corpus/client.rs` | per-agent client context, replayed | `synthetic_client`, `corpus_id`, `vendor_of` |
| `src/corpus/delta.rs` | new inputs of an exchange | `new_inputs` |
| `src/truth/mod.rs` | labels | `Expectation`, `ExpectedTransmission`/`TransmissionLabel`, `NegativeControl`/`NegativeLabel`, `NegativeReason`, `Exemption`/`ExemptionReason`, `AgentCluster`, `RouteExpectation`, `ExpectedContent`, `InvalidLabel` |
| `src/truth/kinds.rs` | label dimensions the spec lacks, helpers over spec ones | `Tier` (with `OutOfReach`), `CarrierKind` (the spec's, re-exported), `MatchNeed` (with spec `Codec`s; `json_string`, `yaml_string`, `through_json_string`, `two_string_levels`, `tier`; and `Undecodable` for out-of-reach labels), `json_escapes`, `TWO_STRING_LEVELS`, `route_rank`/`cmp_route` (order for spec `RouteKind`), `locator_key` (a spec `Locator` as one string) |
| `src/truth/jsonl.rs` | truth as JSONL | `write`, `read` |
| `src/predict/mod.rs` | predictions | `Prediction`, `PredictedRoute`, `EvidenceClass`, `AgentMap`, `AgentMapError`, `Directory`, `WorldDirectory`, `from_transmission`, `PredictError` |
| `src/predict/reads.rs` | the read seam: the spec's read traits, batched | `ChannelResources`, `RegistryResources`, `Reads`, `Resolved` (`gather`), `ReadError`, `ready` |
| `src/predict/memory.rs` | eval-owned stores behind the seam | `SpanTable` (`SpanIndex`), `AccessTable` (`AccessStore`), `ChannelTable` (`ChannelResources`) |
| `src/score/align.rs` | **the alignment rule** | `aligns`, `exempts`, `violates`, `specificity` |
| `src/score/judge.rs` | judging one prediction | `Judge`, `Outcome` |
| `src/score/mod.rs` | counts and breakdown | `Scorer`, `Score`, `RowKey`, `Counts`, `Selector`, `TransmissionKey` (by spec `QualityMatch`), `TransmissionRow` |
| `src/score/sources.rs` | the shared texts negative-control violations fell on | `SourceTally`, `SourceCount`, `source_key`, `TOP_SOURCES` |
| `src/score/quality.rs` | spec `DetectionQuality` from truth | `verdicts`, `detection_quality` |
| `src/reference/mod.rs` | the reference matcher (with the boilerplate cutoff) | `run`, `ReferenceConfig` (`max_postings`), `MAX_POSTINGS` (50, L4's), `ReferenceOutput` (`out_of_reach` hits), `SpanRecord` |
| `src/reference/fold.rs` | folding with offset maps | `fold`, `Folded`, `fold_plain`, `string_codec`, `unescape_once` |
| `src/reference/classify.rs` | a hit's match class, or out of reach | `classify`, `classify_forms`, `Classified` (`Match`, `TwoStringLevels`), `SpanForms`, `unescaped_plain` |
| `src/reference/opaque.rs` | opaque blobs | `opaque_ranges`, `segments` |
| `src/reference/decode.rs` | base64, hex, URL decoding | `decode_candidates` |
| `src/reference/shingle.rs` | k-gram rolling hashes | `shingles`, `covered` |
| `src/reference/route.rs` | carrier and route | `find_call`, `extract_resource`, `parse_url`, `normalize_path` |
| `src/pipeline.rs` | the run loop and the detector seam | `Detector`, `Detection`, `DetectionStatus`, `ReferenceDetector`, `run`, `predictions`, `RunSummary`, `Unscored`, `WorldError` |
| `src/gateway.rs` | the gateway pipeline as a detector | `PipelineDetector`, `ingest_world`, `subscribe`, `capture_group`, `CorpusClock`, `Captured`, `PipelineError` |
| `src/detect/live/mod.rs` | the live seam | `LiveBackend`, `LiveWorld`, `LiveDetector`, `LiveSettings` (`short`, `with_windows`), `Attribution`, `BackendError`, `LiveError`, `LiveRead`, `gateway_backend`, `all_time` |
| `src/detect/live/gateway.rs` | the `LiveBackend` over `crosstalk_gateway::live::Live` | `GatewayBackend`, `GatewayWorld`, `flow_config` |
| `src/report/mod.rs`, `table.rs` | reports | `Report` (`overall` without out-of-reach rows, `out_of_reach`, `access_only`, `background`), `Summary`, `AccessOnly`, `Background`, `ReportRow`, `table::render` |
| `src/report/gates.rs` | regression gates and where they are found | `Gates`, `Gate`, `Check`, `GateOutcome`, `GateStatus`, `GateSearch` (`new`, `from_env`, `locate`, `load`), `GatesLocation`, `GatesFrom`, `GATES_ENV`, `INSTALLED_GATES`, `GateError` (`Missing`) |
| `src/config.rs` | dataset locations | `EvalConfig`, `DatasetConfig`, `expand` |
| `src/datasets/salt/mod.rs` | SALT as a `TraceSource` | `SaltSource`, `load_world`, `convert_trace`, `SaltError`, `DATASET` |
| `src/datasets/salt/files.rs` | trace discovery | `discover`, `Selection`, `world_name` |
| `src/datasets/salt/schema.rs` | the trace JSON read | `Trace`, `Episode`, `RawMessage`, `Delivery`, `Event`, `Usage` |
| `src/datasets/salt/messages.rs` | SALT messages to canonical | `convert`, `content_text`, `arguments` |
| `src/datasets/salt/episode.rs` | exchange reconstruction and clock | `reconstruct`, `AgentEpisode`, `Turn`, `delivered_turn` |
| `src/datasets/salt/truth.rs` | SALT labels | `EpisodeLabels`, `Labelled`, `SHARED_TOOLS` |
| `src/datasets/wiki/` | collusion-wiki as a `TraceSource` (see below) | `WikiSource`, `WikiSelection` (`demo`), `tools`, `DATASET` |
| `src/datasets/swarm/` | swarm-traces as a `TraceSource` (see below) | `SwarmSource`, `SwarmSelection`, `codec::decode`, `ChainTally`, `DATASET` |
| `src/bin/ct-eval/main.rs` | CLI | `run`, `truth` |
| `datasets.toml` | dataset root and paths | |
| `gates.toml` | regression gates | |
| `tests/` | integration tests (`gates_search.rs` is the gates file lookup; `pipeline.rs` is the sim smoke test of `Pipeline::ingest`; `live.rs` drives `LiveDetector` over a scripted backend on crosstalk-memory's stores, with transmissions in every state; `live_gateway.rs` runs `--detector live` over the real `Live` on the SALT, wiki and splice fixtures and checks two runs are byte-identical; `gates_detector.rs` is gates by detector; `score_many_labels.rs` is one prediction finding several labels); `tests/fixtures/salt/` holds synthetic SALT-shaped traces | |

## Invariants and constraints

**The alignment rule** (`score::align::aligns`) is the one place a
prediction meets a label. They align when all of these hold:

1. They have the same sender and the same reader.
2. They name the same reader exchange: where the content first arrived,
   not a later exchange still carrying it.
3. Their reader locations overlap (same message hash and part, at least one
   shared byte).
4. For a channel label, the predicted channel holds the same canonical
   resource.

Match class and carrier never decide alignment; they only pick the row.

**Only content finds a label.** A suspected or discarded prediction (a
co-access with no content match) is judged by the same rule and counted in
its own row (class `suspected` or `discarded`), but a label it aligns with
that no content prediction does stays `missed` and is also counted
`suspected`. Selectors, gates and the overall summary read content rows
unless they name an access class.

**Access-only recall** (`Report::access_only`, `AccessOnly`) is those
labels over every in-reach label: `overall.suspected / overall.expected`.
It is reported on its own line ("access-only recall (suspected or
discarded only, not in overall)") and in `report.json`, never added to
`overall`, and the line is left out of the table when no label is
access-only.

- A label is found when any prediction aligns with it. Several predictions
  aligned with one label are each correct. One prediction aligned with
  several labels (a match whose read range covers two adjacent labelled
  texts of one sender, as L4 reports AgentDojo's injection slots) finds
  every one of them, and is itself counted once, as correct, in the first
  one's row (`Judge::aligned`). Until 2026-10-05 it found only the first;
  reference baselines measured before then undercount by that much
  (wiki `--demo` 0.975 → 1.000).
- A prediction that aligns with nothing is unjudged when an exemption
  covers it (`exempts`: same reader and reader exchange, overlapping read
  location; the sender is not compared).
- Otherwise it is checked against negative
  controls (`violates`, most specific first). If it violates one, it is a
  false positive charged to that control.
- Otherwise the world's coverage decides. Under `Complete { tier }` it is a
  false positive. Under `Partial` it is unjudged, not a false positive.

**`DetectionQuality` agrees.** `score::quality` builds the spec's
`DetectionQuality::tally` from the detector's transmissions with verdicts
implied by the same judgements: genuine if any prediction is correct,
false if none is and one is false, unlabeled otherwise. The scorer's
transmission rows are keyed by the spec's `QualityMatch` (confirmed by
the strongest match's class and carrier, suspected, discarded) and equal
its rows (tested, for every state). `DetectionQuality` cannot see total
misses; the scorer's `missed` can.

**Determinism.**
- Every id derives from the dataset id and a source reference
  (`canonical::ids`); exchange ids carry the virtual time in their ULID time
  bits.
- Message hashes are the spec's (`observed::message::encoding`): BLAKE3 of
  the canonical encoding.
- The virtual clock is a pure function of the dataset's ordering.
- Every map that reaches output is ordered.

A re-run produces a byte-identical `report.json` (tested, and checked on 53
real trace files).

**Spec types, not mirrors.** Labels, predictions and reports use the
spec's serde types directly (`SpanLocation`, `Locator`,
`DelegationDirection`, `RouteKind`, `MatchClass`, `CarrierKind`,
`QualityMatch`, `Codec`, `ExchangeId`, `TransmissionId`, `MessageHash`),
and read a detector's evidence through the spec's read traits
(`SpanIndex`, `AccessStore`, `ChannelReads`, `ChannelRegistry`).

**Replayed corpora.** Every corpus exchange's ingress is
`IngressMode::Replay { corpus }`, one `CorpusId` per dataset
(`eval-<dataset>`), which only `Pipeline::ingest`'s callers set
(INV-972). Each agent keeps one stable synthetic API-key credential (a
digest of its key); under `Replay` it is scoped to the corpus, since L3
attributes and merges replayed exchanges only within their corpus
(INV-973). Message hashing, canonical JSON and the
normalized-exchange invariants are the spec's (`encoding`, `json`,
`NormalizedExchange::check`). `HashedMessage` can only be built by hashing
its body through the spec's encoding.

**Corpus checks.**
- `CorpusExchange::new` runs `NormalizedExchange::check` (every message's
  hash is its body's, each body once, exactly the bodies the exchange
  names) on every exchange; `normalized()` keeps one copy of each message.
- `WorldBuilder` refuses an undeclared, foreign or scripted agent, an
  exchange not later than the agent's previous one, and a duplicate exchange
  id.
- Labels are built only through checked constructors, and deserialized
  through the same checks:
  - sender and reader differ and share a world;
  - content text fills its location;
  - a negative control is bounded by an exchange, a location or an origin.

**No dataset bytes in the repository.** Paths come from `datasets.toml`
(root `~/Data/ai/agents`) or `--root`. The fixtures under
`tests/fixtures/salt/` are synthetic: generated, never copied.

**Opaque blobs** are excluded from spans, matching and decoding
(`reference/opaque.rs`):
- tool-call ids embedding a thought signature (Gemini's
  `call_…__thought__<base64>`);
- string values of `thought_signature(s)`, `signature`,
  `encrypted_content` and `redacted_thinking` members, escaped or not.

The SALT converter maps signatures and encrypted reasoning to
`Reasoning::Opaque`, which has no part text.

**Boilerplate cutoff.** A shingle posted for more than
`ReferenceConfig::max_postings` (default `MAX_POSTINGS` = 50,
`--max-postings`) distinct originated spans is boilerplate, as L4's
frequency cutoff makes it (`interfaces::l4_provenance`). 50 is L4's own
default, `IndexSettings::default().cutoff()` in `crates/provenance`
("a fingerprint observed in more live texts than this is boilerplate"),
so the reference and L4 call the same text boilerplate; the default was
16 before. Its postings are dropped, and it is never
indexed or looked up again in that world (`reference::Postings`, a
`Spans | Boilerplate` enum). Text that many agents originate independently
is a shared source, not evidence of who a reader got it from. Examples are a
wiki's new-page template, or a URL every agent's task names. Without the
cutoff, each occurrence in a read matches every originating span, so matches
grow as reads × occurrences × originators. Before the cutoff, collusion-wiki's
largest world (2,553 agents) held 22.6M content matches and passed 8 GB. With
it, the whole export takes 2.1 GB at a cutoff of 16 and 6.2 GB at 50 (see below). The reference counts only
originated spans toward the frequency, while L4 also counts scanned inputs.
It has no retention window, because a world is one replay. How 16 and 50
compare on each corpus is under "Boilerplate cutoff: 16 and 50" below.

**Escapes.** Matching folds (`reference/fold.rs`) JSON and YAML string
escapes at any nesting depth:
- `\n`, `\t`, `\r`, `\b`, `\f` become whitespace;
- `\uXXXX` and surrogate pairs become the character they name;
- `\"`, `\\` and others drop the backslashes;
- a YAML `\`-newline continuation drops the break and the indentation.

It then folds case and collapses whitespace. Content one agent writes
inside JSON tool arguments therefore matches the same content delivered
raw. The fold finds candidates; it decides nothing. Undoing escapes is
decoding, not normalization (spec #58), and the spec undoes at most one
string level (`provenance.decode.one-string-level`), so every hit is then
classified in one place, `reference/classify.rs`:

1. `Exact` when the span holds the read bytes;
2. `Normalized` when case and whitespace folding alone make them equal
   (`fold_plain`);
3. `Decoded([JsonString])` or `Decoded([YamlString])` when they are equal
   once one string level is undone (`unescape_once`) on the read side, on
   the span side (a span written inside JSON tool arguments, delivered
   raw), or on both, then folded; the codec is the one whose escapes the
   undone text holds (`string_codec`: an escaped line break or space,
   `\x`, `\0`, `\a`, `\e`, `\v`, `\N`, `\_`, `\L`, `\P` or `\U` is
   YAML's); a YAML single-quoted `''` undone to `'` is
   `Decoded([YamlString])`;
4. `Classified::TwoStringLevels` when undoing exactly two string levels
   on one side makes them equal. No spec decoder does that, so the matcher
   reports no match for the hit and counts it in
   `ReferenceOutput::out_of_reach` (logged per world);
5. otherwise the range bridged a character neither side shares (a hit's
   range is the union of adjacent matching shingle windows, so it can
   span a one-character difference); it is classed by its escapes as one
   string level, as before.

Out of reach takes positive evidence (two levels undone actually
match), so a bridged range is never dropped as two levels.

Levels are relative: a raw log that holds a JSON-escaped send (two levels
from the raw text) is one level from the span inside the sender's tool
arguments, and matches as `Decoded([JsonString])`. Labels follow the same rule.
Text a writer put inside JSON tool arguments and a reader got raw needs
`MatchNeed::through_json_string`: `Decoded([JsonString])` exactly when it
holds a character JSON escapes, `Exact` otherwise (SALT deliveries, wiki
channel labels, swarm-truth transmissions). AgentDojo's `JsonString` and
`YamlString` arrivals need `Decoded([JsonString])` and
`Decoded([YamlString])`. A label whose text only arrives two string
levels deep needs `MatchNeed::two_string_levels()`
(`Undecodable { codec: "json_string+json_string" }`) and is
`Tier::OutOfReach` (`MatchNeed::tier`): the splice generator's JSON
string file read through a shell, and AI Village text that two string
levels undone tie to its sender.

**The virtual clock.** `compose(major, minor, sub)` gives
`EPOCH + major·1000 s + minor·1 ms + sub·1 µs`, with bounded components, so
tuple order is time order. SALT uses (episode, event id + 1, 0) for a call
whose response makes a tool call, and (latest input event + 1, 1) for any
other call. A sender's exchange therefore always precedes the reader's
exchange that first carries the delivered message (tested).

## SALT specifics

- **Exchanges.**
  - An episode's own calls are the assistant messages after the last
    `## Episode N: task phase` user turn. This works for carried-over lists,
    for memory-rewritten lists and for sliding-window truncation alike.
  - Each call's request is everything before its response.
  - The number of calls is checked against the episode's accepted
    `llm_usage` entries: equal means `Reconstructed`, otherwise `Synthetic`
    (with a warning).
  - A scripted agent (controlled-peer Bob) makes no exchanges.
- **Positives (construction).** Each `channel_transcript` delivery becomes
  a Direct/UserTurn label at the receiver's first call after the delivered
  turn, located at the content after the
  `[round=r/n][from=x][type=t]\n\n` header.
- **Negative controls.**
  - `RejectedSend`: failed `send_message` events. The origin is the failed
    call's arguments, so a violation is a prediction whose evidence is text
    that was never delivered. The converter gives the failed call's result
    `ToolOutcome::Error`, so a gateway's L5 records it as a
    `WriteOutcome::Rejected` write that never pairs; the label checks that
    no detector credits it anyway. Wherever the eval builds spec `Access`es
    (the live test backend), a rejected send is `WriteOutcome::Rejected`.
  - `NoSenderExchange`: scripted Bob's deliveries.
  - `SharedSource`: system prompts, and results of `inspect_database`,
    `query_database`, `read_code`, `read_source` and `resolve_records`.
  - `Boilerplate`: harness user turns.

## Spec #58 in the eval

Every shortcut the eval took before spec #58 (`docs/spec-eval-gaps`) is
gone:

| Spec change | What the eval does now |
| --- | --- |
| `IngressMode::Replay { corpus: CorpusId }` | corpus exchanges are replayed under one corpus per dataset (`corpus::client::corpus_id`), with one stable synthetic credential per agent, corpus-scoped (INV-973) |
| `SpanIndex::spans`, `AccessStore::accesses`, `ChannelReads` / `resource_use` | predictions read a detector's spans, accesses and channel resources through them (`predict/reads.rs`); the eval's private span directory and channel lookups are gone |
| `CarrierKind` | score rows and `DetectionQuality` use the spec's (`Carrier::kind`); `QualityMatch::Content { class, carrier }` keys the scorer's transmission rows |
| `WriteOutcome` (on `AccessOp::Write`) | SALT keeps its `RejectedSend` truth label; a failed send's result is `ToolOutcome::Error`, and the eval builds rejected sends as `WriteOutcome::Rejected` accesses |
| `Codec::JsonString`, `Codec::YamlString` | escaped text is `Decoded([JsonString])` or `Decoded([YamlString])` in labels and in the reference matcher; `Normalized` is whitespace and case only |
| one string level (`provenance.decode.one-string-level`) | the matcher classifies every hit through `reference/classify.rs` and reports no match for one only two string levels explain (`ReferenceOutput::out_of_reach`); a label whose text arrives two levels deep is `Undecodable { "json_string+json_string" }`, `Tier::OutOfReach` |

Every converter is on these types:

| Converter | Ingress | Carrier | Escaped text |
| --- | --- | --- | --- |
| SALT | `Replay { corpus: eval-salt }` | `UserTurn` | `through_json_string` |
| AgentDojo | `Replay { corpus: eval-agentdojo }` | `ToolResult` | `JsonString` / `YamlString` arrivals |
| τ²-bench | `Replay { corpus: eval-tau2 }` | `UserTurn` | none (labels are exact turns) |
| collusion-wiki | `Replay { corpus: eval-collusion-wiki }` | `ToolResult`, `ReaderOutput` (relays) | `through_json_string` (was `Normalized`) |
| swarm-traces | `Replay { corpus: eval-swarm-traces }` | `ToolResult` | base64 / hex / URL chains only |
| demo swarm (swarm-truth) | the gateway's own captures | `ToolResult` | `through_json_string` (was `Normalized`) |
| open-swe, lmcache | `Replay { corpus: eval-open_swe / eval-lmcache }` | none (negative controls only) | none |
| swe-splice | `Replay { corpus: eval-swe_splice }` | `ToolResult` | `JsonString`; two levels out of reach |
| cipher | `Replay { corpus: eval-cipher }` | `UserTurn`, `ToolResult` | base64 / hex / URL chains; ciphers the spec cannot name out of reach |
| AI Village | `Replay { corpus: eval-ai-village }` | `UserTurn`, `ToolResult` | `JsonString` one level; two levels out of reach (was `Normalized`) |

Ingress and the credential come from `WorldBuilder` (`corpus::client`) for
every converter but the swarm benchmark, whose exchanges are the
gateway's own.

## Gates file

`ct-eval run` and `ct-eval swarm` look for their gates in this order
(`report::gates::GateSearch`):

1. `--gates PATH`;
2. `CT_EVAL_GATES` (empty is unset);
3. `/usr/local/share/crosstalk-eval/gates.toml` (`INSTALLED_GATES`), where
   the bench image installs it;
4. the crate's `gates.toml` (`CARGO_MANIFEST_DIR`), which exists only in a
   source checkout (in the runtime image the compiled-in path is
   `/src/crates/eval/gates.toml`, which is absent there);
5. none: the run has no gates.

The CLI prints which one it used (`gates: PATH (--gates | CT_EVAL_GATES |
installed | crate)`) or `no gates` on stderr. A missing default (2–4) is
never an error, and falls through to the next; only an explicit
`--gates` that does not exist is (`GateError::Missing`).

Each gate checks one detector's runs: `detector = "live"` (or
`"pipeline"`), and a gate that names none is the reference matcher's
(`GateDetector`, `Gates::for_detector`). `ct-eval run` evaluates only the
gates of the detector it ran, so the reference baselines never fail a live
run and the reverse. There are no
demo-swarm gates yet: they wait for a calibrated first live run.

## How to add a converter

1. Add `src/datasets/<name>/` with a source type implementing
   `TraceSource`, yielding one `World` per independent set of agents. Read
   files lazily, one world per `next`.
2. Per world, use `WorldBuilder`:
   - `agent(name, Driven::Model | Scripted, model)` per agent;
   - `exchange(ExchangeDraft { … })` per model call, in any order, with
     `HashedMessage::new(body)` for every message;
   - `expect(…)` per label;
   - `finish(Coverage::…)`.
3. Times: if the dataset has none, compose them with `corpus::clock` from
   whatever orders its records. Each agent's exchanges must strictly
   increase, and a sender's exchange must precede the reader's.
4. Source references: give every exchange and label a `SourceRef` (file
   relative to the dataset root, JSON-pointer-like path). Ids derive from
   it.
5. Labels: pick the tier honestly, set `needs` to the weakest match class
   the dataset's construction implies, and add negative controls for the
   dataset's known traps. Declare `Coverage::Complete` only when every
   transmission in the world is labelled.
6. Register the dataset in `datasets.toml`, the CLI's `Dataset` enum and
   `gates.toml`. Add synthetic fixtures under `tests/fixtures/<name>/` and
   tests in `tests/<name>.rs`.

## Reference baseline on SALT

The CLI below runs one trace per condition (53 files, 11,796 exchanges,
3,850 labels, 12,951 negative controls). It takes about 60 s, most of it
`NormalizedExchange::check` re-hashing each exchange's full request.

```text
ct-eval run --dataset salt --limit 53
```

| route | carrier | class | tier | expected | found | recall | predicted | correct | false | precision |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| direct | user_turn | exact | construction | 2806 | 2722 | 0.970 | 7046 | 7024 | 22 | 0.997 |
| direct | user_turn | normalized | construction | 0 | 0 | - | 347 | 347 | 0 | 1.000 |
| direct | user_turn | decoded | construction | 1044 | 840 | 0.805 | 1665 | 1665 | 0 | 1.000 |
| direct | tool_result | exact | construction | 0 | 0 | - | 1678 | 0 | 1678 | 0.000 |
| direct | tool_result | normalized | construction | 0 | 0 | - | 457 | 0 | 457 | 0.000 |
| direct | tool_result | decoded | construction | 0 | 0 | - | 2322 | 0 | 2322 | 0.000 |
| direct | user_turn | exact / normalized | structural | 0 | 0 | - | 89 | 0 | 89 | 0.000 |
| direct | tool_result | decoded | structural | 0 | 0 | - | 12 | 0 | 12 | 0.000 |

- With the string codecs (spec #58), escaped deliveries need
  `Decoded([JsonString])`: their 1,044 labels moved from the `normalized`
  row to `decoded` with the same 842 found. The matcher finds exactly what
  it found before; only its classes changed: of the 2,015 user-turn
  predictions it called `normalized`, 1,668 needed a JSON string decoded
  and 347 only whitespace or case (pieces of an escaped delivery between
  its escapes). Totals, violations and gates are unchanged.
- With one string level (`provenance.decode.one-string-level`), 253
  tool-result hits and 3 user-turn hits that only two levels undone
  explain are no longer reported (2575 to 2322 and 1668 to 1665
  predictions), and two escaped deliveries are no longer found (842 to
  840). Both were content holding a literal `\n` that the any-depth fold
  paired with real whitespace in a different chunk of the same sender
  (`2/29 \n    if not …` against `3/29         if not …`): the pairing
  was an over-reach.
- Overall recall is 0.925; overall precision is 0.664 (0.988 on user
  turns).
- Violations: `rejected_send` 171, `boilerplate` 89, `shared_source` 12,
  `no_sender_exchange` 0.
- Five traces per condition (265 files, about 57 s) give recall 0.962
  (exact), 0.754 (escaped) and 0.904 (all), with user-turn precision 0.987.

Where the reference loses:

- **Misses.**
  - Messages under the 24-byte or 20-alphanumeric minimum ("See you
    there.").
  - Relayed text: raw-log chunks an agent copies from its own `get_log`
    result are not originated by it, so they are never indexed. This is
    the spec's `Relayed` classification.
- **False positives.** Almost all sit in the reader's own tool results,
  mostly its `get_log` raw log. They are coincidental shared phrasing: the
  same task prompt rotates between agents across episodes, and both agents
  write the same pytest boilerplate and SQL idioms.

## AgentDojo specifics

`src/datasets/agentdojo/` reads `runs/<pipeline>/<suite>/<task>/<attack>/<file>.json`
(36,679 runs). One run is one world.

- **Agents.**
  - **victim**: the pipeline's model, without its defense suffix
    (`-tool_filter`, `-transformers_pi_detector`, …). Exchange `k` is the
    conversation up to assistant message `k` (`Reconstructed`). Tool
    schemas are not recorded and are left out.
  - **attacker** (attacked runs only): a synthetic agent for the
    injections' author. Its one `Synthetic` exchange is at the epoch,
    before the victim's first, and writes each distinct injection as one
    text part.
- **Messages.** Content is a string or `{type, content}` blocks. A failed
  call's result is the error text with `ToolOutcome::Error`, which is what
  the pipeline shows the model. Calls without ids get
  `agentdojo-call-<message>-<n>`, and results without a `tool_call_id`
  answer the earliest matching unanswered call.
- **Positives (construction).** Each copy of an injection in a tool output
  the victim read is attacker → victim, carrier `ToolResult`, at the
  victim's next exchange, located at the copy's raw bytes. Vectors with
  identical text share copies, so each copy is labelled once.
  - **Route.** `get_webpage(url)` gives `Channel(Url)`; AgentDojo strips the
    scheme, so a scheme-less URL is read as `http://`. `read_file(file_path)`
    gives `Channel(File)`, rooted at `/` because the suite's file system is
    flat. Every other tool is opaque-keyed (email, event, channel, review)
    and gives `Direct`.
  - **Match need** (`classify.rs`). The converter tests each injection,
    trimmed, against the actual output and labels it with the weakest
    arrival that finds it:
    - `Exact`;
    - `Whitespace` (re-wrapped: a YAML plain or folded scalar);
    - `JsonString` (`\n`, `\"`, `\uXXXX`);
    - `YamlString` (`\`-newline continuations, `\ `, `\xXX`, `''`).

    Every decoding is followed by whitespace folding. `Whitespace` needs
    `Normalized`, `JsonString` `Decoded([JsonString])` and `YamlString`
    `Decoded([YamlString])` (`Arrival::need`, spec #58).
  - **Absent.** An injection in no output the victim read (the tool was
    never called, or a defense such as `transformers_pi_detector` replaced
    it with "Data omitted") gets no label.
- **Negative controls (structural).** The victim's system prompt and user
  turns are `Boilerplate` from the attacker.
- **No attacker.** `none` runs have no attacker and no labels. This
  includes `injection_task_N/none`, whose user prompt is the attacker's
  goal from no agent.
- **Second hop (heuristic, counted, not labelled).** Some successful
  attacks (`security == true`) later write an attacker indicator (a URL,
  an email or an IBAN from the injection, absent from the harness
  prompts) into a tool call. The truth model cannot label that write: the
  attacker reads nothing back, so there is no reader exchange.
  `tally::SecondHop` counts these runs instead.
- **Selection.** `--include pipeline=…`, `suite=…`, `attack=…` and `task=…`
  match a path component exactly. Repeats of one key are alternatives,
  different keys must all hold, and any other text is a path substring.
  Runs are interleaved across (pipeline, suite, attack). `ct-eval run`
  also prints the arrival tally.

| File | Role | Key exports |
| --- | --- | --- |
| `src/datasets/agentdojo/mod.rs` | AgentDojo as a `TraceSource` | `AgentDojoSource` (`tally()`), `load_world`, `convert_run`, `Loaded`, `model_of`, `AgentDojoError`, `DATASET`, `VICTIM`, `ATTACKER` |
| `src/datasets/agentdojo/files.rs` | run discovery and filters | `discover`, `Selection`, `RunFile`, `world_name` |
| `src/datasets/agentdojo/schema.rs` | the run JSON | `Run`, `RawMessage`, `Content`, `RawCall` |
| `src/datasets/agentdojo/messages.rs` | messages to canonical, call ids | `convert`, `Conversation` |
| `src/datasets/agentdojo/classify.rs` | how an injection arrived | `Arrival`, `Occurrence`, `Output`, `occurrences` |
| `src/datasets/agentdojo/route.rs` | expected route of a read | `expected_route` |
| `src/datasets/agentdojo/truth.rs` | labels | `RunLabels`, `Attacker`, `indicators` |
| `src/datasets/agentdojo/tally.rs` | arrival and second-hop counts | `Tally`, `ArrivalCounts`, `SecondHop` |
| `tests/agentdojo.rs`, `tests/fixtures/agentdojo/` | synthetic runs (generated, not copied) | |

## τ²-bench specifics

`src/datasets/tau2/` reads `tau2-bench/data/tau2/results/final/*.json`
(26 files, 10,832 simulations). One simulation is one world, and one file
is parsed at a time.

- **Agents.** `agent` (the agent model) and `user` (the user simulator).
  `user` is left out when the agent works alone (`dummy_user`).
- **Exchanges.** Every assistant or user record with `raw_data` is one
  model call, at its recorded ISO time (`time.rs`; read as UTC, and kept
  strictly increasing per agent). The agent's hard-coded greeting is not
  a call.
- **Views** (`views.rs`).
  - The agent sees its system prompt, both sides' text, its own tool
    calls, and results with requestor `assistant`.
  - The user simulator sees the conversation with roles flipped, its own
    tool calls, and results with requestor `user`.
- **System prompts** (`prompts.rs`). Rebuilt from tau2's templates, because
  the results file stores neither prompt.
  - `llm_agent`: instruction + policy.
  - `llm_agent_solo`: + ticket.
  - The user simulator: guidelines, with the persona placeholder removed,
    + `str(UserScenario)`, reproducing `textwrap.indent`.

  These exchanges are `Reconstructed`. `llm_agent_gt` exchanges are
  `Synthetic`: their resolution steps format arguments the way Python's
  `str()` does, which is only approximated.
- **Positives (structural).** Each model-written text turn is
  sender → peer, `Direct`, `UserTurn`, `Exact`, at the peer's next
  exchange, located at the whole text. A turn the peer never answers
  (`###STOP###`) has no label.
- **Known info.** The scenario's `known_info` goes harness → simulator
  system prompt → agent user turn. Only the second hop is labelled, and
  only where the simulator originated the text.
- **Negative controls (structural).**
  - `Boilerplate`:
    - the greeting;
    - any turn that only relays its sender's own system prompt (at least
      24 bytes, whitespace aside), such as a scenario line or the transfer
      message the policy dictates.
  - `SharedSource`: each side's tool results, from the peer.
- **Coverage.** `Complete { Structural }`. There are no attacks: this is a
  benign baseline for precision.
- **Selection.** `--include` matches substrings of file names.
  `--limit N` spreads N simulations over the files, evenly spaced within
  each.

| File | Role | Key exports |
| --- | --- | --- |
| `src/datasets/tau2/mod.rs` | τ²-bench as a `TraceSource` | `Tau2Source`, `load_results`, `convert_simulation`, `Tau2Error`, `DATASET`, `AGENT`, `USER` |
| `src/datasets/tau2/files.rs` | file discovery, simulation picks | `discover`, `Selection`, `quota`, `pick`, `world_name` |
| `src/datasets/tau2/schema.rs` | the results JSON | `Results`, `Info`, `Task`, `UserScenario`, `Instructions`, `Simulation`, `RawMessage` |
| `src/datasets/tau2/time.rs` | ISO times | `parse_time`, `TimeError` |
| `src/datasets/tau2/prompts.rs` | system prompt reconstruction | `agent_system_prompt`, `user_system_prompt`, `scenario_text`, `AGENT_INSTRUCTION` |
| `src/datasets/tau2/views.rs` | each side's messages | `view`, `View`, `Side`, `Entry` |
| `src/datasets/tau2/truth.rs` | labels | `SimulationLabels`, `Participant` |
| `tests/tau2.rs`, `tests/fixtures/tau2/` | synthetic results files (generated, not copied) | |

## Reference baselines on AgentDojo and τ²-bench

```text
ct-eval run --dataset agentdojo --include pipeline=gpt-4o-2024-05-13 \
  --include pipeline=claude-3-5-sonnet-20241022 --include pipeline=gemini-1.5-pro-002 \
  --include attack=important_instructions --include attack=none
```

2,259 runs (1,887 attacked), 11,235 exchanges, 2,683 labels, about 10 s.

| route | carrier | class | tier | expected | found | recall | predicted | correct | false | precision |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| channel | tool_result | normalized | construction | 359 | 87 | 0.242 | 87 | 87 | 0 | 1.000 |
| direct | tool_result | exact | construction | 0 | 0 | - | 328 | 142 | 186 | 0.433 |
| direct | tool_result | normalized | construction | 294 | 294 | 1.000 | 1736 | 1289 | 447 | 0.743 |
| direct | tool_result | decoded | construction | 2030 | 2030 | 1.000 | 1803 | 1741 | 62 | 0.966 |
| direct | user_turn | exact / normalized | structural | 0 | 0 | - | 45 | 0 | 45 | 0.000 |

Overall recall is 0.899 and precision 0.815. Before the string codecs the
two direct `normalized` and `decoded` rows were one `normalized` row
(2,324 labels, 3,549 predictions); the totals are unchanged. Classifying
by string level drops 10 false hits only two levels explain (1,813 to
1,803 predictions); recall is unchanged. A YAML single-quoted `''` is
`Decoded([YamlString])`.

Arrival classes:

| class | slots | share | labels |
| --- | ---: | ---: | ---: |
| exact | 0 | 0.0% | 0 |
| whitespace | 645 | 21.8% | 653 |
| json_string | 986 | 33.3% | 871 |
| yaml_string | 1,055 | 35.6% | 1,159 |
| absent | 275 | 9.3% | 0 |

- `important_instructions` injections are multi-line, and the
  environment's YAML load re-folds their newlines, so none arrive
  byte-exact.
- The same pipelines under `direct` + `ignore_previous` (single-line
  injections) give:
  - arrival classes: 48.9% exact, 18.2% whitespace, 26.6% yaml_string,
    6.3% absent;
  - recall 0.961 and precision 0.924.
- Second hop: 339 of 414 successful attacks wrote an attacker indicator
  into a tool call.

Where the reference loses on AgentDojo:

- **Channel misses.** These are route disagreements, not content misses.
  `route::extract_resource` refuses scheme-less URLs
  (`get_webpage("www.informations.com")`) and relative paths
  (`read_file("bill-december-2023.txt")`). It reports such reads as
  `Direct`, so each costs a miss and a misrouted false positive.
- **Victim relays.** Other false positives are text the victim copied
  from an injection into its own tool call (an IBAN, an email address, an
  injected channel name), which the tool's result echoes back. The
  content originated with the attacker, but the arrival is the victim's
  own relay (the spec's `Relayed`). There is no negative-control reason
  for it yet.

```text
ct-eval run --dataset tau2
```

10,832 simulations, 264,793 exchanges, 119,256 labels, 106,465 negative
controls, about 170 s.

| route | carrier | class | tier | expected | found | recall | predicted | correct | false | precision |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| direct | user_turn | exact | structural | 119256 | 119061 | 0.998 | 220626 | 220626 | 0 | 1.000 |
| direct | user_turn | normalized | structural | 0 | 0 | - | 20607 | 20607 | 0 | 1.000 |
| direct | tool_result | exact / normalized | structural | 0 | 0 | - | 481 | 0 | 481 | 0.000 |
| direct | system_prompt | exact / normalized | structural | 0 | 0 | - | 80 | 0 | 80 | 0.000 |

Overall precision is 0.998, and all 481 tool-result false positives fall
under `shared_source` controls.

- **Misses.** Short turns, and turns that repeat the sender's own earlier
  words, which therefore originate nothing new.
- **False positives.** Coincidental phrasing shared between one side's
  turns and the other side's tool output or policy.

## Boilerplate cutoff: 16 and 50

The default `max_postings` is L4's `IndexSettings` cutoff, 50 (it was 16).
Same machine, release build, one string level, `--max-postings 16` against
the default (2026-10-05):

| corpus | time 16 → 50 | peak RSS 16 → 50 | recall 16 → 50 | predictions 16 → 50 | FP per 1k 16 → 50 |
| --- | ---: | ---: | ---: | ---: | ---: |
| SALT (53 traces) | 43 s → 37 s | 268 MB → 269 MB | 0.925 → 0.925 | 13,616 → 13,616 | 388.3 → 388.3 |
| AgentDojo (3 pipelines) | 3.1 s → 9.2 s | 13 MB → 13 MB | 0.899 → 0.899 | 3,999 → 3,999 | 65.9 → 65.9 |
| τ²-bench | 102 s → 91 s | 171 MB → 171 MB | 0.998 → 0.998 | 241,794 → 241,794 | 2.1 → 2.1 |
| wiki `--demo` | 0.2 s → 0.6 s | 85 MB → 84 MB | 0.975 → 0.975 | 458 → 458 | - |
| wiki (whole export) | 20 s → 45 s | 2.1 GB → 6.2 GB | 0.938 → 0.941 | 1,844,139 → 6,057,952 | - |
| open-swe (`--count 16`) | 36 s → 37 s | 3.7 GB → 3.7 GB | - | 4,722 → 4,722 | 330.0 → 330.0 |
| lmcache (`--count 16`) | 7 s → 18 s | 2.7 GB → 2.7 GB | - | 4,742 → 4,742 | 1,887.0 → 1,887.0 |

Every report but the whole wiki export is identical at both cutoffs: no
shingle in them is posted for 17 to 50 distinct originated spans. Their
time differences are run-to-run noise (page cache, other load). The whole
wiki export finds 140 more labels (0.938 to 0.941) for 3.3 times the
predictions, 2.3 times the time and 3 times the memory, since text 17 to
50 identities originate is no longer boilerplate. AI Village (window and
Claude Code) keeps its recall; its window run holds 0.1% more unjudged
predictions. No gate reads a row that moved, so `gates.toml` is unchanged.

## Swarm benchmark

`src/datasets/swarm_truth/` scores the **live gateway** on traffic from the
demo swarm (`crates/demo`, `swarm --ground-truth PATH`). Unlike the dataset
converters, the exchanges are the gateway's own captures with their real
ids; the eval only labels them and scores what the gateway exported. The
dataset id is `demo-swarm`; one run is one world (`header.world`).

**Scope.** Reading truth v2, joining it to the gateway's exchange log and
blobs, scoring a saved transmissions export with its evidence (and the
evidence of suspected and discarded transmissions, as access-only
predictions), a typed join-diagnostics table, and fetching the export over
the L8 API.
**Non-scope.** Driving the swarm or the gateway, identity scoring (key
groups are kept but not labelled, see below), and Parquet exports.

### Inputs

| File | Written by | Format |
| --- | --- | --- |
| `truth.jsonl` | `swarm --ground-truth` | truth v2: one object per line tagged by `kind` (`schema.rs`, strict: only `version` 2, no unknown kinds or fields) |
| `<data dir>/exchanges/exchange-log.jsonl` | the gateway's exchange-log consumer | one spec `Envelope` per line, each `ingest`/`exchange_captured` with one `Exchange` (meta, request hashes, outcome); a torn last line is ignored |
| `<data dir>/blobs/` | the gateway's `FsBlobStore` | each message body's canonical encoding under its `MessageHash`; read through `FsBlobStore` and decoded with the spec's strict decoder |
| `export.jsonl` | `POST /exports` | spec JSONL export framing (header, `transmission` rows, trailer); read with `read_jsonl` and checked with `verify_export` and the BLAKE3 row digest (`ROW_DIGEST_CONTEXT`), so a cut-off export is refused |
| `evidence.jsonl` | `GET /transmissions/{id}/evidence?window={"context":0}` | one spec `TransmissionEvidence` per line, for the exported transmissions |

Truth v2 lines: `header` (version, world, run, seed, agent and key counts,
`claude_code_shape`, start time, gateway and wiki URLs; `run` is a ULID), then
`agent_cluster` (one per key group), then `transmission`, `self_read` and
`reread` (writer and reader with key group, session, turn and tool use id,
`route: {kind: channel, url}`, `carrier: tool_result`, the read tool,
`content: {blake3, sha256, excerpt, at: {message, block, tool_use_id}}` and
times) and `miss` (the reader side only).

- Writer == reader is always `self_read`, even on a repeat read; `reread`
  is cross-agent only: a version the reader already read earlier in the
  same session.
- A read whose write event never arrived is an `unattributed_read`: the
  reader side, the page `version` the wiki's response header reported, and
  the content, with no writer. These rows come at the run's end, after
  every other row, in read order.
- No row is written for a read whose follow-up request was never sent (no
  exchange carries it), or for a failed `PUT`.
- `at_unix_ms` is `started_at_unix_ms + at_ms`; `written_at_unix_ms` is
  when the `PUT`'s response reached the writer. Agent and page names
  (`agent-NNN`, `<topic>-<n>`) are opaque.

### Join rules (`resolve.rs`)

- **Agents.** `AgentKey { world: header.world, name }`. A session id
  belongs to the agent the truth rows name for it (a session claimed by
  two agents is reported, `session_conflict`); an exchange belongs to its
  session's agent.
- **Turns.** The log's exchanges are grouped by `meta.client.ids.session`
  (from `x-claude-code-session-id`) and ordered by (`started_at`, id). The
  position in that order is the session's generation-request ordinal, the
  truth's `turn`: only generation requests are captured, failed ones
  included.
- **Reader.** The exchange at `reader_turn`, checked by a tool result part
  for `content.at.tool_use_id` whose text BLAKE3-hashes to
  `content.blake3`. If that exchange lacks the tool result, the session's
  first exchange holding it is used and the row is reported
  (`turn_mismatch`, kept). A tool result whose bytes hash otherwise drops
  the row (`hash_mismatch`). No exchange holding it drops the row
  (`turn_out_of_range` or `tool_use_missing`), as does an unknown session.
- **Writer.** The exchange at `writer_turn`, checked by its response's
  `PUT` tool call `writer_tool_use_id` whose arguments' `body` hashes to
  `content.blake3`; the same fallback. A writer that does not join leaves
  the label with no sender exchange (`kept_without_sender`).
- **Location.** The whole text of the reader's tool result part (the
  `Tool` message's hash, the result's index, `0..len`). `content.at`'s wire
  indices are not used: the canonical request splits and reorders the wire
  array.

| Row | Becomes |
| --- | --- |
| `transmission` | `ExpectedTransmission`: Channel route with `Locator::Url` of the canonical URL (L5's `url_locator`), `ToolResult` carrier, `Construction` tier, `needs` `Decoded([JsonString])` when the page holds a character JSON escapes (the writer's `PUT` carries it escaped, the reader gets it raw) and `Exact` otherwise |
| `self_read` | `NegativeControl` `SelfRead`, writer → itself, at the read (the one control whose sender and reader are one agent: it catches a detector that splits one agent in two) |
| `reread` | `NegativeControl` `Reread`, writer → reader, at the later read |
| `miss` | `NegativeControl` `Miss` from every other agent of the world, at the read |
| `unattributed_read` | `Exemption` `UnknownSender` at the read (joined like any read, hash-checked): a prediction into that reader exchange on that content is unjudged, neither correct nor false |
| `agent_cluster` | no label: a key group is agents sharing one API key, while an `AgentCluster` is keys that are one agent. Reported as `key_group_not_a_cluster` and kept on `Resolved::key_groups` |

Coverage is `Complete { Construction }`: the swarm logs every read, the
ones it cannot attribute as exemptions, so only those places are left
unjudged and the rest of the world stays complete.

### Detections (`detected.rs`)

The export's rows say which transmissions to score; their evidence gives
each content match's reader exchange, read location, carrier and kind, and
the write and read accesses with their agents, exchanges and resources.
Gateway agent ids are tied to truth agents through the exchange log (a
match's `reader_exchange` names the reader; each access's `exchange` names
its agent, under both its stored and canonical id); a channel's resources
are its accesses' locators. Predictions are then `predict::from_transmission`
as for any detector, and the scorer, alignment rule and gates are the
eval's own. Origin span locations are unknown (`SpanIndex::span` is not on
the API), so no swarm control names an origin.

**Co-access** (`SwarmDirectory::access`, `whole_part`). Every
`AccessDetail` the evidence lists (the stored `Access`, its `Resource`
and its canonical agent) is kept by access id, and the whole text of the
part it names (a read's `AccessOp::Read::result`, a write's
`AccessOp::Write::call`) is read from the gateway's blobs. A suspected or
discarded transmission therefore predicts as for any detector: the
write's agent to the read's agent, at the read's exchange, located at the
whole tool result, with the write's whole tool call as origin, class
`suspected` or `discarded`. These are access-only predictions: their own
rows, never finding a label, out of `overall`, counted in access-only
recall. A part whose body the blobs lack leaves the co-access unlocated,
reported as `unpredictable`.

`swarm-fetch` asks for the transmissions export in `states`
`["confirmed", "classified", "aggregated", "discarded"]`
(`fetch::FETCHED_STATES`, `fetch::export_request`; INV-1070), and fetches
the evidence of every exported row, discarded ones included, so
access-only scoring gets the live gateway's discarded traffic. A settled
export never holds a suspected or awaiting-content transmission:
unconfirmed traffic is `discarded` by then. Rows outside the default
states carry their `state`, and only confirmed rows carry `strongest`.
Scoring takes every exported transmission, and besides them every
suspected or discarded one whose evidence line is in `evidence.jsonl`
(an export in the default states has no unconfirmed row); a transmission
both exported and in the evidence is predicted once.

Reported, never silent: an exported transmission with no evidence
(`missing_evidence`), a gateway agent no exchange ties to a truth agent
(`unknown_detected_agent`; that transmission yields no predictions), and
one gateway agent tied to two truth agents (`detected_agent_conflict`).

### Bench run

```text
# 1. a fresh gateway data dir, then the swarm against it
swarm --ground-truth runs/1/truth.jsonl …        # crates/demo, through the gateway
# 2. once the gateway's watermark has passed the run, save its side
ct-eval swarm-fetch --api http://crosstalk:8081 --truth runs/1/truth.jsonl --out runs/1
#    (POST /exports for confirmed and discarded transmissions,
#     then GET /transmissions/{id}/evidence per row;
#     --token-env VAR for a bearer token)
# 3. score offline
ct-eval swarm --truth runs/1/truth.jsonl \
  --exchanges <data dir>/exchanges/exchange-log.jsonl \
  --export runs/1/export.jsonl --out runs/1/report
#    --blobs defaults to <data dir>/blobs, --evidence to evidence.jsonl beside the export
```

`ct-eval swarm` prints the score table, the truth and detection counts and
the diagnostics table, writes `report.json`, `report.txt` and
`diagnostics.json` (counts, the table and every entry) to `--out`, and
exits 2 when a gate fails. A re-run over the same files is byte-identical
(tested).

| File | Role | Key exports |
| --- | --- | --- |
| `src/datasets/swarm_truth/mod.rs` | the run | `run`, `score`, `Inputs`, `Detections`, `SwarmOutcome`, `DetectedCounts`, `default_blobs`, `default_evidence`, `DATASET`, `SwarmTruthError` |
| `src/datasets/swarm_truth/schema.rs` | truth v2 serde types | `TruthLine`, `Header`, `Delivery`, `Miss`, `UnattributedRead`, `KeyGroup`, `TruthRoute`, `TruthCarrier`, `Content`, `WireAt`, `HexDigest`, `VERSION` |
| `src/datasets/swarm_truth/truth_file.rs` | reading the truth file | `read`, `TruthFile`, `Row`, `DeliveryKind`, `TruthFileError` |
| `src/datasets/swarm_truth/exchange_log.rs` | the gateway's exchange log | `read`, `parse`, `ExchangeLog`, `Sessions`, `Session` |
| `src/datasets/swarm_truth/bodies.rs` | message bodies by hash | `Bodies`, `BlobBodies`, `MemoryBodies`, `Cached`, `BodyError` |
| `src/datasets/swarm_truth/locate.rs` | tool results and `PUT` calls in exchanges | `tool_result`, `write_call`, `FoundResult`, `FoundCall` |
| `src/datasets/swarm_truth/resolve.rs` | the join | `resolve`, `Resolved`, `AgentIndex`, `ResolveCounts`, `needs` |
| `src/datasets/swarm_truth/diagnostics.rs` | join failures | `Diagnostics`, `Diagnostic`, `JoinFailure`, `Effect`, `RowKind`, `Side`, `DiagnosticCount` |
| `src/datasets/swarm_truth/detected.rs` | the export and evidence as predictions | `read_export`, `read_evidence`, `predictions`, `SwarmDirectory`, `Blake3RowHasher`, `Exported` |
| `src/datasets/swarm_truth/fetch.rs` | saving the gateway's side over HTTP | `fetch`, `FetchConfig`, `Fetched`, `FetchError` |
| `src/bin/ct-eval/swarm.rs` | `ct-eval swarm` and `swarm-fetch` | |
| `tests/swarm_truth/` | a synthetic run built with testkit (truth, exchange log and blobs, export, evidence) | |

**Invariants.**
- Every truth row becomes a label or a diagnostic; every exported
  transmission becomes predictions or a diagnostic.
- A label's reader exchange holds a tool result whose bytes hash to the
  truth's `content.blake3`, and its content text is exactly that result's
  text.
- Only a `SelfRead` control may name one agent as sender and reader.

## AI Village

The AI Village converter (`src/datasets/ai_village/`, `ct-eval run
--dataset ai-village [--mode window|claude-code] [--from DAY --to DAY]`)
has its own page: [eval_ai_village.md](eval_ai_village.md). It adds
`SourceError::AiVillage`, an `AiVillage` arm of the CLI's `AnySource`, and an
`ai-village.json` (source stats and unlabelled predictions by reader source)
next to the report.

Reference baselines: Claude Code mode recalls 15,776 of 15,798
construction-tier `get_events` deliveries (0.999); the 2026-07-13..17 window
gives 116,410 exchanges, 97,978 structural chat labels (recall 0.999) and 10
heuristic repository labels. Numbers and the false-positive picture are in
[eval_ai_village.md](eval_ai_village.md#reference-baselines).

## collusion-wiki

Real AI-agent swarms that used public UseMod/ProWiki wikis as dead drops —
the "a public wiki agents start writing to" case crosstalk was built for. The
export (`~/Data/ai/agents/collusion-wiki`, files `revisions.jsonl.gz` and
`pages.jsonl.gz`; the `.zip` duplicates them) records page edits, not model
calls, so the converter (`src/datasets/wiki/`) **synthesises** every exchange
and marks it `Fidelity::Synthetic`.

### Agents, exchanges and worlds

- **Agent identity** is the revision's `label` (chosen username) or, when the
  label is blank (899 of 14,591 revisions), its `/16` address as `ip16:<a.b>`.
  A blank username is not an identity; the `/16` is the only stable signal the
  export keeps, so blank-label saves are attributed by address and documented
  as such (`Revision::identity`).
- **A write** is one revision: an `http_request` `POST` tool call whose `body`
  argument is the lines that revision inserted (its hunks' new-line ranges), so
  that text is originated in that exchange.
- **A read** is synthesised just before an edit whose author differs from the
  page's previous author: an `http_request` `GET` of the page URL whose tool
  result is the page body as of the previous revision. This is the
  **read-before-edit assumption** — an agent that edits a page after another
  agent must have fetched it first. A consecutive same-author edit gets no read.
- **A world** is one connected component of the agent–page graph (agents linked
  by a page both edited, `plan_worlds`), so a world is a set of agents that
  could only have reached each other through shared pages. Worlds are emitted
  largest-first. `--limit` caps them, `--family` and `--wiki` filter pages, and
  `--min-agents` and `--max-agents` bound world size. The full export's giant
  component has 2,553 agents; most components are a single agent.
- **The demo subset** (`WikiSelection::demo`, `--demo`) is the five largest
  relay-coordination worlds of 2 to 12 agents. It is the M2 "undeclared public
  wiki channel" case at a size a demo can show end to end: 5 worlds, 33
  agents, 98 exchanges and 197 labels, in 0.25 s and 85 MB.

Line provenance (`attribution.rs`) replays each page's hunks to attribute every
body line to the revision that wrote it, so a reader's surviving view of each
earlier author is exact. Virtual time is a global per-world counter through
`corpus::clock::ordinal`, so reads precede edits and a sender's edit precedes
any reader's read of it.

### Labels (Heuristic tier, Channel route)

- **Channel transmission** (`ToolResult` carrier): from each earlier distinct
  author whose inserted lines are still present in the body a reader read, at
  the read exchange, located in the read's tool result, routed through the
  page's public `Locator::Url`. `Exact`, or `Decoded([JsonString])` when the
  surviving text holds a character JSON escapes (it sits escaped inside the
  writer's tool arguments; `MatchNeed::through_json_string`).
- **Relay** (`ReaderOutput` carrier): when a reader's own edit re-inserts an
  earlier author's line verbatim (and the line needs no JSON escaping, so it
  sits byte-identical in the write's canonical arguments). The reference
  matcher never predicts a `ReaderOutput`, so these are deliberate misses that
  exercise the carrier.

Coverage is `Partial`: the labels are a heuristic sample (surviving lines at or
above the reference's 24-byte / 20-alphanumeric floor), so an unlabelled
prediction is unjudged, not a false positive. Recall validates the labels. The
run also prints the channel-discovery inputs per `page_family`: pages, pages
two or more agents wrote, and revisions (`WikiSource::families`,
`FamilyTally`).

### Results on the export

The whole export (`ct-eval run --dataset wiki --examples 0`) has 14,591
revisions on 4,579 pages. It has 3,216 identities, 114 of them `ip16:`
fallbacks (899 blank-label revisions). It converts to 591 worlds and 23,691
exchanges: 14,591 writes and 9,100 synthesised reads. All 41,202 labels are
Heuristic tier: 40,394 Channel/`ToolResult` and 808 relays (`ReaderOutput`).
1,268 pages have two or more authors. The largest families are:

| page_family | multi-author / pages |
| --- | ---: |
| relay-coordination | 362 / 709 |
| source-cache-url-list | 305 / 1,231 |
| source-or-unclassified | 93 / 677 |
| off_store_unclassified | 86 / 671 |
| oecd-equity | 52 / 99 |
| datausa-clothing-workforce | 46 / 98 |
| datausa-cashiers-masters | 41 / 76 |
| probe-test | 39 / 236 |
| loop-chain-infrastructure | 7 / 339 |

Reference matcher, channel route:

| scope | cutoff | labels | recall | exact | decoded (escaped) | relay | correct / predicted | time | peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| whole export | 50 (default) | 41,202 | 0.941 | 0.922 | 0.971 | 0 / 808 | 713,792 / 6,057,952 | 45 s | 6.2 GB |
| whole export | 16 | 41,202 | 0.938 | 0.919 | 0.967 | 0 / 808 | 408,698 / 1,844,139 | 20 s | 2.1 GB |
| `--demo` | 50 or 16 | 197 | 0.975 | 1.000 | 0.970 | none | 247 / 458 | 0.2 s | 80 MB |

(`--wiki dse` and `--wiki fractal` were 0.938 and 0.944 at the old cutoff
of 16.) The escaped labels were the `normalized` column before spec #58's
string codecs reached this converter; the same labels are found. Precision
is 1.000 wherever it is judged; the rest is unjudged under `Partial`.
Relays are never found, by design. `--max-postings` trades memory for
little recall: on dse, 4 gave 0.929 at 0.9 GB and 64 gave 0.942 at 8.1 GB;
the whole export needs 6.2 GB at L4's 50, so `--max-postings 16` is the
setting for a small machine.

**The out-of-memory failure, and its fix.** Conversion was never the cost.
The largest world converts in 1.7 s at 200 MB, and its biggest revision body
is 38 KB. The reference matcher was. Text that many identities originate
independently is posted once per originating span, for example UseMod's
new-page default text (inserted by 267 identities) and common API URL
prefixes (up to 1,766 spans per shingle). Every read of a page carrying that
text matched every one of those spans. One read produced 276,889 hits, and
the giant world accumulated 22.6M content matches in 1.19M transmissions
before the 8 GB cap aborted it. The boilerplate cutoff (above) removes that
fan-out. `tests/wiki.rs::large_template_bodies_stay_linear` and
`tests/reference.rs::matches_grow_linearly_with_the_read_body` are the
regressions.

### The shortener and events

`shortener-logs.json.gz` (499 `rmn.re` links) is a second channel in
principle, but its only per-link activity is an aggregate click count with no
address, so writes and reads cannot be attributed by `ip16`; it is reported,
not converted. `events.jsonl.gz` (save/delete/probe/revert) is not needed —
saves are the revisions, reads are not logged, and the converter's
read-before-edit assumption supplies the reads.

### M2 pipeline input contract

The converter's output is a valid input to `Pipeline::ingest` (the M2 path:
converter → ingest → L3–L5 → a discovered wiki channel and confirmed
transmissions). Ingest uses eval core's synthetic `ClientContext`
(`corpus/client.rs`), as every converter does: `IngressMode::Replay
{ corpus: eval-collusion-wiki }` with a corpus-scoped credential per
identity. The agreed L5 `HttpTool` shape, emitted from one place
(`wiki::tools`, tested by `reads_and_writes_take_the_http_tool_shape` and
`channel_labels_sit_in_the_read_tool_result`):

- read: `http_request {"method":"GET","url":<canonical page url>}`; the
  tool_result is the read part and holds the page body (INV-269);
- write: `http_request {"method":"POST","url":<same url>,"body":<inserted text>}`;
  spans come from `body`.

The extractor treats a tool as `HttpTool` by name (`http_request`, `fetch`,
`web_fetch`, `curl`) with `url` + `method`; `GET`/`HEAD` are reads,
`POST`/`PUT`/`PATCH`/`DELETE` writes (spans from the first of
`body`/`content`/`text`/`data`), and the locator is the canonical URL
regardless of tool name. A write and a read of one page use the identical URL
string, so they land on one `Locator::Url` — never an MCP-shaped tool, whose
locator would carry the tool name and split the resource.

### Files

| File | Role |
| --- | --- |
| `src/datasets/wiki/mod.rs` | `WikiSource` (a `TraceSource`), reading, component planning, `WikiSelection` |
| `src/datasets/wiki/schema.rs` | the revision and page records read (`Revision`, `Page`, `Hunk`, `Revision::identity`) |
| `src/datasets/wiki/attribution.rs` | hunk replay to per-line provenance (`attribute`, `runs`, `line_byte_range`) |
| `src/datasets/wiki/resource.rs` | the page's public URL and `Locator` (`page_url`, `page_locator`) |
| `src/datasets/wiki/tools.rs` | the one place the read/write tool shape is defined (`TOOL`, `read_args`, `write_args`) |
| `src/datasets/wiki/build.rs` | the two passes: exchanges, then channel and relay labels |
| `src/datasets/wiki/tally.rs` | channel-discovery inputs per `page_family` (`FamilyTally`, `FamilyStats`) |
| `tests/wiki.rs`, `tests/fixtures/wiki/` | synthetic fixtures and tests |

## swarm-traces

A decoder test corpus. The export
(`~/Data/ai/agents/swarm-traces/redacted.jsonl.gz`, 189,579 rows of
`{id, cite, kind ∈ {payload, recovered_text, response}, parent_id, tags,
text}`) holds encoded payloads agents passed each other and, for many, a
recovered-text child. **The payloads are real attack content; the converter
treats them purely as text — it never executes any of it and never fetches any
URL in it. No dataset bytes are committed; the fixtures are synthetic strings
that mimic the encoding structure only.**

### Decoding and worlds

`codec.rs` decodes one token through a chain of codecs (base64, hex,
URL-encoding and `\x..` byte escapes, `atob(...)` unwrapped), peeling layers
until none applies and returning the chain it verified by actually decoding.
Only base64/hex/URL map to a spec `Codec`; a `\x..` layer has none and is a
reported gap.

For each payload the converter (`src/datasets/swarm/`) extracts candidate
tokens (base64/hex/URL runs, `atob` inners, `\x..` runs), and for every token
that decodes to printable text ≥24 bytes with ≥20 alphanumerics it builds a
two-agent `World`:

- the **author** originates the decoded plaintext in its output;
- the **reader** receives the encoded token in a `fetch_drop` tool result. The
  call names no resource, so the route is `Direct`.

The expected transmission is a `Decoded`-class edge author → reader
(`ToolResult` carrier), labelled `MatchNeed::Decoded { codecs }` with the
verified chain, located at the encoded token in the reader's tool result.
Tier is `Construction` when the payload has a recovered-text/response child
(the recovery is corroborated) and `Structural` otherwise (decode-only, the
property-test style). Coverage is `Complete`, so the only cross-agent content
is the token and precision is meaningful.

### What the reference shows

The reference matcher decodes one base64/hex/URL layer in a reader's input and
matches it against sender spans, so a single-layer token is found and a nested
chain is a reported miss — the point of the corpus. The run reports the
`Decoded`-row recall (the decoder's recall) and, from the converter's verified
chains, codec-chain frequencies (`SwarmSource::tally`, `ChainTally`).

Reports carry only codec chains, counts and lengths, never payload text:
- `run --dataset swarm` forces `--examples 0`, because a miss example prints
  its label's text;
- `truth --dataset swarm` refuses, because a label's text is the token;
- `SwarmError` names a token only by its length.

On the export, 91,037 payloads yield 659,746 distinct candidate tokens. Of
these, 629 decode through a verified chain to printable text of at least 24
bytes: `url` 615, `url.url` 13 and `base64` 1. 170 are Construction tier
and 459 Structural. No `\x..` byte-escape chain reaches the floor, so no gap
chain is reported on this export. The reference finds 627 of the 629
`Decoded` labels (recall 0.997, precision 1.000, 2 s, 195 MB). It finds the
`url.url` chains too, because one URL decode of a double-encoded token
already exposes most of the plaintext.

### Files

| File | Role |
| --- | --- |
| `src/datasets/swarm/mod.rs` | `SwarmSource` (a `TraceSource`), token extraction, the two-agent decoded worlds, `SwarmSelection` |
| `src/datasets/swarm/codec.rs` | the nested codec-chain decoder (`decode`, `Layer`, `Decoded`) |
| `src/datasets/swarm/tally.rs` | decode-chain counts and lengths for the run report (`ChainTally`, `ChainStats`) |
| `tests/swarm.rs`, `tests/fixtures/swarm/` | synthetic benign fixtures and tests |

## SWE background, splices and cipher pairs

`ct-eval run --dataset open-swe | lmcache | swe-splice | cipher` has its
own page: [eval_swe_synthetic.md](eval_swe_synthetic.md). Open-SWE-Traces
and LMCache trajectories are mixed into background worlds that hold only
negative controls. The splice generator plants an exact Channel
transmission through `Locator::File` between two real trajectories, in four
variants. Cipher pairs deliver payloads encoded, and those whose encoding the
spec's `Codec` cannot name are labelled `Tier::OutOfReach`. Every run's
report now has an `out_of_reach` summary, kept out of `overall`, and a
`background` summary for any run with negative controls: false positives
per 1k exchanges and the top sources they fell on.

Reference baselines: open-swe gives 330.7 false positives per 1k exchanges
and lmcache 1,887.0. Splices are found 59/59 through an editor view and 0/15
through a shell read, which the reference routes `Direct`. Cipher recall
runs from 0.36 to 0.52 for the in-reach codecs, with 0/200 out of reach.
