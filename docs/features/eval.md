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
pipeline and transport's bus and blob store, with sim and testkit as
dev-dependencies.

## Scope

- **Corpus model.** `TraceSource` is a stream of `World`s: sets of agents
  that only talk to each other. Each world holds its exchanges in
  virtual-time order and its truth.
- **Labels.** Expected transmissions, negative controls and agent clusters,
  with tiers, as JSONL.
- **Predictions.** The eval-side view of a detector's output, converted
  from spec `Transmission`s and their `ContentMatch`es.
- **Scoring.** One alignment rule, TP/FP/FN broken down by dataset × route
  kind × carrier × match class × tier, negative-control violations, and a
  bridge to the spec's `DetectionQuality`.
- **Detectors.** A naive reference matcher (span/shingle matching with
  escape-aware normalization and decoding), and the gateway pipeline
  itself (`Pipeline::ingest`), unscored until its detection layers exist.
- **Reports and gates.** A table, a JSON report, and regression gates in
  `gates.toml`.
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
                    │  WorldBuilder: agents (synthetic ClientContext), exchanges (ExchangeDraft → spec Exchange
                    │  → checked NormalizedExchange → CorpusExchange), labels, coverage
                    ▼
                  World { agents, exchanges (time order), truth, coverage }
                    │
                    ├──▶ Detector::detect(&World) ─▶ Detection { transmissions, channels, spans }
                    │        ReferenceDetector, or PipelineDetector (below)
                    │                 │
                    │                 ▼
                    │        predict::from_transmission (via WorldDirectory) ─▶ Vec<Prediction>
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
- L3–L5 have no bus consumers yet, so no transmissions come back. The
  detection says `DetectionStatus::NoConsumers { ingested }`; the run
  counts the world as unscored (`RunSummary::unscored`), and the report
  says "no detector consumers yet" instead of scoring zero. When the
  consumers land, `PipelineDetector` reads their transmissions, channels
  and spans back into a `Detection` and the rest of the run is unchanged.
- `gateway::ingest_world` is the same loop over any spec `BlobStore` and
  `EventBus`. The smoke test (`tests/pipeline.rs`) runs it under
  crosstalk-sim's clock (its default epoch is the corpus clock's,
  2026-01-01), advancing paused time to each exchange's corpus time.

`ct-eval run --detector pipeline` runs the pipeline path.

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
| `src/corpus/client.rs` | per-agent client context | `synthetic_client`, `vendor_of` |
| `src/corpus/delta.rs` | new inputs of an exchange | `new_inputs` |
| `src/truth/mod.rs` | labels | `Expectation`, `ExpectedTransmission`/`TransmissionLabel`, `NegativeControl`/`NegativeLabel`, `NegativeReason`, `AgentCluster`, `RouteExpectation`, `ExpectedContent`, `InvalidLabel` |
| `src/truth/kinds.rs` | label dimensions the spec lacks, helpers over spec ones | `Tier` (with `OutOfReach`), `CarrierKind` (the spec's, re-exported), `MatchNeed` (with spec `Codec`s, and `Undecodable` for out-of-reach labels), `route_rank`/`cmp_route` (order for spec `RouteKind`), `locator_key` (a spec `Locator` as one string) |
| `src/truth/jsonl.rs` | truth as JSONL | `write`, `read` |
| `src/predict/mod.rs` | predictions | `Prediction`, `PredictedRoute`, `Directory`, `WorldDirectory`, `from_transmission` |
| `src/score/align.rs` | **the alignment rule** | `aligns`, `violates`, `specificity` |
| `src/score/judge.rs` | judging one prediction | `Judge`, `Outcome` |
| `src/score/mod.rs` | counts and breakdown | `Scorer`, `Score`, `RowKey`, `Counts`, `Selector`, `TransmissionRow` |
| `src/score/sources.rs` | the shared texts negative-control violations fell on | `SourceTally`, `SourceCount`, `source_key`, `TOP_SOURCES` |
| `src/score/quality.rs` | spec `DetectionQuality` from truth | `verdicts`, `detection_quality` |
| `src/reference/mod.rs` | the reference matcher | `run`, `ReferenceConfig`, `ReferenceOutput`, `SpanRecord` |
| `src/reference/fold.rs` | normalization with offset maps | `fold`, `Folded` |
| `src/reference/opaque.rs` | opaque blobs | `opaque_ranges`, `segments` |
| `src/reference/decode.rs` | base64, hex, URL decoding | `decode_candidates` |
| `src/reference/shingle.rs` | k-gram rolling hashes | `shingles`, `covered` |
| `src/reference/route.rs` | carrier and route | `find_call`, `extract_resource`, `parse_url`, `normalize_path` |
| `src/pipeline.rs` | the run loop and the detector seam | `Detector`, `Detection`, `DetectionStatus`, `ReferenceDetector`, `run`, `predictions`, `RunSummary`, `Unscored`, `WorldError` |
| `src/gateway.rs` | the gateway pipeline as a detector | `PipelineDetector`, `ingest_world`, `subscribe`, `capture_group`, `CorpusClock`, `Captured`, `PipelineError` |
| `src/report/mod.rs`, `table.rs` | reports | `Report` (`overall` without out-of-reach rows, `out_of_reach`, `background`), `Summary`, `Background`, `ReportRow`, `table::render` |
| `src/report/gates.rs` | regression gates | `Gates`, `Gate`, `Check`, `GateOutcome`, `GateStatus` |
| `src/config.rs` | dataset locations | `EvalConfig`, `DatasetConfig`, `expand` |
| `src/datasets/salt/mod.rs` | SALT as a `TraceSource` | `SaltSource`, `load_world`, `convert_trace`, `SaltError`, `DATASET` |
| `src/datasets/salt/files.rs` | trace discovery | `discover`, `Selection`, `world_name` |
| `src/datasets/salt/schema.rs` | the trace JSON read | `Trace`, `Episode`, `RawMessage`, `Delivery`, `Event`, `Usage` |
| `src/datasets/salt/messages.rs` | SALT messages to canonical | `convert`, `content_text`, `arguments` |
| `src/datasets/salt/episode.rs` | exchange reconstruction and clock | `reconstruct`, `AgentEpisode`, `Turn`, `delivered_turn` |
| `src/datasets/salt/truth.rs` | SALT labels | `EpisodeLabels`, `Labelled`, `SHARED_TOOLS` |
| `src/bin/ct-eval/main.rs` | CLI | `run`, `truth` |
| `datasets.toml` | dataset root and paths | |
| `gates.toml` | regression gates | |
| `tests/` | integration tests (`pipeline.rs` is the sim smoke test of `Pipeline::ingest`); `tests/fixtures/salt/` holds synthetic SALT-shaped traces | |

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

- A label is found when any prediction aligns with it. Several predictions
  aligned with one label are each correct.
- A prediction that aligns with nothing is checked against negative
  controls (`violates`, most specific first). If it violates one, it is a
  false positive charged to that control.
- Otherwise the world's coverage decides. Under `Complete { tier }` it is a
  false positive. Under `Partial` it is unjudged, not a false positive.

**`DetectionQuality` agrees.** `score::quality` builds the spec's
`DetectionQuality::tally` from the detector's transmissions with verdicts
implied by the same judgements: genuine if any match is correct, false if
none is and one is false, unlabeled otherwise. The scorer's transmission
rows equal its rows (tested). `DetectionQuality` cannot see total misses;
the scorer's `missed` can.

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
`DelegationDirection`, `RouteKind`, `MatchClass`, `Codec`, `ExchangeId`,
`TransmissionId`, `MessageHash`). Message hashing, canonical JSON and the
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

**Escapes.** Folding (`reference/fold.rs`) unfolds JSON and YAML string
escapes at any nesting depth:
- `\n`, `\t`, `\r`, `\b`, `\f` become whitespace;
- `\uXXXX` and surrogate pairs become the character they name;
- `\"`, `\\` and others drop the backslashes;
- a YAML `\`-newline continuation drops the break and the indentation.

It then folds case and collapses whitespace. Content one agent writes
inside JSON tool arguments therefore matches the same content delivered
raw. A SALT label needs `Normalized` exactly when its content holds a
character JSON escapes.

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
    that was never delivered.
  - `NoSenderExchange`: scripted Bob's deliveries.
  - `SharedSource`: system prompts, and results of `inspect_database`,
    `query_database`, `read_code`, `read_source` and `resolve_records`.
  - `Boilerplate`: harness user turns.

## Shortcuts that in-flight spec changes replace

`docs/spec-eval-gaps` adds spec types the eval works around today. The eval
does not depend on them yet; when they land:

| Spec change | Replaces |
| --- | --- |
| `IngressMode::Replay { corpus: CorpusId }` | the fabricated `ClientContext` (`corpus/client.rs`: a reverse-proxy route named after the dataset); corpus-ingested exchanges must use `Replay` |
| `SpanIndex::span` | the span directory (`Detection::spans`, `Directory::span`, `ReferenceOutput::spans`) the eval keeps to locate a match's origin |
| `WriteOutcome` (on `AccessOp::Write`), `ToolOutcome::Unknown` | the eval-only `NegativeReason::RejectedSend` label for failed `send_message` calls, which becomes a rejected write the detector itself sees |
| `Codec::JsonString`, `Codec::YamlString` | the JSON/YAML escape class folded into `Normalized` in the SALT and swarm labels and the reference matcher's escape unfolding (the splice and cipher corpora already label escapes `Decoded([JsonString])`) |
| an access-by-id read | channel-route alignment's dependence on the detector reporting each channel's resources (`Directory::channel`) |

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
| direct | user_turn | normalized | construction | 1044 | 842 | 0.807 | 2015 | 2015 | 0 | 1.000 |
| direct | tool_result | exact | construction | 0 | 0 | - | 1678 | 0 | 1678 | 0.000 |
| direct | tool_result | normalized | construction | 0 | 0 | - | 3032 | 0 | 3032 | 0.000 |
| direct | user_turn | exact / normalized | structural | 0 | 0 | - | 89 | 0 | 89 | 0.000 |
| direct | tool_result | normalized | structural | 0 | 0 | - | 12 | 0 | 12 | 0.000 |

- Overall recall is 0.926; overall precision is 0.652 (0.988 on user
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

    Every decoding is followed by whitespace folding. Until spec PR #58's
    `Codec::JsonString`/`YamlString` land, all but `Exact` map to
    `Normalized` (`Arrival::need`, TODO(#58)).
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
| direct | tool_result | normalized | construction | 2324 | 2324 | 1.000 | 3549 | 3030 | 519 | 0.854 |
| direct | user_turn | exact / normalized | structural | 0 | 0 | - | 45 | 0 | 45 | 0.000 |

Overall recall is 0.899 and precision 0.813.

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

## Swarm benchmark

`src/datasets/swarm_truth/` scores the **live gateway** on traffic from the
demo swarm (`crates/demo`, `swarm --ground-truth PATH`). Unlike the dataset
converters, the exchanges are the gateway's own captures with their real
ids; the eval only labels them and scores what the gateway exported. The
dataset id is `demo-swarm`; one run is one world (`header.world`).

**Scope.** Reading truth v2, joining it to the gateway's exchange log and
blobs, scoring a saved transmissions export with its evidence, a typed
join-diagnostics table, and fetching the export over the L8 API.
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
`claude_code_shape`, start time, gateway and wiki URLs), then
`agent_cluster` (one per key group), then `transmission`, `self_read` and
`reread` (writer and reader with key group, session, turn and tool use id,
`route: {kind: channel, url}`, `carrier: tool_result`, the read tool,
`content: {blake3, sha256, excerpt, at: {message, block, tool_use_id}}` and
times) and `miss` (the reader side only).

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
| `transmission` | `ExpectedTransmission`: Channel route with `Locator::Url` of the canonical URL (L5's `url_locator`), `ToolResult` carrier, `Construction` tier, `needs` `Normalized` when the page holds a character JSON escapes (the writer's `PUT` carries it escaped) and `Exact` otherwise |
| `self_read` | `NegativeControl` `SelfRead`, writer → itself, at the read (the one control whose sender and reader are one agent: it catches a detector that splits one agent in two) |
| `reread` | `NegativeControl` `Reread`, writer → reader, at the later read |
| `miss` | `NegativeControl` `Miss` from every other agent of the world, at the read |
| `agent_cluster` | no label: a key group is agents sharing one API key, while an `AgentCluster` is keys that are one agent. Reported as `key_group_not_a_cluster` and kept on `Resolved::key_groups` |

Coverage is `Complete { Construction }`: the swarm logs every read.

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
#    (POST /exports, then GET /transmissions/{id}/evidence per row;
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
| `src/datasets/swarm_truth/schema.rs` | truth v2 serde types | `TruthLine`, `Header`, `Delivery`, `Miss`, `KeyGroup`, `TruthRoute`, `TruthCarrier`, `Content`, `WireAt`, `HexDigest`, `VERSION` |
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
