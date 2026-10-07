# Bench adapter (`crosstalk-bench-adapter`, `ct-bench-detect`)

`crates/bench-adapter` is crosstalk's side of the
[a2a-transmission-bench](https://github.com/RhizoNymph/a2a-transmission-bench)
(separation design §4, §4.1). The bench owns the datasets, their
converters and labels, the scorer, the reports and the regression gates,
all over its detector-neutral on-disk format `a2a-bench/1` (crate
`a2a-bench-format`, pinned here to the tag `a2a-bench-format-v1.0.0`,
commit `c289eda`). This crate only detects: its one binary,
`ct-bench-detect`, turns a bench input directory into a predictions file
from crosstalk's real detector, and turns a saved node0 bench run into a
bench input directory plus the gateway's own predictions.

## Scope

- The bench's detector contract (design §4): `ct-bench-detect --input DIR
  --output FILE`, its exit and failure rules, and the predictions header
  (detector name, variant, config digest, build commit).
- Bench rows as spec values (`convert`): bench messages to spec messages
  with every part's text checked equal (parity stage P1, run live on every
  world), bench exchanges to checked `NormalizedExchange`s.
- The detectors it drives: the gateway's live composition through the
  `LiveBackend` seam (`detect::live`, `GatewayBackend` over
  `crosstalk_gateway::live::Live`) and the bare gateway pipeline
  (`gateway::PipelineDetector`, which detects nothing and writes
  `no_consumers` worlds).
- Spec detections as bench rows (`to_bench`): attribution, unattributed
  and transmission rows, with spec locations translated to bench
  locations (`MessageHash` → `MessageId` through the world's index) and
  spec locators to bench resources.
- A saved node0 bench run (`run.sh bench`'s run directory: truth header,
  exchange log, blobs, export, evidence, conversation reads) as a bench
  input directory and the gateway's predictions (`from-export`); the same
  run replayed through `Live` in memory (`replay`); and fetching the
  gateway's side of a run over L8 (`swarm-fetch`: export and evidence;
  `fetch`: the conversation reads).

## Non-scope

- Datasets, converters and labels, including the demo swarm's truth →
  labels converter: the bench (`a2a-bench export`, its `datasets` crates).
- Scoring, alignment, reports and gates: the bench (`a2a-bench score`,
  `a2a-bench run`; gates in the bench's `gates/` directory, one file per
  detector: `crosstalk-live.toml`, `crosstalk-gateway-export.toml`, …).
  A gate change is a bench PR.
- The format itself (`a2a-bench-format`, owned by the bench).
- The reference matcher: the bench's `a2a-reference`.
- `run.sh bench` on the compose deployment (`deploy/`, [bench.md](bench.md)),
  which drives the node0 run this crate reads.

## Running against the bench

```text
# a bench export (made by the bench: a2a-bench export …), scored with crosstalk's live detector
a2a-bench run --export <export dir> --out <run dir> \
  --detector-cmd "ct-bench-detect [--mode live|pipeline] [--forwarding off|on] [--extract-config FILE]" \
  [--gates <bench>/gates] [--twice]
#   a2a-bench builds <run dir>/input (the input view, never labels.jsonl), runs
#   ct-bench-detect --input <run dir>/input --output <run dir>/predictions.jsonl [args…],
#   and scores the predictions; --twice runs it again and fails on any byte difference.

# a node0 bench run (run.sh bench's run directory)
ct-bench-detect swarm-fetch --api URL --token-env VAR --truth <run>/truth.jsonl --out <run>   # export, evidence
ct-bench-detect fetch       --api URL --token-env VAR --truth <run>/truth.jsonl --out <run>   # conversation reads (optional)
ct-bench-detect from-export --run <run> --out <out>      # input view + crosstalk-gateway-export predictions
ct-bench-detect replay      --run <run> --out <out>      # the same through Live in memory: crosstalk-live
a2a-bench score …  over <out> with the bench's demo-swarm labels and gates/crosstalk-gateway-export.toml
```

## Commands

```text
ct-bench-detect --input DIR --output FILE [--mode live|pipeline] [--forwarding off|on]
                [--extract-config FILE] [--correlation-window S] [--evidence-window S]
                [--suspected-ttl S] [--seed N]
ct-bench-detect from-export --run RUNDIR --out DIR [--detector-version TEXT]
ct-bench-detect replay      --run RUNDIR --out DIR [--evidence-window-ms MS]
                            [--suspected-ttl-ms MS] [--since-unix-ms MS] [--seed N]
ct-bench-detect fetch       --api URL [--token-env VAR] --truth FILE --out RUNDIR
ct-bench-detect swarm-fetch --api URL [--token-env VAR] [--truth FILE | --since-unix-ms MS] --out RUNDIR
```

- **Input.** `DIR` holds `manifest.json` (the input view),
  `messages.jsonl` and `exchanges.jsonl`; `labels.jsonl` is never read.
  Both files are read with the format's `FileReader` in lockstep
  (`input::InputDir`): their headers must name the manifest's dataset,
  the n-th world of each must be the manifest's n-th world with its
  exchange count, and both trailers' digests must be the manifest's.
  Anything else is a run failure (non-zero exit, no predictions file).
- **Exit.** 0 means a complete predictions file with its trailer, read
  back through its framing (`to_bench::verify::predictions_file`).
- **Failed worlds.** A world that cannot be processed is a world row
  `failed { reason }` with no rows, and the run goes on. The reason starts
  with a stable code and a colon (`FailureCode`): `part_text_mismatch`,
  `conversion` (a bench row with no spec form: a media kind `other`, a
  credential that is not `k:<hex>`, two response messages, an unreadable
  `response.error`; or rows `check_predictions` refuses), `ingest`
  (`BackendError::Build`/`Ingest`), `settle`, `read` (a world whose rows do
  not pass `WorldInputs::new`, a store read), `unlocated_access` (a
  transmission naming an access or part the world does not hold). Reasons
  name ids and byte counts, never text.
- **Agent merges** are not failures: the attribution is written as L3
  made it (`RawDetection::attribution`), and the bench's scorer fails the
  world.
- **`--extract-config`** is L5's `ExtractConfig` JSON (`mcp_servers`,
  `http_tools`, `fetch_tools`, `sites`, `persistent_shells`).
  `crates/bench-adapter/extract/ai-village.json` (`{"persistent_shells":
  ["bash"]}`) is the one AI Village runs use. AgentDojo's
  (`{"fetch_tools": ["get_webpage"]}`) lives with the bench now.
- **`swarm-fetch`** saves `export.jsonl` (`POST /exports` for the
  transmissions in `fetch::FETCHED_STATES`: confirmed, classified,
  aggregated and discarded) and `evidence.jsonl` (`GET
  /transmissions/{id}/evidence` per exported row), from the truth header's
  `started_at_unix_ms` (or `--since-unix-ms`, default 0) to an hour past
  now. It prints one line naming both files and their counts (what
  `run.sh bench`'s holdout mode keeps out of its log). It is what `ct-eval
  swarm-fetch` was, unchanged.

## Header

| mode | `detector.name` | `variant` | `config_digest` |
| --- | --- | --- | --- |
| live | `crosstalk-live` | `forwarding-off` / `forwarding-on` | BLAKE3 of the canonical JSON of `{correlation_window_ms, evidence_window_ms, suspected_ttl_ms, seed, forwarding, extract}` |
| pipeline | `crosstalk-pipeline` | `default` | BLAKE3 of `{seed}` |
| from-export | `crosstalk-gateway-export` | `default` | none; `version` is `bench.env`'s `crosstalk_image` (or `--detector-version`, else `unrecorded`) |
| replay | `crosstalk-live` | `forwarding-off` | BLAKE3 of the flow config, seed, `since_us`, `until_us` |

`version` is `to_bench::manifest::CROSSTALK_COMMIT` (the crosstalk commit
`build.rs` records as `CROSSTALK_ADAPTER_GIT`, `-dirty` when tracked
files differ, `unknown` without git) for every mode but from-export;
`manifest_digest` is `Manifest::digest()` of the manifest read (or
written), which is the same for a full manifest and its input view. The
bench's gates select on `detector.name` and `variant`.

## Data and control flow

### `--input`

```text
InputDir::next_world ──▶ WorldInputs (checked)       | unreadable ─▶ failed { read: … }
convert::world
  convert::message per bench message: convert::body (part for part) ─▶ spec Message::new
      check_part_text: bench part_text(i) == spec part_text(i) for every i, same count  (P1, live)
  convert::exchange per bench exchange, in file order:
      id = ExchangeId::from_ulid(bench raw), started_at = at_us, request/response by hash,
      Completed { stop } or Failed { failure } from response.error, NormalizedExchange::check
      client: Replay { eval-<dataset> }, upstream eval-<dataset> + vendor, ApiKey
              CredentialHash::from_keyed_digest(SecretVersion(0), <hex of k:>), session; turn dropped
  MessageIndex::insert(exchange, spec hash, bench id) for each carried message
live:     LiveDetector::detect_exchanges(&[Timed]) ─▶ RawDetection { transmissions, attribution, resolved }
          to_bench::predictions::rows(transmissions, BenchDirectory, held(attribution), Unlocated::Fail,
                                      index) ─▶ attribution, unattributed, transmission rows
pipeline: PipelineDetector::ingest(&[Timed]) ─▶ no_consumers { ingested }
PredictionsWriter::world (check_predictions, spilled) ─▶ finish(header) ─▶ predictions_file read-back
```

The reverse mapping keeps what the bench dropped absent: a reasoning or
tool-call signature is `None`, `reasoning_opaque` is `Reasoning::Opaque`
with an empty signature, an `unknown` block is kind `unknown` with raw
`{}`, a media part names the empty media blob (held once in the
exchange's `media`). The protocol, which the bench does not carry and no
detection layer reads, follows the vendor. A spec `MessageHash` therefore
differs from the original's when anything was dropped; parity is on part
order and text, and the detector's ids (transmissions from exchange,
sender and route; agents minted in ingest order) do not depend on it. The
replay corpus is still named `eval-<dataset>`, as ct-eval's converters
named it, so the composition sees what it saw at parity.

### The live seam (`detect::live`)

`LiveDetector<B: LiveBackend>` drives a composition one world at a time,
on a current-thread runtime:

1. `LiveBackend::build(settings, first exchange's time)`: a fresh
   composition. One is never reused across worlds: resources canonicalize
   by URL or path, so two worlds would cross-link through one.
2. `LiveWorld::ingest` each exchange in world order at its `at_us`.
3. `settle(last exchange + settle_after)`, `settle_after` being
   `CorrelationTiming::settle_after` (evidence window + suspected TTL).
   `LiveSettings::short` is a 60 s correlation window, a 10 s evidence
   window and a 60 s suspected TTL (70 s of virtual time after the last
   exchange); the three flags override them. A transmission still
   `Detected` or `AwaitingContent` is logged and written without
   evidence.
4. `transmissions(all_time())`, sorted by id.
5. `attribution` of every exchange (L3's `ExchangePlacements::placement`),
   in `IdBatch` chunks.
6. `Resolved::gather` (`reads`) over `spans()`, `accesses()` and
   `channels()`: every span, access and channel the transmissions name,
   through the spec's read traits; then `shutdown`.

`GatewayBackend` (`detect::live::gateway`) is wiring only:

```text
build      clock = ManualClock::at(start)
           Live::start(LiveConfig::new(LiveClock::Manual(clock), flow_config(settings.timing), settings.seed)
                       with extract = the backend's ExtractConfig, provenance forwarding = settings.forwarding)
             memory blobs, Ticking::OnSettle
ingest     clock.set(at) (forward only); live.pipeline().ingest(exchange, at)
settle     live.settle(until)
read       live.stores().transmissions.list(all states, every page, PageSize::MAX)
           live.layers().provenance        SpanIndex::spans
           live.stores().channels          AccessStore; RegistryResources over all time for channels
           live.layers().conversations     ExchangePlacements::placement, one exchange at a time
shutdown   live.shutdown(now + 5 s)
```

`Pipeline::ingest` publishes without suspending and the runtime is
current-thread, so every exchange of a world is captured before any stage
runs; the stages handle them inside `settle`, in publish order, which is
what makes two runs byte-identical.

### Rows (`to_bench::predictions::rows`)

1. `attribution`: each detector agent (its `AgentId`'s ULID text) and the
   world exchanges L3 placed under it, by agent id;
2. `unattributed`: every agent the evidence names that holds no exchange;
3. `transmission`: every transmission, by id, with its state, quality
   (`QualityMatch`) and evidence: one `matches` entry per `ContentMatch`
   of a confirmed, classified or aggregated one (`origin_at` from the
   span's `IndexedSpan`, the route's channel resources), one `co_access`
   entry per `CoAccess` record of a suspected or discarded one (the
   write's agent to the read's, the read's whole tool result, the write's
   whole call, the channel's resource when it holds one, else the read's),
   none for a detected or awaiting-content one.

**Location sort.** A transmission's matches are sorted by `read_at` and
its co-access records by `(read_at, write_at)`, by the format's
`Location` order; ties by origin, then the row's text, never the
detector's order (which follows spec message hashes, re-derived
differently on the bench side), so one detection's bytes are the same
from either side.

**Resources** (`to_bench::resource`): `Repository` → `repository`, a
`File` whose host is `<forge>/<owner…>/<name>` → `repo_file`, any other
`File` → `file`, `Url` → `url` (`<scheme>://<host><path>[?<query>]`, which
the bench canonicalises again), `Mcp` → `mcp`, `Opaque` → `opaque`.

### from-export, replay and fetch

```text
<run>/truth.jsonl         header only: dataset demo-swarm/<scenario>, world, run ULID, the run window,
                          every agent name (public); session owners for the same-µs report only
<run>/exchange-log.jsonl  capture::build: the exchanges that started in the run window
<run>/blobs/                (window::RunWindow: [start - 5 s lead, latest row time + 60 s slack]),
                            every session's and the session-less ones, (started_at, id) order,
                            the gateway's ids, client.session, client.turn = ordinal in the session
<run>/export.jsonl        detected::choose: exported transmissions with evidence, plus every
<run>/evidence.jsonl        suspected/discarded one, less those read only outside the window
<run>/exchange-turns.json attribution: each world exchange under its canonical agent
<run>/span-points.json    origin_at of content matches whose span's exchange is in the world
──▶ <out>/manifest.json, messages.jsonl, exchanges.jsonl, predictions.jsonl, from-export.json
```

- **Run window.** The exchange log accumulates across runs and a reused
  seed reuses session ids, so only exchanges that started inside the
  window are captured (`window::split`), and the session ordinals count
  only those. A transmission whose every reader exchange started outside
  is another run's and is not written (logged).
- **Attribution.** With the two saved query answers (`Queried`), every
  exchange L3 placed is attributed (`attribution: query`). Without them
  (`attribution: evidence`), the evidence's ties
  (`from_export::predict::ties`): a confirmed transmission's reader
  exchanges are its reader's, an access's exchange its canonical agent's;
  a sender tied to nothing is `unattributed`; no `origin_at`. An access's
  own agent id is always an alias of its canonical one.
- **Manifest** (input view): `demo-swarm/<scenario>`, version 1, split
  `dev`, one world keyed by the header's `world`; `source.path` = the run
  directory's name, `source.revision` = the header's `run`,
  `source.digest` over the files read (truth, exchange log, export,
  evidence, and the query answers when used; blobs are content-addressed
  by the log's hashes); converter `ct-bench-detect <crate version>` at
  `CROSSTALK_COMMIT`; selection `{run_lead_ms, run_slack_ms}`; pace `{}`;
  no labels, no notes.
- **Same-microsecond exchanges** of one agent (by the truth's session
  owner) are reported in `from-export.json` (`same_micros`) and logged,
  never nudged.
- **replay** runs `swarm::replay` (the log through `Live` in memory,
  windows from `bench.env` or the demo config's) and reads the export,
  evidence and conversation reads back from the composition's surface
  (`Replayed`), so its predictions (`crosstalk-live`) are attributed by
  query and carry origins.
- **fetch** asks a running gateway's `POST /query/exchange-turns` (every
  in-window exchange) and `POST /query/span-points` (every origin span the
  saved evidence names), in batches of `IdBatch::MAX`, and saves the
  merged answers beside the export, so from-export runs offline.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `Cargo.toml` | package `crosstalk-bench-adapter` (a composer), binary `ct-bench-detect`; `a2a-bench-format` at tag `a2a-bench-format-v1.0.0` | |
| `build.rs` | records the build's crosstalk commit | `CROSSTALK_ADAPTER_GIT` |
| `extract/ai-village.json` | the extract config AI Village runs pass with `--extract-config` | |
| `src/lib.rs` | crate root; the contract's failure codes | `FailureCode`, `WorldFailure`, `converter_version` |
| `src/config.rs` | detector names, variants, config digests | `live_info`, `pipeline_info`, `digest`, `live_variant`, `LIVE`, `PIPELINE`, `DEFAULT_VARIANT` |
| `src/input.rs` | the input directory in lockstep | `InputDir`, `WorldRead`, `InputError`, `read_manifest` |
| `src/convert.rs` | bench → spec messages and exchanges, the live P1 check | `world`, `message`, `body`, `check_part_text`, `exchange`, `client`, `failure`, `stop`, `ConvertedWorld` |
| `src/directory.rs` | what rows look up by id, over a converted world | `Directory`, `BenchDirectory` |
| `src/run.rs` | `--input` runs | `run`, `Mode`, `Summary`, `RunError`, `live_world`, `pipeline_world`, `live_failure`, `rows_failure` |
| `src/location.rs` | a part's whole text as a spec location | `whole_part`, `LocationError` |
| `src/reads.rs` | the read seam over the spec's read traits | `Resolved`, `Reads`, `ChannelResources`, `RegistryResources`, `ReadError` |
| `src/detect/mod.rs` | one exchange as a detector ingests it | `Timed` |
| `src/detect/live/mod.rs` | the `LiveBackend` seam and its driver | `LiveBackend`, `LiveWorld`, `LiveDetector`, `LiveSettings`, `Forwarding`, `RawDetection`, `Attribution`, `BackendError`, `LiveError`, `all_time` |
| `src/detect/live/gateway.rs` | the seam over `crosstalk_gateway::live::Live` | `GatewayBackend`, `GatewayWorld`, `flow_config` |
| `src/gateway.rs` | the bare pipeline as a detector | `PipelineDetector`, `ingest_exchanges`, `subscribe`, `capture_group`, `WorldClock`, `PipelineError` |
| `src/to_bench/mod.rs` | spec → bench errors and counts | `ToBenchError`, `Gap`, `Lossy` |
| `src/to_bench/message.rs` | spec → bench messages | `convert`, `media_kind` |
| `src/to_bench/world.rs` | a world built one exchange at a time; spec hash → bench id | `WorldBuilder`, `Draft`, `WorldExport`, `MessageIndex` |
| `src/to_bench/predictions.rs` | a detection as rows; the location sort | `rows`, `held`, `Unlocated` |
| `src/to_bench/resource.rs` | spec locators as bench resources | `resource` |
| `src/to_bench/kinds.rs` | carrier, codec, class, match kind, direction renames | |
| `src/to_bench/ids.rs` | spec ids and names as bench ids | `dataset`, `world`, `agent`, `detector_agent`, `transmission`, `exchange`, `range` |
| `src/to_bench/writer.rs` | the spilled predictions writer, the manifest file | `PredictionsWriter`, `manifest_digest`, `write_manifest`, `MANIFEST_FILE`, `MESSAGES_FILE`, `EXCHANGES_FILE` |
| `src/to_bench/manifest.rs` | build commit, dataset version, source digest | `CROSSTALK_COMMIT`, `DATASET_VERSION`, `digest_files`, `int` |
| `src/to_bench/verify.rs` | predictions read back against their manifest | `predictions_file`, `read_manifest`, `Mismatch` |
| `src/from_export/mod.rs` | node0 runs: files, detections, writing | `RunFiles`, `Detections`, `Outcome`, `write`, `saved_detections`, `replayed_detections`, `query_ids`, `gateway_version`, `read_truth`, `selection` |
| `src/from_export/capture.rs` | the capture world | `build`, `Capture`, `SameMicros`, `agent_names`, `session_owners` |
| `src/from_export/predict.rs` | the gateway's rows | `rows`, `ties`, `Predicted`, `AttributionSource` |
| `src/swarm/mod.rs` | a node0 run's files; errors | `SwarmError`, `MODEL`, `DATASET_PREFIX` |
| `src/swarm/schema.rs`, `truth_file.rs` | the demo swarm's truth file v2, read strictly (the header and agents' names are what the adapter uses) | `TruthLine`, `Header`, `Scenario`, `read`, `TruthFile`, `Row` |
| `src/swarm/window.rs` | the run window | `RunWindow`, `Margins`, `split`, `Split`, `Reused`, `truth_sessions`, `outside_reader` |
| `src/swarm/exchange_log.rs`, `bodies.rs` | the gateway's exchange log and blob store | `read`, `ExchangeLog`, `Sessions`, `BlobBodies`, `Cached`, `Bodies` |
| `src/swarm/detected.rs` | the verified export and evidence; what rows look up | `read_export`, `read_evidence`, `Exported`, `choose`, `SwarmDirectory`, `Blake3RowHasher` |
| `src/swarm/queried.rs` | the saved conversation reads | `Queried`, `EXCHANGE_TURNS_FILE`, `SPAN_POINTS_FILE`, `batches`, `origin_spans` |
| `src/swarm/fetch.rs` | L8 HTTP: export, evidence, conversation reads | `fetch`, `fetch_queried`, `export_request`, `FETCHED_STATES`, `FetchConfig` |
| `src/swarm/replay.rs` | a run's log through `Live` in memory | `replay`, `ReplaySettings`, `Replayed`, `demo_flow`, `read_bench_env` |
| `src/bin/ct-bench-detect/main.rs` | the CLI | |
| `tests/fixtures/bench/` | input views ct-eval's golden export wrote at ff3dc44 (SALT, collusion-wiki seed 3, swarm-traces, AgentDojo, τ²-bench, swe-splice ×4) and, under `expected/`, ct-eval's `run --predictions-out` on them | |
| `tests/bench_detect/` | P5 (frozen), reruns, failed worlds, world order, part text on every fixture message, from-export (evidence and query attribution, prior runs, access-only rows), replay | |
| `tests/live.rs` | the seam over a scripted backend: call order, every state's rows, rejected writes, merges | |
| `tests/live_gateway.rs` | the real composition on the fixtures; two runs byte-identical | |
| `tests/pipeline.rs` | the pipeline under a sim clock; `no_consumers` counts | |
| `tests/forwarding.rs` | `--forwarding` reaches L4 | |
| `tests/to_bench.rs` | spec ↔ bench messages | |
| `tests/rereads.rs`, `tests/fixtures/bench/wiki-rereads/` | collusion-wiki rereads through the live composition (INV-1155), checked against the bench converter's labels saved in `expected/wiki-rereads.labels.jsonl` | |
| `tests/swarm/` | the truth file, the export, the run window, replay, `swarm-fetch` over a test API, on a synthetic run (`fixture.rs`) | |

## Invariants and constraints

- **Part text.** Every converted part's spec text equals its bench text,
  part for part with the same count, or the world fails
  `part_text_mismatch` (P1, checked on every world of every run).
- **Location sort.** Matches by `read_at`, co-access records by
  `(read_at, write_at)` in `Location` order, ties by origin then row
  text; never the detector's order.
- **Determinism.** On one build and one input, two runs write the same
  bytes (`tests/live_gateway.rs`, `tests/bench_detect/p5.rs`;
  `a2a-bench run --twice` checks it from the bench's side).
- **Fresh composition per world.** No state crosses worlds: each live
  world is a new `Live` (resources canonicalize by URL or path).
- **Frozen parity (P5).** On the checked-in inputs, the predictions equal
  ct-eval's `run --predictions-out` at ff3dc44 byte for byte but for the
  header's `detector.version` and so the trailer's digest
  (`tests/bench_detect/p5.rs`). A detector change that moves them is
  reviewed and the expected files regenerated with it.
- A predictions file exists only complete: every world's rows passed
  `check_predictions`, the trailer is written, and the file is read back.
- No labels are read or written. `--input` never opens `labels.jsonl`;
  from-export reads no truth beyond the header, the agents' names and
  (for the same-µs report) the session owners.
- A failed world's reason names ids and byte counts, never text.

## History

ct-eval (`crates/eval`, `crosstalk-eval`) built the harness the bench grew
from: the SALT harness, reference matcher and pipeline detector (#59),
AgentDojo and τ²-bench (#64), the demo swarm's truth importer and gateway
export scoring (#67), AI Village (#70, #100), swarm truth v2 (#72), spec
58 and the live seam (#73), collusion-wiki and swarm-traces (#74), the SWE
background, splice and cipher corpora (#76), follow-ups (#78), the live
adapter (#83) and its fixes (#87), swarm bench fixes (#91, #93, #98),
rescoring after L4 match quality (#99), gate calibrations (#105, #109,
#114), the a2a-bench/1 golden export (#108) and `ct-bench-detect` (#112).
The bench proved parity P1–P7 against crosstalk commits `7f8a2fb` and
`4d3d2c3`, which stay reachable in history with ct-eval whole; its
scoring, converters, gates and reference matcher were then retired from
crosstalk, and the crate renamed `crosstalk-bench-adapter` (design §4.1).
