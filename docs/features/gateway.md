# Gateway: the `crosstalk` binary and the capture slice (M1)

`crosstalk-gateway` (`crates/gateway`) is the composition crate: the
`crosstalk` binary, its config, and the wiring of the layer crates into one
process. Roadmap item P3 (the capture slice, milestone M1). With it, Claude
Code pointed at the gateway through `ANTHROPIC_BASE_URL` works as it does
without it, and every generation exchange is normalized, its bodies stored,
announced on the bus and persisted.

The command line, the config shape, the listeners and the environment
follow the deployment contract in `docs/features/deploy.md` (branch
`feat/deploy`, PR #47).

## Scope

- The command line: `serve --role`, `migrate`, `healthcheck`, and
  `inspect` for reading back what was captured.
- The JSON config of the deployment contract, unknown fields refused at
  every level, secrets only as environment variable names.
- Process roles (`all`, `proxy`, `pipeline`, `api`, `analysis`) as sets of
  tasks.
- Single-node wiring: the L0 proxy (`crosstalk-ingress`), the capture stage
  (L1 normalization with `crosstalk-canonical`, bodies into `FsBlobStore`,
  `ExchangeCaptured` on `MpscBus` from `crosstalk-transport`), the exchange
  log (a P3 stopgap, see [Persistence](#persistence-a-p3-stopgap)), and the
  ops listener.
- The composition behind the proxy as a library entry point,
  `crosstalk_gateway::pipeline::Pipeline` (P3.1), generic over the spec's
  `BlobStore` and `EventBus` and an injected `Clock`, with
  `Pipeline::ingest` for pre-normalized exchanges (see
  [Pipeline](#pipeline-the-library-entry-point)).
- `crosstalk_gateway::live::Live`: every layer (L3 to L7, a minimal L6
  classifier) and the L8 surface in one process over the memory stores,
  with `Live::settle` for deterministic replay (see
  [Live](#live-the-whole-detection-path-in-one-process)).
- Graceful shutdown on SIGINT and SIGTERM.
- JSON logs on stdout.
- `migrate`: connects to Postgres and ensures the extensions
  (`crosstalk-store`); no layer has migrations yet.
- End-to-end tests over real sockets, simulation tests of the capture
  stage and of `ingest`, and a manual check with a real Claude Code session
  (`scripts/try-claude-code.sh`).

## Non-scope

- The L8 HTTP binding on `api.listen` (P7.1): the section is checked and
  nothing is bound. The `api` role starts nothing and says so in the logs.
- L6 (`analysis` role, `embeddings` section): checked and unused (P6).
- L3 to L7 consumers in the `serve` roles (P4 to P6): they run only in
  `Live`, over the memory stores. The `pipeline` role runs only the
  exchange log today.
- Topic modelling in `Live`: its classifier assigns every transmission
  unassigned under the active version.
- A cross-node bus (P9). The bus is in-process, so a `proxy` process and a
  `pipeline` process do not reach each other; `--role all` is the only role
  that captures and logs end to end (the gateway warns at start otherwise).
- Postgres persistence of exchanges: the spec has no exchange store (see
  [Gaps](#gaps-found)). `serve` does not need the database to forward or
  capture.
- Application metrics beyond the capture counters, and tracing spans.
- TLS on the listeners. The proxy speaks plain HTTP to harnesses.
- The eval harness itself (`crates/eval`, being built separately): this
  crate only gives it `Pipeline` and registers it as a composer in the
  architecture test.

## Commands

```text
crosstalk serve --role <all|proxy|pipeline|api|analysis> --config <path>
crosstalk migrate --config <path>
crosstalk healthcheck --url <url>
crosstalk inspect --config <path> [<exchange-id>]
crosstalk help
```

| Command | Behaviour | Exit |
| --- | --- | --- |
| `serve` | Starts the role's tasks; runs until SIGINT or SIGTERM, then shuts down gracefully | 0 after a shutdown; 1 on a bad config, a missing secret, an unbindable listener or an unopenable data directory; 2 on bad arguments |
| `migrate` | Needs a `store` section and `DATABASE_URL`. Connects, ensures `vector` and `pg_trgm`, then runs each layer's migrations (none exist yet). Idempotent | 0, or 1 with the reason (no `store`, `DATABASE_URL` missing or malformed, server unreachable, extension refused) |
| `healthcheck` | GETs an `http://` URL with hyper (the runtime image has no curl), 5 s limit | 0 on a 2xx, 1 otherwise |
| `inspect` | Without an id, one line per logged exchange (id, start, model, transport, outcome, request message count). With an id, the envelope and every message it names, each body read from the blob store, checked as a canonical encoding and printed as JSON | 0, or 1 (unknown id, unreadable log or store) |

## Config

One JSON document; every object refuses unknown fields. Secrets appear
only as `{"env": "<VARIABLE>"}`. A relative `blobs.root` is resolved
against the config file's directory.

| Key | Required | Shape |
| --- | --- | --- |
| `ingress` | yes | `crosstalk_ingress::config::IngressConfig`, unchanged: `listen`, `routes` (name, prefix, upstream id, kind, base URL), `secrets` (`current` `{version, env}` and an optional `previous` `{version, env, overlap_ends}`, an older version whose digests are also computed for exchanges that start before `overlap_ends`, an RFC 3339 timestamp at microsecond precision), `limits`, `capture.channel_capacity` |
| `api` | no | `{"listen": SocketAddr, "token": {"env": ..}, "operator": {"name": ..}}`: the HTTP API (roles `all`, `api`); the token signs in as `operator` (default `{"name": "admin"}`, every permission) |
| `ops` | yes | `{"listen": SocketAddr}` |
| `store` | no | `{"pool": crosstalk_store::PoolSettings}`; the URL is `DATABASE_URL` |
| `blobs` | yes | `{"root": path}` for `FsBlobStore::open`; its parent is the data directory |
| `embeddings` | no | `{"base_url": http(s) URL, "model": non-empty, "api_key": {"env": ..}}`; checked, unused |
| `bus` | no | transport's `BusConfig` (defaults) |
| `pipeline` | no | `{"blob_put_attempts": 3, "blob_put_backoff_ms": 100}` |
| `shutdown` | no | `{"drain_timeout_ms": 45000, "flush_timeout_ms": 10000}`; together under compose's 60 s grace period |
| `flow` | no | crosstalk-flow's `FlowConfig`, each key defaulted: `{"correlation_window_ms": 600000, "evidence_window_ms": 120000, "suspected_ttl_ms": 1800000, "shards": 1, "tick_ms": 1000}`; checked at start (`LiveError::Flow`) |

Checked values: environment variable names are non-empty without `=` or
NUL (`EnvVarName`), URLs are `http(s)` with a host (`HttpUrl`), the model
is non-empty (`NonEmpty`), counts and durations are non-zero, pool
settings go through `PoolSettings`' own checks, and `blobs.root` must have
a parent directory.

`crates/gateway/config.example.json` is this shape for localhost (ports
8080, 8081, 9464; blobs under the repository's `target/crosstalk-dev`).
`crates/gateway/.env.example` lists the environment.

The example loads one secret. During a rotation, `secrets` names both
versions and when the old one stops keying digests; after that instant
`previous` can be removed:

```json
"secrets": {
  "current": {"version": 2, "env": "CROSSTALK_SECRET_V2"},
  "previous": {"version": 1, "env": "CROSSTALK_SECRET_V1",
               "overlap_ends": "2026-11-01T00:00:00.000000Z"}
}
```

A previous version that is not older than the current one is refused at
start (exit 1).

### Environment

| Variable | Read by |
| --- | --- |
| `CROSSTALK_SECRET_V1` (whatever `ingress.secrets.current.env` names, and `previous.env` during a rotation) | `serve` (roles running the proxy): 64 hex digits keying credential and account digests; surrounding whitespace such as a trailing newline is ignored |
| `DATABASE_URL` | `migrate`; `serve` when `store` is configured (missing or malformed is a start error; unreachable only makes `/readyz` fail) |
| `CROSSTALK_API_TOKEN` (whatever `api.token.env` names) | `serve` (roles `all` and `api` with an `api` section): the operator bearer token, at least 16 `b64token` characters; missing or malformed is a start error |
| `CROSSTALK_EMBEDDINGS_API_KEY` | nothing yet (its section is checked, not used) |
| `RUST_LOG` | the log filter (default `info`; `inspect` defaults to `warn`) |

## Roles

Every role but `analysis` runs a [`Live`](#live-the-whole-detection-path-in-one-process)
process over the memory stores (the wall clock, `Ticking::Periodic`
every `flow.tick_ms`, the blob store at `blobs.root`). `/readyz` lists
the tasks below; `live` is ready while every layer stage runs.

| Role | Tasks |
| --- | --- |
| `all` | `exchange_log`, `capture`, `live`, `proxy`, `api` (with an `api` section), ops: capture, detect and serve end to end |
| `proxy` | `capture`, `live`, `proxy`, ops |
| `pipeline` | `exchange_log`, `live`, ops (nothing publishes in this process) |
| `api` | `live`, `api`, ops: the HTTP API over a live process that nothing feeds yet |
| `analysis` | ops only; the startup log names what is not built |

The bus and the stores are in-process until the cross-node bus (P9) and
the Postgres stores are wired, so processes of different roles do not
reach each other; only `all` is useful today.

## Listeners

| Config key | Serves |
| --- | --- |
| `ingress.listen` | The reverse proxy. `ANTHROPIC_BASE_URL=http://<host>:<port>/anthropic` |
| `ops.listen` | `GET /healthz`: 200 while the process serves, with the counters as JSON. `GET /readyz`: 200 or 503 with the checks (database reachable when `store` is configured, migrations at head, every role task running, not draining). `GET /metrics`: Prometheus text. Anything else is 404 |
| `api.listen` | The L8 HTTP binding (`crosstalk-api`'s `HttpApi`, [http_server.md](http_server.md)) over the live process's surface, roles `all` and `api`. Auth: `Authorization: Bearer <api.token>` is the one operator `api.operator` (default named `admin`, id `ApiOperator::ID`, every permission), loaded in authenticated mode; anything else is `401`. Exports are JSONL |

`/healthz` body (pinned by `ops::tests::health_report_json_is_pinned`).
`live` is `null` for `analysis`; `live.stages` counts the deliveries and
side inputs each stage handled, by slot name; `live.watermark_micros` is
the L7 watermark (microseconds since the epoch, 0 until it first moves):

```json
{"status": "ok",
 "capture": {"captured": 3, "unclassified": 0, "decode_error": 0,
             "channel_full": 0, "channel_closed": 0, "response_too_large": 0,
             "ids_exhausted": 0},
 "pipeline": {"published": 3, "normalize_failed": 0, "store_failed": 0,
              "store_retries": 0, "publish_failed": 0},
 "log": {"written": 3, "duplicates": 0, "write_failed": 0},
 "live": {"stages": {"evidence": 5, "l3-reconstruct": 3, "l4-provenance": 6,
                     "l5-flow": 14, "l6-classify": 1, "l7-topology": 4,
                     "surface-relay": 31},
          "watermark_micros": 1790845200000000}}
```

`/readyz` body: `{"ready": true, "role": "all", "status": "ok", "database":
"not_configured" | "reachable" | "unreachable: <why>", "migrations":
"at_head", "tasks": [{"name": "exchange_log", "running": true}, {"name":
"capture", ..}, {"name": "live", ..}, {"name": "proxy", ..}, {"name": "api",
..}]}`.

`/metrics` series: `crosstalk_draining`,
`crosstalk_capture_exchanges_total`,
`crosstalk_capture_uncaptured_total{reason}`,
`crosstalk_pipeline_exchanges_total{outcome}`,
`crosstalk_pipeline_blob_put_retries_total`,
`crosstalk_exchange_log_deliveries_total{outcome}`, unchanged by `Live`
(the live counts are in `/healthz` only).

The `normalize_failed` outcome of `crosstalk_pipeline_exchanges_total`
also carries `reason` and `protocol`, fixed codes from
`normalize_failure` (never free text): `reason` is `request_body` (the
normalizer refused the body, `NormalizeError::RequestBody`) or
`unsupported_protocol` (no normalizer handles the exchange's protocol);
`protocol` is the exchange's `WireProtocol` wire name
(`anthropic_messages`, `open_ai_chat`, `open_ai_responses`,
`gemini_generate`, `gemini_code_assist`). Every one of the ten pairs is
exposed, zeros included, and there is no unlabelled `normalize_failed`
series beside them, so `sum by (outcome)` is the refusal total that
`/healthz` reports as `pipeline.normalize_failed`. The other outcomes keep
their single `{outcome}` series:

```text
crosstalk_pipeline_exchanges_total{outcome="published"} 3
crosstalk_pipeline_exchanges_total{outcome="normalize_failed",reason="unsupported_protocol",protocol="anthropic_messages"} 0
...
crosstalk_pipeline_exchanges_total{outcome="normalize_failed",reason="request_body",protocol="anthropic_messages"} 2
...
crosstalk_pipeline_exchanges_total{outcome="store_failed"} 0
crosstalk_pipeline_exchanges_total{outcome="publish_failed"} 0
```

## Data and control flow

```text
harness ──HTTP──▶ server::serve (proxy listener, hyper http1, no Date)
                    │ Proxy::handle (crosstalk-ingress): route, classify, forward, relay
                    │ generation exchange ends ──▶ RawExchange ── bounded mpsc (capacity from config) ──┐
                    ▼                                                                                    │
                 client response, unchanged                                                              ▼
                                                                         capture::CaptureStage::run (one task, spawned by Live::start)
                                                                           AnthropicMessages::normalize_with_media (L1)
                                                                             └ refused ─▶ normalize_failed (by reason, protocol); debug log of the body's shape
                                                                           Ingester::ingest(normalization, clock.now())   ◀── Pipeline::ingest(NormalizedExchange, at)
                                                                             crosstalk_canonical::store ─▶ FsBlobStore (blobs.root)
                                                                               └ retried blob_put_attempts times ─▶ store_failed, nothing published
                                                                             under the id lock: EventId minted at `at`;
                                                                             Envelope { EventId, at, ExchangeCaptured(Exchange) }
                                                                             MpscBus::publish ─▶ every subscribed group
                                                                                                │
                                         log::consumer::run (group "exchange-log") ◀────────────┤
                                           ExchangeLog::append: one JSON line, synced; ack after; nack on failure
                                           <data dir>/exchanges/exchange-log.jsonl              │
                                         Live's layer stages (groups live-l3-reconstruct .. ) ◀─┘ ─▶ memory stores ─▶ Surface
operator ──HTTP──▶ crosstalk_api::http::serve (api listener): Auth (Bearer api.token ─▶ api.operator) ─▶ Surface
```

- **Start** (`gateway::start`, or `gateway::start_on` with a
  `LiveClock`, which tests use to drive `Live::settle`): resolve the data
  directory and, with a `store` section, `DATABASE_URL`; open the blob
  store and (pipeline) the log; build the proxy from the ingress config,
  reading its secrets through the environment lookup; read the API token
  (`api.token`); bind the proxy, API and ops listeners; start the `Live`
  process (`LiveConfig::new` with the gateway's `flow`, `bus` and
  `pipeline` sections, the opened blob store, `Ticking::Periodic`, the
  capture channel and the log, and access mode authenticated for
  `api.operator` when there is an `api` section, trusted otherwise), which
  subscribes every group before anything can publish and spawns
  `exchange_log`, `capture` and the layer stages; mount `HttpApi` on its
  surface and spawn the API and proxy listeners (each task tracked by a
  running flag for `/readyz`, the stages together as `live`); with
  `store`, connect to Postgres in the background, retrying every 5 s.
- **Envelope ids** come from the spec's `UlidGenerator` (seeded from the
  operating system's randomness), owned by the pipeline's `Ingester` behind
  a `tokio::sync::Mutex`, and minted with `mint_at` at the envelope time
  `at`. On the proxy path `at` is the injected clock's reading after
  normalization, before the store. If no id is left (`UlidExhausted`),
  the event is not published and is counted `publish_failed`.
- **Shutdown** (`Running::shutdown`), in dependency order:
  1. `/healthz` reports `draining` and `/readyz` 503.
  2. The proxy listener closes (new connections are refused) and every
     open connection gets hyper's graceful shutdown: an idle keep-alive
     connection closes at once, one with a response in flight closes after
     it ends. Up to `drain_timeout_ms`; connections still open are then
     aborted, which the proxy records as `ClientDisconnected` and still
     hands to capture.
  3. The API listener stops (graceful; requests in flight finish, a live
     stream ends with the feed), within `drain_timeout_ms`.
  4. `Live::shutdown`: with the proxy and its connections gone, the
     capture channel closes once the last per-exchange capture task has
     handed off; the capture stage drains it; every layer group and the
     exchange log's group drain (bus depth zero), the bus stops, the
     stages end and the consumer closes the log (flush and `fsync`).
  5. The Postgres pool closes and the ops listener stops last.

  Step 4 has one `flush_timeout_ms` deadline; a task still
  running at it is aborted and the report says so.

## Pipeline: the library entry point

`crosstalk_gateway::pipeline` (roadmap P3.1) is the gateway's composition
behind the proxy, usable without the binary. The `serve` roles build one
(`gateway::start`), and so does the eval harness (`crosstalk-eval`, a
composer in the architecture test), over the simulation's stores and
clock.

```rust
let pipeline = Pipeline::build(
    Settings { put_retry, consumer_retry },   // or Settings::from_config(&config), Settings::default()
    Deps { blobs, bus, id_entropy, capture: Some(receiver), exchange_log: Some(log) },
    clock,                                    // Arc<dyn Clock>: SystemClock, or crosstalk-sim's SimClock
).await?;                                     // Result<Pipeline<B, E>, pipeline::BuildError>
let id: EventId = pipeline.ingest(normalized, at).await?;   // Result<EventId, IngestError>
```

- **Build.** Generic over any `B: BlobStore` and `E: EventBus` (both
  `Send + Sync + 'static`). It subscribes the consumer stages first (the
  exchange log's group, when `Deps::exchange_log` is given), then spawns
  them and the capture stage (when `Deps::capture` is given) on its
  `Tasks` (`exchange_log`, `capture`). `Deps::stores(blobs, bus, entropy)`
  is a pipeline fed only through `ingest`. Nothing reads the system time:
  the id generator and the capture stage read the injected clock, and
  backoffs are tokio sleeps, so a pipeline runs under paused time.
- **Ingest.** `Pipeline::ingest(NormalizedExchange, at)` (or the cloneable
  `Ingester` from `pipeline.ingester()`, for other tasks):
  1. store every message body and media blob with
     `crosstalk_canonical::store`, retrying the whole put set up to
     `put_retry.attempts` times, `put_retry.backoff` apart (puts are
     idempotent); when every attempt fails, publish nothing and return
     `IngestError::NotStored { attempts, source }`;
  2. under the id lock, mint the envelope's `EventId` at `at` (when `at`
     is in or before the last id's millisecond, the last id plus one) and
     publish `ExchangeCaptured` in an envelope stamped `at`; no id left is
     `IngestError::IdsExhausted { at }`, a refused publish
     `IngestError::NotPublished(BusError)`.
  Each outcome is counted in `PipelineStats` (`published`, `store_failed`,
  `store_retries`, `publish_failed`) before it returns.
- **One path after L1.** The capture stage normalizes a `RawExchange` with
  `AnthropicMessages` (a refusal is `CaptureError::NotNormalized(Refusal)`,
  counted `normalize_failed` under its reason and protocol, with the
  refused body's top-level shape logged at debug) and calls the same
  `Ingester::ingest` with the clock's reading.
  `pipeline_ingest_matches_the_proxy_path` checks that both put the same bytes in the same order and publish the same envelope.
- **Shutdown.** `join_capture(deadline)` waits for the capture stage once
  the caller has dropped every capture sender; `join_consumers(deadline)`
  waits for the consumers once the bus has stopped; for `MpscBus`,
  `Pipeline::shutdown(deadline) -> Drained { capture, log }` does both
  around the group drain and the bus shutdown.

### Public interface

| Item | What |
| --- | --- |
| `Pipeline<B, E>` | `build`, `ingest`, `ingester`, `blobs`, `bus`, `stats`, `log_stats`, `tasks`, `join_capture`, `join_consumers`; `shutdown` when `E = MpscBus` |
| `Settings` | `put_retry: PutRetry`, `consumer_retry: RetryPolicy`; `from_config(&GatewayConfig)`, `Default` (3 attempts, 100 ms; the bus's default policy) |
| `Deps<B, E>` | `blobs`, `bus`, `id_entropy: SeededRandom`, `capture: Option<mpsc::Receiver<RawExchange>>`, `exchange_log: Option<ExchangeLog>`; `Deps::stores` |
| `Ingester<B, E>` | cloneable handle: `ingest`, `now`, `blobs`, `bus`, `stats`, `retry` |
| `IngestError` | `NotStored { attempts, source: StoreError }`, `IdsExhausted { at }`, `NotPublished(BusError)` |
| `BuildError` | `Subscribe(BusError)` |
| `Drained` | `capture`, `log`: whether each drained by the deadline |
| `PipelineStats`, `PipelineCounts`, `PutRetry` | the counters (`/healthz`, `/metrics`; refusals by reason and protocol through `normalize_failures`) and the put retry policy, moved here from `capture` |
| `capture::CaptureStage<B, E>` | `new(Ingester)`, `run(receiver)`, `capture(&RawExchange) -> Result<EventId, CaptureError>` |
| `capture::CaptureError` | `NotNormalized(Refusal)`, `Ingest(IngestError)` |
| `capture::Refusal` | `UnsupportedProtocol(WireProtocol)`, `Refused { protocol, error: NormalizeError }`; `failure()` is its `NormalizeFailure` (the `/metrics` labels) |

## Live: the whole detection path in one process

`crosstalk_gateway::live::Live` is every layer and the L8 surface in one
process, over one set of in-memory stores: what the UI binary hosts, what
the e2e smoke (`crosstalk_e2e::compose`) drives, and what eval builds
against. It is memory-only (`crosstalk-memory`'s reference stores, plus
L3's `MemoryConversations` and L4's `MemoryProvenanceStore`); the blob
store is in memory or on disk.

```rust
let live = Live::start(LiveConfig {
    surface,                       // crosstalk_api::InProcessOptions (its clock and timing are replaced)
    clock: LiveClock::Manual(clock), // or LiveClock::Read(Arc<dyn Clock>) for the wall or a sim clock
    blobs: BlobConfig::Memory,     // or BlobConfig::Fs { root }
    bus: BusConfig::default(),
    pipeline: Settings::default(), // put retry; consumer_retry is every slot group's policy
    flow: FlowConfig::default(),   // correlation_window_ms, evidence_window_ms, suspected_ttl_ms, shards, tick_ms
    provenance: ProvenanceConfig::default(),
    threading: ThreadConfig::default(), // L3's seen-message retention (30 days)
    ticking: Ticking::OnSettle,    // or Ticking::Periodic (every flow.tick_ms, plus settle)
    seed: 7,                       // every id generator's entropy
    capture: None,                 // or the proxy's capture receiver
}).await?;                         // Result<Live, LiveError>
// Or with the surface's defaults, on any clock:
// LiveConfig::new(LiveClock::Read(clock), FlowConfig { evidence_window_ms: 10_000, suspected_ttl_ms: 60_000, ..Default::default() }, seed)?
live.pipeline().ingest(normalized, at).await?;   // replay and live capture take the same path
let settled = live.settle(until).await?;         // Result<Settled { at, passes }, SettleError>
let page = live.stores().transmissions.list(&query, &request).await?;  // TransmissionStore::list
let placed = live.layers().conversations.placement(exchange).await?;   // ExchangePlacements::placement
live.shutdown(deadline).await;                   // LiveDrained { capture, stages }
```

### Data and control flow

```text
proxy capture ─▶ CaptureStage (L1) ─┐
replay / caller ─▶ pipeline().ingest(exchange, at) ─▶ blobs + ExchangeCaptured
                                    ▼
MpscBus, one consumer group per slot (live-<slot>):
  L3 reconstruct   ExchangeCaptured ─▶ agents, conversations ─▶ AgentSeen, ConversationDelta
  L4 provenance    ExchangeCaptured, ConversationDelta ─▶ scan (spans, matches)
                   ─▶ extraction step (Extracted, a local channel to L5)
                   ─▶ SpanOriginated, SpanRelayed, ContentMatched
  L5 flow          Extracted + ExchangeCaptured, ContentMatched, ChannelDiscovered, ... + ticks
                   ─▶ resources, accesses, channels, transmissions
                   ─▶ AccessRecorded, ChannelCrossAccessed, TransmissionConfirmed/Suspected
  L6 classify      TransmissionConfirmed ─▶ catalog assignment, Classified state ─▶ TransmissionClassified
  L7 topology      TransmissionClassified, AccessRecorded, VerdictSet, topic versions ─▶ edges ─▶ EdgeUpdated
  evidence         SpanOriginated ─▶ MemoryEvidence (span from SpanIndex::spans on
                   L4's MemoryProvenanceStore; MemoryEvidence reads accesses and resources
                   from the registry itself)
  surface relay    every subject but ExchangeCaptured ─▶ node facts, live feed
stores ─ Outbox ─▶ forward_outbox ─▶ bus      (ChannelDiscovered, Changed::*, AlertRuleChanged, ...)
Surface<LiveStores>: crosstalk-api's InProcess::start_with over the same stores, bus and blobs
```

- **Start.** Open the blob store, start the bus, build the surface with
  `InProcess::start_with(options, Backbone { bus, blobs, outbox, events })`
  (the stores publish into the outbox; the relay reads `events`, fed by
  the surface relay stage), build the pipeline, fill every slot
  (`wiring::wire_all`), subscribe every slot's group, and only then spawn
  the stages, the outbox forwarder, the ticker (`Ticking::Periodic`) and
  the capture stage.
- **Slots.** A slot holds a [`Stage`] (`subjects`, `handle(&Envelope)`,
  `tick(now)`) run by the generic loop, or a whole task
  (`Stages::fill_task`) for a consumer with inputs besides the bus (L5).
  `StageError::Retry` nacks (redelivered under the group's policy, then
  dead-lettered); `StageError::Reject` acks and logs at error. Every
  stage also answers `Command::Tick { now }` and `Command::Drain`.
- **L3.** `crosstalk_reconstruct::consumer::ReconstructConsumer` over the
  shared `MemoryAgents`, a `ConversationThreader` over the process's
  `MemoryConversations`, and ULID sources seeded from `seed`. It
  publishes its own envelopes (ids derived from the exchange).
- **L4 and the extraction step.** `crosstalk_provenance::engine::Provenance`
  over a `MemoryFingerprintIndex`, the process's `MemoryProvenanceStore`,
  no semantic matcher and the blob store's messages, driven by the stage
  rather than the crate's `run` loop, so eviction follows ticks. After a
  delta is scanned, the extraction step (`layers::extract`) turns its
  tool calls and results into the flow consumer's `Extracted` inputs, and
  only then are provenance's envelopes published: the flow consumer takes
  queued extracted inputs before its next delivery, so it sees a read's
  access before the content match the read carried. A write call's
  accesses are held (`Extracted::Write { outcome: None }`) with the spans
  `write_spans` finds at the call's part; its result, in a later delta of
  the conversation, releases them (`WriteResult`) and yields the call's
  reads, by the reading agent in the result's exchange at that exchange's
  start, naming the result part. A server tool's result in the same
  output is extracted at once. Access ids are derived from the exchange,
  the call and the access's place.
- **L5.** `crosstalk_flow::consumer::FlowConsumer` over the shared
  registry (`MemoryChannels`), `MemoryVerdicts` and `MemoryAgents`, on its
  own task: commands first (a tick drains the queued extracted inputs,
  then `FlowConsumer::tick(now)`), then extracted inputs, then
  deliveries.
- **L6.** The gateway's minimal `Classifier`: on `TransmissionConfirmed`
  it assigns the transmission, unassigned (`topic: None`), under the
  catalog's active version (`TopicLifecycle::assign`), saves it as
  `Classified` (`TransmissionStore::save`) and publishes
  `TransmissionClassified { cause: Confirmation }`. No topic model runs
  in the process.
- **L7.** `crosstalk_topology::consumer::handle` over the shared
  `InMemoryEdgeStore`, announcing through a `BusAnnouncer`. **Watermark:**
  on each tick at `now` (after L5's tick at the same instant), when every
  group from L3 to L7 is empty, the stage calls
  `EdgeStore::advance_watermark(PipelineFrontier { ticked_through: now,
  oldest_pending: None })`; the store settles it by the spec's rule
  (`Watermark::settled`: `now` minus `evidence_window + suspected_ttl`,
  aligned down to a bucket) and never lowers it. With a group still busy
  the tick leaves it alone; the next tick or settle pass retries.
  `Live::watermark()` and `/healthz`'s `live.watermark_micros` read it
  (INV-1059).
- **Settle.** `Live::settle(until)` moves a manual clock forwards to
  `until` (never backwards; a read clock stays), then runs passes until
  one handles nothing new: wait until every slot's group is empty, the
  outbox is flushed and every stage's side inputs are drained (twice in a
  row), tick every stage at the clock's time in slot order, wait again.
  It gives up with `SettleError::NotQuiet` after 64 passes. Under
  `Ticking::OnSettle`, nothing time-driven runs between settles, and
  every id is derived from its input or drawn from a seeded generator in
  input order, so the same input settles to the same stores
  (`crosstalk_e2e` `determinism::two_settled_runs_give_identical_transmissions`).
- **Shutdown.** Stop the ticker, join the capture stage, wait for the
  groups to empty, stop the bus, join the stages, stop the forwarder and
  the surface's relay and live feed.

### Public interface

| Item | What |
| --- | --- |
| `Live` | `start(LiveConfig)`, `pipeline() -> &Arc<LivePipeline>`, `surface() -> &Arc<Surface<LiveStores>>`, `stores() -> &LiveStores`, `layers() -> &LayerStores`, `context()`, `clock()`, `filled()`, `caller(RequestIdentity)`, `settle(Timestamp) -> Result<Settled, SettleError>`, `shutdown(Instant) -> LiveDrained` |
| `LiveConfig` | `surface`, `clock: LiveClock`, `blobs: BlobConfig`, `bus`, `pipeline: Settings`, `flow: FlowConfig`, `provenance: ProvenanceConfig`, `threading: ThreadConfig`, `ticking: Ticking`, `seed`, `capture` |
| `LiveConfig::new(LiveClock, FlowConfig, seed)` | the defaults: memory blobs, `Ticking::OnSettle`, trusted access, five-minute buckets (`DEFAULT_BUCKET`), the default provenance and threading configs; `DefaultsError` |
| `LiveError` | `Flow`, `Blobs`, `Bus`, `Surface`, `Pipeline`, `Slot`, `Subscribe { slot, error }` |
| `LiveClock` | `Read(Arc<dyn Clock>)`, `Manual(ManualClock)`; `reader`, `now`, `advance_to` |
| `Ticking` | `Periodic`, `OnSettle` |
| `Settled`, `SettleError` | `{ at, passes }`; `StageStopped(Slot)`, `OutboxStopped`, `Depth { slot, error }`, `NotQuiet { passes }` |
| `LiveStores`, `LayerStores` | `MemoryStores<LiveBlobs>`; `{ conversations: MemoryConversations, provenance: MemoryProvenanceStore }`, `LayerStores::new(ThreadConfig)` |
| `BlobConfig`, `LiveBlobs` | `Memory`, `Fs { root }`; the one `BlobStore` over either |
| `Stage`, `Stages`, `Slot`, `StageContext`, `StageError`, `Command`, `Control`, `Activity`, `Publisher` | the slot interface (above) |
| `wiring::wire_all`, `wire_l3` .. `wire_l7`, `wire_evidence` | what fills each slot |
| `layers::{Reconstruct, ProvenanceStage, Extraction, Topology, l5::fill}` | the layer stages |
| `Classifier`, `EvidenceFeeder`, `IndexedSpans`, `SpanSource` | L6, the evidence feeder and its span source |
| `pipeline::Ingester::publish(BusEvent, at)`, `PublishError` | publish a derived event with an id from the pipeline's generator |

## Persistence: a P3 stopgap

The spec has no exchange store or exchange log trait. Bodies go to the
spec's `BlobStore` (`FsBlobStore` at `blobs.root`), and each
`ExchangeCaptured` envelope is appended, as its wire JSON, to
`<parent of blobs.root>/exchanges/exchange-log.jsonl` by a bus consumer
(`log` module). Everything the gateway writes is under the parent of
`blobs.root` (`/var/lib/crosstalk` in the deployment, a persistent volume).

- An append is written and `fdatasync`ed before the delivery is acked; a
  failed append is nacked and retried by the bus, then dead-lettered.
- Envelope ids already in the log are skipped (the bus is at least once).
- A torn last line (a crash mid-append) is ignored on read and truncated
  when the log is next opened for writing.
- The log is only ever appended to.

When the spec grows an exchange store (Postgres), this module is replaced.

## Manual check with Claude Code

```sh
scripts/try-claude-code.sh          # builds, writes target/crosstalk-dev/config.json, runs serve --role all
```

The script builds the debug binary, generates a deployment secret into
`target/crosstalk-dev/.env` (mode 600, reused on later runs; skipped when
`CROSSTALK_SECRET_V1` is already set), derives
`target/crosstalk-dev/config.json` from the example (listening on
127.0.0.1, `store` dropped so no Postgres is needed, blobs under
`target/crosstalk-dev/blobs`), prints what to export and runs the gateway
in the foreground. `CROSSTALK_PROXY_PORT`, `CROSSTALK_OPS_PORT` and
`CROSSTALK_RUN_DIR` override the defaults (8080, 9464,
`target/crosstalk-dev`). Then, in another terminal:

```sh
export ANTHROPIC_BASE_URL=http://127.0.0.1:8080/anthropic
claude                                     # use it as usual; API key or Claude Pro/Max login both pass through
curl -s http://127.0.0.1:9464/healthz      # capture counters
target/debug/crosstalk inspect --config target/crosstalk-dev/config.json
target/debug/crosstalk inspect --config target/crosstalk-dev/config.json <exchange-id>
```

Requests go on to `https://api.anthropic.com` with the user's own
credentials, unchanged; the gateway keeps only keyed hashes of them. The
example route's upstream kind is `vendor_api`; a Claude Pro/Max login can
set `{"type": "subscription", "data": {"type": "anthropic"}}` instead,
which only changes what the exchanges record. Ctrl-C stops the gateway
gracefully.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `crates/gateway/Cargo.toml` | Manifest: spec, api, canonical, flow, ingress, memory, provenance, reconstruct, store, surface, topology, transport; blake3, bytes, http-body-util, hyper (client, http1, server), hyper-util, serde, serde_json, thiserror, tokio, tracing, tracing-subscriber (env-filter, fmt, json, std); dev: sim, testkit, tempfile | — |
| `config.example.json`, `.env.example` | The localhost config and the environment | — |
| `src/main.rs` | The binary: parse, log, run, wait for SIGINT/SIGTERM, shut down | `main` |
| `src/lib.rs` | Crate doc and modules | — |
| `src/cli.rs` | The command line | `Command` (`parse`), `UsageError`, `USAGE` |
| `src/config/mod.rs`, `sections.rs` | The config and its checked values | `GatewayConfig` (`from_json`, `load`, `data_dir`, `exchange_log_path`, `flow`), `ApiConfig`, `ApiOperator` (`ID`), `FlowConfig` (re-exported from crosstalk-flow), `OpsConfig`, `StoreSection`, `BlobsConfig`, `EmbeddingsConfig`, `PipelineConfig`, `ShutdownConfig`, `EnvRef`, `EnvVarName`, `HttpUrl`, `NonEmpty`, `ConfigError`, `exchange_log_path` |
| `src/role.rs` | Roles and their tasks | `Role` (`runs_proxy`, `runs_pipeline`, `runs_live`, `runs_api`, `not_built`), `UnknownRole` |
| `src/gateway.rs` | Role wiring around a `Live` process: the proxy, API and ops listeners, start and shutdown | `start`, `start_on`, `Running` (`proxy_addr`, `api_addr`, `ops_addr`, `bus`, `blobs`, `live`, `health`, `readiness`, `shutdown`), `StartError`, `ShutdownReport` |
| `src/pipeline/mod.rs` | The library entry point: build, stages, shutdown | `Pipeline`, `Settings`, `Deps`, `BuildError`, `Drained`; re-exports `Ingester`, `IngestError`, `PipelineStats`, `PipelineCounts`, `PutRetry` |
| `src/pipeline/ingest.rs` | Ingest at L1: store (retried), mint, publish | `Ingester` (`ingest`, `now`, `blobs`, `bus`, `stats`, `retry`), `IngestError` |
| `src/pipeline/stats.rs` | Counters and put retry; refusals held as a `FailureStats` by reason and protocol | `PipelineStats` (`snapshot`, `normalize_failures`), `PipelineCounts`, `PutRetry` |
| `src/capture.rs` | The capture stage: L1 normalization (refusals counted by reason and protocol, their request shape logged at debug), then `Ingester::ingest` | `CaptureStage` (`new`, `run`, `capture`), `CaptureError`, `Refusal` (`failure`) |
| `src/normalize_failure.rs` | Refusal codes and counters: the `reason` and `protocol` labels | `FailureReason` (`of`, `code`, `ALL`), `NormalizeFailure`, `FailureStats`, `FailureCounts` (`get`, `total`, `iter`, `with`), `PROTOCOLS`, `protocol_code` |
| `src/log/mod.rs` | The exchange log file | `ExchangeLog` (`open`, `append`, `close`), `Appended`, `read`, `LogContents`, `LogError` |
| `src/log/consumer.rs` | The exchange log's bus consumer | `run`, `GROUP`, `group`, `LogStats`, `LogCounts` |
| `src/server.rs` | Accept loop with graceful, bounded drain (proxy and ops) | `serve`, `ServeOptions`, `DrainReport` |
| `src/ops/mod.rs`, `metrics.rs` | `/healthz`, `/readyz`, `/metrics` | `Ops` (`health`, `readiness`, `handle`), `HealthReport` (with `live`), `Readiness`, `TaskState`, `CaptureReport`, `Phase`, `metrics::render` (the health report and the refusal counts) |
| `src/live/mod.rs` | `Live`: start, accessors, shutdown | `Live`, `LiveConfig`, `LiveError`, `LiveDrained`, `LivePipeline`, `Ticking` |
| `src/live/settle.rs` | Driving the process to a fixed point | `Live::settle`, `Settled`, `SettleError` |
| `src/live/stage.rs` | The slot interface and the generic stage loop | `Stage`, `Stages`, `Slot`, `StageContext`, `StageError`, `Command`, `Control`, `Activity`, `Publisher`, `LiveStores`, `LayerStores`, `settle_delivery` |
| `src/live/wiring.rs` | One function per slot | `wire_all`, `wire_l3`, `wire_l4`, `wire_l5`, `wire_l6`, `wire_l7`, `wire_evidence` |
| `src/live/layers/` | The layer stages: `l3.rs` (reconstruct), `l4.rs` (provenance), `extract.rs` (L5's extraction step), `l5.rs` (flow task), `l7.rs` (topology) | `Reconstruct`, `ProvenanceStage`, `Extraction`, `ExtractStepError`, `l5::fill`, `Topology` |
| `src/live/classify.rs` | The minimal L6 classifier | `Classifier` |
| `src/live/evidence.rs` | Spans into the surface's evidence records (accesses and resources are read from the registry) | `EvidenceFeeder`, `SpanSource`, `IndexedSpans`, `SpanSourceError` |
| `src/live/relay.rs` | The outbox forwarder and the surface relay stage | — |
| `src/live/clock.rs`, `blobs.rs` | The injected clock; the blob store choice | `LiveClock`; `BlobConfig`, `LiveBlobs` |
| `src/live/defaults.rs` | `LiveConfig::new`: the surface's defaults | `DEFAULT_BUCKET`, `DefaultsError` |
| `src/live/tests.rs` | `crosstalk_gateway::live::tests::*` | — |
| `src/tasks.rs` | Per-task running flags, and probes for a group of tasks | `Tasks` (`spawn`, `probe`, `states`) |
| `src/store.rs` | `migrate` and the background connection `/readyz` checks | `migrate`, `MigrateError`, `store_config`, `StoreProbe`, `StoreCheck` |
| `src/healthcheck.rs` | The healthcheck client | `check`, `CheckError`, `TIMEOUT` |
| `src/inspect.rs` | Reading back the log and bodies | `list`, `show`, `InspectError` |
| `src/logging.rs` | JSON log setup | `init`, `try_init`, `Sink` |
| `src/tests/` | `crosstalk_gateway::tests::*`: the capture stage simulation (`dst.rs`), the `ingest` simulations (`ingest.rs`) and the refusal counts (`refusals.rs`) over raw exchanges built from the corpus (`raw.rs`), with recording store and bus wrappers (`record.rs`) | — |
| `tests/e2e/` | `crosstalk_gateway::e2e::*`: end-to-end tests (`support.rs` starts a gateway in front of testkit's fake upstream) | — |
| `tests/logs.rs` | The log redaction test (its own binary: it installs the global subscriber) | — |
| `tests/architecture.rs` | The workspace dependency rule ([workspace](workspace.md)) | — |
| `scripts/try-claude-code.sh` | The manual Claude Code check | — |

## Tests

| Test | What it shows |
| --- | --- |
| `e2e::generation_cases_pass_through_unchanged_and_are_captured_once` | Every generation corpus case: client response and upstream request unchanged (testkit's `differences_from`); exactly one `ExchangeCaptured` per request, equal to L1's golden normalization (continuation, request hashes, outcome without times, protocol, model, transport) with the harness claim, ids, class and credential scheme the case declares; every golden body in the blob store under its hash, decoding as a canonical body equal to the golden's; after shutdown the exchange log holds exactly the published envelopes |
| `e2e::consumer_reads_every_referenced_blob` | INV-48 (integration): a consumer reading the blob root through its own `FsBlobStore` finds every referenced body when each event arrives |
| `e2e::non_generation_routes_are_forwarded_and_not_captured` | Token counting, model listing and the probe pass through unchanged; no event, no log entry, no capture counted |
| `e2e::upstream_errors_are_relayed_and_captured_as_failed` | A 500 reaches the client as sent and is captured `Failed(Upstream { status: 500 })` with its request bodies stored; an unreachable upstream is a 502 from the gateway, captured `Failed(UpstreamUnreachable)` |
| `e2e::in_flight_stream_finishes_during_shutdown` | Shutdown mid-stream: new connections refused at once, the paced stream completes unchanged, its exchange is logged, nothing is cut |
| `e2e::stalled_stream_is_cut_at_the_drain_deadline_and_captured` | Shutdown during a stalled stream: cut at the drain deadline, the client sees an aborted body, the exchange is logged as `ClientDisconnected` with its bodies stored |
| `e2e::ops_endpoints_and_inspect_report_the_capture` | `/healthz` counters, `/readyz` (tasks `exchange_log`, `capture`, `proxy`), `/metrics` lines, 404s, `healthcheck::check` on 2xx and 404, `inspect::list` and `show`, the ops listener stopping last |
| `e2e::system_turn_exchange_is_published` | Claude Code's `role: "system"` turn inside `messages`: the reply reaches the client unchanged, the exchange is published with its request System, User, System in order, and `normalize_failed` stays 0 |
| `tests::refusals_are_counted_by_reason_and_protocol` | The capture stage counts an unknown-role body as `request_body` and an OpenAI Chat exchange as `unsupported_protocol`, and the health total is their sum |
| `logs::logs_are_json_lines_without_secrets_credentials_or_bodies` | At debug level over three cases and one refused request: every line JSON with a top-level `level`; the refusal's `request_shape` at debug level naming the role and content kind; never the deployment secret, the credential, any message text or the refused body's content |
| `tests::pipeline_ingest_matches_the_proxy_path` | Under paused time, for every corpus exchange plus the image request at seeded instants: a pipeline fed the raw exchange through the capture channel and one fed the pre-normalized exchange through `ingest` at the same instant put the same bytes in the same order, publish the same envelope (id, `at`, event), and count the same |
| `tests::pipeline_ingest_retries_blob_faults_then_fails_typed` | With every put failing before (and, separately, after) it commits: exactly `attempts` puts `backoff` apart, `IngestError::NotStored` with the attempt count, nothing published, `store_retries` = attempts - 1; a put that committed is stored once. Under 10% transient failures and latency every exchange is stored and published, with one retry per failed put |
| `tests::pipeline_concurrent_ingests_keep_ids_monotonic` | Four rounds of the corpus ingested concurrently over a slow store and a slow bus (whose acceptance order follows call order only if ingest serializes mint and publish), with `at` spread back and forth: every one published, ids distinct and reaching the bus in strictly increasing order, each envelope stamped its `at` and its id never in an earlier millisecond |
| `tests::dst_blobs_written_before_capture_published` | INV-48 (dst): under put latency and failures before and after the write, with seeded feed timing, every blob (bodies and media) an event names is stored when the event arrives; an exchange whose puts all failed publishes nothing; each exchange is published at most once |
| `live::tests::every_slot_runs_and_shutdown_drains` | Every slot is filled; shutdown drains every group |
| `live::tests::settle_moves_the_clock_forwards_and_reaches_a_fixed_point` | `settle` moves a manual clock to `until`, never back, and returns after a pass that changed nothing |
| `live::tests::a_slot_is_filled_once` | `SlotTaken` for a second fill |
| `live::tests::a_store_event_reaches_the_bus_and_the_live_feed` | A registry write's `Changed::Channel` reaches the bus through the outbox and the live feed through the surface relay |
| `live::tests::a_confirmed_transmission_is_classified_under_the_active_version` | `TransmissionConfirmed` gives `TransmissionClassified` under version 0, unassigned, an assignment in the catalog and a `Classified` stored state |
| `live::tests::an_access_and_its_resource_reach_the_evidence_records` | `AccessRecorded` fills the evidence records from `AccessStore::accesses` |
| `crosstalk_e2e` `serve::serve_all_exports_and_shows_the_confirmed_transmission_over_http` | `gateway::start_on` role `all` on ephemeral ports: the scenario ingested through the live pipeline and settled; over HTTP with crosstalk-client, the agents, the transmissions export (one row, A to B through a channel, trailer complete), the evidence page (a match carried by B's read), the token's operator `admin`, and a 401 without the token |
| `live::tests::the_defaults_start_on_any_clock` | `LiveConfig::new` on a read clock with a 10 s evidence window and 60 s TTL starts every slot and settles |
| `live::tests::bodies_can_live_on_the_filesystem` | `BlobConfig::Fs` stores bodies the surface reads |
| unit tests | Config (the example and the deployment's config parse; strictness at every level; checked values; path resolution), the CLI, roles, task flags, the log file (reopen, duplicates, torn tails, corruption), the health JSON (pinned, strict), readiness, metrics text (the `normalize_failed` series sum to the health total), healthcheck URL checks, refusal codes |

```sh
cargo test -p crosstalk-gateway
CROSSTALK_SIM_SEEDS=300 cargo test -p crosstalk-gateway tests::dst   # a wider seed sweep
```

## Invariants and constraints

- Only the composers (`gateway`, and `api`, `client`, `eval` beside it)
  depend on layer crates (`tests/architecture.rs`).
- There is one path after L1: the capture stage normalizes and calls
  `Ingester::ingest`, the same function `Pipeline::ingest` is.
- Envelope ids reach the bus's `publish` in strictly increasing order,
  however many ingests run at once (mint and publish under one lock); an
  id is never in a millisecond before its envelope's `at`.
- `ExchangeCaptured` is published only after every body and media blob of
  the exchange is stored (`canonical.capture.blobs-before-event`).
- Each exchange the proxy hands off is published at most once; the log
  holds each envelope once.
- Nothing the gateway writes leaves the parent of `blobs.root`.
- Secrets come only from the environment variables the config names; no
  secret, credential, header or body is logged (`tests/logs.rs`). A
  refused exchange logs only its body's top-level shape, at debug level
  (`crosstalk_canonical::anthropic::RequestShape`).
- Concurrency is tokio tasks joined by channels (the capture channel, the
  bus, `watch` stop and phase signals); the only shared state is atomic
  counters, per-task running flags, and the envelope id generator behind
  a `tokio::sync::Mutex`.
- Nothing in the pipeline reads the system time: the injected `Clock` and
  tokio time only, so it runs under paused time.
- Shutdown is bounded: `drain_timeout_ms` plus `flush_timeout_ms` plus a
  second for the ops listener.
- No `unwrap` or `expect` outside tests; errors are typed (`thiserror`).
- `Live`: every slot's group subscribes before anything is published; a
  stage's derived events are published only after the store writes they
  announce; the extraction step hands a delta's accesses to L5 before
  provenance's events for that delta are published; under
  `Ticking::OnSettle` the same input settles to the same transmissions.

### Invariant evidence

INV-48 (`canonical.capture.blobs-before-event`): both evidence paths moved
from `crosstalk_canonical::tests::` to the gateway, where the capture task
lives, and pass: `crosstalk_gateway::tests::dst_blobs_written_before_capture_published`
and `crosstalk_gateway::e2e::consumer_reads_every_referenced_blob`. The
integration evidence runs against the single-node bus and filesystem blob
store; the cluster stores (JetStream, Postgres or object storage) are P9.

## Gaps found

- **`Live`'s frontier is coarse.** With no frontier source over the
  memory stores, `oldest_pending` is taken as `None` whenever the layer
  groups are empty; exchanges in flight at the proxy are not counted
  (their times are later than `now - settle_after` for any request
  shorter than the settle bound).
- **The transmissions export has no content columns in memory**, and
  carries confirmed, classified and aggregated transmissions only: the
  spec's `ExportDataset::Transmissions` is defined over confirmed rows
  (`TransmissionRow::new` refuses any other state). Unconfirmed
  transmissions reach an export only through the `verdicts` dataset (by
  `opened_at`, INV-1062), and only once an operator has judged them: a
  suspected or discarded transmission with no verdict, and an awaiting
  one (which takes none), have no row anywhere in an export.
- **No exchange store in the spec.** Nothing in `spec/types/interfaces`
  persists `Exchange`s or lists them; L8 reads exchanges only through
  L3 to L7's stores. The exchange log here is a stopgap. A spec trait
  (append and read by id or time, idempotent on the exchange id), with a
  Postgres implementation, would replace it.
- **No cross-node bus yet.** `proxy` and `pipeline` as separate processes
  cannot talk (P9).
- **Readiness of migrations is vacuous** until a layer has migrations.
