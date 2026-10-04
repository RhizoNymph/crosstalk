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
- Graceful shutdown on SIGINT and SIGTERM.
- JSON logs on stdout.
- `migrate`: connects to Postgres and ensures the extensions
  (`crosstalk-store`); no layer has migrations yet.
- End-to-end tests over real sockets, a simulation test of the capture
  stage, and a manual check with a real Claude Code session
  (`scripts/try-claude-code.sh`).

## Non-scope

- The L8 HTTP binding on `api.listen` (P7.1): the section is checked and
  nothing is bound. The `api` role starts nothing and says so in the logs.
- L6 (`analysis` role, `embeddings` section): checked and unused (P6).
- L3 to L7 consumers (P4 to P6). The `pipeline` role runs only the exchange
  log today.
- A cross-node bus (P9). The bus is in-process, so a `proxy` process and a
  `pipeline` process do not reach each other; `--role all` is the only role
  that captures and logs end to end (the gateway warns at start otherwise).
- Postgres persistence of exchanges: the spec has no exchange store (see
  [Gaps](#gaps-found)). `serve` does not need the database to forward or
  capture.
- Application metrics beyond the capture counters, and tracing spans.
- TLS on the listeners. The proxy speaks plain HTTP to harnesses.

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
| `ingress` | yes | `crosstalk_ingress::config::IngressConfig`, unchanged: `listen`, `routes` (name, prefix, upstream id, kind, base URL), `secrets` (`current` and optional `previous` `{version, env}`), `limits`, `capture.channel_capacity` |
| `api` | no | `{"listen": SocketAddr, "token": {"env": ..}}`; checked, not bound |
| `ops` | yes | `{"listen": SocketAddr}` |
| `store` | no | `{"pool": crosstalk_store::PoolSettings}`; the URL is `DATABASE_URL` |
| `blobs` | yes | `{"root": path}` for `FsBlobStore::open`; its parent is the data directory |
| `embeddings` | no | `{"base_url": http(s) URL, "model": non-empty, "api_key": {"env": ..}}`; checked, unused |
| `bus` | no | transport's `BusConfig` (defaults) |
| `pipeline` | no | `{"blob_put_attempts": 3, "blob_put_backoff_ms": 100}` |
| `shutdown` | no | `{"drain_timeout_ms": 45000, "flush_timeout_ms": 10000}`; together under compose's 60 s grace period |

Checked values: environment variable names are non-empty without `=` or
NUL (`EnvVarName`), URLs are `http(s)` with a host (`HttpUrl`), the model
is non-empty (`NonEmpty`), counts and durations are non-zero, pool
settings go through `PoolSettings`' own checks, and `blobs.root` must have
a parent directory.

`crates/gateway/config.example.json` is this shape for localhost (ports
8080, 8081, 9464; blobs under the repository's `target/crosstalk-dev`).
`crates/gateway/.env.example` lists the environment.

### Environment

| Variable | Read by |
| --- | --- |
| `CROSSTALK_SECRET_V1` (whatever `ingress.secrets.current.env` names) | `serve` (roles running the proxy): 64 hex digits keying credential and account digests |
| `DATABASE_URL` | `migrate`; `serve` when `store` is configured (missing or malformed is a start error; unreachable only makes `/readyz` fail) |
| `CROSSTALK_API_TOKEN`, `CROSSTALK_EMBEDDINGS_API_KEY` | nothing yet (their sections are checked, not used) |
| `RUST_LOG` | the log filter (default `info`; `inspect` defaults to `warn`) |

## Roles

| Role | Tasks |
| --- | --- |
| `all` | `proxy`, `capture`, `exchange_log`, ops |
| `proxy` | `proxy`, `capture`, ops (published events have no consumer in this process) |
| `pipeline` | `exchange_log`, ops (nothing publishes in this process) |
| `api`, `analysis` | ops only; the startup log names what is not built |

## Listeners

| Config key | Serves |
| --- | --- |
| `ingress.listen` | The reverse proxy. `ANTHROPIC_BASE_URL=http://<host>:<port>/anthropic` |
| `ops.listen` | `GET /healthz`: 200 while the process serves, with the counters as JSON. `GET /readyz`: 200 or 503 with the checks (database reachable when `store` is configured, migrations at head, every role task running, not draining). `GET /metrics`: Prometheus text. Anything else is 404 |
| `api.listen` | Not bound until P7.1 |

`/healthz` body (pinned by `ops::tests::health_report_json_is_pinned`):

```json
{"status": "ok",
 "capture": {"captured": 3, "unclassified": 0, "decode_error": 0,
             "channel_full": 0, "channel_closed": 0, "response_too_large": 0},
 "pipeline": {"published": 3, "normalize_failed": 0, "store_failed": 0,
              "store_retries": 0, "publish_failed": 0},
 "log": {"written": 3, "duplicates": 0, "write_failed": 0}}
```

`/readyz` body: `{"ready": true, "role": "all", "status": "ok", "database":
"not_configured" | "reachable" | "unreachable: <why>", "migrations":
"at_head", "tasks": [{"name": "exchange_log", "running": true}, ..]}`.

`/metrics` series: `crosstalk_draining`,
`crosstalk_capture_exchanges_total`,
`crosstalk_capture_uncaptured_total{reason}`,
`crosstalk_pipeline_exchanges_total{outcome}`,
`crosstalk_pipeline_blob_put_retries_total`,
`crosstalk_exchange_log_deliveries_total{outcome}`.

## Data and control flow

```text
harness ──HTTP──▶ server::serve (proxy listener, hyper http1, no Date)
                    │ Proxy::handle (crosstalk-ingress): route, classify, forward, relay
                    │ generation exchange ends ──▶ RawExchange ── bounded mpsc (capacity from config) ──┐
                    ▼                                                                                    │
                 client response, unchanged                                                              ▼
                                                                         capture::CaptureStage::run (one task)
                                                                           AnthropicMessages::normalize_with_media (L1)
                                                                             └ refused ─▶ normalize_failed
                                                                           crosstalk_canonical::store ─▶ FsBlobStore (blobs.root)
                                                                             └ retried blob_put_attempts times ─▶ store_failed, nothing published
                                                                           Envelope { EventId, clock time, ExchangeCaptured(Exchange) }
                                                                           MpscBus::publish ─▶ every subscribed group
                                                                                                │
                                         log::consumer::run (group "exchange-log") ◀────────────┘
                                           ExchangeLog::append: one JSON line, synced; ack after; nack on failure
                                           <data dir>/exchanges/exchange-log.jsonl
```

- **Start** (`gateway::start`): resolve the data directory and, with a
  `store` section, `DATABASE_URL`; open the blob store and (pipeline) the
  log; build the proxy from the ingress config, reading its secrets
  through the environment lookup; bind the proxy and ops listeners; start
  the bus and subscribe the exchange log before anything can publish;
  spawn the tasks (each tracked by a running flag for `/readyz`); with
  `store`, connect to Postgres in the background, retrying every 5 s.
- **Envelope ids** come from ingress's ULID generator (`ExchangeIds`, the
  stand-in for P0.7's) behind `capture::EventIds`; nothing is duplicated
  here. The envelope time is the injected clock's reading after the store.
- **Shutdown** (`Running::shutdown`), in dependency order:
  1. `/healthz` reports `draining` and `/readyz` 503.
  2. The proxy listener closes (new connections are refused) and every
     open connection gets hyper's graceful shutdown: an idle keep-alive
     connection closes at once, one with a response in flight closes after
     it ends. Up to `drain_timeout_ms`; connections still open are then
     aborted, which the proxy records as `ClientDisconnected` and still
     hands to capture.
  3. With the proxy and its connections gone, the capture channel closes
     once the last per-exchange capture task has handed off; the capture
     stage drains it.
  4. The exchange log's group drains (bus depth zero), the bus stops, and
     the consumer closes the log (flush and `fsync`).
  5. The Postgres pool closes and the ops listener stops last.

  Steps 3 and 4 share one `flush_timeout_ms` deadline; a task still
  running at it is aborted and the report says so.

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
| `crates/gateway/Cargo.toml` | Manifest: spec, canonical, ingress, store, transport; bytes, http-body-util, hyper (client, http1, server), hyper-util, serde, serde_json, thiserror, tokio, tracing, tracing-subscriber (env-filter, fmt, json, std); dev: sim, testkit, tempfile | — |
| `config.example.json`, `.env.example` | The localhost config and the environment | — |
| `src/main.rs` | The binary: parse, log, run, wait for SIGINT/SIGTERM, shut down | `main` |
| `src/lib.rs` | Crate doc and modules | — |
| `src/cli.rs` | The command line | `Command` (`parse`), `UsageError`, `USAGE` |
| `src/config/mod.rs`, `sections.rs` | The config and its checked values | `GatewayConfig` (`from_json`, `load`, `data_dir`, `exchange_log_path`), `ApiConfig`, `OpsConfig`, `StoreSection`, `BlobsConfig`, `EmbeddingsConfig`, `PipelineConfig`, `ShutdownConfig`, `EnvRef`, `EnvVarName`, `HttpUrl`, `NonEmpty`, `ConfigError`, `exchange_log_path` |
| `src/role.rs` | Roles and their tasks | `Role` (`runs_proxy`, `runs_pipeline`, `not_built`), `UnknownRole` |
| `src/gateway.rs` | Wiring, start and shutdown | `start`, `Running` (`proxy_addr`, `ops_addr`, `bus`, `blobs`, `health`, `readiness`, `shutdown`), `StartError`, `ShutdownReport` |
| `src/capture.rs` | The capture stage | `CaptureStage` (`new`, `run`, `capture`), `Captured`, `PutRetry`, `EventIds`, `PipelineStats`, `PipelineCounts` |
| `src/log/mod.rs` | The exchange log file | `ExchangeLog` (`open`, `append`, `close`), `Appended`, `read`, `LogContents`, `LogError` |
| `src/log/consumer.rs` | The exchange log's bus consumer | `run`, `GROUP`, `group`, `LogStats`, `LogCounts` |
| `src/server.rs` | Accept loop with graceful, bounded drain (proxy and ops) | `serve`, `ServeOptions`, `DrainReport` |
| `src/ops/mod.rs`, `metrics.rs` | `/healthz`, `/readyz`, `/metrics` | `Ops` (`health`, `readiness`, `handle`), `HealthReport`, `Readiness`, `TaskState`, `CaptureReport`, `Phase`, `metrics::render` |
| `src/tasks.rs` | Per-task running flags | `Tasks` (`spawn`, `states`) |
| `src/store.rs` | `migrate` and the background connection `/readyz` checks | `migrate`, `MigrateError`, `store_config`, `StoreProbe`, `StoreCheck` |
| `src/healthcheck.rs` | The healthcheck client | `check`, `CheckError`, `TIMEOUT` |
| `src/inspect.rs` | Reading back the log and bodies | `list`, `show`, `InspectError` |
| `src/logging.rs` | JSON log setup | `init`, `try_init`, `Sink` |
| `src/tests/` | `crosstalk_gateway::tests::*`: the capture stage simulation (`dst.rs`) over raw exchanges built from the corpus (`raw.rs`) | — |
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
| `logs::logs_are_json_lines_without_secrets_credentials_or_bodies` | At debug level over three cases: every line JSON with a top-level `level`; never the deployment secret, the credential, or any message text |
| `tests::dst_blobs_written_before_capture_published` | INV-48 (dst): under put latency and failures before and after the write, with seeded feed timing, every blob (bodies and media) an event names is stored when the event arrives; an exchange whose puts all failed publishes nothing; each exchange is published at most once |
| unit tests | Config (the example and the deployment's config parse; strictness at every level; checked values; path resolution), the CLI, roles, task flags, the log file (reopen, duplicates, torn tails, corruption), the health JSON (pinned, strict), readiness, metrics text, healthcheck URL checks |

```sh
cargo test -p crosstalk-gateway
CROSSTALK_SIM_SEEDS=300 cargo test -p crosstalk-gateway tests::dst   # a wider seed sweep
```

## Invariants and constraints

- The gateway is the only crate that depends on layer crates
  (`tests/architecture.rs`).
- `ExchangeCaptured` is published only after every body and media blob of
  the exchange is stored (`canonical.capture.blobs-before-event`).
- Each exchange the proxy hands off is published at most once; the log
  holds each envelope once.
- Nothing the gateway writes leaves the parent of `blobs.root`.
- Secrets come only from the environment variables the config names; no
  secret, credential, header or body is logged (`tests/logs.rs`).
- Concurrency is tokio tasks joined by channels (the capture channel, the
  bus, `watch` stop and phase signals); the only shared state is atomic
  counters and per-task running flags.
- Shutdown is bounded: `drain_timeout_ms` plus `flush_timeout_ms` plus a
  second for the ops listener.
- No `unwrap` or `expect` outside tests; errors are typed (`thiserror`).

### Invariant evidence

INV-48 (`canonical.capture.blobs-before-event`): both evidence paths moved
from `crosstalk_canonical::tests::` to the gateway, where the capture task
lives, and pass: `crosstalk_gateway::tests::dst_blobs_written_before_capture_published`
and `crosstalk_gateway::e2e::consumer_reads_every_referenced_blob`. The
integration evidence runs against the single-node bus and filesystem blob
store; the cluster stores (JetStream, Postgres or object storage) are P9.

## Gaps found

- **No exchange store in the spec.** Nothing in `spec/types/interfaces`
  persists `Exchange`s or lists them; L8 reads exchanges only through
  L3 to L7's stores. The exchange log here is a stopgap. A spec trait
  (append and read by id or time, idempotent on the exchange id), with a
  Postgres implementation, would replace it.
- **No cross-node bus yet.** `proxy` and `pipeline` as separate processes
  cannot talk (P9).
- **Readiness of migrations is vacuous** until a layer has migrations.
