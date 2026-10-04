# Demo and load generation

`crosstalk-demo` (`crates/demo`) shows crosstalk carrying 100–200 live
agents without spending real tokens. One binary has three roles. It runs a
fake Anthropic upstream, a shared wiki, and a swarm of simulated agents.
The agents talk to the model through the crosstalk proxy and to each other
through the wiki. The compose override `deploy/compose.demo.yaml` and
`bash deploy/run.sh demo ...` run it on the single-machine deployment.

## Scope

- A fake Anthropic Messages upstream (`upstream`):
  - `POST /v1/messages`, streaming (SSE) and not, in the real wire format;
  - text and `tool_use` blocks, generated deterministically from a seed
    and the request body;
  - a configurable wait before the first byte, and stream pacing;
  - `POST /v1/messages/count_tokens` (an estimate) and `GET /healthz`.
- A shared wiki (`wiki`): an in-memory HTTP page store with versions and
  authors.
- A swarm driver (`swarm`): N agents with growing conversations. The whole
  conversation is resent every turn. The agents execute wiki tool calls and
  send back their results. The driver reports throughput, client-observed
  latency and the expected cross-agent transmissions, and can write that
  ground truth as JSON lines.
- The container healthcheck (`healthcheck`), since the runtime image is
  distroless.
- The compose override, the image, the demo gateway config and the
  `run.sh demo` subcommands.

## Non-scope

- Real model behaviour. The fake model follows a task marker; it does not
  reason.
- Other protocols (OpenAI, Gemini), WebSocket and TLS.
- Detection itself. The swarm produces traffic whose ground truth is known.
  Detecting it is L4/L5's job (see [What crosstalk should see](#what-crosstalk-should-see)).
- Fault injection (errors, stalls, cut streams). testkit's `FakeUpstream`
  covers that for tests.
- Persistence of the wiki. It lives in memory and is lost on restart.

## Layout and reuse

```text
crates/demo/src/
  main.rs               the binary: parse, log, run the subcommand
  lib.rs                crate doc, modules
  cli.rs                Command, UsageError, USAGE
  knobs.rs              Span (A..B), PositiveSpan, Fraction, parse_duration, Rng
  protocol.rs           wiki tools, PageSlug, Topic/TOPICS, Task (the marker)
  http.rs               DemoBody, serve (accept loop), BaseUrl, request, healthcheck, shutdown_signal
  logging.rs            JSON logs on stderr
  anthropic/mod.rs      Role, Block, Content, Message, ResponseBlock, StopReason, Usage, AssistantMessage
  anthropic/sse.rs      encode (message -> Anthropic event sequence), Split, Frame
  anthropic/assemble.rs assemble / assemble_stream (event stream -> message), AssembleError
  upstream/mod.rs       the fake upstream server, paced feeder, frame_offset
  upstream/generate.rs  parse_request, generate (the fake model), GenConfig, Reply
  upstream/text.rs      deterministic prose from templates
  wiki/mod.rs           the wiki server and the task owning the pages
  wiki/store.rs         Wiki, Page, Author, Summary, WriteError (pure)
  swarm/mod.rs          run: resolve, spawn agents, stop, drain, report
  swarm/config.rs       SwarmConfig, TaskMix
  swarm/conversation.rs Conversation, Profile, ToolCall, ToolResult, PendingTools, Step, OrderError
  swarm/agent.rs        one agent's loop, tool execution, headers, api_key
  swarm/stats.rs        Event, RequestSample, Outcome, collect, Report, percentile
  tests/                sse, generate, conversation, units, servers, deploy
```

**Decision: a workspace crate, not a standalone one.** The architecture
test allows it. It now classifies `crosstalk-demo` as a *tool* crate: a new
`Tool` role in `crates/gateway/tests/architecture.rs`. Rule 3 says no layer
crate depends on a tool crate under any dependency kind. A tool crate may
depend on anything, so the demo takes `crosstalk-testkit` as a normal
dependency. Being in the workspace means the demo is built with the same
pins and lock, `scripts/check.sh` runs its tests, and it shares the build
cache with the gateway image.

What is reused:

- testkit's `HarnessClient`, `CorpusRequest`, `Headers` and `BodyEnd`. They
  carry every HTTP call the swarm makes, to the gateway and to the wiki.
  `ResponseStream::next` gives the chunk arrival times, which are used for
  the time to first byte.
- testkit's byte-exact SSE parser (`EventStream`). The swarm reassembles
  streams with it, and the tests check the encoder against it.
- The spec's `SeededRandom` (SplitMix64), behind `knobs::Rng`.

testkit's `FakeUpstream` is not reused. It replays fixed cases on
`127.0.0.1:0` and keeps every request in memory. The demo needs answers
that depend on the request, a bind on `0.0.0.0` and no growth under load.

No new third-party dependency was added. The crate uses only existing
workspace pins: blake3, bytes, http-body-util, hyper, hyper-util, serde,
serde_json, thiserror, tokio, tracing and tracing-subscriber.

## Data and control flow

```text
swarm agent i ──POST /anthropic/v1/messages (x-api-key per key group)──▶ crosstalk :8080
     ▲                                                                       │ forward unchanged, capture
     │ SSE / JSON answer                                                     ▼
     └───────────────────────────────────────────────────────────── fake-upstream :8070
     │ tool_use wiki_write {page, content}  ──PUT /pages/<p> (x-wiki-author)──▶ wiki :8090
     │ tool_use wiki_read  {page}           ──GET /pages/<p>──────────────────▶ wiki
     └ tool_result (the page text, verbatim) in the next request ──▶ crosstalk ──▶ fake-upstream
```

### The task marker

The swarm plays the user and the fake model plays an obedient model. Each
prompt is a sentence of prose followed by one marker line:

- `[task:write page=<slug> topic=<n>]`
- `[task:read page=<slug>]`
- `[task:chat topic=<n>]`

`protocol::Task` renders and parses these lines.

The fake model reads the marker of the last `user` message. It skips
`system` turns inside `messages`.

- **Write** (when `wiki_write` is declared): a text block, then
  `tool_use wiki_write {page, content}`. `content` is 40–160 words of
  generated prose. Stop reason `tool_use`.
- **Read** (when `wiki_read` is declared): a text block, then
  `tool_use wiki_read {page}`. Stop reason `tool_use`.
- **A turn of tool results:** a closing prose answer. When every result is
  an error, the answer starts "That did not work". Stop reason `end_turn`.
- **Chat, no marker, or an undeclared tool:** prose. Stop reason
  `end_turn`.

So the swarm's knobs decide how often the wiki is written and read, while
the words still come from the model.

### Fake upstream

1. `handle` routes the request:
   - `GET /healthz`;
   - `POST /v1/messages` (the query is ignored, so `?beta=true` works);
   - `POST /v1/messages/count_tokens`;
   - anything else is a 404 `not_found_error`.
2. `read_body` refuses a request that has neither `x-api-key` nor
   `authorization` (401 `authentication_error`). A body over 32 MiB is a
   413 `request_too_large`.
3. `parse_request` reads `model`, `max_tokens`, `messages`, `stream`, the
   tool names and the last user turn. A body that is not a request is a
   400 `invalid_request_error`.
4. `generate` draws everything from `Rng::derive(seed, [body])`:
   - the message id `msg_01…` and the tool ids `toolu_01…`;
   - the first-byte wait, from `--first-byte-ms`;
   - the stream time, from `--stream-ms`;
   - the word count, from `--words`, capped at `max_tokens × 3/4`.

   `input_tokens` is the body's bytes / 4, and `output_tokens` is about
   4/3 of the words. The same seed and body always give the same bytes.
5. Streaming: `sse::encode` produces these frames, in this order:
   - `message_start`, then `ping`;
   - for each block: `content_block_start`, then its deltas, then
     `content_block_stop`. Text deltas are 3 words. Tool input deltas are
     24 characters of the input JSON, and the first is empty.
   - `message_delta`, then `message_stop`.

   The headers (`text/event-stream; charset=utf-8`, `cache-control`,
   `request-id`) go out at once. A feeder task sends frame 0 at the
   first-byte wait and spreads the rest evenly over the stream time
   (`frame_offset`). It sends over an `mpsc` channel of capacity 1, so a
   slow reader holds it back.
6. Non-streaming: the response waits for the first-byte wait plus the
   stream time, then sends the whole JSON document.

### Wiki

`GET /pages` lists the pages: name, version, bytes and last author.

`GET /pages/<name>` returns the text as `text/plain`, with the
`x-wiki-version` and `x-wiki-author` headers, or a 404.

`PUT /pages/<name>` takes a UTF-8 body and an optional `x-wiki-author`. It
answers 201 when it creates the page and 200 when it replaces it. Other
answers:

- 400 for a bad name, a bad author or a body that is not UTF-8;
- 413 for a page over `--max-page-bytes`;
- 507 for a new page past `--max-pages`;
- 405 for any other method.

One task owns the `Wiki`, and handlers reach it over an `mpsc` channel with
`oneshot` answers.

### Swarm

`swarm::run` works in this order:

1. Resolve the gateway and wiki names, retrying for up to 60 s.
2. Build one `HarnessClient` for each. They are read-only and behind an
   `Arc`.
3. Spawn the collector.
4. Spawn agent `i` after a delay of `ramp × i / N`.
5. After `duration`, or on SIGINT or SIGTERM, set the stop `watch`.
6. Give in-flight work up to `grace` to finish, then abort it.
7. When the event channel closes, the collector returns the `Report`.

Each agent works like this:

- **Identity.** It is named `agent-NNN`. Its `x-api-key` is
  `sk-ant-demoGGGG-<40 hex>`, stable per seed and shared by each group of
  `agents-per-key` agents. Its system prompt names its focus topic
  (`i mod topics`).
- **Conversations.** Each has a fresh UUID-shaped
  `x-claude-code-session-id` and a drawn `turns` count of prompts.
- **A prompt.** The agent draws the task:
  - write, with probability `write-fraction`: a uniformly drawn page `p`
    and its topic `p mod topics`;
  - read, with probability `read-fraction`: a page from `GET /pages` that
    another agent wrote last, or else a random page (it may 404);
  - chat on its focus topic otherwise.

  It sends the prompt and runs any tool calls against the wiki. It sends
  the results as one `user` turn of `tool_result` blocks, repeating for up
  to 4 rounds, until the model ends its turn. Then it thinks for a drawn
  `think-ms`.
- **The request.** Every request carries the whole conversation, plus
  `system`, `tools`, `max_tokens`, `metadata.user_id` and `stream`.
  `stream` is true with probability `stream-fraction`. The headers are:
  - `accept`, `anthropic-version: 2023-06-01`,
    `content-type: application/json`;
  - `user-agent: crosstalk-demo-swarm/<version>`;
  - `x-api-key` and `x-claude-code-session-id`;
  - with `--claude-code-shape`: `user-agent: claude-cli/2.1.282
    (external, cli)`, `anthropic-beta: claude-code-20250219` and
    `x-app: cli`.
- **Failures.** A failed request is retried up to 3 times, after 0.5 s and
  then 1 s. After that, the conversation is abandoned and a new one starts.
- **Tool results.**
  - A write answers `Saved \`p\` (version v, n bytes).`.
  - A read answers with the page text, verbatim.
  - A missing page answers `is_error: true`.
- **Events.** Each request reports a `RequestSample`: streaming or not,
  follow-up or not, the outcome, time to first byte, total time, and bytes
  in each direction. Each wiki call reports a `WikiWrite`, a `WikiRead`
  (with the writer and version read) or a `WikiError`.

`Conversation` enforces the order. `ask` works only when idle. `receive`
works only after a request, and returns `Step::Tools(PendingTools)` when the
answer calls tools. `resolve` takes exactly one `ToolResult` per pending
call, in order, and a `ToolResult` can only be made from its `ToolCall`.
With `--claude-code-shape`, a `role: "system"` turn (`SYSTEM_TURN`) comes
before each prompt.

The collector logs progress every 10 s and returns these figures:

- requests, successes, and failures by outcome (`http <status>`,
  `transport`, `malformed`);
- follow-ups and req/s;
- bytes sent and received;
- time to first byte and total time, p50/p95/p99/max, for streaming and
  non-streaming separately;
- conversations completed and abandoned;
- wiki writes, reads, missing pages, own-page reads and errors;
- the expected transmissions: reads of a page another agent wrote last,
  and the distinct writer→reader pairs.

With `--ground-truth PATH`, it writes each expected transmission as one
JSON line: `{"writer","reader","page","version","at_ms"}`. `--json` prints
the report as JSON instead of text.

## What crosstalk should see

Every swarm request is a normal Anthropic Messages generation exchange.
With the deployment defaults the gateway forwards and captures all of them.
A local run of 437 requests through `crosstalk serve --role all` captured,
published and logged all 437. Once L3 to L5 exist, the traffic carries:

- **Distinct credentials and agents.** One `x-api-key` per agent, or per
  group with `--agents-per-key`, so there is one credential digest per key.
  There is one `x-claude-code-session-id` per conversation, and the system
  prompt differs per agent.
- **Conversation threading.** Each request's messages extend the previous
  request's messages of the same session. Tool follow-ups continue a
  prompt.
- **Originated spans.** The `content` of each `wiki_write` tool call is
  40–160 words of model output, distinct across writes.
- **Cross-agent content matches.** When agent B reads a page agent A wrote,
  that text is the `content` of a `tool_result` in B's next request,
  verbatim (`ContentMatched`).
- **A discovered channel.** The tool calls `wiki_write` and `wiki_read`
  with the same `page` argument are writes and reads of one resource family:
  the wiki, a channel nobody declared (`AccessRecorded`, then
  `TransmissionConfirmed`).
- **Ground truth.** The report's expected transmissions and writer→reader
  pairs, or the `--ground-truth` file, are the edges a correct topology
  shows.

## Running it

On node0, from the repository root:

```sh
bash deploy/run.sh init                         # once, if deploy/.env does not exist
bash deploy/run.sh demo up                      # stack + fake-upstream + wiki, demo config
bash deploy/run.sh demo run --agents 200 --duration 10m
bash deploy/run.sh demo run --agents 150 --think-ms 1000..4000 --write-fraction 0.3 --read-fraction 0.4 --json
curl -s 127.0.0.1:8090/pages                    # what the agents wrote
bash deploy/run.sh demo logs fake-upstream wiki
bash deploy/run.sh demo down                    # then `bash deploy/run.sh up` for the real upstream
```

`demo run` first runs `up -d` with the override, so a stack started with
plain `up` is switched to the demo config. Then it runs the swarm once,
which prints the report, and prints the gateway's `/healthz` capture
counters. The swarm runs inside the compose network against
`http://crosstalk:8080/anthropic` and `http://wiki:8090`, so host port
overrides do not matter.

Without docker, each subcommand runs on its own. To check the swarm against
the fake upstream directly, with no gateway:

```sh
crosstalk-demo upstream --listen 127.0.0.1:8070 &
crosstalk-demo wiki --listen 127.0.0.1:8090 &
crosstalk-demo swarm --gateway http://127.0.0.1:8070 --wiki http://127.0.0.1:8090 --agents 20 --duration 30s
```

To run it through a gateway, add a gateway on 8080 whose route points at
8070, and use `--gateway http://127.0.0.1:8080/anthropic`.

### Knobs

| Subcommand | Option | Default | Meaning |
| --- | --- | --- | --- |
| swarm | `--agents N` | 150 | Concurrent agents |
| | `--agents-per-key N` | 1 | Agents sharing one `x-api-key` |
| | `--think-ms A..B` | 2000..8000 | Pause after each answered prompt |
| | `--turns A..B` | 4..12 | Prompts per conversation |
| | `--write-fraction F` | 0.25 | Prompts that write a wiki page |
| | `--read-fraction F` | 0.35 | Prompts that read one; write + read ≤ 1, the rest chat |
| | `--pages N` | 40 | Distinct wiki pages |
| | `--topics N` | 8 | Distinct topics (page `p` is about topic `p mod N`) |
| | `--duration D` | 5m | Run time |
| | `--ramp D` | 20s | Agents start spread over this |
| | `--seed N` | 42 | Agents' choices and keys |
| | `--stream-fraction F` | 1 | Requests asking for a stream |
| | `--model NAME`, `--max-tokens N` | claude-opus-5-5, 4096 | Sent as is |
| | `--idle-timeout D`, `--grace D` | 120s, 30s | Give up on a silent response; let in-flight work finish |
| | `--claude-code-shape` | off | A `role: "system"` turn before each prompt, plus Claude Code headers |
| | `--ground-truth PATH`, `--json` | — | Expected transmissions as JSON lines; the report as JSON |
| | `--gateway URL`, `--wiki URL` | 127.0.0.1:8080/anthropic, :8090 | Fixed to the compose services by `compose.demo.yaml` |
| upstream | `--seed N` | 7 (`DEMO_UPSTREAM_SEED`) | Model output and timing |
| | `--words A..B` | 40..160 (`DEMO_WORDS`) | Prose length per answer or page |
| | `--first-byte-ms A..B` | 300..1500 (`DEMO_FIRST_BYTE_MS`) | Wait before the first body byte |
| | `--stream-ms A..B` | 1000..10000 (`DEMO_STREAM_MS`) | First event to last |
| | `--listen ADDR` | 0.0.0.0:8070 | |
| wiki | `--max-page-bytes N`, `--max-pages N` | 262144, 100000 | Limits |
| | `--listen ADDR` | 0.0.0.0:8090 | Published on `127.0.0.1:${DEMO_WIKI_PORT:-8090}` |

The upstream's knobs are read from `deploy/.env` (the `DEMO_*` variables)
when its container is created. `DEMO_LOG` sets `RUST_LOG` for all three
demo services.

## Files

| File | Role |
| --- | --- |
| `crates/demo/Cargo.toml` | `crosstalk-demo`: spec, testkit; blake3, bytes, http-body-util, hyper (client, http1, server), hyper-util (tokio), serde, serde_json, thiserror, tokio (fs, io-util, macros, net, rt, rt-multi-thread, signal, sync, time), tracing, tracing-subscriber (env-filter, fmt, json, std); bin `crosstalk-demo` |
| `crates/demo/src/**` | See [Layout and reuse](#layout-and-reuse) |
| `crates/gateway/tests/architecture.rs` | Adds the `Tool` role (`demo`), `Violation::LayerOnTool`, and tests for it |
| `deploy/demo.Dockerfile` (+ `.dockerignore`) | Builds `crosstalk-demo` like `crosstalk.Dockerfile` (same toolchain, cache mounts and distroless runtime) |
| `deploy/compose.demo.yaml` | Override: swaps the config mount of `migrate` and `crosstalk` to the demo config; adds `fake-upstream`, `wiki` and `swarm` (profile `swarm`, `restart: "no"`) |
| `deploy/demo/crosstalk.demo.json` | `deploy/config/crosstalk.json` with the Anthropic route's `base_url` set to `http://fake-upstream:8070` |
| `deploy/run.sh` | `demo up`, `demo run [swarm options]`, `demo down`, `demo logs` |

## Tests

| Module | What it shows |
| --- | --- |
| `tests::sse` | The event sequence and the fields of each event. Tool blocks start with empty input and an empty JSON delta. Deltas concatenate to the block. Frames parse with testkit's `EventStream`, concatenate to the exact bytes and reassemble to the message, under three splits. Broken streams are refused with typed errors. The non-streaming document round-trips |
| `tests::generate` | Identical bytes for the same seed and body; another seed or body changes them. Write and read markers produce the matching tool call. Undeclared tools are never called. Tool results get a closing answer. System turns are skipped. Malformed requests are refused. Timing and length stay in range, and `max_tokens` caps the length |
| `tests::conversation` | Each body's messages extend the previous body's. A tool call is answered by its result, refusing out-of-order and foreign results. The Claude Code shape puts a system turn before each prompt. Agent A's generated page reaches agent B's next request verbatim, through the real wiki store and the fake model |
| `tests::units` | Spans, fractions, durations, the random stream, markers, page names, topics, the wiki store, nearest-rank percentiles, base URLs, and the CLI (accepted and refused lines) |
| `tests::servers` | Over sockets: a paced stream (first byte, spread, same bytes twice); the JSON document, 401, 400, count_tokens, 404 and the healthcheck; the wiki (versions, authors, listing, limits); and a short swarm run whose report and ground-truth file show writes, reads and cross-agent transmissions with no failures |
| `tests::deploy` | The demo config equals the deployment config except for the upstream URL |

## Invariants and constraints

- **Determinism.** The fake upstream's answer, ids and timing are a
  function of its seed and the request body bytes. It holds no state.
- **The wiki is the only shared state between agents.** Agents share
  read-only configuration and clients behind an `Arc`. Figures go to the
  collector over a channel, and wiki pages live in the one task that owns
  them.
- **Conversation order.** A conversation only grows by valid steps. Every
  tool call is answered exactly once, in order, before the next request.
- **A transmission in the report** is a read of a page whose last writer
  is another agent, and that page's text is in the reader's next request.
- **Never on by accident.** `up` never starts the swarm (profile `swarm`,
  `restart: "no"`). The demo config differs from the deployment's only in
  the upstream URL (`tests::deploy`), and `deploy/config/crosstalk.json` is
  unchanged.
- **No real secrets.** API keys are fake (`sk-ant-demo…`) and the upstream
  never calls out.
- Tokio only, typed errors (`thiserror`), structured `tracing` fields, no
  `unwrap` or `expect` outside tests, files under 1000 lines. No layer
  crate depends on `crosstalk-demo` (architecture test, rule 3).
- The `RUST_TOOLCHAIN` argument of `deploy/demo.Dockerfile` changes
  together with `rust-toolchain.toml`. Its `.dockerignore` mirrors
  `crosstalk.Dockerfile.dockerignore`, so if `ui/` becomes a workspace
  member, both must stop excluding `ui`.

## Gaps found

- **`role: "system"` inside `messages` is refused by L1.** The canonical
  normalizer rejects it (`message 0 has role "system", not user or
  assistant`), so every exchange with that shape is forwarded but counted
  `pipeline.normalize_failed` and not captured. `--claude-code-shape`
  reproduces this. It stays off by default until the gateway fix lands.
- **Nothing detects the traffic yet.** L3 to L8 are not built. Today the
  demo shows capture: the `/healthz` counters,
  `crosstalk inspect --config /etc/crosstalk/crosstalk.json` inside the
  container, and the bodies in the blob store.
- **Only the Anthropic route exists**, so the swarm speaks only Anthropic
  Messages.
