# End-to-end smoke (`crosstalk-e2e`)

The smoke proves that real harness traffic, fed through the gateway's
pipeline, becomes an edge and a confirmed transmission that the UI's read
path can see. It has three parts:

- **a scenario:** two Claude Code sessions talking through a wiki page,
  written as the HTTP traffic Claude Code and Anthropic exchange;
- **a capture step:** it turns that traffic into the `NormalizedExchange`s
  that `Pipeline::ingest` takes, using L0's and L1's own code;
- **readers and tests:** they read the outcome back through `QueryApi`,
  the API the UI uses.

The crate is `crates/e2e`. It is a composer in the dependency rule
(`crates/gateway/tests/architecture.rs`), not test support, because it
wires the gateway's pipeline to the surface. A layer depending on it would
be a layer depending on the gateway, which the rule refuses.

## Scope

- **The wiki relay scenario.** `Scenario::wiki_relay(start)` is fully
  deterministic: its times are fixed offsets from `start`, and its ids and
  bodies are fixed.
- **Capture through the production L0 and L1 code.** That means
  `Routes::resolve`, `HeaderIdentifier::context`,
  `AdapterDecoder::decode_now` and `AnthropicMessages::normalize`.
- **A composition shaped like `crosstalk_gateway::live::Live`** (pipeline,
  stores, surface). It is wired from what is merged today.
- **Feeding:** the scenario is ingested in time order, with the clock
  moved for each exchange.
- **Surface readers:** agents by session, edges, the A→B channel edge, the
  transmissions behind an edge, transmission rows, the evidence page, and
  channel rows.
- **Tests:**
  - those that run today: the scenario, L0/L1, the pipeline, and the
    surface answering;
  - those that wait for L3 to L7 in `Live`, marked
    `#[ignore = "waits for …"]`.

## Non-scope

- **Detection.** The crate implements no stage. It only drives the stages
  and reads what they produce.
- **The network path.** The proxy on a socket and a fake upstream are
  `crates/gateway/tests/e2e`. Here capture runs the same L0 functions
  without a socket.
- **Volume and realism.** That is the eval's job (`crosstalk-eval`,
  including the collusion-wiki converter on `feat/eval-wiki-swarm`). The
  smoke is the smallest scenario that ought to produce exactly one
  transmission.

## The scenario

| # | Label | Agent | At (from start) | What it carries |
| - | ----- | ----- | --------------- | --------------- |
| 1 | `a1-write` | A | 0 s to 4.2 s | User asks A to publish the runbook. A answers with `Write {file_path, content}`, and the content carries `SENTENCE` on a line of its own. |
| 2 | `a2-ack` | A | 5 s to 6.3 s | The full history plus the `Write` result. A confirms in text. |
| 3 | `b1-read` | B | 120 s to 122.1 s | User asks B what has to happen before a rollback. B answers with `Read {file_path}` on the same page. |
| 4 | `b2-repeat` | B | 123 s to 126.9 s | The full history plus the `Read` result: the page, `cat -n` numbered, with `SENTENCE` in it. B's answer repeats `SENTENCE`. |

- **Identity.** A and B differ in `x-api-key` (scheme `ApiKey`, a stable
  credential) and in `x-claude-code-session-id`. Both send
  `user-agent: claude-cli/2.1.282 (external, cli)`. They share no account
  header and have no agent or parent ids.
- **Request shape.** Every request carries the Claude Code system prompt,
  the tool list (`Read`, `Write`, `Bash`), `metadata.user_id` naming the
  session, `stream: true`, and the full history. Responses are event
  streams with one content block per assistant block.
- **The page** is `/srv/team-wiki/runbooks/ledger-rollback.md`, an absolute
  path on a shared mount.
- **Why `Write`/`Read` on a file, and not a URL or an MCP tool.**
  - A file write followed by a file read is the least ambiguous pair:
    L5's extractor catalog maps Claude Code's `Write` and `Read` on
    `file_path` to a write and a read on the path's file locator.
  - An `Mcp` locator includes the tool name, so an MCP `write_page` and
    `read_page` on one page would be two resources and could never
    co-access.

### What each stage needs, and where the scenario provides it

- **L1.**
  - Anthropic Messages, dialect `Reference` (vendor API upstream).
  - Tool-call arguments are stored as canonical JSON, so the sentence must
    survive canonicalization. It is plain ASCII with no quotes, backslashes
    or newlines.
- **L3.**
  - Each session id is scoped by its stable credential, which gives
    `HarnessSession{Credential(h), sid}`, and that resolves to one main
    agent per session.
  - Each agent's second request replays its first request, then the
    response as returned, then one tool result. So the delta's
    `new_inputs` is just that tool result.
- **L4.**
  - The sentence first appears in A's output: the `Write` arguments of
    `a1-write`. It is therefore originated by A and indexed. A's
    exchanges end before B's begin.
  - The sentence arrives in B's `b2-repeat` request as the `Read` tool
    result, which gives `ContentMatched` with carrier
    `ToolResult(toolu_01E2EReadLedgerRunbook002)`.
  - B's repeat is classified as relayed, not originated. It adds no match.
- **L5.**
  - The write access comes from `a1-write`. The read access comes from the
    `Read` call once its result is back (`b2-repeat`), on the same
    `Locator::File`.
  - The merged extractor (`crates/flow/src/extract/catalog.rs`) maps
    Claude Code's `Write` and `Read` on `file_path` to write and read
    accesses. A local harness's files carry no host, and the page lies
    outside A's repository working directory (INV-1049 does not apply),
    so both calls give one `Locator::File` and one resource. Tested today
    against the extractor itself (`tests/smoke/extract.rs`): A's `Write`
    is a `Write(Delivered)` access and B's `Read` a read, on the same
    locator.
  - A's write alone creates no channel: the resource is on no channel
    and the access records none (INV-850, INV-853).
  - The channel is created by the first cross-agent transmission on the
    page: the channel transmission B's read opens with A's write. It is
    seeded by that resource, that transmission and its opening time, and
    is `Active` since that opening with it as `last_transmission`
    (INV-851, INV-1031). `ChannelRow::created_at` is that opening
    (INV-1035).
  - It is listed as a channel, `Listing::Channel(Unconfirmed)` while the
    transmission awaits content and `Listing::Channel(Confirmed)` once
    the content match confirms it (INV-857).
  - The scenario's fixed, past timestamps are safe to replay: the
    correlator settles only on event times and the injected clock's
    ticks (INV-1046).
  - The read is about 2 minutes after the write, inside any correlation
    window. The match is in the result of the call that produced the read
    access, so the route is `Channel` (INV-269).
- **L6 and L7.**
  - L7 counts `TransmissionClassified`, so L6 must classify the
    transmission first (under version 0 it is an outlier).
  - `Confirmed::at` is `b2-repeat`'s start (INV-576), inside the
    bucket-aligned window `read::window` builds.

## Data and control flow

```text
Scenario::wiki_relay(start)
  └ WireExchange { label, agent, id, started_at, first_chunk_at, ended_at,
                   request: HttpRequest, response: HttpResponse (SSE) }
feed(scenario, pipeline, advance)
  for each exchange, in time order:
    Capture::normalized
      Routes::resolve("/anthropic/v1/messages") → upstream path, ReverseProxy{anthropic}, Upstream
      AnthropicAdapter::classify → Generation
      HeaderIdentifier::context (fixed deployment secret) → ClientContext
      AdapterDecoder::decode_now (head without credential headers) → DecodedRequest
      RawExchange → AnthropicMessages::normalize (L1) → NormalizedExchange
    advance(ended_at)                      (ManualClock::set in the tests)
    Pipeline::ingest(normalized, ended_at) → blobs stored, ExchangeCaptured on the bus
compose(start) → Composition { pipeline, stores, surface, clock, caller }
  today: InProcess::start (memory stores + Surface) and Pipeline::build over
         InProcess's MemoryBlobStore and MpscBus clones; nothing consumes the bus
  later: Live::start(config), the one change, in compose
read::* (generic over QueryApi) → agents, edges, edge transmissions, rows, evidence, channels
```

Detection runs on consumer tasks. The surface tests therefore poll each
read (`eventually`, up to 10 s) until it shows what they expect.

## Files

| File | Role | Key exports |
| ---- | ---- | ----------- |
| `crates/e2e/Cargo.toml` | The crate. It depends on spec, api, canonical, gateway, ingress, memory, surface and transport. | |
| `crates/e2e/src/lib.rs` | The crate root. | re-exports `Capture`, `compose`, `Composition`, `feed`, `Fed`, `Scenario`, `WireExchange` |
| `crates/e2e/src/scenario/mod.rs` | The wiki relay. | `Scenario`, `Scenario::wiki_relay`, `Scenario::ends_at`, `ScenarioAgent`, `WireExchange`, `SENTENCE`, `DEFAULT_START` |
| `crates/e2e/src/scenario/tools.rs` | The page and the `Write`/`Read` calls. | `WIKI_PAGE`, `write_call`, `read_call` |
| `crates/e2e/src/scenario/wire.rs` | Claude Code request heads and bodies, and SSE responses. | `HttpRequest`, `HttpResponse`, `Block`, `Turn`, `SessionHeaders`, `request`, `response` |
| `crates/e2e/src/capture.rs` | L0 and L1 without a socket. | `Capture::{new, raw, normalized}`, `CaptureError`, `ROUTE` |
| `crates/e2e/src/compose.rs` | The composition. | `compose`, `Composition`, `ComposeError`, `E2ePipeline` |
| `crates/e2e/src/feed.rs` | Ingests in time order. | `feed`, `Fed`, `FeedError` |
| `crates/e2e/src/options.rs` | The composition's config: 5-minute buckets, the world seed's correlator timing, and trusted access. | `in_process`, `timing`, `BUCKET`, `OptionsError` |
| `crates/e2e/src/read.rs` | Surface readers. | `window`, `agents`, `Agents`, `AgentRead`, `edges`, `channel_edge`, `edge_transmissions`, `summaries`, `evidence`, `channels`, `ReadError` |
| `crates/e2e/tests/smoke/scenario.rs` | Determinism, time order, and the L0 identity. Also checks the history replay L3 threads by, the `Write` arguments carrying the sentence, and the read result and B's answer carrying it. | |
| `crates/e2e/tests/smoke/extract.rs` | The scenario's `Write` and `Read` calls, with their results, through L5's `ToolExtractors` (crosstalk-flow, a dev-dependency) under the context the system prompt states: one delivered write and one read on the page's file locator. | |
| `crates/e2e/tests/smoke/pipeline.rs` | Every body is stored, and every exchange is published in order, stamped at its end. | |
| `crates/e2e/tests/smoke/surface.rs` | The surface answers today. Plus ignored tests: two agents (L3), the A→B channel edge, the confirmed transmission, the evidence match, and the channel created by the cross-agent transmission, listed and confirmed. | |
| `crates/gateway/tests/architecture.rs` | `Composer::E2e`, and `e2e_composes_gateway_and_layers_and_no_layer_uses_it`. | |

## Switching to `Live`

`compose` is the only place that builds anything.

1. When `crosstalk_gateway::live::Live::start` lands, build `pipeline`,
   `stores` and `surface` from it in `compose`, and keep the
   `Composition` fields. The tests and readers do not change.
2. Then run the smoke with `--include-ignored`. Once it passes, drop the
   `#[ignore]` attributes.

If `Live`'s stores or surface types differ from `MemoryStores` and
`Surface<MemoryStores>`, only the field types of `Composition` change.

To feed a running `Live` from `crosstalk-ui` for a demo:

```rust
let scenario = Scenario::wiki_relay(now);
feed(&scenario, &live.pipeline, |_| {}).await?;
```

Then read through the UI, or through `crosstalk_e2e::read`.

## Invariants and constraints

1. **Determinism.** The same `start` gives byte-identical traffic, and a
   later start shifts every time and changes nothing else. Tested.
2. **The same code as the proxy.** Capture goes through the production
   route table, identifier, adapter and normalizer. The only differences
   are a fixed deployment secret and no socket.
3. **One credential digest per agent, and two different digests.**
   Credentials are `ApiKey`, a stable scope. Tested.
4. **Exact prefixes.** Each follow-up request's message hashes are the
   previous request plus its response, plus exactly one tool result.
   Tested.
5. **The sentence is A's output first.** It appears in A's `Write`
   arguments before any input of any agent contains it. It is plain
   ASCII, has no characters JSON escapes, and is at least 100 bytes.
   Tested.
6. **Ingest at the hand-off time.** Each exchange is ingested at its
   `ended_at`, after the clock is moved there. Envelopes reach the bus in
   time order. Tested.
7. **The surface is the only read path.** The readers use `QueryApi`
   alone, never a store.
8. **No other crate changes.** Gaps found in other crates are reported,
   not patched (below).

## Gaps found (for the gateway's composition)

- **The evidence page has nothing to read.**
  - In-process, `transmission_evidence` reads spans, accesses and
    resources from `MemoryEvidence`, which only a seeder fills.
  - `Live` must hand the surface L4's spans and L5's accesses and
    resources. Otherwise the evidence test fails even with detection
    working.
- **Store events don't reach the bus.**
  - `InProcess`'s stores publish to an in-process `Outbox` that is relayed
    to the node facts and the live feed, not to the `MpscBus`.
  - Events a store publishes for other consumers (L6 and L7 consume L5's
    `TransmissionConfirmed`, then `TransmissionClassified`) need a bus in
    `Live`.
- **Edges need L6.** L7 consumes `TransmissionClassified`, so `Live`
  needs the L6 classifier consumer as well as the L7 edge consumer.
- **Tool extraction** (resolved by the L5 port): the extractor catalog
  maps tool names and argument keys to locators and access kinds, a
  local harness's file locators carry no host, and HTTP tools decide
  write or read by method (INV-1047, INV-1048).
