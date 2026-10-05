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
  - two prose generators, high-entropy (default) and templated, picked per
    request by a style marker in the system prompt
    ([Scenarios](#scenarios-and-prose-generators));
  - a configurable wait before the first byte, and stream pacing;
  - `POST /v1/messages/count_tokens` (an estimate) and `GET /healthz`.
- A shared wiki (`wiki`): an in-memory HTTP page store with versions and
  authors.
- A swarm driver (`swarm`): N agents with growing conversations. The whole
  conversation is resent every turn. The one declared tool is
  `http_request`, the HTTP tool contract L5 recognises; the agents run the
  model's GET and PUT calls against the wiki and send back their results.
  The driver reports throughput, client-observed latency and the expected
  cross-agent transmissions, and can write that ground truth as JSON lines
  ([schema v2](#ground-truth-v2), which ct-eval scores against).
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
  protocol.rs           HTTP_TOOL, tool_definitions, page_url, read_input/write_input, WikiCall, CallRefused, PageSlug, Topic/TOPICS, Scenario (the style marker), Task (the marker)
  http.rs               DemoBody, serve (accept loop), BaseUrl, request, healthcheck, shutdown_signal
  logging.rs            JSON logs on stderr
  anthropic/mod.rs      Role, Block, Content, Message, ResponseBlock, StopReason, Usage, AssistantMessage
  anthropic/sse.rs      encode (message -> Anthropic event sequence), Split, Frame
  anthropic/assemble.rs assemble / assemble_stream (event stream -> message), AssembleError
  upstream/mod.rs       the fake upstream server, paced feeder, frame_offset
  upstream/generate.rs  parse_request, generate (the fake model), GenConfig, Reply
  upstream/text.rs      deterministic prose: prose (by scenario), high_entropy_paragraph, templated_paragraph (frozen), TEMPLATES
  wiki/mod.rs           the wiki server and the task owning the pages
  wiki/store.rs         Wiki, Page, Author, Summary, WriteError (pure)
  swarm/mod.rs          run: resolve, mint the run id, spawn agents, stop, drain, report; RunClock, Stamp, run_id, ulid_text
  swarm/config.rs       SwarmConfig, TaskMix
  swarm/conversation.rs Conversation (claim_turn), Profile, ToolCall, ToolResult, PendingTools, Step, OrderError, locate_result
  swarm/agent.rs        one agent's loop, turns, delivering reads with the request that carries them, headers, api_key, agent_name
  swarm/tools.rs        execute: http_request against the wiki, PendingRead
  swarm/truth.rs        ground truth v2: WriteRecord, ReadRecord, Reader, Content, Row, RunInfo, TruthBook
  swarm/stats.rs        Event, RequestSample, Outcome, collect, Report, percentile
  tests/                sse, generate, text, conversation, units, servers, deploy, truth, truth_run
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

The crate uses workspace pins: blake3, bytes, http-body-util, hyper,
hyper-util, serde, serde_json, sha2, thiserror, tokio, tracing and
tracing-subscriber. `sha2 = "=0.11.0"` (default features off) was added to
the root `[workspace.dependencies]` for the ground truth's `sha256`; that
version was already in `Cargo.lock` through sqlx, so the lock gains no new
package. The run id is a ULID from the spec's `UlidGenerator`, so no ULID
crate is needed.

## Data and control flow

```text
swarm agent i ──POST /anthropic/v1/messages (x-api-key per key group)──▶ crosstalk :8080
     ▲                                                                       │ forward unchanged, capture
     │ SSE / JSON answer                                                     ▼
     └───────────────────────────────────────────────────────────── fake-upstream :8070
     │ tool_use http_request {PUT, <wiki>/pages/<p>, body} ──PUT /pages/<p> (x-wiki-author)──▶ wiki :8090
     │ tool_use http_request {GET, <wiki>/pages/<p>}       ──GET /pages/<p>──────────────────▶ wiki
     └ tool_result (the page text, verbatim) in the next request ──▶ crosstalk ──▶ fake-upstream
     │
     └ Event (Request, WikiWrite, WikiRead, ...) ──mpsc──▶ collector (TruthBook) ──▶ report, --ground-truth file
```

### The task marker

The swarm plays the user and the fake model plays an obedient model. Each
prompt is a sentence of prose followed by one marker line:

- `[task:write page=<slug> topic=<n> base=<wiki url>]`
- `[task:read page=<slug> base=<wiki url>]`
- `[task:chat topic=<n>]`

`protocol::Task` renders and parses these lines. `base` is the wiki base
URL the swarm actually uses (`--wiki`, `http://wiki:8090` in compose), as
`BaseUrl::url` renders it: lower-case host, port 80 left out, no trailing
slash. The fake upstream answers from the request alone, so this is how it
learns the URL to put in its tool calls; it echoes it, so the answer is
still a function of the seed and the request.

**One URL spelling.** `protocol::page_url(base, page)` is the only place a
page URL is built: `<base>/pages/<page>`, the base's trailing slash trimmed,
no trailing slash, no query. The model's tool input, the agent's check of
the call, the truth's `route.url` and `read_tool.input.url` all use it, so
the string is identical everywhere (L5's canonicaliser does not fold
trailing slashes).

The fake model reads the marker of the last `user` message. It skips
`system` turns inside `messages`.

- **Write** (when `http_request` is declared): a text block, then
  `tool_use http_request {"method":"PUT","url":"<base>/pages/<page>","body":<text>}`.
  The body is 40–160 words of generated prose. Stop reason `tool_use`.
- **Read** (when `http_request` is declared): a text block, then
  `tool_use http_request {"method":"GET","url":"<base>/pages/<page>"}`.
  Stop reason `tool_use`.
- **A turn of tool results:** a closing prose answer. When every result is
  an error, the answer starts "That did not work". Stop reason `end_turn`.
- **Chat, no marker, or an undeclared tool:** prose. Stop reason
  `end_turn`.

So the swarm's knobs decide how often the wiki is written and read, while
the words still come from the model.

### Scenarios and prose generators

A swarm run is one of two scenarios (`protocol::Scenario`, snake_case on
the wire), chosen with `crosstalk-demo swarm --scenario headline|boilerplate`
(default `headline`) and kept in `SwarmConfig::scenario`:

| Scenario | Generator | What it measures |
| --- | --- | --- |
| `headline` | `text::high_entropy_paragraph` | The headline precision and recall: unrelated model outputs share no run of 32 bytes, so every shared span the gateway finds is a real copy |
| `boilerplate` | `text::templated_paragraph` | A regression scenario: unrelated outputs share template fragments (30–64 bytes, e.g. `Open question: does consumer lag interact with …`), as real agents share boilerplate; the gateway must not call those transmissions |

**The style marker.** The scenario reaches the fake model inside each
request, so the upstream stays stateless and one running upstream serves
both scenarios with no restart. `Agent::new` ends every agent's system
prompt with a blank line and `Scenario::marker()`: `[style:headline]` or
`[style:boilerplate]`. `parse_request` reads the top-level `system` field
(a string, or blocks whose `text` fields are read joined) into
`Request::style` (`Scenario::of_system`): `boilerplate` only when the text
contains `[style:boilerplate]`; `headline` otherwise, including no marker,
no `system` field, an unknown style or a malformed field. A marker in a
user turn does not count. `generate` passes the style to `text::prose` on
every path that writes prose: the PUT body of a write, chat answers, the
closing answer after tool results, and unmarked prompts. The sentence
before a tool call or a closing answer (`lead_in`) depends on the style
too. Under `boilerplate` it is the fixed sentence every agent says ("I'll
update the wiki page `<page>` with my notes on <label>.", "Let me check
`<page>` on the wiki first.", "That did not work, so I'll continue from what
I know.") and draws nothing from the rng, so boilerplate output is frozen.
Under `headline` it is one high-entropy sentence ending with the page,
when there is one; a failed tool call is still acknowledged with the short
"That did not work." (18 bytes) before it. Two agents writing the same page
then share no lead-in text beyond the page name.

**High-entropy generator.** Sentences of 8 to 16 whitespace words (a drawn
target of 8–14, the last token may push past it). Each token is drawn in
turn: after an invented word, a topic term (22%) or a number (10%: a count
10–999 or a percentage 5–64%); otherwise, and always at the start and
after a term or number, an invented word. So no two fixed tokens are ever
adjacent. An invented word is 2 to 4 syllables, each one of 30 onsets
times 10 nuclei (300 consonant-vowel syllables), plus an optional coda.
Sentences start capitalised and end in `.` (or `?` one time in eight).
`tests::text` generates 2000 paragraphs (about 1.9 MB) from different
seeds and bodies across 40 topics and checks no two share a 32-byte
substring (the longest shared run is 28 bytes: a 19-byte topic term with
the edges of the invented words around it); the same corpus from the
templated generator shares runs of 64 bytes.

**Templated generator.** 16 sentence templates filled from the topic's
terms, its label, 8 common phrases and numbers; unchanged since it was
the only generator. It is frozen: `tests::text::templated_paragraph_is_frozen`
pins its bytes and the rng state it leaves for fixed seeds. Boilerplate
request bodies differ from bodies before the scenarios existed (the
system prompt carries the marker), so a given run's text is not the same
as an earlier run's, but its distribution is.

Both generators stop at the first sentence end at or past the drawn word
budget (`--words`, capped by `max_tokens`), so lengths are the same in
both scenarios.

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

- **Identity.** It is named `agent-NNN`. Its key group is
  `i / agents-per-key`, and its `x-api-key` is `sk-ant-demoGGGG-<40 hex>`
  for group `GGGG`, stable per seed. Its system prompt names its focus
  topic (`i mod topics`) and the wiki's URL, and ends with the scenario's
  style marker.
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
- **Failures.** A request is tried up to 3 times, after 0.5 s and then 1 s.
  After that, the conversation is abandoned and a new one starts.
- **Turns.** Every generation request claims its conversation's next turn
  ordinal (`Conversation::claim_turn`) just before it is sent, once its
  body and headers are built. The first is 0. Failed and retried requests
  count; a request that could not be built was never sent and does not.
- **Tool calls** (`swarm/tools.rs`). `WikiCall::parse` reads each call
  against the configured wiki base: `http_request` GET of
  `<base>/pages/<slug>` reads the page, PUT with a text `body` writes it
  (methods case-insensitive). Every other call gets an error tool_result
  and touches nothing: another tool name, another method, a URL that is not
  `page_url(base, slug)` exactly, or a PUT without a body.
  - A write answers `Saved \`p\` (version v, n bytes).`.
  - A read answers with the page text, verbatim.
  - A missing page answers `is_error: true`.
- **Events.** Each request reports a `RequestSample`: streaming or not,
  follow-up or not, the outcome, time to first byte, total time, and bytes
  in each direction. A write the wiki accepted reports a `WikiWrite`
  (`WriteRecord`) at once: writer, key group, session, the turn of the
  request whose answer held the PUT, the tool_use id, page, version and
  when the wiki's answer arrived. A read (found or 404) is held as a
  `PendingRead` and reported as a `WikiRead` (`ReadRecord`) when the first
  request carrying its result is built (see
  [Ground truth v2](#ground-truth-v2)). A failed wiki call reports a
  `WikiError`. A finished conversation reports `ConversationEnded` with
  its session.

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
- the expected transmissions (first reads in a session of a version another
  agent wrote), the distinct writer→reader pairs, rereads, and reads it
  could not attribute;
- the scenario and the run id.

The counts of transmissions, self-reads, rereads and misses are exactly the
ground-truth file's rows of each kind. With `--ground-truth PATH`, it
writes the file below. `--json` prints the report as JSON instead of text.

### Ground truth v2

The schema is agreed with ct-eval's owner. Every line is one JSON object;
the first line is the header. `turn` is the 0-based ordinal of generation
requests (POST /v1/messages) the agent sent in that session, counting
failed and retried ones and nothing else.

```
{"kind":"header","version":2,"scenario":"headline","world":"swarm-<run>","run":"<ulid>","seed":42,"agents":150,"keys":150,"agents_per_key":1,
 "claude_code_shape":true,"started_at_unix_ms":…,"gateway_url":"<the --target base the swarm uses>","wiki_url":"<the wiki base>"}

{"kind":"transmission","world":…,"writer":"a017","reader":"a042","page":"p12","version":3,
 "writer_key_group":2,"reader_key_group":9,
 "writer_session":"…","writer_turn":3,"writer_tool_use_id":"toolu_…",
 "reader_session":"…","reader_turn":5,"reader_tool_use_id":"toolu_…",
 "route":{"kind":"channel","url":"<wiki base>/pages/p12"},"carrier":"tool_result",
 "read_tool":{"name":"http_request","input":{"method":"GET","url":"<wiki base>/pages/p12"}},
 "content":{"blake3":"<hex>","sha256":"<hex>","excerpt":"<~80 chars of the body>","at":{"message":7,"block":1,"tool_use_id":"toolu_…"}},
 "at_ms":12345,"at_unix_ms":…,"written_at_unix_ms":…,"read_at_unix_ms":…}

{"kind":"self_read", <transmission's fields, writer == reader>}
{"kind":"reread",    <transmission's fields; this reader already read this same page version earlier in the same session>}
{"kind":"miss","world":…,"reader":…,"reader_key_group":…,"page":…,"reader_session":…,"reader_turn":…,"reader_tool_use_id":…,
 "read_tool":{…},"at_ms":…,"at_unix_ms":…}
{"kind":"unattributed_read","world":…,"reader":…,"reader_key_group":…,"page":…,"version":…,"reader_session":…,"reader_turn":…,"reader_tool_use_id":…,
 "read_tool":{…},"content":{…},"at_ms":…,"at_unix_ms":…}   (written at the end of the run)
{"kind":"session","world":…,"agent":…,"key_group":…,"session":…,"started_at_unix_ms":…}   (one per conversation, when it starts)
{"kind":"agent_cluster","world":…,"key_group":2,"agents":["a004","a005"]}   (one per key group, singletons included, written once)
```

The example values are illustrative: agents are named `agent-NNN` and
pages `<topic>-<n>`. The keys and their order are exact
(`tests::truth::rows_have_exactly_the_v2_keys`).

Meanings, and where each value comes from:

- **`run`, `world`.** `run` is a ULID minted at start by the spec's
  `UlidGenerator`, stamped with the start time, its 80 random bits drawn
  from `Rng::derive(seed, ["run", started_at_unix_ms])`. Every other choice
  of the run is seeded, so the id is a function of what the header records
  (`seed`, `started_at_unix_ms`); two runs with one seed differ by their
  start, two runs in one millisecond by their seed. `world` is
  `swarm-<run>`.
- **`gateway_url`, `wiki_url`.** `--gateway` and `--wiki` as
  `BaseUrl::url` renders them.
- **`scenario`.** `headline` or `boilerplate`, right after `version`
  (`--scenario`). The schema version stays 2: a reader treats a header
  without the field (older files) as `headline`.
- **`agent_cluster`.** Written once, right after the header, one per key
  group `0..keys` (singletons included): agents `g × agents_per_key` up to
  the next group.
- **`writer_turn`, `writer_tool_use_id`.** The writer's request whose
  response held the PUT tool_use, and that tool_use's id.
  `send_with_retries` returns the turn of the attempt that got the answer,
  and `tools::execute` stamps it on the `WriteRecord`.
- **`reader_turn`, `reader_tool_use_id`.** The reader's first request whose
  `messages` carry the GET's tool_result (the request sent after the read
  ran), and the GET tool_use's id. `exchange` claims the turn, then hands
  every `PendingRead` to `deliver_reads` with the body value it is about to
  send. If that attempt fails, the retry carries the result too, but the
  row keeps the first.
- **`content.at`.** `message` and `block` index `messages` and that
  message's `content` exactly as serialised and sent:
  `conversation::locate_result` searches the very `serde_json::Value` the
  request body is serialised from, so a `role: "system"` turn under
  `--claude-code-shape` counts like any other entry.
- **`content` hashes and `excerpt`.** blake3 and sha256 (lower-case hex) of
  the UTF-8 bytes of the tool_result's `content` string as found in that
  body value, which is the wiki body verbatim. `excerpt` is a substring of
  at most 80 characters, taken from a third of the way in and starting at a
  word, past a page's generic opening.
- **`route`, `read_tool`.** `route.url` is `page_url(wiki, page)`;
  `read_tool.input` is the GET tool_use's input as the model sent it, which
  the agent accepted only if its `url` is that same string.
- **`key_group`.** The index of the shared fake `x-api-key`
  (`agent / agents_per_key`).
- **Times.** One wall-clock reading at start plus monotonic elapsed time
  (`RunClock`), so stamps are ordered like events and
  `unix_ms = started_at_unix_ms + at_ms` exactly. `at_ms` is the read's
  time since the start; `at_unix_ms` equals `read_at_unix_ms` on read rows,
  the moment the wiki's answer to the GET arrived. `written_at_unix_ms` is
  when the wiki's answer to the PUT arrived at the writer: the wiki's API
  is unchanged and carries no timestamp, so this is the client's view of
  acceptance, at most one local round trip late.

Every conversation also gets a `session` row when it starts, before its
first request, so a scorer can map each session the gateway saw to its
agent even when the conversation never touched the wiki.

Classification (`truth::TruthBook`, owned by the collector, the one place
that sees both sides). It keeps a map from (page, version) to the
`WriteRecord`, and a per-session set of (page, version) already read
(dropped when the session's conversation ends). For each read:

1. A 404 is a `miss`.
2. Otherwise the (page, version) the wiki's headers reported is looked up.
   The session's seen-set is updated first, so "read before" is decided in
   read order.
3. `self_read` when the writer is the reader (whether or not it was read
   before); else `reread` when the session had read that version before;
   else `transmission`.

A read whose write has not been reported yet (the writer's event can trail
the reader's) waits in the book and is classified, in order, when the write
arrives. A read whose write never arrives (a version from before this run,
since the wiki outlives swarm runs, or a writer aborted after the wiki
accepted its PUT) is written when the run ends as an `unattributed_read`
row (after every other row, in read order) and counted in
`unattributed_reads`. It carries the read's content, so a scorer can leave
a detection of it unjudged rather than count it as a false positive.
Writes that fail produce no row. A read whose follow-up request is never
sent (the run stopped) is not reported at all.

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
- **Originated spans.** The `body` of each `http_request` PUT is 40–160
  words of model output, distinct across writes. In the `headline`
  scenario unrelated outputs share no 32-byte run; in `boilerplate` they
  share template fragments, which are not transmissions.
- **Cross-agent content matches.** When agent B reads a page agent A wrote,
  that text is the `content` of a `tool_result` in B's next request,
  verbatim (`ContentMatched`).
- **A discovered channel.** L5's HTTP tool contract reads `http_request`
  PUTs (written spans from `body`) and GETs (read content from the
  tool_result) of the same canonical URL `<wiki>/pages/<page>` as writes and
  reads of one resource: the wiki, a channel nobody declared
  (`AccessRecorded`, then `TransmissionConfirmed`).
- **Ground truth.** The `--ground-truth` file's `transmission` rows are the
  edges a correct topology shows, each with the exchanges (session and
  turn) on both sides and the exact span in the reader's request.

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
| | `--scenario S` | headline | `headline` (high-entropy prose) or `boilerplate` (templated prose), via the system prompt's style marker |
| | `--stream-fraction F` | 1 | Requests asking for a stream |
| | `--model NAME`, `--max-tokens N` | claude-opus-5-5, 4096 | Sent as is |
| | `--idle-timeout D`, `--grace D` | 120s, 30s | Give up on a silent response; let in-flight work finish |
| | `--claude-code-shape` | off | A `role: "system"` turn before each prompt, plus Claude Code headers |
| | `--ground-truth PATH`, `--json` | — | Ground truth v2 as JSON lines; the report as JSON |
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
| `Cargo.toml` | Adds the workspace pin `sha2 = { version = "=0.11.0", default-features = false }` (already locked through sqlx) |
| `crates/demo/Cargo.toml` | `crosstalk-demo`: spec, testkit; blake3, bytes, http-body-util, hyper (client, http1, server), hyper-util (tokio), serde, serde_json, sha2, thiserror, tokio (fs, io-util, macros, net, rt, rt-multi-thread, signal, sync, time), tracing, tracing-subscriber (env-filter, fmt, json, std); bin `crosstalk-demo` |
| `crates/demo/src/**` | See [Layout and reuse](#layout-and-reuse) |
| `crates/gateway/tests/architecture.rs` | Adds the `Tool` role (`demo`), `Violation::LayerOnTool`, and tests for it |
| `deploy/demo.Dockerfile` (+ `.dockerignore`) | Builds `crosstalk-demo` like `crosstalk.Dockerfile` (same toolchain, cache mounts and distroless runtime), and `ct-eval` with `crates/eval/gates.toml` for `run.sh bench` ([bench.md](bench.md)) |
| `deploy/compose.demo.yaml` | Override: swaps the config mount of `migrate` and `crosstalk` to the demo config; adds `fake-upstream`, `wiki`, `swarm` (profile `swarm`, `restart: "no"`) and `bench` (ct-eval, profile `bench`, see [bench.md](bench.md)) |
| `deploy/demo/crosstalk.demo.json` | `deploy/config/crosstalk.json` with the Anthropic route's `base_url` set to `http://fake-upstream:8070` |
| `deploy/run.sh` | `demo up`, `demo run [swarm options]`, `demo down`, `demo logs` |

## Tests

| Module | What it shows |
| --- | --- |
| `tests::sse` | The event sequence and the fields of each event. Tool blocks start with empty input and an empty JSON delta. Deltas concatenate to the block. Frames parse with testkit's `EventStream`, concatenate to the exact bytes and reassemble to the message, under three splits. Broken streams are refused with typed errors. The non-streaming document round-trips |
| `tests::text` | The templated generator's bytes and rng use are pinned. `prose` picks the generator by scenario. The high-entropy generator is deterministic, keeps the length and sentence shape, is mostly invented words, and 2000 paragraphs from different seeds and bodies share no 32-byte substring (the templated ones do). The style comes from the system prompt (string or blocks; no marker, no `system`, unknown or malformed is headline; a user turn's marker is ignored). Every prose path (PUT body, chat, closing, failed closing, unmarked) honours it. Headline answers to 400 bodies over 4 seeds share no 32 bytes. Agents' system prompts end with the marker. Scenario names round-trip |
| `tests::generate` | Identical bytes for the same seed and body; another seed or body changes them. Write and read markers produce the matching tool call. Undeclared tools are never called. Tool results get a closing answer. System turns are skipped. Malformed requests are refused. Timing and length stay in range, and `max_tokens` caps the length |
| `tests::conversation` | Each body's messages extend the previous body's. A tool call is answered by its result, refusing out-of-order and foreign results. The Claude Code shape puts a system turn before each prompt. Agent A's generated page reaches agent B's next request verbatim, through the real wiki store and the fake model |
| `tests::truth` | Page URLs from one function (trailing slash, host case, port 80, IPv6) and their round trip through the marker. The one declared tool's schema. `WikiCall` accepts GET/PUT of a wiki page and refuses other tools, methods, URLs and body-less PUTs. The fake model's GET and PUT carry the marker's base URL, deterministically. The agent runs a batch of calls against a real wiki: results, the `WikiWrite` with its turn, and the pending reads. `locate_result` with and without the system turn. Turn claiming, excerpts, digests (sha256 known answer), run ids. The book: every kind, rereads per session, reads waiting for their write, unattributed reads, counts. Every row kind's exact keys and order |
| `tests::truth_run` | Swarm runs over sockets against a recording model that fails the first attempt of about half the bodies (529). Every row is checked against the recorded requests: the header and clusters; `reader_turn` is the first request of the session carrying the result and `content.at` its exact block; the hashes are of those bytes, which are in the raw body; `writer_turn`'s request answers with the PUT whose body is the text; some ordinals count failed requests; the report's counts equal the rows; in both shapes; and all four read kinds appear |
| `tests::units` | Spans, fractions, durations, the random stream, markers, page names, topics, the wiki store, nearest-rank percentiles, base URLs, and the CLI (accepted and refused lines) |
| `tests::servers` | Over sockets: a paced stream (first byte, spread, same bytes twice); the JSON document, 401, 400, count_tokens, 404 and the healthcheck; the wiki (versions, authors, listing, limits); and a short swarm run whose report and ground-truth file show writes, reads and cross-agent transmissions with no failures |
| `tests::deploy` | The demo config equals the deployment config except for the upstream URL |

## Invariants and constraints

- **Determinism.** The fake upstream's answer, ids and timing are a
  function of its seed and the request body bytes. It holds no state; the
  prose style is part of the body (the system prompt's marker).
- **Headline prose shares nothing by accident.** Unrelated high-entropy
  paragraphs share no 32-byte run (`tests::text`). The templated generator
  is frozen byte for byte.
- **The wiki is the only shared state between agents.** Agents share
  read-only configuration and clients behind an `Arc`. Figures go to the
  collector over a channel, and wiki pages live in the one task that owns
  them.
- **Conversation order.** A conversation only grows by valid steps. Every
  tool call is answered exactly once, in order, before the next request.
- **A transmission in the report** is a session's first read of a page
  version another agent wrote, and that page's text is in the reader's next
  request at `content.at`. The report's transmission, self-read, reread and
  miss counts equal the ground-truth file's rows of each kind.
- **One page URL.** Every page URL comes from `protocol::page_url`.
- **Turns** count every generation request sent in a conversation, failed
  and retried ones included, from 0; nothing else claims one.
- **The ground truth has one author:** the collector's `TruthBook`. Agents
  only report what they did.
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
