# Testkit

`crosstalk-testkit` (`crates/testkit`): builders for common spec values, the
recorded-traffic corpus and its loader, a fake upstream that replays it and
a fake harness client that sends it. Roadmap item P1.4. It is test support:
layer crates take it only as a dev-dependency (`docs/features/workspace.md`).

## Scope

- Deterministic ids and timestamps for test values (`ids`, `time`).
- Builders for agents, resources, accesses, channels, exchanges,
  normalized exchanges and message bodies, content matches and
  co-accesses, transmissions in every state, alerts, alert rules, topic
  version histories, bus events and envelopes (`build`).
- The Anthropic Messages corpus under `crates/testkit/corpus/`, its file
  formats, and a loader that types and self-checks every case (`corpus`).
- A fake upstream: a hyper 1.11.1 HTTP/1.1 server on `127.0.0.1:0` that
  replays cases, paces event streams, stalls, disconnects, withholds or
  fails on command, and records what it received (`upstream`).
- A fake harness client that sends a corpus request and collects the
  response with chunk arrival times (`client`).

## Non-scope

- Real captures. Every case is synthetic, written from documentation
  (`corpus/README.md` says how to add captures and how to redact them).
- Other protocols (OpenAI Chat and Responses, Gemini) and WebSocket
  transports: the corpus has room for them (`corpus/<protocol>/<route>/`),
  but only `anthropic/messages` exists.
- TLS, HTTP/2 and the forward-proxy `CONNECT` path: the fakes speak plain
  HTTP/1.1.
- The canonical encoding itself: `build::message::content_hash` calls the
  spec's (`crosstalk_spec::observed::message::encoding::hash`), so built
  hashes are production hashes.
- Simulation (virtual clock, fault schedules): `crosstalk-sim` (P1.3).
- In-memory store implementations: `crosstalk-memory` (P2.3).

## Data and control flow

### Builders

```text
Ids::seeded(seed) ──&mut──▶ XBuilder::new(&mut ids)   every id allocated up front
                               │ fluent overrides (pure)
                               ▼
                            build() ──▶ value            plain spec types
                            build() ──▶ Result<value, BuildError>   checked types,
                                         through the spec's own constructor
```

- `Ids` hands out ULIDs whose 48-bit time part is `Ids::TIME_MS`
  (2026-10-01, the same instant as `time::T0`) and whose 80 random bits
  are the seed (32) above a counter (48). The same calls under the same
  seed build the same values; different seeds never collide; no id is in
  the reserved built-in rule range. Digests (message, prompt, credential
  and account hashes) are the counter value expanded to 32 bytes.
- A builder takes its ids in `new`, so `build` never needs the generator
  and overriding an id (`with_id`, `between`, `on`, …) never wastes one in
  a way that changes later values.
- Builders of checked types (`ContentMatchBuilder`, `CrossAccessBuilder`,
  `TransmissionBuilder`, `UserRuleBuilder`, `TopicHistoryBuilder`) build
  through `ContentMatch::new`, `CoAccess::new`, `Confirmed::new`,
  `AlertRuleDef::load`, `TopicVersionInfo::with_retention` and
  `TopicVersionHistory::new`. Their defaults always succeed; an override
  that breaks an invariant returns that constructor's refusal inside
  `BuildError`.
- `TransmissionBuilder` lays out one transmission end to end: the sender's
  write, the reader's read of the same resource 30 s later, their
  co-access, and one or more exact content matches located in the read's
  tool result. Every state takes its data from that layout, so a
  suspected transmission's co-access and a confirmed one's matches always
  agree with its sender, reader and times (`build_parts` returns them).
- `TopicHistoryBuilder` derives every status from the order of fitted
  versions (`activated`, `ready`, then optionally `fitting`): the newest
  activated version is active, older ones are superseded by the first
  later activation at its time, newer ones ready.
- `NormalizedExchangeBuilder` takes message bodies, hashes each with
  `content_hash`, keeps each distinct body once, and points the exchange's
  request and response at those hashes: the `NormalizedExchange` invariant
  holds by construction, and `NormalizedExchange::check` passes for
  bodies without `Media` parts (its `media` is empty; a test that needs
  media blobs adds them itself). Its default usage reports no cache reads
  and `Some(0)` cache writes.

### Corpus

```text
corpus/anthropic/messages/<case>/{request.http, response.http, meta.json}
      │ http::read_request / read_response      (head parsed, body verbatim;
      │                                          event stream split into frames)
      │ serde_json → CaseMeta (strict)
      ▼
anthropic::load(dir) ──check──▶ Case          refuses a case inconsistent
anthropic::cases()  = load_all(dir()), sorted, `follows` resolved
```

`check` verifies, per case: the credential placeholder sits in the
declared header (and no credential header exists when none is declared);
every claim header is present and the session, agent and parent ids in
`meta.json` equal the headers; for generation, the body is a JSON object
whose `model` and `stream` match, the response is an event stream exactly
when the request streams and the status is 2xx, and the content (block
types in order, stop reason, message id, a final `message_stop` or
`error` event, or an error JSON body with the expected status) matches the
expected outcome; token counting and model listing answer whole JSON
objects; a probe answers whole.

### Fake upstream

```text
HarnessClient ──TCP──▶ accept task (JoinSet of connection tasks)
                          │ hyper http1, auto date off
                          ▼
                       handle: read whole request ──Command::Serve──▶ dispatcher task
                                                   ◀──oneshot Reply──  (owns Script,
                          │                                             pending Next,
                          ▼                                             request log)
                       respond: status + headers + ReplyBody
                          │  Whole: content-length          FakeUpstream handle:
                          │  Fed: feeder task ─mpsc(1)─▶ body   reply_next, fail_next,
                          ▼                                      fault_next, stall_next,
                       chunks paced; Stall waits for the        disconnect_next, mount,
                       receiver to close; Disconnect sends      received ──mpsc──▶ dispatcher
                       a body error, hyper drops the connection
```

- No state is shared between tasks: the dispatcher owns the script, the
  queue of one-shot commands and the log, and everything reaches it over
  one `mpsc` channel with `oneshot` answers.
- A route (method and path, query ignored) answers its replies in order
  and repeats its last; unmatched requests get the fallback, a 404
  `not_found_error`. A one-shot command applies to the next request on any
  route: `Next::Replace` swaps the reply, `Next::Fault` adds a fault to the
  scripted one.
- An event-stream reply goes out chunked, one SSE frame per chunk; a whole
  reply goes out with `content-length`. The feeder's channel holds one
  chunk, so a slow client holds the feeder back. The server sets no
  `date` and adds nothing but framing, so a client receives exactly the
  recorded status, end-to-end headers and bytes.
- Dropping `FakeUpstream` aborts the accept and dispatcher tasks; the
  accept task's `JoinSet` aborts every connection, which closes every
  body and ends every feeder.

### Fake harness

`HarnessClient::open` connects, hands the hyper connection to a spawned
driver task, sends the recorded method, `prefix + target`, headers and
body (adding only `host`; hyper adds `content-length`), and returns a
`ResponseStream` once the head arrives. `next` yields chunks with their
arrival time since the request was sent, then a `BodyEnd`: `Complete`,
`Aborted` (the connection failed mid-body) or `Stalled` (nothing for the
idle timeout). `send` collects everything into a `CollectedResponse`.

### Comparisons

`ReceivedRequest::differences_from(&CorpusRequest)` and
`CollectedResponse::differences_from(&CorpusResponse)` return a
`Vec<Difference>` (method, target, status, end-to-end headers, body with
the first differing byte), empty when the message went through unchanged.
Headers compare as a multiset by name with each name's values in order,
ignoring `http::NOT_RECORDED`.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `crates/testkit/Cargo.toml` | Manifest: spec, bytes, http-body-util, hyper (client, http1, server), hyper-util (tokio), serde, serde_json, thiserror, tokio (macros, net, rt, sync, time), tracing | — |
| `src/lib.rs` | Crate doc and modules | `build`, `client`, `corpus`, `ids`, `time`, `upstream` |
| `src/ids.rs` | Deterministic id and digest generator | `Ids` (`new`, `seeded`, `id::<I>`, `agent`, `exchange`, `conversation`, `span`, `resource`, `access`, `channel`, `transmission`, `topic`, `alert`, `rule`, `operator`, `event`, `merge`, `digest`, `message`, `prompt`, `credential`, `account`, `issued`), `EntityId`, `SECRET_VERSION` |
| `src/time.rs` | The test epoch | `T0`, `at`, `after`, `secs`, `millis` |
| `src/build/mod.rs` | Builder index and re-exports | every builder, `BuildError` |
| `src/build/error.rs` | Constructor refusals | `BuildError` (`Range`, `ContentMatch`, `CoAccess`, `Confirmed`, `Embedding`, `Similarity`, `Text`, `QueryText`, `ReservedRuleId`, `VersionInfo`, `History`) |
| `src/build/agent.rs` | Agents | `AgentBuilder` (`registered`, `provisional`, `established`, `merged_into`, `active`, `evidence`, `with_evidence`, `subagent_of`, `parent`, `label`) |
| `src/build/flow.rs` | Resources, accesses, channels | `ResourceBuilder` (`file`, `url`, `mcp`, `locator`), `AccessBuilder` (`by`, `on`, `in_exchange`, `at`, `via`, `part`, `write`, `write_spans`, `read`), `ChannelBuilder` (`discovered`, `promoted`, `superseded_by`, `declared`, `declared_unused`, `declared_in_use`, `detection`, `policy`, `sanctioned`, `unsanctioned`, `resources`; the default is a discovered channel seeded by a fresh resource and the fresh cross-agent transmission that discovered it, opened at `T0` and active since then) |
| `src/build/exchange.rs` | Exchanges and normalized exchanges | `ExchangeBuilder` (`client`, `credential`, `session`, `subagent`, `class`, `started_at`, `request`, `increment`, `response`, `stop`, `usage`, `failed`, `failed_after`), `NormalizedExchangeBuilder` (`exchange`, `request`, `then`, `response`, `failed`, `warning`), `claude_code_client`, `anthropic_api`, `CLAUDE_CODE_VERSION`, `CLAUDE_CODE_USER_AGENT`, `MODEL` |
| `src/build/message.rs` | Message bodies | `content_hash`, `message`, `system_text`, `user_text`, `assistant_text`, `assistant`, `tool_call`, `tool_result` |
| `src/build/provenance.rs` | Content matches and co-accesses | `ContentMatchBuilder` (`from`, `to`, `origin`, `reader_exchange`, `part`, `range`, `carrier`, `kind`, `matched`), `CrossAccessBuilder` (`writer`, `reader`, `resource`, `write_at`, `lag`, `window`), `CrossAccess`, `LAG`, `WINDOW`, `MATCHED_BYTES` |
| `src/build/transmission.rs` | Transmissions in every state | `TransmissionBuilder` (`between`, `channel`, `route`, `opened_at`, `accesses`, `with_match`, `matched`, `state` and one method per state, `classification`, `topic`, `watched`, `build`, `build_parts`), `TransmissionParts` |
| `src/build/alert.rs` | Alerts and rules | `AlertBuilder` (`rule`, `builtin`, `subject`, `raised_at`, `occurrences`, `by`, `open`, `acknowledged`, `resolved`, `suppressed`), `UserRuleBuilder` (`watched_topic`, `semantic_query`, `name`, `created`, `status`, `disabled`, `sinks`, `topics`, `query`, `threshold`, `stale`), `builtin_rule`, `test_model` |
| `src/build/topic.rs` | Topic version histories | `TopicHistoryBuilder` (`activated`, `ready`, `fitting`, `pin`, `drop_version`, `build_versions`, `build`) |
| `src/build/event.rs` | Bus events and envelopes | `EnvelopeBuilder` (`at`, `with_id`), `exchange_captured`, `conversation_delta`, `agent_seen`, `content_matched`, `access_recorded` (its channel `None` for a resource on no channel), `channel_discovered` (with the channel's seed), `transmission_confirmed`, `transmission_classified`, `topic_version_ready`, `watermark_advanced`, `alert_opened`, `alert_changed`, `policy_changed`, `changed` |
| `src/corpus/mod.rs` | A case | `Case` (`request_with_credential`, `response_bytes`), `root`, re-exports |
| `src/corpus/http.rs` | `.http` files and comparisons | `CorpusRequest` (`path`, `query`, `json`, `head`, `differences`), `CorpusResponse` (`head`, `framing`, `json`, `differences`), `ResponseBody` (`Whole`, `EventStream`; `bytes`, `chunks`, `events`), `Headers`, `Difference`, `NOT_RECORDED`, `read_request`, `read_response`, `parse_request`, `parse_response`, `Malformed`, `HttpFileError` |
| `src/corpus/sse.rs` | Byte-exact event streams | `EventStream` (`parse`, `raw`, `events`, `dispatched`, `trailing`, `chunks`), `SseEvent` (`raw`, `event`, `data`, `id`, `retry`, `comments`, `kind`, `dispatches`, `json`), `SseError` |
| `src/corpus/meta.rs` | `meta.json` | `CaseMeta`, `Provenance`, `Endpoint` (`kind`), `Expect`, `BlockKind`, `CredentialMeta`, `HarnessMeta` |
| `src/corpus/anthropic.rs` | The Anthropic Messages loader | `cases`, `case`, `dir`, `load`, `load_all`, `check`, `CorpusError`, `Inconsistency` |
| `src/upstream/mod.rs` | The fake upstream | `FakeUpstream` (`start`, `replay`, `addr`, `base_url`, `reply_next`, `fail_next`, `fault_next`, `stall_next`, `disconnect_next`, `mount`, `received`), `ReceivedRequest` (`differences_from`), `UpstreamError` |
| `src/upstream/script.rs` | Replies and routes | `Script` (`route`, `case`, `cases`, `fallback`), `Route` (`new`, `of`, `matches`), `Reply` (`from_response`, `from_case`, `json`, `error`, `header`, `paced`, `with_fault`, `body`), `Pacing` (`IMMEDIATE`, `every`), `Fault` (`Stall`, `Disconnect`, `NoResponse`), `Framing`, `error_type` |
| `src/upstream/body.rs` | The served body and its feeder (private) | `ReplyBody`, `BodyCut`, `reply_body` |
| `src/client.rs` | The fake harness | `HarnessClient` (`new`, `prefix`, `idle_timeout`, `send`, `open`), `ResponseStream` (`status`, `headers`, `next`, `collect`), `Next`, `ReceivedChunk`, `BodyEnd`, `CollectedResponse` (`events`, `differences_from`), `ClientError` |
| `src/tests/` | Builder validity, corpus loading and refusals, SSE framing, upstream round trips and faults | — |
| `corpus/README.md` | The corpus: synthetic status, formats, cases, how to add redacted captures | — |
| `corpus/anthropic/messages/*/` | 15 cases | — |

## Invariants and constraints

- Every value a builder returns passes the spec's checked constructors;
  the tests round-trip each through JSON, whose decoding runs them again.
- Builders are deterministic: the same calls on `Ids::seeded(s)` build
  equal values. Generated rule ids are never reserved.
- A loaded case is consistent with its `meta.json` (see `check`), every
  recording omits `NOT_RECORDED` headers, every event stream's frames
  concatenate to its exact bytes, and every case is marked synthetic until
  replaced by a redacted capture.
- No corpus file holds a real credential, user id or file content.
- The fake upstream changes nothing but framing: a client receives the
  recorded status, end-to-end headers and body bytes, and the upstream
  logs every request exactly as read.
- One-shot commands apply to exactly the next request, in the order sent.
- Concurrency is tokio only, with no shared state: the dispatcher owns all
  mutable state and is reached over channels. Logging is structured
  (`tracing` key-value fields).
- No `unwrap` or `expect` outside tests; errors are typed (`thiserror`).
- `crosstalk-testkit` is never a normal or build dependency of a layer
  crate (enforced by `crates/gateway/tests/architecture.rs`).
