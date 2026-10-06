# Ingress (L0)

`crosstalk-ingress` (`crates/ingress`): the reverse proxy a harness's base
URL points at. It forwards every request upstream unchanged, relays every
response back unchanged, and hands each generation exchange to the capture
task as a `RawExchange` over a bounded in-process channel. Roadmap item
P2.4; it implements `spec/types/interfaces/l0_ingress.rs` for the Anthropic
Messages protocol over HTTP and SSE.

With `ANTHROPIC_BASE_URL=http://gateway:8080/anthropic`, Claude Code talks
to the gateway exactly as it would to `https://api.anthropic.com`.

## Scope

- Reverse-proxy routing from structured config: a path prefix (the base
  URL's path) to an upstream, matched on the request head alone, longest
  prefix on whole segments; an unrouted request is answered 421 locally.
- Credential and account hashing with the spec's `KeyedHasher` (BLAKE3
  keyed with the deployment secret, from an environment variable, with the
  previous version's digests for exchanges that start before the rotation
  overlap's configured end), the credential scheme rule (a Bearer token
  on the Anthropic API is an OAuth access token when its shape says so or
  when `anthropic-beta` carries an `oauth-` value; see
  [claude_code_oauth](claude_code_oauth.md)), and harness
  claims as sent.
- Exchange ids from the spec's `UlidGenerator`, stamped with each
  exchange's start.
- The Anthropic Messages adapter: the endpoint table, request decoding
  (gzip and zstd within a bound), and the response framers.
- Forwarding before decoding: the request body is teed and decoded
  concurrently; a body that does not decode is still forwarded, and the
  exchange is counted, not captured.
- The response tee: relayed frame by frame, framed from the response head,
  kept for capture under a bound.
- Exchange stages, failure causes, timestamps, the hand-off and the
  uncaptured counters.
- The HTTP/1.1 server (accept loop or any byte stream) and the upstream
  client (TLS through rustls, plain HTTP for `http://` upstreams).
- Latency benches for the two performance invariants.

## Non-scope

- Forward-proxy mode (CONNECT, TLS interception for allowlisted hosts),
  WebSocket taps, and the OpenAI, Responses and Gemini adapters: roadmap
  P8. `Routes::intercept` returns `None`; `AnthropicAdapter::tap` returns
  `None` (`NoTap` has no values).
- Normalization (L1, P2.5) and publishing to the bus (P3). The proxy's
  output is the capture channel.
- Gateway wiring, the binary, config files on disk and the health
  endpoint (P3). `anthropic_proxy` builds the production proxy from an
  `IngressConfig`, an environment lookup, the capture sender and a clock.
- HTTP/2 on either side.

## Decisions

### D1: capture-buffer overflow drops the capture, never the client

The relay buffers nothing of its own: hyper polls the response body only
when the client connection can take more, and the body polls the upstream
once per poll, so a slow client slows the upstream read (TCP flow control),
not memory, and capture never slows the client. What capture keeps (the
response bytes, until the stream ends, because the hand-off waits for the
end) is bounded by `limits.response_capture_bytes`. Past it the kept bytes
are released at once, the exchange is marked overflowed, and at the end it
is counted uncaptured as `response_too_large` instead of producing a
`RawExchange`.

It is not marked `StreamTruncated` and not captured with a cut body: a
`RawExchange`'s bytes must be every byte received
(`ingress.raw-exchange.response-body-as-received`), and `StreamTruncated`
means the stream ended before its terminal frame
(`ingress.failure.cause-classified`); either would record something the
agent never saw. This follows the request tee's rule
(`ingress.decode.tee-bounded`: abandon the copy, count the loss, keep
forwarding). Recorded as the new invariant
`ingress.capture.response-bounded` (INV-798).

### D2: the framer reports a held-back error on the next push

`ResponseFramer::push` returns events or an error, not both. When one
chunk completes events and then hits an error, the framer returns the
events and returns the error on the next push; the relay pushes an empty
chunk after every push that returned events. The events and the error a
stream yields, offset included, are then the same however it is chunked
(`ingress.framer.chunking-invariant`).

### D3: what counts as first content and finished

- SSE (2xx `text/event-stream`): `FirstContent` at the first message event
  (`message_start`, `content_block_*`, `message_delta`, `message_stop`);
  `Finished` at `message_stop`. `ping`, comments and unknown event types
  are skipped; an `error` event is `UpstreamErrorEvent`; data that is not
  JSON, a non-UTF-8 `event` field, or an event over
  `limits.sse_event_bytes` is `MalformedFrame` at the event's first byte.
- One body (any other 2xx): `FirstContent` at the document's first byte,
  `Finished` at the bracket that closes it (bracket depth outside strings;
  content validity is L1's). A body not starting with `{` or `[` is
  malformed at that byte.
- Error document (non-2xx): no events. The exchange failed at the head
  with `Upstream { status }`; its body is kept as the partial body.

### D4: failures and what the client sees

| Situation | Client gets | Exchange |
| --- | --- | --- |
| No route | 421, Anthropic error JSON, from the proxy | none, not forwarded |
| Connect fails, or the upstream closes before a head | 502 from the proxy | `Failed(UpstreamUnreachable)` |
| No head within the idle timeout | 504 from the proxy | `Failed(Timeout)` |
| Non-2xx head | the upstream's response | `Failed(Upstream { status })` |
| Error event in a 2xx stream | the stream, unchanged | `Failed(UpstreamErrorEvent)` |
| Malformed frame | the stream, unchanged, to its end | `Failed(MalformedStream { offset })` |
| Stream ends, or is cut, before `Finished` | the same end (a cut is a cut) | `Failed(StreamTruncated)` |
| No body bytes for the idle timeout | the body ends with an error | `Failed(Timeout)` |
| Client leaves first | — | `Failed(ClientDisconnected)` |

The first cause wins: a stage that is already terminal never changes. The
idle timeout (`limits.upstream_idle_timeout_ms`) is off by default, so the
client's own timeout applies as it would without the proxy; when set, it
covers the wait for the head and every gap between body chunks. The proxy
never assigns `UnparseableResponse`.

### D5: a request body hyper let go of is still read for capture

When the upstream cannot be reached, or answers before reading the whole
body, hyper drops the request body unread. The tee hands the rest of the
body to the exchange's capture task, which reads it from the client itself
(under the same tee bound), so a failed exchange still carries its request
(`ingress.capture.one-raw-exchange`). The client is answered without
waiting for that.

### D6: classification sees the upstream path

Adapters classify the head as the upstream will see it: the route prefix
removed and the upstream base URL's path in its place. `/v1/models` is
shared with OpenAI's protocol, so the Anthropic adapter claims it only with
an `anthropic-version` header.

### D7: keyed hashing and ids are the spec's

The keyed hasher, `DeploymentSecret` and the ULID generator are
`crosstalk_spec::ids` (`KeyedHasher`, `DeploymentSecret`, `UlidGenerator`,
`SeededRandom`); ingress has no implementation of its own.

- **Secrets.** `load_secrets` reads each configured secret's environment
  variable through `DeploymentSecret::from_hex` (64 hex digits, either
  case, surrounding ASCII whitespace such as a trailing newline ignored)
  and builds `KeyedHasher::new(current)`, or, with a `previous` secret,
  `KeyedHasher::rotating(current, previous, overlap_ends)`, which refuses a
  previous version that is not older than the current one
  (`SecretError::Rotation`). Neither type is `Clone`, so
  `HeaderIdentifier` holds the hasher behind an `Arc` and clones share it.
- **Rotation overlap.** The overlap ends at `overlap_ends`, an explicit
  instant in the config. The hasher is pure: `HeaderIdentifier::context`
  and the `ClientIdentifier` derivations pass the exchange's `started_at`,
  and previous-version digests are computed exactly for exchanges that
  start before `overlap_ends`
  (`ingress.credential.previous-digests-within-overlap`); current-version
  digests do not depend on the time.
- **Exchange ids.** One `UlidGenerator<SeededRandom>` (seeded from the
  operating system's randomness in production) is shared by every
  connection task behind a `std::sync::Mutex`, locked only for the
  synchronous mint and never across an await. Each id is
  `mint_at(started_at)`: its time is the exchange's start, and ids stay
  monotonic when exchanges started on concurrent connections reach the
  generator out of order (`canonical.ids.ulid-monotonic`). A poisoned lock
  is recovered: the generator is never left half-updated. If no id is left
  (`UlidExhausted`, only reachable with a clock past the year 10889), the
  request is forwarded uncaptured and counted `ids_exhausted`.

### D8: TLS and pooling

Upstreams are reached through hyper-util's pooled client with
`hyper-rustls` (rustls with ring, Mozilla roots from `webpki-roots`,
HTTP/1.1, `TCP_NODELAY`). The pool resends a request only when a reused
connection closed before any of it was written, so the upstream never
receives a request twice (`ingress.passthrough.no-originated-requests`).

## Data and control flow

```text
client ──HTTP/1.1──▶ Proxy::serve / serve_connection (hyper http1, no Date of its own)
                        │
                        ▼ Proxy::handle
                     Routes::resolve(path, query) ── none ──▶ 421 (local)
                        │ strip hop-by-hop + Host
                        ▼
                     adapter.classify(head as upstream sees it)
          ┌─────────────┼───────────────────────────┐
     None (count       Some(not Generation)      Some(Generation)
     unclassified)          │                        │ StageClock::start (one wall reading)
          └──────┬──────────┘                        │ mint_at(started_at), HeaderIdentifier::context(.., started_at)
                 ▼                                   │ (credential hashed; decode head has no credentials)
          forward plainly ──▶ relay Incoming         │ spawn finish(...)   ── capture task ──┐
                                                     ▼                                       │
                                     forward with TeeBody (bounded copy of frames) ──oneshot─▶ tee outcome
                                                     │                                       │  └▶ RequestDecoder::decode
                                                     ▼ response head                         │     (decompress within bound,
                                     adapter.framer(head); Pending::respond                  │      adapter.decode_request)
                                                     ▼                                       │
                                     CaptureBody: each frame ─▶ client                       │
                                       side: Kept (bounded), framer.push, InFlight stages    │
                                       end / error / timeout / drop ──oneshot─▶ ResponseRecord
                                                                                             ▼
                                                     join(decode, record) ─▶ RawExchange ─▶ CaptureSender::offer (try_send)
                                                                          └▶ or CaptureStats::uncaptured(reason)
```

- **Hot path.** Routing, classification, identification and the tee are
  synchronous and cheap; the request goes upstream the moment it is
  routed. Each response frame is returned to hyper in the same poll that
  read it, after the framer's look.
- **Off the hot path.** One capture task per generation exchange joins the
  tee's copy (then the decoder) and the response record, in either order,
  and hands off once both exist (`ingress.capture.decode-attached-at-handoff`).
  The decoder runs on that task; `AdapterDecoder` yields once, then does
  the CPU work.
- **Exactly one record.** `Pending` owns the record sender until a head
  arrives, then `CaptureBody` does; each sends it on every path, including
  being dropped (the client left), so every forwarded exchange ends.
- **Times.** `started_at` is the one clock reading; `first_chunk_at` and
  `ended_at` add `tokio::time::Instant` elapsed time to it, so they are
  ordered even when the wall clock steps.
- **Stages.** `InFlight` reports each stage change to an optional bounded
  observer (`try_send`, never waits): diagnostics and the simulation tests.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `crates/ingress/Cargo.toml` | Manifest (see [workspace](workspace.md)) | — |
| `src/lib.rs` | Crate doc, trait-to-type table, production builder | `anthropic_proxy`, `AnthropicProxy`, `BuildError` |
| `src/config.rs` | Structured config, unknown fields refused | `IngressConfig` (`from_json`), `RouteConfig`, `UpstreamConfig`, `SecretsConfig`, `SecretRef`, `PreviousSecretRef` (with `overlap_ends`), `LimitsConfig` (`upstream_idle_timeout`), `CaptureConfig` |
| `src/routing.rs` | `UpstreamRouter` | `Routes` (`new`, `resolve`), `Resolved`, `RoutePrefix`, `UpstreamBase`, `ConfigError` |
| `src/credential.rs` | Raw credentials, secret loading into the spec's `KeyedHasher` | `RawCredential`, `load_secrets`, `SecretError` (`Missing`, `Malformed` with the spec's `InvalidSecret`, `Rotation` with its `InvalidRotation`) |
| `src/identify.rs` | `ClientIdentifier` | `HeaderIdentifier` (`new`, `raw_credential`, `scheme` with the `OauthCapability` the head carries, `context` at a start time; `Clone`, sharing one `Arc<KeyedHasher>`), `OauthCapability` (`of`), `CredentialSource`, `CREDENTIAL_HEADERS`, `without_credentials` |
| `src/encoding.rs` | `content-encoding` and bounded decompression | `content_encoding`, `decode`, `EncodingError` |
| `src/adapter/mod.rs`, `anthropic.rs` | `ProviderAdapter` for Anthropic Messages | `AnthropicAdapter`, `ANTHROPIC_VERSION`, `NoTap` |
| `src/framer/mod.rs`, `sse.rs`, `json.rs` | `ResponseFramer` | `AnthropicFramer` (`for_response`, `kind`), `FramerKind` (`for_head`), `SseFramer`, `JsonFramer` |
| `src/decode.rs` | Decoding for capture | `RequestDecoder`, `AdapterDecoder` (`decode_now`), `DecodeJob`, `CaptureDecodeError` |
| `src/exchange.rs` | Stages, times, the response record | `InFlight`, `StageClock`, `StageEvent`, `StageObserver` |
| `src/capture.rs` | The hand-off and loss counters | `CaptureSender` (`new`, `offer`, `capacity`), `Offer`, `CaptureStats` (`snapshot`), `CaptureCounts`, `UncapturedReason` |
| `src/proxy/mod.rs` | The proxy and the capture task; mints exchange ids from the shared generator | `Proxy` (`new`, `handle`, `stats`), `ProxyParts` (`ids: UlidGenerator<SeededRandom>`) |
| `src/proxy/server.rs` | Accept loop and per-connection serving | `Proxy::serve`, `Proxy::serve_connection`, `ServeError` |
| `src/proxy/headers.rs` | Hop-by-hop handling, head views | `strip_hop_by_hop`, `strip_request`, `request_head`, `response_head` |
| `src/proxy/tee.rs` | The request-body tee (crate-private) | `TeeBody`, `TeeOutcome`, `read_rest` |
| `src/proxy/relay.rs` | The response tee | `CaptureBody`, `RelayError`; `Pending` (crate-private) |
| `src/proxy/body.rs` | Body types handed to hyper | `ProxyBody`; `UpstreamBody` (crate-private) |
| `src/proxy/connector.rs` | Production upstream connector | `https`, `Https` |
| `src/benches.rs` | Ignored timing tests for the latency budgets | — |
| `src/tests/` | Socket tests on testkit's fakes (`support.rs`), simulations (`sim_support.rs`, `scenario.rs`, `sim.rs`), one module per invariant group | — |

## Invariants and constraints

- The upstream receives the client's method, path under the prefix,
  query, end-to-end headers and body bytes; the client receives the
  upstream's status, end-to-end headers and body bytes. Hop-by-hop fields
  (RFC 9110 7.6.1) and `Host` are the only changes; the proxy adds no
  `Date`.
- No raw credential or secret is logged, stored, published or put in an
  error; `RawCredential` prints a placeholder and the spec's
  `DeploymentSecret` and `KeyedHasher` print versions only.
- A `ClientContext`'s `credential` and `account` are keyed with the current
  version whatever the time; `previous_digests` is `Some` exactly when the
  configured previous secret's `overlap_ends` is after the exchange's
  `started_at`. The previous version is older than the current one.
- An exchange id's ULID time is its exchange's `started_at` millisecond,
  or later only when an earlier-minted id already took that millisecond
  (the generator stays monotonic).
- Every forwarded generation exchange whose request decodes yields exactly
  one `RawExchange`, after its stream ended; every other one is counted by
  reason (`unclassified`, `decode_error`, `channel_full`, `channel_closed`,
  `response_too_large`, `ids_exhausted`). Non-generation endpoints are not
  losses.
- The capture channel is the caller's bounded `tokio::sync::mpsc`;
  `CaptureSender` offers only `try_send`.
- Memory per exchange is bounded: the request tee
  (`request_tee_bytes`, default 32 MiB), the decoded body
  (`decoded_bytes`, 64 MiB), the kept response (`response_capture_bytes`,
  32 MiB), one SSE event (`sse_event_bytes`, 8 MiB).
- Concurrency is tokio only; the capture hand-off and the record and tee
  hand-offs are channels. The only shared state is the atomic counters
  and the exchange id generator, behind a `std::sync::Mutex` held only for
  one synchronous mint.
- No `unwrap` or `expect` outside tests; errors are typed (`thiserror`);
  logs are structured (`tracing`), and never carry headers or queries.

### Invariant evidence

Passing (every evidence key reviewed): INV-1, 5, 6, 7, 8, 9, 10, 12, 13,
14, 15, 16, 17, 19, 20, 21, 22, 24, 25, 26, 27, 32, 33, 34, 36, 40, 41, 42,
43, 384, 385, 386, 387, 388, 389, `ingress.capture.response-bounded`, and
the new `ingress.credential.previous-digests-within-overlap` (INV-816).
INV-1156 (`ingress.credential.oauth-capability-marks-oauth`) and INV-1158
(`ingress.credential.absent-end-to-end`) are covered with
[claude_code_oauth](claude_code_oauth.md).
INV-37 was already satisfied by the spec.

Partly: INV-18 (unit yes; the fuzz target does not exist), INV-23 and
INV-35 (the HTTP tests exist; the WebSocket test in the same evidence list
waits for P8), INV-28 (HTTP relay test exists; the WebSocket one waits),
INV-29 and INV-30 (property yes; the live-upstream integration tests need
real credentials and network).

Pending: INV-2 and INV-3 (one adapter exists: disjointness is vacuous and
the endpoint table's other protocols are P8), INV-4 (no coverage-guided
fuzzing harness), INV-11 (`previous_response_id` belongs to the Responses
adapter), INV-31, 38, 39, 44, 45, 46 (WebSocket and forward proxy, P8).

INV-26's statement also covers WebSocket turns; its test covers HTTP and
SSE, the only transports that exist.

## Testing

```sh
cargo test -p crosstalk-ingress                                   # unit, property and dst
CROSSTALK_SIM_SEEDS=300 cargo test -p crosstalk-ingress tests::sim  # a wider seed sweep
cargo test --release -p crosstalk-ingress -- --ignored --nocapture --test-threads=1 benches
```

- Socket tests start testkit's `FakeUpstream` and the proxy on
  `127.0.0.1:0` and drive them with `HarnessClient`, comparing with
  `differences_from` both ways.
- Simulation tests (`tests::sim`) run under `crosstalk_sim::sim_test!`:
  the proxy's connector and its clients use in-memory duplex pipes (the
  simulation runtime has no IO driver), the clock is `SimClock`, and each
  seed draws its scenarios (completion, 429, cut, clean truncation, stall
  with an impatient client, undecodable body, refused connection).
  `GatedDecoder` holds decodes to show forwarding never waits for them.
- The benches compare the same proxy with capture on and off (a
  generation route against one the adapter classifies `Other`), through
  in-memory pipes, on a 4-thread runtime: per-chunk relay latency, and the
  time until the upstream sees a request's first and last body bytes for
  identity, gzip and zstd bodies up to 4 MiB decoded. Each reports the
  median over 9 rounds of the per-round p99 difference and asserts it is
  at most 1 ms. Measured on a 16-core machine at load average 10 to 25
  (release build): the relay adds 0 to 0.16 ms at p99 (p50 about 40 µs
  either way); decoding adds 0 to 0.86 ms at p99 at load 10 and up to
  about 2 ms at load 25, where the same comparison of a captured route
  against itself shows a noise floor of about 30 µs at the quietest. Run
  them on a quiet machine.
