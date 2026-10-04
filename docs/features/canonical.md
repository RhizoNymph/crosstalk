# Canonical (L1): the Anthropic Messages normalizer

`crosstalk-canonical` implements the spec's `Normalizer`
(`spec/types/interfaces/l1_canonical.rs`) for the Anthropic Messages wire
protocol and every dialect of it, roadmap item P2.5. It turns a
`RawExchange` (what the proxy hands capture) into a `NormalizedExchange`:
the canonical `Exchange`, its messages, the media bytes they name, and the
warnings the spec defines.
Normalization is a set of pure functions; writing the bodies to the blob
store is a separate async step through the spec's `BlobStore`.

## Scope

- Hashing every body with the spec's canonical encoding, and reading
  provider JSON with the spec's exact-number JSON (both moved to the spec
  by P0.7: [spec_primitives](spec_primitives.md)).
- Server-sent events parsed from a whole body (`sse`).
- The Anthropic Messages normalizer (`anthropic`): request bodies (system
  prompt as a string or blocks, messages, role splitting, tool calls and
  results, thinking, media, cache-control markers, unknown blocks),
  responses whole or streamed (reassembly from SSE events, including
  interleaved deltas, pings, `message_delta` usage and mid-stream errors),
  the outcome (stop reason, usage, failures), and the warnings.
- Assembling the `NormalizedExchange` (`assemble`): hashes, one copy of
  each body, the exchange's references, the media blobs, warnings.
- Storing a normalized exchange's message bodies and media through
  `BlobStore` (`capture::store`).

## Non-scope

- Other protocols (OpenAI Chat, OpenAI Responses, Gemini, Code Assist) and
  WebSocket increments: roadmap P8. Their invariant evidence stays pending.
- The capture task: receiving `RawExchange`s from L0, calling `store`, and
  publishing `ExchangeCaptured` only after it succeeds
  (`canonical.capture.blobs-before-event`): the gateway's capture stage
  ([gateway](gateway.md), roadmap P3).
- The encoding, canonical JSON, credential hashing, deployment secrets and
  ULID generation: the spec's ([spec_primitives](spec_primitives.md)),
  with their invariants' evidence (`canonical.encoding.*`,
  `canonical.json.*` vectors and properties, `canonical.ids.*`).
- A fuzz harness (`canonical.normalize.never-panics`): the properties
  feed arbitrary bytes to the response side, but no coverage-guided
  target exists yet.

## Data and control flow

```text
RawExchange ──anthropic::normalize──────────────────────────────────────────────┐
  meta (protocol AnthropicMessages)                                             │
  request.body ──json::Json::parse_bytes──▶ request::normalize                  │
      system ─▶ System message first                                            │
      messages[i] ─▶ turn_messages: user turn split into runs (User | Tool),    │
                      assistant turn one Assistant message                      │
      blocks ──blocks::{system_part, user_item, assistant_parts}──▶ parts       │
                       (media bytes ─▶ MediaSink, keyed by BLAKE3)              │
  response ──response::read                                                     │
      non-2xx complete           ─▶ Failed Upstream { status }                  │
      Http body ─▶ whole()       ─▶ message blocks ─┐                           │
      Sse body  ─▶ stream::read  ─▶ events ─▶ blocks reassembled to whole shape │
      failed (proxy's failure)   ─▶ partial blocks ─┤                           │
                                  blocks::assistant_parts ─▶ ResponseRead       │
  assemble::assemble ◀──────────────────────────────────────────────────────────┘
      warnings (unknown blocks, orphan tool results in a full history)
      MessageSet: Message::new(body) = { hash: BLAKE3(encode(body)), body }, once each
      Exchange { meta, continuation (copied), request: [hash], outcome }
  ─▶ NormalizedExchange { exchange, messages, warnings, media: [MediaBlob] (hash order) }

capture::store(blobs, &normalized)   (async; awaited by the capture task)
  for each message: blobs.put(encode(body)) == message.hash, else HashMismatch
  for each media blob: blobs.put(bytes) == hash
```

`AnthropicMessages` implements `Normalizer`; its `normalize` returns the
whole `NormalizedExchange`, media included, which passes the spec's
`NormalizedExchange::check`.

### The encoding and canonical JSON

Both are the spec's (`crosstalk_spec::observed::message::{encoding,
json}`), documented in [spec_primitives](spec_primitives.md): a body's
encoding is the canonical JSON of its wire-convention shape, its hash the
BLAKE3 of that, and `json::Json` keeps numbers exact. The normalizer
parses provider bodies with `Json::parse_bytes`, writes `CanonicalJson`
with `Json::canonical`, and hashes with `Message::new`.

### Block mapping

| Block | Part |
| --- | --- |
| `text` | `Text` (`citations` dropped) |
| `image`, `document` with a `base64` source | `Media`; the decoded bytes are their own blob, hashed with BLAKE3 |
| `tool_result` in a user turn | a `ToolResult` in a `Tool` message: `content` a string, or `text` / `image` / `document` items; `is_error: true` is `Error` |
| `thinking` | `Reasoning::Visible`, its `signature` verbatim (an empty or missing one is `None`) |
| `redacted_thinking` | `Reasoning::Opaque { signature: data }`, verbatim |
| `tool_use` | `ToolCall`, `Client`, arguments the canonical JSON of `input` |
| `server_tool_use`, `mcp_tool_use` | `ToolCall`, `Server` |
| `*_tool_result` (web search, web fetch, code execution, MCP, ...) after a server call with its id in the same message | `ServerToolResult`; content a string, items, or a single object (kept as one `Unknown`; an `_error` type, or `is_error`, makes it `Error`) |
| anything else, a known block missing a field it needs, a server result with no earlier server call, a URL or file media source | `Unknown { kind: type, raw: canonical JSON }` |

`cache_control` is dropped from every block, `Unknown` ones included: it
says where the harness wants the prompt cache split, not what the model
saw, and Claude Code moves it to the newest message every turn, so keeping
it would change a message's hash as the conversation grows.

A user turn becomes one message per maximal run of one canonical role
(`[tool_result, tool_result, text]` is `Tool` then `User`); a string
content is one text part; an empty array is one empty `User` message. An
assistant turn is one `Assistant` message.

### Streaming reassembly

`stream::read` applies the events in order: `message_start` (id, usage,
any initial content), `content_block_start` at an index,
`content_block_delta` (`text_delta`, `input_json_delta`, `thinking_delta`,
`signature_delta` append; `citations_delta` is dropped; an unknown delta
type makes its block `Unknown` with its start shape), `content_block_stop`,
`message_delta` (stop reason, usage fields laid over the earlier ones),
`message_stop`, `ping` and unknown event types (ignored), and `error`.
Blocks keep their index, whatever order their deltas interleave in. At the
end each block is written back into its whole-body shape (the joined text,
the parsed joined `partial_json` as `input`, the joined thinking), so a
streamed response and the same response sent whole go through one block
mapping and give the same message. An `input_json_delta` text that does
not parse is the call's `ToolArguments::Invalid`, verbatim; no input text
at all means the start block's `input`.

### Outcomes

| Raw response | Outcome |
| --- | --- |
| complete, non-2xx | `Failed`, `Upstream { status }`, no partial response |
| complete 2xx whole body: a `message` | `Completed` |
| complete 2xx whole body: an `error` object | `Failed`, `UpstreamErrorEvent`, no partial |
| complete 2xx stream ending in `message_stop` | `Completed` |
| complete 2xx stream with an `error` event (overloaded) | `Failed`, `UpstreamErrorEvent`, the blocks before it as the partial response |
| complete 2xx stream that started but has no `message_stop` | `Failed`, `StreamTruncated`, the blocks so far |
| complete 2xx body that does not parse, or a stream that never starts a message | `Failed`, `UnparseableResponse`, no partial |
| failed (any proxy failure) | `Failed` with that failure; the blocks its partial bytes started |

A partial response exists when at least one block started. Stop reasons:
`end_turn`, `tool_use`, `max_tokens`, `stop_sequence`, `refusal` map to
their variants, `model_context_window_exceeded` to `MaxTokens`, anything
else (`pause_turn`, unknown, missing) to `Other`; a response holding a
client tool call stops with `ToolUse` unless it hit a token limit. A
completed exchange's `first_chunk_at` is the raw one, or the end time if
the proxy recorded none.

### Token usage

| `TokenUsage` | Anthropic |
| --- | --- |
| `input` | `input_tokens + cache_creation_input_tokens + cache_read_input_tokens`: every prompt token, as OpenAI's `prompt_tokens` counts them |
| `output` | `output_tokens`, the last value (`message_delta` counts are cumulative) |
| `cache_read` | `cache_read_input_tokens` |
| `cache_write` | `Some(cache_creation_input_tokens)` |
| `reasoning` | `None`: thinking tokens are inside `output_tokens` |

Cache writes count in `input` and are reported apart as `cache_write`
(`canonical.usage.cache-writes-reported`). A missing or null cache count is
0. Usage is `None` when
`input_tokens` or `output_tokens` is missing or any count is not a
non-negative integer that fits a `u32`. A stream's usage is
`message_start`'s with each `message_delta`'s fields laid over it.

### Continuations, dialects and warnings

Anthropic over HTTP sends the full transcript every turn; the exchange's
continuation is copied from the decoded request as the adapter read it.
The normalizer handles every dialect (`Reference`, `Vllm`, `Sglang`,
`Copilot`) the same way: nothing branches on it. Warnings, in message
order, request first: one `UnknownBlock` per `Unknown` part anywhere
(system, user, assistant, tool result contents), and, in a `FullHistory`
request, one `OrphanToolResult` per tool result whose call id no earlier
tool call in the request has. The only `NormalizeError` is a request body
that is not an Anthropic Messages request (not JSON, not an object, no
`messages` array, a turn without a `user` or `assistant` role or with
content neither a string nor an array, a `system` neither a string nor an
array) or an exchange of another protocol; the response side never fails
normalization.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `crates/canonical/Cargo.toml` | Manifest: spec, base64, thiserror, tracing; dev: testkit, transport (`MemoryBlobStore`), blake3 (an independent hash oracle), proptest, serde, serde_json, tokio | — |
| `src/lib.rs` | Crate doc, modules, re-exports | `AnthropicMessages`, `store`, `StoreError` |
| `src/sse.rs` | Server-sent events from a whole body | `parse`, `SseEvent`, `SseBody` |
| `src/assemble.rs` | Building the normalized exchange; warnings; media (private) | — |
| `src/capture.rs` | Storing bodies and media through `BlobStore` | `store`, `StoreError` |
| `src/anthropic/mod.rs` | The normalizer | `AnthropicMessages`, `normalize` |
| `src/anthropic/request.rs` | Request bodies to messages | `RequestError` |
| `src/anthropic/blocks.rs` | The block mapping (private) | — |
| `src/anthropic/response.rs` | Outcomes, whole bodies, stop reasons | `stop_reason` |
| `src/anthropic/stream.rs` | Stream reassembly | — |
| `src/anthropic/usage.rs` | The usage mapping | — |
| `src/tests/mod.rs` | The evidence entry points (`crosstalk_canonical::tests::<name>`) | — |
| `src/tests/units/`, `src/tests/props.rs` | Unit and property bodies | — |
| `src/tests/generate/` | Generators: JSON spellings, blocks, turns, requests, whole and streamed responses with random delta cuts, pings, interleaving and CRLF framing | — |
| `src/tests/golden.rs`, `src/tests/support.rs` | The golden harness; raw exchanges from bodies and corpus cases | — |
| `tests/golden/anthropic/<case>.json` | Each captured corpus case's normalized exchange | — |

Goldens are rewritten with `CROSSTALK_BLESS=1 cargo test -p
crosstalk-canonical golden` (review the diff). A golden is the spec's JSON
of the `NormalizedExchange` (`{"exchange", "messages": [{"hash", "body"}],
"warnings", "media": [{"hash", "bytes"}]}`, each body in its encoding's
shape, media bytes in hex; [observed](wire/observed.md)); checking also
decodes the file through the spec's serde, which checks every hash and
reference. The encoding's vectors are the spec's
(`spec/types/tests/golden/encoding/vectors.json`).

## Invariants and constraints

- Normalization is pure and deterministic: no clock, randomness, node
  state or hash-map iteration.
- Every hash an `Exchange` names is a message of the same normalization;
  every message's hash is the BLAKE3 of its encoding; each body is kept
  once.
- The response, or partial response, is always an `Assistant` message.
- A failed exchange always normalizes with its full request; only an
  invalid request body is an error.
- Text, opaque reasoning, tool call ids and invalid argument text are kept
  byte for byte.
- No `unwrap` or `expect` outside tests.
- The crate is a layer crate: it depends on the spec and third-party
  crates only; testkit and transport are dev-dependencies.

Implementation evidence in this crate now passes for: INV-47, 51, 59
(unit), 61, 62, 67, 68, 69, 70 (property; its OpenAI Chat units are P8),
72, 73, 74, 77, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 95, 96, 97, 98, 99,
and the new `canonical.usage.cache-writes-reported` and
`canonical.reasoning.signature-verbatim`. INV-49, 50, 53–56, 58, 59's
property and 60 are the spec's now ([spec_primitives](spec_primitives.md)).
INV-48's evidence moved to the gateway, where the capture task lives, and
passes there ([gateway](gateway.md)). Pending: 57 (cross-node secret agreement,
L0), 66, 71, 75, 78 (other protocols and WebSocket, P8), 76 (fuzz).

## Gaps found

Closed by P0.7 ([spec_primitives](spec_primitives.md)):

- **Decoding bodies outside L1.** The encoding, its strict decoder and the
  canonical JSON moved to the spec, byte for byte (the vectors and the
  corpus goldens passed unchanged against the moved code before the gap
  fixes below changed them).
- **Media bytes.** `NormalizedExchange` carries `media: Vec<MediaBlob>`;
  the `Normalization` wrapper and `normalize_with_media` are gone.
- **Cache writes.** `TokenUsage::cache_write`.
- **The thinking signature.** `Reasoning::Visible` keeps it, and it is
  hashed: echoes carry it unchanged.
- **Goldens in a local shape.** `NormalizedExchange` has serde; the corpus
  goldens are its JSON.
- **`canonical.ids.*` placement.** The keyed hasher, `DeploymentSecret`
  and the ULID generator are the spec's.

Open:

- **Unrepresentable content.** Media given by URL or Files API id has no
  bytes to hash, so it is kept as `Unknown`; web search results and web
  fetch documents have no `ToolResultContent` variant and are `Unknown`
  items, so their text is not indexable.
- **`cache_control` in `Unknown`.** The spec says an `Unknown` part holds
  the block's canonical JSON; the marker is dropped from it so that echo
  stability and request concatenation hold for messages with unknown
  blocks.
