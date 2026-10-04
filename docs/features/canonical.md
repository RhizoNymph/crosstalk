# Canonical (L1): the Anthropic Messages normalizer

`crosstalk-canonical` implements the spec's `Normalizer`
(`spec/types/interfaces/l1_canonical.rs`) for the Anthropic Messages wire
protocol and every dialect of it, roadmap item P2.5. It turns a
`RawExchange` (what the proxy hands capture) into a `NormalizedExchange`:
the canonical `Exchange`, its messages, and the warnings the spec defines.
Normalization is a set of pure functions; writing the bodies to the blob
store is a separate async step through the spec's `BlobStore`.

## Scope

- The canonical encoding of a message body and its `MessageHash` (the
  BLAKE3 of the encoding, the key the blob store computes), with a strict
  decoder (`encoding`).
- JSON with exact numbers and the spec's `CanonicalJson` text (`json`).
- Server-sent events parsed from a whole body (`sse`).
- The Anthropic Messages normalizer (`anthropic`): request bodies (system
  prompt as a string or blocks, messages, role splitting, tool calls and
  results, thinking, media, cache-control markers, unknown blocks),
  responses whole or streamed (reassembly from SSE events, including
  interleaved deltas, pings, `message_delta` usage and mid-stream errors),
  the outcome (stop reason, usage, failures), and the warnings.
- Assembling the `NormalizedExchange` (`assemble`): hashes, one copy of
  each body, the exchange's references, warnings.
- Storing a normalization's message bodies and media through `BlobStore`
  (`capture::store`).

## Non-scope

- Other protocols (OpenAI Chat, OpenAI Responses, Gemini, Code Assist) and
  WebSocket increments: roadmap P8. Their invariant evidence stays pending.
- The capture task: receiving `RawExchange`s from L0, calling `store`, and
  publishing `ExchangeCaptured` only after it succeeds
  (`canonical.capture.blobs-before-event`): the gateway's capture stage
  ([gateway](gateway.md), roadmap P3).
- Credential hashing, deployment secrets and ULID generation
  (`canonical.ids.*`): their evidence names this crate, but L0 and every
  minting layer need them and layer crates cannot depend on each other,
  so they belong in the spec or a shared crate (see Gaps).
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
      MessageSet: encoding::message(body) = { hash: BLAKE3(encode(body)), body }, once each
      Exchange { meta, continuation (copied), request: [hash], outcome }
  ─▶ Normalization { exchange: NormalizedExchange, media: [MediaBlob] }

capture::store(blobs, &normalization)   (async; awaited by the capture task)
  for each message: blobs.put(encode(body)) == message.hash, else HashMismatch
  for each media blob: blobs.put(bytes) == hash
```

`AnthropicMessages` implements `Normalizer` (its `normalize` returns the
`NormalizedExchange`), and `normalize_with_media` also returns the media
bytes, which `NormalizedExchange` has no place for.

### The canonical encoding

A body's encoding is the canonical JSON text (sorted keys, no whitespace,
RFC 8785 escapes) of its JSON shape, which follows the wire contract's
conventions:

| Body or part | JSON |
| --- | --- |
| `MessageBody` | `{"type": "system" \| "user" \| "assistant" \| "tool", "data": [parts]}` (a tool body's items are its results) |
| `Text`, `Reasoning::Visible` | `{"type": "text", "data": "<text>"}`, `{"type": "reasoning", "data": {"type": "visible", "data": "<text>"}}` |
| `Reasoning::Opaque` | `{"type": "reasoning", "data": {"type": "opaque", "data": {"signature": ".."}}}` |
| `Media` | `{"type": "media", "data": {"blob": "<hex>", "kind": "image" \| "audio" \| "document"}}` |
| `Unknown` | `{"type": "unknown", "data": {"kind": "..", "raw": "<canonical JSON text>"}}` |
| `ToolCall` | `{"type": "tool_call", "data": {"arguments": {"type": "json" \| "invalid", "data": ".."}, "execution": "client" \| "server", "id": "..", "name": ".."}}` |
| `ToolResult` | `{"call_id": "..", "content": [{"type": "text" \| "media" \| "unknown", "data": ..}], "outcome": "success" \| "error"}` |

Canonical JSON inside a body travels as a string, so its exact numbers
survive. `decode` accepts only what `encode` writes: canonical bytes of a
valid body, with canonical inner JSON and a non-empty tool body. The
pinned vectors are `crates/canonical/tests/golden/encoding/vectors.json`.

### Canonical JSON

`json::Json` keeps numbers as exact decimals (`json::Number`:
`±digits × 10^exponent`, no leading or trailing zero), so `1`, `1.0` and
`10e-1` are one value and an id beyond 2^53 keeps every digit. The text is
RFC 8785's: members sorted by UTF-16 code units, `JSON.stringify`
escapes, no whitespace; numbers in ECMAScript's `Number::toString` layout
applied to the exact decimal (plain up to 21 integer digits, `0.000…` down
to 10^-7, exponential beyond: `1e+30`, `1.5e-7`). The parser is strict
(RFC 8259, no unpaired surrogate escapes, at most 256 levels, exponents of
at most 30 digits), and a repeated member keeps its last value as
`JSON.parse` does. `serde_json` is not used for this because exact numbers
would need its `arbitrary_precision` feature, which changes number
handling for every crate in the workspace.

### Block mapping

| Block | Part |
| --- | --- |
| `text` | `Text` (`citations` dropped) |
| `image`, `document` with a `base64` source | `Media`; the decoded bytes are their own blob, hashed with BLAKE3 |
| `tool_result` in a user turn | a `ToolResult` in a `Tool` message: `content` a string, or `text` / `image` / `document` items; `is_error: true` is `Error` |
| `thinking` | `Reasoning::Visible` (the signature is dropped) |
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
| `reasoning` | `None`: thinking tokens are inside `output_tokens` |

Cache writes have no field of their own, so they count only inside
`input`. A missing or null cache count is 0. Usage is `None` when
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
| `crates/canonical/Cargo.toml` | Manifest: spec, base64, blake3, serde, serde_json, thiserror, tracing; dev: testkit, transport (`MemoryBlobStore`), proptest, tokio | — |
| `src/lib.rs` | Crate doc, modules, re-exports | `AnthropicMessages`, `Normalization`, `MediaBlob`, `store`, `StoreError` |
| `src/json/mod.rs` | JSON values with exact numbers; canonical text | `Json` (`parse`, `parse_bytes`, `get`, `kind`, `with`, `canonical`), `canonicalize`, `JsonError`, `MAX_DEPTH` |
| `src/json/number.rs` | Exact decimals and their canonical spelling | `Number` (`as_u64`, `canonical`), `MAX_EXPONENT_DIGITS` |
| `src/json/parse.rs` | The strict parser | `JsonError` |
| `src/json/write.rs` | The canonical writer (UTF-16 member order, escapes) | — |
| `src/encoding/mod.rs` | A body's canonical encoding and hash | `encode`, `decode`, `hash`, `hash_bytes`, `message`, `DecodeError` |
| `src/encoding/wire.rs` | The serde mirror of the message types (private) | — |
| `src/sse.rs` | Server-sent events from a whole body | `parse`, `SseEvent`, `SseBody` |
| `src/assemble.rs` | Building the normalization; warnings; media | `Normalization`, `MediaBlob` |
| `src/capture.rs` | Storing bodies and media through `BlobStore` | `store`, `StoreError` |
| `src/anthropic/mod.rs` | The normalizer | `AnthropicMessages`, `normalize` |
| `src/anthropic/request.rs` | Request bodies to messages | `RequestError` |
| `src/anthropic/blocks.rs` | The block mapping (private) | — |
| `src/anthropic/response.rs` | Outcomes, whole bodies, stop reasons | `stop_reason` |
| `src/anthropic/stream.rs` | Stream reassembly | — |
| `src/anthropic/usage.rs` | The usage mapping | — |
| `src/tests/mod.rs` | The evidence entry points (`crosstalk_canonical::tests::<name>`) | — |
| `src/tests/units/`, `src/tests/props.rs` | Unit and property bodies | — |
| `src/tests/generate/` | Generators: JSON spellings, blocks, turns, requests, whole and streamed responses with random delta cuts, pings, interleaving and CRLF framing, message bodies | — |
| `src/tests/golden.rs`, `src/tests/support.rs` | The golden harness; raw exchanges from bodies and corpus cases | — |
| `tests/golden/anthropic/<case>.json` | Each captured corpus case's normalization | — |
| `tests/golden/encoding/vectors.json` | The pinned encodings | — |

Goldens are rewritten with `CROSSTALK_BLESS=1 cargo test -p
crosstalk-canonical golden` (review the diff). A normalization golden is
`{"exchange": <the spec's Exchange JSON>, "messages": [{"hash", "body"}],
"warnings": [..], "media": [{"hash", "base64"}]}`, each body the message's
encoding as JSON; checking also decodes the file back.

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
- No `unwrap` or `expect` outside tests except the two infallible ones in
  `encoding::encode`, each with its reason.
- The crate is a layer crate: it depends on the spec and third-party
  crates only; testkit and transport are dev-dependencies.

Implementation evidence in this crate now passes for: INV-47, 49, 50, 51,
59, 60, 61, 62, 67, 68, 69, 70 (property; its OpenAI Chat units are P8),
72, 73, 74, 77, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 95, 96, 97, 98, 99.
INV-48's evidence moved to the gateway, where the capture task lives, and
passes there ([gateway](gateway.md)). Pending: 53–58 (keyed hashing and
ids, see Gaps), 66, 71, 75, 78 (other protocols and WebSocket, P8), 76 (fuzz).

## Gaps found

- **Decoding bodies outside L1.** Consumers in L3, L4 and L8 read message
  bodies back from the blob store, but the encoding lives here and layer
  crates cannot depend on each other, and the wire contract keeps the
  message types free of serde. The decoder (`encoding::decode`) should
  move into the spec, or a shared crate, before P4.
- **Media bytes.** `NormalizedExchange` holds no media bytes, though a
  `Media` part names a blob L1 must store; `Normalization` carries them.
- **Unrepresentable content.** Media given by URL or Files API id has no
  bytes to hash, so it is kept as `Unknown`; web search results and web
  fetch documents have no `ToolResultContent` variant and are `Unknown`
  items, so their text is not indexable. `TokenUsage` has no field for
  cache writes.
- **`cache_control` in `Unknown`.** The spec says an `Unknown` part holds
  the block's canonical JSON; the marker is dropped from it so that echo
  stability and request concatenation hold for messages with unknown
  blocks.
- **`canonical.ids.*` placement.** The keyed hasher, `DeploymentSecret` and
  ULID generators are needed by L0 and every minting layer, which cannot
  depend on this crate.
