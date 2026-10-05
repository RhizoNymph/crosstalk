# Recorded-traffic corpus

Request and response pairs that tests replay through the fake upstream
(`crosstalk_testkit::upstream`) and the fake harness
(`crosstalk_testkit::client`), and that the proxy (L0) and normalizers (L1)
are tested against.

**Every case here is synthetic.** None was captured from a running
harness. Each was written from the documents its `meta.json` lists under
`provenance.data.sources`: the harness research in
`docs/research/harness-wire-protocols.md`, Claude Code's gateway protocol
page, and Anthropic's streaming, errors and rate-limit API pages. They
follow those documents as closely as the documents allow, and each
`meta.json` says in `notes` where a detail is approximated (the system
prompt's attribution block text, the `anthropic-ratelimit-unified-*`
suffixes, thinking signatures). Ids (`msg_…`, `toolu_…`, `req_…`) are
random but well formed.

## Layout

```text
corpus/
  anthropic/messages/<case>/
    request.http    the request: start line, headers, blank line, body
    response.http   the response: status line, headers, blank line, body
    meta.json       what the case is and what a correct pipeline makes of it
```

`crosstalk_testkit::corpus::anthropic::cases()` loads every case, sorted by
name, and checks it against its own `meta.json`; a case that loads is
consistent.

## File formats

`request.http` and `response.http`:

- The head is a start line (`POST /v1/messages?beta=true HTTP/1.1`,
  `HTTP/1.1 200 OK`), then one `name: value` line per header, in the order
  sent, names lower case; lines end with LF.
- One empty line ends the head. The body follows **verbatim** to the end of
  the file: nothing is added or trimmed, so JSON bodies have no trailing
  newline, and an event stream keeps its exact `event:` / `data:` lines
  and blank-line framing (including `ping` events, whose data the API
  writes as `{"type": "ping"}` with a space).
- Framing and hop-by-hop headers are never recorded: `host`,
  `content-length`, `transfer-encoding`, `connection`, `keep-alive`,
  `proxy-connection`, `te`, `trailer`, `upgrade`, and `date`. The sender
  sets them for its own connection, and comparisons ignore them. The
  loader refuses a file that records one.
- A response is an event stream exactly when its `content-type` is
  `text/event-stream` (the spec's `ResponseHead::framing`), whatever the
  request's `stream` flag says: a 429 to a streaming request is JSON.

`meta.json` (strict: unknown fields are refused):

| Field | Meaning |
| --- | --- |
| `description` | What the case shows |
| `provenance` | `{"type": "synthetic", "data": {"sources": [..]}}`, or `{"type": "captured", "data": {"captured_on": "YYYY-MM-DD", "harness_version": ".."}}` |
| `endpoint` | `generation` (with `model`, `stream` and `expect`), `token_count`, `model_list` or `probe`: the spec's `EndpointKind`; only generation is captured |
| `endpoint.data.expect` | `completed` with `stop` (the spec's `StopReason`), `response_id` and the response's content `blocks` in order; or `failed` with `failure` (the spec's `ExchangeFailure`) and the `partial_blocks` that started first |
| `credential` | `header`, `scheme` (the spec's `CredentialScheme`) and the `placeholder` that replaced the secret; `null` when the request has none |
| `harness` | `claim_headers`: the request headers that are harness claims (client assertions, never identity evidence alone); `claim`, `ids`, `class`: the spec's `HarnessClaim`, `HarnessIds` and `RequestClass` those headers carry (`unknown` without `x-claude-code-request-class`) |
| `follows` | The case whose response this request continues, or `null` |
| `notes` | Anything approximated or worth knowing |

## Cases

| Case | Shows |
| --- | --- |
| `text_turn` | A plain text turn, not streamed |
| `text_turn_streaming` | A plain text turn, streamed, with a ping inside the block |
| `tool_use_streaming` | Text then a `Read` tool_use whose input arrives as `input_json_delta` fragments |
| `tool_result_followup` | The next request: history with the tool_use echoed and the tool_result in a user turn |
| `multi_block_assistant` | Text and two parallel tool_use blocks, not streamed |
| `system_cache_control` | The attribution block, then system blocks with `cache_control` and a 1h `ttl` paired with the `extended-cache-ttl` beta; a cache write in `usage` |
| `system_turn_streaming` | Claude Code's system turn: the top-level `system` array, then a user turn and a `role: "system"` entry at `messages[1]` (a reminder block with `cache_control`), which the upstream accepts |
| `thinking_streaming` | Adaptive thinking: `thinking_delta`s closed by a `signature_delta`, then text |
| `overloaded_mid_stream` | An `overloaded_error` event after two text deltas, then the body ends |
| `rate_limited` | 429 `rate_limit_error` with `retry-after: 17` and `x-should-retry` |
| `unauthorized` | 401 `authentication_error` |
| `count_tokens` | `POST /v1/messages/count_tokens?beta=true`: not captured |
| `models_list` | Gateway model discovery, `GET /v1/models?limit=1000`: not captured |
| `hello_probe` | The `HEAD /api/hello` warm-up probe, without a credential |
| `subagent_oauth_streaming` | A sub-agent on a subscription: OAuth bearer and beta, the session id plus `x-claude-code-agent-id`, gateway hint headers, `anthropic-ratelimit-unified-*` response headers |

Every generation request has Claude Code's shape: `anthropic-version`,
`anthropic-beta` (with `claude-code-20250219`), `anthropic-dangerous-direct-browser-access`,
`user-agent: claude-cli/2.1.282 (external, cli)`, `x-app: cli`,
`x-claude-code-session-id`, the Anthropic SDK's `x-stainless-*` headers,
and a body with the three-block `system` array, the tools, `metadata.user_id`,
`max_tokens`, `thinking: {"type": "adaptive"}` and `stream`.

## Adding real captures

Capture with the proxy's raw tee, or with any intercepting proxy, against a
harness pointed at it through its base URL. Then, for each exchange:

1. **Split** it into `request.http` and `response.http` in the format above:
   lower-case header names, drop the framing headers, keep the body bytes
   exactly (do not re-indent JSON or re-frame events).
2. **Redact credentials.** Replace the secret inside `x-api-key` or
   `authorization` with a placeholder of the same kind
   (`sk-ant-api03-REDACTED`, `sk-ant-oat01-REDACTED`) and declare it in
   `meta.json`'s `credential`. Also scrub any secret in custom headers
   (`ANTHROPIC_CUSTOM_HEADERS`) and in `anthropic-organization-id`.
3. **Redact user ids.** Replace `metadata.user_id`'s user hash, account uuid
   and session uuid, the `x-claude-code-session-id`, agent and prompt id
   headers, and every other account or device identifier with fixed
   placeholders (`0…01`, `00000000-0000-4000-8000-000000000001`). Use one
   placeholder per original value across the whole capture, so the same
   session still has the same id in every request and header.
4. **Redact file contents.** Replace file contents, command output and
   anything else read from the user's machine (in `tool_result` blocks, in
   the system prompt's environment section, in tool_use inputs) with
   synthetic text of the same byte length. Replace each distinct original
   text consistently, so text one agent wrote and another read still
   matches after redaction: provenance tests depend on it.
5. **Write `meta.json`** with `provenance: {"type": "captured", ..}`, the
   harness version, and the expected outcome read from the response.
6. **Run the loader** (`cargo test -p crosstalk-testkit corpus`): it refuses
   a case whose placeholder is missing, whose claim headers are absent, or
   whose body, framing or outcome disagree with its metadata.

Never commit an unredacted capture, even temporarily: the corpus is part of
the repository's history.
