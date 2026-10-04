# Harness and server wire protocols

What each supported harness and inference server sends over the wire, as of
2026-10-03. This is the basis for the client, upstream, credential and
transport types in `spec/types/observed/client.rs` and
`spec/types/observed/exchange.rs`, and for the L0 ingress interface.

Sources were read at these revisions:

| Project | Revision |
| --- | --- |
| Claude Code | binary 2.1.282, plus [LLM gateway](https://code.claude.com/docs/en/llm-gateway), [gateway protocol](https://code.claude.com/docs/en/llm-gateway-protocol) and [network config](https://code.claude.com/docs/en/network-config) docs |
| [openai/codex](https://github.com/openai/codex/tree/550eb50545a78468a09ce86b82426338640ef22d) | `550eb505` |
| [badlogic/pi-mono](https://github.com/badlogic/pi-mono/tree/76dfb88f63ce51ff2e3fe2ead4fcf1f65f71f121) | `76dfb88f` |
| [can1357/oh-my-pi](https://github.com/can1357/oh-my-pi/tree/6d8552d7f9df1852826923f07f0eed4fe29511f3) | `6d8552d7` |
| [vllm-project/vllm](https://github.com/vllm-project/vllm/tree/84bcbc62644356270aaaa5e2d0237d03adc9bb3a) | `84bcbc62` |
| [sgl-project/sglang](https://github.com/sgl-project/sglang/tree/4ab720e6557b44d07bde471ed52a178795298f1a) | `4ab720e6` |

## Summary

| Harness | Auth mode | Wire API | Host / path | Credential | Base URL redirectable |
| --- | --- | --- | --- | --- | --- |
| Claude Code | API key | Anthropic Messages | api.anthropic.com `/v1/messages?beta=true` | `x-api-key` | yes, `ANTHROPIC_BASE_URL` |
| Claude Code | `ANTHROPIC_AUTH_TOKEN` / `apiKeyHelper` | Anthropic Messages | same | `Authorization: Bearer` | yes |
| Claude Code | Pro/Max OAuth | Anthropic Messages | same; refresh at platform.claude.com `/v1/oauth/token` | `Authorization: Bearer`, beta `oauth-2025-04-20` | inference yes; refresh, profile, telemetry need `HTTPS_PROXY` |
| Codex | API key | Responses (WebSocket by default, SSE fallback) | api.openai.com `/v1/responses` | `Authorization: Bearer` | yes, `openai_base_url` |
| Codex | ChatGPT | Responses, `store: false` | chatgpt.com `/backend-api/codex/responses`; refresh auth.openai.com | Bearer + `ChatGPT-Account-ID` | yes, `openai_base_url` ending `/backend-api/codex` |
| pi | API keys | per provider | per provider | per provider | yes, `models.json` |
| pi | Claude OAuth | Anthropic Messages, impersonating Claude Code | api.anthropic.com | Bearer `sk-ant-oat…` | yes |
| pi | Codex (legacy) | Responses, WebSocket first | chatgpt.com `/backend-api/codex/responses` | Bearer + `chatgpt-account-id` | yes |
| pi | Copilot | Messages / Chat / Responses per model | host from the token's `proxy-ep` | exchanged Copilot token | no: needs `HTTPS_PROXY` with interception |
| oh-my-pi | Claude OAuth | Anthropic Messages, closer impersonation | api.anthropic.com | Bearer `sk-ant-oat…` | yes; `PI_PROXY` for interception |
| oh-my-pi | Codex | Responses, WebSocket | chatgpt.com `/backend-api/codex/responses` | Bearer + `chatgpt-account-id` | yes |
| oh-my-pi | Gemini CLI / Antigravity | Code Assist `v1internal:streamGenerateContent` | cloudcode-pa.googleapis.com | Google OAuth Bearer | yes |
| vLLM | optional `--api-key` | Chat, Completions, Responses, Anthropic Messages | `/v1/*` | Bearer, only on `/v1`, `/v2`, `/inference`, `/cohere` | upstream |
| SGLang | optional `--api-key` | Chat, Completions, Responses, Anthropic Messages | `/v1/*` | Bearer on every route but health and metrics | upstream |

## Findings that shaped the types

### Identity

- **Harness ids.** Claude Code sends `X-Claude-Code-Session-Id` on every
  request, and `x-claude-code-agent-id` (plus `x-claude-code-parent-agent-id`
  for nested agents) on sub-agent requests. Codex sends `session-id` (the
  root thread) and a per-agent `thread-id`, with `x-codex-parent-thread-id`
  and `x-openai-subagent` on sub-agents. pi sends `session_id`.
- **Impersonation.** pi and oh-my-pi send Claude Code's User-Agent, `x-app`
  and beta headers on Claude subscription traffic; oh-my-pi also sends
  `X-Claude-Code-Session-Id`. Harness headers are therefore claims, scoped to
  the credential or account they arrive with (`IdentityScope`).
- **Rotating credentials.** Subscription access tokens are refreshed by the
  harness directly with the vendor's auth host (platform.claude.com,
  auth.openai.com), not through the inference base URL. A token hash cannot
  identify an agent across a refresh (`Stability::Rotating`).
- **No credential.** vLLM and SGLang may run without a key, or with one
  shared key (`CredentialScheme::ServerKey`).

### Transport

- **Codex uses WebSocket by default** (`wss://{base}/responses`, handshake
  header `OpenAI-Beta: responses_websockets=2026-02-06`). Each turn is a
  `response.create` frame with `previous_response_id` and only the new input
  items. Over HTTP/SSE it sends the full input every turn. Custom providers
  default to no WebSocket.
- **Codex request bodies can be zstd-compressed** (on by default for ChatGPT
  auth).
- **Claude Code expects** unbuffered streaming, SSE pings kept, and
  `anthropic-*`, `anthropic-ratelimit-unified-*`, `retry-after` and
  `x-should-retry` headers forwarded unchanged. Stripping the
  `oauth-2025-04-20` beta fails subscription requests with 401.

### Non-generation traffic

- Claude Code sends `count_tokens`, a `HEAD /api/hello` warm-up and (opt-in)
  model discovery to the base URL; telemetry, OAuth profile and usage, and
  WebFetch preflight always go to api.anthropic.com.
- Codex calls `{base}/models` and other side routes on its base URL;
  compaction is an ordinary `/responses` call with a trailing
  `compaction_trigger` item.
- vLLM and SGLang also serve tokenize, health and metrics routes.

These are `EndpointKind`s other than `Generation`: forwarded, not captured.

### Hints

- Claude Code can send `x-claude-code-request-class` (main, subagent,
  workflow, compaction, auxiliary) and related hint headers, off by default
  behind a custom base URL; `CLAUDE_CODE_GATEWAY_HINT_HEADERS=1` turns them
  on. Its body also carries a per-conversation attribution block.
- Codex marks sub-agent and compaction requests with `x-openai-subagent`.

### Dialects

| | vLLM | SGLang |
| --- | --- | --- |
| Reasoning field | `reasoning` (`reasoning_content` accepted on input) | `reasoning_content`, null when empty in stream deltas |
| Tool call ids | `chatcmpl-tool-<uuid>` | `call_<24hex>` |
| Named tool choice finish reason | `stop` | `tool_calls` |
| Other finish reasons | | `abort` |
| Extra stream chunks | | `sglext` chunks, `event: sglext_ids` |
| Responses `previous_response_id` | needs `VLLM_ENABLE_RESPONSES_API_STORE=1` | needs `--enable-response-store` |

### Sub-agents

- Claude Code sub-agents run in-process with the same credential and session
  id, distinguished by agent id headers.
- Codex sub-agents share `session-id`, with distinct `thread-id`s.
- pi has no core sub-agents; its example extension spawns a separate `pi`
  process with a new session id and no marker header, so parent and child
  are related only through content.
- oh-my-pi's in-process `task` sub-agents use their own session ids and no
  marker header.

## Not verified

- Claude Code's OAuth access-token lifetime.
- Whether a Claude Code agent-team teammate gets its own session id.
- The vLLM release that renamed `reasoning_content` to `reasoning`.
- An environment variable equivalent of SGLang's `--api-key`.
- pi's custom-CA handling end to end, and whether its Codex WebSocket honours
  the proxy under Node.
- Whether oh-my-pi's Claude OAuth traffic bypasses `HTTPS_PROXY`.
