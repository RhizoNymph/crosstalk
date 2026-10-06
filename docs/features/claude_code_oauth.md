# Claude Code on a subscription (OAuth)

Capturing Claude Code when it runs on a Claude Pro or Max subscription,
which authenticates with an OAuth access token instead of an API key. The
harness points `ANTHROPIC_BASE_URL` at the gateway and keeps its claude.ai
login. The gateway forwards the traffic unchanged, captures the generation
exchanges, and hashes the token on arrival. The raw token is never stored.

Status: design (phase 1). Nothing below is implemented by this document.
Most of the L0 and L3 machinery it relies on already exists
([ingress](ingress.md), [reconstruct](reconstruct.md)). Phase 2 adds the
hardening and tests listed under [Work](#work-phase-2).

## Scope

- The Anthropic Messages reverse-proxy route (`/anthropic` →
  `https://api.anthropic.com`) carrying `Authorization: Bearer <OAuth
  access token>` together with the OAuth capability in `anthropic-beta`.
- Classifying that credential as `CredentialScheme::OauthAccessToken`
  (`Stability::Rotating`) and keeping its keyed digest, never its text.
- Keeping one agent per Claude Code session across token refreshes, which
  the proxy never sees.
- Tests built on fake tokens only. They cover byte-for-byte passthrough,
  capture, classification, identity across a simulated refresh, and the
  raw token's absence from every output.

## Non-scope

- Token refresh, login and revocation. They go to `platform.claude.com`
  and `claude.ai` directly, never through `ANTHROPIC_BASE_URL`, and the
  gateway never handles them (see [Endpoints](#endpoints)).
- Forward-proxy (`HTTPS_PROXY`) interception of `api.anthropic.com`. That
  is roadmap P8; the notes below record what P8 must respect.
- Holding, minting or injecting credentials. The gateway never
  authenticates on a user's behalf. It forwards the harness's own token
  and nothing else.
- Deriving an account from the request body (`metadata.user_id`). See
  [Open questions](#open-questions).
- `ANTHROPIC_AUTH_TOKEN`, `ANTHROPIC_API_KEY` and `apiKeyHelper` setups.
  These already work. When one is set, Claude Code sends that credential
  instead of the subscription login.

## What Claude Code sends (researched)

Sources: [authentication], [llm-gateway], [llm-gateway-protocol] and
[network-config] (code.claude.com docs, read 2026-10-05), plus
`docs/research/harness-wire-protocols.md` (binary 2.1.282).

[authentication]: https://code.claude.com/docs/en/authentication
[llm-gateway]: https://code.claude.com/docs/en/llm-gateway
[llm-gateway-protocol]: https://code.claude.com/docs/en/llm-gateway-protocol
[network-config]: https://code.claude.com/docs/en/network-config

Documented:

- **Credential precedence** ([authentication]): cloud provider >
  `ANTHROPIC_AUTH_TOKEN` (sent as `Authorization: Bearer`) >
  `ANTHROPIC_API_KEY` (`X-Api-Key`) > `apiKeyHelper` >
  `CLAUDE_CODE_OAUTH_TOKEN` (a one-year subscription token from `claude
  setup-token`) > Anthropic profiles > the subscription login from
  `/login`. The subscription is used only when nothing above it is set.
- **Base URL with a subscription** ([llm-gateway], "Subscriptions and
  gateways"): setting only `ANTHROPIC_BASE_URL` does not replace the
  subscription. Requests go through the gateway, the claude.ai login stays
  the active credential, and its usage limits and billing still apply.
  Gateways that pass this traffic on to Anthropic must forward the OAuth
  capability in `anthropic-beta`.
- **Headers** ([llm-gateway-protocol], "Request headers"): forward
  `anthropic-version` (`2023-06-01`) and `anthropic-beta` verbatim. Do not
  allowlist individual values. With a claude.ai login, `anthropic-beta`
  also carries an OAuth capability the upstream requires, and stripping it
  fails the request with 401. `CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS`
  keeps that value. The doc also lists `x-claude-code-session-id` on every
  request, `x-claude-code-agent-id` on sub-agent requests and
  `x-claude-code-parent-agent-id` on nested agents. Hint headers
  (`x-claude-code-request-class` and others) are off behind a custom base
  URL unless `CLAUDE_CODE_GATEWAY_HINT_HEADERS=1` is set.
- **Endpoints** ([llm-gateway-protocol]): `POST /v1/messages?beta=true`,
  optional `POST /v1/messages/count_tokens`, a best-effort `HEAD
  /api/hello` warm-up, and an opt-in `GET /v1/models?limit=1000` (model
  discovery). The fast-mode check and the WebFetch domain safety check
  call `api.anthropic.com` directly.
- **Responses** ([llm-gateway-protocol]): stream unbuffered, keep `ping`
  events, relay `retry-after`, `x-should-retry` and
  `anthropic-ratelimit-unified-*` unchanged, and leave error bodies
  unmodified. Claude Code uses the unified rate-limit headers to show plan
  usage to claude.ai users.
- **Hosts** ([network-config]): `platform.claude.com` handles OAuth token
  exchange, refresh and revocation for claude.ai accounts. `claude.ai` and
  `claude.com` handle sign-in. `api.anthropic.com` also serves feature
  flags and telemetry.
- **Login lifetime** ([authentication]): Claude Code warns three days
  before the stored login expires. Once it expires and cannot be
  refreshed, requests fail with "Login expired".

Observed, not documented (harness research, binary 2.1.282):

- Access tokens start with `sk-ant-oat` (`sk-ant-oat01-…`). The OAuth beta
  value is `oauth-2025-04-20`.
- The User-Agent is `claude-cli/<version> (external, cli)`, sent with
  `x-app: cli`. pi and oh-my-pi send the same values, so neither one
  proves the client is Claude Code.
- The request body's `metadata.user_id` names the session, and on a
  subscription also the account.

Unknown: the access-token lifetime, and therefore how often the hash
changes within a session.

A background research summary claimed that the Messages API rejects OAuth
tokens. No official document says so, and [llm-gateway] describes this
exact path as supported. That claim is not used in this design.

## Today's behaviour, layer by layer

Nothing in the current code breaks a Bearer-authenticated Anthropic
request. The gaps are fragility and missing end-to-end proof.

| Concern | Today | Gap |
| --- | --- | --- |
| Routing | Path prefix only (`routing.rs`); headers never steer it | none |
| Forwarding | Only hop-by-hop fields and `Host` are removed (`proxy/headers.rs`). `authorization`, `anthropic-beta`, `anthropic-version` and the `?beta=true` query go upstream byte for byte. The passthrough property test already generates `Bearer` with `claude-code-20250219,oauth-2025-04-20` (`tests/passthrough.rs`) | none |
| Response relay | Frame by frame. `ping` events, `anthropic-ratelimit-unified-*`, `retry-after`, `x-should-retry` and error bodies are passed unchanged | none |
| Classification | `POST /v1/messages` is `Generation`. `count_tokens`, `/api/hello` and `/v1/models` are forwarded but not captured. Anything unclassified is still forwarded (and counted) | none |
| Credential read | `Authorization` takes precedence over `x-api-key`. A `Bearer` token is read as a `RawCredential` borrow and hashed with the `KeyedHasher` (BLAKE3 keyed by the deployment secret) at the exchange's start (`identify.rs`, `credential.rs`) | none |
| Scheme | On a `VendorApi` route, `Bearer` + shape `sk-ant-oat…` gives `OauthAccessToken`, and any other `Bearer` gives `ApiKey` | **Fragile.** If Anthropic changes the token prefix, OAuth tokens become `ApiKey` (`Stable`). L3 would then scope harness ids by credential hash, so every refresh would start a new agent. The documented signal, the OAuth capability in `anthropic-beta`, is ignored |
| Decode head | `without_credentials` removes `authorization`, `x-api-key` and the other credential fields before the decoder sees the head | none |
| RawExchange / bus / blobs / exchange log | Carry no request head. `ClientContext` carries only `CredentialRef { scheme, hash }` (INV-12) | not proven end to end for a subscription session |
| Logs | Ingress and gateway logs carry no headers, queries or bodies (`gateway/src/logging.rs` header; ingress `tracing` call sites) | none |
| L3 scope | `scope_of`: account, else a stable credential, else the upstream. `OauthAccessToken` is `Rotating`, so harness ids are scoped by the upstream | see [Identity](#identity-across-refresh) |
| L3 evidence | `RotatingCredential(hash)` has specificity 0: it is attached to the agent but never decides when a session or agent id is present | none |
| L1 / canonical | No vendor or credential logic. The dialect comes from the upstream kind (`Reference` for both `VendorApi` and `Subscription` Anthropic) | none |
| e2e scenario | `crates/e2e/src/scenario/wire.rs` sends `x-api-key` only | no subscription-shaped session |
| Reconstruct tests | Rotating credentials appear in the evidence and property tests | no named test that a refresh inside one session keeps the agent |

## Design

### Endpoints

| Traffic | Host | Through the gateway? | Gateway action |
| --- | --- | --- | --- |
| `POST /v1/messages?beta=true` | base URL | yes | forward unchanged, capture (`Generation`) |
| `POST /v1/messages/count_tokens` | base URL | yes | forward unchanged, not captured (`TokenCount`) |
| `HEAD /api/hello` | base URL | yes | forward unchanged (`Probe`) |
| `GET /v1/models?limit=1000` (opt-in) | base URL | yes | forward unchanged (`ModelList`). This request carries credentials, so it gets the same hashing and redaction as any other |
| any other path under the route | base URL | yes | forward unchanged, counted `unclassified` |
| token exchange, refresh, revocation | `platform.claude.com` | no | not touched. In reverse mode the gateway never sees it. In forward mode it is in `InterceptAllowlist::AUTH_HOSTS` and must only be tunnelled (INV-37) |
| login pages | `claude.ai`, `claude.com` | no | not touched |
| feature flags, telemetry, fast-mode check, WebFetch preflight | `api.anthropic.com` | no (reverse mode) | not touched. Under P8 interception these requests carry the same bearer token. They must be forwarded uncaptured with the same hashing and redaction |

The gateway never sends a request of its own. It never retries with a
token and never calls a refresh endpoint. INV-27 (no originated requests)
already enforces this.

### Passthrough

These must reach the upstream unchanged, byte for byte: the method, the
path under the prefix, the query (`beta=true`), every end-to-end header
(`authorization`, `anthropic-beta`, `anthropic-version`,
`anthropic-dangerous-direct-browser-access`, `x-app`, `user-agent`,
`x-claude-code-*`, `x-stainless-*` and any header added through
`ANTHROPIC_CUSTOM_HEADERS`) and the body. The response goes back the same
way: status, end-to-end headers and body. This is the existing INV-29 and
INV-30 rule. No header is allowlisted, rewritten or added.

### Credential classification

The scheme rule gains one documented input: the OAuth capability in
`anthropic-beta`.

| Upstream kind | Credential | Scheme |
| --- | --- | --- |
| `VendorApi(Anthropic)` or `Subscription(Anthropic)` | `Authorization: Bearer`, and some comma-separated `anthropic-beta` value starts with `oauth-` | `OauthAccessToken` |
| `VendorApi(_)` | `Bearer` shaped `sk-ant-oat…` or a JWT | `OauthAccessToken` (unchanged) |
| everything else | as today | as today |

- The rule reads the header only. It parses the comma-separated values,
  trims each one, and matches the `oauth-` prefix without regard to case.
  It does not pin `oauth-2025-04-20`, because the docs say not to
  allowlist beta values.
- A key header (`x-api-key`) with the OAuth beta stays `ApiKey`. Only a
  bearer token can be the subscription login.
- The shape rule is kept. pi and oh-my-pi send `sk-ant-oat…` and may omit
  the beta value.
- Either signal is enough. A false `OauthAccessToken` costs only
  scoping, because the harness ids then fall back to the upstream scope. A
  false `ApiKey` splits one agent at every refresh. Leaning toward
  `OauthAccessToken` is therefore the safer error.

In code, `HeaderIdentifier::scheme(source, raw, kind)` becomes
`scheme(source, raw, kind, oauth_beta: bool)`, with `oauth_beta` computed
from the head by a new `identify::oauth_capability(head)`. A
`CredentialHints` struct is an alternative if more head-derived inputs
appear. The function still reads only the token's shape, never its
content.

### Hashing (unchanged)

- The token is borrowed as `RawCredential<'_>` from the header and hashed
  at once with `KeyedHasher::credential(raw.bytes(), started_at)`:
  BLAKE3 keyed with the current `DeploymentSecret`, plus the previous
  version during a rotation overlap. Only `CredentialHash` digests leave
  the hot path.
- `RawCredential` has no `Display` or `Serialize`, and its `Debug`
  prints `<redacted>`. `SecretError` names environment variables, never
  values. No new type holds the token.
- A digest depends only on the secret and the token text (INV-15,
  `hash-depends-only-on-credential`). So every refreshed token gets a new
  digest, and the same token gets the same digest on every node.

### Identity across refresh

The proxy never sees a refresh. It sees token A, and later token B, under
the same `x-claude-code-session-id`.

- `OauthAccessToken` is `Rotating`, so `scope_of` never scopes by the
  token. With no account header on Anthropic traffic, the scope is the
  upstream (`IdentityScope::Upstream`).
- `HarnessSession` (specificity 4) and `HarnessAgent` (5) decide
  resolution. `RotatingCredential` (0) never does when either is present.
- Before the refresh, the exchange's evidence is {session S,
  rotating(hash A)}, which creates or resolves agent X. After it, the
  evidence is {session S, rotating(hash B)}. S decides, so the exchange
  resolves to X, and X gains `RotatingCredential(hash B)`. One agent per
  session, whatever the token.
- Sub-agents carry the session plus `x-claude-code-agent-id`, which
  decides, and their parent comes from the parent id or the session's
  main agent. Refresh does not affect this.
- Two terminals on one login have different sessions and the same token
  hash, so they are two agents. Their shared rotating hash never makes
  them conflict.
- A request with no harness ids (for example a script on
  `CLAUDE_CODE_OAUTH_TOKEN` that sends no session header) resolves by
  `RotatingCredential` until the next refresh, and by prompt fingerprint
  after it. That matches what the spec already says for rotating
  credentials. A `setup-token` token lasts a year, so in practice the
  credential is stable there.

Proposal: no new identity mechanism. Session continuity is the identity
anchor, which is what `Stability::Rotating` already encodes. Phase 2 only
proves it.

### Logging and redaction

- No change to what is logged. Ingress and gateway logs carry the
  exchange id, path (only on 421), reasons and error kinds. They never
  carry headers, queries or bodies.
- The tests capture `tracing` output while fake tokens flow through the
  proxy and assert that no token substring appears in the logs, in any
  error `Display` or `Debug`, in the stored blobs, in bus envelopes, in the
  exchange log or in L8 API responses.
- Request bodies are stored as received. The body carries no credential,
  but on a subscription `metadata.user_id` may include an account UUID.
  See [Open questions](#open-questions).

## Data and control flow

```text
Claude Code (claude.ai login, ANTHROPIC_BASE_URL=http://gw:8080/anthropic, no API key vars)
  │ POST /anthropic/v1/messages?beta=true
  │ Authorization: Bearer sk-ant-oat…   anthropic-beta: …,oauth-2025-04-20,…
  │ x-claude-code-session-id: S
  ▼
L0 Proxy::handle ─ Routes::resolve(path) ─ strip hop-by-hop + Host
  │ classify → Generation
  │ HeaderIdentifier::context(head, upstream, started_at)
  │   raw_credential → (Bearer, RawCredential<'_>)          (borrow, never stored)
  │   scheme(Bearer, raw, VendorApi(Anthropic), oauth_beta) → OauthAccessToken
  │   KeyedHasher::credential(raw, started_at) → CredentialHash (current [, previous])
  │ forward unchanged ─────────────────────────────────▶ api.anthropic.com
  │ relay SSE frame by frame (pings, ratelimit headers) ◀─
  │ decode head = without_credentials(head)
  ▼
RawExchange { meta.client: ClientContext { credential: CredentialRef{OauthAccessToken, hash}, ids: {session S} } }
  ▼ L1 normalize, blobs, ExchangeCaptured
L3 caller_evidence → [RotatingCredential(hash)], HeaderEvidence → [HarnessSession(Upstream, S)]
  resolve: session decides → agent X (same after refresh: hash B attaches to X)

Refresh (never through the gateway):
Claude Code ──HTTPS──▶ platform.claude.com /v1/oauth/token   (refresh token never seen by crosstalk)
```

## Files

Phase 2 touches only these files. All paths are relative to the repo
root.

| File | Role | Change |
| --- | --- | --- |
| `crates/ingress/src/identify.rs` | `HeaderIdentifier`, the scheme rule | add `oauth_capability(head)` and pass it to `scheme`; update the rule table in the module doc |
| `crates/ingress/src/credential.rs` | `RawCredential`, `TokenShape` | unchanged (shape rule kept) |
| `crates/ingress/src/tests/credential.rs` | scheme rule tests | add rows for the new rule: `Bearer` with an opaque token plus the OAuth beta gives `OauthAccessToken`; `x-api-key` plus the OAuth beta gives `ApiKey`; non-OAuth beta values do not trigger it; case and whitespace variants. Existing rows are unchanged |
| `crates/ingress/src/tests/subscription.rs` (new) | socket test | a Claude Code subscription-shaped streamed exchange through the proxy against `FakeUpstream`: the upstream sees the head and body unchanged, the client sees `anthropic-ratelimit-unified-*` and pings unchanged, one `RawExchange` with `OauthAccessToken`, and no token bytes in it or in captured logs |
| `crates/testkit/src/…` (corpus or harness builders) | fixtures | a subscription session builder: fake `sk-ant-oat01-TEST…` tokens, a token switch mid-session to simulate a refresh, and beta values with `oauth-2025-04-20` |
| `crates/e2e/src/scenario/wire.rs` | e2e Claude Code wire | an `Auth::{ApiKey, Subscription}` choice on the session. Subscription sends `Authorization: Bearer` plus the OAuth beta and no `x-api-key` |
| `crates/reconstruct/src/tests/consumer.rs` | L3 unit tests | refresh inside a session keeps one agent; two sessions on one token give two agents and no conflict |
| `crates/gateway/src/…/tests` (Live composition) | end to end in process | a subscription session with a refresh: one agent, its conversation threaded across the refresh, and no fake-token substring in the blobs, bus envelopes, exchange log or L8 responses |
| `spec/invariants/INV-X-*.toml` | invariants | see below |
| `docs/features/ingress.md`, `docs/features/claude_code_oauth.md`, `docs/OVERVIEW.md`, `docs/features/deploy.md` | docs | the scheme rule, this doc's status, and the operator note: run Claude Code with only `ANTHROPIC_BASE_URL` set, since `ANTHROPIC_AUTH_TOKEN` or `ANTHROPIC_API_KEY` would replace the subscription |

## Invariants and constraints

Existing invariants this feature depends on (unchanged):

- INV-12 `ingress.credential.absent-from-raw-exchange`, INV-14
  `ingress.credential.every-scheme-hashed`, INV-15 hash depends only on
  the credential.
- INV-17 `ingress.credential.scheme-follows-documented-rule`. Its
  statement says Bearer tokens on a vendor API are `ApiKey`. Phase 2
  extends the rule (see the questions below).
- INV-27 no originated requests. INV-29 and INV-30 request and response
  unchanged.
- INV-37 `ingress.routing.auth-hosts-never-intercepted`.
- INV-162 `reconstruct.evidence.credential-follows-stability`.
- INV-372 `surface.api.no-raw-credentials`.

Proposed new invariants (`INV-X-<id>`, numbered by the coordinator):

- `ingress.credential.oauth-capability-marks-oauth`: on an Anthropic
  upstream, a Bearer credential sent with an `anthropic-beta` value
  starting `oauth-` is `OauthAccessToken`, whatever its shape. A key
  header never is. Evidence: unit and property tests in
  `crosstalk_ingress::tests::credential`.
- `reconstruct.identity.refresh-keeps-session-agent`: two exchanges with
  the same harness session id in the same scope, whose rotating
  credentials differ, resolve to the same agent. Evidence: a unit test in
  `crosstalk_reconstruct::tests::consumer`.
- `gateway.credential.absent-end-to-end`: no byte sequence of a request's
  bearer token (8 or more bytes) appears in any blob, bus envelope,
  exchange-log record, log line or L8 response produced from that
  exchange. Evidence: a Live composition test in `crosstalk_gateway`.

Constraints:

- Test tokens are fake, built in code with an obviously synthetic body
  (`sk-ant-oat01-TEST-…`). No test reads `~/.claude`, the environment's
  real credentials, or the network.
- The scheme decision reads token shape and headers only, never token
  content beyond a prefix.
- No new dependency.

## Work (phase 2)

1. Tests first: scheme rows, the subscription socket test, the L3 refresh
   test, and the Live end-to-end confidentiality and identity test.
2. Implement `oauth_capability` and the `scheme` signature change.
3. Add the e2e and testkit subscription session.
4. Add the invariant TOMLs and update the docs. Run the crate-scoped
   checks for ingress, reconstruct, testkit, e2e and gateway, then
   `inv_check`.

## Open questions

- **Amending INV-17.** The new rule changes INV-17's documented rule.
  Should INV-17's statement be amended, or should the new invariant sit
  beside it with INV-17 left as is?
- **Spoofed session ids.** With no account, harness ids are scoped by the
  upstream. Any client of the same gateway route that sends another
  user's `x-claude-code-session-id` is attributed to that user's agent.
  An API key prevents this, because harness ids are then scoped by the
  stable credential. Session ids are random UUIDs, so this takes intent,
  not accident. Should it be accepted and documented for subscription
  traffic?
- **Account from the body.** `metadata.user_id` in the body includes an
  account UUID on a subscription (observed, not documented). Hashing it
  into `ClientContext::account` would give cross-session identity and
  stop session spoofing across accounts. But the spec's `ClientIdentifier`
  reads the head only, the field is undocumented, and it is still a
  client claim. Proposed as a follow-up spec decision, not part of this
  work.
- **Raw account id in stored bodies.** Request bodies are stored as
  received, so that same account UUID is stored raw in the blob store. It
  is not a credential and is out of scope here, but it is personal data
  to flag.
- **`claude.ai` in `AUTH_HOSTS`.** `InterceptAllowlist::AUTH_HOSTS`
  lists `platform.claude.com` and `console.anthropic.com` but not
  `claude.ai` or `claude.com`, the sign-in hosts. Should P8 add them?
  That would be a spec change.
- **Terms of use.** Anthropic's docs describe a subscription through
  `ANTHROPIC_BASE_URL` as a working path ([llm-gateway]), and they do not
  endorse third-party gateways. Whether running crosstalk in front of
  other people's subscriptions is acceptable is the operator's call.
