//! Who is calling, through which ingress, to which upstream.
//!
//! These are observed facts about a request, independent of its body. Three
//! axes that are easy to conflate are kept apart:
//!
//! - [`crate::observed::exchange::WireProtocol`]: the request and response
//!   format.
//! - [`UpstreamKind`]: what serves it. A vendor's API, a subscription backend
//!   (Claude Pro/Max, ChatGPT/Codex, GitHub Copilot) or a self-hosted
//!   inference server (vLLM, SGLang). The [`Dialect`] of the wire bytes
//!   follows from it.
//! - [`CredentialScheme`]: how the caller authenticated, if at all.
//!
//! Harness identity (Claude Code, Codex, pi, oh-my-pi) comes from headers the
//! client chooses to send. Some harnesses impersonate others, so a
//! [`HarnessClaim`] is recorded but never used as identity evidence, and
//! harness session and agent ids only count as evidence within the scope of a
//! credential or account (see [`crate::observed::agent`]).

use serde::{Deserialize, Serialize};

use crate::derived::flow::resource::Host;
use crate::ids::{AccountHash, CredentialHash};

/// How the request reached the gateway.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum IngressMode {
    /// The harness's base URL points at the gateway; `route` names the
    /// configured upstream route that matched.
    ReverseProxy { route: RouteName },
    /// The harness sent the request through the gateway as an HTTPS proxy and
    /// the gateway intercepted TLS for `host`. Only allowlisted hosts are
    /// intercepted; everything else is tunnelled untouched.
    ForwardProxy { host: Host },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RouteName(pub String);

/// A configured upstream, by name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct UpstreamId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Vendor {
    Anthropic,
    OpenAi,
    Google,
    GithubCopilot,
    Other(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InferenceServer {
    Vllm,
    Sglang,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum UpstreamKind {
    /// Pay-per-token API (api.anthropic.com, api.openai.com, …).
    VendorApi(Vendor),
    /// A subscription backend reached with an OAuth or exchanged token
    /// (Claude Pro/Max on api.anthropic.com, chatgpt.com/backend-api/codex,
    /// *.githubcopilot.com, cloudcode-pa.googleapis.com).
    Subscription(Vendor),
    /// A self-hosted, OpenAI- or Anthropic-compatible server.
    InferenceServer(InferenceServer),
}

/// Quirks of the wire bytes beyond the protocol itself: field names for
/// reasoning, tool-call id formats, finish reasons, extra stream chunks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Dialect {
    /// The vendor's own implementation of the protocol.
    Reference,
    /// `reasoning` field, `chatcmpl-tool-*` ids, `finish_reason: stop` for
    /// named tool choice.
    Vllm,
    /// `reasoning_content` (null when empty), `call_*` ids, explicit nulls,
    /// `sglext` chunks, `finish_reason: abort`.
    Sglang,
    /// Copilot's proxy of the Anthropic and OpenAI formats.
    Copilot,
}

impl UpstreamKind {
    pub fn dialect(&self) -> Dialect {
        match self {
            Self::InferenceServer(InferenceServer::Vllm) => Dialect::Vllm,
            Self::InferenceServer(InferenceServer::Sglang) => Dialect::Sglang,
            Self::VendorApi(Vendor::GithubCopilot) | Self::Subscription(Vendor::GithubCopilot) => {
                Dialect::Copilot
            }
            Self::VendorApi(_) | Self::Subscription(_) => Dialect::Reference,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Upstream {
    pub id: UpstreamId,
    pub kind: UpstreamKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialScheme {
    /// A long-lived API key (`x-api-key`, or `Authorization: Bearer`).
    ApiKey,
    /// An OAuth access token for a subscription. Refreshed by the harness
    /// directly with the vendor's auth server, so it changes over the life of
    /// one agent.
    OauthAccessToken,
    /// A short-lived token minted from another credential (Copilot).
    ExchangedToken,
    /// The static key of a self-hosted server, shared by every caller.
    ServerKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Stability {
    /// Stays the same for the life of the caller.
    Stable,
    /// Changes on refresh: never identifies a caller across a refresh.
    Rotating,
    /// The same for every caller of the upstream: identifies no one.
    Shared,
}

impl CredentialScheme {
    pub fn stability(self) -> Stability {
        match self {
            Self::ApiKey => Stability::Stable,
            Self::OauthAccessToken | Self::ExchangedToken => Stability::Rotating,
            Self::ServerKey => Stability::Shared,
        }
    }
}

/// A credential, hashed at the proxy. The raw credential is forwarded
/// upstream unchanged and never stored, logged or published.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct CredentialRef {
    pub scheme: CredentialScheme,
    pub hash: CredentialHash,
}

/// The harness the request says it came from. A claim: pi and oh-my-pi send
/// Claude Code's User-Agent on Claude subscription traffic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct HarnessClaim {
    pub family: HarnessFamily,
    pub version: Option<String>,
    pub user_agent: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HarnessFamily {
    ClaudeCode,
    Codex,
    Pi,
    OhMyPi,
    Unknown,
}

/// Session and agent ids a harness sends (`X-Claude-Code-Session-Id`,
/// `x-claude-code-agent-id`, `x-claude-code-parent-agent-id`; Codex
/// `session-id`, `thread-id`, `x-codex-parent-thread-id`; pi `session_id`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct HarnessIds {
    pub session: Option<String>,
    pub agent: Option<String>,
    pub parent_agent: Option<String>,
}

/// What the harness says this request is for (`x-claude-code-request-class`,
/// `x-openai-subagent`, compaction markers). A hint, not a fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestClass {
    Main,
    Subagent,
    Compaction,
    Auxiliary,
    Unknown,
}

/// Digests under the previous secret version during a rotation overlap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct PreviousDigests {
    pub credential: Option<CredentialHash>,
    pub account: Option<AccountHash>,
}

/// Everything known about the caller, from the connection and headers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ClientContext {
    pub ingress: IngressMode,
    pub upstream: Upstream,
    /// `None` for an unauthenticated self-hosted upstream.
    pub credential: Option<CredentialRef>,
    /// ChatGPT-Account-ID and similar, hashed.
    pub account: Option<AccountHash>,
    /// During a secret rotation overlap, the same credential and account
    /// hashed under the previous `SecretVersion`, so identity resolution can
    /// link evidence across the change. `None` outside an overlap.
    pub previous_digests: Option<PreviousDigests>,
    pub harness: Option<HarnessClaim>,
    pub ids: HarnessIds,
    pub class: RequestClass,
}

/// What a request to a generation-capable API is for. Only `Generation`
/// produces an exchange; everything else is forwarded and not captured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EndpointKind {
    /// Messages, chat completions, responses, generateContent.
    Generation,
    /// count_tokens, tokenize.
    TokenCount,
    /// Model listing.
    ModelList,
    /// Connection warm-up, health checks.
    Probe,
    /// Anything else on the upstream (files, sessions, telemetry, …).
    Other,
}
