//! `meta.json`: what a case is and what a correct pipeline makes of it.
//!
//! The file is strict JSON in the spec's wire conventions (snake_case keys,
//! adjacently tagged enums, unknown fields refused), reusing the spec's
//! types where one fits: the harness claim and ids a `ClientIdentifier`
//! should read, the credential scheme, the stop reason and the exchange
//! failure.

use crosstalk_spec::observed::client::{
    CredentialScheme, EndpointKind, HarnessClaim, HarnessIds, RequestClass,
};
use crosstalk_spec::observed::exchange::{ExchangeFailure, ModelName, ResponseId, StopReason};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct CaseMeta {
    pub description: String,
    pub provenance: Provenance,
    pub endpoint: Endpoint,
    /// The request's credential, replaced by a placeholder. `None` for a
    /// request that carries none.
    pub credential: Option<CredentialMeta>,
    pub harness: HarnessMeta,
    /// The case whose response this request continues (a tool result
    /// follow-up names its tool-use turn).
    pub follows: Option<String>,
    pub notes: Vec<String>,
}

/// Where a case came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Provenance {
    /// Written from documentation, not captured. `sources` are the
    /// documents it follows.
    Synthetic { sources: Vec<String> },
    /// Captured from a real harness and redacted (see the corpus README).
    Captured {
        /// The capture date, `YYYY-MM-DD`.
        captured_on: String,
        /// The harness and version that sent it.
        harness_version: String,
    },
}

/// What the request is for, and for generation what should come of it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Endpoint {
    /// Captured: becomes an exchange.
    Generation {
        model: ModelName,
        /// The request body's `stream` flag.
        stream: bool,
        expect: Expect,
    },
    /// Forwarded, not captured.
    TokenCount,
    ModelList,
    Probe,
}

impl Endpoint {
    /// The spec's endpoint kind.
    pub fn kind(&self) -> EndpointKind {
        match self {
            Self::Generation { .. } => EndpointKind::Generation,
            Self::TokenCount => EndpointKind::TokenCount,
            Self::ModelList => EndpointKind::ModelList,
            Self::Probe => EndpointKind::Probe,
        }
    }
}

/// How a generation exchange ends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Expect {
    Completed {
        stop: StopReason,
        response_id: ResponseId,
        /// The response's content blocks, in order.
        blocks: Vec<BlockKind>,
    },
    Failed {
        failure: ExchangeFailure,
        /// Content blocks that started before the failure, in order.
        partial_blocks: Vec<BlockKind>,
    },
}

/// An Anthropic response content block type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockKind {
    Text,
    Thinking,
    RedactedThinking,
    ToolUse,
    ServerToolUse,
}

/// Where the credential is and what stands in for it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct CredentialMeta {
    /// The header carrying it, lower case (`x-api-key`, `authorization`).
    pub header: String,
    pub scheme: CredentialScheme,
    /// The placeholder that replaced the secret inside the header's value.
    pub placeholder: String,
}

/// What the request claims about its harness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct HarnessMeta {
    /// The request headers that are harness claims, lower case: client
    /// assertions that are never identity evidence on their own.
    pub claim_headers: Vec<String>,
    /// The claim the headers make.
    pub claim: HarnessClaim,
    /// The session and agent ids the headers carry.
    pub ids: HarnessIds,
    /// From `x-claude-code-request-class`; `unknown` when absent.
    pub class: RequestClass,
}
