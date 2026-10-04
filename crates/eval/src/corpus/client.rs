//! A synthetic `ClientContext` per agent.
//!
//! Datasets carry no wire headers, so each agent gets one stable synthetic
//! API-key credential (a digest of its key, so it is the same on every run
//! and different for every agent) on a reverse-proxy route named after the
//! dataset, and no harness claim, session or agent ids.
//!
//! TODO(docs/spec-eval-gaps): use `IngressMode::Replay { corpus }` instead of
//! a fabricated reverse-proxy route once the spec has it.

use crosstalk_spec::ids::{CredentialHash, SecretVersion};
use crosstalk_spec::observed::client::{
    ClientContext, CredentialRef, CredentialScheme, HarnessIds, IngressMode, RequestClass,
    RouteName, Upstream, UpstreamId, UpstreamKind, Vendor,
};

use crate::ids::digest;
use crate::keys::{AgentKey, DatasetId};

/// The vendor a provider-prefixed model name (`gemini/…`,
/// `bedrock/converse/…anthropic…`, `openai/…`) points at.
pub fn vendor_of(model: &str) -> Vendor {
    let lower = model.to_ascii_lowercase();
    if lower.contains("anthropic") || lower.contains("claude") {
        Vendor::Anthropic
    } else if lower.starts_with("gemini") || lower.contains("gemini") || lower.contains("gemma") {
        Vendor::Google
    } else if lower.starts_with("openai/gpt") || lower.starts_with("gpt") {
        Vendor::OpenAi
    } else {
        let provider = lower.split('/').next().unwrap_or(&lower).to_owned();
        Vendor::Other(provider)
    }
}

/// The client context every exchange of `agent` carries.
pub fn synthetic_client(dataset: &DatasetId, agent: &AgentKey, model: &str) -> ClientContext {
    let credential = digest("credential", dataset, &[agent.world.as_str(), &agent.name]);
    ClientContext {
        ingress: IngressMode::ReverseProxy {
            route: RouteName(format!("eval-{dataset}")),
        },
        upstream: Upstream {
            id: UpstreamId(format!("eval-{dataset}")),
            kind: UpstreamKind::VendorApi(vendor_of(model)),
        },
        credential: Some(CredentialRef {
            scheme: CredentialScheme::ApiKey,
            hash: CredentialHash::from_keyed_digest(SecretVersion(0), credential),
        }),
        account: None,
        previous_digests: None,
        harness: None,
        ids: HarnessIds {
            session: None,
            agent: None,
            parent_agent: None,
        },
        class: RequestClass::Main,
    }
}
