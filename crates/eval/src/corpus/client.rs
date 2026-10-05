//! A synthetic `ClientContext` per agent.
//!
//! Corpus exchanges are replayed, not proxied: their ingress is
//! `IngressMode::Replay { corpus }` with one [`CorpusId`] per dataset
//! ([`corpus_id`]), which only `Pipeline::ingest`'s callers set
//! (`ingress.mode.never-replay`). Datasets carry no wire headers, so each
//! agent gets one stable synthetic API-key credential (a digest of its key,
//! so it is the same on every run and different for every agent), and no
//! harness claim, session or agent ids. Under `Replay` the credential is
//! scoped to the corpus: L3 attributes and merges a replayed exchange only
//! within its corpus (`reconstruct.identity.replay-within-corpus`), so two
//! datasets that happen to share a key never share an agent.

use crosstalk_spec::ids::{CredentialHash, SecretVersion};
use crosstalk_spec::observed::client::{
    ClientContext, CorpusId, CredentialRef, CredentialScheme, HarnessIds, IngressMode,
    RequestClass, Upstream, UpstreamId, UpstreamKind, Vendor,
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

/// The corpus a dataset's exchanges are replayed under: one per dataset, so
/// every world family of a dataset is one population for identity.
pub fn corpus_id(dataset: &DatasetId) -> CorpusId {
    CorpusId(format!("eval-{dataset}"))
}

/// The client context every exchange of `agent` carries.
pub fn synthetic_client(dataset: &DatasetId, agent: &AgentKey, model: &str) -> ClientContext {
    let credential = digest("credential", dataset, &[agent.world.as_str(), &agent.name]);
    ClientContext {
        ingress: IngressMode::Replay {
            corpus: corpus_id(dataset),
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
