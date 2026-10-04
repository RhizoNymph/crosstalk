//! Who is calling: the credential, account and harness claims in a request
//! head.
//!
//! [`HeaderIdentifier`] implements the spec's
//! [`ClientIdentifier`]. It reads headers (and, for keys that ride in the
//! query, `key=`) and nothing else: not the body, not other requests. The
//! credential is hashed the moment it is read ([`crate::credential`]).
//!
//! The scheme rule (`ingress.credential.scheme-follows-documented-rule`):
//!
//! | Upstream kind | Credential | Scheme |
//! | --- | --- | --- |
//! | self-hosted inference server | any | `ServerKey` |
//! | Copilot (either kind), or any token shaped `tid=…` | any | `ExchangedToken` |
//! | subscription | `Authorization` | `OauthAccessToken` |
//! | subscription | a key header or `key=` | `ApiKey` |
//! | vendor API | `Authorization: Bearer` shaped `sk-ant-oat…` or a JWT | `OauthAccessToken` |
//! | vendor API | anything else | `ApiKey` |
//!
//! Claude Pro/Max traffic goes to api.anthropic.com, the same host as the
//! API, so on a vendor API route the token's shape decides.

use crosstalk_spec::ids::AccountHash;
use crosstalk_spec::interfaces::l0_ingress::{ClientIdentifier, RequestHead};
use crosstalk_spec::observed::client::{
    ClientContext, CredentialRef, CredentialScheme, HarnessClaim, HarnessFamily, HarnessIds,
    IngressMode, PreviousDigests, RequestClass, Upstream, UpstreamKind, Vendor,
};

use crate::credential::{KeyedHasher, RawCredential, TokenShape};

/// Headers that carry credentials. Removed from the head the decoder sees,
/// so the raw credential never leaves the hot path.
pub const CREDENTIAL_HEADERS: &[&str] = &[
    "authorization",
    "x-api-key",
    "x-goog-api-key",
    "api-key",
    "proxy-authorization",
];

/// Where a credential was found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialSource {
    /// `Authorization: Bearer <token>`.
    Bearer,
    /// `Authorization` with another scheme: the whole value is the
    /// credential.
    Authorization,
    /// `x-api-key`, `x-goog-api-key` or `api-key`.
    KeyHeader,
    /// The `key` query parameter.
    QueryKey,
}

/// Reads identity from headers and hashes it with the deployment secrets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderIdentifier {
    keys: KeyedHasher,
}

impl HeaderIdentifier {
    pub fn new(keys: KeyedHasher) -> Self {
        Self { keys }
    }

    pub fn keys(&self) -> &KeyedHasher {
        &self.keys
    }

    /// The request's raw credential and where it was, borrowed from the
    /// head. `Authorization` wins over key headers, which win over `key=`.
    pub fn raw_credential(head: &RequestHead) -> Option<(CredentialSource, RawCredential<'_>)> {
        if let Some(value) = header(head, "authorization") {
            let value = value.trim();
            let (source, token) = match value.split_once(' ') {
                Some((scheme, token)) if scheme.eq_ignore_ascii_case("bearer") => {
                    (CredentialSource::Bearer, token.trim())
                }
                _ => (CredentialSource::Authorization, value),
            };
            if let Some(raw) = RawCredential::new(token) {
                return Some((source, raw));
            }
        }
        for name in ["x-api-key", "x-goog-api-key", "api-key"] {
            if let Some(raw) = header(head, name).and_then(|value| RawCredential::new(value.trim()))
            {
                return Some((CredentialSource::KeyHeader, raw));
            }
        }
        let query = head.query.as_deref()?;
        query
            .split('&')
            .find_map(|pair| pair.strip_prefix("key="))
            .and_then(RawCredential::new)
            .map(|raw| (CredentialSource::QueryKey, raw))
    }

    /// The scheme the documented rule assigns.
    pub fn scheme(
        source: CredentialSource,
        raw: RawCredential<'_>,
        kind: &UpstreamKind,
    ) -> CredentialScheme {
        let shape = raw.shape();
        match kind {
            UpstreamKind::InferenceServer(_) => CredentialScheme::ServerKey,
            UpstreamKind::VendorApi(Vendor::GithubCopilot)
            | UpstreamKind::Subscription(Vendor::GithubCopilot) => CredentialScheme::ExchangedToken,
            _ if shape == TokenShape::CopilotMinted => CredentialScheme::ExchangedToken,
            UpstreamKind::Subscription(_) => match source {
                CredentialSource::Bearer | CredentialSource::Authorization => {
                    CredentialScheme::OauthAccessToken
                }
                CredentialSource::KeyHeader | CredentialSource::QueryKey => {
                    CredentialScheme::ApiKey
                }
            },
            UpstreamKind::VendorApi(_) => match (source, shape) {
                (CredentialSource::Bearer, TokenShape::AnthropicOauth | TokenShape::Jwt) => {
                    CredentialScheme::OauthAccessToken
                }
                _ => CredentialScheme::ApiKey,
            },
        }
    }

    /// The account id header's raw value (`ChatGPT-Account-ID`).
    fn raw_account(head: &RequestHead) -> Option<&str> {
        header(head, "chatgpt-account-id")
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }

    /// Everything known about the caller, from the head alone.
    pub fn context(
        &self,
        head: &RequestHead,
        ingress: IngressMode,
        upstream: Upstream,
    ) -> ClientContext {
        let raw = Self::raw_credential(head);
        let account = Self::raw_account(head);
        let credential = raw.map(|(source, raw)| CredentialRef {
            scheme: Self::scheme(source, raw, &upstream.kind),
            hash: self.keys.credential(raw.bytes()),
        });
        let previous_digests = self.keys.in_overlap().then(|| PreviousDigests {
            credential: raw.and_then(|(_, raw)| self.keys.previous_credential(raw.bytes())),
            account: account.and_then(|raw| self.keys.previous_account(raw.as_bytes())),
        });
        let (harness, ids, class) = self.harness(head);
        ClientContext {
            ingress,
            upstream,
            credential,
            account: account.map(|raw| self.keys.account(raw.as_bytes())),
            previous_digests,
            harness,
            ids,
            class,
        }
    }
}

impl ClientIdentifier for HeaderIdentifier {
    fn credential(&self, head: &RequestHead, upstream: &Upstream) -> Option<CredentialRef> {
        Self::raw_credential(head).map(|(source, raw)| CredentialRef {
            scheme: Self::scheme(source, raw, &upstream.kind),
            hash: self.keys.credential(raw.bytes()),
        })
    }

    fn account(&self, head: &RequestHead) -> Option<AccountHash> {
        Self::raw_account(head).map(|raw| self.keys.account(raw.as_bytes()))
    }

    fn harness(&self, head: &RequestHead) -> (Option<HarnessClaim>, HarnessIds, RequestClass) {
        let claim = header(head, "user-agent").map(harness_claim);
        let ids = HarnessIds {
            session: first_header(
                head,
                &["x-claude-code-session-id", "session-id", "session_id"],
            ),
            agent: first_header(head, &["x-claude-code-agent-id", "thread-id"]),
            parent_agent: first_header(
                head,
                &["x-claude-code-parent-agent-id", "x-codex-parent-thread-id"],
            ),
        };
        (claim, ids, request_class(head))
    }
}

/// The claim a User-Agent makes. Only families whose User-Agent the
/// harness research documents are recognised; anything else is `Unknown`
/// with the text kept.
fn harness_claim(user_agent: &str) -> HarnessClaim {
    let product = user_agent.split_whitespace().next().unwrap_or_default();
    let (name, version) = match product.split_once('/') {
        Some((name, version)) if !version.is_empty() => (name, Some(version.to_owned())),
        _ => (product, None),
    };
    let family = match name {
        "claude-cli" => HarnessFamily::ClaudeCode,
        "codex_cli_rs" | "codex_exec" | "codex" => HarnessFamily::Codex,
        _ => HarnessFamily::Unknown,
    };
    let version = (family != HarnessFamily::Unknown)
        .then_some(version)
        .flatten();
    HarnessClaim {
        family,
        version,
        user_agent: user_agent.to_owned(),
    }
}

fn request_class(head: &RequestHead) -> RequestClass {
    if let Some(class) = header(head, "x-claude-code-request-class") {
        return match class.trim().to_ascii_lowercase().as_str() {
            "main" => RequestClass::Main,
            "subagent" => RequestClass::Subagent,
            "compaction" => RequestClass::Compaction,
            "auxiliary" => RequestClass::Auxiliary,
            _ => RequestClass::Unknown,
        };
    }
    if let Some(subagent) = header(head, "x-openai-subagent") {
        return if subagent.to_ascii_lowercase().contains("compact") {
            RequestClass::Compaction
        } else {
            RequestClass::Subagent
        };
    }
    RequestClass::Unknown
}

/// The first value of header `name`, matched without regard to case.
pub(crate) fn header<'h>(head: &'h RequestHead, name: &str) -> Option<&'h str> {
    head.headers
        .iter()
        .find(|(header, _)| header.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn first_header(head: &RequestHead, names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| header(head, name))
        .map(str::to_owned)
}

/// `head` without its credential headers, for the decoder.
pub fn without_credentials(head: &RequestHead) -> RequestHead {
    RequestHead {
        method: head.method.clone(),
        path: head.path.clone(),
        query: head.query.as_deref().and_then(|query| {
            let kept: Vec<&str> = query
                .split('&')
                .filter(|pair| !pair.starts_with("key="))
                .collect();
            (!kept.is_empty()).then(|| kept.join("&"))
        }),
        headers: head
            .headers
            .iter()
            .filter(|(name, _)| {
                !CREDENTIAL_HEADERS
                    .iter()
                    .any(|credential| name.eq_ignore_ascii_case(credential))
            })
            .cloned()
            .collect(),
    }
}
