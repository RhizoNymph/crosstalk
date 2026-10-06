//! Wire-level traffic: the exchanges the world's agents sent through the
//! gateway, as L1 would have captured them, so a host can run them
//! through L3's threading and L4's provenance and serve real
//! conversations ([`Wire`]).
//!
//! ```text
//! confirmed transmissions ─▶ needs::events     a sender exchange per origin body,
//!                                               a reader exchange per arrival
//! per agent, oldest first ─▶ sessions::Sessions full-history sessions: pauses, compactions,
//!                                               forks, failed attempts, tool calls
//!                          ─▶ Wire              every exchange, oldest first; the bodies
//!                                               only the wire carries
//! ```
//!
//! The exchanges carry the world's own bodies (each origin span's body as
//! a sender's response, each reader copy as a tool result, user turn,
//! system prompt or output), under the world's own ids where it has them
//! (a channel write's and read's exchange, every match's reader exchange),
//! so a transmission's evidence and the conversation turns that sent and
//! received it name the same exchanges and the same bytes. Everything else
//! (prompts, replies, tool calls, summaries) is generated here.
//!
//! Who sent an exchange is the world's attribution ([`WireExchange::agent`]):
//! the world states identity; a host threads each exchange under it.

mod messages;
mod needs;
mod sessions;

use std::collections::{BTreeMap, BTreeSet};

use crosstalk_spec::ids::{AgentId, MessageHash, TransmissionId};
use crosstalk_spec::observed::agent::IdentityEvidence;
use crosstalk_spec::observed::client::{
    ClientContext, CredentialRef, CredentialScheme, HarnessIds, InferenceServer, IngressMode,
    RequestClass, RouteName, Upstream, UpstreamId, UpstreamKind, Vendor,
};
use crosstalk_spec::observed::exchange::{Exchange, ModelName, Transport, WireProtocol};
use crosstalk_spec::support::Timestamp;

use crate::error::WorldError;
use crate::mint::Mint;
use crate::rng::Rng;

use super::agents::{Cast, Family, PlannedAgent, claude_code_claim, own_claim};
use super::states::TxRecord;
use super::traffic::Traffic;

pub use messages::SUMMARY_PREAMBLE;
pub use sessions::{MAX_TURNS, SESSION_GAP};

/// One captured exchange and the agent the world attributes it to (as
/// attributed then: an alias merged later keeps its own id).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireExchange {
    pub agent: AgentId,
    pub exchange: Exchange,
}

/// The world's wire traffic.
#[derive(Debug, Clone, Default)]
pub struct Wire {
    /// Oldest first; one agent's in the order it sent them.
    pub exchanges: Vec<WireExchange>,
    /// The canonical encoding of every body only the wire carries
    /// (system prompts, prompts, replies, tool calls, summaries), by hash.
    /// The world's own bodies are written by the seed.
    pub bodies: BTreeMap<MessageHash, Vec<u8>>,
    /// The world bodies content retention dropped after capture, by hash:
    /// what L3 and L4 read when they handled the exchange, which no store
    /// the surface reads holds (the seed never writes them).
    pub dropped: BTreeMap<MessageHash, Vec<u8>>,
}

/// Which confirmed transmissions the wire traffic carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WireScope {
    /// Every one: the whole week (about 14,500 exchanges for the default
    /// seed).
    #[default]
    All,
    /// Those opened at or after the instant, and the ones retention drops a
    /// side of (so dropped bodies show): a lighter world for hosts that
    /// thread and scan the wire at start.
    Since(Timestamp),
}

impl WireScope {
    fn holds(self, opened: Timestamp) -> bool {
        match self {
            Self::All => true,
            Self::Since(since) => opened >= since,
        }
    }
}

/// What the wire is built from.
pub struct Inputs<'a> {
    pub seed: u64,
    pub cast: &'a Cast,
    pub traffic: &'a Traffic,
    pub scope: WireScope,
    /// Transmissions carried whatever the scope.
    pub always: &'a BTreeSet<TransmissionId>,
}

/// Builds the wire traffic of `traffic`'s confirmed transmissions in
/// scope, before content retention drops any body. Deterministic per seed
/// and scope.
pub fn build(inputs: Inputs<'_>, mint: &mut Mint) -> Result<Wire, WorldError> {
    let Inputs {
        seed,
        cast,
        traffic,
        scope,
        always,
    } = inputs;
    let mut rng = Rng::fork(seed, "wire");
    let carried = |record: &TxRecord| {
        scope.holds(record.transmission.opened_at) || always.contains(&record.id())
    };
    let mut events = needs::events(traffic, &carried, mint, &mut rng)?;
    events.sort_by_key(|event| (event.at, event.exchange));
    let mut by_agent: BTreeMap<AgentId, Vec<needs::Event>> = BTreeMap::new();
    for event in events {
        by_agent.entry(event.agent).or_default().push(event);
    }
    let mut bodies = messages::Bodies::default();
    let mut sent = Vec::new();
    for (index, (agent, events)) in by_agent.into_iter().enumerate() {
        let planned = cast
            .agent(agent)
            .ok_or_else(|| WorldError::missing(format!("agent {}", agent.ulid_text())))?;
        let caller = caller(planned, cast.impersonators.contains(&agent));
        let mut agent_rng = Rng::fork(seed ^ agent.as_ulid() as u64, "wire-agent");
        let mut sessions = sessions::Sessions::new(&caller, &mut agent_rng, mint, &mut bodies);
        for event in &events {
            sessions.add(event)?;
        }
        for (seq, exchange) in sessions.finish().into_iter().enumerate() {
            sent.push((index, seq, exchange));
        }
    }
    sent.sort_by_key(|(index, seq, wire)| (wire.exchange.meta.started_at, *index, *seq));
    let exchanges: Vec<WireExchange> = sent.into_iter().map(|(_, _, wire)| wire).collect();
    tracing::debug!(exchanges = exchanges.len(), "wire traffic generated");
    Ok(Wire {
        exchanges,
        bodies: bodies.into_map(),
        dropped: BTreeMap::new(),
    })
}

/// How `agent`'s harness reaches the gateway; an impersonator sends
/// Claude Code's claim, as on its Claude subscription traffic.
fn caller(agent: &PlannedAgent, impersonator: bool) -> sessions::Caller {
    let mut session = None;
    let mut harness_agent = None;
    let mut credential = None;
    let mut account = None;
    for item in agent.evidence.iter() {
        match item {
            IdentityEvidence::HarnessSession { session: s, .. } => session = Some(s.clone()),
            IdentityEvidence::HarnessAgent { agent: a, .. } => harness_agent = Some(a.clone()),
            IdentityEvidence::StableCredential(hash) => {
                credential = Some(CredentialRef {
                    scheme: CredentialScheme::ApiKey,
                    hash: *hash,
                });
            }
            IdentityEvidence::RotatingCredential(hash) => {
                credential = Some(CredentialRef {
                    scheme: CredentialScheme::OauthAccessToken,
                    hash: *hash,
                });
            }
            IdentityEvidence::Account(hash) => account = Some(*hash),
            IdentityEvidence::PromptFingerprint(_) => {}
        }
    }
    let upstream = |id: &str, kind| Upstream {
        id: UpstreamId(id.to_owned()),
        kind,
    };
    let anthropic = || upstream("anthropic", UpstreamKind::Subscription(Vendor::Anthropic));
    let (harness, protocol, transport, model, upstream, route) = match agent.family {
        Family::ClaudeCode => (
            "Claude Code",
            WireProtocol::AnthropicMessages,
            Transport::Sse,
            "claude-sonnet-4-5",
            anthropic(),
            "anthropic",
        ),
        Family::Codex => (
            "Codex",
            WireProtocol::OpenAiResponses,
            Transport::Sse,
            "gpt-5-codex",
            upstream("chatgpt", UpstreamKind::Subscription(Vendor::OpenAi)),
            "chatgpt",
        ),
        Family::Pi => (
            "pi",
            WireProtocol::AnthropicMessages,
            Transport::Sse,
            "claude-sonnet-4-5",
            anthropic(),
            "anthropic",
        ),
        Family::OhMyPi => (
            "oh-my-pi",
            WireProtocol::AnthropicMessages,
            Transport::Sse,
            "claude-sonnet-4-5",
            anthropic(),
            "anthropic",
        ),
        Family::SelfHosted => (
            "a batch script",
            WireProtocol::OpenAiChat,
            Transport::Http,
            "qwen3-coder-30b",
            upstream(
                "vllm-local",
                UpstreamKind::InferenceServer(InferenceServer::Vllm),
            ),
            "vllm-local",
        ),
    };
    let claim = if impersonator {
        claude_code_claim()
    } else {
        own_claim(agent.family)
    };
    sessions::Caller {
        agent: agent.id,
        key: agent.key,
        harness,
        protocol,
        transport,
        model: ModelName(model.to_owned()),
        client: ClientContext {
            ingress: IngressMode::ReverseProxy {
                route: RouteName(route.to_owned()),
            },
            upstream,
            credential,
            account,
            previous_digests: None,
            harness: Some(claim),
            ids: HarnessIds {
                session,
                agent: harness_agent,
                parent_agent: None,
            },
            class: if agent.parent.is_some() {
                RequestClass::Subagent
            } else {
                RequestClass::Main
            },
        },
    }
}
