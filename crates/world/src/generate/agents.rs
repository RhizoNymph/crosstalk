//! The cast: 40 canonical agents across every harness family, their
//! sub-agents, four merged aliases (one merged twice, so a merge repointed
//! it) and one merge an operator reverted.
//!
//! Agents are named by short keys (`cc0`, `cc0.a`, `pi1`, `al0`); tests and
//! the scenario list find them through [`crate::Scenario::agent`].

use std::collections::{BTreeMap, HashMap};

use crosstalk_spec::ids::{
    AccountHash, AgentId, CredentialHash, OperatorId, PromptHash, SecretVersion,
};
use crosstalk_spec::observed::agent::{
    AgentLabel, IdentityEvidence, IdentityScope, MergeAuthor, MergeRequest,
};
use crosstalk_spec::observed::client::{HarnessClaim, HarnessFamily, UpstreamId};
use crosstalk_spec::support::{Blake3, NonEmpty, Timestamp};

use crate::clock::{Anchor, DAY, HOUR, minus};
use crate::config::OPERATOR_RESEARCHER;
use crate::error::WorldError;
use crate::mint::Mint;
use crate::rng::Rng;
use crate::scenario::MergeKey;

/// The harness an agent runs in (what it really is, not what it claims).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Family {
    ClaudeCode,
    Codex,
    Pi,
    OhMyPi,
    SelfHosted,
}

/// The state an agent is generated into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Planned {
    /// Declared in config, never seen.
    Registered,
    /// Created by its first exchange.
    Provisional,
    /// Created by its first exchange, then established by corroborated
    /// evidence at `since`.
    Established { since: Timestamp },
}

struct Spec {
    key: &'static str,
    family: Family,
    parent: Option<&'static str>,
    label: Option<&'static str>,
    state: State,
    /// Sends Claude Code's User-Agent on Claude subscription traffic.
    impersonates: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Registered,
    Provisional,
    Established,
}

const fn spec(
    key: &'static str,
    family: Family,
    parent: Option<&'static str>,
    label: Option<&'static str>,
    state: State,
    impersonates: bool,
) -> Spec {
    Spec {
        key,
        family,
        parent,
        label,
        state,
        impersonates,
    }
}

use Family::{ClaudeCode as Cc, Codex as Cx, OhMyPi as Omp, Pi, SelfHosted as Sh};
use State::{Established as E, Provisional as P, Registered as R};

const SPECS: &[Spec] = &[
    spec("cc0", Cc, None, Some("atlas-lead"), E, false),
    spec("cc1", Cc, None, None, E, false),
    spec("cc2", Cc, None, Some("infra-bot"), E, false),
    spec("cc3", Cc, None, None, E, false),
    spec("cc4", Cc, None, None, E, false),
    spec("cc5", Cc, None, None, P, false),
    spec("cc6", Cc, None, None, P, false),
    spec("cc7", Cc, None, Some("notes-keeper"), E, false),
    spec("cc0.a", Cc, Some("cc0"), None, E, false),
    spec("cc0.b", Cc, Some("cc0"), None, P, false),
    spec("cc0.c", Cc, Some("cc0"), None, P, false),
    spec("cc1.a", Cc, Some("cc1"), None, P, false),
    spec("cc1.b", Cc, Some("cc1"), None, P, false),
    spec("cc2.a", Cc, Some("cc2"), None, E, false),
    spec("cc2.b", Cc, Some("cc2"), None, P, false),
    spec("cc3.a", Cc, Some("cc3"), None, P, false),
    spec("cc4.a", Cc, Some("cc4"), None, P, false),
    spec("cx0", Cx, None, Some("codex-reviewer"), E, false),
    spec("cx1", Cx, None, None, E, false),
    spec("cx2", Cx, None, None, E, false),
    spec("cx3", Cx, None, None, P, false),
    spec("cx4", Cx, None, None, E, false),
    spec("cx0.a", Cx, Some("cx0"), None, P, false),
    spec("cx0.b", Cx, Some("cx0"), None, P, false),
    spec("cx1.a", Cx, Some("cx1"), None, P, false),
    spec("pi0", Pi, None, Some("pi-scraper"), E, true),
    spec("pi1", Pi, None, None, E, true),
    spec("pi2", Pi, None, None, E, false),
    spec("pi3", Pi, None, None, P, false),
    spec("omp0", Omp, None, Some("omp-orchestrator"), E, true),
    spec("omp1", Omp, None, None, E, false),
    spec("omp2", Omp, None, None, P, true),
    spec("omp3", Omp, None, None, P, false),
    spec("omp0.a", Omp, Some("omp0"), None, P, false),
    spec("omp0.b", Omp, Some("omp0"), None, P, true),
    spec("sh0", Sh, None, Some("batch-evals"), E, false),
    spec("sh1", Sh, None, None, P, false),
    spec("reg0", Cc, None, Some("release-bot"), R, false),
    spec("reg1", Cx, None, Some("nightly-evals"), R, false),
    spec("reg2", Pi, None, None, R, false),
    // Aliases: merged below.
    spec("al0", Cc, None, None, P, false),
    spec("al1", Cx, None, None, E, false),
    spec("al2", Pi, None, None, P, false),
    spec("al3", Pi, None, None, P, false),
];

/// One merge in the generated history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedMerge {
    pub key: MergeKey,
    pub request: MergeRequest,
    pub at: Timestamp,
    /// Operator and time of the revert, if reverted.
    pub reverted: Option<(OperatorId, Timestamp)>,
}

/// One generated agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedAgent {
    pub key: &'static str,
    pub id: AgentId,
    pub family: Family,
    pub parent: Option<AgentId>,
    pub label: Option<AgentLabel>,
    pub evidence: NonEmpty<IdentityEvidence>,
    pub state: Planned,
    /// When its first exchange started; `None` for a registered agent.
    pub first_seen: Option<Timestamp>,
}

/// The generated agents and their merge history.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Cast {
    pub agents: Vec<PlannedAgent>,
    keys: BTreeMap<&'static str, AgentId>,
    pub impersonators: Vec<AgentId>,
    /// Aliases stop producing traffic when they are merged.
    pub active_until: HashMap<AgentId, Timestamp>,
    /// `(parent, child)` for every sub-agent.
    pub delegations: Vec<(AgentId, AgentId)>,
    pub merges: Vec<PlannedMerge>,
}

impl Cast {
    pub fn get(&self, key: &str) -> Option<AgentId> {
        self.keys.get(key).copied()
    }

    pub fn id(&self, key: &str) -> Result<AgentId, WorldError> {
        self.get(key)
            .ok_or_else(|| WorldError::missing(format!("agent {key}")))
    }

    pub fn ids(&self, keys: &[&str]) -> Result<Vec<AgentId>, WorldError> {
        keys.iter().map(|k| self.id(k)).collect()
    }

    pub fn keys(&self) -> impl Iterator<Item = (&'static str, AgentId)> + '_ {
        self.keys.iter().map(|(key, id)| (*key, *id))
    }

    pub fn agent(&self, id: AgentId) -> Option<&PlannedAgent> {
        self.agents.iter().find(|agent| agent.id == id)
    }

    /// Whether `agent` can produce traffic at `at`.
    pub fn active_at(&self, agent: AgentId, at: Timestamp) -> bool {
        self.active_until
            .get(&agent)
            .is_none_or(|until| at < *until)
    }

    pub fn is_registered(&self, agent: AgentId) -> bool {
        self.agent(agent)
            .is_some_and(|a| a.state == Planned::Registered)
    }
}

/// One planned merge: its key, source and target keys, author, time,
/// and the revert's operator and time.
type MergeRow = (
    MergeKey,
    &'static str,
    &'static str,
    MergeAuthor,
    Timestamp,
    Option<(OperatorId, Timestamp)>,
);

/// The planned merges, oldest first.
fn planned_merges(anchor: Anchor) -> [MergeRow; 5] {
    [
        (
            MergeKey::PiChainFirst,
            "al2",
            "al3",
            MergeAuthor::Resolver,
            anchor.ago(6 * DAY),
            None,
        ),
        (
            MergeKey::Reverted,
            "omp3",
            "omp1",
            MergeAuthor::Resolver,
            anchor.ago(5 * DAY + 3 * HOUR),
            Some((OPERATOR_RESEARCHER, anchor.ago(2 * DAY))),
        ),
        (
            MergeKey::AtlasAlias,
            "al0",
            "cc0",
            MergeAuthor::Resolver,
            anchor.ago(5 * DAY),
            None,
        ),
        (
            MergeKey::PiChainSecond,
            "al3",
            "pi2",
            MergeAuthor::Operator(OPERATOR_RESEARCHER),
            anchor.ago(4 * DAY),
            None,
        ),
        (
            MergeKey::CodexAlias,
            "al1",
            "cx1",
            MergeAuthor::Operator(OPERATOR_RESEARCHER),
            anchor.ago(3 * DAY),
            None,
        ),
    ]
}

pub fn build(seed: u64, anchor: Anchor, mint: &mut Mint) -> Result<Cast, WorldError> {
    let mut rng = Rng::fork(seed, "agents");
    let mut cast = Cast::default();
    let start = anchor.start();

    for spec in SPECS {
        let first_seen = minus(start, rng.between(DAY, 20 * DAY));
        let id: AgentId = mint.at(first_seen)?;
        cast.keys.insert(spec.key, id);
        if spec.impersonates {
            cast.impersonators.push(id);
        }
        let state = match spec.state {
            State::Registered => Planned::Registered,
            State::Provisional => Planned::Provisional,
            State::Established => Planned::Established {
                since: first_seen.max(minus(start, rng.between(HOUR, DAY))),
            },
        };
        let label = spec
            .label
            .map(AgentLabel::new)
            .transpose()
            .map_err(|e| WorldError::invalid("AgentLabel", e))?;
        cast.agents.push(PlannedAgent {
            key: spec.key,
            id,
            family: spec.family,
            parent: None,
            label,
            evidence: evidence(spec, &mut rng)?,
            state,
            first_seen: (spec.state != State::Registered).then_some(first_seen),
        });
    }

    for (spec, agent) in SPECS.iter().zip(cast.agents.iter_mut()) {
        if let Some(parent) = spec.parent {
            let parent = *cast
                .keys
                .get(parent)
                .ok_or_else(|| WorldError::missing(format!("parent {parent}")))?;
            agent.parent = Some(parent);
            cast.delegations.push((parent, agent.id));
        }
    }

    plan_merges(&mut cast, anchor)?;
    Ok(cast)
}

/// Plans the merges and the revert, tracking which ids stop producing
/// traffic as the merge table would resolve them.
fn plan_merges(cast: &mut Cast, anchor: Anchor) -> Result<(), WorldError> {
    // Each merged id and the canonical agent it resolves to.
    let mut into: BTreeMap<AgentId, AgentId> = BTreeMap::new();
    for (key, from, target, by, at, reverted) in planned_merges(anchor) {
        let from = cast.id(from)?;
        let target = cast.id(target)?;
        let request = MergeRequest::new(from, target, by)
            .map_err(|e| WorldError::invalid("MergeRequest", e))?;
        let repointed: Vec<AgentId> = into
            .iter()
            .filter(|(_, canonical)| **canonical == from)
            .map(|(agent, _)| *agent)
            .collect();
        for agent in &repointed {
            into.insert(*agent, target);
        }
        into.insert(from, target);
        cast.active_until.insert(from, at);
        for agent in &repointed {
            cast.active_until.insert(*agent, at);
        }
        if reverted.is_some() {
            // Only `omp3`, which nothing was merged into: the revert
            // restores it alone.
            into.remove(&from);
            cast.active_until.remove(&from);
            for agent in &repointed {
                into.insert(*agent, from);
                cast.active_until.remove(agent);
            }
        }
        cast.merges.push(PlannedMerge {
            key,
            request,
            at,
            reverted,
        });
    }
    Ok(())
}

fn digest(rng: &mut Rng) -> Blake3 {
    Blake3::from_bytes(rng.bytes32())
}

fn evidence(spec: &Spec, rng: &mut Rng) -> Result<NonEmpty<IdentityEvidence>, WorldError> {
    let credential = CredentialHash::from_keyed_digest(SecretVersion(1), digest(rng));
    let account = AccountHash::from_keyed_digest(SecretVersion(1), digest(rng));
    let session = format!("{:08x}-{:04x}", rng.next_u64() as u32, rng.below(0xffff));
    let mut items = Vec::new();
    match spec.family {
        Family::ClaudeCode => {
            let scope = IdentityScope::Credential(credential);
            if spec.parent.is_some() {
                items.push(IdentityEvidence::HarnessAgent {
                    scope: scope.clone(),
                    agent: format!("agent-{:012x}", rng.next_u64() >> 16),
                });
            }
            items.push(IdentityEvidence::HarnessSession { scope, session });
            items.push(IdentityEvidence::StableCredential(credential));
        }
        Family::Codex => {
            let scope = IdentityScope::Account(account);
            if spec.parent.is_some() {
                items.push(IdentityEvidence::HarnessAgent {
                    scope: scope.clone(),
                    agent: format!("thread-{:012x}", rng.next_u64() >> 16),
                });
            }
            items.push(IdentityEvidence::HarnessSession { scope, session });
            items.push(IdentityEvidence::Account(account));
            items.push(IdentityEvidence::RotatingCredential(credential));
        }
        Family::Pi | Family::OhMyPi => {
            let scope = IdentityScope::Account(account);
            if spec.parent.is_some() {
                items.push(IdentityEvidence::HarnessAgent {
                    scope: scope.clone(),
                    agent: format!("task-{:012x}", rng.next_u64() >> 16),
                });
            }
            items.push(IdentityEvidence::HarnessSession { scope, session });
            items.push(IdentityEvidence::RotatingCredential(credential));
        }
        Family::SelfHosted => {
            items.push(IdentityEvidence::HarnessSession {
                scope: IdentityScope::Upstream(UpstreamId("vllm-local".to_owned())),
                session,
            });
            items.push(IdentityEvidence::PromptFingerprint(
                PromptHash::from_digest(digest(rng)),
            ));
        }
    }
    if spec.state == State::Registered {
        items = vec![IdentityEvidence::StableCredential(credential)];
    }
    NonEmpty::from_vec(items).ok_or_else(|| WorldError::missing("identity evidence"))
}

pub fn claude_code_claim() -> HarnessClaim {
    HarnessClaim {
        family: HarnessFamily::ClaudeCode,
        version: Some("2.1.30".to_owned()),
        user_agent: "claude-cli/2.1.30 (external, cli)".to_owned(),
    }
}

/// The claim an agent's own harness sends.
pub fn own_claim(family: Family) -> HarnessClaim {
    match family {
        Family::ClaudeCode => claude_code_claim(),
        Family::Codex => HarnessClaim {
            family: HarnessFamily::Codex,
            version: Some("0.46.0".to_owned()),
            user_agent: "codex_cli_rs/0.46.0 (Ubuntu 24.04; x86_64) xterm-256color".to_owned(),
        },
        Family::Pi => HarnessClaim {
            family: HarnessFamily::Pi,
            version: Some("0.9.2".to_owned()),
            user_agent: "pi/0.9.2".to_owned(),
        },
        Family::OhMyPi => HarnessClaim {
            family: HarnessFamily::OhMyPi,
            version: Some("1.4.0".to_owned()),
            user_agent: "oh-my-pi/1.4.0".to_owned(),
        },
        Family::SelfHosted => HarnessClaim {
            family: HarnessFamily::Unknown,
            version: None,
            user_agent: "python-httpx/0.28.1".to_owned(),
        },
    }
}

/// The claims an agent's exchanges carried and when each was last seen:
/// impersonators claim Claude Code on their Claude subscription traffic
/// (the latest exchange) and their own family elsewhere (seven hours
/// before); everyone else their own family.
pub fn claims(
    agent: &PlannedAgent,
    impersonator: bool,
    last: Timestamp,
) -> Vec<(HarnessClaim, Timestamp)> {
    if impersonator {
        vec![
            (claude_code_claim(), last),
            (own_claim(agent.family), minus(last, 7 * HOUR)),
        ]
    } else {
        vec![(own_claim(agent.family), last)]
    }
}
