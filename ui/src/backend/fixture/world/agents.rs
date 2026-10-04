//! The cast: 40 canonical agents across every harness family, their
//! sub-agents, four merged aliases (one merged twice, so a merge repointed
//! it) and one merge an operator reverted.
//!
//! Agents are named by short keys (`cc0`, `cc0.a`, `pi1`, `al0`) that only
//! the fixture uses; tests and the scenario list find them through
//! [`Cast::get`].

use std::collections::{BTreeMap, HashMap};

use crosstalk_spec::ids::{
    AccountHash, AgentId, CredentialHash, OperatorId, PromptHash, SecretVersion,
};
use crosstalk_spec::observed::agent::{
    Agent, AgentState, IdentityEvidence, IdentityScope, MergeAuthor,
};
use crosstalk_spec::observed::client::{HarnessClaim, HarnessFamily, UpstreamId};
use crosstalk_spec::support::{Blake3, NonEmpty, Timestamp};

use crate::contract::MergeId;
use crate::contract::agents::{ActiveAgentState, AgentLabel, ClaimSeen, MergeRecord, MergeVeto};

use super::GenError;
use super::history::OPERATOR_RESEARCHER;
use crate::backend::fixture::clock::{DAY, HOUR, Mint, START, ago, minus};
use crate::backend::fixture::rng::Rng;
use crate::backend::fixture::store::AgentRecord;

/// The harness an agent runs in (what it really is, not what it claims).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Family {
    ClaudeCode,
    Codex,
    Pi,
    OhMyPi,
    SelfHosted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Planned {
    Registered,
    Provisional,
    Established,
}

struct Spec {
    key: &'static str,
    family: Family,
    parent: Option<&'static str>,
    label: Option<&'static str>,
    state: Planned,
    /// Sends Claude Code's User-Agent on Claude subscription traffic.
    impersonates: bool,
}

const fn spec(
    key: &'static str,
    family: Family,
    parent: Option<&'static str>,
    label: Option<&'static str>,
    state: Planned,
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
use Planned::{Established as E, Provisional as P, Registered as R};

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
struct PlannedMerge {
    from: &'static str,
    into: &'static str,
    by: MergeAuthor,
    at: Timestamp,
    /// Operator and time of the revert, if reverted.
    reverted: Option<(OperatorId, Timestamp)>,
}

fn planned_merges() -> [PlannedMerge; 5] {
    [
        PlannedMerge {
            from: "al2",
            into: "al3",
            by: MergeAuthor::Resolver,
            at: ago(6 * DAY),
            reverted: None,
        },
        PlannedMerge {
            from: "omp3",
            into: "omp1",
            by: MergeAuthor::Resolver,
            at: ago(5 * DAY + 3 * HOUR),
            reverted: Some((OPERATOR_RESEARCHER, ago(2 * DAY))),
        },
        PlannedMerge {
            from: "al0",
            into: "cc0",
            by: MergeAuthor::Resolver,
            at: ago(5 * DAY),
            reverted: None,
        },
        PlannedMerge {
            from: "al3",
            into: "pi2",
            by: MergeAuthor::Operator(OPERATOR_RESEARCHER),
            at: ago(4 * DAY),
            reverted: None,
        },
        PlannedMerge {
            from: "al1",
            into: "cx1",
            by: MergeAuthor::Operator(OPERATOR_RESEARCHER),
            at: ago(3 * DAY),
            reverted: None,
        },
    ]
}

/// The generated agents and their merge history.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Cast {
    pub records: Vec<AgentRecord>,
    keys: BTreeMap<String, AgentId>,
    pub families: HashMap<AgentId, Family>,
    pub impersonators: Vec<AgentId>,
    /// Aliases stop producing traffic when they are merged.
    pub active_until: HashMap<AgentId, Timestamp>,
    /// `(parent, child)` for every sub-agent.
    pub delegations: Vec<(AgentId, AgentId)>,
    pub merges: Vec<MergeRecord>,
    pub priors: HashMap<MergeId, ActiveAgentState>,
    pub vetoes: Vec<MergeVeto>,
}

impl Cast {
    pub fn get(&self, key: &str) -> Option<AgentId> {
        self.keys.get(key).copied()
    }

    pub fn id(&self, key: &str) -> Result<AgentId, GenError> {
        self.get(key)
            .ok_or_else(|| GenError::Missing(format!("agent {key}")))
    }

    pub fn ids(&self, keys: &[&str]) -> Result<Vec<AgentId>, GenError> {
        keys.iter().map(|k| self.id(k)).collect()
    }

    pub fn key_of(&self, id: AgentId) -> Option<&str> {
        self.keys
            .iter()
            .find(|(_, v)| **v == id)
            .map(|(k, _)| k.as_str())
    }

    /// Whether `agent` can produce traffic at `at`.
    pub fn active_at(&self, agent: AgentId, at: Timestamp) -> bool {
        self.active_until
            .get(&agent)
            .is_none_or(|until| at < *until)
    }

    pub fn is_registered(&self, agent: AgentId) -> bool {
        self.records
            .iter()
            .any(|r| r.agent.id == agent && matches!(r.agent.state, AgentState::Registered { .. }))
    }
}

pub fn build(seed: u64, mint: &mut Mint) -> Result<Cast, GenError> {
    let mut rng = Rng::fork(seed, "agents");
    let mut keys = BTreeMap::new();
    let mut families = HashMap::new();
    let mut impersonators = Vec::new();
    let mut records: Vec<AgentRecord> = Vec::new();

    for spec in SPECS {
        let first_seen = minus(START, rng.between(DAY, 20 * DAY));
        let id = AgentId::from_ulid(mint.ulid(first_seen));
        keys.insert(spec.key.to_owned(), id);
        families.insert(id, spec.family);
        if spec.impersonates {
            impersonators.push(id);
        }
        let state = match spec.state {
            Planned::Registered => AgentState::Registered { at: first_seen },
            Planned::Provisional => AgentState::Provisional { first_seen },
            Planned::Established => AgentState::Established {
                since: first_seen.max(minus(START, rng.between(HOUR, DAY))),
            },
        };
        let label = spec
            .label
            .map(AgentLabel::new)
            .transpose()
            .map_err(|e| GenError::invalid("AgentLabel", e))?;
        records.push(AgentRecord {
            agent: Agent {
                id,
                evidence: evidence(spec, &mut rng)?,
                parent: None,
                state,
            },
            label,
        });
    }

    let mut delegations = Vec::new();
    for (spec, record) in SPECS.iter().zip(records.iter_mut()) {
        if let Some(parent) = spec.parent {
            let parent = *keys
                .get(parent)
                .ok_or_else(|| GenError::Missing(format!("parent {parent}")))?;
            record.agent.parent = Some(parent);
            delegations.push((parent, record.agent.id));
        }
    }

    let mut cast = Cast {
        records,
        keys,
        families,
        impersonators,
        active_until: HashMap::new(),
        delegations,
        merges: Vec::new(),
        priors: HashMap::new(),
        vetoes: Vec::new(),
    };
    apply_merges(&mut cast, mint)?;
    Ok(cast)
}

/// Replays the planned merges the way `MergeAgents` applies them, and the
/// one revert the way `Unmerge` does.
fn apply_merges(cast: &mut Cast, mint: &mut Mint) -> Result<(), GenError> {
    for plan in planned_merges() {
        let from = cast.id(plan.from)?;
        let into = cast.id(plan.into)?;
        let id = MergeId::from_ulid(mint.ulid(plan.at));
        let prior = record(cast, from)
            .and_then(|r| crate::backend::fixture::store::active_state(&r.agent.state))
            .ok_or_else(|| GenError::Missing(format!("active state of {}", plan.from)))?;
        let repointed: Vec<AgentId> = cast
            .records
            .iter()
            .filter(|r| matches!(r.agent.state, AgentState::Merged { into: i, .. } if i == from))
            .map(|r| r.agent.id)
            .collect();
        for agent in &repointed {
            if let Some(r) = record_mut(cast, *agent)
                && let AgentState::Merged { into: target, .. } = &mut r.agent.state
            {
                *target = into;
            }
        }
        if let Some(r) = record_mut(cast, from) {
            r.agent.state = AgentState::Merged {
                into,
                at: plan.at,
                by: plan.by,
            };
        }
        cast.active_until.insert(from, plan.at);
        for agent in &repointed {
            cast.active_until.insert(*agent, plan.at);
        }
        cast.priors.insert(id, prior);
        cast.merges.push(MergeRecord {
            id,
            from,
            into,
            by: plan.by,
            at: plan.at,
            repointed,
            reverted: None,
        });
        if let Some((by, at)) = plan.reverted {
            revert(cast, id, by, at)?;
        }
    }
    Ok(())
}

fn revert(cast: &mut Cast, merge: MergeId, by: OperatorId, at: Timestamp) -> Result<(), GenError> {
    let index = cast
        .merges
        .iter()
        .position(|m| m.id == merge)
        .ok_or_else(|| GenError::Missing("merge to revert".to_owned()))?;
    let prior = *cast
        .priors
        .get(&merge)
        .ok_or_else(|| GenError::Missing("merge prior".to_owned()))?;
    let (from, into) = (cast.merges[index].from, cast.merges[index].into);
    if let Some(r) = record_mut(cast, from) {
        r.agent.state = crate::backend::fixture::store::from_active(prior);
    }
    cast.active_until.remove(&from);
    cast.merges[index].reverted = Some((by, at));
    cast.vetoes.push(MergeVeto {
        a: from,
        b: into,
        by,
        at,
    });
    Ok(())
}

fn record(cast: &Cast, id: AgentId) -> Option<&AgentRecord> {
    cast.records.iter().find(|r| r.agent.id == id)
}

fn record_mut(cast: &mut Cast, id: AgentId) -> Option<&mut AgentRecord> {
    cast.records.iter_mut().find(|r| r.agent.id == id)
}

fn digest(rng: &mut Rng) -> Blake3 {
    Blake3::from_bytes(rng.bytes32())
}

fn evidence(spec: &Spec, rng: &mut Rng) -> Result<NonEmpty<IdentityEvidence>, GenError> {
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
    if matches!(spec.state, Planned::Registered) {
        items = vec![IdentityEvidence::StableCredential(credential)];
    }
    NonEmpty::from_vec(items).ok_or_else(|| GenError::Missing("identity evidence".to_owned()))
}

fn claude_code_claim() -> HarnessClaim {
    HarnessClaim {
        family: HarnessFamily::ClaudeCode,
        version: Some("2.1.30".to_owned()),
        user_agent: "claude-cli/2.1.30 (external, cli)".to_owned(),
    }
}

fn own_claim(family: Family) -> HarnessClaim {
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

/// The harness claims seen on each agent's exchanges. Impersonators claim
/// Claude Code on their Claude subscription traffic and their own family
/// elsewhere. Agents that never sent traffic have no claims.
pub fn claims(
    cast: &Cast,
    last_activity: &HashMap<AgentId, Timestamp>,
) -> HashMap<AgentId, Vec<ClaimSeen>> {
    let mut out = HashMap::new();
    for record in &cast.records {
        let id = record.agent.id;
        let (Some(family), Some(last)) = (cast.families.get(&id), last_activity.get(&id)) else {
            continue;
        };
        let mut seen = Vec::new();
        if cast.impersonators.contains(&id) {
            seen.push(ClaimSeen {
                claim: claude_code_claim(),
                last_seen: *last,
            });
            seen.push(ClaimSeen {
                claim: own_claim(*family),
                last_seen: minus(*last, 7 * HOUR),
            });
        } else {
            seen.push(ClaimSeen {
                claim: own_claim(*family),
                last_seen: *last,
            });
        }
        out.insert(id, seen);
    }
    out
}
