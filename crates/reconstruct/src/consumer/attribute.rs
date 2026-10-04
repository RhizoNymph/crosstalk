//! Attribution: from an exchange's evidence to the agent it is attributed
//! to, through `IdentityResolver::resolve` and the lifecycle writes.
//!
//! - `New`: a new agent from traffic (`Provisional`, its first exchange
//!   recorded as activity in the same transaction), holding every item of
//!   evidence, with its parent derived from the harness ids
//!   (`reconstruct.agent.parent-derivation`).
//! - `Known`: the evidence the agent lacks is attached; a registered agent
//!   moves to `Provisional` on its first exchange
//!   (`reconstruct.agent-state.traffic-leaves-registered`); a provisional
//!   agent seen again (its deciding evidence corroborated by a later
//!   exchange) whose evidence spans two variants, one strong, becomes
//!   `Established`
//!   (`reconstruct.agent-state.established-needs-two-kinds`).
//! - `Conflict`: when every deciding item is strong, the resolver merges
//!   every other candidate into the oldest (lowest id) and the exchange is
//!   attributed to it; when any merge is refused (a veto), or the deciding
//!   evidence is weak, the exchange is left for operator review and
//!   attributed to no one (`reconstruct.resolve.conflict-not-silently-
//!   attributed`).

use crosstalk_spec::ids::AgentId;
use crosstalk_spec::interfaces::l3_reconstruction::lifecycle::{
    Advance, AgentLifecycleError, AgentOrigin, NewAgent,
};
use crosstalk_spec::interfaces::l3_reconstruction::{Resolution, ResolveError};
use crosstalk_spec::observed::agent::{
    AgentState, IdentityEvidence, MergeAuthor, MergeRequest, Strength,
};
use crosstalk_spec::observed::client::RequestClass;
use crosstalk_spec::observed::exchange::ExchangeMeta;
use crosstalk_spec::support::{NonEmpty, Timestamp};

use super::{AgentStore, ConsumeError};
use crate::evidence::{parent_agent_evidence, session_evidence};
use crate::ids::IdSource;

/// What resolution decided for one exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Attribution {
    /// Attributed to `agent`, which `seen` evidence was newly attributed
    /// to (each published once as `AgentSeen`).
    Agent {
        agent: AgentId,
        seen: Vec<IdentityEvidence>,
        created: bool,
    },
    /// Left for operator review: the evidence points at `candidates` and
    /// no merge settled it.
    Review {
        candidates: NonEmpty<AgentId>,
        evidence: NonEmpty<IdentityEvidence>,
    },
}

/// Whether `evidence` corroborates an agent: two different variants, at
/// least one strong.
pub fn corroborated(evidence: &[IdentityEvidence]) -> bool {
    let strong = evidence
        .iter()
        .any(|item| item.strength() == Strength::Strong);
    let first = evidence.first().map(std::mem::discriminant);
    let varied = evidence
        .iter()
        .any(|item| Some(std::mem::discriminant(item)) != first);
    strong && varied
}

/// The agent a resolution of `evidence` names when it is `Known`.
async fn known_holder<A: AgentStore>(
    agents: &A,
    evidence: Vec<IdentityEvidence>,
) -> Result<Option<AgentId>, ConsumeError> {
    let Some(evidence) = NonEmpty::from_vec(evidence) else {
        return Ok(None);
    };
    Ok(match agents.resolve(&evidence).await? {
        Resolution::Known { agent, .. } => Some(agent),
        Resolution::New { .. } | Resolution::Conflict { .. } => None,
    })
}

/// A new agent's parent: the agent holding the harness parent agent id in
/// the exchange's scope when the harness sent one; else, for a harness
/// sub-agent, its session's main agent; else none.
pub async fn derive_parent<A: AgentStore>(
    agents: &A,
    meta: &ExchangeMeta,
) -> Result<Option<AgentId>, ConsumeError> {
    let parent_ids = parent_agent_evidence(meta);
    if !parent_ids.is_empty() {
        return known_holder(agents, parent_ids).await;
    }
    let ids = &meta.client.ids;
    let subagent = ids.agent.is_some() || meta.client.class == RequestClass::Subagent;
    if subagent {
        return known_holder(agents, session_evidence(meta)).await;
    }
    Ok(None)
}

/// Attach `items` to `agent`, returning the ones attached now (another
/// consumer may have attached one first).
async fn attach<A: AgentStore>(
    agents: &mut A,
    agent: AgentId,
    items: Vec<IdentityEvidence>,
) -> Result<Vec<IdentityEvidence>, ConsumeError> {
    let mut seen = Vec::new();
    for item in items {
        match agents.attach_evidence(agent, item.clone()).await {
            Ok(()) => seen.push(item),
            Err(AgentLifecycleError::DuplicateEvidence(_)) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(seen)
}

/// Move a known agent forward after its exchange at `at`.
async fn advance<A: AgentStore>(
    agents: &mut A,
    agent: AgentId,
    at: Timestamp,
) -> Result<(), ConsumeError> {
    let Some(cluster) = agents.cluster(agent).await? else {
        return Ok(());
    };
    let record = cluster.agent();
    let step = match &record.state {
        AgentState::Registered { .. } => Some(Advance::FirstTraffic { at }),
        AgentState::Provisional { .. } => {
            let held: Vec<IdentityEvidence> = record.evidence.iter().cloned().collect();
            corroborated(&held).then_some(Advance::Establish { since: at })
        }
        AgentState::Established { .. } | AgentState::Merged(_) => None,
    };
    if let Some(step) = step {
        match agents.advance(agent, step).await {
            // Another consumer moved it first.
            Ok(()) | Err(AgentLifecycleError::IllegalTransition { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

/// Attribute an exchange with `meta` carrying `evidence`.
pub async fn attribute<A, X>(
    agents: &mut A,
    agent_ids: &X,
    meta: &ExchangeMeta,
    evidence: NonEmpty<IdentityEvidence>,
) -> Result<Attribution, ConsumeError>
where
    A: AgentStore,
    X: IdSource<AgentId>,
{
    let at = meta.started_at;
    match agents.resolve(&evidence).await? {
        Resolution::New { evidence } => {
            let parent = derive_parent(agents, meta).await?;
            let id = agent_ids.next_id(at).map_err(ConsumeError::Ids)?;
            agents
                .create(NewAgent {
                    id,
                    evidence: evidence.clone(),
                    parent,
                    origin: AgentOrigin::Traffic { first_seen: at },
                    label: None,
                })
                .await?;
            Ok(Attribution::Agent {
                agent: id,
                seen: evidence.into_vec(),
                created: true,
            })
        }
        Resolution::Known {
            agent,
            new_evidence,
        } => {
            let seen = attach(agents, agent, new_evidence).await?;
            advance(agents, agent, at).await?;
            Ok(Attribution::Agent {
                agent,
                seen,
                created: false,
            })
        }
        Resolution::Conflict {
            candidates,
            evidence: deciding,
        } => {
            let strong = deciding
                .iter()
                .all(|item| item.strength() == Strength::Strong);
            if !strong {
                return Ok(Attribution::Review {
                    candidates,
                    evidence: deciding,
                });
            }
            let target = *candidates.first();
            let others: Vec<AgentId> = candidates.iter().skip(1).copied().collect();
            for other in &others {
                let Ok(request) = MergeRequest::new(*other, target, MergeAuthor::Resolver) else {
                    continue;
                };
                match agents.merge(request, at).await {
                    Ok(_) | Err(ResolveError::MergeIntoSelf { .. }) => {}
                    Err(
                        ResolveError::Vetoed(_)
                        | ResolveError::AgentMerged { .. }
                        | ResolveError::UnknownAgent(_),
                    ) => {
                        return Ok(Attribution::Review {
                            candidates,
                            evidence: deciding,
                        });
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            match agents.resolve(&evidence).await? {
                Resolution::Known {
                    agent,
                    new_evidence,
                } => {
                    let seen = attach(agents, agent, new_evidence).await?;
                    advance(agents, agent, at).await?;
                    Ok(Attribution::Agent {
                        agent,
                        seen,
                        created: false,
                    })
                }
                Resolution::New { .. } | Resolution::Conflict { .. } => Ok(Attribution::Review {
                    candidates,
                    evidence: deciding,
                }),
            }
        }
    }
}
