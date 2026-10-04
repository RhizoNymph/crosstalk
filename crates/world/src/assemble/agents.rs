//! Agents as L3 recorded them: config registering the registered agents,
//! each other agent created by its first exchange and established when
//! the resolver corroborated it, the merges and the revert, the
//! researcher's renames, and the claims and activity their exchanges
//! carried.

use std::collections::BTreeMap;

use crosstalk_spec::ids::AgentId;
use crosstalk_spec::interfaces::l3_reconstruction::lifecycle::{Advance, AgentOrigin, NewAgent};
use crosstalk_spec::support::Timestamp;

use crate::clock::{DAY, plus};
use crate::config::OPERATOR_RESEARCHER;
use crate::error::WorldError;
use crate::generate::Generated;
use crate::generate::agents::{Planned, claims};
use crate::rng::Rng;
use crate::script::{Op, Script};

pub fn assemble(generated: &Generated, script: &mut Script) -> Result<(), WorldError> {
    let times = &generated.times;
    let cast = &generated.cast;
    for agent in &cast.agents {
        let (origin, at, label) = match (agent.state, agent.first_seen) {
            (Planned::Registered, _) => (
                AgentOrigin::Config {
                    at: times.config_at,
                },
                times.config_at,
                agent.label.clone(),
            ),
            (_, Some(first_seen)) => (AgentOrigin::Traffic { first_seen }, first_seen, None),
            (_, None) => {
                return Err(WorldError::missing(format!(
                    "first sighting of {}",
                    agent.key
                )));
            }
        };
        script.push(
            at,
            Op::CreateAgent(NewAgent {
                id: agent.id,
                evidence: agent.evidence.clone(),
                parent: agent.parent,
                origin,
                label,
            }),
        );
        if let Planned::Established { since } = agent.state {
            script.push(
                since,
                Op::Advance {
                    agent: agent.id,
                    advance: Advance::Establish { since },
                },
            );
        }
    }

    for merge in &cast.merges {
        script.push(
            merge.at,
            Op::Merge {
                key: merge.key,
                request: merge.request,
            },
        );
        if let Some((by, at)) = merge.reverted {
            script.push(at, Op::Unmerge { key: merge.key, by });
        }
    }

    // The researcher labelled the labelled agents that came from traffic
    // during the first five days; config labelled the registered ones.
    let mut rng = Rng::fork(generated.seed, "renames");
    let mut labelled: Vec<_> = cast
        .agents
        .iter()
        .filter(|agent| agent.state != Planned::Registered)
        .filter_map(|agent| agent.label.clone().map(|label| (agent.id, label)))
        .collect();
    labelled.sort_by_key(|(id, _)| *id);
    for (agent, label) in labelled {
        let at = plus(times.start, rng.below(5 * DAY));
        script.push(
            at,
            Op::Rename {
                agent,
                label,
                by: OPERATOR_RESEARCHER,
            },
        );
    }

    for (agent, last) in last_activity(generated) {
        let Some(planned) = cast.agent(agent) else {
            continue;
        };
        if planned.first_seen.is_some_and(|first| last > first) {
            script.push(last, Op::Activity { agent });
        }
        let impersonator = cast.impersonators.contains(&agent);
        for (claim, at) in claims(planned, impersonator, last) {
            // Never before the exchange that created the agent.
            let at = planned.first_seen.map_or(at, |first| at.max(first));
            script.push(at, Op::Claim { agent, claim });
        }
    }
    Ok(())
}

/// When each agent created from traffic was last seen: its first
/// exchange, or its last access or transmission after that.
fn last_activity(generated: &Generated) -> BTreeMap<AgentId, Timestamp> {
    let mut out: BTreeMap<AgentId, Timestamp> = generated
        .cast
        .agents
        .iter()
        .filter_map(|agent| Some((agent.id, agent.first_seen?)))
        .collect();
    let mut bump = |agent: AgentId, at: Timestamp| {
        if let Some(slot) = out.get_mut(&agent)
            && at > *slot
        {
            *slot = at;
        }
    };
    for access in &generated.traffic.accesses {
        bump(access.agent, access.at);
    }
    for record in &generated.traffic.transmissions {
        bump(record.transmission.to, record.transmission.opened_at);
        if let Some(from) = record.from {
            bump(from, record.transmission.opened_at);
        }
    }
    out
}
