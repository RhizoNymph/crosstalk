//! ct-eval truth as bench label rows.
//!
//! A world's labels are one `exchange_agent` row per exchange, in exchange
//! order, then one row per ct-eval [`Expectation`] in truth order, with id
//! `t<index>` (its index in the world's truth), then any extra rows a
//! source adds (the demo swarm's key groups, `k<key group>`).
//!
//! | ct-eval | bench |
//! | --- | --- |
//! | `Transmission` | `transmission` |
//! | `AccessOnly` | `access_only` |
//! | `NoTransmission` | `negative_control` |
//! | `Unjudged` | `exemption` |
//! | `AgentCluster` (keys that are one agent) | `agent_cluster`, kind `identity`: refused ([`Gap::ClusterRow`]) |
//!
//! Locations: a label's content and an exemption's place are in its reader
//! exchange. A negative control's place is in its reader exchange when it
//! names one; ct-eval also writes controls whose place names a message but
//! no exchange (every exchange of the reader that carries it: SALT's and
//! AgentDojo's shared prompts and boilerplate) and origins that name no
//! exchange (SALT's rejected sends). Those are placed in the first exchange
//! of the reader (or, for an origin, the sender) that carries the message,
//! and the control's `reader_exchange` stays absent, so it still covers
//! every exchange. A place in a message no exchange carries (τ²-bench's
//! controls on a record after the reader's last call) has no bench form
//! ([`Gap::UncarriedLocation`]).
//!
//! An `agent_cluster` row cannot be written today: the format's row tag and
//! the cluster's kind are both `kind`, so the line holds the field twice
//! and no reader accepts it ([`Gap::ClusterRow`]). Clusters are built and
//! checked (`AgentCluster::new`), then refused.

use a2a_bench_format as bench;
use bench::labels::{
    AgentCluster, ClusterFields, ClusterKind, ControlFields, ExchangeAgent, Exemption,
    ExemptionFields, ExpectedAccess, ExpectedContent, ExpectedTransmission, Label, NegativeControl,
    Route, TransmissionFields,
};
use crosstalk_spec::ids::ExchangeId;

use super::kinds;
use super::world::MessageIndex;
use super::{Gap, GoldenError, ids, resource};
use crate::keys::{AgentKey, WorldKey};
use crate::truth::{AgentCluster as EvalCluster, Expectation, RouteExpectation, TransmissionLabel};

/// One `exchange_agent` row per exchange.
pub fn exchange_agents(owners: &[(ExchangeId, String)]) -> Result<Vec<Label>, GoldenError> {
    owners
        .iter()
        .map(|(exchange, agent)| {
            Ok(Label::ExchangeAgent(ExchangeAgent {
                exchange: ids::exchange(*exchange),
                agent: ids::agent(agent)?,
            }))
        })
        .collect()
}

/// The world's truth rows, `t<index>`.
pub fn truth(
    world: &WorldKey,
    truth: &[Expectation],
    index: &MessageIndex,
) -> Result<Vec<Label>, GoldenError> {
    let mut out = Vec::with_capacity(truth.len());
    for (at, expectation) in truth.iter().enumerate() {
        let id = ids::label(format!("t{at}"))?;
        let invalid = |source| GoldenError::Label {
            label: id.to_string(),
            source,
        };
        out.push(match expectation {
            Expectation::Transmission(expected) => Label::Transmission(
                ExpectedTransmission::new(transmission(
                    world,
                    id.clone(),
                    expected.label(),
                    index,
                )?)
                .map_err(invalid)?,
            ),
            Expectation::AccessOnly(expected) => Label::AccessOnly(
                ExpectedAccess::new(transmission(world, id.clone(), expected.label(), index)?)
                    .map_err(invalid)?,
            ),
            Expectation::NoTransmission(control) => {
                let label = control.label();
                let from = name(world, &label.from)?;
                let to = name(world, &label.to)?;
                let uncarried = |message| {
                    GoldenError::Unexpressible(Gap::UncarriedLocation {
                        label: id.to_string(),
                        message,
                    })
                };
                let at = match &label.at {
                    Some(at) => {
                        let exchange = match label.reader_exchange {
                            Some(reader) => reader,
                            None => index
                                .carrier(at.part.message, &label.to.name)
                                .map_err(|_| uncarried(at.part.message))?,
                        };
                        Some(index.location(exchange, at)?)
                    }
                    None => None,
                };
                let origin = match &label.origin {
                    Some(origin) => {
                        let exchange = index
                            .carrier(origin.part.message, &label.from.name)
                            .map_err(|_| uncarried(origin.part.message))?;
                        Some(index.location(exchange, origin)?)
                    }
                    None => None,
                };
                Label::NegativeControl(
                    NegativeControl::new(ControlFields {
                        id: id.clone(),
                        from,
                        to,
                        reader_exchange: label.reader_exchange.map(ids::exchange),
                        at,
                        origin,
                        text: label.text.clone(),
                        reason: kinds::negative(label.reason),
                        tier: kinds::tier(label.tier),
                        source: ids::source(&label.source),
                    })
                    .map_err(invalid)?,
                )
            }
            Expectation::Unjudged(exemption) => Label::Exemption(
                Exemption::new(ExemptionFields {
                    id: id.clone(),
                    to: name(world, &exemption.to)?,
                    reader_exchange: ids::exchange(exemption.reader_exchange),
                    at: index.location(exemption.reader_exchange, &exemption.at)?,
                    text: exemption.text.clone(),
                    reason: kinds::exemption(exemption.reason),
                    tier: kinds::tier(exemption.tier),
                    source: ids::source(&exemption.source),
                })
                .map_err(invalid)?,
            ),
            Expectation::AgentCluster(cluster) => {
                identity(world, id.clone(), cluster)?;
                return Err(GoldenError::Unexpressible(Gap::ClusterRow {
                    label: id.to_string(),
                }));
            }
        });
    }
    Ok(out)
}

/// A ct-eval cluster: keys that are one agent.
fn identity(
    world: &WorldKey,
    id: bench::ids::LabelId,
    cluster: &EvalCluster,
) -> Result<AgentCluster, GoldenError> {
    let label = cluster.label();
    let agents = label
        .agents
        .iter()
        .map(|agent| name(world, agent))
        .collect::<Result<Vec<_>, _>>()?;
    AgentCluster::new(ClusterFields {
        id: id.clone(),
        agents,
        kind: ClusterKind::Identity,
        tier: kinds::tier(label.tier),
        source: ids::source(&label.source),
    })
    .map_err(|source| GoldenError::Label {
        label: id.to_string(),
        source,
    })
}

fn transmission(
    world: &WorldKey,
    id: bench::ids::LabelId,
    label: &TransmissionLabel,
    index: &MessageIndex,
) -> Result<TransmissionFields, GoldenError> {
    Ok(TransmissionFields {
        id,
        from: name(world, &label.from)?,
        to: name(world, &label.to)?,
        sender_exchange: label.sender_exchange.map(ids::exchange),
        reader_exchange: ids::exchange(label.reader_exchange),
        route: route(&label.route)?,
        carrier: kinds::carrier(label.carrier),
        content: ExpectedContent {
            text: label.content.text.clone(),
            at: index.location(label.reader_exchange, &label.content.at)?,
        },
        needs: kinds::need(&label.needs),
        tier: kinds::tier(label.tier),
        source: ids::source(&label.source),
    })
}

pub fn route(route: &RouteExpectation) -> Result<Route, GoldenError> {
    Ok(match route {
        RouteExpectation::Channel { resource: locator } => Route::Channel {
            resource: resource::resource(locator)?,
        },
        RouteExpectation::Delegation { direction } => Route::Delegation {
            direction: kinds::direction(*direction),
        },
        RouteExpectation::Direct => Route::Direct,
        RouteExpectation::Unobserved => Route::Unobserved,
    })
}

/// An agent of `world` by name; an agent of another world is refused.
fn name(world: &WorldKey, agent: &AgentKey) -> Result<bench::ids::AgentKey, GoldenError> {
    if agent.world != *world {
        return Err(GoldenError::ForeignAgent {
            agent: agent.to_string(),
        });
    }
    ids::agent(&agent.name)
}
