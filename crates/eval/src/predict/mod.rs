//! Predictions: what a detector reports, in the eval's terms.
//!
//! A [`Prediction`] is one piece of evidence of one transmission: who sent,
//! who read, at which reader exchange and location, by which route and
//! carrier, and what the evidence was. [`from_transmission`] turns a spec
//! `Transmission` into predictions, so any detector's output plugs in
//! unchanged:
//!
//! - confirmed (or classified, or aggregated): one per `ContentMatch`, of
//!   the match's class;
//! - suspected or discarded: one per `CoAccess` record, aligned through its
//!   two accesses (the sender is the write's agent, the reader the read's
//!   agent at the read's exchange, located at the whole tool result the
//!   read returned), of class [`EvidenceClass::Suspected`] or
//!   [`EvidenceClass::Discarded`];
//! - detected or awaiting content: none (the detector has not decided).
//!
//! Every prediction also carries its transmission's row in the spec's
//! `DetectionQuality` (`QualityMatch`).
//!
//! The [`Directory`] answers what the transmission only names by id: the
//! corpus agent behind a detector's agent id ([`AgentMap`]), and the spans,
//! accesses and channel resources read through the spec's read traits
//! ([`reads::Resolved`]).

pub mod memory;
pub mod reads;

use std::collections::BTreeMap;

use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::aggregates::quality::{MatchClass, QualityMatch};
use crosstalk_spec::derived::flow::access::{Access, AccessKind, AccessOp};
use crosstalk_spec::derived::flow::evidence::CoAccess;
use crosstalk_spec::derived::flow::resource::{Locator, Resource};
use crosstalk_spec::derived::flow::transmission::DelegationDirection;
use crosstalk_spec::derived::flow::transmission::{Route, Transmission};
use crosstalk_spec::derived::flow::verdict::Judgeable;
use crosstalk_spec::derived::provenance::matching::CarrierKind;
use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::ids::{
    AccessId, AgentId, ChannelId, ExchangeId, MessageHash, SpanId, TransmissionId,
};
use crosstalk_spec::interfaces::l4_provenance::IndexedSpan;
use crosstalk_spec::observed::message::PartRef;
use crosstalk_spec::support::NonEmpty;
use serde::{Deserialize, Serialize};

use crate::corpus::World;
use crate::keys::AgentKey;
use crate::location::{sort_key, whole_part};
use reads::Resolved;

/// The route a detector chose; a channel is named by the resources it holds.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PredictedRoute {
    Channel { resources: Vec<Locator> },
    Delegation { direction: DelegationDirection },
    Direct,
    Unobserved,
}

impl PredictedRoute {
    pub fn kind(&self) -> RouteKind {
        match self {
            Self::Channel { .. } => RouteKind::Channel,
            Self::Delegation { .. } => RouteKind::Delegation,
            Self::Direct => RouteKind::Direct,
            Self::Unobserved => RouteKind::Unobserved,
        }
    }
}

/// What a prediction rests on: a content match of a class (strongest
/// first), or only an access pattern, still suspected or discarded. A
/// label's need is always a content class. Rows are split by it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceClass {
    Exact,
    Normalized,
    Decoded,
    Semantic,
    /// A co-access only, with no content match yet.
    Suspected,
    /// A co-access that expired without content evidence.
    Discarded,
}

impl EvidenceClass {
    /// The match class of content evidence; `None` for access-only.
    pub fn content(self) -> Option<MatchClass> {
        match self {
            Self::Exact => Some(MatchClass::Exact),
            Self::Normalized => Some(MatchClass::Normalized),
            Self::Decoded => Some(MatchClass::Decoded),
            Self::Semantic => Some(MatchClass::Semantic),
            Self::Suspected | Self::Discarded => None,
        }
    }

    pub fn is_content(self) -> bool {
        self.content().is_some()
    }
}

impl From<MatchClass> for EvidenceClass {
    fn from(class: MatchClass) -> Self {
        match class {
            MatchClass::Exact => Self::Exact,
            MatchClass::Normalized => Self::Normalized,
            MatchClass::Decoded => Self::Decoded,
            MatchClass::Semantic => Self::Semantic,
        }
    }
}

/// One piece of evidence a detector reported.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Prediction {
    /// The detector's transmission this evidence belongs to, so predictions
    /// can be grouped back into transmissions.
    pub transmission: TransmissionId,
    pub from: AgentKey,
    pub to: AgentKey,
    pub reader_exchange: ExchangeId,
    pub route: PredictedRoute,
    pub carrier: CarrierKind,
    pub class: EvidenceClass,
    /// The transmission's row in the spec's `DetectionQuality`: its call
    /// and, when confirmed, its strongest match's class and carrier.
    pub quality: QualityMatch,
    pub read_at: SpanLocation,
    /// Where the matched span (or the write's tool call) sits in the
    /// sender's output, when the detector's stores know it.
    pub origin_at: Option<SpanLocation>,
}

impl Prediction {
    /// A total order for predictions (spec locations and locators have
    /// none): by reader exchange, location, sender, transmission, class.
    pub fn sort_key(
        &self,
    ) -> (
        ExchangeId,
        (MessageHash, u16, u32, u32),
        &AgentKey,
        TransmissionId,
        EvidenceClass,
    ) {
        (
            self.reader_exchange,
            sort_key(&self.read_at),
            &self.from,
            self.transmission,
            self.class,
        )
    }
}

/// A detector's agent ids, as corpus agents, and the detector's own
/// attribution of the world's exchanges (which of its agents made each).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AgentMap {
    agents: BTreeMap<AgentId, AgentKey>,
    exchanges: BTreeMap<ExchangeId, AgentId>,
}

/// Why a detector's attribution cannot be read as corpus agents.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AgentMapError {
    #[error("detector agent {agent:?} holds exchanges of corpus agents {first} and {second}")]
    Merged {
        agent: AgentId,
        first: AgentKey,
        second: AgentKey,
    },
    #[error("exchange {0:?} is not in the world")]
    UnknownExchange(ExchangeId),
}

impl AgentMap {
    /// The world's own agent ids: a detector that attributes exchanges by
    /// the corpus's ids (the reference matcher).
    pub fn of_world(world: &World) -> Self {
        let ids: BTreeMap<&AgentKey, AgentId> = world
            .agents()
            .iter()
            .map(|agent| (&agent.key, agent.id))
            .collect();
        Self {
            agents: world
                .agents()
                .iter()
                .map(|agent| (agent.id, agent.key.clone()))
                .collect(),
            exchanges: world
                .exchanges()
                .iter()
                .filter_map(|exchange| {
                    ids.get(exchange.agent())
                        .map(|agent| (exchange.id(), *agent))
                })
                .collect(),
        }
    }

    /// A detector's own attribution of the world's exchanges (L3's agent of
    /// each exchange): each detector agent is the corpus agent whose
    /// exchanges it holds. Several detector agents may stand for one corpus
    /// agent (a split); one detector agent holding two corpus agents'
    /// exchanges (a merge the corpus does not have) is refused, since its
    /// evidence could not be told apart.
    pub fn from_attribution(
        world: &World,
        attribution: &BTreeMap<ExchangeId, AgentId>,
    ) -> Result<Self, AgentMapError> {
        let mut agents: BTreeMap<AgentId, AgentKey> = BTreeMap::new();
        for (&exchange, &agent) in attribution {
            let key = world
                .exchange(exchange)
                .ok_or(AgentMapError::UnknownExchange(exchange))?
                .agent();
            match agents.get(&agent) {
                Some(known) if known != key => {
                    return Err(AgentMapError::Merged {
                        agent,
                        first: known.clone(),
                        second: key.clone(),
                    });
                }
                Some(_) => {}
                None => {
                    agents.insert(agent, key.clone());
                }
            }
        }
        Ok(Self {
            agents,
            exchanges: attribution.clone(),
        })
    }

    pub fn get(&self, id: AgentId) -> Option<&AgentKey> {
        self.agents.get(&id)
    }

    /// The detector's agent of each exchange it attributed: every world
    /// exchange for [`AgentMap::of_world`], the detector's own placements
    /// for [`AgentMap::from_attribution`].
    pub fn attribution(&self) -> &BTreeMap<ExchangeId, AgentId> {
        &self.exchanges
    }
}

/// What predictions look up by id.
pub trait Directory {
    fn agent(&self, id: AgentId) -> Option<AgentKey>;

    /// The canonical resources a channel holds; `None` when unknown.
    fn channel(&self, id: ChannelId) -> Option<&[Locator]>;

    /// The record of an originated span; `None` when unknown.
    fn span(&self, id: SpanId) -> Option<IndexedSpan>;

    /// A recorded access and its resource; `None` when unknown.
    fn access(&self, id: AccessId) -> Option<&(Access, Resource)>;

    /// The whole text of `part` as `exchange` carries it; `None` when the
    /// exchange or the part is unknown or has no text.
    fn whole_part(&self, exchange: ExchangeId, part: PartRef) -> Option<SpanLocation>;
}

/// A world, the detector's agents, and what was read through the seam.
pub struct WorldDirectory<'a> {
    world: &'a World,
    agents: &'a AgentMap,
    resolved: &'a Resolved,
}

impl<'a> WorldDirectory<'a> {
    pub fn new(world: &'a World, agents: &'a AgentMap, resolved: &'a Resolved) -> Self {
        Self {
            world,
            agents,
            resolved,
        }
    }
}

impl Directory for WorldDirectory<'_> {
    fn agent(&self, id: AgentId) -> Option<AgentKey> {
        self.agents.get(id).cloned()
    }

    fn channel(&self, id: ChannelId) -> Option<&[Locator]> {
        self.resolved.channel(id)
    }

    fn span(&self, id: SpanId) -> Option<IndexedSpan> {
        self.resolved.span(id).copied()
    }

    fn access(&self, id: AccessId) -> Option<&(Access, Resource)> {
        self.resolved.access(id)
    }

    fn whole_part(&self, exchange: ExchangeId, part: PartRef) -> Option<SpanLocation> {
        let message = self.world.exchange(exchange)?.message(part.message)?;
        whole_part(message, part.index).ok()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PredictError {
    #[error("agent {0:?} is not in the directory")]
    UnknownAgent(AgentId),
    #[error("access {0:?} is not in the directory")]
    UnknownAccess(AccessId),
    #[error("access {access:?} of a co-access record is not a {expected:?}")]
    WrongAccess {
        access: AccessId,
        expected: AccessKind,
    },
    #[error("the part {part:?} access {access:?} names is not in its exchange")]
    UnlocatedAccess { access: AccessId, part: PartRef },
}

/// The predictions a transmission makes (module docs).
pub fn from_transmission(
    transmission: &Transmission,
    directory: &impl Directory,
) -> Result<Vec<Prediction>, PredictError> {
    let Ok(judgeable) = transmission.state.judgeable() else {
        return Ok(Vec::new());
    };
    let quality = QualityMatch::from(judgeable);
    let route = match &transmission.route {
        Route::Channel(channel) => PredictedRoute::Channel {
            resources: directory
                .channel(*channel)
                .map(<[Locator]>::to_vec)
                .unwrap_or_default(),
        },
        Route::Delegation(direction) => PredictedRoute::Delegation {
            direction: *direction,
        },
        Route::Direct(_) => PredictedRoute::Direct,
        Route::Unobserved => PredictedRoute::Unobserved,
    };
    let agent = |id: AgentId| directory.agent(id).ok_or(PredictError::UnknownAgent(id));
    match judgeable {
        Judgeable::Confirmed(confirmed) => {
            let from = agent(confirmed.from())?;
            let to = agent(transmission.to)?;
            Ok(confirmed
                .content()
                .iter()
                .map(|content| Prediction {
                    transmission: transmission.id,
                    from: from.clone(),
                    to: to.clone(),
                    reader_exchange: content.reader_exchange(),
                    route: route.clone(),
                    carrier: content.carrier().kind(),
                    class: EvidenceClass::from(MatchClass::from(content.kind())),
                    quality,
                    read_at: content.read_at(),
                    origin_at: directory.span(content.origin()).map(|span| span.location),
                })
                .collect())
        }
        Judgeable::Suspected(co_access) => co_access_predictions(
            transmission,
            co_access,
            EvidenceClass::Suspected,
            quality,
            &route,
            directory,
        ),
        Judgeable::Discarded(co_access) => co_access_predictions(
            transmission,
            co_access,
            EvidenceClass::Discarded,
            quality,
            &route,
            directory,
        ),
    }
}

/// One prediction per co-access record: the write's agent to the read's
/// agent, at the read's exchange and the tool result it returned.
fn co_access_predictions(
    transmission: &Transmission,
    co_access: &NonEmpty<CoAccess>,
    class: EvidenceClass,
    quality: QualityMatch,
    route: &PredictedRoute,
    directory: &impl Directory,
) -> Result<Vec<Prediction>, PredictError> {
    co_access
        .iter()
        .map(|record| {
            let (write, _) = directory
                .access(record.write())
                .ok_or(PredictError::UnknownAccess(record.write()))?;
            let (read, _) = directory
                .access(record.read())
                .ok_or(PredictError::UnknownAccess(record.read()))?;
            let AccessOp::Write { call, .. } = &write.op else {
                return Err(PredictError::WrongAccess {
                    access: write.id,
                    expected: AccessKind::Write,
                });
            };
            let AccessOp::Read { result } = &read.op else {
                return Err(PredictError::WrongAccess {
                    access: read.id,
                    expected: AccessKind::Read,
                });
            };
            let read_at = directory.whole_part(read.exchange, *result).ok_or(
                PredictError::UnlocatedAccess {
                    access: read.id,
                    part: *result,
                },
            )?;
            Ok(Prediction {
                transmission: transmission.id,
                from: directory
                    .agent(write.agent)
                    .ok_or(PredictError::UnknownAgent(write.agent))?,
                to: directory
                    .agent(read.agent)
                    .ok_or(PredictError::UnknownAgent(read.agent))?,
                reader_exchange: read.exchange,
                route: route.clone(),
                carrier: CarrierKind::ToolResult,
                class,
                quality,
                read_at,
                origin_at: directory.whole_part(write.exchange, *call),
            })
        })
        .collect()
}
