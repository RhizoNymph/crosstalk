//! Predictions: what a detector reports, in the eval's terms.
//!
//! A [`Prediction`] is one content match of one transmission: who sent, who
//! read, at which reader exchange and location, by which route and carrier,
//! and how strong the match was. [`from_transmission`] turns a spec
//! `Transmission` (with its `ContentMatch`es) into predictions, so the
//! gateway's output plugs in unchanged; the [`Directory`] maps the gateway's
//! ids back to corpus keys.

use std::collections::BTreeMap;

use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::aggregates::quality::MatchClass;
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::derived::flow::transmission::DelegationDirection;
use crosstalk_spec::derived::flow::transmission::{Route, Transmission};
use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::ids::{AgentId, ChannelId, ExchangeId, MessageHash, SpanId, TransmissionId};
use serde::{Deserialize, Serialize};

use crate::corpus::World;
use crate::keys::AgentKey;
use crate::location::sort_key;
use crate::truth::CarrierKind;

/// The route a detector chose; a channel is named by the resources it holds.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PredictedRoute {
    Channel { resources: Vec<Locator> },
    Delegation { direction: DelegationDirection },
    Direct,
    Unobserved,
}

impl Prediction {
    /// A total order for predictions (spec locations and locators have
    /// none): by reader exchange, location, sender, transmission.
    pub fn sort_key(
        &self,
    ) -> (
        ExchangeId,
        (MessageHash, u16, u32, u32),
        &AgentKey,
        TransmissionId,
    ) {
        (
            self.reader_exchange,
            sort_key(&self.read_at),
            &self.from,
            self.transmission,
        )
    }
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

/// One content match a detector reported.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Prediction {
    /// The detector's transmission this match belongs to, so matches can be
    /// grouped back into transmissions.
    pub transmission: TransmissionId,
    pub from: AgentKey,
    pub to: AgentKey,
    pub reader_exchange: ExchangeId,
    pub route: PredictedRoute,
    pub carrier: CarrierKind,
    pub class: MatchClass,
    pub read_at: SpanLocation,
    /// Where the matched span sits in the sender's output, when the
    /// detector's directory knows it.
    pub origin_at: Option<SpanLocation>,
}

/// Maps the detector's ids back to the corpus.
pub trait Directory {
    fn agent(&self, id: AgentId) -> Option<AgentKey>;

    /// The canonical resources a channel holds; empty when unknown.
    fn channel(&self, id: ChannelId) -> Vec<Locator>;

    /// Where an originated span sits; `None` when unknown.
    ///
    /// TODO(docs/spec-eval-gaps): read it through `SpanIndex::span`.
    fn span(&self, id: SpanId) -> Option<SpanLocation>;
}

/// A world's agents plus the channels a detector registered.
pub struct WorldDirectory<'a> {
    world: &'a World,
    channels: BTreeMap<ChannelId, Vec<Locator>>,
    spans: BTreeMap<SpanId, SpanLocation>,
}

impl<'a> WorldDirectory<'a> {
    pub fn new(world: &'a World, channels: BTreeMap<ChannelId, Vec<Locator>>) -> Self {
        Self {
            world,
            channels,
            spans: BTreeMap::new(),
        }
    }

    /// The same, with the locations of the detector's spans.
    pub fn with_spans(mut self, spans: BTreeMap<SpanId, SpanLocation>) -> Self {
        self.spans = spans;
        self
    }
}

impl Directory for WorldDirectory<'_> {
    fn agent(&self, id: AgentId) -> Option<AgentKey> {
        self.world.agent_by_id(id).map(|agent| agent.key.clone())
    }

    fn channel(&self, id: ChannelId) -> Vec<Locator> {
        self.channels.get(&id).cloned().unwrap_or_default()
    }

    fn span(&self, id: SpanId) -> Option<SpanLocation> {
        self.spans.get(&id).copied()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PredictError {
    #[error("agent {0:?} is not in the directory")]
    UnknownAgent(AgentId),
}

/// The predictions a transmission makes: one per content match of a
/// confirmed (or classified, or aggregated) transmission, none for one the
/// detector has not confirmed (no sender is known there).
pub fn from_transmission(
    transmission: &Transmission,
    directory: &impl Directory,
) -> Result<Vec<Prediction>, PredictError> {
    let Some(confirmed) = transmission.state.confirmed() else {
        return Ok(Vec::new());
    };
    let from = directory
        .agent(confirmed.from())
        .ok_or(PredictError::UnknownAgent(confirmed.from()))?;
    let to = directory
        .agent(transmission.to)
        .ok_or(PredictError::UnknownAgent(transmission.to))?;
    let route = match &transmission.route {
        Route::Channel(channel) => PredictedRoute::Channel {
            resources: directory.channel(*channel),
        },
        Route::Delegation(direction) => PredictedRoute::Delegation {
            direction: *direction,
        },
        Route::Direct(_) => PredictedRoute::Direct,
        Route::Unobserved => PredictedRoute::Unobserved,
    };
    let id = transmission.id;
    Ok(confirmed
        .content()
        .iter()
        .map(|content| Prediction {
            transmission: id,
            from: from.clone(),
            to: to.clone(),
            reader_exchange: content.reader_exchange(),
            route: route.clone(),
            carrier: content.carrier().kind(),
            class: MatchClass::from(content.kind()),
            read_at: content.read_at(),
            origin_at: directory.span(content.origin()),
        })
        .collect())
}
