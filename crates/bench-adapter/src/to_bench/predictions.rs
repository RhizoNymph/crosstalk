//! A detection as bench prediction rows.
//!
//! The rows of one world, in order:
//!
//! 1. `attribution`: each detector agent (its spec `AgentId`'s ULID text)
//!    and the world exchanges the detector attributed to it (L3's
//!    placement of each exchange, [`held`]), by agent id;
//! 2. `unattributed`: every agent the evidence names that holds no
//!    exchange (a sender the detector cannot place), by agent id;
//! 3. `transmission`: every transmission of the detection, by id, with its
//!    state, its quality (`QualityMatch`) and evidence: one `matches` entry per `ContentMatch`
//!    of a confirmed, classified or aggregated one, one `co_access` entry
//!    per `CoAccess` record of a suspected or discarded one, none for a
//!    detected or awaiting-content one.
//!
//! A transmission's matches are sorted by their read location and its
//! co-access records by (read, write), by `Location`'s order, as the format
//! requires; ties by origin, then the row.
//!
//! Locations translate by exchange: a content match's read location is in
//! its reader exchange, its origin (the matched span's `IndexedSpan`) in the
//! span's exchange, a co-access's read and write in their accesses'
//! exchanges. A content match's channel route names every canonical
//! resource the detector's channel holds (`PredictedRoute::Channel`, none
//! when the channel is unknown). A co-access names one resource: the
//! channel's, when it holds exactly one, else the read access's own.

use std::collections::{BTreeMap, BTreeSet};

use a2a_bench_format as bench;
use bench::location::Location;
use bench::predictions::{
    Attribution, CoAccess, ContentEvidence, PredictedRoute, Prediction, Quality, State,
    Transmission, TransmissionFields, Unattributed,
};
use crosstalk_spec::aggregates::quality::QualityMatch;
use crosstalk_spec::derived::flow::access::{AccessKind, AccessOp};
use crosstalk_spec::derived::flow::evidence::CoAccess as SpecCoAccess;
use crosstalk_spec::derived::flow::transmission::{
    Route as SpecRoute, Transmission as SpecTransmission, TransmissionState,
};
use crosstalk_spec::derived::flow::verdict::Judgeable;
use crosstalk_spec::ids::{AgentId, ExchangeId};

use super::world::MessageIndex;
use super::{Lossy, ToBenchError, ids, kinds, resource};
use crate::directory::Directory;

/// How [`rows`] treats a transmission it cannot locate (an access the
/// evidence lacks, a part or exchange outside the world).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unlocated {
    /// Fail the world (`ct-bench-detect`'s live mode).
    Fail,
    /// Drop the transmission and count it (`from-export`: evidence that
    /// names an exchange outside the captured run window).
    Drop,
}

/// Each agent's exchanges, from a detector's agent of each exchange.
pub fn held(
    attribution: &BTreeMap<ExchangeId, AgentId>,
) -> BTreeMap<AgentId, BTreeSet<ExchangeId>> {
    let mut held: BTreeMap<AgentId, BTreeSet<ExchangeId>> = BTreeMap::new();
    for (exchange, agent) in attribution {
        held.entry(*agent).or_default().insert(*exchange);
    }
    held
}

/// The prediction rows of one world's detection (module docs). `held` is
/// each detector agent's exchanges, by canonical id (an exchange held twice
/// is written as such, and `check_predictions` refuses it); `aliases` maps
/// any other id the evidence names to its canonical one.
pub fn rows(
    transmissions: &[SpecTransmission],
    directory: &impl Directory,
    held: &BTreeMap<AgentId, BTreeSet<ExchangeId>>,
    aliases: &BTreeMap<AgentId, AgentId>,
    unlocated: Unlocated,
    index: &MessageIndex,
    lossy: &mut Lossy,
) -> Result<Vec<Prediction>, ToBenchError> {
    let mut sorted: Vec<&SpecTransmission> = transmissions.iter().collect();
    sorted.sort_by_key(|transmission| transmission.id);
    let mut named = BTreeSet::new();
    let mut rows = Vec::with_capacity(sorted.len());
    let canonical = |agent: AgentId| aliases.get(&agent).copied().unwrap_or(agent);
    for transmission in sorted {
        let mut names = BTreeSet::new();
        match convert(
            transmission,
            directory,
            index,
            &canonical,
            &mut names,
            lossy,
        ) {
            Ok(row) => {
                named.extend(names);
                rows.push(Prediction::Transmission(row));
            }
            Err(
                ToBenchError::UnknownAccess(_)
                | ToBenchError::WrongAccess { .. }
                | ToBenchError::UnlocatedAccess { .. }
                | ToBenchError::UnknownExchange(_)
                | ToBenchError::UnknownMessage(_),
            ) if unlocated == Unlocated::Drop => lossy.dropped_transmissions += 1,
            Err(error) => return Err(error),
        }
    }
    let mut out = Vec::with_capacity(held.len() + rows.len());
    for (agent, exchanges) in held {
        out.push(Prediction::Attribution(Attribution {
            agent: ids::detector_agent(*agent)?,
            exchanges: exchanges.iter().copied().map(ids::exchange).collect(),
        }));
    }
    for agent in named.iter().filter(|agent| !held.contains_key(agent)) {
        lossy.unattributed_agents += 1;
        out.push(Prediction::Unattributed(Unattributed {
            agent: ids::detector_agent(*agent)?,
        }));
    }
    out.extend(rows);
    Ok(out)
}

/// One transmission row; the agents its evidence names go to `named`.
fn convert(
    transmission: &SpecTransmission,
    directory: &impl Directory,
    index: &MessageIndex,
    canonical: &impl Fn(AgentId) -> AgentId,
    named: &mut BTreeSet<AgentId>,
    lossy: &mut Lossy,
) -> Result<Transmission, ToBenchError> {
    let id = ids::transmission(transmission.id)?;
    let state = state(&transmission.state);
    let mut fields = TransmissionFields {
        id: id.clone(),
        state,
        quality: None,
        matches: Vec::new(),
        co_access: Vec::new(),
    };
    if let Ok(judgeable) = transmission.state.judgeable() {
        fields.quality = Some(quality(QualityMatch::from(judgeable)));
        match judgeable {
            Judgeable::Confirmed(confirmed) => {
                let route = route(&transmission.route, directory);
                let (from, to) = (canonical(confirmed.from()), canonical(transmission.to));
                named.insert(from);
                named.insert(to);
                for content in confirmed.content().iter() {
                    let origin_at = match directory.span(content.origin()) {
                        Some(span) => Some(index.location(span.exchange, &span.location)?),
                        None => None,
                    };
                    fields.matches.push(ContentEvidence {
                        from: ids::detector_agent(from)?,
                        to: ids::detector_agent(to)?,
                        reader_exchange: ids::exchange(content.reader_exchange()),
                        read_at: index.location(content.reader_exchange(), &content.read_at())?,
                        origin_at,
                        kind: kinds::match_kind(content.kind(), lossy),
                        carrier: kinds::carrier(content.carrier().kind()),
                        route: route.clone(),
                    });
                }
            }
            Judgeable::Suspected(records) | Judgeable::Discarded(records) => {
                for record in records.iter() {
                    fields.co_access.push(co_access(
                        transmission,
                        record,
                        directory,
                        index,
                        canonical,
                        named,
                    )?);
                }
            }
        }
    }
    // a2a-bench/1 requires a transmission's matches sorted by `read_at` and
    // its co-access records by `(read_at, write_at)`, by `Location`'s order.
    // Ties keep a fixed order (origin, then the rest of the row), never the
    // detector's: that follows ids derived from spec message hashes, which
    // a bench reader re-derives differently (dropped signatures), so this
    // keeps one detection's bytes the same from either side (parity P5).
    fields.matches.sort_by(|a, b| {
        Location::cmp(&a.read_at, &b.read_at)
            .then_with(|| a.origin_at.cmp(&b.origin_at))
            .then_with(|| format!("{a:?}").cmp(&format!("{b:?}")))
    });
    fields.co_access.sort_by(|a, b| {
        Location::cmp(&a.read_at, &b.read_at)
            .then_with(|| Location::cmp(&a.write_at, &b.write_at))
            .then_with(|| format!("{a:?}").cmp(&format!("{b:?}")))
    });
    Transmission::new(fields).map_err(|source| ToBenchError::Transmission {
        transmission: id.to_string(),
        source,
    })
}

/// One co-access record: the write's agent to the read's agent, the read's
/// whole tool result and the write's whole tool call.
fn co_access(
    transmission: &SpecTransmission,
    record: &SpecCoAccess,
    directory: &impl Directory,
    index: &MessageIndex,
    canonical: &impl Fn(AgentId) -> AgentId,
    named: &mut BTreeSet<AgentId>,
) -> Result<CoAccess, ToBenchError> {
    let (write, _) = directory
        .access(record.write())
        .ok_or(ToBenchError::UnknownAccess(record.write()))?;
    let (read, read_resource) = directory
        .access(record.read())
        .ok_or(ToBenchError::UnknownAccess(record.read()))?;
    let AccessOp::Write { call, .. } = &write.op else {
        return Err(ToBenchError::WrongAccess {
            access: write.id,
            expected: AccessKind::Write,
        });
    };
    let AccessOp::Read { result } = &read.op else {
        return Err(ToBenchError::WrongAccess {
            access: read.id,
            expected: AccessKind::Read,
        });
    };
    let read_at = directory
        .whole_part(read.exchange, *result)
        .ok_or(ToBenchError::UnlocatedAccess { access: read.id })?;
    let write_at = directory
        .whole_part(write.exchange, *call)
        .ok_or(ToBenchError::UnlocatedAccess { access: write.id })?;
    let resource = match route(&transmission.route, directory) {
        PredictedRoute::Channel { mut resources } if resources.len() == 1 => resources.remove(0),
        _ => resource::resource(&read_resource.locator),
    };
    let (from, to) = (canonical(write.agent), canonical(read.agent));
    named.insert(from);
    named.insert(to);
    Ok(CoAccess {
        from: ids::detector_agent(from)?,
        to: ids::detector_agent(to)?,
        write_exchange: ids::exchange(write.exchange),
        write_at: index.location(write.exchange, &write_at)?,
        reader_exchange: ids::exchange(read.exchange),
        read_at: index.location(read.exchange, &read_at)?,
        resource,
    })
}

fn route(route: &SpecRoute, directory: &impl Directory) -> PredictedRoute {
    match route {
        SpecRoute::Channel(channel) => PredictedRoute::Channel {
            resources: directory
                .channel(*channel)
                .unwrap_or_default()
                .iter()
                .map(resource::resource)
                .collect(),
        },
        SpecRoute::Delegation(direction) => PredictedRoute::Delegation {
            direction: kinds::direction(*direction),
        },
        SpecRoute::Direct(_) => PredictedRoute::Direct,
        SpecRoute::Unobserved => PredictedRoute::Unobserved,
    }
}

fn state(state: &TransmissionState) -> State {
    match state {
        TransmissionState::Detected => State::Detected,
        TransmissionState::AwaitingContent { .. } => State::AwaitingContent,
        TransmissionState::Suspected { .. } => State::Suspected,
        TransmissionState::Confirmed(_) => State::Confirmed,
        TransmissionState::Classified { .. } => State::Classified,
        TransmissionState::Aggregated { .. } => State::Aggregated,
        TransmissionState::Discarded { .. } => State::Discarded,
    }
}

fn quality(quality: QualityMatch) -> Quality {
    match quality {
        QualityMatch::Content { class, carrier } => Quality::Content {
            class: kinds::class(class),
            carrier: kinds::carrier(carrier),
        },
        QualityMatch::Suspected => Quality::Suspected,
        QualityMatch::Discarded => Quality::Discarded,
    }
}
