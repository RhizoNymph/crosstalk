//! The correlator's unit and property tests, and the fixtures the
//! consumer's tests share.

use std::collections::{BTreeMap, BTreeSet};

use crosstalk_spec::ids::{AccessId, AgentId, TransmissionId};
use crosstalk_spec::interfaces::l5_flow::TransmissionUpdate;

use super::Decided;
use super::lifecycle::{self, Stage, UpdateKind};
use super::medium::MatchKey;

pub(crate) mod fixtures;

mod channel;
mod handoff;
mod pairing;
mod props;
mod routes;
mod shared_web;

/// The kinds of `decided`, in order.
fn kinds(decided: &[Decided]) -> Vec<UpdateKind> {
    decided
        .iter()
        .map(|decided| UpdateKind::of(&decided.update))
        .collect()
}

/// The transmission an update is about.
fn subject(update: &TransmissionUpdate) -> TransmissionId {
    match update {
        TransmissionUpdate::OpenChannel { transmission, .. }
        | TransmissionUpdate::OpenConfirmed { transmission, .. }
        | TransmissionUpdate::Extend { transmission, .. }
        | TransmissionUpdate::Confirm { transmission, .. }
        | TransmissionUpdate::Suspect { transmission, .. }
        | TransmissionUpdate::Discard { transmission } => *transmission,
    }
}

/// What a stream of updates left a transmission as: its reader, its route
/// (`Debug` of the opening's `on` or `route`), its stage, its content and
/// its co-accesses, all as sets.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Final {
    to: AgentId,
    route: String,
    stage: Stage,
    content: BTreeSet<MatchKey>,
    co_access: BTreeSet<(AccessId, AccessId)>,
}

/// Fold `decided` into each transmission's final state, checking every
/// step is one the lifecycle admits.
fn fold(decided: &[Decided]) -> Result<BTreeMap<TransmissionId, Final>, String> {
    let mut finals: BTreeMap<TransmissionId, Final> = BTreeMap::new();
    for decided in decided {
        let id = subject(&decided.update);
        let kind = UpdateKind::of(&decided.update);
        let from = finals.get(&id).map(|known| known.stage);
        let stage = lifecycle::advance(from, kind)
            .map_err(|illegal| format!("{}: {illegal:?}", id.ulid_text()))?;
        match &decided.update {
            TransmissionUpdate::OpenChannel {
                to, on, co_access, ..
            } => {
                finals.insert(
                    id,
                    Final {
                        to: *to,
                        route: format!("{on:?}"),
                        stage,
                        content: BTreeSet::new(),
                        co_access: BTreeSet::from([(co_access.write(), co_access.read())]),
                    },
                );
            }
            TransmissionUpdate::OpenConfirmed {
                to,
                route,
                confirmed,
                ..
            } => {
                finals.insert(
                    id,
                    Final {
                        to: *to,
                        route: format!("{route:?}"),
                        stage,
                        content: confirmed.content().iter().map(MatchKey::of).collect(),
                        co_access: BTreeSet::new(),
                    },
                );
            }
            TransmissionUpdate::Confirm { confirmed, .. } => {
                if let Some(known) = finals.get_mut(&id) {
                    known.stage = stage;
                    known.content = confirmed.content().iter().map(MatchKey::of).collect();
                    known.co_access = confirmed
                        .co_access()
                        .iter()
                        .map(|co| (co.write(), co.read()))
                        .collect();
                }
            }
            TransmissionUpdate::Extend { content, .. } => {
                if let Some(known) = finals.get_mut(&id) {
                    known.content.insert(MatchKey::of(content));
                }
            }
            TransmissionUpdate::Suspect { co_access, .. } => {
                if let Some(known) = finals.get_mut(&id) {
                    known.stage = stage;
                    known.co_access = co_access.iter().map(|co| (co.write(), co.read())).collect();
                }
            }
            TransmissionUpdate::Discard { .. } => {
                if let Some(known) = finals.get_mut(&id) {
                    known.stage = stage;
                }
            }
        }
    }
    Ok(finals)
}
