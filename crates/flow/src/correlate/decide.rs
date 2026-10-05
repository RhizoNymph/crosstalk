//! Channel transmissions within one medium: pairing a write and a read
//! into a co-access, opening or joining the transmission of its reader
//! exchange and writer, and deciding each transmission as of a time
//! (confirm, suspect, extend, discard, and reopen for content that
//! arrived after a discard).

use std::collections::BTreeMap;

use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::evidence::CoAccess;
use crosstalk_spec::derived::flow::timing::CorrelationTiming;
use crosstalk_spec::derived::flow::transmission::Confirmed;
use crosstalk_spec::derived::provenance::matching::ContentMatch;
use crosstalk_spec::ids::TransmissionId;
use crosstalk_spec::interfaces::l5_flow::TransmissionUpdate;
use crosstalk_spec::support::{NonEmpty, Timestamp};

use super::ids;
use super::key::MediumKey;
use super::medium::{Channeled, Ident, MatchKey, Medium, Phase};
use super::pairing;
use super::windowed::Decided;

/// Pair `write` and `read` in `medium`, joining or opening their
/// transmission.
pub(crate) fn pair(
    medium: &mut Medium,
    key: MediumKey,
    write: &Access,
    read: &Access,
    timing: CorrelationTiming,
    out: &mut Vec<Decided>,
) {
    match pairing::co_access(write, read, timing) {
        Ok(co_access) => join(medium, key, write, read, co_access, timing, out),
        Err(why) => {
            tracing::trace!(write = %write.id.ulid_text(), read = %read.id.ulid_text(), why = ?why, "no co-access");
        }
    }
}

/// Add `co_access` to the open transmission of its reader exchange and
/// writer, or open one.
fn join(
    medium: &mut Medium,
    key: MediumKey,
    write: &Access,
    read: &Access,
    co_access: CoAccess,
    timing: CorrelationTiming,
    out: &mut Vec<Decided>,
) {
    let ident = Ident {
        exchange: read.exchange,
        sender: write.agent,
    };
    let slot = (co_access.write(), co_access.read());
    if let Some(id) = medium.open_of(ident) {
        if let Some(tx) = medium.open.get_mut(&(ident, id)) {
            tx.co_access.insert(slot, co_access);
        }
        return;
    }
    let generation = medium.retired.get(&ident).map_or(0, |(count, _)| *count);
    let id = ids::transmission_id(
        read.at,
        read.exchange,
        write.agent,
        &ids::medium_route(key),
        generation,
    );
    medium.open.insert(
        (ident, id),
        Channeled {
            to: read.agent,
            opened_at: read.at,
            co_access: BTreeMap::from([(slot, co_access)]),
            phase: Phase::Awaiting {
                closes_at: timing.window_closes_at(read.at),
            },
        },
    );
    tracing::debug!(transmission = %id.ulid_text(), reader = %read.agent.ulid_text(), writer = %write.agent.ulid_text(), "channel transmission opened");
    out.push(Decided {
        update: TransmissionUpdate::OpenChannel {
            transmission: id,
            to: read.agent,
            on: key.opens_on(),
            co_access,
        },
        opened_at: read.at,
    });
}

/// Decide every channel transmission of `medium` as of `now`, then open a
/// transmission for each held match a write explains whose identity has
/// none (one discarded before the match arrived), and decide those.
pub(crate) fn settle(
    medium: &mut Medium,
    key: MediumKey,
    timing: CorrelationTiming,
    now: Timestamp,
    out: &mut Vec<Decided>,
) {
    decide_open(medium, timing, now, out);
    if open_for_held(medium, key, timing, out) {
        decide_open(medium, timing, now, out);
    }
}

fn decide_open(
    medium: &mut Medium,
    timing: CorrelationTiming,
    now: Timestamp,
    out: &mut Vec<Decided>,
) {
    let slots: Vec<(Ident, TransmissionId)> = medium.open.keys().copied().collect();
    for slot in slots {
        let Some(tx) = medium.open.get(&slot) else {
            continue;
        };
        let linking: Vec<MatchKey> = medium
            .held
            .iter()
            .filter(|(_, held)| {
                Ident::of_match(&held.content) == slot.0 && medium.explains(tx, &held.content)
            })
            .map(|(key, _)| *key)
            .collect();
        match tx.phase.clone() {
            Phase::Awaiting { closes_at } => {
                if closes_at > now {
                    continue;
                }
                if !linking.is_empty() {
                    confirm(medium, slot, &linking, out);
                } else {
                    suspect(medium, slot, closes_at, out);
                    if timing.expires_at(closes_at) <= now {
                        discard(medium, slot, timing.expires_at(closes_at), out);
                    }
                }
            }
            Phase::Suspected { since } => {
                if !linking.is_empty() {
                    confirm(medium, slot, &linking, out);
                } else if timing.expires_at(since) <= now {
                    discard(medium, slot, timing.expires_at(since), out);
                }
            }
            Phase::Confirmed(_) => extend(medium, slot, &linking, out),
        }
    }
}

fn confirm(
    medium: &mut Medium,
    slot: (Ident, TransmissionId),
    linking: &[MatchKey],
    out: &mut Vec<Decided>,
) {
    let content: Vec<ContentMatch> = linking
        .iter()
        .filter_map(|key| medium.held.remove(key))
        .map(|held| held.content)
        .collect();
    let Some(tx) = medium.open.get_mut(&slot) else {
        return;
    };
    let Some(content) = NonEmpty::from_vec(content) else {
        return;
    };
    match Confirmed::new(content, tx.co_accesses(), tx.opened_at) {
        Ok(confirmed) => {
            tx.phase = Phase::Confirmed(Box::new(confirmed.clone()));
            tracing::debug!(transmission = %slot.1.ulid_text(), "channel transmission confirmed");
            out.push(Decided {
                update: TransmissionUpdate::Confirm {
                    transmission: slot.1,
                    confirmed,
                },
                opened_at: tx.opened_at,
            });
        }
        Err(mixed) => {
            tracing::warn!(transmission = %slot.1.ulid_text(), error = ?mixed, "matches of one identity disagree; not confirmed");
        }
    }
}

fn extend(
    medium: &mut Medium,
    slot: (Ident, TransmissionId),
    linking: &[MatchKey],
    out: &mut Vec<Decided>,
) {
    for key in linking {
        let Some(held) = medium.held.remove(key) else {
            continue;
        };
        let Some(tx) = medium.open.get_mut(&slot) else {
            return;
        };
        let Phase::Confirmed(confirmed) = &mut tx.phase else {
            return;
        };
        if confirmed
            .content()
            .iter()
            .any(|known| *known == held.content)
        {
            continue;
        }
        match confirmed.extend(held.content.clone()) {
            Ok(()) => out.push(Decided {
                update: TransmissionUpdate::Extend {
                    transmission: slot.1,
                    content: held.content,
                },
                opened_at: tx.opened_at,
            }),
            Err(mixed) => {
                tracing::warn!(transmission = %slot.1.ulid_text(), error = ?mixed, "match not extended");
            }
        }
    }
}

fn suspect(
    medium: &mut Medium,
    slot: (Ident, TransmissionId),
    closes_at: Timestamp,
    out: &mut Vec<Decided>,
) {
    let Some(tx) = medium.open.get_mut(&slot) else {
        return;
    };
    let Some(co_access) = NonEmpty::from_vec(tx.co_accesses()) else {
        return;
    };
    tx.phase = Phase::Suspected { since: closes_at };
    out.push(Decided {
        update: TransmissionUpdate::Suspect {
            transmission: slot.1,
            co_access,
        },
        opened_at: tx.opened_at,
    });
}

fn discard(
    medium: &mut Medium,
    slot: (Ident, TransmissionId),
    at: Timestamp,
    out: &mut Vec<Decided>,
) {
    let Some(tx) = medium.open.remove(&slot) else {
        return;
    };
    let retired = medium.retired.entry(slot.0).or_insert((0, at));
    *retired = (retired.0.saturating_add(1), at);
    out.push(Decided {
        update: TransmissionUpdate::Discard {
            transmission: slot.1,
        },
        opened_at: tx.opened_at,
    });
}

/// Open the transmission a held match needs when its identity has none
/// open: a write of its sender explains it and pairs with the read that
/// carried it. Returns whether it opened any.
fn open_for_held(
    medium: &mut Medium,
    key: MediumKey,
    timing: CorrelationTiming,
    out: &mut Vec<Decided>,
) -> bool {
    let mut pairs: Vec<(Access, Access)> = Vec::new();
    for held in medium.held.values() {
        let ident = Ident::of_match(&held.content);
        if medium.open_of(ident).is_some() {
            continue;
        }
        let Some(read) = medium
            .reads
            .values()
            .find(|read| pairing::carried_by(&held.content, read))
        else {
            continue;
        };
        for write in medium
            .writes
            .values()
            .filter(|write| pairing::links(&held.content, write))
        {
            if pairing::co_access(write, read, timing).is_ok() {
                pairs.push((write.clone(), read.clone()));
            }
        }
    }
    let opened = !pairs.is_empty();
    for (write, read) in &pairs {
        pair(medium, key, write, read, timing, out);
    }
    opened
}
