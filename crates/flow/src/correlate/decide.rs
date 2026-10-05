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
use super::medium::{Channeled, Delivered, Delivery, Held, Ident, MatchKey, Medium, Phase};
use super::pairing;
use super::retention::ContentRetention;
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

/// Decide every channel transmission of `medium` as of `now`, after
/// pairing every held match with the writes that explain it: joining the
/// open transmission of its identity, or opening one when it has none
/// (never opened, or discarded before the match arrived).
pub(crate) fn settle(
    medium: &mut Medium,
    key: MediumKey,
    timing: CorrelationTiming,
    retention: ContentRetention,
    now: Timestamp,
    out: &mut Vec<Decided>,
) {
    pair_content(medium, key, timing, retention, out);
    decide_open(medium, timing, now, out);
}

fn decide_open(
    medium: &mut Medium,
    timing: CorrelationTiming,
    now: Timestamp,
    out: &mut Vec<Decided>,
) {
    // Earlier reads first: a reread is decided after the read whose
    // content it repeats, whatever order the evidence arrived in.
    let mut slots: Vec<(Timestamp, (Ident, TransmissionId))> = medium
        .open
        .iter()
        .map(|(slot, tx)| (tx.opened_at, *slot))
        .collect();
    slots.sort();
    for (_, slot) in slots {
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
        let phase = tx.phase.clone();
        if matches!(phase, Phase::Awaiting { closes_at } if closes_at > now) {
            continue;
        }
        let linking = refresh_repeats(medium, slot.1, linking);
        match phase {
            Phase::Awaiting { closes_at } => {
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

/// Of `linking`, the matches whose (origin span, reader) another
/// transmission of this medium already delivered: a reread of content the
/// reader already received (`flow.correlator.reread-refreshes-delivery`).
/// Each refreshes that delivery and leaves the held matches; the rest are
/// returned, new content `tx` may confirm or extend with.
fn refresh_repeats(
    medium: &mut Medium,
    tx: TransmissionId,
    linking: Vec<MatchKey>,
) -> Vec<MatchKey> {
    let mut fresh = Vec::with_capacity(linking.len());
    for key in linking {
        let Some(held) = medium.held.get(&key) else {
            continue;
        };
        let delivery = Delivery::of(&held.content);
        let read_at = held.read_at;
        match medium.delivered.get_mut(&delivery) {
            Some(delivered) if delivered.transmission != tx => {
                delivered.last = delivered.last.max(read_at);
                tracing::debug!(transmission = %delivered.transmission.ulid_text(), reread_by = %tx.ulid_text(), "reread refreshes a delivery");
                medium.held.remove(&key);
            }
            Some(_) | None => fresh.push(key),
        }
    }
    fresh
}

/// A held match repeating a span another transmission already delivered
/// to its reader, carried by a read in another exchange, opens nothing: it
/// refreshes the delivery (`flow.correlator.reread-refreshes-delivery`).
/// A match in the delivering transmission's own exchange stays held, to
/// extend it.
fn refresh_delivered(medium: &mut Medium) {
    let repeats: Vec<MatchKey> = medium
        .held
        .iter()
        .filter(|(_, held)| {
            medium
                .delivered
                .get(&Delivery::of(&held.content))
                .is_some_and(|delivered| {
                    !medium
                        .open
                        .contains_key(&(Ident::of_match(&held.content), delivered.transmission))
                })
        })
        .map(|(key, _)| *key)
        .collect();
    for key in repeats {
        let Some(held) = medium.held.remove(&key) else {
            continue;
        };
        if let Some(delivered) = medium.delivered.get_mut(&Delivery::of(&held.content)) {
            delivered.last = delivered.last.max(held.read_at);
            tracing::debug!(transmission = %delivered.transmission.ulid_text(), "reread refreshes a delivery");
        }
    }
}

/// Record that `tx` delivered `held`'s span to its reader.
fn deliver(medium: &mut Medium, tx: TransmissionId, held: &Held) {
    let delivered = medium
        .delivered
        .entry(Delivery::of(&held.content))
        .or_insert(Delivered {
            transmission: tx,
            last: held.read_at,
        });
    delivered.last = delivered.last.max(held.read_at);
}

fn confirm(
    medium: &mut Medium,
    slot: (Ident, TransmissionId),
    linking: &[MatchKey],
    out: &mut Vec<Decided>,
) {
    let held: Vec<Held> = linking
        .iter()
        .filter_map(|key| medium.held.remove(key))
        .collect();
    for one in &held {
        deliver(medium, slot.1, one);
    }
    let content: Vec<ContentMatch> = held.into_iter().map(|held| held.content).collect();
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
        deliver(medium, slot.1, &held);
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

/// Pair every held match with each write of its sender that explains it
/// and the read that carried it, within `retention`
/// (`flow.correlator.content-confirms-past-window`): the co-access joins
/// its identity's open transmission or opens one. Joining is idempotent,
/// so the transmission's co-accesses are the same whatever order the
/// evidence arrived in.
fn pair_content(
    medium: &mut Medium,
    key: MediumKey,
    timing: CorrelationTiming,
    retention: ContentRetention,
    out: &mut Vec<Decided>,
) {
    refresh_delivered(medium);
    let mut pairs: Vec<(Access, Access, CoAccess)> = Vec::new();
    for held in medium.held.values() {
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
            if let Ok(co_access) = pairing::content_co_access(write, read, retention) {
                pairs.push((write.clone(), read.clone(), co_access));
            }
        }
    }
    for (write, read, co_access) in pairs {
        join(medium, key, &write, &read, co_access, timing, out);
    }
}
