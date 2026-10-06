//! The world's wire traffic (`generate::wire`): the exchanges behind the
//! confirmed transmissions, before any store sees them.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crosstalk_spec::derived::provenance::matching::Carrier;
use crosstalk_spec::ids::{AgentId, ChannelId, ExchangeId, MessageHash};
use crosstalk_spec::observed::exchange::{Continuation, ExchangeOutcome};
use crosstalk_spec::observed::message::encoding;
use crosstalk_spec::observed::message::{MessageBody, Role};
use crosstalk_world::clock::{DAY, minus};
use crosstalk_world::generate::wire::{SESSION_GAP, SUMMARY_PREAMBLE};
use crosstalk_world::generate::{self, Generated};
use crosstalk_world::{
    Anchor, ChannelKey, UI_ANCHOR, WireExchange, WireScope, WorldConfig, WorldError,
};

const SEED: u64 = 0x005e_edc0_ffee;

fn declared() -> BTreeMap<ChannelKey, ChannelId> {
    [
        ChannelKey::InternalWiki,
        ChannelKey::Monorepo,
        ChannelKey::IssueTracker,
        ChannelKey::ReleaseBucket,
        ChannelKey::DesignDocs,
    ]
    .into_iter()
    .zip(1u128..)
    .map(|(key, n)| (key, ChannelId::from_ulid(n)))
    .collect()
}

fn world(seed: u64) -> Result<Generated, WorldError> {
    scoped(seed, WireScope::All)
}

fn scoped(seed: u64, scope: WireScope) -> Result<Generated, WorldError> {
    let anchor = Anchor::new(UI_ANCHOR)?;
    let mut config = WorldConfig::new(seed, anchor)?;
    config.wire = scope;
    generate::generate(seed, anchor, &config, &declared())
}

fn by_id(w: &Generated) -> HashMap<ExchangeId, &WireExchange> {
    w.wire
        .exchanges
        .iter()
        .map(|wire| (wire.exchange.meta.id, wire))
        .collect()
}

fn response(wire: &WireExchange) -> Option<MessageHash> {
    match &wire.exchange.outcome {
        ExchangeOutcome::Completed { response, .. } => Some(*response),
        ExchangeOutcome::Failed { .. } => None,
    }
}

/// Every body the wire names, decoded: the wire's own, the dropped ones
/// and the world's.
fn bodies(w: &Generated) -> BTreeMap<MessageHash, MessageBody> {
    let mut all = BTreeMap::new();
    let world = w.traffic.blobs.clone().into_bodies();
    let encoded = w
        .wire
        .bodies
        .iter()
        .chain(w.wire.dropped.iter())
        .map(|(hash, bytes)| (*hash, bytes.clone()))
        .chain(world.into_iter().map(|(hash, e)| (hash, e.bytes)));
    for (hash, bytes) in encoded {
        if let Ok(body) = encoding::decode(&bytes) {
            all.insert(hash, body);
        }
    }
    all
}

#[test]
fn the_same_seed_gives_the_same_wire_and_another_seed_another() -> Result<(), WorldError> {
    let (a, b, c) = (world(SEED)?, world(SEED)?, world(SEED + 1)?);
    assert_eq!(a.wire.exchanges, b.wire.exchanges);
    assert_eq!(a.wire.bodies, b.wire.bodies);
    assert_eq!(a.wire.dropped, b.wire.dropped);
    assert_ne!(a.wire.exchanges, c.wire.exchanges);
    Ok(())
}

#[test]
fn exchanges_are_oldest_first_with_unique_ids() -> Result<(), WorldError> {
    let w = world(SEED)?;
    assert!(w.wire.exchanges.len() > 5_000, "{}", w.wire.exchanges.len());
    let starts: Vec<_> = w
        .wire
        .exchanges
        .iter()
        .map(|wire| wire.exchange.meta.started_at)
        .collect();
    assert!(starts.windows(2).all(|pair| pair[0] <= pair[1]));
    let ids: BTreeSet<ExchangeId> = w
        .wire
        .exchanges
        .iter()
        .map(|x| x.exchange.meta.id)
        .collect();
    assert_eq!(ids.len(), w.wire.exchanges.len());
    Ok(())
}

#[test]
fn every_named_body_is_carried_or_written_by_the_seed() -> Result<(), WorldError> {
    let w = world(SEED)?;
    let known = bodies(&w);
    for wire in &w.wire.exchanges {
        let named = wire.exchange.request.iter().copied().chain(response(wire));
        for hash in named {
            assert!(
                known.contains_key(&hash),
                "{hash:?} in {:?}",
                wire.exchange.meta.id
            );
        }
    }
    // The wire's own bodies hash to their keys.
    for (hash, bytes) in &w.wire.bodies {
        assert_eq!(encoding::hash_bytes(bytes), *hash);
    }
    Ok(())
}

#[test]
fn retention_drops_bodies_the_wire_carried() -> Result<(), WorldError> {
    let w = world(SEED)?;
    assert!(!w.dropped.is_empty());
    assert!(!w.wire.dropped.is_empty());
    let named: BTreeSet<MessageHash> = w
        .wire
        .exchanges
        .iter()
        .flat_map(|wire| wire.exchange.request.iter().copied().chain(response(wire)))
        .collect();
    for (hash, bytes) in &w.wire.dropped {
        assert!(!w.traffic.blobs.contains(*hash), "dropped body still held");
        assert!(named.contains(hash), "a dropped body no exchange carried");
        assert_eq!(encoding::hash_bytes(bytes), *hash);
    }
    Ok(())
}

#[test]
fn each_match_is_sent_before_it_is_read_in_the_worlds_reader_exchange() -> Result<(), WorldError> {
    let w = world(SEED)?;
    let exchanges = by_id(&w);
    let mut sent_at: HashMap<(AgentId, MessageHash), _> = HashMap::new();
    for wire in &w.wire.exchanges {
        if let Some(response) = response(wire) {
            sent_at
                .entry((wire.agent, response))
                .or_insert(wire.exchange.meta.started_at);
        }
    }
    let mut checked = 0;
    for record in &w.traffic.transmissions {
        let Some(confirmed) = record.confirmed() else {
            continue;
        };
        for matched in confirmed.content().iter() {
            let origin = w
                .traffic
                .blobs
                .span(matched.origin())
                .ok_or_else(|| WorldError::missing("span"))?;
            let Some(sent) = sent_at.get(&(matched.origin_agent(), origin.part.message)) else {
                panic!("the origin body is no response of its sender");
            };
            let reader = exchanges
                .get(&matched.reader_exchange())
                .unwrap_or_else(|| panic!("the reader exchange is not on the wire"));
            assert_eq!(reader.agent, matched.reader());
            assert!(*sent < reader.exchange.meta.started_at);
            let read = matched.read_at().part.message;
            match matched.carrier() {
                Carrier::ReaderOutput => {
                    let outputs: Vec<_> = w
                        .wire
                        .exchanges
                        .iter()
                        .filter(|x| x.agent == matched.reader())
                        .filter_map(response)
                        .collect();
                    assert!(outputs.contains(&read));
                }
                Carrier::SystemPrompt => {
                    let systems: BTreeSet<_> = w
                        .wire
                        .exchanges
                        .iter()
                        .filter(|x| x.agent == matched.reader())
                        .filter_map(|x| x.exchange.request.first().copied())
                        .collect();
                    assert!(systems.contains(&read));
                }
                Carrier::ToolResult(_) | Carrier::UserTurn => {
                    assert!(reader.exchange.request.contains(&read));
                }
            }
            checked += 1;
        }
    }
    assert!(checked > 4_000, "{checked}");
    Ok(())
}

#[test]
fn a_channel_transmission_is_sent_in_the_writers_exchange() -> Result<(), WorldError> {
    let w = world(SEED)?;
    let exchanges = by_id(&w);
    let writes = w
        .traffic
        .accesses
        .iter()
        .filter(|access| {
            matches!(
                access.op,
                crosstalk_spec::derived::flow::access::AccessOp::Write { .. }
            )
        })
        .filter(|access| exchanges.contains_key(&access.exchange))
        .count();
    assert!(writes > 1_000, "{writes}");
    for access in &w.traffic.accesses {
        if let Some(wire) = exchanges.get(&access.exchange) {
            assert_eq!(wire.agent, access.agent);
            assert_eq!(wire.exchange.meta.started_at, access.at);
        }
    }
    Ok(())
}

#[test]
fn sessions_grow_pause_compact_and_fork() -> Result<(), WorldError> {
    let w = world(SEED)?;
    let known = bodies(&w);
    let mut agents: BTreeMap<AgentId, Vec<&WireExchange>> = BTreeMap::new();
    for wire in &w.wire.exchanges {
        agents.entry(wire.agent).or_default().push(wire);
        assert_eq!(wire.exchange.continuation, Continuation::FullHistory);
        let first = wire.exchange.request.first().and_then(|h| known.get(h));
        assert_eq!(first.map(MessageBody::role), Some(Role::System));
    }
    let (mut extends, mut starts, mut compactions, mut forks, mut failures) = (0, 0, 0, 0, 0);
    for exchanges in agents.values() {
        let mut previous: Option<(&WireExchange, Vec<MessageHash>)> = None;
        for wire in exchanges {
            let history: Vec<MessageHash> = wire.exchange.request.iter().skip(1).copied().collect();
            let opening = history.first().and_then(|h| known.get(h));
            let summary = matches!(opening, Some(MessageBody::User(parts))
                if format!("{parts:?}").contains(&SUMMARY_PREAMBLE[..40]));
            if summary && previous.is_some() {
                compactions += 1;
            }
            if matches!(wire.exchange.outcome, ExchangeOutcome::Failed { .. }) {
                failures += 1;
            }
            match &previous {
                None => starts += 1,
                Some((before, before_history)) => {
                    let gap = wire.exchange.meta.started_at.as_micros()
                        - before.exchange.meta.started_at.as_micros();
                    let mut grown = before_history.clone();
                    if let Some(out) = response(before) {
                        grown.push(out);
                    }
                    if history.starts_with(&grown) {
                        extends += 1;
                    } else if history.starts_with(before_history)
                        && matches!(before.exchange.outcome, ExchangeOutcome::Failed { .. })
                    {
                        // The retry of a failed attempt.
                    } else if gap > SESSION_GAP && !summary {
                        starts += 1;
                    } else if !summary {
                        forks += 1;
                    }
                }
            }
            previous = Some((wire, history));
        }
    }
    assert!(extends > 1_000, "extends {extends}");
    assert!(starts > 100, "starts {starts}");
    assert!(compactions > 10, "compactions {compactions}");
    assert!(forks > 10, "forks {forks}");
    assert!(failures > 10, "failures {failures}");
    Ok(())
}

#[test]
fn a_scope_keeps_its_transmissions_and_the_dropped_ones_and_changes_nothing_else()
-> Result<(), WorldError> {
    let since = minus(UI_ANCHOR, DAY);
    let (all, recent) = (world(SEED)?, scoped(SEED, WireScope::Since(since))?);
    assert_eq!(all.traffic.transmissions, recent.traffic.transmissions);
    assert_eq!(all.dropped, recent.dropped);
    assert!(recent.wire.exchanges.len() < all.wire.exchanges.len() / 2);
    assert_eq!(all.wire.dropped, recent.wire.dropped);
    let carried: BTreeSet<ExchangeId> = recent
        .wire
        .exchanges
        .iter()
        .map(|x| x.exchange.meta.id)
        .collect();
    let dropped: BTreeSet<_> = recent.dropped.iter().map(|(id, _)| *id).collect();
    for record in &recent.traffic.transmissions {
        let Some(confirmed) = record.confirmed() else {
            continue;
        };
        let held = record.transmission.opened_at >= since || dropped.contains(&record.id());
        for matched in confirmed.content().iter() {
            assert_eq!(carried.contains(&matched.reader_exchange()), held);
        }
    }
    Ok(())
}
