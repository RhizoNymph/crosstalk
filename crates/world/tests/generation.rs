//! The generated data itself, before any store sees it: the UI fixture's
//! world tests, ported. Declared channels get stand-in ids (a registry
//! would assign them).

use std::collections::{BTreeMap, BTreeSet};

use crosstalk_spec::derived::flow::evidence::CoAccess;
use crosstalk_spec::derived::flow::transmission::{DirectCarrier, Route, TransmissionState};
use crosstalk_spec::derived::provenance::matching::{Carrier, Codec, MatchKind};
use crosstalk_spec::ids::{AccessId, ChannelId};
use crosstalk_spec::observed::message::encoding;
use crosstalk_world::generate::{self, Generated};
use crosstalk_world::{Anchor, ChannelKey, UI_ANCHOR, WorldConfig, WorldError};

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
    let anchor = Anchor::new(UI_ANCHOR)?;
    let config = WorldConfig::new(seed, anchor)?;
    generate::generate(seed, anchor, &config, &declared())
}

#[test]
fn generation_succeeds_for_many_seeds() -> Result<(), WorldError> {
    for seed in 0..4 {
        world(seed)?;
    }
    Ok(())
}

#[test]
fn same_seed_same_world_other_seed_other_world() -> Result<(), WorldError> {
    let (a, b, c) = (world(SEED)?, world(SEED)?, world(SEED + 1)?);
    assert_eq!(a.traffic.transmissions, b.traffic.transmissions);
    assert_eq!(a.traffic.accesses, b.traffic.accesses);
    assert_eq!(a.traffic.resources, b.traffic.resources);
    assert_eq!(a.topics, b.topics);
    assert_eq!(a.cast, b.cast);
    assert_eq!(a.dropped, b.dropped);
    assert_ne!(a.traffic.transmissions, c.traffic.transmissions);
    Ok(())
}

#[test]
fn about_five_thousand_transmissions_over_the_week() -> Result<(), WorldError> {
    let w = world(SEED)?;
    let n = w.traffic.transmissions.len();
    assert!((4500..=5500).contains(&n), "{n}");
    let ids: BTreeSet<_> = w.traffic.transmissions.iter().map(|t| t.id()).collect();
    assert_eq!(ids.len(), n, "distinct ids");
    assert!(w.traffic.transmissions.iter().all(
        |t| t.transmission.opened_at >= w.times.start && t.transmission.opened_at < w.times.now
    ));
    Ok(())
}

#[test]
fn every_transmission_state_is_present() -> Result<(), WorldError> {
    let w = world(SEED)?;
    let kinds: BTreeSet<&str> = w
        .traffic
        .transmissions
        .iter()
        .map(|t| match t.transmission.state {
            TransmissionState::Detected => "detected",
            TransmissionState::AwaitingContent { .. } => "awaiting",
            TransmissionState::Suspected { .. } => "suspected",
            TransmissionState::Discarded { .. } => "discarded",
            TransmissionState::Confirmed(_) => "confirmed",
            TransmissionState::Classified { .. } => "classified",
            TransmissionState::Aggregated { .. } => "aggregated",
        })
        .collect();
    assert_eq!(kinds.len(), 7, "{kinds:?}");
    Ok(())
}

#[test]
fn every_route_variant_is_present() -> Result<(), WorldError> {
    let w = world(SEED)?;
    let routes: Vec<&Route> = w
        .traffic
        .transmissions
        .iter()
        .map(|t| &t.transmission.route)
        .collect();
    assert!(routes.iter().any(|r| matches!(r, Route::Channel(_))));
    assert!(routes.iter().any(|r| matches!(r, Route::Delegation(_))));
    for carrier in [
        |c: &DirectCarrier| matches!(c, DirectCarrier::UserTurn),
        |c: &DirectCarrier| matches!(c, DirectCarrier::SystemPrompt),
        |c: &DirectCarrier| matches!(c, DirectCarrier::ToolResult(_)),
    ] {
        assert!(
            routes
                .iter()
                .any(|r| matches!(r, Route::Direct(c) if carrier(c)))
        );
    }
    assert!(routes.iter().any(|r| **r == Route::Unobserved));
    Ok(())
}

#[test]
fn every_match_kind_codec_chain_and_carrier_is_present() -> Result<(), WorldError> {
    let w = world(SEED)?;
    let matches: Vec<_> = w
        .traffic
        .transmissions
        .iter()
        .filter_map(|t| t.confirmed())
        .flat_map(|c| c.content().iter().cloned().collect::<Vec<_>>())
        .collect();
    assert!(matches.iter().any(|m| *m.kind() == MatchKind::Exact));
    assert!(matches.iter().any(|m| *m.kind() == MatchKind::Normalized));
    assert!(
        matches
            .iter()
            .any(|m| matches!(m.kind(), MatchKind::Semantic(_)))
    );
    let chains: BTreeSet<Vec<String>> = matches
        .iter()
        .filter_map(|m| match m.kind() {
            MatchKind::Decoded(chain) => Some(chain.iter().map(|c| format!("{c:?}")).collect()),
            _ => None,
        })
        .collect();
    assert!(chains.contains(&vec![
        format!("{:?}", Codec::Base64),
        format!("{:?}", Codec::UrlEncoding)
    ]));
    assert!(chains.len() >= 4, "{chains:?}");
    for carrier in [
        Carrier::UserTurn,
        Carrier::SystemPrompt,
        Carrier::ReaderOutput,
    ] {
        assert!(
            matches.iter().any(|m| *m.carrier() == carrier),
            "{carrier:?}"
        );
    }
    assert!(
        matches
            .iter()
            .any(|m| matches!(m.carrier(), Carrier::ToolResult(_)))
    );
    Ok(())
}

#[test]
fn confirmed_transmissions_have_text_assignments_and_located_bodies() -> Result<(), WorldError> {
    let w = world(SEED)?;
    let bodies = w.traffic.blobs.clone();
    let dropped: BTreeSet<_> = w.dropped.iter().map(|(id, _)| *id).collect();
    for record in &w.traffic.transmissions {
        let Some(confirmed) = record.confirmed() else {
            assert!(record.texts.is_empty());
            assert!(record.from.is_none());
            assert!(record.assignments.is_empty());
            continue;
        };
        assert_eq!(confirmed.content().iter().count(), record.texts.len());
        assert_eq!(record.from, Some(confirmed.from()));
        match record.transmission.state {
            TransmissionState::Confirmed(_) => assert!(record.assignments.is_empty()),
            _ => assert_eq!(record.assignments.len(), 3),
        }
        if dropped.contains(&record.id()) {
            continue;
        }
        for (content, text) in confirmed.content().iter().zip(&record.texts) {
            let read = content.read_at();
            assert!(
                bodies.contains(read.part.message),
                "the reader's body is stored"
            );
            let span = bodies
                .span(content.origin())
                .ok_or_else(|| WorldError::missing("span"))?;
            assert!(
                bodies.contains(span.part.message),
                "the sender's body is stored"
            );
            assert!(read.range.len().get() as usize <= text.read.len());
        }
    }
    Ok(())
}

#[test]
fn stored_bodies_hash_to_their_keys() -> Result<(), WorldError> {
    let w = world(SEED)?;
    for (hash, body) in w.traffic.blobs.clone().into_bodies().into_iter().take(200) {
        assert_eq!(encoding::hash_bytes(&body.bytes), hash);
        assert!(encoding::decode(&body.bytes).is_ok());
    }
    Ok(())
}

#[test]
fn co_access_records_are_consistent_with_accesses() -> Result<(), WorldError> {
    let w = world(SEED)?;
    let accesses: BTreeMap<AccessId, _> = w.traffic.accesses.iter().map(|a| (a.id, a)).collect();
    let window = std::time::Duration::from_secs(24 * 3600);
    let mut checked = 0;
    for record in &w.traffic.transmissions {
        for co in generate::states::co_accesses(&record.transmission.state) {
            let (Some(write), Some(read)) = (accesses.get(&co.write()), accesses.get(&co.read()))
            else {
                return Err(WorldError::missing("a co-access's accesses"));
            };
            assert_eq!(CoAccess::new(write, read, window), Ok(co));
            assert_eq!(read.agent, record.transmission.to);
            assert!(matches!(record.transmission.route, Route::Channel(_)));
            checked += 1;
        }
    }
    assert!(checked > 1000, "{checked}");
    Ok(())
}

#[test]
fn the_lone_entry_is_used_by_one_agent_only() -> Result<(), WorldError> {
    let w = world(SEED)?;
    let users: BTreeSet<_> = w
        .traffic
        .accesses
        .iter()
        .filter(|a| a.resource == w.traffic.lone)
        .map(|a| a.agent)
        .collect();
    assert_eq!(users, BTreeSet::from([w.cast.id("cc7")?]));
    Ok(())
}
