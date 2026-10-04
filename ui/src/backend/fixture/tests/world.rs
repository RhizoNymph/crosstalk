//! The generated world: determinism, size, and the traffic every page
//! relies on.

use crate::contract::present::Present;
use std::collections::HashSet;
use std::time::Instant;

use crosstalk_spec::aggregates::topic::{Assignment, TopicModelVersion};
use crosstalk_spec::derived::flow::evidence::CoAccess;
use crosstalk_spec::derived::flow::transmission::{
    DelegationDirection, DirectCarrier, Route, TransmissionState,
};
use crosstalk_spec::derived::provenance::matching::{Carrier, Codec, MatchKind};

use super::super::FixtureBackend;
use super::super::clock::{CORRELATION_WINDOW, NOW, START};
use super::super::world::{co_accesses, confirmed};
use super::{SEED, shared};
use crosstalk_spec::interfaces::l8_surface::summary::TransmissionStateKind;

fn state_of(b: &FixtureBackend) -> tokio::sync::RwLockReadGuard<'_, super::super::store::State> {
    b.state.blocking_read()
}

#[test]
fn generation_succeeds_for_many_seeds() {
    for seed in [0, 1, 7, 42, 1234, u64::MAX] {
        if let Err(e) = FixtureBackend::try_new(seed) {
            panic!("seed {seed}: {e}");
        }
    }
}

#[test]
fn generation_is_fast() {
    let started = Instant::now();
    let _ = FixtureBackend::try_new(SEED).expect("generates");
    // Well under a second in release; debug builds get some slack.
    assert!(
        started.elapsed().as_secs_f64() < 3.0,
        "{:?}",
        started.elapsed()
    );
}

#[test]
fn same_seed_same_world() {
    let a = FixtureBackend::try_new(SEED).expect("a");
    let b = FixtureBackend::try_new(SEED).expect("b");
    assert_eq!(a.world.transmissions, b.world.transmissions);
    assert_eq!(a.world.accesses, b.world.accesses);
    assert_eq!(a.world.resources, b.world.resources);
    assert_eq!(a.world.topics, b.world.topics);
    let (sa, sb) = (state_of(&a), state_of(&b));
    assert_eq!(sa.identity, sb.identity);
    assert_eq!(sa.channels, sb.channels);
    assert_eq!(sa.alerts, sb.alerts);
    assert_eq!(sa.rules, sb.rules);
    assert_eq!(sa.audit, sb.audit);
    assert_eq!(sa.verdicts, sb.verdicts);
}

#[test]
fn different_seeds_differ() {
    let a = FixtureBackend::try_new(1).expect("a");
    let b = FixtureBackend::try_new(2).expect("b");
    assert_ne!(a.world.transmissions, b.world.transmissions);
}

#[tokio::test]
async fn now_and_versions() {
    use crate::backend::Backend;
    use crosstalk_spec::interfaces::l8_surface::Permission;
    use crosstalk_spec::interfaces::l8_surface::QueryError;

    let b = shared();
    let c = super::researcher();
    assert_eq!(b.now(&c).await, Ok(NOW));
    let active = |history: crosstalk_spec::aggregates::topic_history::TopicVersionHistory| {
        history.active().version()
    };
    assert_eq!(
        b.topic_versions(&c).await.map(active),
        Ok(TopicModelVersion(2))
    );
    assert_eq!(b.seed(), SEED);
    let nobody = super::caller(&[Permission::Audit]);
    assert_eq!(
        b.now(&nobody).await,
        Err(QueryError::Forbidden {
            missing: Permission::View
        })
    );
    let viewer = super::caller(&[Permission::View]);
    assert_eq!(
        b.topic_versions(&viewer).await.map(active),
        Ok(TopicModelVersion(2)),
        "the topic history is not content"
    );
}

#[test]
fn about_five_thousand_transmissions_over_the_week() {
    let w = &shared().world;
    let n = w.transmissions.len();
    assert!((4500..=5500).contains(&n), "{n}");
    assert!(
        w.transmissions
            .iter()
            .all(|t| { t.transmission.opened_at >= START && t.transmission.opened_at < NOW })
    );
    // Ids are unique.
    let ids: HashSet<_> = w.transmissions.iter().map(|t| t.transmission.id).collect();
    assert_eq!(ids.len(), n);
}

#[test]
fn every_transmission_state_is_present() {
    let w = &shared().world;
    let kinds: HashSet<TransmissionStateKind> = w
        .transmissions
        .iter()
        .map(|t| TransmissionStateKind::of(&t.transmission.state))
        .collect();
    for kind in [
        TransmissionStateKind::Detected,
        TransmissionStateKind::AwaitingContent,
        TransmissionStateKind::Suspected,
        TransmissionStateKind::Confirmed,
        TransmissionStateKind::Classified,
        TransmissionStateKind::Aggregated,
        TransmissionStateKind::Discarded,
    ] {
        assert!(kinds.contains(&kind), "{kind:?}");
    }
    let aggregated = w
        .transmissions
        .iter()
        .filter(|t| matches!(t.transmission.state, TransmissionState::Aggregated { .. }))
        .count();
    assert!(
        aggregated * 2 > w.transmissions.len(),
        "most are aggregated"
    );
}

#[test]
fn every_route_variant_is_present() {
    let w = &shared().world;
    let routes: Vec<&Route> = w
        .transmissions
        .iter()
        .map(|t| &t.transmission.route)
        .collect();
    assert!(routes.iter().any(|r| matches!(r, Route::Channel(_))));
    assert!(
        routes
            .iter()
            .any(|r| **r == Route::Delegation(DelegationDirection::ParentToChild))
    );
    assert!(
        routes
            .iter()
            .any(|r| **r == Route::Delegation(DelegationDirection::ChildToParent))
    );
    assert!(
        routes
            .iter()
            .any(|r| **r == Route::Direct(DirectCarrier::UserTurn))
    );
    assert!(
        routes
            .iter()
            .any(|r| **r == Route::Direct(DirectCarrier::SystemPrompt))
    );
    assert!(
        routes
            .iter()
            .any(|r| matches!(r, Route::Direct(DirectCarrier::ToolResult(_))))
    );
    assert!(routes.iter().any(|r| **r == Route::Unobserved));
}

#[test]
fn every_match_kind_codec_chain_and_carrier_is_present() {
    let w = &shared().world;
    let matches: Vec<_> = w
        .transmissions
        .iter()
        .filter_map(|t| confirmed(&t.transmission.state))
        .flat_map(|c| c.content().iter().cloned().collect::<Vec<_>>())
        .collect();
    assert!(matches.iter().any(|m| *m.kind() == MatchKind::Exact));
    assert!(matches.iter().any(|m| *m.kind() == MatchKind::Normalized));
    assert!(
        matches
            .iter()
            .any(|m| matches!(m.kind(), MatchKind::Semantic(s) if s.get() > 0.5))
    );
    let chains: HashSet<Vec<Codec>> = matches
        .iter()
        .filter_map(|m| match m.kind() {
            MatchKind::Decoded(chain) => Some(chain.iter().copied().collect()),
            _ => None,
        })
        .collect();
    assert!(chains.contains(&vec![Codec::Base64, Codec::UrlEncoding]));
    assert!(chains.len() >= 4, "{chains:?}");
    assert!(
        matches
            .iter()
            .any(|m| matches!(m.carrier(), Carrier::ToolResult(_)))
    );
    assert!(matches.iter().any(|m| *m.carrier() == Carrier::UserTurn));
    assert!(
        matches
            .iter()
            .any(|m| *m.carrier() == Carrier::SystemPrompt)
    );
    assert!(
        matches
            .iter()
            .any(|m| *m.carrier() == Carrier::ReaderOutput)
    );
}

#[test]
fn confirmed_transmissions_have_text_and_assignments() {
    let w = &shared().world;
    for record in &w.transmissions {
        match confirmed(&record.transmission.state) {
            Some(c) => {
                assert_eq!(c.content().iter().count(), record.texts.len());
                assert_eq!(record.from, Some(c.from()));
                assert_eq!(record.matched_bytes, c.matched_bytes().get());
                // Only a classified transmission has a topic.
                if matches!(record.transmission.state, TransmissionState::Confirmed(_)) {
                    assert!(record.assignments.is_empty(), "not classified yet");
                } else {
                    assert_eq!(record.assignments.len(), 3);
                    assert_eq!(record.assignments[0], Assignment::Outlier, "v0 is unfitted");
                }
                for (m, text) in c.content().iter().zip(&record.texts) {
                    // The span and the match locate their text in the
                    // stored bodies, as the evidence page cuts it.
                    let origin = w.blobs.span(m.origin()).expect("span recorded");
                    for (at, core) in [(origin, &text.origin), (m.read_at(), &text.read)] {
                        let Some(body) = w.blobs.body(at.part.message) else {
                            continue;
                        };
                        let part = body.part_text(at.part.index).expect("part text");
                        let range = at.range.start() as usize..at.range.end() as usize;
                        let located = part.get(range).expect("range in part");
                        assert!(core.contains(located), "the location is in the text");
                    }
                    assert!(text.read.len() as u32 >= m.read_at().range.len().get());
                }
            }
            None => {
                assert!(record.texts.is_empty());
                assert!(record.from.is_none());
                assert!(record.assignments.is_empty());
            }
        }
    }
}

#[test]
fn co_access_records_are_consistent_with_accesses() {
    let w = &shared().world;
    let mut checked = 0;
    for record in &w.transmissions {
        for co in co_accesses(&record.transmission.state) {
            let write = w.access(co.write()).expect("write access");
            let read = w.access(co.read()).expect("read access");
            assert_eq!(CoAccess::new(write, read, CORRELATION_WINDOW), Ok(co));
            assert_eq!(read.agent, record.transmission.to);
            assert!(matches!(record.transmission.route, Route::Channel(_)));
            checked += 1;
        }
    }
    assert!(checked > 1000, "{checked}");
}

#[test]
fn decoded_matches_show_encoded_text() {
    let w = &shared().world;
    let found = w.transmissions.iter().any(|t| {
        confirmed(&t.transmission.state).is_some_and(|c| {
            c.content().iter().any(|m| {
                let at = m.read_at();
                let matched = w.blobs.body(at.part.message).and_then(|body| {
                    let part = body.part_text(at.part.index).ok()?;
                    part.get(at.range.start() as usize..at.range.end() as usize)
                        .map(str::to_owned)
                });
                matches!(m.kind(), MatchKind::Decoded(chain) if chain.iter().copied().collect::<Vec<_>>() == vec![Codec::Base64, Codec::UrlEncoding])
                    && matched.is_some_and(|text| text.contains("%3D"))
            })
        })
    });
    assert!(found, "base64 then url-encoded padding shows as %3D");
}
