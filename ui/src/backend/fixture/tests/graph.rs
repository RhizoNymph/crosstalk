//! Graph reads: every method answers for the default day and the whole
//! week, views satisfy their invariants, and the scope filter behaves as
//! `contract/scope.rs` documents.

use std::collections::HashSet;

use crosstalk_spec::aggregates::edge::{RouteKind, Weighting};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::interfaces::l8_surface::AlertFilter;

use super::super::clock::WATERMARK;
use super::super::world::ChannelKey;
use super::{day, first, researcher, shared, week};
use crate::backend::Backend;
use crate::contract::channels::ChannelListFilter;
use crate::contract::errors::QueryError;
use crate::contract::graph::TransmissionSelector;
use crate::contract::research::{AuditFilter, ProjectionJob};
use crate::contract::scope::{Scope, TopologyFilter, VerdictFilter};
use crate::contract::search::SearchMode;
use crate::contract::verdict::Verdict;

use super::reads_support::*;

#[tokio::test]
async fn every_method_answers_for_the_day_and_the_week() {
    let b = shared();
    let c = researcher();
    for scope in [day(), week()] {
        let topo = b
            .topology(&c, &scope, Weighting::Transmissions)
            .await
            .expect("topology");
        assert!(!topo.graph().edges.is_empty());
        let bip = b
            .channel_topology(&c, &scope, Weighting::MatchedBytes)
            .await
            .expect("bipartite");
        assert!(!bip.accesses().is_empty() && !bip.channels().is_empty());
        assert!(!bip.transmissions().is_empty());
        let timeline = b.timeline(&c, &scope, n(24)).await.expect("timeline");
        assert_eq!(timeline.buckets.len(), 24);
        assert!(
            timeline
                .buckets
                .iter()
                .map(|b| b.transmissions)
                .sum::<u64>()
                > 0
        );
        assert!(!all_transmissions(&scope).await.is_empty());
        let hits = b
            .search(
                &c,
                &search("deploy", SearchMode::Hybrid),
                &scope,
                &first(20),
            )
            .await
            .expect("search");
        assert!(!hits.items.is_empty());
        let stats = b.topic_stats(&c, &scope, n(12)).await.expect("stats");
        assert!(stats.iter().map(|s| s.transmissions).sum::<u64>() > 0);
        let id = b
            .fit_projection(&c, &scope, params(1, 500))
            .await
            .expect("fit");
        assert!(matches!(
            b.projection_job(&c, id).await,
            Ok(ProjectionJob::Ready(_))
        ));
        assert!(!b.projection(&c, id).await.expect("points").is_empty());
        assert!(
            !b.detection_quality(&c, scope.window)
                .await
                .expect("quality")
                .is_empty()
        );
        let wiki = channel(ChannelKey::HijackedWiki);
        let uses = b
            .channel_resources(&c, wiki, scope.window)
            .await
            .expect("resources");
        assert!(uses.iter().any(|u| !u.readers.is_empty()));
    }
    assert!(
        !b.channels(&c, &ChannelListFilter::default(), &first(50))
            .await
            .expect("channels")
            .items
            .is_empty()
    );
    assert!(
        b.channel(&c, channel(ChannelKey::Pastebin))
            .await
            .expect("channel")
            .is_some()
    );
    assert!(
        !b.agents(&c, &Default::default(), &first(50))
            .await
            .expect("agents")
            .items
            .is_empty()
    );
    assert!(b.agent(&c, agent("cc0")).await.expect("agent").is_some());
    assert!(
        !b.alerts(&c, &AlertFilter::default(), &first(50))
            .await
            .expect("alerts")
            .items
            .is_empty()
    );
    assert!(b.rules(&c).await.expect("rules").len() >= 9);
    assert_eq!(b.sinks(&c).await.expect("sinks").len(), 3);
    assert!(
        !b.audit(&c, &AuditFilter::default(), &first(50))
            .await
            .expect("audit")
            .items
            .is_empty()
    );
    assert_eq!(b.operators(&c).await.expect("operators").len(), 2);
    assert!(
        !b.dead_letters(&c, &first(50))
            .await
            .expect("dead letters")
            .items
            .is_empty()
    );
    assert_eq!(b.topic_versions(&c).await.expect("versions").len(), 3);
    assert_eq!(
        b.topics(&c, TopicModelVersion(2))
            .await
            .expect("topics")
            .len(),
        10
    );
    assert!(
        b.topic_remap(&c, TopicModelVersion(1))
            .await
            .expect("remap")
            .is_some()
    );
    assert!(
        b.topic_remap(&c, TopicModelVersion(2))
            .await
            .expect("remap")
            .is_none()
    );
}

#[tokio::test]
async fn topology_is_canonical_with_shares_summing_to_one() {
    let b = shared();
    for scope in [day(), week()] {
        for weighting in [Weighting::Transmissions, Weighting::MatchedBytes] {
            let view = b
                .topology(&researcher(), &scope, weighting)
                .await
                .expect("topology");
            let edges = &view.graph().edges;
            let total = sum_shares(edges.iter().map(|e| e.share.get()));
            assert!((total - 1.0).abs() < 1e-9, "{total}");
            assert!(edges.iter().all(|e| e.from != e.to), "no self-edges");
            let state = b.state.read().await;
            let node_ids: HashSet<_> = view.nodes().iter().map(|n| n.id).collect();
            for node in view.nodes() {
                assert!(!state.is_merged(node.id), "nodes are canonical");
                if let Some(parent) = node.parent {
                    assert!(node_ids.contains(&parent), "parents are included");
                }
            }
            let mut keys = HashSet::new();
            for e in edges {
                assert!(
                    keys.insert((e.from, e.to, format!("{:?}", e.route))),
                    "one edge per key"
                );
            }
            assert_eq!(view.watermark(), WATERMARK);
        }
    }
}

#[tokio::test]
async fn bipartite_view_normalises_accesses_and_transmissions_separately() {
    let view = shared()
        .channel_topology(&researcher(), &week(), Weighting::Transmissions)
        .await
        .expect("bipartite");
    let access_total = sum_shares(view.accesses().iter().map(|a| a.share.get()));
    assert!((access_total - 1.0).abs() < 1e-9);
    let tx_total = sum_shares(view.transmissions().iter().map(|e| e.share.get()));
    assert!((tx_total - 1.0).abs() < 1e-9);
    assert!(
        view.transmissions()
            .iter()
            .all(|e| !matches!(e.route, Route::Channel(_)))
    );
    let state = shared().state.read().await;
    for node in view.channels() {
        assert!(
            state
                .channels
                .get(&node.id)
                .is_some_and(|r| r.superseded.is_none())
        );
    }
    // The hijacked wiki shows its writers and readers.
    let wiki = channel(ChannelKey::HijackedWiki);
    let ops: HashSet<_> = view
        .accesses()
        .iter()
        .filter(|a| a.channel == wiki)
        .map(|a| format!("{:?}", a.op))
        .collect();
    assert_eq!(ops.len(), 2);
}

#[tokio::test]
async fn timeline_totals_match_the_graph() {
    let b = shared();
    let c = researcher();
    for scope in [day(), week()] {
        let view = b
            .topology(&c, &scope, Weighting::Transmissions)
            .await
            .expect("topology");
        let edge_tx: u64 = view
            .graph()
            .edges
            .iter()
            .map(|e| e.stats.transmissions.get())
            .sum();
        let edge_bytes: u64 = view
            .graph()
            .edges
            .iter()
            .map(|e| e.stats.matched_bytes.get())
            .sum();
        let timeline = b.timeline(&c, &scope, n(7)).await.expect("timeline");
        assert_eq!(
            timeline
                .buckets
                .iter()
                .map(|b| b.transmissions)
                .sum::<u64>(),
            edge_tx
        );
        assert_eq!(
            timeline
                .buckets
                .iter()
                .map(|b| b.matched_bytes)
                .sum::<u64>(),
            edge_bytes
        );
        assert_eq!(
            timeline.buckets.first().map(|b| b.bucket.start()),
            Some(scope.window.start())
        );
        assert_eq!(
            timeline.buckets.last().map(|b| b.bucket.end()),
            Some(scope.window.end())
        );
        assert!(
            timeline
                .buckets
                .windows(2)
                .all(|w| w[0].bucket.end() == w[1].bucket.start())
        );
    }
}

#[tokio::test]
async fn agent_filter_matches_sender_or_reader_after_alias_resolution() {
    let b = shared();
    let (cc0, al0) = (agent("cc0"), agent("al0"));
    let by_canonical = b
        .topology(
            &researcher(),
            &with(TopologyFilter {
                agents: vec![cc0],
                ..Default::default()
            }),
            Weighting::Transmissions,
        )
        .await
        .expect("topology");
    assert!(!by_canonical.graph().edges.is_empty());
    assert!(
        by_canonical
            .graph()
            .edges
            .iter()
            .all(|e| e.from == cc0 || e.to == cc0)
    );
    let by_alias = b
        .topology(
            &researcher(),
            &with(TopologyFilter {
                agents: vec![al0],
                ..Default::default()
            }),
            Weighting::Transmissions,
        )
        .await
        .expect("topology");
    assert_eq!(by_canonical.graph().edges, by_alias.graph().edges);
    // No node is a merged alias.
    let all = b
        .topology(&researcher(), &week(), Weighting::Transmissions)
        .await
        .expect("topology");
    assert!(all.nodes().iter().all(|n| n.id != al0));
}

#[tokio::test]
async fn channel_filter_follows_supersession() {
    let b = shared();
    let notes = channel(ChannelKey::TeamNotes);
    let old = channel(ChannelKey::OldTeamNotes);
    let by_new = all_transmissions(&with(TopologyFilter {
        channels: vec![notes],
        ..Default::default()
    }))
    .await;
    let by_old = all_transmissions(&with(TopologyFilter {
        channels: vec![old],
        ..Default::default()
    }))
    .await;
    assert_eq!(by_new, by_old);
    assert!(
        by_new.iter().all(|t| t.route == Route::Channel(notes)),
        "routes resolve to the declared channel"
    );
    let raw_old = b
        .world
        .transmissions
        .iter()
        .filter(|t| t.transmission.route == Route::Channel(old))
        .count();
    assert!(
        raw_old > 0 && by_new.len() > raw_old,
        "old traffic counts for the new channel"
    );
}

#[tokio::test]
async fn route_and_topic_filters_and_their_conjunction() {
    let routes = vec![RouteKind::Delegation, RouteKind::Direct];
    let by_route = all_transmissions(&with(TopologyFilter {
        route_kinds: routes.clone(),
        ..Default::default()
    }))
    .await;
    assert!(!by_route.is_empty());
    assert!(by_route.iter().all(|t| routes.contains(&t.route_kind)));

    let topic = shared()
        .world
        .topics
        .theme_topic(TopicModelVersion(2), super::super::text::Theme::Credentials)
        .expect("topic");
    let by_topic = all_transmissions(&with(TopologyFilter {
        topics: vec![topic],
        ..Default::default()
    }))
    .await;
    assert!(!by_topic.is_empty());
    assert!(
        by_topic.iter().all(|t| t.topic == Some(topic)),
        "outliers never match"
    );

    let both = all_transmissions(&with(TopologyFilter {
        topics: vec![topic],
        route_kinds: routes.clone(),
        ..Default::default()
    }))
    .await;
    assert!(
        both.iter()
            .all(|t| t.topic == Some(topic) && routes.contains(&t.route_kind))
    );
    assert!(both.len() < by_topic.len() && both.len() < by_route.len());

    // Topics are read under the scope's version: v1 ids do not match v2.
    let v1_scope = Scope {
        topic_version: TopicModelVersion(1),
        ..with(TopologyFilter {
            topics: vec![topic],
            ..Default::default()
        })
    };
    assert!(all_transmissions(&v1_scope).await.is_empty());
}

#[tokio::test]
async fn verdict_filter_drops_false_detections() {
    let all = all_transmissions(&week()).await;
    assert!(
        all.iter()
            .any(|t| t.verdict == Some(Verdict::FalseDetection))
    );
    let kept = all_transmissions(&with(TopologyFilter {
        verdicts: VerdictFilter::ExcludeFalseDetections,
        ..Default::default()
    }))
    .await;
    assert!(
        kept.iter()
            .all(|t| t.verdict != Some(Verdict::FalseDetection))
    );
    let dropped = all
        .iter()
        .filter(|t| t.verdict == Some(Verdict::FalseDetection))
        .count();
    assert_eq!(kept.len() + dropped, all.len());
    // The withdrawn verdict leaves its transmission unlabelled.
    let state = shared().state.read().await;
    let withdrawn = state
        .verdicts
        .iter()
        .find(|v| v.verdict.is_none())
        .expect("withdrawn")
        .transmission;
    assert!(all.iter().any(|t| t.id == withdrawn && t.verdict.is_none()));
}

#[tokio::test]
async fn unretained_topic_versions_are_typed_errors() {
    let b = shared();
    let c = researcher();
    let version = TopicModelVersion(9);
    let scope = Scope {
        topic_version: version,
        ..week()
    };
    let expected = QueryError::VersionNotRetained { version };
    assert_eq!(
        b.topology(&c, &scope, Weighting::Transmissions).await.err(),
        Some(expected.clone())
    );
    assert_eq!(
        b.channel_topology(&c, &scope, Weighting::Transmissions)
            .await
            .err(),
        Some(expected.clone())
    );
    assert_eq!(
        b.timeline(&c, &scope, n(4)).await.err(),
        Some(expected.clone())
    );
    assert_eq!(
        b.transmissions(&c, &scope, &TransmissionSelector::All, &first(5))
            .await
            .err(),
        Some(expected.clone())
    );
    assert_eq!(
        b.search(&c, &search("deploy", SearchMode::Text), &scope, &first(5))
            .await
            .err(),
        Some(expected.clone())
    );
    assert_eq!(b.topics(&c, version).await.err(), Some(expected.clone()));
    assert_eq!(
        b.topic_stats(&c, &scope, n(4)).await.err(),
        Some(expected.clone())
    );
    assert_eq!(
        b.topic_remap(&c, version).await.err(),
        Some(expected.clone())
    );
    assert_eq!(
        b.fit_projection(&c, &scope, params(1, 10)).await.err(),
        Some(expected)
    );
}
