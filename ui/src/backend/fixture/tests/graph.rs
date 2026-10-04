//! Graph reads: every method answers for the default day and the whole
//! week, graphs satisfy the spec's invariants, and the filter behaves as
//! `TopologyFilter::admits` defines.

use std::collections::{BTreeSet, HashSet};

use crosstalk_spec::aggregates::edge::{RouteKind, TopologyGraph, Weighting};
use crosstalk_spec::aggregates::filter::FalseDetections;
use crosstalk_spec::aggregates::node::GraphNode;
use crosstalk_spec::aggregates::series::SeriesGrouping;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::interfaces::l8_surface::lists::SearchMode;
use crosstalk_spec::interfaces::l8_surface::summary::TopicUnder;
use crosstalk_spec::interfaces::l8_surface::{AlertFilter, ConflictKind, QueryError};

use super::super::clock::WATERMARK;
use super::super::world::{ChannelKey, confirmed};
use super::{day, first, graph_of, node_ids, researcher, shared, week};
use crate::pending::channel_semantics::ChannelFilter;
use crate::url::scope::{Scope, ViewFilter};
use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::aggregates::projection::ProjectionStatusKind;
use crosstalk_spec::interfaces::l8_surface::QueryApi;
use crosstalk_spec::interfaces::l8_surface::audit::AuditFilter;

use super::reads_support::*;

#[tokio::test]
async fn every_method_answers_for_the_day_and_the_week() {
    let b = shared();
    let c = researcher();
    assert_eq!(
        b.watermark(&c).await.map(|w| w.at()),
        Ok(WATERMARK),
        "the watermark"
    );
    for scope in [day(), week()] {
        let filter = scope.topology_filter();
        let topo = graph_of(b, &c, &scope, Weighting::Transmissions)
            .await
            .expect("topology");
        assert!(!topo.value.edges().is_empty());
        let bip = b
            .channel_topology(&c, scope.window, Weighting::MatchedBytes, &filter)
            .await
            .expect("bipartite");
        assert!(!bip.value.accesses().is_empty());
        assert!(
            bip.value
                .nodes()
                .iter()
                .any(|n| matches!(n, GraphNode::Channel(_)))
        );
        assert!(!bip.value.transmissions().is_empty());
        let series = b
            .series(
                &c,
                grid(scope.window, 24),
                Weighting::Transmissions,
                SeriesGrouping::Total,
                &filter,
            )
            .await
            .expect("series");
        assert_eq!(series.value.grid().points().get(), 24);
        assert!(series.value.total() > 0);
        let overview = b
            .overview(&c, scope.window, &filter)
            .await
            .expect("overview");
        assert!(overview.value.activity.transmissions > 0);
        assert!(!all_transmissions(&scope).await.is_empty());
        let hits = search_in(
            b,
            &c,
            &search("deploy", SearchMode::Hybrid),
            &scope,
            &first(20),
        )
        .await
        .expect("search");
        assert!(!hits.items().is_empty());
        let sizes = b
            .topic_sizes(&c, None, Some(scope.window))
            .await
            .expect("sizes");
        assert!(sizes.value.topics().iter().any(|size| size.stats.is_some()));
        let id = b
            .fit_projection(&c, scope.window, &filter, params(1, 500))
            .await
            .expect("fit");
        assert_eq!(
            b.projection_status(&c, id)
                .await
                .map(|info| info.status().kind()),
            Ok(ProjectionStatusKind::Ready)
        );
        assert!(
            b.projection(&c, id)
                .await
                .expect("projection")
                .frame()
                .count()
                > 0
        );
        assert!(
            !b.detection_quality(&c, scope.window)
                .await
                .expect("quality")
                .rows()
                .is_empty()
        );
        let wiki = channel(ChannelKey::HijackedWiki);
        let uses = b
            .channel_resources(&c, wiki, scope.window, &first(50))
            .await
            .expect("resources")
            .value
            .page;
        assert!(uses.items().iter().any(|u| !u.readers().is_empty()));
    }
    assert!(
        !b.channels(&c, &ChannelFilter::default(), &first(50))
            .await
            .expect("channels")
            .value
            .items()
            .is_empty()
    );
    assert!(
        b.channel(&c, channel(ChannelKey::Pastebin), None)
            .await
            .expect("channel")
            .is_some()
    );
    assert!(
        !b.agents(&c, &Default::default(), week().window, &first(50))
            .await
            .expect("agents")
            .value
            .items()
            .is_empty()
    );
    assert!(
        b.agent(&c, agent("cc0"), week().window)
            .await
            .expect("agent")
            .is_some()
    );
    assert!(
        !b.alerts(&c, &AlertFilter::default(), &first(50))
            .await
            .expect("alerts")
            .items()
            .is_empty()
    );
    assert!(
        b.alert_rules(&c, &Default::default(), &first(50))
            .await
            .expect("rules")
            .items()
            .len()
            >= 9
    );
    assert_eq!(b.sinks(&c).await.expect("sinks").len(), 3);
    assert!(
        !b.audit(&c, &AuditFilter::default(), &first(50))
            .await
            .expect("audit")
            .items()
            .is_empty()
    );
    assert_eq!(b.operators(&c).await.expect("operators").len(), 2);
    assert!(
        !b.dead_letters(&c, None, &first(50))
            .await
            .expect("dead letters")
            .items()
            .is_empty()
    );
    assert_eq!(
        b.topic_versions(&c)
            .await
            .expect("versions")
            .versions()
            .len(),
        3
    );
    assert_eq!(
        b.topics(
            &c,
            TopicVersionSelector::Pinned(TopicModelVersion(2)),
            &first(50)
        )
        .await
        .expect("topics")
        .page
        .items()
        .len(),
        10
    );
    assert!(
        b.topic_lineage(&c, TopicModelVersion(1))
            .await
            .expect("lineage")
            .is_some()
    );
    assert!(
        b.topic_lineage(&c, TopicModelVersion(2))
            .await
            .expect("lineage")
            .is_none()
    );
    assert!(
        !b.projections(&c, &first(50))
            .await
            .expect("jobs")
            .items()
            .is_empty()
    );
}

#[tokio::test]
async fn topology_is_canonical_with_shares_summing_to_one() {
    let b = shared();
    for scope in [day(), week()] {
        for weighting in [Weighting::Transmissions, Weighting::MatchedBytes] {
            let graph = graph_of(b, &researcher(), &scope, weighting)
                .await
                .expect("topology");
            assert_eq!(graph.watermark.at(), WATERMARK);
            let value = &graph.value;
            assert_eq!(value.topic_version(), scope.topic_version);
            assert!(TopologyGraph::new(value.clone().into_parts()).is_ok());
            let total = sum_shares(value.edges().iter().map(|e| e.share.get()));
            assert!((total - 1.0).abs() < 1e-9, "{total}");
            assert!(
                value.edges().iter().all(|e| e.from != e.to),
                "no self-edges"
            );
            let state = b.state.read().await;
            for id in node_ids(value) {
                assert!(!state.identity.is_merged(id), "nodes are canonical");
            }
            let mut keys = HashSet::new();
            for e in value.edges() {
                assert!(
                    keys.insert((e.from, e.to, format!("{:?}", e.route))),
                    "one edge per key"
                );
            }
        }
    }
}

#[tokio::test]
async fn agent_nodes_carry_labels_claims_and_parents() {
    let graph = graph_of(shared(), &researcher(), &week(), Weighting::Transmissions)
        .await
        .expect("topology");
    let agents: Vec<_> = graph
        .value
        .nodes()
        .iter()
        .filter_map(|n| match n {
            GraphNode::Agent(a) => Some(a),
            GraphNode::Channel(_) => None,
        })
        .collect();
    assert!(
        agents
            .iter()
            .any(|a| a.label.as_ref().is_some_and(|l| l.as_str() == "pi-scraper")),
        "labels are the spec's"
    );
    assert!(agents.iter().any(|a| a.parent.is_some()), "sub-agents");
    // A pi agent that also claims Claude Code shows both claims.
    let scraper = agents
        .iter()
        .find(|a| a.label.as_ref().is_some_and(|l| l.as_str() == "pi-scraper"))
        .expect("pi-scraper");
    assert!(scraper.claims.entries().len() >= 2, "{:?}", scraper.claims);
}

#[tokio::test]
async fn edges_count_confirmations_by_their_time() {
    let b = shared();
    let state = b.state.read().await;
    let scope = day();
    let canonical = |id| state.identity.canonical(id);
    let expected = b
        .world
        .transmissions
        .iter()
        .filter_map(|r| confirmed(&r.transmission.state))
        .filter(|c| scope.window.contains(c.at()))
        .count();
    let self_edges = b
        .world
        .transmissions
        .iter()
        .filter_map(|r| Some((r, confirmed(&r.transmission.state)?)))
        .filter(|(r, c)| {
            scope.window.contains(c.at()) && canonical(c.from()) == canonical(r.transmission.to)
        })
        .count();
    drop(state);
    let graph = graph_of(b, &researcher(), &scope, Weighting::Transmissions)
        .await
        .expect("topology");
    let counted: u64 = graph
        .value
        .edges()
        .iter()
        .map(|e| e.stats.transmissions.get())
        .sum();
    assert_eq!(counted as usize, expected - self_edges);
}

#[tokio::test]
async fn the_channel_centred_view_shares_the_topology_edges() {
    let b = shared();
    let c = researcher();
    let scope = week();
    let graph = b
        .channel_topology(
            &c,
            scope.window,
            Weighting::Transmissions,
            &scope.topology_filter(),
        )
        .await
        .expect("bipartite");
    assert_eq!(graph.watermark.at(), WATERMARK);
    let value = &graph.value;
    let access_total = sum_shares(value.accesses().iter().map(|a| a.share.get()));
    assert!((access_total - 1.0).abs() < 1e-9);
    let tx_total = sum_shares(value.transmissions().iter().map(|e| e.share.get()));
    assert!((tx_total - 1.0).abs() < 1e-9);
    let topology = graph_of(b, &c, &scope, Weighting::Transmissions)
        .await
        .expect("topology");
    assert_eq!(value.transmissions(), topology.value.edges());
    let state = b.state.read().await;
    for node in value.nodes() {
        if let GraphNode::Channel(channel) = node {
            assert!(
                state.channels.get(&channel.id).is_some_and(|r| r
                    .channel()
                    .origin
                    .supersession()
                    .is_none()),
                "channel nodes are in force"
            );
        }
    }
    // The hijacked wiki shows its writers and readers.
    let wiki = channel(ChannelKey::HijackedWiki);
    let ops: HashSet<_> = value
        .accesses()
        .iter()
        .filter(|a| a.channel == wiki)
        .map(|a| format!("{:?}", a.op))
        .collect();
    assert_eq!(ops.len(), 2);
}

#[tokio::test]
async fn a_topic_filter_keeps_the_accesses_of_channels_it_flowed_through() {
    let b = shared();
    let topic = b
        .world
        .topics
        .theme_topic(TopicModelVersion(2), super::super::text::Theme::Credentials)
        .expect("topic");
    let scope = with(ViewFilter {
        topics: vec![topic],
        ..Default::default()
    });
    let graph = b
        .channel_topology(
            &researcher(),
            scope.window,
            Weighting::Transmissions,
            &scope.topology_filter(),
        )
        .await
        .expect("bipartite");
    let routed: BTreeSet<_> = graph
        .value
        .transmissions()
        .iter()
        .filter_map(|e| match e.route {
            Route::Channel(channel) => Some(channel),
            _ => None,
        })
        .collect();
    let accessed: BTreeSet<_> = graph.value.accesses().iter().map(|a| a.channel).collect();
    assert!(!accessed.is_empty());
    assert!(
        routed.is_subset(&accessed),
        "every channel the topic was routed through keeps its accesses"
    );
}

#[tokio::test]
async fn agent_filter_matches_sender_or_reader_after_alias_resolution() {
    let b = shared();
    let c = researcher();
    let (cc0, al0) = (agent("cc0"), agent("al0"));
    let filtered = |agent| {
        with(ViewFilter {
            agents: vec![agent],
            ..Default::default()
        })
    };
    let by_canonical = graph_of(b, &c, &filtered(cc0), Weighting::Transmissions)
        .await
        .expect("topology");
    assert!(!by_canonical.value.edges().is_empty());
    assert!(
        by_canonical
            .value
            .edges()
            .iter()
            .all(|e| e.from == cc0 || e.to == cc0)
    );
    let by_alias = graph_of(b, &c, &filtered(al0), Weighting::Transmissions)
        .await
        .expect("topology");
    assert_eq!(by_canonical.value.edges(), by_alias.value.edges());
    // No node is a merged alias.
    let all = graph_of(b, &c, &week(), Weighting::Transmissions)
        .await
        .expect("topology");
    assert!(node_ids(&all.value).iter().all(|n| *n != al0));
}

#[tokio::test]
async fn channel_filter_follows_supersession() {
    let b = shared();
    let notes = channel(ChannelKey::TeamNotes);
    let old = channel(ChannelKey::OldTeamNotes);
    let filtered = |channel| {
        with(ViewFilter {
            channels: vec![channel],
            ..Default::default()
        })
    };
    let by_new = all_transmissions(&filtered(notes)).await;
    let by_old = all_transmissions(&filtered(old)).await;
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
    let c = researcher();
    let graph_new = graph_of(b, &c, &filtered(notes), Weighting::Transmissions)
        .await
        .expect("topology");
    let graph_old = graph_of(b, &c, &filtered(old), Weighting::Transmissions)
        .await
        .expect("topology");
    assert!(!graph_new.value.edges().is_empty());
    assert_eq!(graph_new.value.edges(), graph_old.value.edges());
}

#[tokio::test]
async fn route_and_topic_filters_and_their_conjunction() {
    let routes = vec![RouteKind::Delegation, RouteKind::Direct];
    let by_route = all_transmissions(&with(ViewFilter {
        route_kinds: routes.clone(),
        ..Default::default()
    }))
    .await;
    assert!(!by_route.is_empty());
    assert!(
        by_route
            .iter()
            .all(|t| routes.contains(&RouteKind::from(&t.route)))
    );

    let topic = shared()
        .world
        .topics
        .theme_topic(TopicModelVersion(2), super::super::text::Theme::Credentials)
        .expect("topic");
    let by_topic = all_transmissions(&with(ViewFilter {
        topics: vec![topic],
        ..Default::default()
    }))
    .await;
    assert!(!by_topic.is_empty());
    assert!(
        by_topic
            .iter()
            .all(|t| t.state.topic() == Some(TopicUnder::Topic(topic))),
        "outliers never match"
    );

    let both = all_transmissions(&with(ViewFilter {
        topics: vec![topic],
        route_kinds: routes.clone(),
        ..Default::default()
    }))
    .await;
    assert!(both.iter().all(|t| {
        t.state.topic() == Some(TopicUnder::Topic(topic))
            && routes.contains(&RouteKind::from(&t.route))
    }));
    assert!(both.len() < by_topic.len() && both.len() < by_route.len());

    // A v2 topic under v1 is refused, not silently matched against nothing.
    let v1_scope = Scope {
        topic_version: TopicModelVersion(1),
        ..with(ViewFilter {
            topics: vec![topic],
            ..Default::default()
        })
    };
    let conflict = QueryError::Conflict(ConflictKind::TopicsNotInVersion {
        version: TopicModelVersion(1),
        topics: vec![topic],
    });
    assert_eq!(
        graph_of(shared(), &researcher(), &v1_scope, Weighting::Transmissions)
            .await
            .err(),
        Some(conflict.clone())
    );
    assert_eq!(
        search_in(
            shared(),
            &researcher(),
            &search("deploy", SearchMode::Text),
            &v1_scope,
            &first(5)
        )
        .await
        .err(),
        Some(conflict)
    );
}

#[tokio::test]
async fn verdict_filter_drops_false_detections() {
    let all = all_transmissions(&week()).await;
    assert!(
        all.iter()
            .any(|t| t.state.verdict() == Some(Verdict::FalseDetection))
    );
    let kept = all_transmissions(&with(ViewFilter {
        false_detections: FalseDetections::Exclude,
        ..Default::default()
    }))
    .await;
    assert!(
        kept.iter()
            .all(|t| t.state.verdict() != Some(Verdict::FalseDetection))
    );
    let dropped = all
        .iter()
        .filter(|t| t.state.verdict() == Some(Verdict::FalseDetection))
        .count();
    assert_eq!(kept.len() + dropped, all.len());
    // The withdrawn verdict leaves its transmission unlabelled.
    let state = shared().state.read().await;
    let withdrawn = state
        .verdicts
        .values()
        .find(|log| log.records().iter().any(|r| r.verdict().is_none()))
        .expect("withdrawn")
        .transmission();
    assert!(
        all.iter()
            .any(|t| t.id == withdrawn && t.state.verdict().is_none())
    );
    drop(state);
    // The graph subtracts them too.
    let c = researcher();
    let include = graph_of(shared(), &c, &week(), Weighting::Transmissions)
        .await
        .expect("topology");
    let exclude = graph_of(
        shared(),
        &c,
        &with(ViewFilter {
            false_detections: FalseDetections::Exclude,
            ..Default::default()
        }),
        Weighting::Transmissions,
    )
    .await
    .expect("topology");
    let total = |edges: &[crosstalk_spec::aggregates::edge::WeightedEdge]| -> u64 {
        edges.iter().map(|e| e.stats.transmissions.get()).sum()
    };
    assert!(total(exclude.value.edges()) < total(include.value.edges()));
}

#[tokio::test]
async fn channel_graph_draws_only_listed_channels() {
    use crate::pending::channel_semantics::Confirmation;
    use crate::pending::channel_semantics::UnconfirmedChannels;

    let b = shared();
    let c = researcher();
    let scope = week();
    let graph = async |unconfirmed| {
        let mut filter = scope.topology_filter();
        filter.unconfirmed_channels = unconfirmed;
        b.channel_topology(&c, scope.window, Weighting::Transmissions, &filter)
            .await
            .expect("graph")
            .value
    };
    let all = graph(UnconfirmedChannels::Include).await;
    let nodes: Vec<_> = all
        .nodes()
        .iter()
        .filter_map(|n| match n {
            GraphNode::Channel(channel) => Some((
                channel.id,
                all.confirmation(channel.id).expect("confirmation"),
            )),
            GraphNode::Agent(_) => None,
        })
        .collect();
    let id = |key| b.world.scenario.channel(key).expect("channel");
    let s3 = id(ChannelKey::S3Handoff);
    assert!(nodes.contains(&(s3, Confirmation::Unconfirmed)), "marked");
    for absent in [
        ChannelKey::SelfNotes,
        ChannelKey::DesignDocs,
        ChannelKey::ReleaseBucket,
    ] {
        assert!(
            nodes.iter().all(|(n, _)| *n != id(absent)),
            "{absent:?} is not drawn"
        );
    }
    assert!(
        nodes
            .iter()
            .filter(|(n, _)| *n != s3)
            .all(|(_, confirmation)| *confirmation == Confirmation::Confirmed)
    );
    // The resource only cc7 uses has no node and no access edge: its
    // accesses never reach a channel.
    let cc7 = b.world.scenario.agent("cc7").expect("cc7");
    let lone_accesses = b
        .world
        .accesses
        .iter()
        .filter(|a| a.resource == b.world.scenario.lone_resource)
        .count() as u64;
    let cc7_drawn: u64 = all
        .accesses()
        .iter()
        .filter(|a| a.agent == cc7)
        .map(|a| a.accesses.get())
        .sum();
    let cc7_on_channels = b
        .world
        .accesses
        .iter()
        .filter(|a| a.agent == cc7 && scope.window.contains(a.at))
        .filter(|a| b.world.resource_channel.contains_key(&a.resource))
        .count() as u64;
    assert!(lone_accesses > 0);
    assert!(
        cc7_drawn <= cc7_on_channels,
        "none of the lone accesses is drawn"
    );
    // Confirmed only drops the unconfirmed channel and nothing else.
    let confirmed = graph(UnconfirmedChannels::Exclude).await;
    let kept: Vec<_> = confirmed
        .nodes()
        .iter()
        .filter_map(|n| match n {
            GraphNode::Channel(channel) => Some((
                channel.id,
                confirmed.confirmation(channel.id).expect("confirmation"),
            )),
            GraphNode::Agent(_) => None,
        })
        .collect();
    let expected: Vec<_> = nodes.iter().copied().filter(|(n, _)| *n != s3).collect();
    assert_eq!(kept, expected);
    assert!(confirmed.accesses().iter().all(|a| a.channel != s3));
    assert_eq!(confirmed.transmissions(), all.transmissions());
}

#[tokio::test]
async fn no_view_lists_a_transmission_within_one_agent() {
    use crate::pending::channel_semantics::Crossing;

    let b = shared();
    let c = researcher();
    let scope = week();
    let state = b.state.read().await;
    let ctx = super::super::queries::Ctx::new(&b.world, &state);
    let within: HashSet<_> = b
        .world
        .transmissions
        .iter()
        .filter(|t| ctx.crossing(&t.transmission) == Crossing::WithinOneAgent)
        .map(|t| t.transmission.id)
        .collect();
    drop(state);
    assert!(!within.is_empty(), "merges left some within one agent");
    let request = search("the", SearchMode::Text);
    let hits = super::collect(400, async |p| search_in(b, &c, &request, &scope, &p).await).await;
    assert!(!hits.is_empty());
    assert!(hits.iter().all(|h| !within.contains(&h.transmission)));
}
