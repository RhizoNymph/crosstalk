//! Graph, totals, channel topology, drill-down and agent traffic.

use crosstalk_spec::aggregates::agents::AgentTraffic;
use crosstalk_spec::aggregates::edge::{
    EdgeSelector, EdgeTotals, RouteKind, TopologyFilter, TopologyGraph, Weighting,
};
use crosstalk_spec::aggregates::filter::{
    FalseDetections, TopicVersionSelector, UnconfirmedChannels,
};
use crosstalk_spec::aggregates::node::{CanonicalOriginKind, CanonicalStateKind, GraphNode};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::channel::confirmation::{Confirmation, Listing};
use crosstalk_spec::derived::flow::channel::detection::DetectionKind;
use crosstalk_spec::derived::flow::channel::policy::PolicyKind;
use crosstalk_spec::derived::flow::transmission::{DelegationDirection, DirectCarrier, Route};
use crosstalk_spec::derived::flow::verdict::{Verdict, VerdictRevision};
use crosstalk_spec::interfaces::l7_topology::{
    AccessContribution, EdgeError, EdgeQueryError, EdgeStore,
};
use crosstalk_spec::paging::{EdgeTransmissionList, PageRequest, PageSize};
use crosstalk_spec::support::TimeWindow;

use super::support::{World, all, contribution, edge_counts, graph, hold, plain, refit, world};
use crate::model::build::{access, agent, channel, resource, transmission, ts, window};
use crosstalk_spec::interfaces::l7_topology::AgentFacts as AgentDescription;

fn first_page(size: u16) -> PageRequest<EdgeTransmissionList> {
    PageRequest {
        size: PageSize::new(size).unwrap(),
        after: None,
    }
}

async fn apply_all(
    world: &mut World,
    contributions: &[crosstalk_spec::interfaces::l7_topology::EdgeContribution],
) {
    for one in contributions {
        world.store.apply(one).await.unwrap();
    }
}

#[tokio::test]
async fn apply_buckets_boundary_instant_into_next_bucket() {
    // topology.apply.aligned-bucket
    let mut world = world();
    let key = world
        .store
        .apply(&plain(1, 1, 2, Route::Unobserved, 20, 5))
        .await
        .unwrap();
    assert_eq!(key.bucket(), window(20, 30).unwrap());
    let key = world
        .store
        .apply(&plain(2, 1, 2, Route::Unobserved, 29, 5))
        .await
        .unwrap();
    assert_eq!(key.bucket(), window(20, 30).unwrap());
    assert_eq!(key.from(), agent(1));
    assert_eq!(key.to(), agent(2));
    assert_eq!(key.topic().version, TopicModelVersion(0));
    assert_eq!(key.topic().topic, None);
}

#[tokio::test]
async fn apply_self_edge_errors_and_stores_nothing() {
    // topology.apply.rejects-self-edge
    let mut world = world();
    assert_eq!(
        world
            .store
            .apply(&plain(1, 1, 1, Route::Unobserved, 20, 5))
            .await,
        Err(EdgeError::SelfEdge)
    );
    assert!(world.store.contributions().is_empty());
}

#[tokio::test]
async fn apply_is_idempotent_per_transmission_and_version() {
    // topology.apply.idempotent and increments-one-bucket
    let mut world = world();
    world
        .store
        .apply(&plain(1, 1, 2, Route::Unobserved, 20, 5))
        .await
        .unwrap();
    let before = graph(&world, all(), &TopologyFilter::default()).await;
    world
        .store
        .apply(&plain(1, 1, 2, Route::Unobserved, 20, 5))
        .await
        .unwrap();
    assert_eq!(
        graph(&world, all(), &TopologyFilter::default()).await,
        before
    );
    assert_eq!(edge_counts(&before), vec![(1, 2, 1, 5)]);
    world
        .store
        .apply(&plain(2, 1, 2, Route::Unobserved, 21, 7))
        .await
        .unwrap();
    let after = graph(&world, window(20, 30).unwrap(), &TopologyFilter::default()).await;
    assert_eq!(edge_counts(&after), vec![(1, 2, 2, 12)]);
    let other_bucket = graph(&world, window(30, 40).unwrap(), &TopologyFilter::default()).await;
    assert!(other_bucket.edges().is_empty());
}

#[tokio::test]
async fn graph_rejects_window_cutting_a_bucket() {
    // topology.graph.rejects-unaligned-window
    let world = world();
    for (start, end) in [(5, 20), (0, 25), (3, 7)] {
        assert_eq!(
            world
                .store
                .graph(
                    window(start, end).unwrap(),
                    Weighting::Transmissions,
                    &TopologyFilter::default()
                )
                .await,
            Err(EdgeQueryError::UnalignedWindow)
        );
    }
    assert!(
        world
            .store
            .graph(
                window(10, 40).unwrap(),
                Weighting::Transmissions,
                &TopologyFilter::default()
            )
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn fresh_store_graph_reports_version_zero() {
    // topology.version.initial-zero
    let world = world();
    assert_eq!(
        graph(&world, all(), &TopologyFilter::default())
            .await
            .topic_version(),
        TopicModelVersion(0)
    );
}

#[tokio::test]
async fn agent_filter_resolves_merged_ids() {
    // topology.filter.agent-membership
    let mut world = world();
    apply_all(
        &mut world,
        &[
            plain(1, 1, 2, Route::Unobserved, 20, 5),
            plain(2, 3, 4, Route::Unobserved, 20, 5),
            plain(3, 9, 4, Route::Unobserved, 20, 5),
        ],
    )
    .await;
    world.directory.merge(agent(9), agent(1)).unwrap();
    let filter = TopologyFilter {
        agents: vec![agent(9)],
        ..TopologyFilter::default()
    };
    assert_eq!(
        edge_counts(&graph(&world, all(), &filter).await),
        vec![(1, 2, 1, 5), (1, 4, 1, 5)]
    );
}

#[tokio::test]
async fn channel_filter_excludes_non_channel_routes() {
    // topology.filter.channel-membership and route.resolves-supersession
    let mut world = world();
    apply_all(
        &mut world,
        &[
            plain(1, 1, 2, Route::Channel(channel(1)), 20, 5),
            plain(2, 1, 2, Route::Channel(channel(2)), 20, 5),
            plain(3, 1, 2, Route::Unobserved, 20, 5),
        ],
    )
    .await;
    world.directory.supersede(channel(2), channel(1)).unwrap();
    let filter = TopologyFilter {
        channels: vec![channel(1)],
        ..TopologyFilter::default()
    };
    let filtered = graph(&world, all(), &filter).await;
    assert_eq!(filtered.edges().len(), 1);
    assert_eq!(filtered.edges()[0].route, Route::Channel(channel(1)));
    assert_eq!(filtered.edges()[0].stats.transmissions.get(), 2);
    // Listing the superseded channel selects its superseder.
    let by_old = TopologyFilter {
        channels: vec![channel(2)],
        ..TopologyFilter::default()
    };
    assert_eq!(graph(&world, all(), &by_old).await, filtered);
}

#[tokio::test]
async fn route_kind_filter_maps_each_route_variant() {
    // topology.filter.route-kind-membership
    let mut world = world();
    let routes = [
        Route::Channel(channel(1)),
        Route::Delegation(DelegationDirection::ChildToParent),
        Route::Direct(DirectCarrier::UserTurn),
        Route::Unobserved,
    ];
    for (n, route) in routes.iter().enumerate() {
        let n = u64::try_from(n).unwrap();
        world
            .store
            .apply(&plain(n, 1, 2, route.clone(), 20, 1))
            .await
            .unwrap();
    }
    for kind in [
        RouteKind::Channel,
        RouteKind::Delegation,
        RouteKind::Direct,
        RouteKind::Unobserved,
    ] {
        let filter = TopologyFilter {
            route_kinds: vec![kind],
            ..TopologyFilter::default()
        };
        let filtered = graph(&world, all(), &filter).await;
        assert_eq!(filtered.edges().len(), 1);
        assert_eq!(RouteKind::of(&filtered.edges()[0].route), kind);
    }
}

#[tokio::test]
async fn merge_rekeys_sums_and_drops_edges_and_unmerge_restores_graph() {
    // topology.graph.merge-sums-edges, unmerge-splits-edges,
    // no-self-edges and nodes-canonical
    let mut world = world();
    apply_all(
        &mut world,
        &[
            plain(1, 1, 3, Route::Unobserved, 20, 5),
            plain(2, 2, 3, Route::Unobserved, 20, 7),
            plain(3, 1, 2, Route::Unobserved, 20, 9),
        ],
    )
    .await;
    let before = graph(&world, all(), &TopologyFilter::default()).await;
    world.directory.merge(agent(1), agent(2)).unwrap();
    let merged = graph(&world, all(), &TopologyFilter::default()).await;
    assert_eq!(edge_counts(&merged), vec![(2, 3, 2, 12)]);
    let node_ids: Vec<_> = merged.nodes().iter().map(GraphNode::id).collect();
    assert!(!node_ids.contains(&crosstalk_spec::aggregates::node::NodeId::Agent(agent(1))));
    world.directory.unmerge(agent(1));
    assert_eq!(
        graph(&world, all(), &TopologyFilter::default()).await,
        before
    );
}

#[tokio::test]
async fn weighting_changes_only_shares() {
    // topology.weighting.preserves-edges and graph.shares-sum-to-one
    let mut world = world();
    apply_all(
        &mut world,
        &[
            plain(1, 1, 2, Route::Unobserved, 20, 1),
            plain(2, 2, 3, Route::Unobserved, 20, 3),
            plain(3, 2, 3, Route::Unobserved, 21, 6),
        ],
    )
    .await;
    let counted = world
        .store
        .graph(all(), Weighting::Transmissions, &TopologyFilter::default())
        .await
        .unwrap()
        .value;
    let weighed = world
        .store
        .graph(all(), Weighting::MatchedBytes, &TopologyFilter::default())
        .await
        .unwrap()
        .value;
    assert_eq!(edge_counts(&counted), edge_counts(&weighed));
    let shares = |graph: &TopologyGraph| {
        graph
            .edges()
            .iter()
            .map(|edge| edge.share.get())
            .collect::<Vec<_>>()
    };
    assert_eq!(shares(&counted), vec![1.0 / 3.0, 2.0 / 3.0]);
    assert_eq!(shares(&weighed), vec![0.1, 0.9]);
    for graph in [counted, weighed] {
        let sum: f64 = graph.edges().iter().map(|edge| edge.share.get()).sum();
        assert!((sum - 1.0).abs() <= 1e-9);
        TopologyGraph::new(graph.into_parts()).unwrap();
    }
}

#[tokio::test]
async fn graph_stats_add_over_adjacent_windows() {
    // topology.graph.window-additive
    let mut world = world();
    apply_all(
        &mut world,
        &[
            plain(1, 1, 2, Route::Unobserved, 15, 1),
            plain(2, 1, 2, Route::Unobserved, 25, 3),
            plain(3, 2, 3, Route::Unobserved, 35, 6),
        ],
    )
    .await;
    let a = graph(&world, window(10, 30).unwrap(), &TopologyFilter::default()).await;
    let b = graph(&world, window(30, 50).unwrap(), &TopologyFilter::default()).await;
    let union = graph(&world, window(10, 50).unwrap(), &TopologyFilter::default()).await;
    assert_eq!(edge_counts(&a), vec![(1, 2, 2, 4)]);
    assert_eq!(edge_counts(&b), vec![(2, 3, 1, 6)]);
    assert_eq!(edge_counts(&union), vec![(1, 2, 2, 4), (2, 3, 1, 6)]);
}

#[tokio::test]
async fn graph_nodes_cover_endpoints_and_ancestors() {
    // topology.graph.nodes-cover-endpoints
    let mut world = world();
    world
        .store
        .apply(&plain(1, 1, 2, Route::Unobserved, 20, 1))
        .await
        .unwrap();
    // Agent 2's parent is 5, whose parent is 6; agent 1's parent was
    // merged into it, so it has none.
    world.nodes.set_parent(agent(2), Some(agent(5)));
    world.nodes.set_parent(agent(5), Some(agent(6)));
    world.nodes.set_parent(agent(1), Some(agent(7)));
    world.directory.merge(agent(7), agent(1)).unwrap();
    world.nodes.set_agent(
        agent(6),
        AgentDescription {
            label: None,
            state: CanonicalStateKind::Registered,
            parent: None,
            claims: Default::default(),
        },
    );
    let graph = graph(&world, all(), &TopologyFilter::default()).await;
    TopologyGraph::new(graph.clone().into_parts()).unwrap();
    let agents: Vec<_> = graph
        .nodes()
        .iter()
        .filter_map(|node| match node {
            GraphNode::Agent(agent) => Some((
                agent.id,
                agent.parent,
                agent.transmissions_in,
                agent.transmissions_out,
            )),
            GraphNode::Channel(_) => None,
        })
        .collect();
    assert_eq!(
        agents,
        vec![
            (agent(1), None, 0, 1),
            (agent(2), Some(agent(5)), 1, 0),
            (agent(5), Some(agent(6)), 0, 0),
            (agent(6), None, 0, 0),
        ]
    );
}

#[tokio::test]
async fn totals_match_graph() {
    // topology.totals.match-graph
    let mut world = world();
    apply_all(
        &mut world,
        &[
            plain(1, 1, 2, Route::Channel(channel(1)), 20, 1),
            plain(2, 2, 3, Route::Channel(channel(2)), 20, 3),
            plain(3, 2, 3, Route::Unobserved, 21, 6),
        ],
    )
    .await;
    world.directory.supersede(channel(2), channel(1)).unwrap();
    let totals = world
        .store
        .totals(all(), &TopologyFilter::default())
        .await
        .unwrap()
        .value;
    for weighting in [Weighting::Transmissions, Weighting::MatchedBytes] {
        let graph = world
            .store
            .graph(all(), weighting, &TopologyFilter::default())
            .await
            .unwrap()
            .value;
        assert_eq!(totals, EdgeTotals::of(&graph));
    }
    assert_eq!(totals.transmissions, 3);
    assert_eq!(totals.matched_bytes, 10);
    assert_eq!(totals.active_channels, 1);
}

#[tokio::test]
async fn agent_traffic_counts_node_transmissions() {
    let mut world = world();
    apply_all(
        &mut world,
        &[
            plain(1, 1, 2, Route::Unobserved, 20, 1),
            plain(2, 3, 2, Route::Unobserved, 20, 1),
            plain(3, 2, 4, Route::Unobserved, 20, 1),
        ],
    )
    .await;
    world.directory.merge(agent(3), agent(1)).unwrap();
    let traffic = world
        .store
        .agent_traffic(all(), &[agent(3), agent(2), agent(8)])
        .await
        .unwrap()
        .value;
    assert_eq!(
        traffic.get(&agent(3)),
        Some(&AgentTraffic {
            transmissions_in: 0,
            transmissions_out: 2
        })
    );
    assert_eq!(
        traffic.get(&agent(2)),
        Some(&AgentTraffic {
            transmissions_in: 2,
            transmissions_out: 1
        })
    );
    assert_eq!(traffic.get(&agent(8)), Some(&AgentTraffic::default()));
    assert_eq!(
        world.store.agent_traffic(window(5, 20).unwrap(), &[]).await,
        Err(EdgeQueryError::UnalignedWindow)
    );
}

#[tokio::test]
async fn judge_leaves_buckets_and_exclude_subtracts() {
    // topology.verdict.buckets-untouched, exclude-subtracts and
    // reflected-next-query
    let mut world = world();
    apply_all(
        &mut world,
        &[
            plain(1, 1, 2, Route::Unobserved, 20, 4),
            plain(2, 1, 2, Route::Unobserved, 21, 6),
            plain(3, 2, 3, Route::Unobserved, 21, 1),
        ],
    )
    .await;
    let include = TopologyFilter::default();
    let exclude = TopologyFilter {
        false_detections: FalseDetections::Exclude,
        ..TopologyFilter::default()
    };
    let before = graph(&world, all(), &include).await;
    world
        .store
        .judge(
            transmission(1),
            Some(Verdict::FalseDetection),
            VerdictRevision::FIRST,
        )
        .await
        .unwrap();
    world
        .store
        .judge(
            transmission(3),
            Some(Verdict::FalseDetection),
            VerdictRevision::FIRST,
        )
        .await
        .unwrap();
    // A verdict for a transmission not applied yet is kept too.
    world
        .store
        .judge(
            transmission(4),
            Some(Verdict::FalseDetection),
            VerdictRevision::FIRST,
        )
        .await
        .unwrap();
    assert_eq!(graph(&world, all(), &include).await, before);
    let excluded = graph(&world, all(), &exclude).await;
    assert_eq!(edge_counts(&excluded), vec![(1, 2, 1, 6)]);
    assert_eq!(excluded.edges()[0].share.get(), 1.0);
    world
        .store
        .apply(&plain(4, 1, 2, Route::Unobserved, 22, 9))
        .await
        .unwrap();
    assert_eq!(
        edge_counts(&graph(&world, all(), &exclude).await),
        vec![(1, 2, 1, 6)]
    );
    // Withdrawn: counted again from the next query on.
    world
        .store
        .judge(
            transmission(1),
            None,
            VerdictRevision::FIRST.next().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        edge_counts(&graph(&world, all(), &exclude).await),
        vec![(1, 2, 2, 10)]
    );
}

#[tokio::test]
async fn edge_transmissions_match_graph() {
    // topology.transmissions.match-graph
    let mut world = world();
    apply_all(
        &mut world,
        &[
            plain(1, 1, 2, Route::Unobserved, 20, 4),
            plain(2, 9, 2, Route::Unobserved, 25, 6),
            plain(3, 1, 2, Route::Unobserved, 25, 1),
            plain(4, 1, 2, Route::Direct(DirectCarrier::UserTurn), 25, 1),
            plain(5, 1, 3, Route::Unobserved, 25, 1),
        ],
    )
    .await;
    world.directory.merge(agent(9), agent(1)).unwrap();
    let edge = EdgeSelector::new(agent(9), agent(2), Route::Unobserved).unwrap();
    let mut rows = Vec::new();
    let mut request = first_page(1);
    loop {
        let page = world
            .store
            .transmissions(
                &edge,
                window(0, 1_000).unwrap(),
                &TopologyFilter::default(),
                &request,
            )
            .await
            .unwrap()
            .value;
        assert_eq!(page.topic_version, TopicModelVersion(0));
        let (items, next) = page.page.into_parts();
        rows.extend(items.iter().map(|row| (row.transmission, row.confirmed_at)));
        match next {
            Some(cursor) => request.after = Some(cursor),
            None => break,
        }
    }
    // Newest confirmation first, ties by descending id.
    assert_eq!(
        rows,
        vec![
            (transmission(3), ts(25)),
            (transmission(2), ts(25)),
            (transmission(1), ts(20)),
        ]
    );
    let graph = graph(&world, all(), &TopologyFilter::default()).await;
    let edge_stat = graph
        .edges()
        .iter()
        .find(|graph_edge| {
            graph_edge.from == agent(1)
                && graph_edge.to == agent(2)
                && graph_edge.route == Route::Unobserved
        })
        .unwrap();
    assert_eq!(edge_stat.stats.transmissions.get(), 3);
}

#[tokio::test]
async fn edge_transmission_pages_keep_version_across_activation() {
    // topology.transmissions.pinned-version
    let mut world = world();
    let original = [
        plain(1, 1, 2, Route::Unobserved, 20, 4),
        plain(2, 1, 2, Route::Unobserved, 21, 4),
        plain(3, 1, 2, Route::Unobserved, 22, 4),
    ];
    apply_all(&mut world, &original).await;
    let edge = EdgeSelector::new(agent(1), agent(2), Route::Unobserved).unwrap();
    let first = world
        .store
        .transmissions(&edge, all(), &TopologyFilter::default(), &first_page(1))
        .await
        .unwrap()
        .value;
    assert_eq!(first.topic_version, TopicModelVersion(0));
    let refits: Vec<_> = original
        .iter()
        .map(|one| {
            contribution(
                u64::try_from(one.transmission.as_ulid() - (1u128 << 100)).unwrap(),
                1,
                2,
                Route::Unobserved,
                one.at.as_micros(),
                4,
                1,
                Some(11),
            )
        })
        .collect();
    let v1 = refit(&mut world, 100, &[11], &refits).await;
    assert_eq!(v1, TopicModelVersion(1));
    let mut request = first_page(1);
    request.after = first.page.next().cloned();
    let second = world
        .store
        .transmissions(&edge, all(), &TopologyFilter::default(), &request)
        .await
        .unwrap()
        .value;
    assert_eq!(second.topic_version, TopicModelVersion(0));
    assert_eq!(second.page.items()[0].topic, None);
    // Another edge with the same cursor is an invalid cursor.
    let other = EdgeSelector::new(agent(2), agent(1), Route::Unobserved).unwrap();
    assert_eq!(
        world
            .store
            .transmissions(&other, all(), &TopologyFilter::default(), &request)
            .await,
        Err(EdgeQueryError::InvalidCursor)
    );
    // Once version 0 is dropped, the next page names it.
    world
        .store
        .drop_version(TopicModelVersion(0))
        .await
        .unwrap();
    assert_eq!(
        world
            .store
            .transmissions(&edge, all(), &TopologyFilter::default(), &request)
            .await,
        Err(EdgeQueryError::Version(
            crosstalk_spec::aggregates::filter::VersionUnavailable::NotRetained(TopicModelVersion(
                0
            ))
        ))
    );
    // Current reads the new version.
    let current = world
        .store
        .transmissions(&edge, all(), &TopologyFilter::default(), &first_page(5))
        .await
        .unwrap()
        .value;
    assert_eq!(current.topic_version, v1);
    assert!(current.page.items().iter().all(|row| row.topic.is_some()));
    let pinned = TopologyFilter {
        topic_version: TopicVersionSelector::Pinned(TopicModelVersion(0)),
        ..TopologyFilter::default()
    };
    assert!(
        world
            .store
            .graph(all(), Weighting::Transmissions, &pinned)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn channel_topology_counts_unread_writes() {
    // topology.bipartite.listed-channels-only, transmissions-match-graph
    // and access.apply-idempotent
    let mut world = world();
    // Resource 1 is channel 1's, resource 2 channel 2's; channel 1 is
    // listed as a confirmed channel.
    hold(&world, 1, 1, Listing::Channel(Confirmation::Confirmed));
    hold(&world, 2, 2, Listing::Channel(Confirmation::Unconfirmed));
    let write = AccessContribution {
        access: access(1),
        agent: agent(1),
        resource: resource(1),
        op: AccessKind::Write,
        at: ts(20),
    };
    let edge = world.store.apply_access(&write).await.unwrap();
    assert_eq!(edge.accesses.get(), 1);
    let again = world.store.apply_access(&write).await.unwrap();
    assert_eq!(again, edge);
    world
        .store
        .apply_access(&AccessContribution {
            access: access(2),
            agent: agent(9),
            resource: resource(2),
            ..write
        })
        .await
        .unwrap();
    world
        .store
        .apply_access(&AccessContribution {
            access: access(3),
            agent: agent(2),
            op: AccessKind::Read,
            ..write
        })
        .await
        .unwrap();
    world
        .store
        .apply(&plain(1, 1, 2, Route::Channel(channel(1)), 21, 4))
        .await
        .unwrap();
    world.directory.merge(agent(9), agent(1)).unwrap();
    world.directory.supersede(channel(2), channel(1)).unwrap();
    let bipartite = world
        .store
        .channel_topology(all(), Weighting::Transmissions, &TopologyFilter::default())
        .await
        .unwrap()
        .value;
    let accesses: Vec<_> = bipartite
        .accesses()
        .iter()
        .map(|access| {
            (
                access.agent,
                access.channel,
                access.op,
                access.accesses.get(),
            )
        })
        .collect();
    assert_eq!(
        accesses,
        vec![
            (agent(1), channel(1), AccessKind::Write, 2),
            (agent(2), channel(1), AccessKind::Read, 1),
        ]
    );
    let plain_graph = graph(&world, all(), &TopologyFilter::default()).await;
    assert_eq!(bipartite.transmissions(), plain_graph.edges());
    assert_eq!(bipartite.topic_version(), plain_graph.topic_version());
    // A topic filter keeps no access of a channel without such a topic.
    let channel_nodes = bipartite
        .nodes()
        .iter()
        .filter(|node| matches!(node, GraphNode::Channel(_)))
        .count();
    assert_eq!(channel_nodes, 1);
    let writes_only = TopologyFilter {
        route_kinds: vec![RouteKind::Unobserved],
        ..TopologyFilter::default()
    };
    let none = world
        .store
        .channel_topology(all(), Weighting::Transmissions, &writes_only)
        .await
        .unwrap()
        .value;
    assert!(none.accesses().is_empty());
}

#[test]
fn unaligned_windows_are_refused_by_every_graph_read() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let world = world();
    let cut: TimeWindow = window(5, 15).unwrap();
    runtime.block_on(async {
        assert_eq!(
            world.store.totals(cut, &TopologyFilter::default()).await,
            Err(EdgeQueryError::UnalignedWindow)
        );
        assert_eq!(
            world
                .store
                .channel_topology(cut, Weighting::Transmissions, &TopologyFilter::default())
                .await,
            Err(EdgeQueryError::UnalignedWindow)
        );
    });
}

#[tokio::test]
async fn channel_topology_draws_listed_channels_only() {
    // topology.bipartite.listed-channels-only: an access to a resource on no
    // channel, to a hidden channel or to a declaration without traffic is
    // not drawn; an unconfirmed channel is drawn marked, and left out under
    // UnconfirmedChannels::Exclude.
    let mut world = world();
    hold(&world, 1, 1, Listing::Channel(Confirmation::Unconfirmed));
    hold(&world, 2, 2, Listing::Hidden);
    hold(&world, 3, 3, Listing::Declaration);
    world.nodes.set_resource(resource(4), None);
    for (n, r) in [(1, 1), (2, 2), (3, 3), (4, 4)] {
        world
            .store
            .apply_access(&AccessContribution {
                access: access(n),
                agent: agent(1),
                resource: resource(r),
                op: AccessKind::Write,
                at: ts(20),
            })
            .await
            .unwrap();
    }
    let drawn = world
        .store
        .channel_topology(all(), Weighting::Transmissions, &TopologyFilter::default())
        .await
        .unwrap()
        .value;
    let channels: Vec<_> = drawn
        .accesses()
        .iter()
        .map(|access| access.channel)
        .collect();
    assert_eq!(channels, vec![channel(1)]);
    let confirmations: Vec<_> = drawn
        .nodes()
        .iter()
        .filter_map(|node| match node {
            GraphNode::Channel(node) => Some((node.id, node.confirmation)),
            GraphNode::Agent(_) => None,
        })
        .collect();
    assert_eq!(confirmations, vec![(channel(1), Confirmation::Unconfirmed)]);
    let confirmed_only = TopologyFilter {
        unconfirmed_channels: UnconfirmedChannels::Exclude,
        ..TopologyFilter::default()
    };
    let none = world
        .store
        .channel_topology(all(), Weighting::Transmissions, &confirmed_only)
        .await
        .unwrap()
        .value;
    assert!(none.accesses().is_empty());
    assert!(none.nodes().is_empty());
}

#[tokio::test]
async fn an_unseen_channel_routed_by_an_edge_is_drawn_with_defaults() {
    // topology.node-facts.unknown-channel-defaults: a channel node whose
    // facts the cache has not seen is drawn discovered, active, unreviewed
    // and confirmed, summarized by its id; an access to a resource the
    // cache holds on no known channel is not drawn.
    let mut world = world();
    world.nodes.set_resource(resource(5), Some(channel(5)));
    world
        .store
        .apply_access(&AccessContribution {
            access: access(1),
            agent: agent(1),
            resource: resource(5),
            op: AccessKind::Write,
            at: ts(20),
        })
        .await
        .unwrap();
    world
        .store
        .apply(&plain(1, 1, 2, Route::Channel(channel(5)), 21, 4))
        .await
        .unwrap();
    let drawn = world
        .store
        .channel_topology(all(), Weighting::Transmissions, &TopologyFilter::default())
        .await
        .unwrap()
        .value;
    assert!(drawn.accesses().is_empty());
    let nodes: Vec<_> = drawn
        .nodes()
        .iter()
        .filter_map(|node| match node {
            GraphNode::Channel(node) => Some(node.clone()),
            GraphNode::Agent(_) => None,
        })
        .collect();
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id, channel(5));
    assert_eq!(nodes[0].origin_kind, CanonicalOriginKind::Discovered);
    assert_eq!(nodes[0].detection_kind, DetectionKind::Active);
    assert_eq!(nodes[0].policy_kind, PolicyKind::Unreviewed);
    assert_eq!(nodes[0].confirmation, Confirmation::Confirmed);
    assert!(
        nodes[0]
            .locator_summary
            .as_str()
            .contains(&channel(5).ulid_text())
    );
}
