//! Properties of graphs, filters, applies and the drill-down.

use std::collections::BTreeSet;

use crosstalk_memory::model::build::{agent, channel, ts};
use crosstalk_spec::aggregates::edge::{
    EdgeSelector, EdgeTotals, RouteKind, TopologyFilter, TopologyGraph, Weighting,
};
use crosstalk_spec::aggregates::filter::UnconfirmedChannels;
use crosstalk_spec::aggregates::node::GraphNode;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::watermark::{PipelineFrontier, Watermark};
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l7_topology::EdgeStore;
use crosstalk_spec::paging::{EdgeTransmissionList, PageRequest, PageSize};
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;

use super::{
    Scene, aligned_window, canonical_channel, check, contribution, ensure, filter, fold, listed,
    load, read, scene, sent, window,
};
use crate::store::bucket_of;
use crate::tests::buckets;
use crate::tests::support::{World, config};

fn fail(what: impl std::fmt::Debug) -> TestCaseError {
    TestCaseError::fail(format!("{what:?}"))
}

/// topology.weighting.preserves-edges
#[test]
fn weighting_changes_only_shares() {
    check(
        "weighting_changes_only_shares",
        (scene(), aligned_window(), filter()),
        async |world: &mut World, (scene, window, filter): &(Scene, _, TopologyFilter)| {
            load(world, scene).await?;
            let by_count = read(world, *window, Weighting::Transmissions, filter).await?;
            let by_bytes = read(world, *window, Weighting::MatchedBytes, filter).await?;
            ensure(listed(&by_count) == listed(&by_bytes), || {
                format!("{:?} vs {:?}", listed(&by_count), listed(&by_bytes))
            })
        },
    );
}

/// topology.totals.match-graph
#[test]
fn totals_match_graph() {
    check(
        "totals_match_graph",
        (scene(), aligned_window(), filter()),
        async |world: &mut World, (scene, window, filter): &(Scene, _, TopologyFilter)| {
            load(world, scene).await?;
            let totals = world
                .store
                .totals(*window, filter)
                .await
                .map_err(fail)?
                .value;
            for weighting in [Weighting::Transmissions, Weighting::MatchedBytes] {
                let graph = read(world, *window, weighting, filter).await?;
                ensure(totals == EdgeTotals::of(&graph), || {
                    format!("totals {totals:?} vs graph {:?}", EdgeTotals::of(&graph))
                })?;
            }
            Ok(())
        },
    );
}

/// topology.graph.shares-sum-to-one
#[test]
fn nonempty_graph_shares_sum_to_one() {
    check(
        "nonempty_graph_shares_sum_to_one",
        (scene(), aligned_window(), filter(), any::<bool>()),
        async |world: &mut World,
               (scene, window, filter, bytes): &(Scene, _, TopologyFilter, bool)| {
            load(world, scene).await?;
            let weighting = if *bytes {
                Weighting::MatchedBytes
            } else {
                Weighting::Transmissions
            };
            let graph = read(world, *window, weighting, filter).await?;
            if graph.edges().is_empty() {
                return Ok(());
            }
            let sum: f64 = graph.edges().iter().map(|edge| edge.share.get()).sum();
            ensure((sum - 1.0).abs() <= 1e-9, || format!("shares sum to {sum}"))
        },
    );
}

/// topology.graph.no-self-edges
#[test]
fn graph_has_no_self_edges_after_merges() {
    check(
        "graph_has_no_self_edges_after_merges",
        (scene(), aligned_window()),
        async |world: &mut World, (scene, window): &(Scene, _)| {
            load(world, scene).await?;
            let graph = read(
                world,
                *window,
                Weighting::Transmissions,
                &TopologyFilter::default(),
            )
            .await?;
            ensure(
                graph.edges().iter().all(|edge| edge.from != edge.to),
                || format!("a self-edge in {:?}", graph.edges()),
            )
        },
    );
}

/// topology.filter.agent-membership
#[test]
fn agent_filter_keeps_edges_touching_listed_agents() {
    check(
        "agent_filter_keeps_edges_touching_listed_agents",
        (
            scene(),
            aligned_window(),
            prop::collection::vec(0u64..5, 1..3),
        ),
        async |world: &mut World, (scene, window, listed_agents): &(Scene, _, Vec<u64>)| {
            load(world, scene).await?;
            let filter = TopologyFilter {
                agents: listed_agents.iter().copied().map(agent).collect(),
                ..TopologyFilter::default()
            };
            let canonical: Vec<_> = filter
                .agents
                .iter()
                .map(|id| AgentDirectory::canonical(&world.directory, *id))
                .collect();
            let graph = read(world, *window, Weighting::Transmissions, &filter).await?;
            ensure(
                graph
                    .edges()
                    .iter()
                    .all(|edge| canonical.contains(&edge.from) || canonical.contains(&edge.to)),
                || format!("an edge touching none of {canonical:?}"),
            )?;
            ensure(
                listed(&graph) == fold(world, scene, *window, &filter),
                || "fold".to_owned(),
            )
        },
    );
}

/// topology.filter.channel-membership
#[test]
fn channel_filter_keeps_only_listed_channels() {
    check(
        "channel_filter_keeps_only_listed_channels",
        (
            scene(),
            aligned_window(),
            prop::collection::vec(0u64..4, 1..3),
        ),
        async |world: &mut World, (scene, window, channels): &(Scene, _, Vec<u64>)| {
            load(world, scene).await?;
            let filter = TopologyFilter {
                channels: channels.iter().copied().map(channel).collect(),
                ..TopologyFilter::default()
            };
            let canonical: Vec<_> = filter
                .channels
                .iter()
                .map(|id| canonical_channel(world, *id))
                .collect();
            let graph = read(world, *window, Weighting::Transmissions, &filter).await?;
            ensure(
                graph.edges().iter().all(|edge| {
                    matches!(edge.route, Route::Channel(c) if canonical.contains(&canonical_channel(world, c)))
                }),
                || format!("an edge on none of {canonical:?}: {:?}", graph.edges()),
            )
        },
    );
}

/// topology.filter.route-kind-membership
#[test]
fn route_kind_filter_keeps_only_listed_kinds() {
    check(
        "route_kind_filter_keeps_only_listed_kinds",
        (
            scene(),
            aligned_window(),
            prop::collection::vec(0u8..4, 1..3),
        ),
        async |world: &mut World, (scene, window, kinds): &(Scene, _, Vec<u8>)| {
            load(world, scene).await?;
            let kinds: Vec<RouteKind> = kinds
                .iter()
                .map(|k| match k {
                    0 => RouteKind::Channel,
                    1 => RouteKind::Delegation,
                    2 => RouteKind::Direct,
                    _ => RouteKind::Unobserved,
                })
                .collect();
            let filter = TopologyFilter {
                route_kinds: kinds.clone(),
                ..TopologyFilter::default()
            };
            let graph = read(world, *window, Weighting::Transmissions, &filter).await?;
            ensure(
                graph
                    .edges()
                    .iter()
                    .all(|edge| kinds.contains(&RouteKind::of(&edge.route))),
                || format!("an edge of another kind than {kinds:?}"),
            )
        },
    );
}

/// topology.filter.narrowing-never-grows
#[test]
fn narrowed_filter_graph_is_dominated() {
    check(
        "narrowed_filter_graph_is_dominated",
        (
            scene(),
            aligned_window(),
            filter(),
            0u64..5,
            0u64..4,
            0u8..4,
        ),
        async |world: &mut World,
               (scene, window, wide, a, c, k): &(Scene, _, TopologyFilter, u64, u64, u8)| {
            load(world, scene).await?;
            let base = read(world, *window, Weighting::Transmissions, wide).await?;
            let narrowings = [
                TopologyFilter {
                    agents: if wide.agents.len() > 1 {
                        wide.agents[1..].to_vec()
                    } else {
                        vec![agent(*a)]
                    },
                    ..wide.clone()
                },
                TopologyFilter {
                    channels: if wide.channels.len() > 1 {
                        wide.channels[1..].to_vec()
                    } else {
                        vec![channel(*c)]
                    },
                    ..wide.clone()
                },
                TopologyFilter {
                    route_kinds: if wide.route_kinds.len() > 1 {
                        wide.route_kinds[1..].to_vec()
                    } else {
                        vec![
                            [
                                RouteKind::Channel,
                                RouteKind::Delegation,
                                RouteKind::Direct,
                                RouteKind::Unobserved,
                            ][usize::from(*k)],
                        ]
                    },
                    ..wide.clone()
                },
            ];
            for narrow in &narrowings {
                // A non-empty list made non-empty again with another entry
                // is not a narrowing; only compare true narrowings.
                let narrows = |wide: usize, narrow: usize| wide == 0 || narrow < wide;
                if !(narrows(wide.agents.len(), narrow.agents.len())
                    && narrows(wide.channels.len(), narrow.channels.len())
                    && narrows(wide.route_kinds.len(), narrow.route_kinds.len()))
                {
                    continue;
                }
                let graph = read(world, *window, Weighting::Transmissions, narrow).await?;
                for edge in graph.edges() {
                    let wider = base.edges().iter().find(|one| {
                        one.from == edge.from && one.to == edge.to && one.route == edge.route
                    });
                    ensure(
                        wider.is_some_and(|wider| {
                            wider.stats.transmissions >= edge.stats.transmissions
                                && wider.stats.matched_bytes >= edge.stats.matched_bytes
                        }),
                        || format!("{edge:?} grew under {narrow:?}"),
                    )?;
                }
            }
            Ok(())
        },
    );
}

/// topology.graph.merge-sums-edges
#[test]
fn merge_rekeys_sums_and_drops_edges() {
    check(
        "merge_rekeys_sums_and_drops_edges",
        (scene(), aligned_window(), 0u64..5, 0u64..5),
        async |world: &mut World, (scene, window, from, into): &(Scene, _, u64, u64)| {
            load(world, scene).await?;
            let before = read(
                world,
                *window,
                Weighting::Transmissions,
                &TopologyFilter::default(),
            )
            .await?;
            let a = AgentDirectory::canonical(&world.directory, agent(*from));
            let b = AgentDirectory::canonical(&world.directory, agent(*into));
            if a == b || world.directory.merge(a, b).is_err() {
                return Ok(());
            }
            let mut expected: Vec<(_, _, Route, u64, u64)> = Vec::new();
            for edge in before.edges() {
                let rekey = |id| if id == a { b } else { id };
                let (from, to) = (rekey(edge.from), rekey(edge.to));
                if from == to {
                    continue;
                }
                match expected
                    .iter_mut()
                    .find(|one| one.0 == from && one.1 == to && one.2 == edge.route)
                {
                    Some(one) => {
                        one.3 += edge.stats.transmissions.get();
                        one.4 += edge.stats.matched_bytes.get();
                    }
                    None => expected.push((
                        from,
                        to,
                        edge.route.clone(),
                        edge.stats.transmissions.get(),
                        edge.stats.matched_bytes.get(),
                    )),
                }
            }
            expected.sort_by_key(|edge| (edge.0, edge.1, crate::store::fold_route_key(&edge.2)));
            let after = read(
                world,
                *window,
                Weighting::Transmissions,
                &TopologyFilter::default(),
            )
            .await?;
            ensure(listed(&after) == expected, || {
                format!("{:?} vs {expected:?}", listed(&after))
            })
        },
    );
}

/// topology.graph.window-additive
#[test]
fn graph_stats_add_over_adjacent_windows() {
    check(
        "graph_stats_add_over_adjacent_windows",
        (scene(), 0u64..10, 1u64..10, 1u64..10, filter()),
        async |world: &mut World, (scene, start, first, second, filter): &(Scene, u64, u64, u64, TopologyFilter)| {
            load(world, scene).await?;
            let a = window(start * 10, (start + first) * 10);
            let b = window((start + first) * 10, (start + first + second) * 10);
            let union = window(start * 10, (start + first + second) * 10);
            let (ga, gb, gu) = (
                read(world, a, Weighting::Transmissions, filter).await?,
                read(world, b, Weighting::Transmissions, filter).await?,
                read(world, union, Weighting::Transmissions, filter).await?,
            );
            for edge in gu.edges() {
                let part = |graph: &TopologyGraph| {
                    graph
                        .edges()
                        .iter()
                        .find(|one| one.from == edge.from && one.to == edge.to && one.route == edge.route)
                        .map_or((0, 0), |one| (one.stats.transmissions.get(), one.stats.matched_bytes.get()))
                };
                let (pa, pb) = (part(&ga), part(&gb));
                ensure(
                    (pa.0 + pb.0, pa.1 + pb.1)
                        == (edge.stats.transmissions.get(), edge.stats.matched_bytes.get()),
                    || format!("{edge:?} is not {pa:?} + {pb:?}"),
                )?;
            }
            ensure(ga.total() + gb.total() == gu.total(), || "totals".to_owned())
        },
    );
}

/// topology.apply.aligned-bucket
#[test]
fn apply_returns_aligned_bucket_containing_at() {
    check(
        "apply_returns_aligned_bucket_containing_at",
        prop::collection::vec(sent(), 1..10),
        async |world: &mut World, sent: &Vec<super::Sent>| {
            for (index, one) in sent.iter().enumerate() {
                let contribution = contribution(index, *one, TopicModelVersion(0), None);
                match world.store.apply(&contribution).await {
                    Ok(key) => {
                        let bucket = key.bucket();
                        ensure(
                            bucket.contains(contribution.at)
                                && Some(bucket)
                                    == bucket_of(config().bucket_width, contribution.at)
                                && key.from() == contribution.from
                                && key.to() == contribution.to
                                && key.route() == &contribution.route
                                && key.topic().version == TopicModelVersion(0),
                            || format!("{key:?} for {contribution:?}"),
                        )?;
                    }
                    Err(error) => ensure(one.from == one.to, || format!("{error:?}"))?,
                }
            }
            Ok(())
        },
    );
}

/// topology.apply.idempotent
#[test]
fn apply_is_idempotent_per_transmission_and_version() {
    check(
        "apply_is_idempotent_per_transmission_and_version",
        (
            prop::collection::vec(sent(), 1..10),
            prop::collection::vec(sent(), 1..10),
        ),
        async |world: &mut World, (first, again): &(Vec<super::Sent>, Vec<super::Sent>)| {
            let mut applied = Vec::new();
            for (index, one) in first.iter().enumerate() {
                if world
                    .store
                    .apply(&contribution(index, *one, TopicModelVersion(0), None))
                    .await
                    .is_ok()
                {
                    applied.push(index);
                }
            }
            let stored = buckets(world.store.pool()).await;
            // The applied ids again, with other facts: nothing changes.
            for (index, one) in again.iter().enumerate() {
                if applied.contains(&index) {
                    let _ = world
                        .store
                        .apply(&contribution(index, *one, TopicModelVersion(0), None))
                        .await;
                }
            }
            for (index, one) in first.iter().enumerate() {
                let _ = world
                    .store
                    .apply(&contribution(index, *one, TopicModelVersion(0), None))
                    .await;
            }
            let after = buckets(world.store.pool()).await;
            ensure(after == stored, || format!("{after:?} vs {stored:?}"))
        },
    );
}

/// topology.apply.increments-one-bucket
#[test]
fn first_apply_increments_only_its_bucket() {
    check(
        "first_apply_increments_only_its_bucket",
        (prop::collection::vec(sent(), 0..8), sent()),
        async |world: &mut World, (before, next): &(Vec<super::Sent>, super::Sent)| {
            for (index, one) in before.iter().enumerate() {
                let _ = world
                    .store
                    .apply(&contribution(index, *one, TopicModelVersion(0), None))
                    .await;
            }
            if next.from == next.to {
                return Ok(());
            }
            let stored = buckets(world.store.pool()).await;
            let one = contribution(100, *next, TopicModelVersion(0), None);
            let key = world.store.apply(&one).await.map_err(fail)?;
            let start = i64::try_from(key.bucket().start().as_micros()).map_err(fail)?;
            let after = buckets(world.store.pool()).await;
            // Bucket rows are (version, start, transmissions, bytes), ordered
            // by key: exactly one row of the key's start changed by +1 and
            // +bytes, or one new row appeared there.
            let total = |rows: &[(i64, i64, i64, i64)], at: i64| -> (i64, i64) {
                rows.iter()
                    .filter(|row| row.1 == at)
                    .fold((0, 0), |sum, row| (sum.0 + row.2, sum.1 + row.3))
            };
            let bytes = i64::try_from(next.bytes).map_err(fail)?;
            let (b, a) = (total(&stored, start), total(&after, start));
            ensure(a == (b.0 + 1, b.1 + bytes), || format!("{b:?} -> {a:?}"))?;
            let others = |rows: &[(i64, i64, i64, i64)]| -> Vec<(i64, i64, i64, i64)> {
                rows.iter().copied().filter(|row| row.1 != start).collect()
            };
            ensure(others(&stored) == others(&after), || {
                "another bucket changed".to_owned()
            })
        },
    );
}

/// topology.transmissions.match-graph
#[test]
fn edge_transmissions_match_fold() {
    check(
        "edge_transmissions_match_fold",
        (scene(), 0u64..100, 1u64..150, filter(), 1u16..4),
        async |world: &mut World, (scene, start, length, filter, size): &(Scene, u64, u64, TopologyFilter, u16)| {
            load(world, scene).await?;
            // Any window: the drill-down need not be aligned.
            let cut = window(*start, start + length);
            for (from, to, routed, count, bytes) in fold(world, scene, cut, filter) {
                let selector = EdgeSelector::new(from, to, routed).map_err(fail)?;
                let mut request: PageRequest<EdgeTransmissionList> = PageRequest {
                    size: PageSize::new(*size).map_err(fail)?,
                    after: None,
                };
                let mut rows = Vec::new();
                loop {
                    let page = world
                        .store
                        .transmissions(&selector, cut, filter, &request)
                        .await
                        .map_err(fail)?;
                    let (items, next) = page.value.page.into_parts();
                    rows.extend(items);
                    match next {
                        Some(cursor) => request.after = Some(cursor),
                        None => break,
                    }
                }
                let ids: BTreeSet<_> = rows.iter().map(|row| row.transmission).collect();
                let listed_bytes: u64 = rows.iter().map(|row| row.matched_bytes.get()).sum();
                ensure(
                    ids.len() == rows.len() && rows.len() as u64 == count && listed_bytes == bytes,
                    || format!("{} rows ({listed_bytes} bytes) for {count} ({bytes})", rows.len()),
                )?;
                let ordered = rows.windows(2).all(|pair| {
                    (pair[0].confirmed_at, pair[0].transmission) > (pair[1].confirmed_at, pair[1].transmission)
                });
                ensure(ordered, || "rows out of order".to_owned())?;
            }
            Ok(())
        },
    );
}

/// topology.bipartite.transmissions-match-graph
#[test]
fn channel_topology_transmissions_match_graph() {
    check(
        "channel_topology_transmissions_match_graph",
        (scene(), aligned_window(), filter(), any::<bool>()),
        async |world: &mut World,
               (scene, window, filter, bytes): &(Scene, _, TopologyFilter, bool)| {
            load(world, scene).await?;
            let weighting = if *bytes {
                Weighting::MatchedBytes
            } else {
                Weighting::Transmissions
            };
            let graph = read(world, *window, weighting, filter).await?;
            let bipartite = world
                .store
                .channel_topology(*window, weighting, filter)
                .await
                .map_err(fail)?
                .value;
            ensure(
                bipartite.transmissions() == graph.edges()
                    && bipartite.topic_version() == graph.topic_version(),
                || format!("{:?} vs {:?}", bipartite.transmissions(), graph.edges()),
            )
        },
    );
}

/// topology.graph.nodes-cover-endpoints
#[test]
fn graph_nodes_pass_check() {
    check(
        "graph_nodes_pass_check",
        (
            scene(),
            aligned_window(),
            prop::collection::vec((0u64..6, prop::option::of(0u64..6)), 0..4),
        ),
        async |world: &mut World,
               (scene, window, parents): &(Scene, _, Vec<(u64, Option<u64>)>)| {
            load(world, scene).await?;
            for (child, parent) in parents {
                world.nodes.set_parent(agent(*child), parent.map(agent));
            }
            let graph = read(
                world,
                *window,
                Weighting::Transmissions,
                &TopologyFilter::default(),
            )
            .await?;
            let rebuilt = TopologyGraph::new(graph.clone().into_parts());
            ensure(rebuilt.is_ok(), || format!("{rebuilt:?}"))?;
            let endpoints: BTreeSet<_> = graph
                .edges()
                .iter()
                .flat_map(|edge| [edge.from, edge.to])
                .collect();
            let nodes: BTreeSet<_> = graph
                .nodes()
                .iter()
                .filter_map(|node| match node {
                    GraphNode::Agent(one) => Some(one.id),
                    GraphNode::Channel(_) => None,
                })
                .collect();
            ensure(endpoints.is_subset(&nodes), || {
                "an endpoint without a node".to_owned()
            })
        },
    );
}

/// topology.graph.nodes-canonical
#[test]
fn graph_nodes_are_canonical() {
    check(
        "graph_nodes_are_canonical",
        (scene(), aligned_window(), filter()),
        async |world: &mut World, (scene, window, filter): &(Scene, _, TopologyFilter)| {
            load(world, scene).await?;
            let graph = read(world, *window, Weighting::Transmissions, filter).await?;
            let bipartite = world
                .store
                .channel_topology(*window, Weighting::Transmissions, filter)
                .await
                .map_err(fail)?
                .value;
            for node in graph.nodes().iter().chain(bipartite.nodes()) {
                let canonical = match node {
                    GraphNode::Agent(one) => {
                        AgentDirectory::canonical(&world.directory, one.id) == one.id
                    }
                    GraphNode::Channel(one) => canonical_channel(world, one.id) == one.id,
                };
                ensure(canonical, || format!("{node:?} is not canonical"))?;
            }
            Ok(())
        },
    );
}

/// topology.route.resolves-supersession
#[test]
fn superseded_routes_count_on_canonical() {
    check(
        "superseded_routes_count_on_canonical",
        (scene(), aligned_window()),
        async |world: &mut World, (scene, window): &(Scene, _)| {
            load(world, scene).await?;
            let graph = read(
                world,
                *window,
                Weighting::Transmissions,
                &TopologyFilter::default(),
            )
            .await?;
            for edge in graph.edges() {
                if let Route::Channel(id) = edge.route {
                    ensure(canonical_channel(world, id) == id, || {
                        format!("{edge:?} on a superseded channel")
                    })?;
                }
            }
            ensure(
                listed(&graph) == fold(world, scene, *window, &TopologyFilter::default()),
                || "the graph differs from the fold".to_owned(),
            )
        },
    );
}

/// topology.filter.channels-resolve-supersession
#[test]
fn filter_channels_resolve() {
    check(
        "filter_channels_resolve",
        (
            scene(),
            aligned_window(),
            prop::collection::vec(0u64..4, 1..3),
        ),
        async |world: &mut World, (scene, window, channels): &(Scene, _, Vec<u64>)| {
            load(world, scene).await?;
            let listed_ids: Vec<_> = channels.iter().copied().map(channel).collect();
            let as_listed = TopologyFilter {
                channels: listed_ids.clone(),
                ..TopologyFilter::default()
            };
            let as_canonical = TopologyFilter {
                channels: listed_ids
                    .iter()
                    .map(|id| canonical_channel(world, *id))
                    .collect(),
                ..TopologyFilter::default()
            };
            let a = read(world, *window, Weighting::Transmissions, &as_listed).await?;
            let b = read(world, *window, Weighting::Transmissions, &as_canonical).await?;
            ensure(a.edges() == b.edges(), || {
                format!("{:?} vs {:?}", a.edges(), b.edges())
            })
        },
    );
}

/// topology.filter.unconfirmed-changes-no-transmission-view
#[test]
fn unconfirmed_filter_keeps_transmission_views() {
    check(
        "unconfirmed_filter_keeps_transmission_views",
        (scene(), aligned_window(), filter()),
        async |world: &mut World, (scene, window, filter): &(Scene, _, TopologyFilter)| {
            load(world, scene).await?;
            let include = TopologyFilter {
                unconfirmed_channels: UnconfirmedChannels::Include,
                ..filter.clone()
            };
            let exclude = TopologyFilter {
                unconfirmed_channels: UnconfirmedChannels::Exclude,
                ..filter.clone()
            };
            let a = read(world, *window, Weighting::Transmissions, &include).await?;
            let b = read(world, *window, Weighting::Transmissions, &exclude).await?;
            ensure(a.edges() == b.edges(), || {
                "the confirmed-only switch changed a graph".to_owned()
            })
        },
    );
}

/// topology.watermark.exposed-is-settled
#[test]
fn advance_watermark_exposes_settled() {
    check(
        "advance_watermark_exposes_settled",
        prop::collection::vec((0u64..400, prop::option::of(0u64..400)), 1..8),
        async |world: &mut World, frontiers: &Vec<(u64, Option<u64>)>| {
            let mut exposed = Watermark(ts(0));
            for (ticked, pending) in frontiers {
                let frontier = PipelineFrontier {
                    ticked_through: ts(*ticked),
                    oldest_pending: pending.map(ts),
                };
                let settled = Watermark::settled(frontier, config().timing, config().bucket_width);
                let returned = world
                    .store
                    .advance_watermark(frontier)
                    .await
                    .map_err(fail)?;
                let expected = exposed.max(settled);
                ensure(returned == (settled > exposed).then_some(settled), || {
                    format!("returned {returned:?} for settled {settled:?} over {exposed:?}")
                })?;
                let read = world.store.watermark().await.map_err(fail)?;
                ensure(read == expected, || format!("{read:?} vs {expected:?}"))?;
                exposed = expected;
            }
            Ok(())
        },
    );
}

/// Resources placed on channels, each channel listed as (0 a confirmed
/// channel, 1 an unconfirmed one, 2 a declaration, 3 hidden).
type Holds = Vec<(u64, Option<u64>, u8)>;

/// topology.bipartite.listed-channels-only: the access edges are exactly
/// the access buckets in the window whose resource is on a channel listed
/// as a channel, resolved and summed per agent, channel and op.
#[test]
fn channel_topology_accesses_match_buckets() {
    use crosstalk_memory::model::build::resource;
    use crosstalk_spec::derived::flow::channel::confirmation::{Confirmation, Listing};
    check(
        "channel_topology_accesses_match_buckets",
        (
            scene(),
            aligned_window(),
            prop::collection::vec((0u64..4, prop::option::of(0u64..4), 0u8..4), 0..5),
        ),
        async |world: &mut World, (scene, window, holds): &(Scene, _, Holds)| {
            load(world, scene).await?;
            for (what, on, listing) in holds {
                world.nodes.set_resource(resource(*what), on.map(channel));
                if let Some(on) = on {
                    let listing = match listing {
                        0 => Listing::Channel(Confirmation::Confirmed),
                        1 => Listing::Channel(Confirmation::Unconfirmed),
                        2 => Listing::Declaration,
                        _ => Listing::Hidden,
                    };
                    let mut facts = crate::env::default_channel(channel(*on));
                    facts.listing = listing;
                    world.nodes.set_channel(channel(*on), facts);
                }
            }
            let mut expected: std::collections::BTreeMap<(_, _, bool), u64> =
                std::collections::BTreeMap::new();
            for (who, what, write, at) in &scene.accesses {
                if !window.contains(ts(*at)) {
                    continue;
                }
                let Some(on) = crosstalk_spec::interfaces::l7_topology::NodeFacts::channel_of(
                    &world.nodes,
                    resource(*what),
                ) else {
                    continue;
                };
                let on = canonical_channel(world, on);
                let listed_as_channel = matches!(
                    crosstalk_spec::interfaces::l7_topology::NodeFacts::channel(&world.nodes, on)
                        .map(|facts| facts.listing),
                    Some(Listing::Channel(_))
                );
                if listed_as_channel {
                    let who = AgentDirectory::canonical(&world.directory, agent(*who));
                    *expected.entry((who, on, *write)).or_default() += 1;
                }
            }
            let bipartite = world
                .store
                .channel_topology(
                    *window,
                    Weighting::Transmissions,
                    &TopologyFilter::default(),
                )
                .await
                .map_err(fail)?
                .value;
            let got: std::collections::BTreeMap<_, _> = bipartite
                .accesses()
                .iter()
                .map(|one| {
                    (
                        (
                            one.agent,
                            one.channel,
                            one.op == crosstalk_spec::derived::flow::access::AccessKind::Write,
                        ),
                        one.accesses.get(),
                    )
                })
                .collect();
            ensure(got == expected, || format!("{got:?} vs {expected:?}"))
        },
    );
}
