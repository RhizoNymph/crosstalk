//! Graph reads and the linked-view filter: canonical nodes, shares summing
//! to one, counting by confirmation time, windows that add up, edges whose
//! transmissions are exactly what they count, and `TopologyFilter::admits`
//! as every view applies it.

use std::collections::{BTreeSet, HashSet};

use crosstalk_spec::aggregates::agents::AgentLookup;
use crosstalk_spec::aggregates::edge::{EdgeSelector, RouteKind, TopologyFilter, Weighting};
use crosstalk_spec::aggregates::filter::{
    FalseDetections, TopicVersionSelector, UnconfirmedChannels,
};
use crosstalk_spec::aggregates::node::GraphNode;
use crosstalk_spec::derived::flow::channel::confirmation::{Confirmation, Listing};
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::interfaces::l8_surface::summary::TopicUnder;
use crosstalk_spec::interfaces::l8_surface::{ConflictKind, QueryApi, QueryError};
use crosstalk_spec::observed::client::HarnessFamily;

use crate::harness::Harness;
use crate::scenario::named::{
    declared, hidden_channel, hijacked_wiki, impersonation, late_confirmation, merges, promotion,
    suspected, verdicts,
};
use crate::support::reads::{
    agent_detail, channel_row, counted, edge_rows, graph, pinned, row, total,
};
use crate::support::windows::{bucket_of, halves};
use crate::support::{World, collect, first};

const WEIGHTINGS: [Weighting; 2] = [Weighting::Transmissions, Weighting::MatchedBytes];

/// Nodes are canonical agents (INV-680) covering every edge's ends
/// (INV-681), no edge joins an agent to itself (INV-758), one edge per
/// sender, reader and route, and shares sum to one per weighting.
pub async fn topology_is_canonical_with_shares_summing_to_one<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    for window in [w.day(), w.extent] {
        for weighting in WEIGHTINGS {
            let g = graph(
                &w.backend,
                &w.lead,
                window,
                weighting,
                &TopologyFilter::default(),
            )
            .await;
            assert_eq!(g.window(), window);
            assert_eq!(g.weighting(), weighting);
            assert!(!g.edges().is_empty(), "the world has traffic in {window:?}");
            let shares: f64 = g.edges().iter().map(|e| e.share.get()).sum();
            assert!((shares - 1.0).abs() < 1e-9, "shares sum to one: {shares}");
            assert!(g.edges().iter().all(|e| e.from != e.to), "no self-edges");
            let mut keys = HashSet::new();
            for e in g.edges() {
                assert!(
                    keys.insert((e.from, e.to, e.route.clone())),
                    "one edge per key"
                );
            }
            for node in g.nodes() {
                if let GraphNode::Agent(agent) = node {
                    let detail = agent_detail(&w.backend, &w.lead, agent.id, window).await;
                    assert_eq!(
                        detail.cluster.lookup(),
                        AgentLookup::Canonical,
                        "{:?}",
                        agent.id
                    );
                }
            }
        }
    }
}

/// A transmission counts in the bucket it was confirmed in, not the one it
/// opened in (`Confirmed::at`; INV-589).
pub async fn edges_count_confirmations_by_their_time<H: Harness>(h: &H) {
    let w = World::of(h, late_confirmation::scenario()).await;
    let late = w.id(late_confirmation::LATE);
    let summary = row(&w.backend, &w.lead, late).await.expect("the late row");
    let delivery = *summary.state.delivery().expect("confirmed");
    let opened = bucket_of(w.bucket, summary.opened_at);
    let confirmed = bucket_of(w.bucket, delivery.confirmed_at);
    assert!(opened.end() <= confirmed.start(), "a later bucket");
    let edge = EdgeSelector::new(delivery.from, summary.to, summary.route.clone()).expect("edge");
    let listed = async |window| {
        collect(100, async |p| {
            w.backend
                .edge_transmissions(&w.lead, &edge, window, &TopologyFilter::default(), &p)
                .await
                .map(|page| page.value.page)
        })
        .await
        .into_iter()
        .map(|r| r.transmission)
        .collect::<Vec<_>>()
    };
    assert!(!listed(opened).await.contains(&late), "not where it opened");
    assert!(
        listed(confirmed).await.contains(&late),
        "where it was confirmed"
    );
    let g = graph(
        &w.backend,
        &w.lead,
        confirmed,
        Weighting::Transmissions,
        &TopologyFilter::default(),
    )
    .await;
    assert!(
        g.edges()
            .iter()
            .any(|e| e.from == delivery.from && e.to == summary.to && e.route == summary.route),
        "its edge is in the confirming bucket's graph"
    );
}

/// Counts over two adjacent aligned windows add up to the count over their
/// union, edge by edge (INV-354).
pub async fn windows_add_up<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    let (left, right) = halves(w.bucket, w.extent).expect("the extent splits");
    let f = TopologyFilter::default();
    for weighting in WEIGHTINGS {
        let whole = graph(&w.backend, &w.lead, w.extent, weighting, &f).await;
        let a = graph(&w.backend, &w.lead, left, weighting, &f).await;
        let b = graph(&w.backend, &w.lead, right, weighting, &f).await;
        assert_eq!(
            total(&a, weighting) + total(&b, weighting),
            total(&whole, weighting)
        );
        for e in whole.edges() {
            let part = |g: &crosstalk_spec::aggregates::edge::TopologyGraph| {
                g.edges()
                    .iter()
                    .find(|x| x.from == e.from && x.to == e.to && x.route == e.route)
                    .map_or(0, |x| x.stats.transmissions.get())
            };
            assert_eq!(part(&a) + part(&b), e.stats.transmissions.get(), "{e:?}");
        }
    }
}

/// An edge's transmissions page has exactly as many items as the edge's
/// count, the same matched bytes, newest confirmation first, every one
/// confirmed in the window and carried by the edge's sender, reader and
/// route (INV-409).
pub async fn edge_transmissions_are_exactly_the_edge<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    let window = w.day();
    let f = TopologyFilter::default();
    let g = graph(&w.backend, &w.lead, window, Weighting::Transmissions, &f).await;
    let mut checked = 0;
    for edge in g.edges() {
        let rows = edge_rows(&w.backend, &w.lead, edge, window, &f).await;
        assert_eq!(
            rows.len() as u64,
            edge.stats.transmissions.get(),
            "{edge:?}"
        );
        let bytes: u64 = rows.iter().map(|r| r.matched_bytes.get()).sum();
        assert_eq!(bytes, edge.stats.matched_bytes.get(), "{edge:?}");
        assert!(
            rows.windows(2)
                .all(|p| p[0].confirmed_at >= p[1].confirmed_at)
        );
        assert!(rows.iter().all(|r| window.contains(r.confirmed_at)));
        let ids: Vec<_> = rows.iter().take(5).map(|r| r.transmission).collect();
        for summary in crate::support::reads::rows(
            &w.backend,
            &w.lead,
            &ids,
            TopicVersionSelector::Pinned(g.topic_version()),
        )
        .await
        {
            assert_eq!(summary.state.delivery().map(|d| d.from), Some(edge.from));
            assert_eq!((summary.to, &summary.route), (edge.to, &edge.route));
        }
        checked += 1;
    }
    assert!(checked > 0);
}

/// The channel-centred view carries exactly `topology`'s transmission
/// edges (INV-676), normalizes access and transmission shares separately
/// (INV-675), and draws only channels listed as channels (INV-757).
pub async fn the_channel_centred_view_shares_the_topology_edges<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    let f = TopologyFilter::default();
    let bipartite = w
        .backend
        .channel_topology(&w.lead, w.extent, Weighting::Transmissions, &f)
        .await
        .expect("bipartite")
        .value;
    let topology = graph(&w.backend, &w.lead, w.extent, Weighting::Transmissions, &f).await;
    assert_eq!(bipartite.transmissions(), topology.edges());
    let access: f64 = bipartite.accesses().iter().map(|a| a.share.get()).sum();
    assert!((access - 1.0).abs() < 1e-9, "access shares: {access}");
    let tx: f64 = bipartite
        .transmissions()
        .iter()
        .map(|e| e.share.get())
        .sum();
    assert!((tx - 1.0).abs() < 1e-9, "transmission shares: {tx}");
    for node in bipartite.nodes() {
        if let GraphNode::Channel(channel) = node {
            let listed = channel_row(&w.backend, &w.lead, channel.id, None).await;
            assert_eq!(
                listed.listing(),
                Some(Listing::Channel(channel.confirmation)),
                "{:?} is drawn as it is listed",
                channel.id
            );
        }
    }
    let wiki = w.id(hijacked_wiki::WIKI);
    let ops: HashSet<_> = bipartite
        .accesses()
        .iter()
        .filter(|a| a.channel == wiki)
        .map(|a| format!("{:?}", a.op))
        .collect();
    assert_eq!(ops.len(), 2, "the wiki is written and read");
}

/// An agent filter matches the sender or the reader after alias
/// resolution: an alias filters as its canonical agent, and no node is an
/// alias.
pub async fn agent_filter_matches_after_alias_resolution<H: Harness>(h: &H) {
    let w = World::of(h, merges::scenario()).await;
    let (canonical, alias) = (w.id(merges::CANONICAL), w.id(merges::ALIAS));
    let by = |agent| TopologyFilter {
        agents: vec![agent],
        ..TopologyFilter::default()
    };
    let g = graph(
        &w.backend,
        &w.lead,
        w.extent,
        Weighting::Transmissions,
        &by(canonical),
    )
    .await;
    assert!(!g.edges().is_empty());
    assert!(
        g.edges()
            .iter()
            .all(|e| e.from == canonical || e.to == canonical)
    );
    let via_alias = graph(
        &w.backend,
        &w.lead,
        w.extent,
        Weighting::Transmissions,
        &by(alias),
    )
    .await;
    assert_eq!(g.edges(), via_alias.edges());
    let all = graph(
        &w.backend,
        &w.lead,
        w.extent,
        Weighting::Transmissions,
        &TopologyFilter::default(),
    )
    .await;
    assert!(
        all.nodes()
            .iter()
            .all(|n| !matches!(n, GraphNode::Agent(a) if a.id == alias))
    );
}

/// A channel filter resolves supersession on both sides (INV-679): the
/// superseded channel filters as the channel that superseded it, and
/// routes name the channel in force (INV-682).
pub async fn channel_filter_follows_supersession<H: Harness>(h: &H) {
    let w = World::of(h, promotion::scenario()).await;
    let (notes, old) = (w.id(promotion::NOTES), w.id(promotion::OLD));
    let by = |channel| TopologyFilter {
        channels: vec![channel],
        ..TopologyFilter::default()
    };
    let new = counted(&w.backend, &w.lead, w.extent, &by(notes)).await;
    let via_old = counted(&w.backend, &w.lead, w.extent, &by(old)).await;
    assert!(!new.is_empty());
    assert_eq!(new, via_old);
    assert!(new.iter().all(|t| t.route == Route::Channel(notes)));
    let standup = w.id(promotion::ON_STANDUP);
    assert!(
        new.iter().any(|t| t.id == standup),
        "the old channel's traffic counts on the new"
    );
}

/// Route-kind and topic filters each keep exactly the transmissions they
/// admit, and together their intersection; outliers never match a topic
/// (INV-345); a topic of another version is refused (INV-637).
pub async fn route_and_topic_filters_and_their_conjunction<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    let all = counted(&w.backend, &w.lead, w.extent, &TopologyFilter::default()).await;
    let version = graph(
        &w.backend,
        &w.lead,
        w.extent,
        Weighting::Transmissions,
        &TopologyFilter::default(),
    )
    .await
    .topic_version();
    let routes = vec![RouteKind::Delegation, RouteKind::Direct];
    let by_route = counted(
        &w.backend,
        &w.lead,
        w.extent,
        &TopologyFilter {
            route_kinds: routes.clone(),
            ..pinned(version)
        },
    )
    .await;
    let expected: Vec<_> = all
        .iter()
        .filter(|t| routes.contains(&RouteKind::from(&t.route)))
        .cloned()
        .collect();
    assert!(!expected.is_empty());
    assert_eq!(by_route, expected);
    let topic = all
        .iter()
        .find_map(|t| match t.state.topic() {
            Some(TopicUnder::Topic(topic)) => Some(topic),
            _ => None,
        })
        .expect("a classified transmission");
    let topic_filter = TopologyFilter {
        topics: vec![topic],
        ..pinned(version)
    };
    let by_topic = counted(&w.backend, &w.lead, w.extent, &topic_filter).await;
    let expected: Vec<_> = all
        .iter()
        .filter(|t| t.state.topic() == Some(TopicUnder::Topic(topic)))
        .cloned()
        .collect();
    assert_eq!(by_topic, expected);
    let both = counted(
        &w.backend,
        &w.lead,
        w.extent,
        &TopologyFilter {
            route_kinds: routes.clone(),
            ..topic_filter.clone()
        },
    )
    .await;
    let expected: Vec<_> = by_topic
        .iter()
        .filter(|t| routes.contains(&RouteKind::from(&t.route)))
        .cloned()
        .collect();
    assert_eq!(both, expected);
    let history = w.backend.topic_versions(&w.lead).await.expect("history");
    let other = history
        .versions()
        .iter()
        .map(|v| v.version())
        .find(|v| *v != version && history.get(*v).is_some_and(|i| i.retention().is_retained()))
        .expect("another retained version");
    let foreign = TopologyFilter {
        topics: vec![topic],
        ..pinned(other)
    };
    let topics_of_other: Vec<_> = collect(100, async |p| {
        w.backend
            .topics(&w.lead, TopicVersionSelector::Pinned(other), &p)
            .await
            .map(|t| t.page)
    })
    .await
    .into_iter()
    .map(|t| t.id)
    .collect();
    if !topics_of_other.contains(&topic) {
        assert_eq!(
            w.backend
                .topology(&w.lead, w.extent, Weighting::Transmissions, &foreign)
                .await
                .err(),
            Some(QueryError::Conflict(ConflictKind::TopicsNotInVersion {
                version: other,
                topics: vec![topic],
            }))
        );
    }
}

/// Excluding false detections subtracts exactly the transmissions judged
/// false (INV-534); a withdrawn verdict leaves its transmission counted.
pub async fn false_detections_are_subtracted<H: Harness>(h: &H) {
    let w = World::of(h, verdicts::scenario()).await;
    let all = counted(&w.backend, &w.lead, w.extent, &TopologyFilter::default()).await;
    let kept = counted(
        &w.backend,
        &w.lead,
        w.extent,
        &TopologyFilter {
            false_detections: FalseDetections::Exclude,
            ..TopologyFilter::default()
        },
    )
    .await;
    let expected: Vec<_> = all
        .iter()
        .filter(|t| t.state.verdict() != Some(Verdict::FalseDetection))
        .cloned()
        .collect();
    assert_eq!(kept, expected);
    let (judged, withdrawn) = (w.id(verdicts::JUDGED_FALSE), w.id(verdicts::WITHDRAWN));
    assert!(all.iter().any(|t| t.id == judged));
    assert!(kept.iter().all(|t| t.id != judged));
    assert!(
        kept.iter()
            .any(|t| t.id == withdrawn && t.state.verdict().is_none())
    );
}

/// The channel-centred view draws listed channels only: an unconfirmed one
/// marked, a hidden one and a declaration without traffic not at all
/// (INV-757); confirmed only drops the unconfirmed channel and its access
/// edges (INV-756) and changes no transmission edge (INV-759).
pub async fn channel_graph_draws_only_listed_channels<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    let draw = async |unconfirmed| {
        w.backend
            .channel_topology(
                &w.lead,
                w.extent,
                Weighting::Transmissions,
                &TopologyFilter {
                    unconfirmed_channels: unconfirmed,
                    ..TopologyFilter::default()
                },
            )
            .await
            .expect("bipartite")
            .value
    };
    let all = draw(UnconfirmedChannels::Include).await;
    let nodes: Vec<_> = all
        .nodes()
        .iter()
        .filter_map(|n| match n {
            GraphNode::Channel(c) => Some((c.id, c.confirmation)),
            GraphNode::Agent(_) => None,
        })
        .collect();
    let s3 = w.id(suspected::S3);
    assert!(nodes.contains(&(s3, Confirmation::Unconfirmed)), "marked");
    for absent in [w.id(hidden_channel::SELF_NOTES), w.id(declared::UNUSED)] {
        assert!(
            nodes.iter().all(|(n, _)| *n != absent),
            "{absent:?} is not drawn"
        );
    }
    let confirmed = draw(UnconfirmedChannels::Exclude).await;
    let kept: Vec<_> = confirmed
        .nodes()
        .iter()
        .filter_map(|n| match n {
            GraphNode::Channel(c) => Some((c.id, c.confirmation)),
            GraphNode::Agent(_) => None,
        })
        .collect();
    let expected: Vec<_> = nodes
        .iter()
        .copied()
        .filter(|(_, c)| *c == Confirmation::Confirmed)
        .collect();
    assert_eq!(kept, expected);
    assert!(confirmed.accesses().iter().all(|a| a.channel != s3));
    assert_eq!(confirmed.transmissions(), all.transmissions());
}

/// A transmission between two ids of one merged agent counts nowhere
/// (INV-758): not in any edge, and an edge between the two ids lists
/// nothing.
pub async fn no_view_counts_a_transmission_within_one_agent<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    let within: BTreeSet<_> = [w.id(merges::SELF_EDGE), w.id(hidden_channel::BETWEEN)].into();
    let all = counted(&w.backend, &w.lead, w.extent, &TopologyFilter::default()).await;
    assert!(all.iter().all(|t| !within.contains(&t.id)));
    let (alias, canonical) = (w.id(merges::ALIAS), w.id(merges::CANONICAL));
    for route in [
        Route::Unobserved,
        Route::Channel(w.id(hidden_channel::SELF_NOTES)),
    ] {
        let edge = EdgeSelector::new(alias, canonical, route).expect("two ids");
        let page = w
            .backend
            .edge_transmissions(
                &w.lead,
                &edge,
                w.extent,
                &TopologyFilter::default(),
                &first(10),
            )
            .await
            .expect("edge rows");
        assert!(page.value.page.items().is_empty());
    }
}

/// Agent nodes carry the spec's labels and every claim seen, as claims:
/// the impersonating agent shows both its own family and the one it
/// claimed.
pub async fn agent_nodes_carry_labels_and_claims<H: Harness>(h: &H) {
    let w = World::of(h, impersonation::scenario()).await;
    let id = w.id(impersonation::IMPERSONATOR);
    let g = graph(
        &w.backend,
        &w.lead,
        w.extent,
        Weighting::Transmissions,
        &TopologyFilter::default(),
    )
    .await;
    let node = g
        .nodes()
        .iter()
        .find_map(|n| match n {
            GraphNode::Agent(a) if a.id == id => Some(a),
            _ => None,
        })
        .expect("the impersonator is a node");
    assert_eq!(
        node.label.as_ref().map(|l| l.as_str()),
        Some(impersonation::LABEL)
    );
    let families: HashSet<_> = node
        .claims
        .entries()
        .iter()
        .map(|c| c.claim.family.clone())
        .collect();
    assert!(families.contains(&HarnessFamily::Pi) && families.contains(&HarnessFamily::ClaudeCode));
}
