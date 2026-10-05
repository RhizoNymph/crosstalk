//! Graph nodes, the channel-centred graph, resource use and harness claims.

use std::num::NonZeroU64;

use crate::aggregates::access::{
    AgentAccesses, BipartiteGraph, BipartiteParts, InvalidBipartite, InvalidResourceUse,
    ResourceUse, WeightedAccess,
};
use crate::aggregates::edge::{
    EdgeStats, InvalidGraph, TopologyGraph, TopologyGraphParts, WeightedEdge, Weighting,
};
use crate::aggregates::node::{
    AgentNode, CanonicalOriginKind, CanonicalStateKind, ChannelNode, GraphNode, InvalidNodes,
    NodeId,
};
use crate::aggregates::topic::TopicModelVersion;
use crate::derived::flow::access::AccessKind;
use crate::derived::flow::channel::confirmation::Confirmation;
use crate::derived::flow::channel::detection::{
    DeclaredDetection, DetectionKind, TrafficDetection,
};
use crate::derived::flow::channel::policy::{PolicyAuthor, PolicyKind};
use crate::derived::flow::channel::{
    ChannelOrigin, Declaration, DeclaredHistory, Seed, Supersession,
};
use crate::derived::flow::resource::{Host, Locator, Resource, ResourcePattern};
use crate::derived::flow::transmission::{DirectCarrier, Route};
use crate::ids::{AgentId, MergeId};
use crate::observed::agent::{
    ActiveAgentState, AgentLabel, AgentState, ClaimSet, DuplicateClaim, MergedInto, SeenClaim,
};
use crate::observed::client::{HarnessClaim, HarnessFamily};
use crate::support::{NonBlank, Share, TimeWindow, Timestamp};
use crate::tests::fixtures::{agent, agent_node, at, channel, resource, transmission};

fn n(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).expect("fixture values are non-zero")
}

fn share(value: f64) -> Share {
    Share::new(value).expect("in range")
}

fn edge(from: u128, to: u128, route: Route, transmissions: u64, value: f64) -> WeightedEdge {
    WeightedEdge {
        from: agent(from),
        to: agent(to),
        route,
        stats: EdgeStats {
            transmissions: n(transmissions),
            matched_bytes: n(transmissions * 10),
        },
        share: share(value),
    }
}

fn child_of(id: u128, parent: u128, transmissions_in: u64, transmissions_out: u64) -> GraphNode {
    GraphNode::Agent(AgentNode {
        parent: Some(agent(parent)),
        ..agent_struct(id, transmissions_in, transmissions_out)
    })
}

fn agent_struct(id: u128, transmissions_in: u64, transmissions_out: u64) -> AgentNode {
    match agent_node(id, transmissions_in, transmissions_out) {
        GraphNode::Agent(node) => node,
        GraphNode::Channel(_) => unreachable!("agent_node builds agent nodes"),
    }
}

fn channel_node(id: u128) -> GraphNode {
    GraphNode::Channel(ChannelNode {
        id: channel(id),
        label: None,
        origin_kind: CanonicalOriginKind::Discovered,
        detection_kind: DetectionKind::Active,
        confirmation: Confirmation::Confirmed,
        policy_kind: PolicyKind::Unreviewed,
        locator_summary: NonBlank::new("https://wiki.example/a").expect("not blank"),
    })
}

fn window() -> TimeWindow {
    TimeWindow::new(at(0), at(100)).expect("start < end")
}

/// A graph checked by its constructor: `Err` names the rule it breaks.
fn graph(nodes: Vec<GraphNode>, edges: Vec<WeightedEdge>) -> Result<TopologyGraph, InvalidGraph> {
    TopologyGraph::new(TopologyGraphParts {
        window: window(),
        weighting: Weighting::Transmissions,
        topic_version: TopicModelVersion(1),
        nodes,
        edges,
    })
}

/// The constructor's refusal of a graph whose nodes break `error`'s rule.
fn nodes_err(error: InvalidNodes) -> Result<(), InvalidGraph> {
    Err(InvalidGraph::Nodes(error))
}

// The topology graph's checked constructor.

#[test]
fn a_graph_is_built_only_through_its_checks() {
    let nodes = || vec![agent_node(1, 1, 3), agent_node(2, 3, 1)];
    let valid = graph(
        nodes(),
        vec![
            edge(1, 2, Route::Unobserved, 3, 0.75),
            edge(2, 1, Route::Unobserved, 1, 0.25),
        ],
    )
    .expect("valid graph");
    assert_eq!(valid.window(), window());
    assert_eq!(valid.weighting(), Weighting::Transmissions);
    assert_eq!(valid.topic_version(), TopicModelVersion(1));
    assert_eq!((valid.nodes().len(), valid.edges().len()), (2, 2));
    let parts = valid.clone().into_parts();
    assert_eq!(TopologyGraph::new(parts), Ok(valid));
}

#[test]
fn a_graph_refuses_a_self_edge() {
    assert_eq!(
        graph(
            vec![agent_node(1, 0, 1), agent_node(2, 1, 0)],
            vec![
                edge(1, 2, Route::Unobserved, 1, 0.5),
                edge(2, 2, Route::Unobserved, 1, 0.5),
            ],
        )
        .map(drop),
        Err(InvalidGraph::SelfEdge { index: 1 })
    );
}

#[test]
fn a_graph_refuses_an_edge_twice() {
    assert_eq!(
        graph(
            vec![agent_node(1, 0, 2), agent_node(2, 2, 0)],
            vec![
                edge(1, 2, Route::Unobserved, 1, 0.5),
                edge(1, 2, Route::Unobserved, 1, 0.5),
            ],
        )
        .map(drop),
        Err(InvalidGraph::DuplicateEdge { index: 1 })
    );
}

#[test]
fn a_graph_refuses_shares_that_are_not_their_stat_over_the_total() {
    assert_eq!(
        graph(
            vec![agent_node(1, 1, 3), agent_node(2, 3, 1)],
            vec![
                edge(1, 2, Route::Unobserved, 3, 0.5),
                edge(2, 1, Route::Unobserved, 1, 0.5),
            ],
        )
        .map(drop),
        Err(InvalidGraph::Share { index: 0 })
    );
}

// Topology graph nodes.

#[test]
fn graph_nodes_cover_endpoints_and_their_ancestors() {
    // 1 → 2 and 3 → 2; 1's parent is 4, whose parent is 5.
    let edges = vec![
        edge(1, 2, Route::Unobserved, 2, 2.0 / 3.0),
        edge(3, 2, Route::Unobserved, 1, 1.0 / 3.0),
    ];
    let nodes = vec![
        child_of(1, 4, 0, 2),
        agent_node(2, 3, 0),
        agent_node(3, 0, 1),
        child_of(4, 5, 0, 0),
        agent_node(5, 0, 0),
    ];
    assert_eq!(graph(nodes, edges).map(drop), Ok(()));
}

#[test]
fn agent_nodes_carry_a_typed_label() {
    let planner = AgentLabel::new("planner").expect("valid label");
    let labelled = GraphNode::Agent(AgentNode {
        label: Some(planner.clone()),
        ..agent_struct(1, 0, 1)
    });
    let edges = vec![edge(1, 2, Route::Unobserved, 1, 1.0)];
    let graph = graph(vec![labelled, agent_node(2, 1, 0)], edges).expect("valid graph");
    match &graph.nodes()[0] {
        GraphNode::Agent(node) => assert_eq!(node.label.as_ref(), Some(&planner)),
        GraphNode::Channel(_) => unreachable!("the first node is an agent"),
    }
}

#[test]
fn an_empty_graph_has_no_nodes() {
    assert_eq!(graph(Vec::new(), Vec::new()).map(drop), Ok(()));
    assert_eq!(
        graph(vec![agent_node(1, 0, 0)], Vec::new()).map(drop),
        nodes_err(InvalidNodes::Unexpected(NodeId::Agent(agent(1))))
    );
}

#[test]
fn every_endpoint_needs_a_node() {
    let edges = vec![edge(1, 2, Route::Unobserved, 1, 1.0)];
    assert_eq!(
        graph(vec![agent_node(1, 0, 1)], edges).map(drop),
        nodes_err(InvalidNodes::Missing(NodeId::Agent(agent(2))))
    );
}

#[test]
fn no_node_appears_twice() {
    let edges = vec![edge(1, 2, Route::Unobserved, 1, 1.0)];
    let nodes = vec![
        agent_node(1, 0, 1),
        agent_node(2, 1, 0),
        agent_node(1, 0, 1),
    ];
    assert_eq!(
        graph(nodes, edges).map(drop),
        nodes_err(InvalidNodes::Duplicate(NodeId::Agent(agent(1))))
    );
}

#[test]
fn parents_have_nodes_and_are_never_the_agent() {
    let edges = || vec![edge(1, 2, Route::Unobserved, 1, 1.0)];
    assert_eq!(
        graph(vec![child_of(1, 1, 0, 1), agent_node(2, 1, 0)], edges()).map(drop),
        nodes_err(InvalidNodes::SelfParent(agent(1)))
    );
    assert_eq!(
        graph(vec![child_of(1, 4, 0, 1), agent_node(2, 1, 0)], edges()).map(drop),
        nodes_err(InvalidNodes::MissingParent {
            agent: agent(1),
            parent: agent(4),
        })
    );
}

#[test]
fn nodes_beyond_endpoints_and_ancestors_are_rejected() {
    let edges = || vec![edge(1, 2, Route::Channel(channel(1)), 1, 1.0)];
    assert_eq!(
        graph(
            vec![
                agent_node(1, 0, 1),
                agent_node(2, 1, 0),
                agent_node(9, 0, 0)
            ],
            edges()
        )
        .map(drop),
        nodes_err(InvalidNodes::Unexpected(NodeId::Agent(agent(9))))
    );
    // The agent-centred graph has no channel nodes, even for its routes.
    assert_eq!(
        graph(
            vec![agent_node(1, 0, 1), agent_node(2, 1, 0), channel_node(1)],
            edges()
        )
        .map(drop),
        nodes_err(InvalidNodes::Unexpected(NodeId::Channel(channel(1))))
    );
}

#[test]
fn node_counts_agree_with_edges() {
    let edges = || {
        vec![
            edge(1, 2, Route::Unobserved, 3, 0.75),
            edge(2, 1, Route::Direct(DirectCarrier::UserTurn), 1, 0.25),
        ]
    };
    assert_eq!(
        graph(vec![agent_node(1, 1, 3), agent_node(2, 3, 1)], edges()).map(drop),
        Ok(())
    );
    assert_eq!(
        graph(vec![agent_node(1, 1, 3), agent_node(2, 4, 1)], edges()).map(drop),
        nodes_err(InvalidNodes::Counts(agent(2)))
    );
    // An ancestor that is no endpoint counts nothing.
    let edges = vec![edge(1, 2, Route::Unobserved, 1, 1.0)];
    assert_eq!(
        graph(
            vec![
                child_of(1, 3, 0, 1),
                agent_node(2, 1, 0),
                agent_node(3, 0, 1)
            ],
            edges
        )
        .map(drop),
        nodes_err(InvalidNodes::Counts(agent(3)))
    );
}

#[test]
fn canonical_state_kind_excludes_merged_agents() {
    let merged = AgentState::Merged(MergedInto {
        merge: MergeId::from_ulid(1),
        into: agent(2),
        prior: ActiveAgentState::Provisional { first_seen: at(1) },
        repointed_by: Vec::new(),
    });
    assert_eq!(CanonicalStateKind::of(&merged), None);
    assert_eq!(
        CanonicalStateKind::of(&AgentState::Registered { at: at(1) }),
        Some(CanonicalStateKind::Registered)
    );
    assert_eq!(
        CanonicalStateKind::of(&AgentState::Provisional { first_seen: at(1) }),
        Some(CanonicalStateKind::Provisional)
    );
    assert_eq!(
        CanonicalStateKind::of(&AgentState::Established { since: at(1) }),
        Some(CanonicalStateKind::Established)
    );
}

#[test]
fn canonical_origin_kind_excludes_superseded_channels() {
    let seed = Seed {
        resource: resource(1),
        first_transmission: transmission(1),
        opened_at: at(1),
    };
    let detection = TrafficDetection::Active {
        since: at(1),
        last_transmission: transmission(1),
    };
    let declaration = Declaration {
        pattern: ResourcePattern::Host(Host("wiki.example".into())),
        by: PolicyAuthor::Config,
        at: at(0),
    };
    let cases = [
        (
            ChannelOrigin::Declared {
                declaration: declaration.clone(),
                history: DeclaredHistory::BeforeTraffic(DeclaredDetection::AwaitingTraffic),
            },
            Some(CanonicalOriginKind::DeclaredBeforeTraffic),
        ),
        (
            ChannelOrigin::Declared {
                declaration,
                history: DeclaredHistory::Promoted {
                    from: seed,
                    detection: detection.clone(),
                },
            },
            Some(CanonicalOriginKind::Promoted),
        ),
        (
            ChannelOrigin::Discovered {
                seed,
                detection: detection.clone(),
            },
            Some(CanonicalOriginKind::Discovered),
        ),
        (
            ChannelOrigin::Superseded {
                seed,
                detection,
                supersession: Supersession {
                    by: channel(9),
                    at: at(3),
                },
            },
            None,
        ),
    ];
    for (origin, kind) in cases {
        assert_eq!(CanonicalOriginKind::of(&origin), kind);
    }
}

// The channel-centred graph.

fn weighted_access(
    agent_id: u128,
    channel_id: u128,
    op: AccessKind,
    count: u64,
    value: f64,
) -> WeightedAccess {
    WeightedAccess {
        agent: agent(agent_id),
        channel: channel(channel_id),
        op,
        accesses: n(count),
        share: share(value),
    }
}

fn parts(
    nodes: Vec<GraphNode>,
    accesses: Vec<WeightedAccess>,
    transmissions: Vec<WeightedEdge>,
) -> BipartiteParts {
    BipartiteParts {
        window: window(),
        weighting: Weighting::Transmissions,
        topic_version: TopicModelVersion(1),
        nodes,
        accesses,
        transmissions,
    }
}

/// Agent 1 wrote channel 1 three times, agent 2 read it once, and one
/// transmission from 1 to 2 was confirmed on it.
fn wiki_parts() -> BipartiteParts {
    parts(
        vec![agent_node(1, 0, 1), agent_node(2, 1, 0), channel_node(1)],
        vec![
            weighted_access(1, 1, AccessKind::Write, 3, 0.75),
            weighted_access(2, 1, AccessKind::Read, 1, 0.25),
        ],
        vec![edge(1, 2, Route::Channel(channel(1)), 1, 1.0)],
    )
}

#[test]
fn a_bipartite_graph_holds_accesses_transmissions_and_both_node_kinds() {
    let graph = BipartiteGraph::new(wiki_parts()).expect("valid graph");
    assert_eq!(graph.accesses().len(), 2);
    assert_eq!(graph.transmissions().len(), 1);
    assert_eq!(graph.nodes().len(), 3);
    assert_eq!(graph.topic_version(), TopicModelVersion(1));
    assert_eq!(graph.weighting(), Weighting::Transmissions);
    assert_eq!(graph.window(), window());
    assert_eq!(graph.into_parts(), wiki_parts());
}

#[test]
fn writes_nobody_read_are_drawn() {
    let unread = parts(
        vec![agent_node(1, 0, 0), channel_node(1)],
        vec![weighted_access(1, 1, AccessKind::Write, 5, 1.0)],
        Vec::new(),
    );
    let graph = BipartiteGraph::new(unread).expect("valid graph");
    assert_eq!(graph.accesses()[0].op, AccessKind::Write);
    assert!(graph.transmissions().is_empty());
}

#[test]
fn every_access_endpoint_and_route_channel_needs_a_node() {
    let mut missing_channel = wiki_parts();
    missing_channel.nodes.pop();
    assert_eq!(
        BipartiteGraph::new(missing_channel),
        Err(InvalidBipartite::Nodes(InvalidNodes::Missing(
            NodeId::Channel(channel(1))
        )))
    );
    let route_only = parts(
        vec![agent_node(1, 0, 1), agent_node(2, 1, 0)],
        Vec::new(),
        vec![edge(1, 2, Route::Channel(channel(2)), 1, 1.0)],
    );
    assert_eq!(
        BipartiteGraph::new(route_only),
        Err(InvalidBipartite::Nodes(InvalidNodes::Missing(
            NodeId::Channel(channel(2))
        )))
    );
    let reader_only = parts(
        vec![channel_node(1)],
        vec![weighted_access(2, 1, AccessKind::Read, 1, 1.0)],
        Vec::new(),
    );
    assert_eq!(
        BipartiteGraph::new(reader_only),
        Err(InvalidBipartite::Nodes(InvalidNodes::Missing(
            NodeId::Agent(agent(2))
        )))
    );
}

#[test]
fn bipartite_edges_are_distinct_and_never_self_edges() {
    let mut duplicate_access = wiki_parts();
    duplicate_access.accesses = vec![
        weighted_access(1, 1, AccessKind::Write, 1, 0.5),
        weighted_access(1, 1, AccessKind::Write, 1, 0.5),
    ];
    assert_eq!(
        BipartiteGraph::new(duplicate_access),
        Err(InvalidBipartite::DuplicateAccess { index: 1 })
    );
    let mut duplicate_edge = wiki_parts();
    duplicate_edge.transmissions = vec![
        edge(1, 2, Route::Channel(channel(1)), 1, 0.5),
        edge(1, 2, Route::Channel(channel(1)), 1, 0.5),
    ];
    assert_eq!(
        BipartiteGraph::new(duplicate_edge),
        Err(InvalidBipartite::DuplicateTransmission { index: 1 })
    );
    let mut self_edge = wiki_parts();
    self_edge.transmissions = vec![edge(1, 1, Route::Unobserved, 1, 1.0)];
    assert_eq!(
        BipartiteGraph::new(self_edge),
        Err(InvalidBipartite::SelfEdge { index: 0 })
    );
}

#[test]
fn access_and_transmission_shares_are_normalized_separately() {
    let mut wrong_access = wiki_parts();
    wrong_access.accesses[0].share = share(0.5);
    assert_eq!(
        BipartiteGraph::new(wrong_access),
        Err(InvalidBipartite::AccessShare { index: 0 })
    );
    // Transmission shares follow the weighting: by matched bytes, 30 of 40.
    let by_bytes = |first: f64| BipartiteParts {
        weighting: Weighting::MatchedBytes,
        nodes: vec![agent_node(1, 1, 3), agent_node(2, 3, 1), channel_node(1)],
        transmissions: vec![
            edge(1, 2, Route::Channel(channel(1)), 3, first),
            edge(2, 1, Route::Channel(channel(1)), 1, 1.0 - first),
        ],
        ..wiki_parts()
    };
    assert!(BipartiteGraph::new(by_bytes(0.75)).is_ok());
    assert_eq!(
        BipartiteGraph::new(by_bytes(0.5)),
        Err(InvalidBipartite::TransmissionShare { index: 0 })
    );
}

// Resource use.

fn wiki_page() -> Resource {
    Resource {
        id: resource(1),
        locator: Locator::Url {
            scheme: "https".into(),
            host: Host("wiki.example".into()),
            path: "/team/a".into(),
            query: None,
        },
        first_seen: at(1),
    }
}

fn uses(agent_id: u128, count: u64) -> AgentAccesses {
    AgentAccesses {
        agent: agent(agent_id),
        accesses: n(count),
    }
}

#[test]
fn resource_use_needs_a_writer_or_reader() {
    assert_eq!(
        ResourceUse::new(wiki_page(), Vec::new(), Vec::new()),
        Err(InvalidResourceUse::Unused)
    );
    let written = ResourceUse::new(wiki_page(), vec![uses(1, 2)], Vec::new())
        .expect("a written, unread resource is listed");
    assert!(written.readers().is_empty());
    assert_eq!(written.resource(), &wiki_page());
}

#[test]
fn resource_use_lists_each_agent_once() {
    assert_eq!(
        ResourceUse::new(wiki_page(), vec![uses(1, 1), uses(1, 2)], Vec::new()),
        Err(InvalidResourceUse::DuplicateWriter(agent(1)))
    );
    assert_eq!(
        ResourceUse::new(wiki_page(), Vec::new(), vec![uses(2, 1), uses(2, 1)]),
        Err(InvalidResourceUse::DuplicateReader(agent(2)))
    );
    // One agent may both write and read.
    assert!(ResourceUse::new(wiki_page(), vec![uses(1, 1)], vec![uses(1, 1)]).is_ok());
}

#[test]
fn resource_use_orders_by_accesses_then_agent() {
    let usage = ResourceUse::new(
        wiki_page(),
        vec![uses(3, 1), uses(2, 5), uses(1, 1)],
        vec![uses(4, 2), uses(5, 7)],
    )
    .expect("valid");
    let writers: Vec<AgentId> = usage.writers().iter().map(|w| w.agent).collect();
    let readers: Vec<AgentId> = usage.readers().iter().map(|r| r.agent).collect();
    assert_eq!(writers, vec![agent(2), agent(1), agent(3)]);
    assert_eq!(readers, vec![agent(5), agent(4)]);
}

// Harness claims.

fn claim(family: HarnessFamily, user_agent: &str) -> HarnessClaim {
    HarnessClaim {
        family,
        version: Some("1.0".into()),
        user_agent: user_agent.into(),
    }
}

fn claude() -> HarnessClaim {
    claim(HarnessFamily::ClaudeCode, "claude-cli/1.0")
}

fn codex() -> HarnessClaim {
    claim(HarnessFamily::Codex, "codex_cli_rs/1.0")
}

fn seen(claim: HarnessClaim, micros: u64) -> SeenClaim {
    SeenClaim {
        claim,
        last_seen: Timestamp::from_micros(micros),
    }
}

#[test]
fn observing_keeps_the_latest_time_in_any_order() {
    let mut forward = ClaimSet::default();
    forward.observe(claude(), at(1));
    forward.observe(claude(), at(5));
    forward.observe(codex(), at(3));
    let mut backward = ClaimSet::default();
    backward.observe(codex(), at(3));
    backward.observe(claude(), at(5));
    backward.observe(claude(), at(1));
    backward.observe(claude(), at(5));
    assert_eq!(forward, backward);
    assert_eq!(forward.entries().len(), 2);
    assert_eq!(forward.last_seen(&claude()), Some(at(5)));
    assert_eq!(forward.entries()[0].claim, claude());
}

#[test]
fn a_stored_claim_set_lists_each_claim_once() {
    assert_eq!(
        ClaimSet::from_entries(vec![seen(claude(), 1), seen(claude(), 2)]),
        Err(DuplicateClaim(claude()))
    );
    let set =
        ClaimSet::from_entries(vec![seen(codex(), 1), seen(claude(), 2)]).expect("distinct claims");
    assert_eq!(set.entries()[0].claim, claude());
    let same_version_other_agent = claim(HarnessFamily::ClaudeCode, "pi/0.9");
    assert!(
        ClaimSet::from_entries(vec![seen(claude(), 1), seen(same_version_other_agent, 1)]).is_ok()
    );
}

#[test]
fn claim_ties_are_ordered_deterministically() {
    let a = ClaimSet::from_entries(vec![seen(codex(), 4), seen(claude(), 4)]).expect("distinct");
    let b = ClaimSet::from_entries(vec![seen(claude(), 4), seen(codex(), 4)]).expect("distinct");
    assert_eq!(a, b);
    assert_eq!(a.entries()[0].claim, claude());
}

#[test]
fn a_canonical_agent_claims_the_union_of_its_aliases() {
    let own = ClaimSet::from_entries(vec![seen(claude(), 2)]).expect("distinct");
    let alias =
        ClaimSet::from_entries(vec![seen(claude(), 7), seen(codex(), 3)]).expect("distinct");
    let union = ClaimSet::union([&own, &alias]);
    assert_eq!(union, ClaimSet::union([&alias, &own]));
    assert_eq!(union.last_seen(&claude()), Some(at(7)));
    assert_eq!(union.last_seen(&codex()), Some(at(3)));
    assert_eq!(union.entries().len(), 2);
    // Unmerging is reading without the alias: the stored sets are untouched.
    assert_eq!(ClaimSet::union([&own]), own);
    assert!(ClaimSet::union([]).is_empty());
}
