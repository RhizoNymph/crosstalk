//! The topology payload built from the hand-built spec graphs.

use super::*;
use crate::data::fixtures;

#[test]
fn agents_mode_maps_nodes_and_edges() {
    let graph = fixtures::topology_graph();
    let payload = TopologyPayload::agents(&graph);
    assert_eq!(payload.mode, ModeCode::Agents);
    assert_eq!(payload.nodes.len(), graph.value.nodes.len());
    assert_eq!(payload.edges.len(), graph.value.edges.len());

    let NodePayload::Agent(planner) = &payload.nodes[0] else {
        panic!("agent node expected");
    };
    assert_eq!(planner.name, "planner");
    assert_eq!(
        planner.volume,
        planner.transmissions_in + planner.transmissions_out
    );
    assert_eq!(planner.claims[0].harness, "Claude Code");

    let unlabelled = payload
        .nodes
        .iter()
        .find_map(|n| match n {
            NodePayload::Agent(a) if a.name.starts_with('…') => Some(a),
            _ => None,
        })
        .expect("an unlabelled agent shows its id tail");
    assert_eq!(unlabelled.name.chars().count(), 7);

    let share_sum: f64 = payload
        .edges
        .iter()
        .map(|e| match e {
            EdgePayload::Transmission(t) => t.share,
            EdgePayload::Access(a) => a.share,
        })
        .sum();
    assert!((share_sum - 1.0).abs() < 1e-9, "shares sum to {share_sum}");
    assert_eq!(payload.watermark, format_time(graph.watermark.at()));
}

#[test]
fn transmission_edges_carry_route_codes() {
    let graph = fixtures::topology_graph();
    let payload = TopologyPayload::agents(&graph);
    for (edge, source) in payload.edges.iter().zip(&graph.value.edges) {
        let EdgePayload::Transmission(edge) = edge else {
            panic!("agents mode has transmission edges only");
        };
        assert_eq!(edge.route, encode(&source.route));
        assert_eq!(
            crate::url::route::decode(&edge.route).as_ref(),
            Ok(&source.route)
        );
        assert_eq!(edge.from, source.from.to_ulid());
        assert_eq!(edge.transmissions, source.stats.transmissions.get());
    }
}

#[test]
fn channels_mode_adds_channel_nodes_and_access_edges() {
    let graph = fixtures::bipartite_graph();
    let names = fixtures::channel_names();
    let payload = TopologyPayload::channels(&graph, &names);
    assert_eq!(payload.mode, ModeCode::Channels);
    let channels: Vec<&ChannelNodePayload> = payload
        .nodes
        .iter()
        .filter_map(|n| match n {
            NodePayload::Channel(c) => Some(c),
            NodePayload::Agent(_) => None,
        })
        .collect();
    let channel_nodes = graph
        .value
        .nodes()
        .iter()
        .filter(|n| matches!(n, GraphNode::Channel(_)))
        .count();
    assert_eq!(channels.len(), channel_nodes);
    for channel in &channels {
        let expected: u64 = graph
            .value
            .accesses()
            .iter()
            .filter(|a| a.channel.to_ulid() == channel.id)
            .map(|a| a.accesses.get())
            .sum();
        assert_eq!(channel.volume, expected);
        assert!(!channel.name.starts_with("channel "), "named by lookup");
    }
    let accesses = payload
        .edges
        .iter()
        .filter(|e| matches!(e, EdgePayload::Access(_)))
        .count();
    assert_eq!(accesses, graph.value.accesses().len());
    // Channel-routed transmissions are drawn as accesses; the others
    // keep their share of the whole filtered total.
    let direct: Vec<&WeightedEdge> = graph
        .value
        .transmissions()
        .iter()
        .filter(|e| !matches!(e.route, Route::Channel(_)))
        .collect();
    let drawn: Vec<&TransmissionEdgePayload> = payload
        .edges
        .iter()
        .filter_map(|e| match e {
            EdgePayload::Transmission(t) => Some(t),
            EdgePayload::Access(_) => None,
        })
        .collect();
    assert_eq!(drawn.len(), direct.len());
    assert!(drawn.iter().all(|t| t.route_kind != RouteKindCode::Channel));
    for (payload, edge) in drawn.iter().zip(direct) {
        assert!((payload.share - edge.share.get()).abs() < f64::EPSILON);
    }
}

#[test]
fn promoted_and_declared_channels_are_declared() {
    assert_eq!(
        OriginCode::from(CanonicalOriginKind::Promoted),
        OriginCode::Declared
    );
    assert_eq!(
        OriginCode::from(CanonicalOriginKind::DeclaredBeforeTraffic),
        OriginCode::Declared
    );
    assert_eq!(
        OriginCode::from(CanonicalOriginKind::Discovered),
        OriginCode::Discovered
    );
}

#[test]
fn serializes_with_kind_tags_and_camel_case() {
    let payload =
        TopologyPayload::channels(&fixtures::bipartite_graph(), &fixtures::channel_names());
    let json = serde_json::to_value(&payload).expect("serialize");
    assert_eq!(json["mode"], "channels");
    assert_eq!(json["weighting"], "tx");
    let kinds: Vec<&str> = json["nodes"]
        .as_array()
        .expect("nodes")
        .iter()
        .filter_map(|n| n["kind"].as_str())
        .collect();
    assert!(kinds.contains(&"agent") && kinds.contains(&"channel"));
    let access = json["edges"]
        .as_array()
        .expect("edges")
        .iter()
        .find(|e| e["kind"] == "access")
        .expect("access edge");
    assert!(access["op"] == "read" || access["op"] == "write");
    assert!(json["nodes"][0].get("transmissionsIn").is_some());
}
