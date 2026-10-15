//! The agent-centred graph (`QueryApi::topology`) with its nodes, its
//! totals (`overview`), the edge-table key `EdgeUpdated` carries, and the
//! drill-down behind an edge (`edge_transmissions`).

use serde_json::{Value, json};

use super::super::harness::{assert_golden, assert_rejected};
use super::super::{ULID_A, ULID_B, ULID_C, id, ts};
use super::{
    AREA, a, agent_nodes, array, at, b, edges, edited, hour, n, object, topic, version, wiki,
    wiki_node,
};
use crate::aggregates::edge::{
    EdgeKey, EdgeStats, EdgeTotals, EdgeTransmission, EdgeTransmissionPage, TopicSlot,
    TopologyGraph, Weighting,
};
use crate::aggregates::node::{CanonicalOriginKind, CanonicalStateKind};
use crate::aggregates::watermark::Watermarked;
use crate::derived::flow::transmission::Route;
use crate::ids::TransmissionId;
use crate::paging::{Cursor, EdgeTransmissionList, Page, PageSize};
use crate::support::{NonEmpty, TimeWindow, Watermark};

fn graph() -> TopologyGraph {
    TopologyGraph {
        window: hour(),
        weighting: Weighting::Transmissions,
        topic_version: version(),
        nodes: agent_nodes(),
        edges: edges(),
    }
}

/// `QueryApi::topology`'s answer, and the graph of a quiet window.
#[test]
fn topology_graph_goldens() {
    assert_eq!(graph().check(), Ok(()));
    assert_golden(
        AREA,
        "topology_graph",
        &Watermarked {
            watermark: Watermark(ts("2026-10-04T12:58:00.000000Z")),
            value: graph(),
        },
    );
    let quiet = TopologyGraph {
        weighting: Weighting::MatchedBytes,
        nodes: Vec::new(),
        edges: Vec::new(),
        ..graph()
    };
    assert_golden(AREA, "topology_graph_empty", &quiet);
    assert_golden(AREA, "edge_totals", &EdgeTotals::of(&graph()));
}

#[test]
fn node_kind_goldens() {
    fn state(kind: CanonicalStateKind) -> CanonicalStateKind {
        match kind {
            CanonicalStateKind::Registered
            | CanonicalStateKind::Provisional
            | CanonicalStateKind::Established => kind,
        }
    }
    let states = [
        CanonicalStateKind::Registered,
        CanonicalStateKind::Provisional,
        CanonicalStateKind::Established,
    ]
    .map(state);
    assert_golden(AREA, "canonical_state_kinds", &states.to_vec());
    fn origin(kind: CanonicalOriginKind) -> CanonicalOriginKind {
        match kind {
            CanonicalOriginKind::DeclaredBeforeTraffic
            | CanonicalOriginKind::Promoted
            | CanonicalOriginKind::Discovered => kind,
        }
    }
    let origins = [
        CanonicalOriginKind::DeclaredBeforeTraffic,
        CanonicalOriginKind::Promoted,
        CanonicalOriginKind::Discovered,
    ]
    .map(origin);
    assert_golden(AREA, "canonical_origin_kinds", &origins.to_vec());
    // Both node variants, the channel one as the channel-centred view
    // draws it.
    assert_golden(
        AREA,
        "graph_nodes",
        &vec![agent_nodes()[1].clone(), wiki_node()],
    );
}

fn edge_key(topic_slot: TopicSlot) -> EdgeKey {
    let bucket = TimeWindow::new(
        ts("2026-10-04T12:41:00.000000Z"),
        ts("2026-10-04T12:42:00.000000Z"),
    )
    .expect("one bucket");
    EdgeKey::new(a(), b(), Route::Channel(wiki()), topic_slot, bucket).expect("two agents")
}

/// The edge-table key `InsightEvent::EdgeUpdated` carries: in a topic, and
/// an outlier's.
#[test]
fn edge_key_goldens() {
    let in_topic = TopicSlot {
        version: version(),
        topic: Some(topic()),
    };
    assert_golden(AREA, "edge_key", &edge_key(in_topic));
    let outlier = TopicSlot {
        version: version(),
        topic: None,
    };
    assert_golden(AREA, "edge_key_outlier", &edge_key(outlier));
}

/// `QueryApi::edge_transmissions`: a page with more to come, and the last
/// page; a row in a topic and an outlier's.
#[test]
fn edge_transmission_page_goldens() {
    let row = |ulid: &str, at: &str, bytes: u64, in_topic: bool| EdgeTransmission {
        transmission: id(TransmissionId::from_ulid_text, ulid),
        confirmed_at: ts(at),
        matched_bytes: n(bytes),
        topic: in_topic.then(topic),
    };
    let size = PageSize::new(2).expect("a valid size");
    let next: Cursor<EdgeTransmissionList> =
        Cursor::from_token("ZWRnZS0wMUo5WjNLOA".into()).expect("URL-safe base64");
    let first = NonEmpty::from_vec(vec![
        row(ULID_C, "2026-10-04T12:41:30.250000Z", 512, true),
        row(ULID_B, "2026-10-04T12:20:02.000001Z", 400, false),
    ])
    .expect("two rows");
    let more = EdgeTransmissionPage {
        topic_version: version(),
        page: Page::more(size, first, next).expect("two fit a page of two"),
    };
    assert_golden(
        AREA,
        "edge_transmission_page",
        &Watermarked {
            watermark: Watermark(ts("2026-10-04T12:58:00.000000Z")),
            value: more,
        },
    );
    let last = EdgeTransmissionPage {
        topic_version: version(),
        page: Page::last(
            size,
            vec![row(ULID_A, "2026-10-04T12:05:00.000000Z", 288, true)],
        )
        .expect("one fits"),
    };
    assert_golden(AREA, "edge_transmission_last_page", &last);
}

/// Each rule `TopologyGraph::check` states, broken in the golden graph's
/// JSON, refused on decode.
#[test]
fn topology_graphs_refuse_what_check_refuses() {
    let refused = |reason: &str, edit: &dyn Fn(&mut Value)| {
        assert_rejected::<TopologyGraph>(&edited(&graph(), edit), reason);
    };
    refused("invalid topology graph: SelfEdge { index: 1 }", &|json| {
        *at(json, "/edges/1/to") = at(json, "/edges/1/from").clone();
    });
    refused(
        "invalid topology graph: DuplicateEdge { index: 2 }",
        &|json| {
            let first = at(json, "/edges/0").clone();
            array(json, "/edges").push(first);
        },
    );
    refused("invalid topology graph: Share { index: 0 }", &|json| {
        *at(json, "/edges/0/share") = json!(0.5);
    });
    // Weighted by matched bytes the shares are 0.8 and 0.2, not 0.75 and
    // 0.25.
    refused("invalid topology graph: Share { index: 0 }", &|json| {
        *at(json, "/weighting") = json!("matched_bytes");
    });
    refused("invalid topology graph: Nodes(Missing(Agent(", &|json| {
        array(json, "/nodes").remove(2);
    });
    refused("invalid topology graph: Nodes(Duplicate(Agent(", &|json| {
        let first = at(json, "/nodes/0").clone();
        array(json, "/nodes").push(first);
    });
    // The agent-centred graph has no channel nodes.
    refused(
        "invalid topology graph: Nodes(Unexpected(Channel(",
        &|json| {
            let channel = serde_json::to_value(wiki_node()).unwrap_or(Value::Null);
            array(json, "/nodes").push(channel);
        },
    );
    refused("invalid topology graph: Nodes(MissingParent {", &|json| {
        array(json, "/nodes").remove(3);
    });
    refused("invalid topology graph: Nodes(SelfParent(", &|json| {
        *at(json, "/nodes/1/data/parent") = at(json, "/nodes/1/data/id").clone();
    });
    refused("invalid topology graph: Nodes(Counts(", &|json| {
        *at(json, "/nodes/0/data/transmissions_out") = json!(4);
    });
    refused("unknown field `total`", &|json| {
        object(json, "").insert("total".into(), json!(4));
    });
    refused("invalid value", &|json| {
        *at(json, "/edges/0/stats/transmissions") = json!(0);
    });
    refused("unknown variant `merged`", &|json| {
        *at(json, "/nodes/2/data/state_kind") = json!("merged");
    });
}

#[test]
fn edge_keys_refuse_self_edges_and_unknown_fields() {
    let in_topic = TopicSlot {
        version: version(),
        topic: Some(topic()),
    };
    let key = edge_key(in_topic);
    assert_rejected::<EdgeKey>(
        &edited(&key, |json| *at(json, "/to") = json!(ULID_A)),
        "invalid edge key: SelfEdge",
    );
    assert_rejected::<EdgeKey>(
        &edited(&key, |json| {
            object(json, "/topic").insert("label".into(), json!("plans"));
        }),
        "unknown field `label`",
    );
    assert_rejected::<EdgeStats>(
        r#"{"transmissions": 1, "matched_bytes": 0}"#,
        "invalid value",
    );
    assert_rejected::<EdgeTotals>(
        r#"{"topic_version": 3, "transmissions": 4, "matched_bytes": 1500, "active_channels": 1, "agents": 4}"#,
        "unknown field `agents`",
    );
    assert_rejected::<CanonicalOriginKind>(r#""superseded""#, "unknown variant `superseded`");
}
