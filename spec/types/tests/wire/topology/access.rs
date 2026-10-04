//! The channel-centred graph (`QueryApi::channel_topology`) and a channel's
//! resources with who used them (`QueryApi::channel_resources`).

use serde_json::{Value, json};

use super::super::harness::{assert_golden, assert_rejected};
use super::super::{ULID_A, ULID_B, ULID_C, id, ts};
use super::{
    AREA, a, agent_node, array, at, b, d, edge, edited, hour, n, object, share, stats, version,
    wiki, wiki_node,
};
use crate::aggregates::access::{
    AgentAccesses, BipartiteGraph, BipartiteParts, ResourceUse, ResourceUsePage, WeightedAccess,
};
use crate::aggregates::edge::Weighting;
use crate::aggregates::node::{CanonicalStateKind, GraphNode};
use crate::aggregates::watermark::Watermarked;
use crate::derived::flow::access::AccessKind;
use crate::derived::flow::resource::{Host, Locator, Resource};
use crate::derived::flow::transmission::Route;
use crate::ids::ResourceId;
use crate::paging::{Page, PageSize};
use crate::support::Watermark;

fn every_access_kind() -> Vec<AccessKind> {
    fn declared(kind: AccessKind) -> AccessKind {
        match kind {
            AccessKind::Write | AccessKind::Read => kind,
        }
    }
    [AccessKind::Write, AccessKind::Read]
        .into_iter()
        .map(declared)
        .collect()
}

fn access(agent: crate::ids::AgentId, op: AccessKind, count: u64, value: f64) -> WeightedAccess {
    WeightedAccess {
        agent,
        channel: wiki(),
        op,
        accesses: n(count),
        share: share(value),
    }
}

/// The planner wrote the wiki three times, the coder read it once, and one
/// transmission from the planner to the coder was confirmed on it.
fn parts() -> BipartiteParts {
    BipartiteParts {
        window: hour(),
        weighting: Weighting::Transmissions,
        topic_version: version(),
        nodes: vec![
            GraphNode::Agent(agent_node(
                a(),
                Some("planner"),
                CanonicalStateKind::Established,
                None,
                (0, 1),
            )),
            GraphNode::Agent(agent_node(
                b(),
                Some("coder"),
                CanonicalStateKind::Provisional,
                Some(d()),
                (1, 0),
            )),
            GraphNode::Agent(agent_node(
                d(),
                Some("orchestrator"),
                CanonicalStateKind::Registered,
                None,
                (0, 0),
            )),
            wiki_node(),
        ],
        accesses: vec![
            access(a(), AccessKind::Write, 3, 0.75),
            access(b(), AccessKind::Read, 1, 0.25),
        ],
        transmissions: vec![edge(a(), b(), Route::Channel(wiki()), stats(1, 400), 1.0)],
    }
}

fn graph() -> BipartiteGraph {
    BipartiteGraph::new(parts()).expect("the fixture graph keeps every rule")
}

#[test]
fn channel_topology_goldens() {
    assert_golden(
        AREA,
        "bipartite_graph",
        &Watermarked {
            watermark: Watermark(ts("2026-10-04T12:58:00.000000Z")),
            value: graph(),
        },
    );
    // A write nobody has read yet: an access edge, no transmission.
    let unread = BipartiteParts {
        nodes: vec![
            GraphNode::Agent(agent_node(
                a(),
                Some("planner"),
                CanonicalStateKind::Established,
                None,
                (0, 0),
            )),
            wiki_node(),
        ],
        accesses: vec![access(a(), AccessKind::Write, 2, 1.0)],
        transmissions: Vec::new(),
        ..parts()
    };
    assert_golden(
        AREA,
        "bipartite_graph_unread_write",
        &BipartiteGraph::new(unread).expect("valid"),
    );
    assert_golden(AREA, "access_kinds", &every_access_kind());
}

/// Each rule `BipartiteGraph::new` checks, broken in the golden graph's
/// JSON, refused on decode.
#[test]
fn bipartite_graphs_refuse_what_their_constructor_refuses() {
    let refused = |reason: &str, edit: &dyn Fn(&mut Value)| {
        assert_rejected::<BipartiteGraph>(&edited(&graph(), edit), reason);
    };
    refused("invalid bipartite graph: SelfEdge { index: 0 }", &|json| {
        *at(json, "/transmissions/0/to") = json!(ULID_A);
    });
    refused(
        "invalid bipartite graph: DuplicateTransmission { index: 1 }",
        &|json| {
            let first = at(json, "/transmissions/0").clone();
            array(json, "/transmissions").push(first);
        },
    );
    refused(
        "invalid bipartite graph: DuplicateAccess { index: 2 }",
        &|json| {
            let first = at(json, "/accesses/0").clone();
            array(json, "/accesses").push(first);
        },
    );
    refused(
        "invalid bipartite graph: AccessShare { index: 0 }",
        &|json| {
            *at(json, "/accesses/0/share") = json!(0.5);
        },
    );
    refused(
        "invalid bipartite graph: TransmissionShare { index: 0 }",
        &|json| {
            *at(json, "/transmissions/0/share") = json!(0.9);
        },
    );
    refused("invalid bipartite graph: Nodes(Missing(Channel(", &|json| {
        array(json, "/nodes").remove(3);
    });
    refused("invalid bipartite graph: Nodes(Missing(Agent(", &|json| {
        array(json, "/nodes").remove(1);
    });
    refused("invalid bipartite graph: Nodes(Counts(", &|json| {
        *at(json, "/nodes/1/data/transmissions_in") = json!(0);
    });
    refused("unknown field `channels`", &|json| {
        object(json, "").insert("channels".into(), json!([]));
    });
    refused("unknown variant `append`", &|json| {
        *at(json, "/accesses/0/op") = json!("append");
    });
    refused("invalid value", &|json| {
        *at(json, "/accesses/1/accesses") = json!(0);
    });
}

fn page_resource() -> Resource {
    Resource {
        id: id(ResourceId::from_ulid_text, ULID_C),
        locator: Locator::Url {
            scheme: "https".into(),
            host: Host("wiki.example".into()),
            path: "/team/plan".into(),
            query: None,
        },
        first_seen: ts("2026-10-04T09:15:42.000000Z"),
    }
}

fn uses(agent: crate::ids::AgentId, count: u64) -> AgentAccesses {
    AgentAccesses {
        agent,
        accesses: n(count),
    }
}

fn resource_use() -> ResourceUse {
    ResourceUse::new(
        page_resource(),
        vec![uses(a(), 3)],
        vec![uses(b(), 1), uses(d(), 1)],
    )
    .expect("written and read, each agent once")
}

#[test]
fn channel_resources_golden() {
    let page = ResourceUsePage {
        channel: wiki(),
        window: hour(),
        page: Page::last(
            PageSize::new(50).expect("a valid size"),
            vec![resource_use()],
        )
        .expect("one fits"),
    };
    assert_golden(
        AREA,
        "resource_use_page",
        &Watermarked {
            watermark: Watermark(ts("2026-10-04T12:58:00.000000Z")),
            value: page,
        },
    );
}

#[test]
fn resource_use_decodes_through_its_constructor() {
    // Readers out of order decode to the constructor's order: most accesses
    // first, ties by agent id.
    let unordered = edited(&resource_use(), |json| {
        array(json, "/readers").reverse();
    });
    let decoded: ResourceUse = serde_json::from_str(&unordered)
        .unwrap_or_else(|error| panic!("reordered readers decode: {error}"));
    assert_eq!(decoded, resource_use());
    assert_rejected::<ResourceUse>(
        &edited(&resource_use(), |json| {
            *at(json, "/writers") = json!([]);
            *at(json, "/readers") = json!([]);
        }),
        "invalid resource use: Unused",
    );
    assert_rejected::<ResourceUse>(
        &edited(&resource_use(), |json| {
            let writer = at(json, "/writers/0").clone();
            array(json, "/writers").push(writer);
        }),
        "invalid resource use: DuplicateWriter(",
    );
    assert_rejected::<ResourceUse>(
        &edited(&resource_use(), |json| {
            *at(json, "/readers/1/agent") = json!(ULID_B);
        }),
        "invalid resource use: DuplicateReader(",
    );
    assert_rejected::<ResourceUse>(
        &edited(&resource_use(), |json| {
            object(json, "").insert("channel".into(), json!(null));
        }),
        "unknown field `channel`",
    );
}
