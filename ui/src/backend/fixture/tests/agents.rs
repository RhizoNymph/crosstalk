//! Agent reads as `QueryApi` defines them: rows are canonical, their
//! traffic is their topology node's, the window never changes the rows,
//! details follow aliases, names come in batches.

use std::collections::HashMap;

use crosstalk_spec::aggregates::agents::{AgentLookup, AgentRow, AgentTraffic};
use crosstalk_spec::aggregates::edge::Weighting;
use crosstalk_spec::aggregates::node::GraphNode;
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::ids::AgentId;
use crosstalk_spec::interfaces::l8_surface::{InputError, Permission, QueryError};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use super::super::clock::WATERMARK;
use super::reads_support::agent;
use super::{caller, collect, day, first, graph_of, researcher, shared, week};
use crate::url::scope::Scope;
use crosstalk_spec::interfaces::l8_surface::QueryApi;

async fn rows(scope: &Scope) -> Vec<AgentRow> {
    let b = shared();
    let c = researcher();
    collect(9, async |p| {
        b.agents(&c, &Default::default(), scope.window, &p)
            .await
            .map(|rows| rows.value)
    })
    .await
}

#[tokio::test]
async fn rows_carry_their_topology_nodes_counts() {
    for scope in [day(), week()] {
        // The default filter: the scopes filter nothing.
        let graph = graph_of(shared(), &researcher(), &scope, Weighting::Transmissions)
            .await
            .expect("topology");
        let nodes: HashMap<AgentId, AgentTraffic> = graph
            .value
            .nodes()
            .iter()
            .filter_map(|n| match n {
                GraphNode::Agent(a) => Some((
                    a.id,
                    AgentTraffic {
                        transmissions_in: a.transmissions_in,
                        transmissions_out: a.transmissions_out,
                    },
                )),
                GraphNode::Channel(_) => None,
            })
            .collect();
        let rows = rows(&scope).await;
        assert_eq!(rows.len(), 40);
        for row in &rows {
            let id = row.profile.id();
            assert_eq!(
                row.traffic,
                nodes.get(&id).copied().unwrap_or_default(),
                "{id:?}"
            );
            if let Some(GraphNode::Agent(node)) = graph
                .value
                .nodes()
                .iter()
                .find(|n| matches!(n, GraphNode::Agent(a) if a.id == id))
            {
                assert_eq!(node.label.as_ref(), row.profile.label());
                assert_eq!(node.parent, row.profile.parent());
                assert_eq!(&node.claims, row.profile.claims());
                assert_eq!(node.state_kind, row.profile.state_kind());
            }
        }
        assert!(rows.iter().any(|r| r.traffic != AgentTraffic::default()));
    }
}

#[tokio::test]
async fn the_window_changes_counts_never_rows() {
    let ids = |rows: Vec<AgentRow>| rows.iter().map(|r| r.profile.id()).collect::<Vec<_>>();
    let (daily, weekly) = (rows(&day()).await, rows(&week()).await);
    let busier = daily
        .iter()
        .zip(&weekly)
        .any(|(d, w)| d.traffic.transmissions_in < w.traffic.transmissions_in);
    assert!(busier, "a week counts more than a day");
    assert_eq!(ids(daily), ids(weekly));
}

#[tokio::test]
async fn unaligned_windows_are_refused() {
    let b = shared();
    let c = researcher();
    let window = TimeWindow::new(Timestamp::from_micros(WATERMARK.as_micros() - 1), WATERMARK)
        .expect("window");
    let unaligned = Some(QueryError::InvalidInput(InputError::UnalignedWindow));
    assert_eq!(
        b.agents(&c, &Default::default(), window, &first(5))
            .await
            .err(),
        unaligned
    );
    assert_eq!(b.agent(&c, agent("cc0"), window).await.err(), unaligned);
}

#[tokio::test]
async fn details_follow_aliases_and_keep_their_history() {
    let b = shared();
    let c = researcher();
    let read = async |id: AgentId| {
        b.agent(&c, id, week().window)
            .await
            .expect("read")
            .expect("detail")
    };
    let canonical = read(agent("cc0")).await;
    assert_eq!(canonical.watermark.at(), WATERMARK);
    assert_eq!(canonical.value.cluster.lookup(), AgentLookup::Canonical);
    let via_alias = read(agent("al0")).await.value;
    assert_eq!(
        via_alias.cluster.lookup(),
        AgentLookup::Redirected { from: agent("al0") }
    );
    assert_eq!(via_alias.traffic, canonical.value.traffic);
    // The pi agent holding two aliases: one merged into it, one repointed.
    let pi2 = read(agent("pi2")).await.value;
    assert_eq!(pi2.cluster.alias_ids().len(), 2);
    assert_eq!(pi2.cluster.merges().len(), 2);
    assert!(
        pi2.cluster
            .merges()
            .iter()
            .any(|m| m.repointed() == [agent("al2")])
    );
    // The oh-my-pi agent whose merge was reverted shows the reversal and
    // the veto it left.
    for key in ["omp3", "omp1"] {
        let detail = read(agent(key)).await.value;
        assert!(detail.cluster.alias_ids().is_empty(), "{key}");
        assert!(
            detail
                .cluster
                .merges()
                .iter()
                .any(|m| m.reverted().is_some()),
            "{key}"
        );
        assert_eq!(detail.cluster.vetoes().len(), 1, "{key}");
    }
    assert_eq!(
        b.agent(&c, AgentId::from_ulid(1), week().window).await,
        Ok(None)
    );
    let nobody = caller(&[Permission::Audit]);
    assert_eq!(
        b.agent(&nobody, agent("cc0"), week().window).await.err(),
        Some(QueryError::Forbidden {
            missing: Permission::View
        })
    );
}

#[tokio::test]
async fn names_cover_every_known_id_of_a_batch() {
    let b = shared();
    let c = researcher();
    let all: Vec<AgentId> = b
        .state
        .read()
        .await
        .identity
        .agents()
        .map(|a| a.id)
        .collect();
    let names = b
        .agent_names(&c, &IdBatch::new(all.iter().copied()).expect("batch"))
        .await
        .expect("names");
    assert_eq!(names.len(), all.len(), "aliases are named too");
    let canonical: Vec<AgentId> = rows(&week()).await.iter().map(|r| r.profile.id()).collect();
    assert!(names.values().all(|n| canonical.contains(&n.id)));
}
