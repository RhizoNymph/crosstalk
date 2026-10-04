//! Governance actions: merges and renames. Rules are in `rules`, channel
//! policy and promotion in `channels`.

use crosstalk_spec::aggregates::edge::Weighting;
use crosstalk_spec::interfaces::l8_surface::Permission;

use super::super::clock::NOW;
use super::{caller, fresh, graph_of, node_ids, researcher, week};
use crosstalk_spec::aggregates::agents::AgentLookup;
use crosstalk_spec::ids::AgentId;
use crosstalk_spec::interfaces::l8_surface::ConflictKind;
use crosstalk_spec::interfaces::l8_surface::{ActionError, ActionOutcome, OperatorAction};
use crosstalk_spec::interfaces::l8_surface::{OperatorActions, QueryApi};
use crosstalk_spec::observed::agent::{AgentLabel, AgentState, MergeVeto};

use super::actions_support::*;

/// Whether a veto between `x` and `y` is stored.
fn vetoed(vetoes: &[MergeVeto], x: AgentId, y: AgentId) -> bool {
    vetoes
        .iter()
        .any(|v| (v.a(), v.b()) == (x.min(y), x.max(y)))
}

#[tokio::test]
async fn merge_then_unmerge_restores_the_graph() {
    let b = fresh();
    let c = researcher();
    let (pi1, pi2, al2, al3) = (
        agent(&b, "pi1"),
        agent(&b, "pi2"),
        agent(&b, "al2"),
        agent(&b, "al3"),
    );
    let before = graph_of(&b, &c, &week(), Weighting::Transmissions)
        .await
        .expect("topology");
    let prior = b.state.read().await.identity.clone();

    let outcome = b.act(&c, merge(&b, "pi2", "pi1")).await.expect("merge");
    let ActionOutcome::Merged(id) = outcome else {
        panic!("{outcome:?}")
    };
    {
        let state = b.state.read().await;
        let record = state
            .identity
            .merges()
            .iter()
            .find(|m| m.id() == id)
            .expect("record");
        assert_eq!((record.source(), record.target()), (pi2, pi1));
        let mut repointed = record.repointed().to_vec();
        repointed.sort();
        let mut expected = vec![al2, al3];
        expected.sort();
        assert_eq!(
            repointed, expected,
            "aliases of the source move to the target"
        );
        assert_eq!(state.identity.canonical(al2), pi1);
    }
    let merged = graph_of(&b, &c, &week(), Weighting::Transmissions)
        .await
        .expect("topology");
    assert!(node_ids(&merged.value).iter().all(|n| *n != pi2));
    assert!(
        merged
            .value
            .edges
            .iter()
            .all(|e| e.from != pi2 && e.to != pi2)
    );
    let detail = b
        .agent(&c, pi2, week().window)
        .await
        .expect("ok")
        .expect("detail")
        .value;
    assert_eq!(detail.cluster.agent().id, pi1);
    assert_eq!(
        detail.cluster.lookup(),
        AgentLookup::Redirected { from: pi2 }
    );

    assert_eq!(
        b.act(&c, OperatorAction::Unmerge { merge: id }).await,
        Ok(ActionOutcome::Applied)
    );
    {
        let state = b.state.read().await;
        assert_eq!(state.identity.agent(pi2), prior.agent(pi2), "prior state");
        assert_eq!(state.identity.agent(al2), prior.agent(al2), "restored");
        assert_eq!(state.identity.canonical(al2), pi2);
        assert_eq!(state.identity.canonical(al3), pi2);
        let reversal = state
            .identity
            .merges()
            .iter()
            .find(|m| m.id() == id)
            .and_then(|m| m.reverted())
            .expect("reverted");
        assert_eq!(reversal.at, NOW);
        assert!(vetoed(state.identity.vetoes(), pi2, pi1));
    }
    let after = graph_of(&b, &c, &week(), Weighting::Transmissions)
        .await
        .expect("topology");
    assert_eq!(before.value.edges, after.value.edges);
    assert_eq!(node_ids(&before.value), node_ids(&after.value));
    assert!(matches!(
        b.act(&c, OperatorAction::Unmerge { merge: id }).await.err(),
        Some(ActionError::Conflict(
            ConflictKind::MergeAlreadyReverted { .. }
        ))
    ));
    // Merging the pair again clears the veto.
    b.act(&c, merge(&b, "pi2", "pi1"))
        .await
        .expect("merge again");
    assert!(!vetoed(b.state.read().await.identity.vetoes(), pi2, pi1));
}

#[tokio::test]
async fn merges_name_canonical_agents_only() {
    let b = fresh();
    let c = researcher();
    let (al1, cx1) = (agent(&b, "al1"), agent(&b, "cx1"));
    // A merged target is a conflict naming its canonical agent: the merge
    // table never redirects (spec: both agents must be canonical).
    assert_eq!(
        b.act(&c, merge(&b, "cx3", "al1")).await.err(),
        Some(ActionError::Conflict(ConflictKind::AgentMerged {
            agent: al1,
            into: cx1
        }))
    );
    // So is a merged source.
    assert!(matches!(
        b.act(&c, merge(&b, "al0", "cx0")).await.err(),
        Some(ActionError::Conflict(ConflictKind::AgentMerged { .. }))
    ));
    // Two ids of one cluster are checked first, whichever way round.
    let (pi2, al3) = (agent(&b, "pi2"), agent(&b, "al3"));
    assert_eq!(
        b.act(&c, merge(&b, "pi2", "al3")).await.err(),
        Some(ActionError::Conflict(ConflictKind::MergeIntoSelf {
            from: pi2,
            into: al3,
            canonical: pi2
        }))
    );
    assert!(matches!(
        b.act(&c, merge(&b, "al2", "al3")).await.err(),
        Some(ActionError::Conflict(ConflictKind::MergeIntoSelf { .. }))
    ));
    // Naming the canonical agent goes ahead.
    let ActionOutcome::Merged(id) = b.act(&c, merge(&b, "cx3", "cx1")).await.expect("merge") else {
        panic!("not a merge")
    };
    let record_target = b
        .state
        .read()
        .await
        .identity
        .merges()
        .iter()
        .find(|m| m.id() == id)
        .map(|m| m.target());
    assert_eq!(record_target, Some(cx1));
    // An operator merge clears the veto on the pair.
    let (omp3, omp1) = (agent(&b, "omp3"), agent(&b, "omp1"));
    assert!(vetoed(b.state.read().await.identity.vetoes(), omp3, omp1));
    b.act(&c, merge(&b, "omp3", "omp1")).await.expect("merge");
    assert!(!vetoed(b.state.read().await.identity.vetoes(), omp3, omp1));
    // Unknown merge.
    let unknown = crosstalk_spec::ids::MergeId::from_ulid(1);
    assert_eq!(
        b.act(&c, OperatorAction::Unmerge { merge: unknown })
            .await
            .err(),
        Some(ActionError::NotFound)
    );
    // Merging needs Govern.
    let triage = caller(&[Permission::View, Permission::Triage]);
    assert_eq!(
        b.act(&triage, merge(&b, "cc5", "cc6")).await.err(),
        Some(ActionError::Forbidden {
            missing: Permission::Govern
        })
    );
}

#[tokio::test]
async fn merges_are_authored_by_the_caller() {
    use crosstalk_spec::observed::agent::{MergeAuthor, MergeRequest};

    let b = fresh();
    let c = researcher();
    // A request claiming the resolver is stamped with the caller.
    let forged = MergeRequest::new(agent(&b, "cc6"), agent(&b, "cc5"), MergeAuthor::Resolver)
        .expect("request");
    let ActionOutcome::Merged(id) = b
        .act(&c, OperatorAction::MergeAgents(forged))
        .await
        .expect("merge")
    else {
        panic!("not a merge")
    };
    let by = b
        .state
        .read()
        .await
        .identity
        .merges()
        .iter()
        .find(|m| m.id() == id)
        .map(|m| m.by());
    assert_eq!(by, Some(MergeAuthor::Operator(c.operator())));
}

#[tokio::test]
async fn rename_labels_canonical_agents_only() {
    let b = fresh();
    let c = researcher();
    let cc1 = agent(&b, "cc1");
    let label = AgentLabel::new("  planner  ").expect("label");
    b.act(
        &c,
        OperatorAction::RenameAgent {
            agent: cc1,
            label: Some(label.clone()),
        },
    )
    .await
    .expect("rename");
    let label_of = async |id| {
        b.agent(&c, id, week().window)
            .await
            .expect("ok")
            .expect("detail")
            .value
            .cluster
            .profile()
            .label()
            .map(|l| l.as_str().to_owned())
    };
    assert_eq!(label_of(cc1).await.as_deref(), Some("planner"));
    b.act(
        &c,
        OperatorAction::RenameAgent {
            agent: cc1,
            label: None,
        },
    )
    .await
    .expect("clear");
    assert_eq!(label_of(cc1).await, None);
    let alias = agent(&b, "al0");
    let before = b.state.read().await.identity.agent(alias).cloned();
    assert_eq!(
        b.act(
            &c,
            OperatorAction::RenameAgent {
                agent: alias,
                label: Some(label)
            }
        )
        .await
        .err(),
        Some(ActionError::Conflict(ConflictKind::AgentMerged {
            agent: alias,
            into: agent(&b, "cc0")
        }))
    );
    assert_eq!(
        b.state.read().await.identity.agent(alias).cloned(),
        before,
        "a refused rename changes nothing"
    );
}

#[tokio::test]
async fn merged_agents_show_their_new_state() {
    let b = fresh();
    let c = researcher();
    b.act(&c, merge(&b, "cc6", "cc5")).await.expect("merge");
    let state = b.state.read().await;
    let cc6 = state.identity.agent(agent(&b, "cc6")).expect("cc6");
    let AgentState::Merged(merged) = &cc6.state else {
        panic!("cc6 is merged")
    };
    assert_eq!(merged.into, agent(&b, "cc5"));
    let record = state
        .identity
        .merges()
        .iter()
        .find(|m| m.id() == merged.merge)
        .expect("its record");
    assert_eq!(record.at(), NOW);
}
