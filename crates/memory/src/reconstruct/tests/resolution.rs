//! The reference lookup behind `IdentityResolver::resolve`.

use super::*;

// ---- resolution ------------------------------------------------------------------

/// `reconstruct.resolve.conflict-has-two-candidates`: two clusters holding
/// the deciding evidence conflict; one cluster does not.
#[tokio::test]
async fn conflict_names_two_distinct_canonical_agents() {
    let (mut store, _events) = seeded(3).await;
    let shared = evidence(5);
    assert_eq!(store.attach(agent(0), shared.clone()).await, Ok(()));
    assert_eq!(store.attach(agent(1), shared.clone()).await, Ok(()));
    let resolved = resolve_evidence(&store.state.read(), vec![shared.clone()]);
    let Some(Resolution::Conflict { candidates, .. }) = resolved else {
        panic!("expected a conflict, got {resolved:?}");
    };
    assert_eq!(candidates.into_vec(), vec![agent(0), agent(1)]);
    merge(&mut store, 0, 1, 10).await;
    assert_eq!(
        resolve_evidence(&store.state.read(), vec![shared]),
        Some(Resolution::Known {
            agent: agent(1),
            new_evidence: Vec::new()
        })
    );
}

proptest! {
    /// `reconstruct.agent-label.never-evidence`: labels never change a
    /// resolution.
    #[test]
    fn resolution_ignores_labels(
        holders in proptest::collection::vec(0u8..4, 0..4),
        item in 0u8..8,
        labels in proptest::collection::vec(proptest::option::of(0u8..3), 4),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let (plain, labelled) = runtime.block_on(async {
            let (mut store, _events) = seeded(4).await;
            for n in &holders {
                let _ = store.attach(agent(*n), evidence(item)).await;
            }
            let plain = resolve_evidence(&store.state.read(), vec![evidence(item)]);
            for (n, l) in labels.iter().enumerate() {
                let n = u8::try_from(n).unwrap_or(0);
                let _ = store.rename(agent(n), l.and_then(label), op(1)).await;
            }
            let labelled = resolve_evidence(&store.state.read(), vec![evidence(item)]);
            (plain, labelled)
        });
        prop_assert_eq!(plain, labelled);
    }

    /// `reconstruct.resolve.most-specific-evidence-decides`: adding less
    /// specific evidence never changes which agents a resolution names.
    #[test]
    fn less_specific_evidence_never_changes_resolved_agents(
        holders in proptest::collection::vec((0u8..4, 0u8..8), 0..8),
        deciding in 0u8..8,
        extra in proptest::collection::vec(0u8..8, 0..4),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let (alone, with_extra) = runtime.block_on(async {
            let (mut store, _events) = seeded(4).await;
            for (n, e) in &holders {
                let _ = store.attach(agent(*n), evidence(*e)).await;
            }
            let top = evidence(deciding);
            let weaker: Vec<IdentityEvidence> = extra
                .iter()
                .map(|e| evidence(*e))
                .filter(|e| e.specificity() < top.specificity())
                .collect();
            let table = store.state.read();
            let alone = named(resolve_evidence(&table, vec![top.clone()]));
            let with_extra = named(resolve_evidence(&table, std::iter::once(top).chain(weaker).collect()));
            (alone, with_extra)
        });
        prop_assert_eq!(alone, with_extra);
    }
}

/// The agents a resolution names.
fn named(resolution: Option<Resolution>) -> Vec<AgentId> {
    match resolution {
        Some(Resolution::Known { agent, .. }) => vec![agent],
        Some(Resolution::Conflict { candidates, .. }) => candidates.into_vec(),
        Some(Resolution::New { .. }) | None => Vec::new(),
    }
}

/// A harness session id resolves only to the session's main agent.
#[tokio::test]
async fn session_resolves_to_its_main_agent() {
    let (mut store, _events) = seeded(3).await;
    let session = IdentityEvidence::HarnessSession {
        scope: crosstalk_spec::observed::agent::IdentityScope::Upstream(
            crosstalk_spec::observed::client::UpstreamId("upstream".to_owned()),
        ),
        session: "s".to_owned(),
    };
    // Agents 1 and 2 were seeded with an account and a credential; both
    // hold the session, and agent 2 also a harness agent id, which makes it
    // a sub-agent of the session rather than its main agent.
    assert_eq!(store.attach(agent(1), session.clone()).await, Ok(()));
    assert_eq!(store.attach(agent(2), session.clone()).await, Ok(()));
    assert_eq!(store.attach(agent(2), evidence(4)).await, Ok(()));
    let resolved = resolve_evidence(&store.state.read(), vec![session]);
    assert!(matches!(
        resolved,
        Some(Resolution::Known { agent: found, .. }) if found == agent(1)
    ));
}
