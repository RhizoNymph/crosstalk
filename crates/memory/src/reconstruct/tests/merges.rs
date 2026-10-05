//! The directory, merges, unmerges, vetoes and renames.

use super::*;

// ---- the directory ----------------------------------------------------------

/// `reconstruct.directory.canonical-follows-merge`.
#[tokio::test]
async fn canonical_follows_merge_table() {
    let (mut store, _events) = seeded(3).await;
    merge(&mut store, 0, 1, 10).await;
    assert_eq!(store.canonical(agent(0)), agent(1));
    assert_eq!(store.canonical(agent(1)), agent(1));
    assert_eq!(store.canonical(agent(2)), agent(2));
    assert_eq!(
        store.canonical(model::unknown_agent()),
        model::unknown_agent()
    );
}

/// `reconstruct.directory.merge-visible-after-return`: a `canonical` call
/// through another handle, started after `merge` returned, sees it.
#[tokio::test]
async fn merge_visible_to_later_canonical_calls() {
    let (mut store, _events) = seeded(2).await;
    let reader = store.clone();
    merge(&mut store, 0, 1, 10).await;
    assert_eq!(reader.canonical(agent(0)), agent(1));
}

// ---- merges -----------------------------------------------------------------

/// `reconstruct.agent-merge.records-restore-data`.
#[tokio::test]
async fn merge_records_prior_and_repointed() {
    let (mut store, _events) = seeded(3).await;
    merge(&mut store, 0, 1, 10).await;
    let Ok(record) = store.merge(operator_merge(1, 2), at(20)).await else {
        panic!("second merge refused");
    };
    assert_eq!(record.repointed(), &[agent(0)]);
    let AgentState::Merged(merged) = state(&store, 1).await else {
        panic!("source not merged");
    };
    assert_eq!(merged.merge, record.id());
    assert_eq!(
        merged.prior,
        ActiveAgentState::Provisional { first_seen: at(2) }
    );
    assert_eq!(store.canonical(agent(0)), agent(2));
}

/// `reconstruct.agent-merge.canonical-only`.
#[tokio::test]
async fn merge_refuses_merged_agents() {
    let (mut store, _events) = seeded(4).await;
    merge(&mut store, 0, 1, 10).await;
    merge(&mut store, 2, 3, 11).await;
    let before = store.state.read().clone();
    assert_eq!(
        store.merge(operator_merge(0, 3), at(20)).await,
        Err(ResolveError::AgentMerged {
            agent: agent(0),
            into: agent(1)
        })
    );
    assert_eq!(
        store.merge(operator_merge(1, 2), at(20)).await,
        Err(ResolveError::AgentMerged {
            agent: agent(2),
            into: agent(3)
        })
    );
    assert_eq!(*store.state.read(), before);
}

/// `reconstruct.agent-merge.into-self-refused`, before vetoes and whoever
/// asks.
#[tokio::test]
async fn merge_refuses_one_cluster() {
    let (mut store, _events) = seeded(3).await;
    merge(&mut store, 0, 2, 10).await;
    merge(&mut store, 1, 2, 11).await;
    for by in [MergeAuthor::Resolver, MergeAuthor::Operator(op(1))] {
        assert_eq!(
            store.merge(request(0, 1, by), at(20)).await,
            Err(ResolveError::MergeIntoSelf {
                from: agent(0),
                into: agent(1),
                canonical: agent(2)
            })
        );
        assert_eq!(
            store.merge(request(2, 0, by), at(20)).await,
            Err(ResolveError::MergeIntoSelf {
                from: agent(2),
                into: agent(0),
                canonical: agent(2)
            })
        );
    }
}

/// Unknown agents are refused first.
#[tokio::test]
async fn merge_refuses_unknown_agents() {
    let (mut store, _events) = seeded(1).await;
    assert_eq!(
        store.merge(operator_merge(0, 5), at(10)).await,
        Err(ResolveError::UnknownAgent(agent(5)))
    );
    assert_eq!(
        store.merge(operator_merge(5, 0), at(10)).await,
        Err(ResolveError::UnknownAgent(agent(5)))
    );
}

/// `reconstruct.agent-merged.once-per-merge`,
/// `reconstruct.agent-merged.none-for-repoints` and
/// `reconstruct.agent-merged.names-record`.
#[tokio::test]
async fn agent_merged_names_record() {
    let (mut store, mut events) = seeded(3).await;
    merge(&mut store, 0, 1, 10).await;
    drain(&mut events);
    let Ok(record) = store.merge(operator_merge(1, 2), at(20)).await else {
        panic!("merge refused");
    };
    let published = drain(&mut events);
    assert_eq!(agent_merged_count(&published), 1);
    assert!(
        published.contains(&BusEvent::Ingest(IngestEvent::AgentMerged {
            merge: record.id(),
            from: agent(1),
            into: agent(2),
            repointed: vec![agent(0)],
            by: MergeAuthor::Operator(op(1)),
        }))
    );
    // A refused repeat publishes nothing.
    assert!(store.merge(operator_merge(1, 2), at(30)).await.is_err());
    assert!(drain(&mut events).is_empty());
}

// ---- unmerges ---------------------------------------------------------------

/// `reconstruct.agent-unmerge.restores-prior-state`.
#[tokio::test]
async fn unmerge_restores_prior_state() {
    let (mut store, _events) = seeded(2).await;
    assert_eq!(
        store
            .advance(agent(0), Advance::Establish { since: at(5) })
            .await,
        Ok(())
    );
    let id = merge(&mut store, 0, 1, 10).await;
    assert!(store.unmerge(id, op(2), at(20)).await.is_ok());
    assert_eq!(
        state(&store, 0).await,
        AgentState::Established { since: at(5) }
    );
    assert_eq!(store.canonical(agent(0)), agent(0));
}

/// `reconstruct.merge-record.revert-once`.
#[tokio::test]
async fn unmerge_twice_conflicts() {
    let (mut store, _events) = seeded(2).await;
    let id = merge(&mut store, 0, 1, 10).await;
    assert!(store.unmerge(id, op(2), at(20)).await.is_ok());
    let before = store.state.read().clone();
    assert_eq!(
        store.unmerge(id, op(2), at(30)).await,
        Err(ResolveError::MergeAlreadyReverted(id))
    );
    assert_eq!(
        store.unmerge(MergeId::from_ulid(7), op(2), at(30)).await,
        Err(ResolveError::UnknownMerge(MergeId::from_ulid(7)))
    );
    assert_eq!(*store.state.read(), before);
}

/// `reconstruct.agent-unmerged.once-per-unmerge` and
/// `reconstruct.agent-unmerged.lists-restored`.
#[tokio::test]
async fn agent_unmerged_lists_restored_agents() {
    let (mut store, mut events) = seeded(4).await;
    merge(&mut store, 0, 1, 10).await;
    let second = merge(&mut store, 1, 2, 11).await;
    merge(&mut store, 2, 3, 12).await;
    drain(&mut events);
    let Ok(reversal) = store.unmerge(second, op(2), at(20)).await else {
        panic!("unmerge refused");
    };
    assert_eq!(reversal.restored, vec![agent(0)]);
    let published = drain(&mut events);
    let unmerged: Vec<&BusEvent> = published
        .iter()
        .filter(|event| matches!(event, BusEvent::Ingest(IngestEvent::AgentUnmerged { .. })))
        .collect();
    assert_eq!(
        unmerged,
        vec![&BusEvent::Ingest(IngestEvent::AgentUnmerged {
            merge: second,
            agent: agent(1),
            was_into: agent(3),
            restored: vec![agent(0)],
            by: op(2),
        })]
    );
    assert_eq!(store.canonical(agent(0)), agent(1));
    assert_eq!(store.canonical(agent(1)), agent(1));
    assert!(store.unmerge(second, op(2), at(30)).await.is_err());
    assert!(drain(&mut events).is_empty());
}

/// `reconstruct.agent-unmerge.leaves-fresh-merges`: an agent unmerged and
/// merged afresh since the reverted record repointed it is left alone.
#[tokio::test]
async fn unmerge_leaves_later_decisions() {
    let (mut store, _events) = seeded(4).await;
    let first = merge(&mut store, 0, 1, 10).await;
    let second = merge(&mut store, 1, 2, 11).await;
    assert!(store.unmerge(first, op(2), at(12)).await.is_ok());
    merge(&mut store, 0, 3, 13).await;
    let Ok(reversal) = store.unmerge(second, op(2), at(14)).await else {
        panic!("unmerge refused");
    };
    assert!(reversal.restored.is_empty());
    assert_eq!(store.canonical(agent(0)), agent(3));
}

/// `reconstruct.merge-veto.recorded-on-unmerge`.
#[tokio::test]
async fn unmerge_records_veto() {
    let (mut store, _events) = seeded(2).await;
    let id = merge(&mut store, 0, 1, 10).await;
    assert!(store.unmerge(id, op(2), at(20)).await.is_ok());
    let Ok(Some(cluster)) = store.cluster(agent(0)).await else {
        panic!("no cluster");
    };
    let Ok(expected) = MergeVeto::new(agent(0), agent(1), op(2), at(20)) else {
        panic!("veto");
    };
    assert_eq!(cluster.vetoes(), &[expected]);
}

/// `reconstruct.merge-veto.blocks-resolver`, over whole clusters.
#[tokio::test]
async fn veto_blocks_resolver_merge() {
    let (mut store, _events) = seeded(3).await;
    let id = merge(&mut store, 0, 1, 10).await;
    assert!(store.unmerge(id, op(2), at(20)).await.is_ok());
    merge(&mut store, 2, 1, 21).await;
    let before = store.state.read().clone();
    let refused = store
        .merge(request(0, 1, MergeAuthor::Resolver), at(30))
        .await;
    assert!(matches!(refused, Err(ResolveError::Vetoed(_))));
    assert_eq!(*store.state.read(), before);
}

/// `reconstruct.merge-veto.operator-clears`.
#[tokio::test]
async fn operator_merge_clears_veto() {
    let (mut store, _events) = seeded(2).await;
    let id = merge(&mut store, 0, 1, 10).await;
    assert!(store.unmerge(id, op(2), at(20)).await.is_ok());
    merge(&mut store, 0, 1, 30).await;
    assert!(store.state.read().vetoes.is_empty());
    assert_eq!(store.canonical(agent(0)), agent(1));
}

/// `reconstruct.agent-unmerge.round-trip`, on random histories: merging and
/// reverting that merge leaves every agent's state as it was.
#[test]
fn merge_unmerge_round_trip() {
    let strategy = (model::agent_ops(20), 0u8..model::AGENTS, 0u8..model::AGENTS);
    let outcome = crate::model::run(
        pipeline_harness(),
        strategy,
        |runtime, (ops, from, into)| {
            runtime.block_on(async {
                let (mut store, _events) = store();
                for op in ops {
                    replay(&mut store, op).await;
                }
                let before = store.state.read().agents.clone();
                let Ok(request) =
                    MergeRequest::new(agent(*from), agent(*into), MergeAuthor::Operator(op(1)))
                else {
                    return Ok(());
                };
                let Ok(record) = store.merge(request, at(10_000)).await else {
                    return Ok(());
                };
                store
                    .unmerge(record.id(), op(2), at(10_001))
                    .await
                    .map_err(|error| Divergence::new(0, format!("unmerge refused: {error:?}")))?;
                if store.state.read().agents == before {
                    Ok(())
                } else {
                    Err(Divergence::new(
                        0,
                        "agent states differ after merge and revert",
                    ))
                }
            })
        },
    );
    assert_eq!(outcome, Ok(()));
}

/// Apply a generated operation to one store, ignoring refusals.
async fn replay(store: &mut MemoryAgents, step: &model::AgentOp) {
    match step {
        model::AgentOp::Create {
            agent: n,
            evidence: e,
            parent,
            from_config,
            label: l,
            at: t,
        } => {
            let origin = if *from_config {
                AgentOrigin::Config { at: at(*t) }
            } else {
                AgentOrigin::Traffic { first_seen: at(*t) }
            };
            let _ = store
                .create(NewAgent {
                    id: agent(*n),
                    evidence: NonEmpty::new(evidence(*e)),
                    parent: parent.map(agent),
                    origin,
                    label: l.and_then(label),
                })
                .await;
        }
        model::AgentOp::Merge {
            from,
            into,
            operator,
        } => {
            let by = operator.map_or(MergeAuthor::Resolver, |n| {
                MergeAuthor::Operator(op(u128::from(n)))
            });
            if let Ok(request) = MergeRequest::new(agent(*from), agent(*into), by) {
                let _ = store.merge(request, at(5_000)).await;
            }
        }
        model::AgentOp::Rename {
            agent: n, label: l, ..
        } => {
            let _ = store.rename(agent(*n), l.and_then(label), op(1)).await;
        }
        _ => {}
    }
}

/// `reconstruct.agent-label.merge-keeps-labels`.
#[tokio::test]
async fn merge_and_unmerge_keep_labels() {
    let (mut store, _events) = seeded(2).await;
    assert_eq!(
        store.rename(agent(0), label(0), op(1)).await,
        Ok(Change::Applied)
    );
    assert_eq!(
        store.rename(agent(1), label(1), op(1)).await,
        Ok(Change::Applied)
    );
    let id = merge(&mut store, 0, 1, 10).await;
    let labels = |store: &MemoryAgents| {
        let table = store.state.read();
        (
            table.agents.get(&agent(0)).and_then(|a| a.label.clone()),
            table.agents.get(&agent(1)).and_then(|a| a.label.clone()),
        )
    };
    assert_eq!(labels(&store), (label(0), label(1)));
    assert!(store.unmerge(id, op(2), at(20)).await.is_ok());
    assert_eq!(labels(&store), (label(0), label(1)));
}

// ---- renames ----------------------------------------------------------------

/// `reconstruct.agent-rename.active-only` and
/// `reconstruct.agent-renamed.once-per-change`.
#[tokio::test]
async fn rename_refuses_merged_agent() {
    let (mut store, mut events) = seeded(2).await;
    assert_eq!(
        store.rename(agent(0), label(0), op(1)).await,
        Ok(Change::Applied)
    );
    assert_eq!(
        drain(&mut events),
        vec![
            BusEvent::Ingest(IngestEvent::AgentRenamed {
                agent: agent(0),
                label: label(0),
                by: op(1)
            }),
            BusEvent::Changed(Changed::Agent(agent(0))),
        ]
    );
    assert_eq!(
        store.rename(agent(0), label(0), op(1)).await,
        Ok(Change::Unchanged)
    );
    assert!(drain(&mut events).is_empty());
    merge(&mut store, 0, 1, 10).await;
    drain(&mut events);
    assert_eq!(
        store.rename(agent(0), label(1), op(1)).await,
        Err(ResolveError::AgentMerged {
            agent: agent(0),
            into: agent(1)
        })
    );
    assert!(drain(&mut events).is_empty());
    assert_eq!(
        store
            .state
            .read()
            .agents
            .get(&agent(0))
            .and_then(|a| a.label.clone()),
        label(0)
    );
}
