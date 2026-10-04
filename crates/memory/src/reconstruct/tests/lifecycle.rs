//! The agent lifecycle, change announcements, claims, activity and reads.

use super::*;

// ---- lifecycle and announcements ---------------------------------------------

/// `reconstruct.agent-state.traffic-leaves-registered`: a registered
/// agent's first exchange makes it provisional.
#[tokio::test]
async fn first_exchange_moves_registered_to_provisional() {
    let (mut store, _events) = store();
    let config = AgentOrigin::Config { at: at(1) };
    assert_eq!(store.create(new_agent(0, None, config)).await, Ok(()));
    assert_eq!(
        store
            .advance(agent(0), Advance::FirstTraffic { at: at(5) })
            .await,
        Ok(())
    );
    assert_eq!(
        state(&store, 0).await,
        AgentState::Provisional { first_seen: at(5) }
    );
    assert_eq!(
        ActivityStore::last_seen(&store, agent(0)).await,
        Ok(Some(at(5)))
    );
    assert_eq!(
        store
            .advance(agent(0), Advance::FirstTraffic { at: at(6) })
            .await,
        Err(AgentLifecycleError::IllegalTransition { agent: agent(0) })
    );
}

/// `reconstruct.agent-state.traffic-leaves-registered`: an agent created by
/// traffic starts provisional and is seen.
#[tokio::test]
async fn discovered_agent_starts_provisional() {
    let (store, _events) = seeded(1).await;
    assert_eq!(
        state(&store, 0).await,
        AgentState::Provisional { first_seen: at(1) }
    );
    assert_eq!(
        ActivityStore::last_seen(&store, agent(0)).await,
        Ok(Some(at(1)))
    );
}

/// `reconstruct.agent.change-announced`: creation announces the agent and
/// its canonical parent; a merge announces its source, target, repointed
/// agents, their children and the source's canonical parent.
#[tokio::test]
async fn agent_changes_announced_after_commit() {
    let (mut store, mut events) = store();
    let traffic = AgentOrigin::Traffic { first_seen: at(1) };
    assert_eq!(store.create(new_agent(0, None, traffic)).await, Ok(()));
    assert_eq!(store.create(new_agent(1, Some(0), traffic)).await, Ok(()));
    assert_eq!(store.create(new_agent(2, None, traffic)).await, Ok(()));
    assert_eq!(store.create(new_agent(3, Some(1), traffic)).await, Ok(()));
    let published = drain(&mut events);
    assert!(
        changed_agents(&published).is_superset(&[agent(0), agent(1), agent(2), agent(3)].into())
    );
    merge(&mut store, 1, 2, 10).await;
    let published = drain(&mut events);
    assert_eq!(
        changed_agents(&published),
        [agent(0), agent(1), agent(2), agent(3)].into()
    );
}

// ---- claims and activity -------------------------------------------------------

/// `reconstruct.claims.union-over-aliases`.
#[tokio::test]
async fn claims_follow_merge_and_unmerge() {
    let (mut store, _events) = seeded(2).await;
    assert_eq!(
        ClaimStore::record(&mut store, agent(0), &claim(0), at(5)).await,
        Ok(())
    );
    assert_eq!(
        ClaimStore::record(&mut store, agent(1), &claim(1), at(6)).await,
        Ok(())
    );
    let own = |n: u8, t: u64| {
        let mut set = ClaimSet::default();
        set.observe(claim(n), at(t));
        set
    };
    let id = merge(&mut store, 0, 1, 10).await;
    let union = ClaimSet::union([&own(0, 5), &own(1, 6)]);
    assert_eq!(
        ClaimStore::claims(&store, agent(0)).await,
        Ok(union.clone())
    );
    assert_eq!(ClaimStore::claims(&store, agent(1)).await, Ok(union));
    assert!(store.unmerge(id, op(2), at(20)).await.is_ok());
    assert_eq!(ClaimStore::claims(&store, agent(0)).await, Ok(own(0, 5)));
    assert_eq!(ClaimStore::claims(&store, agent(1)).await, Ok(own(1, 6)));
}

proptest! {
    /// `reconstruct.claims.latest-time`, through the store: recording the
    /// same claims in any order, with repeats, gives the same set.
    #[test]
    fn claim_observation_order_independent(
        records in proptest::collection::vec((0u8..4, 0u64..20), 0..12),
        seed in any::<u64>(),
    ) {
        let mut shuffled = records.clone();
        let len = shuffled.len().max(1);
        shuffled.rotate_left(usize::try_from(seed).unwrap_or(0) % len);
        shuffled.extend(records.iter().take(3).copied());
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let (a, b) = runtime.block_on(async {
            let (mut first, _e1) = store();
            let (mut second, _e2) = store();
            for (n, t) in &records {
                let _ = ClaimStore::record(&mut first, agent(0), &claim(*n), at(*t)).await;
            }
            for (n, t) in &shuffled {
                let _ = ClaimStore::record(&mut second, agent(0), &claim(*n), at(*t)).await;
            }
            (
                ClaimStore::claims(&first, agent(0)).await,
                ClaimStore::claims(&second, agent(0)).await,
            )
        });
        prop_assert_eq!(a, b);
    }
}

/// `ActivityStore::record` keeps the latest time, idempotently and in any
/// order, and `last_seen` is the latest over the cluster.
#[tokio::test]
async fn activity_keeps_latest_over_the_cluster() {
    let (mut store, _events) = seeded(2).await;
    for t in [9, 3, 9, 7] {
        assert_eq!(
            ActivityStore::record(&mut store, agent(0), at(t)).await,
            Ok(())
        );
    }
    assert_eq!(
        ActivityStore::last_seen(&store, agent(0)).await,
        Ok(Some(at(9)))
    );
    assert_eq!(
        ActivityStore::last_seen(&store, agent(1)).await,
        Ok(Some(at(2)))
    );
    let id = merge(&mut store, 0, 1, 10).await;
    assert_eq!(
        ActivityStore::last_seen(&store, agent(1)).await,
        Ok(Some(at(9)))
    );
    assert!(store.unmerge(id, op(2), at(20)).await.is_ok());
    assert_eq!(
        ActivityStore::last_seen(&store, agent(1)).await,
        Ok(Some(at(2)))
    );
    assert_eq!(
        ActivityStore::last_seen(&store, model::unknown_agent()).await,
        Ok(None)
    );
}

// ---- reads -----------------------------------------------------------------------

/// `surface.agent.rows-canonical`: rows are canonical agents with their
/// aliases, canonical parent, unioned claims and cluster last-seen time.
#[tokio::test]
async fn agent_rows_are_canonical() {
    let (mut store, _events) = store();
    let traffic = |t| AgentOrigin::Traffic { first_seen: at(t) };
    assert_eq!(store.create(new_agent(0, None, traffic(1))).await, Ok(()));
    assert_eq!(store.create(new_agent(1, None, traffic(2))).await, Ok(()));
    assert_eq!(
        store.create(new_agent(2, Some(0), traffic(3))).await,
        Ok(())
    );
    assert_eq!(
        store.create(new_agent(3, Some(2), traffic(4))).await,
        Ok(())
    );
    merge(&mut store, 0, 1, 10).await;
    merge(&mut store, 3, 2, 11).await;
    let pages = model::traverse(&store, &AgentFilter::default(), 10).await;
    let Ok(pages) = pages else {
        panic!("list failed");
    };
    let rows: Vec<_> = pages.concat();
    let ids: Vec<AgentId> = rows.iter().map(|row| row.id()).collect();
    assert_eq!(ids, vec![agent(2), agent(1)]);
    assert_eq!(rows[0].aliases(), &[agent(3)]);
    assert_eq!(rows[0].parent(), Some(agent(1)));
    assert_eq!(rows[0].last_seen(), Some(at(4)));
    assert_eq!(rows[1].aliases(), &[agent(0)]);
    assert_eq!(rows[1].parent(), None);
}

/// Pages are newest first, cover every row once, and refuse cursors issued
/// for another filter or never issued.
#[tokio::test]
async fn agent_list_pages_and_refuses_foreign_cursors() {
    let (store, _events) = seeded(5).await;
    let Ok(pages) = model::traverse(&store, &AgentFilter::default(), 2).await else {
        panic!("list failed");
    };
    let ids: Vec<AgentId> = pages.concat().iter().map(|row| row.id()).collect();
    assert_eq!(ids, (0..5).rev().map(agent).collect::<Vec<_>>());
    assert_eq!(pages.len(), 3);
    let Ok(size) = PageSize::new(2) else {
        panic!("size");
    };
    let Ok(first) = store
        .list(&AgentFilter::default(), &PageRequest { size, after: None })
        .await
    else {
        panic!("first page");
    };
    let other = AgentFilter {
        parents: vec![agent(0)],
        ..AgentFilter::default()
    };
    let after = first.next().cloned();
    assert_eq!(
        store
            .list(&other, &PageRequest { size, after })
            .await
            .map(|_| ()),
        Err(AgentReadError::InvalidCursor)
    );
    let Ok(bogus) = Cursor::from_token("agents-999".to_owned()) else {
        panic!("token");
    };
    assert_eq!(
        store
            .list(
                &AgentFilter::default(),
                &PageRequest {
                    size,
                    after: Some(bogus)
                }
            )
            .await
            .map(|_| ()),
        Err(AgentReadError::InvalidCursor)
    );
}

/// `surface.agent.detail-resolves-alias`, at L3: `cluster` of a merged id is
/// the canonical agent's, redirected; of an unknown id, `None`.
#[tokio::test]
async fn cluster_redirects_merged_ids() {
    let (mut store, _events) = seeded(2).await;
    merge(&mut store, 0, 1, 10).await;
    let Ok(Some(cluster)) = store.cluster(agent(0)).await else {
        panic!("no cluster");
    };
    assert_eq!(cluster.profile().id(), agent(1));
    assert_eq!(cluster.lookup(), AgentLookup::Redirected { from: agent(0) });
    let Ok(Some(own)) = store.cluster(agent(1)).await else {
        panic!("no cluster");
    };
    assert_eq!(own.lookup(), AgentLookup::Canonical);
    assert_eq!(store.cluster(model::unknown_agent()).await, Ok(None));
}

/// `surface.agent.names-batch`, at L3.
#[tokio::test]
async fn names_resolve_aliases_and_skip_unknown() {
    let (mut store, _events) = seeded(2).await;
    assert_eq!(
        store.rename(agent(1), label(2), op(1)).await,
        Ok(Change::Applied)
    );
    merge(&mut store, 0, 1, 10).await;
    let Ok(batch) = IdBatch::new([agent(0), agent(1), model::unknown_agent()]) else {
        panic!("batch");
    };
    let Ok(names) = store.names(&batch).await else {
        panic!("names");
    };
    assert_eq!(
        names.keys().copied().collect::<Vec<_>>(),
        vec![agent(0), agent(1)]
    );
    for name in names.values() {
        assert_eq!(name.id, agent(1));
        assert_eq!(name.label, label(2));
    }
}
