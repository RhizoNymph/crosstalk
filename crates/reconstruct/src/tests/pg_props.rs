//! Properties of `PgAgents` on generated agent tables, on Postgres:
//! resolution (against the reference resolver and on its own), merge
//! round trips and unmerges, and claim recording order.

use std::collections::BTreeSet;

use crosstalk_memory::reconstruct::MemoryAgents;
use crosstalk_memory::support::{IdSequence, Outbox};
use crosstalk_spec::ids::{AccountHash, AgentId, CredentialHash, PromptHash, SecretVersion};
use crosstalk_spec::interfaces::l3_reconstruction::agents::AgentReads;
use crosstalk_spec::interfaces::l3_reconstruction::lifecycle::{
    AgentLifecycle, AgentOrigin, NewAgent,
};
use crosstalk_spec::interfaces::l3_reconstruction::{
    AgentDirectory, ClaimStore, IdentityResolver, Resolution,
};
use crosstalk_spec::observed::agent::{
    Agent, AgentState, IdentityEvidence, IdentityScope, MergeAuthor, MergeRequest,
};
use crosstalk_spec::support::{Blake3, NonEmpty, Timestamp};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};

use super::pg::{CURSOR_KEY, Recorder, SeqIds, TestAgents, close, database, pool_on, truncate};
use crate::agents::PgAgents;

/// Run `body` on `cases` values of `strategy`, each with a fresh, empty
/// `PgAgents` on its own runtime and pool.
fn pg_property<S, F, Fut>(test: &str, cases: u32, strategy: S, body: F)
where
    S: Strategy,
    S::Value: std::fmt::Debug,
    F: Fn(S::Value, TestAgents) -> Fut,
    Fut: Future<Output = Result<(), TestCaseError>>,
{
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => panic!("no runtime: {error}"),
    };
    let Some(db) = runtime.block_on(database(test)) else {
        return;
    };
    let url = db.url().clone();
    let mut runner = TestRunner::new(Config {
        cases,
        failure_persistence: None,
        ..Config::default()
    });
    let result = runner.run(&strategy, |value| {
        let case = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| TestCaseError::fail(format!("no runtime: {error}")))?;
        case.block_on(async {
            let pool = pool_on(&url).await;
            truncate(&pool).await;
            let store = PgAgents::open(
                pool,
                Recorder::default(),
                SeqIds(IdSequence::default()),
                CURSOR_KEY,
            )
            .await
            .map_err(|error| TestCaseError::fail(format!("store: {error}")))?;
            body(value, store).await
        })
    });
    runtime.block_on(close(db));
    if let Err(error) = result {
        panic!("{error}");
    }
}

fn digest(n: u8) -> Blake3 {
    Blake3::from_bytes([n; 32])
}

fn credential(n: u8) -> CredentialHash {
    CredentialHash::from_keyed_digest(SecretVersion(1), digest(n))
}

/// The `n`th item of the evidence pool: harness agent and session ids
/// under two scopes, accounts, stable and rotating credentials, prompt
/// fingerprints.
fn item(n: u8) -> IdentityEvidence {
    let scope = |k: u8| IdentityScope::Credential(credential(100 + k % 2));
    match n % 7 {
        0 => IdentityEvidence::HarnessAgent {
            scope: scope(n / 7),
            agent: format!("agent-{}", n % 3),
        },
        1 => IdentityEvidence::HarnessSession {
            scope: scope(n / 7),
            session: format!("session-{}", n % 2),
        },
        2 => IdentityEvidence::Account(AccountHash::from_keyed_digest(
            SecretVersion(1),
            digest(n % 3),
        )),
        3 => IdentityEvidence::StableCredential(credential(n % 3)),
        4 => IdentityEvidence::RotatingCredential(credential(50 + n % 3)),
        _ => IdentityEvidence::PromptFingerprint(PromptHash::from_digest(digest(70 + n % 3))),
    }
}

/// A generated agent table: agents holding pool evidence, some merged.
#[derive(Debug, Clone)]
struct Population {
    agents: Vec<Vec<u8>>,
    merges: Vec<(u8, u8)>,
}

fn population() -> impl Strategy<Value = Population> {
    (
        proptest::collection::vec(proptest::collection::vec(0u8..28, 1..4), 1..6),
        proptest::collection::vec((0u8..6, 0u8..6), 0..3),
    )
        .prop_map(|(agents, merges)| Population { agents, merges })
}

fn agent_id(n: usize) -> AgentId {
    AgentId::from_ulid(0x0A6E_0000_0000_0000_0000_0000_0000_0100 + n as u128)
}

/// Store `population` in `store` and in a reference store.
async fn build<S>(store: &mut S, population: &Population) -> Result<(), TestCaseError>
where
    S: AgentLifecycle + IdentityResolver,
{
    for (n, items) in population.agents.iter().enumerate() {
        let mut evidence = NonEmpty::new(item(items[0]));
        for extra in &items[1..] {
            evidence.push(item(*extra));
        }
        store
            .create(NewAgent {
                id: agent_id(n),
                evidence,
                parent: None,
                origin: AgentOrigin::Traffic {
                    first_seen: Timestamp::from_micros(10),
                },
                label: None,
            })
            .await
            .map_err(|error| TestCaseError::fail(format!("create: {error:?}")))?;
    }
    let count = population.agents.len();
    for (from, into) in &population.merges {
        let (from, into) = (usize::from(*from) % count, usize::from(*into) % count);
        if let Ok(request) =
            MergeRequest::new(agent_id(from), agent_id(into), MergeAuthor::Resolver)
        {
            // Refusals (already one cluster, a merged agent) are fine.
            let _refused_or_applied = store.merge(request, Timestamp::from_micros(20)).await;
        }
    }
    Ok(())
}

fn query(items: &[u8]) -> NonEmpty<IdentityEvidence> {
    let mut evidence = NonEmpty::new(item(items[0]));
    for extra in &items[1..] {
        evidence.push(item(*extra));
    }
    evidence
}

fn named(resolution: &Resolution) -> BTreeSet<AgentId> {
    match resolution {
        Resolution::Known { agent, .. } => [*agent].into(),
        Resolution::New { .. } => BTreeSet::new(),
        Resolution::Conflict { candidates, .. } => candidates.iter().copied().collect(),
    }
}

async fn agents_of(store: &TestAgents, count: usize) -> Result<Vec<Agent>, TestCaseError> {
    let mut agents = Vec::new();
    for n in 0..count {
        let cluster = store
            .cluster(agent_id(n))
            .await
            .map_err(|error| TestCaseError::fail(format!("{error:?}")))?
            .ok_or_else(|| TestCaseError::fail("agent missing"))?;
        let record = std::iter::once(cluster.agent())
            .chain(cluster.aliases())
            .find(|agent| agent.id == agent_id(n))
            .cloned()
            .ok_or_else(|| TestCaseError::fail("agent not in its cluster"))?;
        agents.push(record);
    }
    Ok(agents)
}

fn queries() -> impl Strategy<Value = Vec<Vec<u8>>> {
    proptest::collection::vec(proptest::collection::vec(0u8..28, 1..4), 1..5)
}

/// `reconstruct.resolve.depends-only-on-evidence`: two exchanges carrying
/// the same evidence resolve alike, and as the reference resolver does
/// over the same table.
#[test]
fn resolution_depends_only_on_evidence_and_store() {
    pg_property(
        "resolution_depends_only_on_evidence_and_store",
        10,
        (population(), queries()),
        |(population, queries), mut store| async move {
            let mut reference = MemoryAgents::new(IdSequence::default(), Outbox::none());
            build(&mut store, &population).await?;
            build(&mut reference, &population).await?;
            for items in &queries {
                let evidence = query(items);
                let first = store.resolve(&evidence).await;
                let second = store.resolve(&evidence).await;
                prop_assert_eq!(&first, &second);
                prop_assert_eq!(&first, &reference.resolve(&evidence).await);
            }
            Ok(())
        },
    );
}

/// `reconstruct.resolve.conflict-has-two-candidates`.
#[test]
fn conflict_names_two_distinct_canonical_agents() {
    pg_property(
        "conflict_names_two_distinct_canonical_agents",
        10,
        (population(), queries()),
        |(population, queries), mut store| async move {
            build(&mut store, &population).await?;
            for items in &queries {
                if let Ok(Resolution::Conflict { candidates, .. }) =
                    store.resolve(&query(items)).await
                {
                    let all: Vec<AgentId> = candidates.iter().copied().collect();
                    let distinct: BTreeSet<AgentId> = all.iter().copied().collect();
                    prop_assert!(all.len() >= 2);
                    prop_assert_eq!(distinct.len(), all.len());
                    for candidate in all {
                        prop_assert_eq!(store.canonical(candidate), candidate);
                    }
                }
            }
            Ok(())
        },
    );
}

/// `reconstruct.resolve.most-specific-evidence-decides`: adding or
/// removing less specific evidence never changes the agents named.
#[test]
fn less_specific_evidence_never_changes_resolved_agents() {
    pg_property(
        "less_specific_evidence_never_changes_resolved_agents",
        10,
        (
            population(),
            queries(),
            proptest::collection::vec(0u8..28, 1..4),
        ),
        |(population, queries, extra), mut store| async move {
            build(&mut store, &population).await?;
            for items in &queries {
                let evidence = query(items);
                let top = evidence.iter().map(IdentityEvidence::specificity).max();
                let mut more = evidence.clone();
                for added in &extra {
                    let added = item(*added);
                    if Some(added.specificity()) < top {
                        more.push(added);
                    }
                }
                let deciding: Vec<IdentityEvidence> = evidence
                    .iter()
                    .filter(|candidate| Some(candidate.specificity()) == top)
                    .cloned()
                    .collect();
                let fewer = NonEmpty::from_vec(deciding)
                    .ok_or_else(|| TestCaseError::fail("no deciding evidence"))?;
                let base = store
                    .resolve(&evidence)
                    .await
                    .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;
                for variant in [more, fewer] {
                    let other = store
                        .resolve(&variant)
                        .await
                        .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;
                    prop_assert_eq!(named(&base), named(&other));
                }
            }
            Ok(())
        },
    );
}

/// Whether `agent` holds `item` as the resolver counts it.
fn holds(agent: &Agent, item: &IdentityEvidence) -> bool {
    let held = agent.evidence.iter().any(|held| held == item);
    match item {
        IdentityEvidence::HarnessSession { .. } => {
            held && !agent
                .evidence
                .iter()
                .any(|held| matches!(held, IdentityEvidence::HarnessAgent { .. }))
        }
        _ => held,
    }
}

/// `reconstruct.resolve.new-only-for-unknown-evidence`, and
/// `reconstruct.resolve.session-resolves-to-main-agent`: the agents named
/// are exactly the canonical forms of the holders of the deciding
/// evidence, a session counting only for agents with no harness agent id.
#[test]
fn new_resolution_only_when_deciding_evidence_unheld() {
    pg_property(
        "new_resolution_only_when_deciding_evidence_unheld",
        10,
        (population(), queries()),
        |(population, queries), mut store| async move {
            build(&mut store, &population).await?;
            let agents = agents_of(&store, population.agents.len()).await?;
            for items in &queries {
                let evidence = query(items);
                let top = evidence.iter().map(IdentityEvidence::specificity).max();
                let expected: BTreeSet<AgentId> = agents
                    .iter()
                    .filter(|agent| {
                        evidence
                            .iter()
                            .filter(|item| Some(item.specificity()) == top)
                            .any(|item| holds(agent, item))
                    })
                    .map(|agent| store.canonical(agent.id))
                    .collect();
                let resolution = store
                    .resolve(&evidence)
                    .await
                    .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;
                prop_assert_eq!(named(&resolution), expected.clone());
                prop_assert_eq!(
                    matches!(resolution, Resolution::New { .. }),
                    expected.is_empty()
                );
            }
            Ok(())
        },
    );
}

/// `reconstruct.resolve.session-resolves-to-main-agent`: a session id
/// held by a main agent and by its sub-agents resolves to the main agent.
#[test]
fn session_id_resolves_to_main_agent_only() {
    pg_property(
        "session_id_resolves_to_main_agent_only",
        8,
        (1usize..4, any::<bool>()),
        |(subagents, main_exists), mut store| async move {
            let scope = IdentityScope::Credential(credential(100));
            let session = IdentityEvidence::HarnessSession {
                scope: scope.clone(),
                session: "s".to_owned(),
            };
            let mut n = 0;
            let mut main = None;
            if main_exists {
                build_one(&mut store, n, vec![session.clone()]).await?;
                main = Some(agent_id(n));
                n += 1;
            }
            for k in 0..subagents {
                let sub = IdentityEvidence::HarnessAgent {
                    scope: scope.clone(),
                    agent: format!("sub-{k}"),
                };
                build_one(&mut store, n, vec![session.clone(), sub]).await?;
                n += 1;
            }
            let resolution = store
                .resolve(&NonEmpty::new(session))
                .await
                .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;
            prop_assert_eq!(
                named(&resolution),
                main.into_iter().collect::<BTreeSet<_>>()
            );
            Ok(())
        },
    );
}

async fn build_one(
    store: &mut TestAgents,
    n: usize,
    items: Vec<IdentityEvidence>,
) -> Result<(), TestCaseError> {
    let evidence = NonEmpty::from_vec(items).ok_or_else(|| TestCaseError::fail("no evidence"))?;
    store
        .create(NewAgent {
            id: agent_id(n),
            evidence,
            parent: None,
            origin: AgentOrigin::Traffic {
                first_seen: Timestamp::from_micros(10),
            },
            label: None,
        })
        .await
        .map_err(|error| TestCaseError::fail(format!("create: {error:?}")))
}

/// `reconstruct.resolve.harness-ids-scoped`: the same harness id under two
/// scopes, with no other evidence shared, never names the same agent.
#[test]
fn harness_ids_do_not_match_across_scopes() {
    pg_property(
        "harness_ids_do_not_match_across_scopes",
        8,
        (any::<bool>(), "[a-z]{1,6}"),
        |(agent_id_kind, id), mut store| async move {
            let (a, b) = (
                IdentityScope::Credential(credential(1)),
                IdentityScope::Account(AccountHash::from_keyed_digest(SecretVersion(1), digest(2))),
            );
            let under = |scope: IdentityScope| {
                if agent_id_kind {
                    IdentityEvidence::HarnessAgent {
                        scope,
                        agent: id.clone(),
                    }
                } else {
                    IdentityEvidence::HarnessSession {
                        scope,
                        session: id.clone(),
                    }
                }
            };
            build_one(&mut store, 0, vec![under(a)]).await?;
            let resolution = store
                .resolve(&NonEmpty::new(under(b)))
                .await
                .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;
            prop_assert!(
                matches!(resolution, Resolution::New { .. }),
                "{:?}",
                resolution
            );
            Ok(())
        },
    );
}

/// `reconstruct.resolve.token-refresh-keeps-agent`: replacing only the
/// rotating credential leaves the agent a session or account names.
#[test]
fn token_refresh_keeps_agent() {
    pg_property(
        "token_refresh_keeps_agent",
        8,
        (any::<bool>(), 0u8..3, 0u8..3),
        |(by_account, before, after), mut store| async move {
            let anchor = if by_account {
                IdentityEvidence::Account(AccountHash::from_keyed_digest(
                    SecretVersion(1),
                    digest(9),
                ))
            } else {
                IdentityEvidence::HarnessSession {
                    scope: IdentityScope::Upstream(crosstalk_spec::observed::client::UpstreamId(
                        "anthropic".to_owned(),
                    )),
                    session: "s".to_owned(),
                }
            };
            let token = |n: u8| IdentityEvidence::RotatingCredential(credential(60 + n));
            build_one(&mut store, 0, vec![anchor.clone(), token(before)]).await?;
            let mut refreshed = NonEmpty::new(anchor);
            refreshed.push(token(after + 3));
            let resolution = store
                .resolve(&refreshed)
                .await
                .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;
            prop_assert_eq!(named(&resolution), [agent_id(0)].into());
            Ok(())
        },
    );
}

/// `reconstruct.agent-label.never-evidence`: labels change no resolution.
#[test]
fn resolution_ignores_labels() {
    pg_property(
        "resolution_ignores_labels",
        8,
        (
            population(),
            queries(),
            proptest::collection::vec(0u8..3, 1..6),
        ),
        |(population, queries, labels), mut store| async move {
            build(&mut store, &population).await?;
            let before: Vec<_> = {
                let mut out = Vec::new();
                for items in &queries {
                    out.push(store.resolve(&query(items)).await);
                }
                out
            };
            let operator = crosstalk_spec::ids::OperatorId::from_ulid(5);
            for (n, label) in labels.iter().enumerate() {
                let id = agent_id(n % population.agents.len());
                // Merged agents refuse renames; that is fine here.
                let _ = store
                    .rename(
                        id,
                        crosstalk_memory::reconstruct::model::label(*label),
                        operator,
                    )
                    .await;
            }
            for (items, before) in queries.iter().zip(before) {
                prop_assert_eq!(store.resolve(&query(items)).await, before);
            }
            Ok(())
        },
    );
}

/// `reconstruct.agent-unmerge.round-trip`: a merge followed by the revert
/// of its record leaves every agent's state as it was.
#[test]
fn merge_unmerge_round_trip() {
    pg_property(
        "merge_unmerge_round_trip",
        10,
        (population(), 0u8..6, 0u8..6),
        |(population, from, into), mut store| async move {
            build(&mut store, &population).await?;
            let count = population.agents.len();
            let before = agents_of(&store, count).await?;
            let request = MergeRequest::new(
                agent_id(usize::from(from) % count),
                agent_id(usize::from(into) % count),
                MergeAuthor::Operator(crosstalk_spec::ids::OperatorId::from_ulid(5)),
            );
            let Ok(request) = request else {
                return Ok(());
            };
            let Ok(record) = store.merge(request, Timestamp::from_micros(30)).await else {
                return Ok(());
            };
            store
                .unmerge(
                    record.id(),
                    crosstalk_spec::ids::OperatorId::from_ulid(5),
                    Timestamp::from_micros(31),
                )
                .await
                .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;
            let after = agents_of(&store, count).await?;
            let states = |agents: &[Agent]| -> Vec<AgentState> {
                agents.iter().map(|agent| agent.state.clone()).collect()
            };
            prop_assert_eq!(states(&before), states(&after));
            Ok(())
        },
    );
}

/// `reconstruct.agent-unmerge.leaves-fresh-merges`: reverting a record
/// changes only its source and the agents it repointed that nothing moved
/// since.
#[test]
fn unmerge_leaves_later_decisions() {
    pg_property(
        "unmerge_leaves_later_decisions",
        10,
        (
            population(),
            proptest::collection::vec((0u8..6, 0u8..6), 1..4),
            0u8..4,
        ),
        |(population, more, pick), mut store| async move {
            build(&mut store, &population).await?;
            let count = population.agents.len();
            let operator = crosstalk_spec::ids::OperatorId::from_ulid(5);
            let mut records = Vec::new();
            for (step, (from, into)) in more.iter().enumerate() {
                if let Ok(request) = MergeRequest::new(
                    agent_id(usize::from(*from) % count),
                    agent_id(usize::from(*into) % count),
                    MergeAuthor::Operator(operator),
                ) && let Ok(record) = store
                    .merge(request, Timestamp::from_micros(40 + step as u64))
                    .await
                {
                    records.push(record);
                }
            }
            let Some(record) = records
                .get(usize::from(pick) % records.len().max(1))
                .cloned()
            else {
                return Ok(());
            };
            let before = agents_of(&store, count).await?;
            let Ok(reversal) = store
                .unmerge(record.id(), operator, Timestamp::from_micros(90))
                .await
            else {
                return Ok(());
            };
            let after = agents_of(&store, count).await?;
            for (old, new) in before.iter().zip(&after) {
                let moved = old.id == record.source() || reversal.restored.contains(&old.id);
                if !moved {
                    prop_assert_eq!(&old.state, &new.state, "{:?} changed", old.id);
                } else if old.id != record.source() {
                    let repointed_by_record = matches!(&old.state,
                        AgentState::Merged(merged) if merged.repointed_by.contains(&record.id()));
                    prop_assert!(repointed_by_record);
                    prop_assert_eq!(store.canonical(old.id), record.source());
                }
            }
            Ok(())
        },
    );
}

/// `reconstruct.claims.latest-time`: recording the same claims at the same
/// times, in any order and with repeats, gives the same claims, each at its
/// latest time.
#[test]
fn claim_observation_order_independent() {
    pg_property(
        "claim_observation_order_independent",
        8,
        proptest::collection::vec((0u8..4, 0u64..20), 1..8)
            .prop_flat_map(|records| (Just(records.clone()), Just(records).prop_shuffle())),
        |(records, shuffled), mut store| async move {
            let (a, b) = (agent_id(0), agent_id(1));
            for (claim, at) in &records {
                let claim = crosstalk_memory::reconstruct::model::claim(*claim);
                ClaimStore::record(&mut store, a, &claim, Timestamp::from_micros(*at))
                    .await
                    .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;
            }
            for (claim, at) in shuffled.iter().chain(shuffled.iter()) {
                let claim = crosstalk_memory::reconstruct::model::claim(*claim);
                ClaimStore::record(&mut store, b, &claim, Timestamp::from_micros(*at))
                    .await
                    .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;
            }
            let (claims_a, claims_b) = (store.claims(a).await, store.claims(b).await);
            prop_assert_eq!(&claims_a, &claims_b);
            let claims_a = claims_a.map_err(|e| TestCaseError::fail(format!("{e:?}")))?;
            for (claim, _) in &records {
                let claim = crosstalk_memory::reconstruct::model::claim(*claim);
                let latest = records
                    .iter()
                    .filter(|(other, _)| {
                        crosstalk_memory::reconstruct::model::claim(*other) == claim
                    })
                    .map(|(_, at)| Timestamp::from_micros(*at))
                    .max();
                prop_assert_eq!(claims_a.last_seen(&claim), latest);
            }
            Ok(())
        },
    );
}
