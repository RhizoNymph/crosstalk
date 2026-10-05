//! Model-based property harness for L3 agent stores.
//!
//! [`check_agent_store`] generates random sequences of [`AgentOp`]s (agent
//! creation and state changes, merges, unmerges, renames, claims, activity
//! and filtered list traversals) and runs each sequence on a store under
//! test and on [`MemoryAgents`], the reference. After every step it
//! requires:
//!
//! - equal results from the operation itself;
//! - equal published events, except `Changed` notifications, where the
//!   store under test must announce at least every agent the reference
//!   does (announcing more is allowed: a notification only triggers a
//!   re-query);
//! - equal observations: `canonical`, `claims` and `last_seen` of every
//!   agent id, `cluster` of every id, `names` of every id plus an unknown
//!   one, and a full traversal of the unfiltered agents list in pages of 2;
//! - three invariants of the store under test's own agent records, read
//!   back through `cluster`: merge chains are flat
//!   (`reconstruct.agent-merge.target-not-merged`), an agent is merged
//!   exactly when one unreverted record names it
//!   (`reconstruct.agent-merge.record-agreement`), and every state change
//!   follows the lifecycle (`reconstruct.agent-state.legal-transitions`).
//!
//! The store under test is built by `make` (or, asynchronously, by
//! [`check_agent_store_with`]'s) from the [`IdSequence`] it must
//! draw merge record ids from (one id per accepted merge, nothing else) and
//! the [`Outbox`] it must publish its events to, so that ids and events can
//! be compared without translation.

use std::collections::{BTreeMap, HashSet};

use crosstalk_spec::aggregates::agents::AgentProfile;
use crosstalk_spec::aggregates::agents::filter::{AgentFilter, AgentText};
use crosstalk_spec::aggregates::node::CanonicalStateKind;
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::ids::{
    AccountHash, AgentId, CredentialHash, MergeId, OperatorId, SecretVersion,
};
use crosstalk_spec::interfaces::l3_reconstruction::agents::{
    ActivityStore, AgentReadError, AgentReads,
};
use crosstalk_spec::interfaces::l3_reconstruction::lifecycle::{
    Advance, AgentLifecycle, AgentOrigin, NewAgent,
};
use crosstalk_spec::interfaces::l3_reconstruction::{AgentDirectory, ClaimStore, IdentityResolver};
use crosstalk_spec::observed::agent::{
    Agent, AgentLabel, AgentState, IdentityEvidence, IdentityScope, MergeAuthor, MergeRecord,
    MergeRequest,
};
use crosstalk_spec::observed::client::{HarnessClaim, HarnessFamily, UpstreamId};
use crosstalk_spec::paging::{AgentList, Cursor, PageRequest, PageSize};
use crosstalk_spec::support::{Blake3, NonEmpty, Timestamp};
use proptest::prelude::*;

use super::MemoryAgents;
use crate::model::{Divergence, HarnessConfig, ModelMismatch, run, same};
use crate::support::{IdSequence, Outbox, drain};

/// Every spec trait an L3 agent store implements.
pub trait AgentStore:
    AgentDirectory + IdentityResolver + AgentLifecycle + ClaimStore + ActivityStore + AgentReads
{
}

impl<T> AgentStore for T where
    T: AgentDirectory + IdentityResolver + AgentLifecycle + ClaimStore + ActivityStore + AgentReads
{
}

/// How many agent ids the harness draws from.
pub const AGENTS: u8 = 6;

/// The `n`th agent id of the harness's pool.
pub fn agent(n: u8) -> AgentId {
    AgentId::from_ulid(0x0A6E_0000_0000_0000_0000_0000_0000_0000 | u128::from(n % AGENTS))
}

/// An id no operation ever creates.
pub fn unknown_agent() -> AgentId {
    AgentId::from_ulid(0x0A6E_0000_0000_0000_0000_0000_0000_FFFF)
}

/// The `n`th piece of identity evidence of the harness's pool.
pub fn evidence(n: u8) -> IdentityEvidence {
    let digest = Blake3::from_bytes([n; 32]);
    match n % 4 {
        0 => IdentityEvidence::HarnessAgent {
            scope: IdentityScope::Upstream(UpstreamId("upstream".to_owned())),
            agent: format!("harness-{n}"),
        },
        1 => IdentityEvidence::Account(AccountHash::from_keyed_digest(SecretVersion(1), digest)),
        2 => IdentityEvidence::StableCredential(CredentialHash::from_keyed_digest(
            SecretVersion(1),
            digest,
        )),
        _ => IdentityEvidence::RotatingCredential(CredentialHash::from_keyed_digest(
            SecretVersion(1),
            digest,
        )),
    }
}

/// The `n`th harness claim of the pool: five families, a few versions and
/// User-Agents, so claims repeat and tie.
pub fn claim(n: u8) -> HarnessClaim {
    let family = match n % 5 {
        0 => HarnessFamily::ClaudeCode,
        1 => HarnessFamily::Codex,
        2 => HarnessFamily::Pi,
        3 => HarnessFamily::OhMyPi,
        _ => HarnessFamily::Unknown,
    };
    HarnessClaim {
        family,
        version: n.is_multiple_of(2).then(|| format!("1.{}", n % 3)),
        user_agent: format!("agent/{}", n % 3),
    }
}

const LABELS: [&str; 3] = ["alpha", "Beta", "gamma beta"];

/// The `n`th label of the pool.
pub fn label(n: u8) -> Option<AgentLabel> {
    LABELS
        .get(usize::from(n) % LABELS.len())
        .and_then(|text| AgentLabel::new(text).ok())
}

fn operator(n: u8) -> OperatorId {
    OperatorId::from_ulid(0x0B0B_0000 | u128::from(n % 2))
}

/// One step of a generated sequence. Small integers index the pools above.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentOp {
    Create {
        agent: u8,
        evidence: u8,
        parent: Option<u8>,
        from_config: bool,
        label: Option<u8>,
        at: u64,
    },
    Advance {
        agent: u8,
        establish: bool,
        at: u64,
    },
    Attach {
        agent: u8,
        evidence: u8,
    },
    Merge {
        from: u8,
        into: u8,
        /// `None` for a resolver merge.
        operator: Option<u8>,
    },
    /// Revert the `merge`th merge made so far (an unknown id past the end).
    Unmerge {
        merge: u8,
        operator: u8,
    },
    Rename {
        agent: u8,
        label: Option<u8>,
        operator: u8,
    },
    Claim {
        agent: u8,
        claim: u8,
        at: u64,
    },
    Activity {
        agent: u8,
        at: u64,
    },
    List {
        filter: FilterSpec,
        size: u16,
    },
}

/// A generated agents filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterSpec {
    pub states: Vec<u8>,
    pub claimed: Vec<u8>,
    /// 0..3 a label word, 3 an agent id prefix, 4 an alias-free prefix.
    pub text: Option<u8>,
    pub parents: Vec<u8>,
}

impl FilterSpec {
    pub fn filter(&self) -> AgentFilter {
        let states = self
            .states
            .iter()
            .map(|n| match n % 3 {
                0 => CanonicalStateKind::Registered,
                1 => CanonicalStateKind::Provisional,
                _ => CanonicalStateKind::Established,
            })
            .collect();
        let claimed = self.claimed.iter().map(|n| claim(*n).family).collect();
        let text = self.text.and_then(|n| {
            let text = match n % 5 {
                0 => "a".to_owned(),
                1 => "BETA".to_owned(),
                2 => "gamma".to_owned(),
                3 => agent(n).ulid_text()[..24].to_ascii_lowercase(),
                _ => "0A6E".to_owned(),
            };
            AgentText::new(&text).ok()
        });
        AgentFilter {
            states,
            claimed,
            text,
            parents: self.parents.iter().map(|n| agent(*n)).collect(),
        }
    }
}

fn slot() -> impl Strategy<Value = u8> {
    0..AGENTS
}

fn filter_spec() -> impl Strategy<Value = FilterSpec> {
    (
        proptest::collection::vec(0u8..3, 0..3),
        proptest::collection::vec(0u8..5, 0..3),
        proptest::option::of(0u8..5),
        proptest::collection::vec(slot(), 0..3),
    )
        .prop_map(|(states, claimed, text, parents)| FilterSpec {
            states,
            claimed,
            text,
            parents,
        })
}

/// One generated operation.
pub fn agent_op() -> impl Strategy<Value = AgentOp> {
    prop_oneof![
        4 => (slot(), 0u8..8, proptest::option::of(slot()), any::<bool>(), proptest::option::of(0u8..3), 0u64..50)
            .prop_map(|(agent, evidence, parent, from_config, label, at)| AgentOp::Create {
                agent, evidence, parent, from_config, label, at,
            }),
        1 => (slot(), any::<bool>(), 0u64..50)
            .prop_map(|(agent, establish, at)| AgentOp::Advance { agent, establish, at }),
        1 => (slot(), 0u8..8).prop_map(|(agent, evidence)| AgentOp::Attach { agent, evidence }),
        4 => (slot(), slot(), proptest::option::of(0u8..2))
            .prop_map(|(from, into, operator)| AgentOp::Merge { from, into, operator }),
        3 => (0u8..5, 0u8..2).prop_map(|(merge, operator)| AgentOp::Unmerge { merge, operator }),
        1 => (slot(), proptest::option::of(0u8..3), 0u8..2)
            .prop_map(|(agent, label, operator)| AgentOp::Rename { agent, label, operator }),
        2 => (slot(), 0u8..6, 0u64..50).prop_map(|(agent, claim, at)| AgentOp::Claim { agent, claim, at }),
        2 => (slot(), 0u64..50).prop_map(|(agent, at)| AgentOp::Activity { agent, at }),
        1 => (filter_spec(), 1u16..4).prop_map(|(filter, size)| AgentOp::List { filter, size }),
    ]
}

/// The creation of every agent of the pool, each from config or traffic,
/// with a random parent, label and evidence, so later steps mostly act on
/// agents that exist.
fn population() -> impl Strategy<Value = Vec<AgentOp>> {
    proptest::collection::vec(
        (
            0u8..8,
            proptest::option::of(slot()),
            any::<bool>(),
            proptest::option::of(0u8..3),
            0u64..50,
        ),
        usize::from(AGENTS),
    )
    .prop_map(|params| {
        params
            .into_iter()
            .zip(0..AGENTS)
            .map(
                |((evidence, parent, from_config, label, at), agent)| AgentOp::Create {
                    agent,
                    evidence,
                    parent,
                    from_config,
                    label,
                    at,
                },
            )
            .collect()
    })
}

/// Generated operation sequences: the pool's creation, then up to `max`
/// random steps.
pub fn agent_ops(max: usize) -> impl Strategy<Value = Vec<AgentOp>> {
    (
        population(),
        proptest::collection::vec(agent_op(), 1..=max.max(1)),
    )
        .prop_map(|(mut ops, steps)| {
            ops.extend(steps);
            ops
        })
}

/// Run the harness: the store `make` builds must agree with
/// [`MemoryAgents`] on every generated sequence. A failure is a
/// [`ModelMismatch`] with the shrunk sequence.
pub fn check_agent_store<S, F>(config: HarnessConfig, make: F) -> Result<(), ModelMismatch>
where
    S: AgentStore,
    F: Fn(IdSequence, Outbox) -> S,
{
    check_agent_store_with(config, |ids, outbox| std::future::ready(make(ids, outbox)))
}

/// [`check_agent_store`] with a store built asynchronously, inside the
/// case's runtime: a Postgres store connects (and empties its tables)
/// there, so its connections live and die with the case.
pub fn check_agent_store_with<S, F, Fut>(
    config: HarnessConfig,
    make: F,
) -> Result<(), ModelMismatch>
where
    S: AgentStore,
    F: Fn(IdSequence, Outbox) -> Fut,
    Fut: Future<Output = S>,
{
    run(config, agent_ops(config.max_ops), |runtime, ops| {
        runtime.block_on(async {
            let (sut_outbox, sut_events) = Outbox::channel();
            let sut = make(IdSequence::default(), sut_outbox).await;
            run_case(sut, sut_events, ops).await
        })
    })
}

/// Every operation's result, compared between the two stores.
async fn run_case<S: AgentStore>(
    mut sut: S,
    mut sut_events: tokio::sync::mpsc::UnboundedReceiver<BusEvent>,
    ops: &[AgentOp],
) -> Result<(), Divergence> {
    let (model_outbox, mut model_events) = Outbox::channel();
    let mut model = MemoryAgents::new(IdSequence::default(), model_outbox);
    let mut merges: Vec<MergeId> = Vec::new();
    let mut before = states(0, &sut).await?;
    for (step, op) in ops.iter().enumerate() {
        let at = Timestamp::from_micros(1_000 + 10 * step as u64);
        apply(step, op, at, &mut sut, &mut model, &mut merges).await?;
        compare_events(step, drain(&mut sut_events), drain(&mut model_events))?;
        observe(step, &sut, &model).await?;
        let after = states(step, &sut).await?;
        check_invariants(step, op, &merges, &before, &after)?;
        before = after;
    }
    Ok(())
}

async fn apply<S: AgentStore>(
    step: usize,
    op: &AgentOp,
    at: Timestamp,
    sut: &mut S,
    model: &mut MemoryAgents,
    merges: &mut Vec<MergeId>,
) -> Result<(), Divergence> {
    match op {
        AgentOp::Create {
            agent: n,
            evidence: e,
            parent,
            from_config,
            label: l,
            at: t,
        } => {
            let at = Timestamp::from_micros(*t);
            let new = NewAgent {
                id: agent(*n),
                evidence: NonEmpty::new(evidence(*e)),
                parent: parent.map(agent),
                origin: if *from_config {
                    AgentOrigin::Config { at }
                } else {
                    AgentOrigin::Traffic { first_seen: at }
                },
                label: l.and_then(label),
            };
            let s = AgentLifecycle::create(sut, new.clone()).await;
            let m = AgentLifecycle::create(model, new).await;
            same(step, "create", &s, &m)
        }
        AgentOp::Advance {
            agent: n,
            establish,
            at: t,
        } => {
            let at = Timestamp::from_micros(*t);
            let advance = if *establish {
                Advance::Establish { since: at }
            } else {
                Advance::FirstTraffic { at }
            };
            let s = AgentLifecycle::advance(sut, agent(*n), advance).await;
            let m = AgentLifecycle::advance(model, agent(*n), advance).await;
            same(step, "advance", &s, &m)
        }
        AgentOp::Attach {
            agent: n,
            evidence: e,
        } => {
            let s = AgentLifecycle::attach_evidence(sut, agent(*n), evidence(*e)).await;
            let m = AgentLifecycle::attach_evidence(model, agent(*n), evidence(*e)).await;
            same(step, "attach_evidence", &s, &m)
        }
        AgentOp::Merge {
            from,
            into,
            operator: by,
        } => {
            let author = by.map_or(MergeAuthor::Resolver, |n| {
                MergeAuthor::Operator(operator(n))
            });
            let Ok(request) = MergeRequest::new(agent(*from), agent(*into), author) else {
                return Ok(());
            };
            let s = IdentityResolver::merge(sut, request, at).await;
            let m = IdentityResolver::merge(model, request, at).await;
            if let Ok(record) = &m {
                merges.push(record.id());
            }
            same(step, "merge", &s, &m)
        }
        AgentOp::Unmerge {
            merge,
            operator: by,
        } => {
            let id = merges
                .get(usize::from(*merge))
                .copied()
                .unwrap_or(MergeId::from_ulid(0xDEAD));
            let s = IdentityResolver::unmerge(sut, id, operator(*by), at).await;
            let m = IdentityResolver::unmerge(model, id, operator(*by), at).await;
            same(step, "unmerge", &s, &m)
        }
        AgentOp::Rename {
            agent: n,
            label: l,
            operator: by,
        } => {
            let label = l.and_then(label);
            let s = IdentityResolver::rename(sut, agent(*n), label.clone(), operator(*by)).await;
            let m = IdentityResolver::rename(model, agent(*n), label, operator(*by)).await;
            same(step, "rename", &s, &m)
        }
        AgentOp::Claim {
            agent: n,
            claim: c,
            at: t,
        } => {
            let at = Timestamp::from_micros(*t);
            let s = ClaimStore::record(sut, agent(*n), &claim(*c), at).await;
            let m = ClaimStore::record(model, agent(*n), &claim(*c), at).await;
            same(step, "claim record", &s, &m)
        }
        AgentOp::Activity { agent: n, at: t } => {
            let at = Timestamp::from_micros(*t);
            let s = ActivityStore::record(sut, agent(*n), at).await;
            let m = ActivityStore::record(model, agent(*n), at).await;
            same(step, "activity record", &s, &m)
        }
        AgentOp::List { filter, size } => {
            let filter = filter.filter();
            let s = traverse(sut, &filter, *size).await;
            let m = traverse(model, &filter, *size).await;
            same(step, "filtered list", &s, &m)
        }
    }
}

/// Every page of a full traversal, as item lists.
pub async fn traverse<S: AgentReads>(
    store: &S,
    filter: &AgentFilter,
    size: u16,
) -> Result<Vec<Vec<AgentProfile>>, AgentReadError> {
    let size = PageSize::new(size).map_err(|error| AgentReadError::Store {
        reason: format!("{error:?}"),
    })?;
    let mut pages = Vec::new();
    let mut after: Option<Cursor<AgentList>> = None;
    loop {
        let page = store.list(filter, &PageRequest { size, after }).await?;
        let (items, next) = page.into_parts();
        pages.push(items);
        match next {
            Some(next) => after = Some(next),
            None => return Ok(pages),
        }
    }
}

fn split(events: Vec<BusEvent>) -> (Vec<BusEvent>, HashSet<Changed>) {
    let mut others = Vec::new();
    let mut changed = HashSet::new();
    for event in events {
        match event {
            BusEvent::Changed(change) => {
                changed.insert(change);
            }
            other => others.push(other),
        }
    }
    (others, changed)
}

fn compare_events(step: usize, sut: Vec<BusEvent>, model: Vec<BusEvent>) -> Result<(), Divergence> {
    let (sut_events, sut_changed) = split(sut);
    let (model_events, model_changed) = split(model);
    same(step, "published events", &sut_events, &model_events)?;
    let missing: Vec<&Changed> = model_changed.difference(&sut_changed).collect();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(Divergence::new(
            step,
            format!("change notifications missing: {missing:?}"),
        ))
    }
}

async fn observe<S: AgentStore>(
    step: usize,
    sut: &S,
    model: &MemoryAgents,
) -> Result<(), Divergence> {
    let ids: Vec<AgentId> = (0..AGENTS).map(agent).chain([unknown_agent()]).collect();
    for id in &ids {
        same(
            step,
            "canonical",
            &sut.canonical(*id),
            &model.canonical(*id),
        )?;
        same(
            step,
            "claims",
            &ClaimStore::claims(sut, *id).await,
            &ClaimStore::claims(model, *id).await,
        )?;
        same(
            step,
            "last_seen",
            &ActivityStore::last_seen(sut, *id).await,
            &ActivityStore::last_seen(model, *id).await,
        )?;
        same(
            step,
            "cluster",
            &sut.cluster(*id).await,
            &model.cluster(*id).await,
        )?;
    }
    let batch = IdBatch::new(ids.iter().copied())
        .map_err(|error| Divergence::new(step, format!("id batch: {error:?}")))?;
    same(
        step,
        "names",
        &sut.names(&batch).await,
        &model.names(&batch).await,
    )?;
    let filter = AgentFilter::default();
    same(
        step,
        "agents list",
        &traverse(sut, &filter, 2).await,
        &traverse(model, &filter, 2).await,
    )?;
    let foreign = Cursor::from_token("never-issued".to_owned())
        .map_err(|error| Divergence::new(step, format!("cursor: {error:?}")))?;
    let size =
        PageSize::new(2).map_err(|error| Divergence::new(step, format!("size: {error:?}")))?;
    let request = PageRequest {
        size,
        after: Some(foreign),
    };
    same(
        step,
        "foreign cursor",
        &sut.list(&filter, &request).await.map(|_| ()),
        &Err(AgentReadError::InvalidCursor),
    )
}

/// The store's agent records and merge log, read back through `cluster`.
struct Snapshot {
    agents: BTreeMap<AgentId, Agent>,
    merges: BTreeMap<MergeId, MergeRecord>,
}

async fn states<S: AgentReads>(step: usize, store: &S) -> Result<Snapshot, Divergence> {
    let mut snapshot = Snapshot {
        agents: BTreeMap::new(),
        merges: BTreeMap::new(),
    };
    for n in 0..AGENTS {
        let cluster = store
            .cluster(agent(n))
            .await
            .map_err(|error| Divergence::new(step, format!("cluster read failed: {error:?}")))?;
        if let Some(cluster) = cluster {
            for record in std::iter::once(cluster.agent()).chain(cluster.aliases()) {
                snapshot.agents.insert(record.id, record.clone());
            }
            for record in cluster.merges() {
                snapshot.merges.insert(record.id(), record.clone());
            }
        }
    }
    Ok(snapshot)
}

fn check_invariants(
    step: usize,
    op: &AgentOp,
    merges: &[MergeId],
    before: &Snapshot,
    after: &Snapshot,
) -> Result<(), Divergence> {
    for agent in after.agents.values() {
        if let AgentState::Merged(merged) = &agent.state {
            let target = after.agents.get(&merged.into).map(|target| &target.state);
            if !matches!(
                target,
                Some(
                    AgentState::Registered { .. }
                        | AgentState::Provisional { .. }
                        | AgentState::Established { .. }
                )
            ) {
                return Err(Divergence::new(
                    step,
                    format!(
                        "{:?} is merged into {:?}, which is not an active agent",
                        agent.id, merged.into
                    ),
                ));
            }
        }
        let naming: Vec<&MergeRecord> = after
            .merges
            .values()
            .filter(|record| record.source() == agent.id && record.reverted().is_none())
            .collect();
        let agrees = match (&agent.state, naming.as_slice()) {
            (AgentState::Merged(merged), [record]) => merged.merge == record.id(),
            (AgentState::Merged(_), _) => false,
            (_, records) => records.is_empty(),
        };
        if !agrees {
            return Err(Divergence::new(
                step,
                format!("{:?}'s state disagrees with the merge log", agent.id),
            ));
        }
        if let Some(previous) = before.agents.get(&agent.id)
            && !legal(op, merges, &previous.state, &agent.state)
        {
            return Err(Divergence::new(
                step,
                format!(
                    "illegal transition of {:?}: {:?} to {:?} on {op:?}",
                    agent.id, previous.state, agent.state
                ),
            ));
        }
    }
    Ok(())
}

/// Whether `op` may move an agent from `from` to `to`
/// (`reconstruct.agent-state.legal-transitions`).
fn legal(op: &AgentOp, merges: &[MergeId], from: &AgentState, to: &AgentState) -> bool {
    match (from, to) {
        (AgentState::Merged(_), AgentState::Merged(_)) => true,
        (AgentState::Registered { .. }, AgentState::Provisional { .. }) => {
            matches!(
                op,
                AgentOp::Advance {
                    establish: false,
                    ..
                }
            )
        }
        (AgentState::Provisional { .. }, AgentState::Established { .. }) => {
            matches!(
                op,
                AgentOp::Advance {
                    establish: true,
                    ..
                }
            )
        }
        (_, AgentState::Merged(merged)) => {
            matches!(op, AgentOp::Merge { .. })
                && merges.last() == Some(&merged.merge)
                && from.active() == Ok(merged.prior)
        }
        (AgentState::Merged(merged), restored) => {
            matches!(op, AgentOp::Unmerge { merge, .. }
                if merges.get(usize::from(*merge)) == Some(&merged.merge))
                && AgentState::from(merged.prior) == *restored
        }
        (from, to) => from == to,
    }
}
