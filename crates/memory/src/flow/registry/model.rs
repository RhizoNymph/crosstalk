//! Model-based property harness for L5 channel registries.
//!
//! [`check_channel_registry`] generates random sequences of [`RegistryOp`]s
//! over small pools of channels, locators, patterns and agents (so patterns
//! overlap, promotions supersede and policies race), runs each sequence on
//! the registry under test and on [`MemoryChannels`], and after every step
//! requires:
//!
//! - equal results from the operation itself (lookups, declarations,
//!   policy decisions, promotions, coverage previews, traffic writes);
//! - equal published events, except that the store under test must
//!   announce at least the `Changed` notifications the reference does;
//! - equal observations: every stored channel (a full `ChannelReads::channels`
//!   traversal), `channel` of every channel id the case knows,
//!   `canonical` and `policy_history` of every channel id the case knows,
//!   `lookup` of every pooled locator, and a full `resource_use` traversal
//!   of every channel in pages of 2;
//! - three invariants of the registry under test: declared patterns never
//!   overlap (`flow.registry.declared-patterns-disjoint`), each channel's
//!   policy is its history's current one
//!   (`flow.policy.current-is-history-latest`), and supersession resolves in
//!   one step to a declared channel (`flow.channel.supersession-one-step`).
//!
//! `make` builds the registry under test from the agent directory it must
//! resolve agents through (a [`MemoryAgents`] in which agent 3 is merged
//! into agent 2), the [`IdSequence`] it must draw declared channel ids from
//! (one per accepted declaration) and the outbox for its events.
//! Declarations are dated by the time passed to `declare`.

use std::collections::HashSet;

use crosstalk_spec::aggregates::access::ResourceUse;
use crosstalk_spec::derived::flow::access::{Access, AccessOp, Extraction};
use crosstalk_spec::derived::flow::channel::Channel;
use crosstalk_spec::derived::flow::channel::Declaration;
use crosstalk_spec::derived::flow::channel::detection::TrafficDetection;
use crosstalk_spec::derived::flow::channel::policy::{
    Decision, Policy, PolicyAuthor, PolicyDecision, PolicyKind,
};
use crosstalk_spec::derived::flow::channel::promotion::Promotion;
use crosstalk_spec::derived::flow::resource::{Host, Locator, Resource, ResourcePattern};
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::ids::{
    AccessId, AgentId, ChannelId, ExchangeId, MessageHash, OperatorId, ResourceId, TransmissionId,
};
use crosstalk_spec::interfaces::l3_reconstruction::IdentityResolver;
use crosstalk_spec::interfaces::l3_reconstruction::lifecycle::{
    AgentLifecycle, AgentOrigin, NewAgent,
};
use crosstalk_spec::interfaces::l5_flow::channels::{
    ChannelReads, ChannelTraffic, DetectionUpdate,
};
use crosstalk_spec::interfaces::l5_flow::{ChannelDirectory, ChannelRegistry, RegistryError};
use crosstalk_spec::interfaces::l8_surface::lists::{ChannelFilter, OriginFilter};
use crosstalk_spec::observed::agent::{MergeAuthor, MergeRequest};
use crosstalk_spec::observed::message::{PartRef, ToolName};
use crosstalk_spec::paging::{Cursor, PageRequest, PageSize, ResourceUseList};
use crosstalk_spec::support::{Blake3, NonEmpty, TimeWindow, Timestamp};
use proptest::prelude::*;

use super::MemoryChannels;
use crate::model::{Divergence, HarnessConfig, ModelMismatch, run, same};
use crate::reconstruct::MemoryAgents;
use crate::support::{IdSequence, Outbox, drain};

/// Every spec trait a registry implements.
pub trait ChannelStore: ChannelRegistry + ChannelTraffic + ChannelReads + ChannelDirectory {}

impl<T: ChannelRegistry + ChannelTraffic + ChannelReads + ChannelDirectory> ChannelStore for T {}

/// Every stored channel, ascending by id: a full traversal of
/// `ChannelReads::channels` with a filter that keeps every channel, in
/// pages of 2.
pub async fn all_channels<S: ChannelReads>(store: &S) -> Result<Vec<Channel>, RegistryError> {
    let filter = ChannelFilter {
        origin: OriginFilter::WithSuperseded(Vec::new()),
        ..ChannelFilter::default()
    };
    let size = PageSize::new(2).map_err(|error| RegistryError::Store {
        reason: format!("{error:?}"),
    })?;
    let mut request = PageRequest { size, after: None };
    let mut channels = Vec::new();
    loop {
        let (items, next) = store.channels(&filter, &request).await?.into_parts();
        channels.extend(items);
        match next {
            Some(cursor) => request.after = Some(cursor),
            None => break,
        }
    }
    channels.reverse();
    Ok(channels)
}

/// Discovered channel ids the harness picks; declared ones come from the
/// registry.
pub const POOL: u8 = 4;

pub fn channel(n: u8) -> ChannelId {
    ChannelId::from_ulid(0x0C4A_0000 | u128::from(n % POOL))
}

/// An id no channel has.
pub fn unknown_channel() -> ChannelId {
    ChannelId::from_ulid(0x0C4A_FFFF)
}

/// How many locators (and resources) the pool holds.
pub const LOCATORS: u8 = 8;

fn url(host: &str, path: &str) -> Locator {
    Locator::Url {
        scheme: "https".to_owned(),
        host: Host(host.to_owned()),
        path: path.to_owned(),
        query: None,
    }
}

/// The `n`th locator: URLs under two hosts with nested paths, files under
/// one directory, an MCP target and an opaque tool key.
pub fn locator(n: u8) -> Locator {
    match n % LOCATORS {
        0 => url("wiki.example", "/a"),
        1 => url("wiki.example", "/a/b"),
        2 => url("wiki.example", "/b"),
        3 => url("other.example", "/a"),
        4 => Locator::File {
            host: None,
            path: "/shared/x".to_owned(),
        },
        5 => Locator::File {
            host: None,
            path: "/shared/y".to_owned(),
        },
        6 => Locator::Mcp {
            server: "notes".to_owned(),
            tool: ToolName("read".to_owned()),
            target: Some("t1".to_owned()),
        },
        _ => Locator::Opaque {
            tool: ToolName("bash".to_owned()),
            key: "k".to_owned(),
        },
    }
}

/// The resource of the `n`th locator.
pub fn resource(n: u8) -> Resource {
    let n = n % LOCATORS;
    Resource {
        id: ResourceId::from_ulid(0x4E50_0000 | u128::from(n)),
        locator: locator(n),
        first_seen: Timestamp::from_micros(u64::from(n)),
    }
}

/// The `n`th pattern: overlapping URL patterns, a file prefix, an exact
/// locator, an MCP server and a second host.
pub fn pattern(n: u8) -> ResourcePattern {
    match n % 6 {
        0 => ResourcePattern::Host(Host("wiki.example".to_owned())),
        1 => ResourcePattern::UrlPrefix {
            host: Host("wiki.example".to_owned()),
            path_prefix: "/a".to_owned(),
        },
        2 => ResourcePattern::PathPrefix {
            host: None,
            prefix: "/shared".to_owned(),
        },
        3 => ResourcePattern::Exact(locator(2)),
        4 => ResourcePattern::McpServer("notes".to_owned()),
        _ => ResourcePattern::Host(Host("other.example".to_owned())),
    }
}

/// Agents of the accesses; agent 3 is merged into agent 2 in the directory
/// every registry resolves through.
pub fn access_agent(n: u8) -> AgentId {
    AgentId::from_ulid(0x0A6E_0000 | u128::from(n % 4))
}

fn operator(n: u8) -> OperatorId {
    OperatorId::from_ulid(0x0B0B_0000 | u128::from(n % 2))
}

fn kind(n: u8) -> PolicyKind {
    match n % 3 {
        0 => PolicyKind::Unreviewed,
        1 => PolicyKind::Sanctioned,
        _ => PolicyKind::Unsanctioned,
    }
}

/// A policy decision of kind `n` at `at`, by an operator or config.
pub fn decision(n: u8, at: u64, by_config: bool, note: bool) -> PolicyDecision {
    PolicyDecision {
        kind: kind(n),
        decision: Decision {
            by: if by_config {
                PolicyAuthor::Config
            } else {
                PolicyAuthor::Operator(operator(n))
            },
            at: Timestamp::from_micros(at),
            note: note.then(|| "reviewed".to_owned()),
        },
    }
}

/// The directory both registries resolve agents through.
pub async fn directory() -> Result<MemoryAgents, Divergence> {
    let mut agents = MemoryAgents::default();
    for n in 0..4 {
        let agent = NewAgent {
            id: access_agent(n),
            evidence: NonEmpty::new(crate::reconstruct::model::evidence(n)),
            parent: None,
            origin: AgentOrigin::Traffic {
                first_seen: Timestamp::from_micros(1),
            },
            label: None,
        };
        agents
            .create(agent)
            .await
            .map_err(|error| Divergence::new(0, format!("directory: {error:?}")))?;
    }
    let request = MergeRequest::new(access_agent(3), access_agent(2), MergeAuthor::Resolver)
        .map_err(|_| Divergence::new(0, "directory: self merge"))?;
    agents
        .merge(request, Timestamp::from_micros(2))
        .await
        .map_err(|error| Divergence::new(0, format!("directory: {error:?}")))?;
    Ok(agents)
}

/// One step. Channel slots below [`POOL`] name pool ids; above, the
/// declared channels in order of declaration (an unknown id past the end).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryOp {
    Declare {
        pattern: u8,
        policy: Option<(u8, u64)>,
        config: bool,
    },
    Discover {
        channel: u8,
        resource: u8,
    },
    AddResource {
        channel: u8,
        resource: u8,
    },
    Access {
        resource: u8,
        agent: u8,
        write: bool,
        at: u64,
    },
    SetPolicy {
        channel: u8,
        kind: u8,
        at: u64,
        config: bool,
        note: bool,
    },
    Promote {
        channel: u8,
        pattern: u8,
        kind: u8,
        note: bool,
    },
    Coverage {
        channel: u8,
        pattern: u8,
    },
    Lookup {
        locator: u8,
    },
    Detect {
        channel: u8,
        update: u8,
        at: u64,
    },
    Confirm {
        channel: u8,
        transmission: u8,
        at: u64,
    },
    Uses {
        channel: u8,
        start: u64,
        len: u64,
        size: u16,
    },
}

fn slot() -> impl Strategy<Value = u8> {
    0u8..(POOL + 3)
}

/// One generated operation.
pub fn registry_op() -> impl Strategy<Value = RegistryOp> {
    prop_oneof![
        1 => (0u8..6, proptest::option::of((0u8..3, 0u64..40)), any::<bool>())
            .prop_map(|(pattern, policy, config)| RegistryOp::Declare { pattern, policy, config }),
        4 => (0u8..POOL, 0u8..LOCATORS)
            .prop_map(|(channel, resource)| RegistryOp::Discover { channel, resource }),
        2 => (slot(), 0u8..LOCATORS)
            .prop_map(|(channel, resource)| RegistryOp::AddResource { channel, resource }),
        4 => (0u8..LOCATORS, 0u8..4, any::<bool>(), 0u64..60)
            .prop_map(|(resource, agent, write, at)| RegistryOp::Access { resource, agent, write, at }),
        3 => (slot(), 0u8..3, 0u64..40, any::<bool>(), any::<bool>())
            .prop_map(|(channel, kind, at, config, note)| RegistryOp::SetPolicy { channel, kind, at, config, note }),
        5 => (0u8..POOL, 0u8..6, 0u8..3, any::<bool>())
            .prop_map(|(channel, pattern, kind, note)| RegistryOp::Promote { channel, pattern, kind, note }),
        2 => (slot(), 0u8..6).prop_map(|(channel, pattern)| RegistryOp::Coverage { channel, pattern }),
        1 => (0u8..LOCATORS).prop_map(|locator| RegistryOp::Lookup { locator }),
        2 => (slot(), 0u8..5, 0u64..60)
            .prop_map(|(channel, update, at)| RegistryOp::Detect { channel, update, at }),
        2 => (slot(), 0u8..4, 0u64..60)
            .prop_map(|(channel, transmission, at)| RegistryOp::Confirm { channel, transmission, at }),
        1 => (slot(), 0u64..40, 1u64..40, 1u16..4)
            .prop_map(|(channel, start, len, size)| RegistryOp::Uses { channel, start, len, size }),
    ]
}

/// The discovery of every pool channel, each seeded by a random resource
/// (a repeat is refused), so later steps mostly act on stored channels.
fn population() -> impl Strategy<Value = Vec<RegistryOp>> {
    proptest::collection::vec(0u8..LOCATORS, usize::from(POOL)).prop_map(|resources| {
        resources
            .into_iter()
            .zip(0..POOL)
            .map(|(resource, channel)| RegistryOp::Discover { channel, resource })
            .collect()
    })
}

/// Generated sequences: the pool's discovery, then up to `max` random
/// steps.
pub fn registry_ops(max: usize) -> impl Strategy<Value = Vec<RegistryOp>> {
    (
        population(),
        proptest::collection::vec(registry_op(), 1..=max.max(1)),
    )
        .prop_map(|(mut ops, steps)| {
            ops.extend(steps);
            ops
        })
}

/// Run the harness: the registry `make` builds must agree with
/// [`MemoryChannels`] on every generated sequence. A failure is a
/// [`ModelMismatch`] with the shrunk sequence.
pub fn check_channel_registry<S, F>(config: HarnessConfig, make: F) -> Result<(), ModelMismatch>
where
    S: ChannelStore,
    F: Fn(MemoryAgents, IdSequence, Outbox) -> S,
{
    run(config, registry_ops(config.max_ops), |runtime, ops| {
        runtime.block_on(async {
            let agents = directory().await?;
            let (sut_outbox, sut_events) = Outbox::channel();
            let sut = make(agents.clone(), IdSequence::default(), sut_outbox);
            run_case(sut, sut_events, agents, ops).await
        })
    })
}

struct Case {
    declared: Vec<ChannelId>,
    accesses: u128,
}

impl Case {
    fn channel(&self, slot: u8) -> ChannelId {
        if slot < POOL {
            return channel(slot);
        }
        self.declared
            .get(usize::from(slot - POOL))
            .copied()
            .unwrap_or_else(unknown_channel)
    }

    fn known(&self) -> Vec<ChannelId> {
        (0..POOL)
            .map(channel)
            .chain(self.declared.iter().copied())
            .chain([unknown_channel()])
            .collect()
    }
}

async fn run_case<S: ChannelStore>(
    mut sut: S,
    mut sut_events: tokio::sync::mpsc::UnboundedReceiver<BusEvent>,
    agents: MemoryAgents,
    ops: &[RegistryOp],
) -> Result<(), Divergence> {
    let (model_outbox, mut model_events) = Outbox::channel();
    let mut model = MemoryChannels::new(agents, IdSequence::default(), model_outbox);
    let mut case = Case {
        declared: Vec::new(),
        accesses: 0,
    };
    for (step, op) in ops.iter().enumerate() {
        let now = Timestamp::from_micros(100 + step as u64);
        apply(step, now, op, &mut case, &mut sut, &mut model).await?;
        compare_events(step, drain(&mut sut_events), drain(&mut model_events))?;
        observe(step, &case, &sut, &model).await?;
        check_invariants(step, &case, &sut).await?;
    }
    Ok(())
}

fn access(id: u128, resource: u8, agent: u8, write: bool, at: u64) -> Access {
    let part = PartRef {
        message: MessageHash::from_digest(Blake3::from_bytes([resource; 32])),
        index: 0,
    };
    Access {
        id: AccessId::from_ulid(0xACC0_0000 | id),
        agent: access_agent(agent),
        exchange: ExchangeId::from_ulid(0xE0 | id),
        resource: self::resource(resource).id,
        at: Timestamp::from_micros(at),
        via: Extraction::Structured,
        op: if write {
            AccessOp::Write {
                call: part,
                spans: Vec::new(),
            }
        } else {
            AccessOp::Read { result: part }
        },
    }
}

fn update(n: u8, at: u64) -> DetectionUpdate {
    let at = Timestamp::from_micros(at);
    let transmission = TransmissionId::from_ulid(0x7A00 | u128::from(n));
    let first_access = AccessId::from_ulid(0xACC0_0000);
    match n % 5 {
        0 => DetectionUpdate::Unused { since: at },
        1 => DetectionUpdate::Traffic(TrafficDetection::Observed { first_access }),
        2 => DetectionUpdate::Traffic(TrafficDetection::Active {
            since: at,
            last_transmission: transmission,
        }),
        3 => DetectionUpdate::Traffic(TrafficDetection::Dormant {
            since: at,
            last_transmission: transmission,
        }),
        _ => DetectionUpdate::Unused {
            since: Timestamp::from_micros(0),
        },
    }
}

async fn apply<S: ChannelStore>(
    step: usize,
    now: Timestamp,
    op: &RegistryOp,
    case: &mut Case,
    sut: &mut S,
    model: &mut MemoryChannels<MemoryAgents>,
) -> Result<(), Divergence> {
    match op {
        RegistryOp::Declare {
            pattern: p,
            policy,
            config,
        } => {
            let policy = policy.map_or(Policy::Unreviewed(None), |(n, at)| {
                decision(n, at, *config, false).policy()
            });
            let by = if *config {
                PolicyAuthor::Config
            } else {
                PolicyAuthor::Operator(operator(0))
            };
            let s = sut.declare(pattern(*p), policy.clone(), by, now).await;
            let m = model.declare(pattern(*p), policy, by, now).await;
            if let Ok(id) = m {
                case.declared.push(id);
            }
            same(step, "declare", &s, &m)
        }
        RegistryOp::Discover {
            channel: c,
            resource: r,
        } => {
            case.accesses += 1;
            let first = AccessId::from_ulid(0xF1A0_0000 | case.accesses);
            let s = sut.discover(channel(*c), resource(*r), first).await;
            let m = model.discover(channel(*c), resource(*r), first).await;
            same(step, "discover", &s, &m)
        }
        RegistryOp::AddResource {
            channel: c,
            resource: r,
        } => {
            let id = case.channel(*c);
            let s = sut.add_resource(id, resource(*r)).await;
            let m = model.add_resource(id, resource(*r)).await;
            same(step, "add_resource", &s, &m)
        }
        RegistryOp::Access {
            resource: r,
            agent,
            write,
            at,
        } => {
            case.accesses += 1;
            let access = access(case.accesses, *r, *agent, *write, *at);
            let s = sut.record_access(access.clone()).await;
            let m = model.record_access(access).await;
            same(step, "record_access", &s, &m)
        }
        RegistryOp::SetPolicy {
            channel: c,
            kind: k,
            at,
            config,
            note,
        } => {
            let id = case.channel(*c);
            let decision = decision(*k, *at, *config, *note);
            let s = sut.set_policy(id, decision.clone()).await;
            let m = model.set_policy(id, decision).await;
            same(step, "set_policy", &s, &m)
        }
        RegistryOp::Promote {
            channel: c,
            pattern: p,
            kind: k,
            note,
        } => {
            let id = case.channel(*c);
            let at = now;
            let promotion = Promotion::new(
                pattern(*p),
                kind(*k),
                operator(1),
                at,
                note.then(|| "promoted".to_owned()),
            );
            let s = sut.promote(id, promotion.clone()).await;
            let m = model.promote(id, promotion).await;
            same(step, "promote", &s, &m)
        }
        RegistryOp::Coverage {
            channel: c,
            pattern: p,
        } => {
            let id = case.channel(*c);
            let declaration = Declaration {
                pattern: pattern(*p),
                by: PolicyAuthor::Operator(operator(1)),
                at: now,
            };
            let s = sut.promotion_coverage(id, &declaration).await;
            let m = model.promotion_coverage(id, &declaration).await;
            same(step, "promotion_coverage", &s, &m)
        }
        RegistryOp::Lookup { locator: l } => {
            let s = sut.lookup(&locator(*l)).await;
            let m = model.lookup(&locator(*l)).await;
            same(step, "lookup", &s, &m)
        }
        RegistryOp::Detect {
            channel: c,
            update: u,
            at,
        } => {
            let id = case.channel(*c);
            let s = sut.set_detection(id, update(*u, *at)).await;
            let m = model.set_detection(id, update(*u, *at)).await;
            same(step, "set_detection", &s, &m)
        }
        RegistryOp::Confirm {
            channel: c,
            transmission,
            at,
        } => {
            let id = case.channel(*c);
            let transmission = TransmissionId::from_ulid(0x7A00 | u128::from(*transmission));
            let at = Timestamp::from_micros(*at);
            let s = sut.confirm(id, transmission, at).await;
            let m = model.confirm(id, transmission, at).await;
            same(step, "confirm", &s, &m)
        }
        RegistryOp::Uses {
            channel: c,
            start,
            len,
            size,
        } => {
            let id = case.channel(*c);
            let window = TimeWindow::new(
                Timestamp::from_micros(*start),
                Timestamp::from_micros(start + len),
            )
            .map_err(|_| Divergence::new(step, "window"))?;
            same(
                step,
                "resource_use",
                &traverse(sut, id, window, *size).await,
                &traverse(model, id, window, *size).await,
            )
        }
    }
}

/// A full `resource_use` traversal: the canonical channel of each page and
/// its rows.
pub async fn traverse<S: ChannelRegistry>(
    store: &S,
    channel: ChannelId,
    window: TimeWindow,
    size: u16,
) -> Result<Vec<(ChannelId, Vec<ResourceUse>)>, RegistryError> {
    let size = PageSize::new(size).map_err(|error| RegistryError::Store {
        reason: format!("{error:?}"),
    })?;
    let mut pages = Vec::new();
    let mut after: Option<Cursor<ResourceUseList>> = None;
    loop {
        let page = store
            .resource_use(channel, window, &PageRequest { size, after })
            .await?;
        let (items, next) = page.page.into_parts();
        pages.push((page.channel, items));
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

pub(crate) fn compare_events(
    step: usize,
    sut: Vec<BusEvent>,
    model: Vec<BusEvent>,
) -> Result<(), Divergence> {
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

async fn observe<S: ChannelStore>(
    step: usize,
    case: &Case,
    sut: &S,
    model: &MemoryChannels<MemoryAgents>,
) -> Result<(), Divergence> {
    same(
        step,
        "channels",
        &all_channels(sut).await,
        &all_channels(model).await,
    )?;
    let all_time = TimeWindow::new(Timestamp::from_micros(0), Timestamp::from_micros(10_000))
        .map_err(|_| Divergence::new(step, "window"))?;
    for id in case.known() {
        same(step, "canonical", &sut.canonical(id), &model.canonical(id))?;
        same(
            step,
            "channel",
            &sut.channel(id).await,
            &model.channel(id).await,
        )?;
        same(
            step,
            "policy_history",
            &sut.policy_history(id).await,
            &model.policy_history(id).await,
        )?;
        same(
            step,
            "resource_use traversal",
            &traverse(sut, id, all_time, 2).await,
            &traverse(model, id, all_time, 2).await,
        )?;
    }
    for n in 0..LOCATORS {
        same(
            step,
            "lookup of every locator",
            &sut.lookup(&locator(n)).await,
            &model.lookup(&locator(n)).await,
        )?;
    }
    Ok(())
}

async fn check_invariants<S: ChannelStore>(
    step: usize,
    case: &Case,
    sut: &S,
) -> Result<(), Divergence> {
    let channels: Vec<Channel> = all_channels(sut)
        .await
        .map_err(|error| Divergence::new(step, format!("channels: {error:?}")))?;
    let declared: Vec<(&Channel, &ResourcePattern)> = channels
        .iter()
        .filter_map(|channel| channel.origin.pattern().map(|pattern| (channel, pattern)))
        .collect();
    for (index, (a, p)) in declared.iter().enumerate() {
        if let Some((b, _)) = declared[index + 1..].iter().find(|(_, q)| p.overlaps(q)) {
            return Err(Divergence::new(
                step,
                format!("declared channels {:?} and {:?} overlap", a.id, b.id),
            ));
        }
    }
    for channel in &channels {
        let history = sut.policy_history(channel.id).await.map_err(|error| {
            Divergence::new(step, format!("history of {:?}: {error:?}", channel.id))
        })?;
        if channel.policy != history.current() {
            return Err(Divergence::new(
                step,
                format!("{:?}'s policy is not its history's current one", channel.id),
            ));
        }
    }
    for id in case.known() {
        let canonical = sut.canonical(id);
        if sut.canonical(canonical) != canonical {
            return Err(Divergence::new(
                step,
                format!("{id:?} resolves in more than one step"),
            ));
        }
        if canonical != id {
            let target = channels.iter().find(|channel| channel.id == canonical);
            if target
                .and_then(|channel| channel.origin.pattern())
                .is_none()
            {
                return Err(Divergence::new(
                    step,
                    format!("{id:?} resolves to {canonical:?}, which is not declared"),
                ));
            }
        }
    }
    Ok(())
}
