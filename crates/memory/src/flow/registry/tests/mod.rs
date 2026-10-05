//! Reference tests for the channel registry, one or more per invariant
//! that names `ChannelRegistry` or `ChannelDirectory`. Each test's doc
//! names the invariant it checks.

use crosstalk_spec::derived::flow::channel::detection::{DeclaredDetection, TrafficDetection};
use crosstalk_spec::derived::flow::channel::policy::{Policy, PolicyAuthor, Recorded};
use crosstalk_spec::derived::flow::channel::promotion::{
    Promotion, PromotionRefusal, coverage, plan,
};
use crosstalk_spec::derived::flow::channel::{
    Channel, ChannelOrigin, Declaration, DeclaredHistory, Seed,
};
use crosstalk_spec::derived::flow::transmission::{Route, Transmission};
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::ids::{AccessId, ChannelId, OperatorId, TransmissionId};
use crosstalk_spec::interfaces::l5_flow::{
    ChannelDirectory, ChannelLookup, ChannelRegistry, Discovery, PromoteError, RegistryError,
};
use crosstalk_spec::interfaces::l8_surface::lists::{ChannelFilter, OriginFilter};
use crosstalk_spec::support::{TimeWindow, Timestamp};
use tokio::sync::mpsc::UnboundedReceiver;

use super::MemoryChannels;
use super::model::{self, access_agent, channel, decision, directory, locator, pattern, resource};
use crate::flow::verdicts::model::state_between;
use crate::model::{HarnessConfig, ModelMismatch};
use crate::support::{IdSequence, Outbox, drain};
use crosstalk_spec::interfaces::l5_flow::channels::{
    ChannelReads, ChannelTraffic, DetectionUpdate, TrafficError,
};
use crosstalk_spec::support::Change;

mod traffic;

/// The case count the pipeline harnesses have always run with.
fn pipeline_harness() -> HarnessConfig {
    HarnessConfig {
        cases: 64,
        ..HarnessConfig::default()
    }
}
use crate::reconstruct::MemoryAgents;

fn at(micros: u64) -> Timestamp {
    Timestamp::from_micros(micros)
}

type Registry = MemoryChannels<MemoryAgents>;

async fn registry() -> (Registry, UnboundedReceiver<BusEvent>) {
    let Ok(agents) = directory().await else {
        panic!("directory");
    };
    let (outbox, events) = Outbox::channel();
    let registry = MemoryChannels::new(agents, IdSequence::default(), outbox);
    (registry, events)
}

fn seed_transmission(n: u8) -> TransmissionId {
    TransmissionId::from_ulid(0xF1A0_0000 | u128::from(n))
}

/// Store resource `r` (on whatever channel its lookup names, or none),
/// unless it is stored already.
async fn store_resource(registry: &mut Registry, r: u8) {
    match registry.add_resource(resource(r)).await {
        Ok(_) | Err(TrafficError::DuplicateResource(_)) => {}
        Err(error) => panic!("resource {r} refused: {error:?}"),
    }
}

/// A transmission `id` opened at `opened` through `routed`, in state
/// `state` (as numbered by the verdict harness), from agent 1 (agent 3,
/// merged into agent 2, when `merged`) to agent 2.
fn routed_transmission(
    id: TransmissionId,
    routed: ChannelId,
    state: u8,
    merged: bool,
    opened: u64,
) -> Transmission {
    let from = if merged {
        access_agent(3)
    } else {
        access_agent(1)
    };
    let Some(state) = state_between(state, &[0], from, access_agent(2)) else {
        panic!("state fixture");
    };
    Transmission {
        id,
        to: access_agent(2),
        route: Route::Channel(routed),
        opened_at: at(opened),
        state,
    }
}

/// Discover channel `c` from resource `r` by the cross-agent transmission
/// `seed_transmission(r)` opened at `r` µs, and record that transmission
/// (awaiting content) as the flow consumer does.
async fn discover(registry: &mut Registry, c: u8, r: u8) {
    store_resource(registry, r).await;
    assert_eq!(
        registry
            .discover(
                channel(c),
                resource(r).id,
                seed_transmission(r),
                at(u64::from(r))
            )
            .await,
        Ok(Discovery::Created(channel(c)))
    );
    let opened = routed_transmission(seed_transmission(r), channel(c), 1, false, u64::from(r));
    assert_eq!(
        registry.record_transmission(&opened).await,
        Ok(Change::Applied)
    );
}

fn promotion(p: u8, time: u64) -> Promotion {
    Promotion::new(
        pattern(p),
        crosstalk_spec::derived::flow::channel::policy::PolicyKind::Sanctioned,
        OperatorId::from_ulid(9),
        at(time),
        None,
    )
}

async fn stored(registry: &Registry, id: ChannelId) -> Channel {
    match registry.channel(id).await {
        Ok(Some(read)) => read.into_parts().0,
        other => panic!("channel {id:?} not stored: {other:?}"),
    }
}

// ---- lookups ------------------------------------------------------------------

/// `flow.registry.lookup-never-creates`: Known for a resource on a channel
/// (its canonical channel), else Declared for a matching pattern (a
/// resource stored on no channel included), else NoChannel; no lookup
/// creates a channel.
#[tokio::test]
async fn lookup_never_creates_a_channel() {
    let (mut registry, _events) = registry().await;
    discover(&mut registry, 0, 0).await;
    // Resource 4 is seen before any declaration: it is on no channel.
    assert_eq!(registry.add_resource(resource(4)).await, Ok(None));
    assert_eq!(
        registry.lookup(&locator(4)).await,
        Ok(ChannelLookup::NoChannel)
    );
    let Ok(declared) = registry
        .declare(
            pattern(2),
            Policy::Unreviewed(None),
            PolicyAuthor::Config,
            at(100),
        )
        .await
    else {
        panic!("declare");
    };
    assert_eq!(
        registry.lookup(&locator(0)).await,
        Ok(ChannelLookup::Known(channel(0)))
    );
    assert_eq!(
        registry.lookup(&locator(4)).await,
        Ok(ChannelLookup::Declared(declared))
    );
    assert_eq!(
        registry.lookup(&locator(6)).await,
        Ok(ChannelLookup::NoChannel)
    );
    let before = model::all_channels(&registry).await;
    // Looking every locator up creates nothing.
    for n in 0..model::LOCATORS {
        assert!(registry.lookup(&locator(n)).await.is_ok());
    }
    assert_eq!(model::all_channels(&registry).await, before);
    // Resource 4, stored on no channel, joins the declared channel on its
    // next sighting and is Known there.
    assert_eq!(registry.add_resource(resource(4)).await, Ok(Some(declared)));
    assert_eq!(
        registry.lookup(&locator(4)).await,
        Ok(ChannelLookup::Known(declared))
    );
}

/// `flow.registry.lookup-never-superseded`.
#[tokio::test]
async fn lookup_of_superseded_resource_is_known_on_superseder() {
    let (mut registry, _events) = registry().await;
    discover(&mut registry, 0, 0).await;
    discover(&mut registry, 1, 1).await;
    assert!(
        registry
            .promote(channel(0), promotion(1, 200))
            .await
            .is_ok()
    );
    assert_eq!(
        registry.lookup(&locator(1)).await,
        Ok(ChannelLookup::Known(channel(0)))
    );
    // A resource of the superseded channel is on a channel: it is stored
    // once.
    assert_eq!(
        registry.add_resource(resource(1)).await,
        Err(TrafficError::DuplicateResource(resource(1).id))
    );
}

// ---- declarations and policy ---------------------------------------------------

/// `flow.registry.declared-patterns-disjoint`, by declaration.
#[tokio::test]
async fn declared_patterns_never_overlap() {
    let (mut registry, _events) = registry().await;
    let Ok(host) = registry
        .declare(
            pattern(0),
            Policy::Unreviewed(None),
            PolicyAuthor::Config,
            at(100),
        )
        .await
    else {
        panic!("declare");
    };
    for overlapping in [0, 1, 3] {
        assert_eq!(
            registry
                .declare(
                    pattern(overlapping),
                    Policy::Unreviewed(None),
                    PolicyAuthor::Config,
                    at(100)
                )
                .await,
            Err(RegistryError::OverlappingDeclaration { existing: host })
        );
    }
    assert!(
        registry
            .declare(
                pattern(2),
                Policy::Unreviewed(None),
                PolicyAuthor::Config,
                at(100)
            )
            .await
            .is_ok()
    );
    // A promotion whose pattern overlaps a declared one is refused too.
    discover(&mut registry, 0, 3).await;
    assert_eq!(
        registry.promote(channel(0), promotion(5, 200)).await,
        Ok(crosstalk_spec::interfaces::l5_flow::Promoted {
            superseded: Vec::new(),
            policy: Recorded::Current
        })
    );
    discover(&mut registry, 1, 6).await;
    assert!(
        registry
            .promote(channel(1), promotion(4, 201))
            .await
            .is_ok()
    );
}

/// A declaration is dated by the time `declare` is given, and its decision
/// is the first entry of its history.
#[tokio::test]
async fn declare_records_the_initial_decision() {
    let (mut registry, _events) = registry().await;
    let first = decision(1, 10, true, false);
    let Ok(id) = registry
        .declare(pattern(4), first.policy(), PolicyAuthor::Config, at(150))
        .await
    else {
        panic!("declare");
    };
    let channel = stored(&registry, id).await;
    assert_eq!(
        channel.origin,
        ChannelOrigin::Declared {
            declaration: Declaration {
                pattern: pattern(4),
                by: PolicyAuthor::Config,
                at: at(150)
            },
            history: DeclaredHistory::BeforeTraffic(DeclaredDetection::AwaitingTraffic),
        }
    );
    let Ok(history) = registry.policy_history(id).await else {
        panic!("history");
    };
    assert_eq!(history.entries(), &[first]);
}

/// `flow.policy.current-is-history-latest`, including a late decision kept
/// behind a newer one and a redelivered one.
#[tokio::test]
async fn stored_policy_is_history_current() {
    let (mut registry, mut events) = registry().await;
    discover(&mut registry, 0, 0).await;
    drain(&mut events);
    let newer = decision(1, 20, false, false);
    let older = decision(2, 10, false, true);
    assert_eq!(
        registry.set_policy(channel(0), newer.clone()).await,
        Ok(Recorded::Current)
    );
    assert_eq!(
        registry.set_policy(channel(0), older.clone()).await,
        Ok(Recorded::Superseded)
    );
    assert_eq!(drain(&mut events).len(), 2);
    assert_eq!(
        registry.set_policy(channel(0), newer.clone()).await,
        Ok(Recorded::Duplicate)
    );
    assert!(drain(&mut events).is_empty());
    let Ok(history) = registry.policy_history(channel(0)).await else {
        panic!("history");
    };
    assert_eq!(history.entries(), &[older, newer.clone()]);
    assert_eq!(stored(&registry, channel(0)).await.policy, newer.policy());
    assert_eq!(
        stored(&registry, channel(0)).await.policy,
        history.current()
    );
}

/// `flow.policy.history-records-every-decision`, under redelivery: each
/// decision is in the history once, however often it is recorded.
#[tokio::test]
async fn policy_history_complete_under_redelivery() {
    let (mut registry, _events) = registry().await;
    discover(&mut registry, 0, 0).await;
    let decisions = [
        decision(0, 5, true, false),
        decision(1, 7, false, true),
        decision(2, 3, false, false),
    ];
    for _ in 0..3 {
        for decision in &decisions {
            assert!(
                registry
                    .set_policy(channel(0), decision.clone())
                    .await
                    .is_ok()
            );
        }
    }
    let Ok(history) = registry.policy_history(channel(0)).await else {
        panic!("history");
    };
    assert_eq!(history.entries().len(), decisions.len());
    for decision in &decisions {
        assert_eq!(
            history.entries().iter().filter(|e| *e == decision).count(),
            1
        );
    }
}

/// `flow.registry.superseded-takes-no-policy`.
#[tokio::test]
async fn set_policy_on_superseded_channel_refused() {
    let (mut registry, _events) = registry().await;
    discover(&mut registry, 0, 0).await;
    discover(&mut registry, 1, 1).await;
    assert!(
        registry
            .promote(channel(0), promotion(1, 200))
            .await
            .is_ok()
    );
    let Ok(before) = registry.policy_history(channel(1)).await else {
        panic!("history");
    };
    assert_eq!(
        registry
            .set_policy(channel(1), decision(1, 300, false, false))
            .await,
        Err(RegistryError::Superseded {
            channel: channel(1),
            by: channel(0)
        })
    );
    assert_eq!(registry.policy_history(channel(1)).await, Ok(before));
    assert_eq!(
        registry.policy_history(model::unknown_channel()).await,
        Err(RegistryError::UnknownChannel(model::unknown_channel()))
    );
}

// ---- promotion -----------------------------------------------------------------

/// `flow.registry.promote-applies-plan` and
/// `flow.registry.promote-publishes-once`.
#[tokio::test]
async fn promote_applies_plan() {
    let (mut registry, mut events) = registry().await;
    discover(&mut registry, 0, 0).await;
    discover(&mut registry, 1, 1).await;
    discover(&mut registry, 2, 2).await;
    drain(&mut events);
    let before = model::all_channels(&registry).await.unwrap();
    let expected = {
        let table = registry.state.read();
        plan(
            channel(0),
            promotion(1, 200).declaration(),
            &table.registered(),
        )
    };
    let Ok(expected) = expected else {
        panic!("plan refused");
    };
    let Ok(promoted) = registry.promote(channel(0), promotion(1, 200)).await else {
        panic!("promote refused");
    };
    assert_eq!(promoted.superseded, vec![channel(1)]);
    assert_eq!(promoted.policy, Recorded::Current);
    let after = stored(&registry, channel(0)).await;
    assert_eq!(after.origin, expected.origin);
    assert_eq!(after.resources, before[0].channel().resources);
    assert_eq!(after.policy, promotion(1, 200).decision().policy());
    for (id, origin) in &expected.superseded {
        assert_eq!(stored(&registry, *id).await.origin, *origin);
    }
    assert_eq!(&stored(&registry, channel(2)).await, before[2].channel());
    let published = drain(&mut events);
    let promoted_events: Vec<&BusEvent> = published
        .iter()
        .filter(|event| matches!(event, BusEvent::Detect(DetectEvent::ChannelPromoted { .. })))
        .collect();
    assert_eq!(
        promoted_events,
        vec![&BusEvent::Detect(DetectEvent::ChannelPromoted {
            channel: channel(0),
            declaration: promotion(1, 200).declaration().clone(),
            policy: promotion(1, 200).decision().clone(),
            superseded: vec![channel(1)],
        })]
    );
    for change in Changed::promotion(channel(0), &[channel(1)]) {
        assert!(published.contains(&BusEvent::Changed(change)));
    }
}

/// `flow.registry.promote-rejects-without-effect`: each refusal is plan's,
/// and changes and publishes nothing.
#[tokio::test]
async fn promote_rejections_change_nothing() {
    let (mut registry, mut events) = registry().await;
    discover(&mut registry, 0, 0).await;
    discover(&mut registry, 1, 1).await;
    assert!(
        registry
            .promote(channel(0), promotion(1, 200))
            .await
            .is_ok()
    );
    let Ok(declared) = registry
        .declare(
            pattern(2),
            Policy::Unreviewed(None),
            PolicyAuthor::Config,
            at(100),
        )
        .await
    else {
        panic!("declare");
    };
    discover(&mut registry, 2, 2).await;
    discover(&mut registry, 3, 3).await;
    drain(&mut events);
    let cases = [
        (
            model::unknown_channel(),
            0,
            PromotionRefusal::UnknownChannel(model::unknown_channel()),
        ),
        (
            channel(1),
            0,
            PromotionRefusal::Superseded {
                channel: channel(1),
                by: channel(0),
            },
        ),
        (declared, 2, PromotionRefusal::NotDiscovered(declared)),
        (channel(3), 4, PromotionRefusal::PatternMissesSeed),
        (
            channel(2),
            0,
            PromotionRefusal::PatternOverlaps {
                existing: channel(0),
            },
        ),
    ];
    for (target, p, refusal) in cases {
        let before = registry.state.read().clone();
        let expected = {
            let table = registry.state.read();
            plan(target, promotion(p, 300).declaration(), &table.registered()).map(|_| ())
        };
        assert_eq!(expected, Err(refusal));
        assert_eq!(
            registry.promote(target, promotion(p, 300)).await,
            Err(PromoteError::Refused(refusal))
        );
        assert_eq!(*registry.state.read(), before);
        assert!(drain(&mut events).is_empty());
    }
}

/// `flow.registry.coverage-reads-like-promote`.
#[tokio::test]
async fn promotion_coverage_matches_reference() {
    let (mut registry, _events) = registry().await;
    discover(&mut registry, 0, 0).await;
    discover(&mut registry, 1, 1).await;
    discover(&mut registry, 2, 2).await;
    let before = registry.state.read().clone();
    for (target, p) in [
        (channel(0), 1),
        (channel(0), 0),
        (channel(2), 3),
        (channel(1), 4),
    ] {
        let declaration = promotion(p, 200).declaration().clone();
        let expected = {
            let table = registry.state.read();
            let held = |id: ChannelId| {
                let Some(channel) = table.channels.get(&id) else {
                    return Vec::new();
                };
                channel
                    .origin
                    .seed()
                    .map(|seed| seed.resource)
                    .into_iter()
                    .chain(channel.resources.iter().copied())
                    .filter_map(|r| table.resources.get(&r).map(|s| s.resource.clone()))
                    .collect()
            };
            coverage(target, &declaration, &table.registered(), held)
        };
        assert_eq!(
            registry.promotion_coverage(target, &declaration).await,
            expected.map_err(PromoteError::Refused)
        );
    }
    assert_eq!(*registry.state.read(), before);
}

/// `flow.channel.supersession-one-step`.
#[tokio::test]
async fn channel_canonical_is_idempotent() {
    let (mut registry, _events) = registry().await;
    discover(&mut registry, 0, 0).await;
    discover(&mut registry, 1, 1).await;
    assert!(
        registry
            .promote(channel(0), promotion(1, 200))
            .await
            .is_ok()
    );
    for id in [channel(0), channel(1), channel(2), model::unknown_channel()] {
        let canonical = registry.canonical(id);
        assert_eq!(registry.canonical(canonical), canonical);
    }
    assert_eq!(registry.canonical(channel(1)), channel(0));
    assert_eq!(registry.canonical(channel(2)), channel(2));
}

// ---- detection -----------------------------------------------------------------

/// `flow.channel.confirmation-advances-canonical-detection`.
#[tokio::test]
async fn late_confirmation_on_a_superseded_channel_advances_its_superseder() {
    let (mut registry, mut events) = registry().await;
    discover(&mut registry, 0, 0).await;
    discover(&mut registry, 1, 1).await;
    assert!(
        registry
            .promote(channel(0), promotion(1, 200))
            .await
            .is_ok()
    );
    // Channel 0 turned dormant before the late confirmation.
    let dormant = TrafficDetection::Dormant {
        since: at(250),
        last_transmission: seed_transmission(0),
    };
    assert_eq!(
        registry
            .set_detection(channel(0), DetectionUpdate::Traffic(dormant))
            .await,
        Ok(Change::Applied)
    );
    let frozen = stored(&registry, channel(1)).await;
    drain(&mut events);
    // Channel 1's seed transmission, routed through it before the
    // promotion, is confirmed (at 20 µs in the fixture) after it.
    let confirmed = routed_transmission(seed_transmission(1), channel(1), 3, false, 1);
    assert_eq!(
        registry.record_transmission(&confirmed).await,
        Ok(Change::Applied)
    );
    let promoted = stored(&registry, channel(0)).await;
    assert_eq!(
        promoted.origin.traffic(),
        Some(&TrafficDetection::Active {
            since: at(20),
            last_transmission: seed_transmission(1)
        })
    );
    assert_eq!(stored(&registry, channel(1)).await, frozen);
    assert_eq!(
        drain(&mut events),
        vec![BusEvent::Changed(Changed::Channel(channel(0)))]
    );
    // A second cross-agent transmission keeps `since` and names itself.
    let next = routed_transmission(TransmissionId::from_ulid(0x7A02), channel(0), 1, false, 400);
    assert_eq!(
        registry.record_transmission(&next).await,
        Ok(Change::Applied)
    );
    assert_eq!(
        stored(&registry, channel(0)).await.origin.traffic(),
        Some(&TrafficDetection::Active {
            since: at(20),
            last_transmission: next.id
        })
    );
    // Recording the same state again changes nothing.
    assert_eq!(
        registry.record_transmission(&next).await,
        Ok(Change::Unchanged)
    );
    assert_eq!(
        registry
            .set_detection(channel(1), DetectionUpdate::Unused { since: at(1) })
            .await,
        Err(TrafficError::Superseded {
            channel: channel(1),
            by: channel(0)
        })
    );
}

/// `flow.channel.change-announced`: discovery, declaration, a new
/// resource, a detection change and a recorded decision each announce the
/// channel; an access and a no-op detection do not.
#[tokio::test]
async fn channel_changes_announced_after_commit() {
    let (mut registry, mut events) = registry().await;
    let announced = |events: Vec<BusEvent>, id: ChannelId| {
        events.contains(&BusEvent::Changed(Changed::Channel(id)))
    };
    discover(&mut registry, 0, 0).await;
    assert!(announced(drain(&mut events), channel(0)));
    let Ok(declared) = registry
        .declare(
            pattern(2),
            Policy::Unreviewed(None),
            PolicyAuthor::Config,
            at(100),
        )
        .await
    else {
        panic!("declare");
    };
    assert!(announced(drain(&mut events), declared));
    assert_eq!(registry.add_resource(resource(4)).await, Ok(Some(declared)));
    assert!(announced(drain(&mut events), declared));
    let observed = DetectionUpdate::Traffic(TrafficDetection::Active {
        since: at(4),
        last_transmission: seed_transmission(4),
    });
    assert_eq!(
        registry.set_detection(declared, observed.clone()).await,
        Ok(Change::Applied)
    );
    assert!(announced(drain(&mut events), declared));
    assert_eq!(
        registry.set_detection(declared, observed).await,
        Ok(Change::Unchanged)
    );
    assert!(drain(&mut events).is_empty());
    assert!(
        registry
            .set_policy(declared, decision(1, 5, true, false))
            .await
            .is_ok()
    );
    assert!(announced(drain(&mut events), declared));
    let access = crosstalk_spec::derived::flow::access::Access {
        id: AccessId::from_ulid(77),
        agent: access_agent(0),
        exchange: crosstalk_spec::ids::ExchangeId::from_ulid(77),
        resource: resource(4).id,
        at: at(5),
        via: crosstalk_spec::derived::flow::access::Extraction::Structured,
        op: crosstalk_spec::derived::flow::access::AccessOp::Read {
            result: crosstalk_spec::observed::message::PartRef {
                message: crosstalk_spec::ids::MessageHash::from_digest(
                    crosstalk_spec::support::Blake3::from_bytes([0; 32]),
                ),
                index: 0,
            },
        },
    };
    assert_eq!(registry.record_access(access).await, Ok(()));
    assert!(drain(&mut events).is_empty());
}

// ---- resource use --------------------------------------------------------------

/// `surface.query.channel-resources-canonical`, at L5: a superseded
/// channel's resources list under the channel that superseded it, with
/// merged agents' counts summed into their canonical agent, and only
/// accesses in the window counted.
#[tokio::test]
async fn resource_use_includes_superseded_and_sums_aliases() {
    let (mut registry, _events) = registry().await;
    discover(&mut registry, 0, 0).await;
    discover(&mut registry, 1, 1).await;
    assert!(
        registry
            .promote(channel(0), promotion(1, 200))
            .await
            .is_ok()
    );
    let mut id = 0u128;
    let mut record = |resource: u8, agent: u8, write: bool, time: u64| {
        id += 1;
        let part = crosstalk_spec::observed::message::PartRef {
            message: crosstalk_spec::ids::MessageHash::from_digest(
                crosstalk_spec::support::Blake3::from_bytes([1; 32]),
            ),
            index: 0,
        };
        crosstalk_spec::derived::flow::access::Access {
            id: AccessId::from_ulid(id),
            agent: access_agent(agent),
            exchange: crosstalk_spec::ids::ExchangeId::from_ulid(id),
            resource: model::resource(resource).id,
            at: at(time),
            via: crosstalk_spec::derived::flow::access::Extraction::Structured,
            op: if write {
                crosstalk_spec::derived::flow::access::AccessOp::Write {
                    call: part,
                    spans: Vec::new(),
                    outcome: crosstalk_spec::derived::flow::access::WriteOutcome::Delivered,
                }
            } else {
                crosstalk_spec::derived::flow::access::AccessOp::Read { result: part }
            },
        }
    };
    let accesses = [
        record(0, 0, true, 10),
        record(1, 2, false, 11),
        record(1, 3, false, 12),
        record(1, 1, false, 500),
    ];
    for access in accesses {
        assert_eq!(registry.record_access(access).await, Ok(()));
    }
    let Ok(window) = TimeWindow::new(at(0), at(100)) else {
        panic!("window");
    };
    let Ok(pages) = model::traverse(&registry, channel(1), window, 1).await else {
        panic!("resource use");
    };
    assert!(pages.iter().all(|(canonical, _)| *canonical == channel(0)));
    let rows: Vec<_> = pages.into_iter().flat_map(|(_, rows)| rows).collect();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].resource().id, model::resource(1).id);
    assert_eq!(rows[0].readers().len(), 1);
    assert_eq!(rows[0].readers()[0].agent, access_agent(2));
    assert_eq!(rows[0].readers()[0].accesses.get(), 2);
    assert_eq!(rows[1].resource().id, model::resource(0).id);
    assert_eq!(rows[1].writers()[0].agent, access_agent(0));
}

/// `flow.access.rejected-write-recorded`, at the store: a write is stored
/// and counted whatever its outcome, so a rejected write still lists its
/// agent as a writer of the resource, beside a delivered and an unknown one.
#[tokio::test]
async fn writes_of_every_outcome_are_recorded_and_listed() {
    use crosstalk_spec::derived::flow::access::{Access, AccessOp, Extraction, WriteOutcome};
    let (mut registry, _events) = registry().await;
    discover(&mut registry, 0, 0).await;
    let outcomes = [
        WriteOutcome::Rejected,
        WriteOutcome::Delivered,
        WriteOutcome::Unknown,
    ];
    for (n, outcome) in (1u128..).zip(outcomes) {
        let access = Access {
            id: AccessId::from_ulid(n),
            agent: access_agent(0),
            exchange: crosstalk_spec::ids::ExchangeId::from_ulid(n),
            resource: model::resource(0).id,
            at: at(5),
            via: Extraction::Structured,
            op: AccessOp::Write {
                call: crosstalk_spec::observed::message::PartRef {
                    message: crosstalk_spec::ids::MessageHash::from_digest(
                        crosstalk_spec::support::Blake3::from_bytes([1; 32]),
                    ),
                    index: 0,
                },
                spans: Vec::new(),
                outcome,
            },
        };
        assert_eq!(registry.record_access(access).await, Ok(()));
    }
    let Ok(window) = TimeWindow::new(at(0), at(100)) else {
        panic!("window");
    };
    let Ok(pages) = model::traverse(&registry, channel(0), window, 10).await else {
        panic!("resource use");
    };
    let rows: Vec<_> = pages.into_iter().flat_map(|(_, rows)| rows).collect();
    assert_eq!(rows.len(), 1);
    let written: u64 = rows[0]
        .writers()
        .iter()
        .map(|writer| writer.accesses.get())
        .sum();
    assert_eq!(written, 3);
    assert!(rows[0].readers().is_empty());
}

/// `flow.access-store.accesses-as-recorded` and
/// `flow.access-store.keys-within-batch`: each recorded access reads back
/// as recorded, its write outcome included, with its stored resource;
/// ids never recorded are left out.
#[tokio::test]
async fn accesses_read_back_in_batches_with_their_resources() {
    use crosstalk_spec::batch::IdBatch;
    use crosstalk_spec::derived::flow::access::{Access, AccessOp, Extraction, WriteOutcome};
    use crosstalk_spec::interfaces::l5_flow::channels::AccessStore;
    use std::collections::BTreeMap;
    let (mut registry, _events) = registry().await;
    discover(&mut registry, 0, 0).await;
    let rejected = Access {
        id: AccessId::from_ulid(41),
        agent: access_agent(1),
        exchange: crosstalk_spec::ids::ExchangeId::from_ulid(41),
        resource: model::resource(0).id,
        at: at(5),
        via: Extraction::Parsed,
        op: AccessOp::Write {
            call: crosstalk_spec::observed::message::PartRef {
                message: crosstalk_spec::ids::MessageHash::from_digest(
                    crosstalk_spec::support::Blake3::from_bytes([4; 32]),
                ),
                index: 2,
            },
            spans: Vec::new(),
            outcome: WriteOutcome::Rejected,
        },
    };
    let Ok(ids) = IdBatch::new([AccessId::from_ulid(41), AccessId::from_ulid(42)]) else {
        panic!("batch");
    };
    assert_eq!(registry.accesses(&ids).await, Ok(BTreeMap::new()));
    assert_eq!(registry.record_access(rejected.clone()).await, Ok(()));
    assert_eq!(
        registry.accesses(&ids).await,
        Ok(BTreeMap::from([(
            rejected.id,
            (rejected, model::resource(0))
        )]))
    );
}

/// A cursor issued for one channel or window is refused for another.
#[tokio::test]
async fn resource_use_refuses_foreign_cursors() {
    let (mut registry, _events) = registry().await;
    discover(&mut registry, 0, 0).await;
    discover(&mut registry, 1, 6).await;
    // A discovered channel holds only its seed: promote channel 0 so its
    // pattern brings in a second resource.
    assert!(
        registry
            .promote(channel(0), promotion(1, 200))
            .await
            .is_ok()
    );
    assert_eq!(
        registry.add_resource(resource(1)).await,
        Ok(Some(channel(0)))
    );
    for (n, r) in [(1u128, 0u8), (2, 1)] {
        let part = crosstalk_spec::observed::message::PartRef {
            message: crosstalk_spec::ids::MessageHash::from_digest(
                crosstalk_spec::support::Blake3::from_bytes([1; 32]),
            ),
            index: 0,
        };
        let access = crosstalk_spec::derived::flow::access::Access {
            id: AccessId::from_ulid(n),
            agent: access_agent(0),
            exchange: crosstalk_spec::ids::ExchangeId::from_ulid(n),
            resource: model::resource(r).id,
            at: at(5),
            via: crosstalk_spec::derived::flow::access::Extraction::Structured,
            op: crosstalk_spec::derived::flow::access::AccessOp::Read { result: part },
        };
        assert_eq!(registry.record_access(access).await, Ok(()));
    }
    let (Ok(window), Ok(other)) = (
        TimeWindow::new(at(0), at(100)),
        TimeWindow::new(at(0), at(50)),
    ) else {
        panic!("window");
    };
    let Ok(size) = crosstalk_spec::paging::PageSize::new(1) else {
        panic!("size");
    };
    let request = crosstalk_spec::paging::PageRequest { size, after: None };
    let Ok(first) = registry.resource_use(channel(0), window, &request).await else {
        panic!("first page");
    };
    let after = first.page.next().cloned();
    assert!(after.is_some());
    let next = crosstalk_spec::paging::PageRequest { size, after };
    assert_eq!(
        registry
            .resource_use(channel(0), other, &next)
            .await
            .map(|_| ()),
        Err(RegistryError::InvalidCursor)
    );
    assert_eq!(
        registry
            .resource_use(channel(1), window, &next)
            .await
            .map(|_| ()),
        Err(RegistryError::InvalidCursor)
    );
    assert!(
        registry
            .resource_use(channel(0), window, &next)
            .await
            .is_ok()
    );
}

// ---- the harness ---------------------------------------------------------------

/// `flow.registry.declared-patterns-disjoint`,
/// `flow.policy.current-is-history-latest`,
/// `flow.channel.supersession-one-step` and
/// `flow.registry.lookup-never-creates` on random histories: the harness runs
/// the reference against itself, checking those invariants after every
/// step.
#[test]
fn reference_agrees_with_itself_under_the_harness() {
    let outcome = model::check_channel_registry(pipeline_harness(), MemoryChannels::new);
    assert_eq!(outcome, Ok(()));
}

/// The harness catches a registry that ignores accesses' agents' merges:
/// one built over a directory with no merges.
#[test]
fn harness_rejects_a_registry_that_ignores_merges() {
    let outcome = model::check_channel_registry(pipeline_harness(), |_agents, ids, outbox| {
        MemoryChannels::new(MemoryAgents::default(), ids, outbox)
    });
    assert!(
        matches!(outcome, Err(ModelMismatch::Failed { .. })),
        "{outcome:?}"
    );
}

/// Declared channels created by declaration only for distinct ids.
#[tokio::test]
async fn declared_channels_take_fresh_ids() {
    let (mut registry, _events) = registry().await;
    let Ok(first) = registry
        .declare(
            pattern(2),
            Policy::Unreviewed(None),
            PolicyAuthor::Config,
            at(100),
        )
        .await
    else {
        panic!("declare");
    };
    let Ok(second) = registry
        .declare(
            pattern(4),
            Policy::Unreviewed(None),
            PolicyAuthor::Config,
            at(100),
        )
        .await
    else {
        panic!("declare");
    };
    assert!(second > first);
    assert_ne!(first, channel(0));
}

/// The registry is `Send + Sync`, so its futures are `Send`.
#[test]
fn the_registry_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync + 'static>() {}
    assert_send_sync::<MemoryChannels<MemoryAgents>>();
}

/// `ChannelReads`: a channel by id (a superseded one as itself, with its
/// supersession), and the channels a filter keeps, newest first, a page at
/// a time, with the cursor bound to the filter.
#[tokio::test]
async fn channel_reads_list_filtered_newest_first() {
    use crosstalk_spec::paging::{PageRequest, PageSize};

    let (mut registry, _events) = registry().await;
    discover(&mut registry, 0, 0).await;
    discover(&mut registry, 1, 1).await;
    discover(&mut registry, 2, 6).await;
    // The prefix pattern covers channel 1's seed: it is superseded.
    assert!(
        registry
            .promote(channel(0), promotion(1, 200))
            .await
            .is_ok()
    );
    assert_eq!(registry.channel(model::unknown_channel()).await, Ok(None));
    let Ok(Some(superseded)) = registry.channel(channel(1)).await else {
        panic!("channel 1 is stored");
    };
    assert_eq!(superseded.traffic(), None);
    assert_eq!(
        superseded
            .channel()
            .origin
            .supersession()
            .map(|supersession| supersession.by),
        Some(channel(0))
    );
    let in_force = ChannelFilter::default();
    let one = PageSize::new(1).unwrap();
    let first = registry
        .channels(
            &in_force,
            &PageRequest {
                size: one,
                after: None,
            },
        )
        .await
        .unwrap();
    let (items, next) = first.into_parts();
    assert_eq!(
        items
            .iter()
            .map(|read| read.channel().id)
            .collect::<Vec<_>>(),
        vec![channel(2)]
    );
    let cursor = next.unwrap();
    let superseded_only = ChannelFilter {
        origin: OriginFilter::Superseded,
        ..ChannelFilter::default()
    };
    assert_eq!(
        registry
            .channels(
                &superseded_only,
                &PageRequest {
                    size: one,
                    after: Some(cursor.clone())
                }
            )
            .await
            .map(|_| ()),
        Err(RegistryError::InvalidCursor)
    );
    let second = registry
        .channels(
            &in_force,
            &PageRequest {
                size: one,
                after: Some(cursor),
            },
        )
        .await
        .unwrap();
    let (items, next) = second.into_parts();
    assert_eq!(
        items
            .iter()
            .map(|read| read.channel().id)
            .collect::<Vec<_>>(),
        vec![channel(0)]
    );
    assert!(next.is_none());
    let all = registry
        .channels(
            &superseded_only,
            &PageRequest {
                size: PageSize::new(10).unwrap(),
                after: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        all.items()
            .iter()
            .map(|read| read.channel().id)
            .collect::<Vec<_>>(),
        vec![channel(1)]
    );
}
