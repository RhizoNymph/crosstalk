//! Integration tests of `PgChannelRegistry` against Postgres, one or more
//! per invariant the registry upholds. Each test's doc names it.

use crosstalk_memory::flow::registry::model::{
    access_agent, all_channels, channel, decision, locator, pattern, resource,
};
use crosstalk_memory::flow::verdicts::model::state_between;
use crosstalk_memory::reconstruct::MemoryAgents;
use crosstalk_spec::derived::flow::channel::detection::TrafficDetection;
use crosstalk_spec::derived::flow::channel::policy::{Policy, PolicyAuthor, PolicyKind, Recorded};
use crosstalk_spec::derived::flow::channel::promotion::{Promotion, PromotionRefusal};
use crosstalk_spec::derived::flow::channel::{ChannelOrigin, Declaration, Seed, Supersession};
use crosstalk_spec::derived::flow::transmission::{Route, Transmission};
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::ids::{ChannelId, OperatorId, TransmissionId};
use crosstalk_spec::interfaces::l5_flow::channels::{ChannelReads, ChannelTraffic, TrafficError};
use crosstalk_spec::interfaces::l5_flow::{
    ChannelDirectory, ChannelLookup, ChannelRegistry, Discovery, PromoteError, RegistryError,
};
use crosstalk_spec::support::Change;
use sqlx::PgPool;

use super::support::{TestResult, at, db, drain, ensure, registry, same};
use crate::store::PgChannelRegistry;

type Registry = PgChannelRegistry<MemoryAgents>;

fn seed_transmission(n: u8) -> TransmissionId {
    TransmissionId::from_ulid(0xF1A0_0000 | u128::from(n))
}

/// A transmission `id` opened at `opened` through `routed` in state `state`
/// (as the verdict harness numbers them), from agent 1 to agent 2.
fn routed(id: TransmissionId, routed: ChannelId, state: u8, opened: u64) -> Transmission {
    let state = state_between(state, &[0], access_agent(1), access_agent(2));
    Transmission {
        id,
        to: access_agent(2),
        route: Route::Channel(routed),
        opened_at: at(opened),
        state: state
            .unwrap_or(crosstalk_spec::derived::flow::transmission::TransmissionState::Detected),
    }
}

async fn store_resource(registry: &mut Registry, r: u8) -> TestResult {
    match registry.add_resource(resource(r)).await {
        Ok(_) | Err(TrafficError::DuplicateResource(_)) => Ok(()),
        Err(error) => Err(super::support::Failure::Unexpected(format!(
            "resource {r} refused: {error:?}"
        ))),
    }
}

/// Discover channel `c` from resource `r` at `r` µs and record its seed
/// transmission awaiting content, as the flow consumer does.
async fn discover(registry: &mut Registry, c: u8, r: u8) -> TestResult {
    store_resource(registry, r).await?;
    let found = registry
        .discover(
            channel(c),
            resource(r).id,
            seed_transmission(r),
            at(u64::from(r)),
        )
        .await;
    same("discover", &found, &Ok(Discovery::Created(channel(c))))?;
    let opened = routed(seed_transmission(r), channel(c), 1, u64::from(r));
    same(
        "record seed transmission",
        &registry.record_transmission(&opened).await,
        &Ok(Change::Applied),
    )
}

fn promotion(p: u8, time: u64) -> Promotion {
    Promotion::new(
        pattern(p),
        PolicyKind::Sanctioned,
        OperatorId::from_ulid(0x0B0B_0001),
        at(time),
        Some("promoted".to_owned()),
    )
}

/// Every channel row, history and lookup: what a refused write must leave
/// unchanged.
async fn state_of(registry: &Registry) -> Result<String, super::support::Failure> {
    let channels = all_channels(registry).await;
    let mut histories = Vec::new();
    for n in 0..4 {
        histories.push(registry.policy_history(channel(n)).await);
    }
    let mut lookups = Vec::new();
    for n in 0..8 {
        lookups.push(registry.lookup(&locator(n)).await);
    }
    Ok(format!("{channels:?}\n{histories:?}\n{lookups:?}"))
}

async fn count(pool: &PgPool, table: &str) -> Result<i64, sqlx::Error> {
    let sql = match table {
        "channels" => "SELECT count(*) FROM flow.channels",
        "outbox" => "SELECT count(*) FROM flow.outbox",
        _ => "SELECT count(*) FROM flow.resources",
    };
    sqlx::query_scalar(sql).fetch_one(pool).await
}

/// INV-850 `flow.registry.lookup-never-creates`: lookups create nothing and
/// answer NoChannel, Declared or Known(canonical).
#[tokio::test(flavor = "multi_thread")]
async fn pg_lookup_never_creates() -> TestResult {
    let Some(db) = db("pg_lookup_never_creates").await? else {
        return Ok(());
    };
    let (mut registry, _events) = registry(db.pool()).await?;
    for n in 0..8 {
        same(
            "fresh lookup",
            &registry.lookup(&locator(n)).await,
            &Ok(ChannelLookup::NoChannel),
        )?;
    }
    same("no channel", &count(db.pool(), "channels").await?, &0)?;
    same("no resource", &count(db.pool(), "resources").await?, &0)?;
    store_resource(&mut registry, 4).await?;
    same(
        "stored on none",
        &registry.lookup(&locator(4)).await,
        &Ok(ChannelLookup::NoChannel),
    )?;
    let declared = registry
        .declare(
            pattern(2),
            Policy::Unreviewed(None),
            PolicyAuthor::Config,
            at(10),
        )
        .await
        .map_err(|e| super::support::Failure::Unexpected(format!("{e:?}")))?;
    same(
        "a stored resource on no channel matching a pattern",
        &registry.lookup(&locator(4)).await,
        &Ok(ChannelLookup::Declared(declared)),
    )?;
    discover(&mut registry, 0, 0).await?;
    same(
        "known",
        &registry.lookup(&locator(0)).await,
        &Ok(ChannelLookup::Known(channel(0))),
    )?;
    same("two channels", &count(db.pool(), "channels").await?, &2)?;
    db.close().await?;
    Ok(())
}

/// INV-851 `flow.channel.discovered-by-cross-agent-transmission`: discover
/// creates a discovered channel seeded by the resource and transmission,
/// Active since it opened, Unreviewed with an empty history, publishing
/// ChannelDiscovered and Changed; a repeat or a declared pattern finds the
/// existing channel and publishes no discovery.
#[tokio::test(flavor = "multi_thread")]
async fn pg_discover_seeds_channel() -> TestResult {
    let Some(db) = db("pg_discover_seeds_channel").await? else {
        return Ok(());
    };
    let (mut registry, mut events) = registry(db.pool()).await?;
    store_resource(&mut registry, 3).await?;
    drain(&mut events);
    let found = registry
        .discover(channel(0), resource(3).id, seed_transmission(3), at(30))
        .await;
    same("created", &found, &Ok(Discovery::Created(channel(0))))?;
    let seed = Seed {
        resource: resource(3).id,
        first_transmission: seed_transmission(3),
        opened_at: at(30),
    };
    same(
        "events",
        &drain(&mut events),
        &vec![
            BusEvent::Detect(DetectEvent::ChannelDiscovered {
                channel: channel(0),
                seed,
            }),
            BusEvent::Changed(Changed::Channel(channel(0))),
        ],
    )?;
    let read = registry.channel(channel(0)).await;
    let Ok(Some(read)) = read else {
        return Err(super::support::Failure::Unexpected(format!("{read:?}")));
    };
    let (stored, _) = read.into_parts();
    same(
        "origin",
        &stored.origin,
        &ChannelOrigin::Discovered {
            seed,
            detection: TrafficDetection::Active {
                since: at(30),
                last_transmission: seed_transmission(3),
            },
        },
    )?;
    same("policy", &stored.policy, &Policy::Unreviewed(None))?;
    same("resources", &stored.resources, &Vec::new())?;
    ensure(
        registry
            .policy_history(channel(0))
            .await
            .is_ok_and(|history| history.entries().is_empty()),
        || "history not empty".to_owned(),
    )?;
    let again = registry
        .discover(channel(1), resource(3).id, seed_transmission(9), at(40))
        .await;
    same("existing", &again, &Ok(Discovery::Existing(channel(0))))?;
    same("no second discovery", &drain(&mut events), &Vec::new())?;
    // A declared pattern claims a resource on no channel.
    store_resource(&mut registry, 2).await?;
    let declared = registry
        .declare(
            pattern(3),
            Policy::Unreviewed(None),
            PolicyAuthor::Config,
            at(50),
        )
        .await;
    let Ok(declared) = declared else {
        return Err(super::support::Failure::Unexpected(format!("{declared:?}")));
    };
    drain(&mut events);
    let joined = registry
        .discover(channel(2), resource(2).id, seed_transmission(2), at(60))
        .await;
    same(
        "joins the declared channel",
        &joined,
        &Ok(Discovery::Existing(declared)),
    )?;
    same(
        "only its change",
        &drain(&mut events),
        &vec![BusEvent::Changed(Changed::Channel(declared))],
    )?;
    same(
        "unknown resource",
        &registry
            .discover(channel(3), resource(7).id, seed_transmission(7), at(70))
            .await,
        &Err(TrafficError::UnknownResource(resource(7).id)),
    )?;
    db.close().await?;
    Ok(())
}

/// INV-259 `flow.registry.declared-patterns-disjoint`: an overlapping
/// declaration is refused with the overlapping channel, changing nothing.
#[tokio::test(flavor = "multi_thread")]
async fn pg_declare_rejects_overlap() -> TestResult {
    let Some(db) = db("pg_declare_rejects_overlap").await? else {
        return Ok(());
    };
    let (mut registry, mut events) = registry(db.pool()).await?;
    let host = registry
        .declare(
            pattern(0),
            Policy::Unreviewed(None),
            PolicyAuthor::Config,
            at(1),
        )
        .await;
    let Ok(host) = host else {
        return Err(super::support::Failure::Unexpected(format!("{host:?}")));
    };
    drain(&mut events);
    let before = state_of(&registry).await?;
    for overlapping in [1, 3] {
        same(
            "overlap refused",
            &registry
                .declare(
                    pattern(overlapping),
                    Policy::Unreviewed(None),
                    PolicyAuthor::Config,
                    at(2),
                )
                .await,
            &Err(RegistryError::OverlappingDeclaration { existing: host }),
        )?;
    }
    same("nothing changed", &state_of(&registry).await?, &before)?;
    same("nothing published", &drain(&mut events), &Vec::new())?;
    same("one channel", &count(db.pool(), "channels").await?, &1)?;
    ensure(
        registry
            .declare(
                pattern(2),
                Policy::Unreviewed(None),
                PolicyAuthor::Config,
                at(3),
            )
            .await
            .is_ok(),
        || "a disjoint pattern is accepted".to_owned(),
    )?;
    db.close().await?;
    Ok(())
}

/// INV-449 `flow.policy.current-is-history-latest`: the stored policy and
/// the history are written in one transaction, so the channel row's policy
/// is always its history's current one, a late decision included.
#[tokio::test(flavor = "multi_thread")]
async fn policy_and_history_written_atomically() -> TestResult {
    let Some(db) = db("policy_and_history_written_atomically").await? else {
        return Ok(());
    };
    let (mut registry, _events) = registry(db.pool()).await?;
    discover(&mut registry, 0, 0).await?;
    let decisions = [(1, 20), (2, 40), (0, 30), (1, 40), (2, 40)];
    let mut results = Vec::new();
    for (kind, time) in decisions {
        results.push(
            registry
                .set_policy(channel(0), decision(kind, time, false, false))
                .await,
        );
        let stored: String = sqlx::query_scalar("SELECT policy FROM flow.channels WHERE id = $1")
            .bind(crate::store::codec::id_text(channel(0)))
            .fetch_one(db.pool())
            .await?;
        let stored: Policy = serde_json::from_str(&stored)
            .map_err(|e| super::support::Failure::Unexpected(e.to_string()))?;
        let history = registry.policy_history(channel(0)).await;
        same(
            "row policy is the history's current one",
            &Ok(stored),
            &history.map(|history| history.current()),
        )?;
    }
    same(
        "recorded",
        &results,
        &vec![
            Ok(Recorded::Current),
            Ok(Recorded::Current),
            Ok(Recorded::Superseded),
            Ok(Recorded::Current),
            Ok(Recorded::Duplicate),
        ],
    )?;
    db.close().await?;
    Ok(())
}

/// Channels 0 and 1 discovered from wiki resources 0 and 2, channel 2 from
/// the file resource 4.
async fn three_discovered(registry: &mut Registry) -> TestResult {
    discover(registry, 0, 0).await?;
    discover(registry, 1, 2).await?;
    discover(registry, 2, 4).await
}

/// INV-490 `flow.registry.promote-rejects-without-effect`: every refusal
/// changes nothing and publishes nothing.
#[tokio::test(flavor = "multi_thread")]
async fn pg_promote_rejections_change_nothing() -> TestResult {
    let Some(db) = db("pg_promote_rejections_change_nothing").await? else {
        return Ok(());
    };
    let (mut registry, mut events) = registry(db.pool()).await?;
    three_discovered(&mut registry).await?;
    let declared = registry
        .declare(
            pattern(4),
            Policy::Unreviewed(None),
            PolicyAuthor::Config,
            at(5),
        )
        .await
        .map_err(|e| super::support::Failure::Unexpected(format!("{e:?}")))?;
    drain(&mut events);
    let before = state_of(&registry).await?;
    let refusals = [
        (
            ChannelId::from_ulid(0x0C4A_FFFF),
            0,
            PromotionRefusal::UnknownChannel(ChannelId::from_ulid(0x0C4A_FFFF)),
        ),
        (declared, 4, PromotionRefusal::NotDiscovered(declared)),
        (channel(0), 2, PromotionRefusal::PatternMissesSeed),
        (channel(2), 4, PromotionRefusal::PatternMissesSeed),
    ];
    for (target, p, refusal) in refusals {
        same(
            "refused",
            &registry.promote(target, promotion(p, 100)).await,
            &Err(PromoteError::Refused(refusal)),
        )?;
    }
    // A declared host pattern, which the wiki promotion below overlaps.
    let declared_host = registry
        .declare(
            pattern(0),
            Policy::Unreviewed(None),
            PolicyAuthor::Config,
            at(6),
        )
        .await;
    let host = declared_host.map_err(|e| super::support::Failure::Unexpected(format!("{e:?}")))?;
    drain(&mut events);
    let before_overlap = state_of(&registry).await?;
    same(
        "overlap",
        &registry.promote(channel(0), promotion(1, 100)).await,
        &Err(PromoteError::Refused(PromotionRefusal::PatternOverlaps {
            existing: host,
        })),
    )?;
    same(
        "nothing changed",
        &state_of(&registry).await?,
        &before_overlap,
    )?;
    ensure(before != before_overlap, || {
        "the second declaration is visible".to_owned()
    })?;
    same("nothing published", &drain(&mut events), &Vec::new())?;
    same("nothing staged", &count(db.pool(), "outbox").await?, &0)?;
    db.close().await?;
    Ok(())
}

/// INV-657 `flow.registry.promote-applies-plan` and INV-658
/// `flow.registry.promote-publishes-once`: one promotion makes the channel
/// declared from its seed, records the decision, supersedes every
/// discovered channel whose seed the pattern matches, all in one
/// transaction, and publishes one ChannelPromoted (and the changes) only
/// once committed.
#[tokio::test(flavor = "multi_thread")]
async fn pg_promote_applies_plan_in_one_transaction() -> TestResult {
    let Some(db) = db("pg_promote_applies_plan_in_one_transaction").await? else {
        return Ok(());
    };
    let (mut registry, mut events) = registry(db.pool()).await?;
    three_discovered(&mut registry).await?;
    drain(&mut events);
    let promoted = registry.promote(channel(0), promotion(0, 100)).await;
    let Ok(promoted) = promoted else {
        return Err(super::support::Failure::Unexpected(format!("{promoted:?}")));
    };
    same("superseded", &promoted.superseded, &vec![channel(1)])?;
    same("policy", &promoted.policy, &Recorded::Current)?;
    let channels = all_channels(&registry)
        .await
        .map_err(|e| super::support::Failure::Unexpected(format!("{e:?}")))?;
    let origin = |id: ChannelId| {
        channels
            .iter()
            .find(|read| read.channel().id == id)
            .map(|read| read.channel().origin.clone())
    };
    ensure(
        origin(channel(0)).is_some_and(|o| o.pattern() == Some(&pattern(0)) && o.seed().is_some()),
        || format!("promoted origin {:?}", origin(channel(0))),
    )?;
    same(
        "supersession",
        &origin(channel(1)).and_then(|o| o.supersession()),
        &Some(Supersession {
            by: channel(0),
            at: at(100),
        }),
    )?;
    ensure(
        origin(channel(2)).is_some_and(|o| o.supersession().is_none()),
        || "the file channel is untouched".to_owned(),
    )?;
    same("canonical", &registry.canonical(channel(1)), &channel(0))?;
    ensure(
        registry
            .policy_history(channel(0))
            .await
            .is_ok_and(|h| h.current().kind() == PolicyKind::Sanctioned),
        || "decision recorded".to_owned(),
    )?;
    let published = drain(&mut events);
    let promoted_events: Vec<&BusEvent> = published
        .iter()
        .filter(|e| matches!(e, BusEvent::Detect(DetectEvent::ChannelPromoted { .. })))
        .collect();
    same("one ChannelPromoted", &promoted_events.len(), &1)?;
    for id in [channel(0), channel(1)] {
        ensure(
            published.contains(&BusEvent::Changed(Changed::Channel(id))),
            || format!("no change for {id:?}"),
        )?;
    }
    same(
        "relayed, none left staged",
        &count(db.pool(), "outbox").await?,
        &0,
    )?;
    db.close().await?;
    Ok(())
}

/// INV-658 `flow.registry.promote-publishes-once`: the promotion event is
/// relayed only after the commit, so a reader woken by it already sees the
/// promotion; a second registry on the database (another node) resolves
/// the supersession once it refreshes its directory.
#[tokio::test(flavor = "multi_thread")]
async fn pg_promote_publishes_after_commit() -> TestResult {
    let Some(db) = db("pg_promote_publishes_after_commit").await? else {
        return Ok(());
    };
    let (mut registry, mut events) = registry(db.pool()).await?;
    let (other, _other_events) = super::support::registry(db.pool()).await?;
    three_discovered(&mut registry).await?;
    drain(&mut events);
    let task = {
        let mut registry = registry.clone();
        tokio::spawn(async move { registry.promote(channel(0), promotion(0, 100)).await })
    };
    let first = events
        .recv()
        .await
        .ok_or_else(|| super::support::Failure::Unexpected("no event".to_owned()))?;
    ensure(
        matches!(first, BusEvent::Detect(DetectEvent::ChannelPromoted { channel: c, .. }) if c == channel(0)),
        || format!("first event {first:?}"),
    )?;
    // Woken by the event, a fresh read sees the committed promotion.
    let superseded_by: Option<String> =
        sqlx::query_scalar("SELECT superseded_by FROM flow.channels WHERE id = $1")
            .bind(crate::store::codec::id_text(channel(1)))
            .fetch_one(db.pool())
            .await?;
    same(
        "committed",
        &superseded_by,
        &Some(crate::store::codec::id_text(channel(0))),
    )?;
    ensure(task.await?.is_ok(), || "promotion failed".to_owned())?;
    same(
        "other node before refresh",
        &other.canonical(channel(1)),
        &channel(1),
    )?;
    other.refresh_directory().await?;
    same(
        "other node after refresh",
        &other.canonical(channel(1)),
        &channel(0),
    )?;
    db.close().await?;
    Ok(())
}

/// INV-656 `flow.registry.lookup-never-superseded`: after a promotion the
/// superseded channel's resources are Known on the promoted channel and a
/// matching locator on no channel is Declared on it.
#[tokio::test(flavor = "multi_thread")]
async fn pg_lookup_after_promotion() -> TestResult {
    let Some(db) = db("pg_lookup_after_promotion").await? else {
        return Ok(());
    };
    let (mut registry, _events) = registry(db.pool()).await?;
    three_discovered(&mut registry).await?;
    ensure(
        registry
            .promote(channel(0), promotion(0, 100))
            .await
            .is_ok(),
        || "promote".to_owned(),
    )?;
    same(
        "superseded resource",
        &registry.lookup(&locator(2)).await,
        &Ok(ChannelLookup::Known(channel(0))),
    )?;
    same(
        "seed",
        &registry.lookup(&locator(0)).await,
        &Ok(ChannelLookup::Known(channel(0))),
    )?;
    same(
        "unstored match",
        &registry.lookup(&locator(1)).await,
        &Ok(ChannelLookup::Declared(channel(0))),
    )?;
    same(
        "joins",
        &registry.add_resource(resource(1)).await,
        &Ok(Some(channel(0))),
    )?;
    same(
        "other host",
        &registry.lookup(&locator(3)).await,
        &Ok(ChannelLookup::NoChannel),
    )?;
    db.close().await?;
    Ok(())
}

/// INV-659 `flow.registry.superseded-takes-no-policy`.
#[tokio::test(flavor = "multi_thread")]
async fn pg_set_policy_on_superseded_channel() -> TestResult {
    let Some(db) = db("pg_set_policy_on_superseded_channel").await? else {
        return Ok(());
    };
    let (mut registry, mut events) = registry(db.pool()).await?;
    three_discovered(&mut registry).await?;
    ensure(
        registry
            .promote(channel(0), promotion(0, 100))
            .await
            .is_ok(),
        || "promote".to_owned(),
    )?;
    drain(&mut events);
    let before = state_of(&registry).await?;
    same(
        "refused",
        &registry
            .set_policy(channel(1), decision(1, 200, false, false))
            .await,
        &Err(RegistryError::Superseded {
            channel: channel(1),
            by: channel(0),
        }),
    )?;
    same("nothing changed", &state_of(&registry).await?, &before)?;
    same("nothing published", &drain(&mut events), &Vec::new())?;
    db.close().await?;
    Ok(())
}

/// INV-686 `flow.registry.coverage-reads-like-promote`: the preview changes
/// nothing and names exactly the channels the promotion then supersedes.
#[tokio::test(flavor = "multi_thread")]
async fn pg_promotion_coverage_changes_nothing() -> TestResult {
    let Some(db) = db("pg_promotion_coverage_changes_nothing").await? else {
        return Ok(());
    };
    let (mut registry, mut events) = registry(db.pool()).await?;
    three_discovered(&mut registry).await?;
    drain(&mut events);
    let before = state_of(&registry).await?;
    let promotion = promotion(0, 100);
    let coverage = registry
        .promotion_coverage(channel(0), promotion.declaration())
        .await
        .map_err(|e| super::support::Failure::Unexpected(format!("{e:?}")))?;
    same("nothing changed", &state_of(&registry).await?, &before)?;
    same("nothing published", &drain(&mut events), &Vec::new())?;
    same("covered", &coverage.covered().total(), &2)?;
    same("uncovered", &coverage.uncovered().total(), &0)?;
    let promoted = registry
        .promote(channel(0), promotion)
        .await
        .map_err(|e| super::support::Failure::Unexpected(format!("{e:?}")))?;
    same(
        "same superseded",
        &coverage.superseded().to_vec(),
        &promoted.superseded,
    )?;
    let declaration = Declaration {
        pattern: pattern(0),
        by: PolicyAuthor::Config,
        at: at(200),
    };
    same(
        "a refusal is promote's",
        &registry.promotion_coverage(channel(1), &declaration).await,
        &Err(PromoteError::Refused(PromotionRefusal::Superseded {
            channel: channel(1),
            by: channel(0),
        })),
    )?;
    db.close().await?;
    Ok(())
}

/// INV-740 `flow.channel.confirmation-advances-canonical-detection`: a
/// transmission opened on a channel before a promotion superseded it and
/// confirmed after advances the superseding channel's detection; the
/// superseded channel's stays frozen.
#[tokio::test(flavor = "multi_thread")]
async fn pg_late_confirmation_after_promotion() -> TestResult {
    let Some(db) = db("pg_late_confirmation_after_promotion").await? else {
        return Ok(());
    };
    let (mut registry, mut events) = registry(db.pool()).await?;
    three_discovered(&mut registry).await?;
    let late = TransmissionId::from_ulid(0xF1A0_0099);
    let opened = routed(late, channel(1), 1, 5);
    same(
        "opened",
        &registry.record_transmission(&opened).await,
        &Ok(Change::Applied),
    )?;
    ensure(
        registry
            .promote(channel(0), promotion(0, 100))
            .await
            .is_ok(),
        || "promote".to_owned(),
    )?;
    let frozen = registry
        .channel(channel(1))
        .await
        .map_err(|e| super::support::Failure::Unexpected(format!("{e:?}")))?
        .map(|read| read.into_parts().0.origin);
    drain(&mut events);
    let confirmed = routed(late, channel(1), 3, 5);
    same(
        "confirmed",
        &registry.record_transmission(&confirmed).await,
        &Ok(Change::Applied),
    )?;
    same(
        "announced on the canonical channel",
        &drain(&mut events),
        &vec![BusEvent::Changed(Changed::Channel(channel(0)))],
    )?;
    let promoted = registry
        .channel(channel(0))
        .await
        .map_err(|e| super::support::Failure::Unexpected(format!("{e:?}")))?
        .map(|read| read.into_parts().0.origin);
    ensure(
        promoted
            .as_ref()
            .and_then(|o| o.traffic())
            .is_some_and(|d| {
                matches!(
                    d,
                    TrafficDetection::Active { last_transmission, .. } if *last_transmission == late
                )
            }),
        || format!("promoted detection {promoted:?}"),
    )?;
    let after = registry
        .channel(channel(1))
        .await
        .map_err(|e| super::support::Failure::Unexpected(format!("{e:?}")))?
        .map(|read| read.into_parts().0.origin);
    same("superseded detection frozen", &after, &frozen)?;
    db.close().await?;
    Ok(())
}
