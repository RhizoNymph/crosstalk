//! Operator actions under simulation: races, a slow bus, refused
//! publishes, and the alert store's revisions and announcements.

use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::time::Duration;

use crosstalk_sim::{CheckFailed, DurationRange, Probability};
use crosstalk_spec::aggregates::alert::{
    AlertDraft, AlertRevision, AlertState, AlertSubject, BuiltinRule,
};
use crosstalk_spec::derived::flow::channel::policy::PolicyKind;
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::ids::{AlertId, ChannelId};
use crosstalk_spec::interfaces::l6_analysis::AlertTriage;
use crosstalk_spec::interfaces::l6_analysis::alerts::AlertReads;
use crosstalk_spec::interfaces::l8_surface::live::{LiveFeed, LiveItem, LiveStream, Resume, UiEvent};
use crosstalk_spec::interfaces::l8_surface::{
    ActionError, ActionOutcome, ConflictKind, OperatorAction, OperatorActions, QueryApi,
};

use super::{check, failed};
use crate::tests::world::{Fixture, Who, minute};

const KINDS: [PolicyKind; 3] = [
    PolicyKind::Unreviewed,
    PolicyKind::Sanctioned,
    PolicyKind::Unsanctioned,
];

crosstalk_sim::sim_test! {
    /// INV-362 (`surface.action.alert-concurrent-single-transition`):
    /// racing acknowledgements take the transition once, recorded with the
    /// operator of the one that was applied; racing resolves likewise.
    fn racing_alert_actions_single_transition(ctx) {
        let fixture = std::sync::Arc::new(Fixture::new().await);
        let scene = fixture.scene().await;
        let mut rng = ctx.rng();
        let callers = [fixture.caller(Who::Admin).await, fixture.caller(Who::Triager).await];
        let jitter = DurationRange::new(Duration::ZERO, Duration::from_millis(5))
            .map_err(|error| failed("jitter", error))?;
        for step in 0..2 {
            let racers = 2 + rng.below(NonZeroU64::MIN.saturating_add(3));
            let mut tasks = Vec::new();
            for _ in 0..racers {
                let caller = rng.pick(&callers).cloned().ok_or_else(|| CheckFailed::new("no caller"))?;
                let wait = rng.duration_in(jitter);
                let fixture = std::sync::Arc::clone(&fixture);
                let alert = scene.alert;
                let action = if step == 0 {
                    OperatorAction::Acknowledge { alert }
                } else {
                    OperatorAction::Resolve { alert, note: None }
                };
                tasks.push(ctx.spawn("racer", async move {
                    tokio::time::sleep(wait).await;
                    let result = fixture.surface.act(&caller, action).await;
                    (caller.operator(), result)
                }));
            }
            let mut results = Vec::new();
            for task in tasks {
                results.push(task.join().await.map_err(|error| failed("racer", error))?);
            }
            let applied: Vec<_> = results
                .iter()
                .filter(|(_, result)| *result == Ok(ActionOutcome::Applied))
                .collect();
            check(&ctx, applied.len() == 1, || format!("step {step}: {results:?}"))?;
            let loser = if step == 0 {
                Ok(ActionOutcome::Unchanged)
            } else {
                Err(ActionError::Conflict(ConflictKind::AlertNotActive { alert: scene.alert }))
            };
            check(
                &ctx,
                results
                    .iter()
                    .all(|(_, result)| *result == Ok(ActionOutcome::Applied) || *result == loser),
                || format!("step {step}: {results:?}"),
            )?;
            let stored = fixture
                .world
                .alerts
                .alert(scene.alert)
                .await
                .map_err(|error| failed("alert", error))?
                .ok_or_else(|| CheckFailed::new("alert gone"))?;
            let winner = applied[0].0;
            let by = match stored.state {
                AlertState::Acknowledged { by, .. } | AlertState::Resolved { by, .. } => by,
                other => return Err(CheckFailed::new(format!("state {other:?}"))),
            };
            check(&ctx, by == winner, || format!("recorded {by:?}, applied by {winner:?}"))?;
        }
        Ok(())
    }
}

crosstalk_sim::sim_test! {
    /// INV-366 (`surface.action.policy-read-your-writes`): after `SetPolicy`
    /// returns `Ok`, the caller's next channel read shows that policy, however
    /// slowly the bus delivers the `PolicyChanged`.
    fn policy_read_your_writes_under_bus_delay(ctx) {
        let fixture = Fixture::new().await;
        let scene = fixture.scene().await;
        let caller = fixture.caller(Who::Governor).await;
        let mut rng = ctx.rng();
        let delays = DurationRange::new(Duration::ZERO, Duration::from_secs(2))
            .map_err(|error| failed("delays", error))?;
        for step in 0..8_u64 {
            fixture.world.bus.delay_publishes(rng.duration_in(delays));
            fixture.clock.set(minute(10 + step));
            let kind = *rng.pick(&KINDS).ok_or_else(|| CheckFailed::new("kinds"))?;
            let action = OperatorAction::SetPolicy { channel: scene.c1, policy: kind, note: None };
            let result = fixture.surface.act(&caller, action).await;
            check(&ctx, result.is_ok(), || format!("set policy: {result:?}"))?;
            let row = fixture
                .surface
                .channel(&caller, scene.c1, None)
                .await
                .map_err(|error| failed("channel", error))?
                .ok_or_else(|| CheckFailed::new("channel gone"))?;
            let shown = row.value.channel().policy.kind();
            check(&ctx, shown == kind, || format!("step {step}: set {kind:?}, read {shown:?}"))?;
        }
        Ok(())
    }
}

crosstalk_sim::sim_test! {
    /// INV-369 (`surface.action.set-policy-publishes-once`): each `SetPolicy`
    /// publishes one `PolicyChanged` when it returns `Ok` and none when it
    /// fails, under a bus that refuses at random and requests the registry
    /// refuses.
    fn set_policy_publish_count_under_store_and_bus_faults(ctx) {
        let fixture = Fixture::new().await;
        let scene = fixture.scene().await;
        let caller = fixture.caller(Who::Admin).await;
        let mut rng = ctx.rng();
        let refusal = Probability::percent(30).map_err(|error| failed("p", error))?;
        let channels = [scene.c1, scene.c1, ChannelId::from_ulid(0xDEAD)];
        for step in 0..12_u64 {
            fixture.world.bus.refuse_publishes(rng.chance(refusal));
            fixture.clock.set(minute(20 + step));
            let channel = *rng.pick(&channels).ok_or_else(|| CheckFailed::new("channels"))?;
            let kind = *rng.pick(&KINDS).ok_or_else(|| CheckFailed::new("kinds"))?;
            let before = fixture.world.bus.published().len();
            let result = fixture
                .surface
                .act(&caller, OperatorAction::SetPolicy { channel, policy: kind, note: None })
                .await;
            let published = fixture.world.bus.published().len() - before;
            let expected = usize::from(result.is_ok());
            check(&ctx, published == expected, || {
                format!("step {step}: {result:?} published {published}")
            })?;
        }
        Ok(())
    }
}

/// The revision of every alert event the stores published, by alert.
fn alert_revisions(events: &[BusEvent]) -> BTreeMap<AlertId, Vec<(AlertRevision, AlertState)>> {
    let mut revisions: BTreeMap<AlertId, Vec<(AlertRevision, AlertState)>> = BTreeMap::new();
    for event in events {
        match event {
            BusEvent::Insight(InsightEvent::AlertOpened(alert)) => revisions
                .entry(alert.id)
                .or_default()
                .push((AlertRevision::OPENED, alert.state.clone())),
            BusEvent::Insight(InsightEvent::AlertChanged { alert, revision }) => revisions
                .entry(alert.id)
                .or_default()
                .push((*revision, alert.state.clone())),
            _ => {}
        }
    }
    revisions
}

crosstalk_sim::sim_test! {
    /// INV-456 (`surface.alert.revision-consecutive`): triage folding drafts
    /// in and operators acknowledging and resolving, interleaved at random,
    /// leave each alert's events at revisions 1, 2, 3, … with the last one
    /// the alert as stored.
    fn alert_revisions_consecutive_across_triage_and_actions(ctx) {
        let mut fixture = Fixture::new().await;
        let scene = fixture.scene().await;
        let caller = fixture.caller(Who::Triager).await;
        let mut rng = ctx.rng();
        let subjects = [AlertSubject::Agent(scene.a1), AlertSubject::Agent(scene.a2)];
        let mut alerts = vec![scene.alert];
        for step in 0..16_u64 {
            fixture.clock.set(minute(30 + step));
            match rng.below(NonZeroU64::MIN.saturating_add(2)) {
                0 => {
                    let subject = *rng.pick(&subjects).ok_or_else(|| CheckFailed::new("subjects"))?;
                    let draft = AlertDraft {
                        rule: BuiltinRule::NewChannel.id(),
                        subject,
                        raised_at: minute(30 + step),
                    };
                    let mut store = fixture.world.alerts.clone();
                    match store.triage(draft).await.map_err(|error| failed("triage", error))? {
                        crosstalk_spec::aggregates::alert::TriageOutcome::Opened(alert) => alerts.push(alert.id),
                        _ => {}
                    }
                }
                1 => {
                    let alert = *rng.pick(&alerts).ok_or_else(|| CheckFailed::new("alerts"))?;
                    let _ = fixture.surface.act(&caller, OperatorAction::Acknowledge { alert }).await;
                }
                _ => {
                    let alert = *rng.pick(&alerts).ok_or_else(|| CheckFailed::new("alerts"))?;
                    let _ = fixture
                        .surface
                        .act(&caller, OperatorAction::Resolve { alert, note: None })
                        .await;
                }
            }
        }
        let events = fixture.published();
        for (alert, revisions) in alert_revisions(&events) {
            for (index, (revision, _)) in revisions.iter().enumerate() {
                let expected = u32::try_from(index + 1).unwrap_or(u32::MAX);
                check(&ctx, revision.get().get() == expected, || {
                    format!("{alert:?}: revisions {revisions:?}")
                })?;
            }
            let stored = fixture
                .world
                .alerts
                .alert(alert)
                .await
                .map_err(|error| failed("alert", error))?
                .ok_or_else(|| CheckFailed::new("alert gone"))?;
            let last = revisions.last().map(|(_, state)| state.clone());
            check(&ctx, last == Some(stored.state.clone()), || {
                format!("{alert:?}: last event {last:?}, stored {:?}", stored.state)
            })?;
        }
        Ok(())
    }
}

crosstalk_sim::sim_test! {
    /// INV-541 (`surface.alert.action-change-announced`): an applied
    /// acknowledgement announces `Changed::Alert`, and a client re-querying
    /// on the event sees the new state.
    fn acknowledge_announces_alert_change(ctx) {
        let mut fixture = Fixture::new().await;
        let scene = fixture.scene().await;
        fixture.published();
        let triager = fixture.caller(Who::Triager).await;
        let viewer = fixture.caller(Who::Viewer).await;
        let mut stream = fixture
            .surface
            .subscribe(&viewer, Resume::Fresh)
            .await
            .map_err(|error| failed("subscribe", error))?;
        let mut rng = ctx.rng();
        let wait = DurationRange::new(Duration::ZERO, Duration::from_secs(3))
            .map_err(|error| failed("wait", error))?;
        tokio::time::sleep(rng.duration_in(wait)).await;
        let result = fixture.surface.act(&triager, OperatorAction::Acknowledge { alert: scene.alert }).await;
        check(&ctx, result == Ok(ActionOutcome::Applied), || format!("{result:?}"))?;
        // Relay what the stores published to the feed, as the bus would.
        let mut announced = false;
        for event in fixture.published() {
            if let BusEvent::Changed(changed) = event {
                announced |= changed == Changed::Alert(scene.alert);
                fixture.feed.append(changed).await.map_err(|error| failed("append", error))?;
            }
        }
        check(&ctx, announced, || "no Changed::Alert".to_owned())?;
        loop {
            let item = stream.next().await.map_err(|end| failed("stream", end))?;
            if let LiveItem::Event { event: UiEvent::AlertChanged { id }, .. } = item {
                check(&ctx, id == scene.alert, || format!("{id:?}"))?;
                break;
            }
        }
        let alert = fixture
            .surface
            .alert(&viewer, scene.alert)
            .await
            .map_err(|error| failed("alert", error))?
            .ok_or_else(|| CheckFailed::new("alert gone"))?;
        check(&ctx, matches!(alert.state, AlertState::Acknowledged { .. }), || {
            format!("{:?}", alert.state)
        })
    }
}
