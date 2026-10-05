//! Triage and the alert lifecycle on Postgres: opening and deduplication,
//! suppression by sanction, disable and false-detection verdict, and the
//! acknowledge and resolve actions, with what each publishes.

use crosstalk_memory::model::build::{agent, channel, operator, transmission, ts};
use crosstalk_spec::aggregates::alert::{
    Alert, AlertDraft, AlertRevision, AlertState, AlertSubject, BuiltinRule, SuppressReason,
    TriageOutcome,
};
use crosstalk_spec::derived::flow::verdict::{Verdict, VerdictRevision};
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::ids::{AlertId, AlertRuleId};
use crosstalk_spec::interfaces::l6_analysis::alerts::{AlertActionError, AlertActions, AlertReads};
use crosstalk_spec::interfaces::l6_analysis::{AlertRuleStore, AlertTriage};
use crosstalk_spec::support::Change;

use super::{TestStore, drain, store};
use crate::pg::testing::database;

const NEW_CHANNEL: AlertRuleId = BuiltinRule::NewChannel.id();
const SUSPECTED: AlertRuleId = BuiltinRule::SuspectedTransmission.id();

fn draft(rule: AlertRuleId, subject: AlertSubject, at: u64) -> AlertDraft {
    AlertDraft {
        rule,
        subject,
        raised_at: ts(at),
    }
}

async fn triage(store: &mut TestStore, draft: AlertDraft) -> TriageOutcome {
    store
        .triage(draft)
        .await
        .unwrap_or_else(|error| panic!("triage: {error:?}"))
}

/// Triage `draft`, which must open an alert; returns it.
async fn open(store: &mut TestStore, draft: AlertDraft) -> Alert {
    match triage(store, draft).await {
        TriageOutcome::Opened(alert) => alert,
        other => panic!("expected an opened alert, got {other:?}"),
    }
}

async fn read(store: &TestStore, id: AlertId) -> Alert {
    store
        .alert(id)
        .await
        .unwrap_or_else(|error| panic!("{error:?}"))
        .unwrap_or_else(|| panic!("no alert {id:?}"))
}

async fn acknowledge(store: &mut TestStore, id: AlertId) -> Result<Change, AlertActionError> {
    store.acknowledge(id, operator(1), ts(50)).await
}

async fn resolve(store: &mut TestStore, id: AlertId) -> Result<Change, AlertActionError> {
    store
        .resolve(id, operator(1), ts(60), Some("fixed".to_owned()))
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn opened_alert_copies_draft_and_starts_open() {
    let Some(db) = database("opened_alert_copies_draft_and_starts_open").await else {
        return;
    };
    let (mut store, _, mut events) = store(db.pool().clone()).await;
    drain(&mut events);
    let alert = open(
        &mut store,
        draft(NEW_CHANNEL, AlertSubject::Channel(channel(1)), 7),
    )
    .await;
    assert_eq!(
        (
            alert.rule,
            alert.subject,
            alert.raised_at,
            alert.occurrences,
            alert.state.clone()
        ),
        (
            NEW_CHANNEL,
            AlertSubject::Channel(channel(1)),
            ts(7),
            1,
            AlertState::Open
        )
    );
    assert_eq!(read(&store, alert.id).await, alert);
    assert_eq!(
        drain(&mut events),
        vec![
            BusEvent::Insight(InsightEvent::AlertOpened(alert.clone())),
            BusEvent::Changed(Changed::Alert(alert.id)),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn draft_folds_into_open_alert() {
    let Some(db) = database("draft_folds_into_open_alert").await else {
        return;
    };
    let (mut store, _, _) = store(db.pool().clone()).await;
    let alert = open(
        &mut store,
        draft(NEW_CHANNEL, AlertSubject::Channel(channel(1)), 7),
    )
    .await;
    let outcome = triage(
        &mut store,
        draft(NEW_CHANNEL, AlertSubject::Channel(channel(1)), 8),
    )
    .await;
    assert_eq!(outcome, TriageOutcome::Deduplicated { into: alert.id });
}

#[tokio::test(flavor = "multi_thread")]
async fn draft_folds_into_acknowledged_alert() {
    let Some(db) = database("draft_folds_into_acknowledged_alert").await else {
        return;
    };
    let (mut store, _, _) = store(db.pool().clone()).await;
    let alert = open(
        &mut store,
        draft(NEW_CHANNEL, AlertSubject::Channel(channel(1)), 7),
    )
    .await;
    assert_eq!(acknowledge(&mut store, alert.id).await, Ok(Change::Applied));
    let outcome = triage(
        &mut store,
        draft(NEW_CHANNEL, AlertSubject::Channel(channel(1)), 8),
    )
    .await;
    assert_eq!(outcome, TriageOutcome::Deduplicated { into: alert.id });
}

#[tokio::test(flavor = "multi_thread")]
async fn draft_after_resolved_opens_new_alert() {
    let Some(db) = database("draft_after_resolved_opens_new_alert").await else {
        return;
    };
    let (mut store, _, _) = store(db.pool().clone()).await;
    let first = open(
        &mut store,
        draft(NEW_CHANNEL, AlertSubject::Channel(channel(1)), 7),
    )
    .await;
    assert_eq!(acknowledge(&mut store, first.id).await, Ok(Change::Applied));
    assert_eq!(resolve(&mut store, first.id).await, Ok(Change::Applied));
    let second = open(
        &mut store,
        draft(NEW_CHANNEL, AlertSubject::Channel(channel(1)), 8),
    )
    .await;
    assert_ne!(second.id, first.id);
}

#[tokio::test(flavor = "multi_thread")]
async fn draft_after_suppressed_opens_new_alert() {
    let Some(db) = database("draft_after_suppressed_opens_new_alert").await else {
        return;
    };
    let (mut store, _, _) = store(db.pool().clone()).await;
    let first = open(
        &mut store,
        draft(NEW_CHANNEL, AlertSubject::Channel(channel(1)), 7),
    )
    .await;
    assert_eq!(store.channel_sanctioned(channel(1), ts(9)).await, Ok(1));
    let second = open(
        &mut store,
        draft(NEW_CHANNEL, AlertSubject::Channel(channel(1)), 10),
    )
    .await;
    assert_ne!(second.id, first.id);
}

#[tokio::test(flavor = "multi_thread")]
async fn dedup_increments_occurrences_by_one() {
    let Some(db) = database("dedup_increments_occurrences_by_one").await else {
        return;
    };
    let (mut store, _, mut events) = store(db.pool().clone()).await;
    let alert = open(
        &mut store,
        draft(SUSPECTED, AlertSubject::Transmission(transmission(1)), 7),
    )
    .await;
    drain(&mut events);
    for expected in 2..=4u32 {
        triage(
            &mut store,
            draft(SUSPECTED, AlertSubject::Transmission(transmission(1)), 8),
        )
        .await;
        let stored = read(&store, alert.id).await;
        assert_eq!(stored.occurrences, expected);
        let revision = AlertRevision::new(
            std::num::NonZeroU32::new(expected).unwrap_or(std::num::NonZeroU32::MIN),
        );
        assert_eq!(
            drain(&mut events),
            vec![
                BusEvent::Insight(InsightEvent::AlertChanged {
                    alert: stored.clone(),
                    revision,
                }),
                BusEvent::Changed(Changed::Alert(alert.id)),
            ]
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn dedup_preserves_state_and_raised_at() {
    let Some(db) = database("dedup_preserves_state_and_raised_at").await else {
        return;
    };
    let (mut store, _, _) = store(db.pool().clone()).await;
    let alert = open(
        &mut store,
        draft(NEW_CHANNEL, AlertSubject::Channel(channel(2)), 7),
    )
    .await;
    assert_eq!(acknowledge(&mut store, alert.id).await, Ok(Change::Applied));
    let acknowledged = read(&store, alert.id).await;
    triage(
        &mut store,
        draft(NEW_CHANNEL, AlertSubject::Channel(channel(2)), 99),
    )
    .await;
    let after = read(&store, alert.id).await;
    assert_eq!(
        after,
        Alert {
            occurrences: 2,
            ..acknowledged
        }
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn alert_store_rejects_second_active_alert_per_key() {
    let Some(db) = database("alert_store_rejects_second_active_alert_per_key").await else {
        return;
    };
    let (store, _, _) = store(db.pool().clone()).await;
    // Concurrent drafts of one key open one alert; the rest fold into it.
    let tasks: Vec<_> = (0..8u64)
        .map(|n| {
            let mut store = store.clone();
            tokio::spawn(async move {
                store
                    .triage(draft(NEW_CHANNEL, AlertSubject::Channel(channel(5)), n))
                    .await
            })
        })
        .collect();
    let mut opened = 0;
    for task in tasks {
        match task.await {
            Ok(Ok(TriageOutcome::Opened(_))) => opened += 1,
            Ok(Ok(TriageOutcome::Deduplicated { .. })) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(opened, 1);
    let page = store
        .alerts(
            &Default::default(),
            &crosstalk_spec::paging::PageRequest {
                size: crosstalk_spec::paging::PageSize::new(20).unwrap_or_else(|_| panic!("size")),
                after: None,
            },
        )
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(page.items().len(), 1);
    assert_eq!(page.items()[0].occurrences, 8);
    // The schema itself refuses a second active alert for the key.
    let second = sqlx::query(
        "INSERT INTO analysis.alerts (id, rule, subject, subject_kind, subject_id, state, alert, revision) \
         SELECT 'X' || id, rule, subject, subject_kind, subject_id, 'open', alert, 1 FROM analysis.alerts",
    )
    .execute(db.pool())
    .await;
    assert!(second.is_err(), "a second active alert per key was stored");
}

#[tokio::test(flavor = "multi_thread")]
async fn resolved_alert_rejects_every_action() {
    let Some(db) = database("resolved_alert_rejects_every_action").await else {
        return;
    };
    let (mut store, _, mut events) = store(db.pool().clone()).await;
    let alert = open(
        &mut store,
        draft(NEW_CHANNEL, AlertSubject::Channel(channel(1)), 7),
    )
    .await;
    assert_eq!(acknowledge(&mut store, alert.id).await, Ok(Change::Applied));
    assert_eq!(resolve(&mut store, alert.id).await, Ok(Change::Applied));
    let resolved = read(&store, alert.id).await;
    assert!(matches!(resolved.state, AlertState::Resolved { .. }));
    drain(&mut events);
    assert_eq!(
        acknowledge(&mut store, alert.id).await,
        Err(AlertActionError::NotActive(alert.id))
    );
    assert_eq!(
        resolve(&mut store, alert.id).await,
        Err(AlertActionError::NotActive(alert.id))
    );
    assert_eq!(store.channel_sanctioned(channel(1), ts(70)).await, Ok(0));
    assert_eq!(read(&store, alert.id).await, resolved);
    assert_eq!(drain(&mut events), Vec::new());
}

#[tokio::test(flavor = "multi_thread")]
async fn suppressed_alert_rejects_every_action() {
    let Some(db) = database("suppressed_alert_rejects_every_action").await else {
        return;
    };
    let (mut store, _, mut events) = store(db.pool().clone()).await;
    let alert = open(
        &mut store,
        draft(NEW_CHANNEL, AlertSubject::Channel(channel(1)), 7),
    )
    .await;
    assert_eq!(store.rule_disabled(NEW_CHANNEL, ts(8)).await, Ok(1));
    let suppressed = read(&store, alert.id).await;
    assert_eq!(
        suppressed.state,
        AlertState::Suppressed {
            at: ts(8),
            reason: SuppressReason::RuleDisabled,
        }
    );
    drain(&mut events);
    assert_eq!(
        acknowledge(&mut store, alert.id).await,
        Err(AlertActionError::NotActive(alert.id))
    );
    assert_eq!(
        resolve(&mut store, alert.id).await,
        Err(AlertActionError::NotActive(alert.id))
    );
    assert_eq!(store.channel_sanctioned(channel(1), ts(9)).await, Ok(0));
    assert_eq!(read(&store, alert.id).await, suppressed);
    assert_eq!(drain(&mut events), Vec::new());
}

#[tokio::test(flavor = "multi_thread")]
async fn open_alert_cannot_be_resolved_directly() {
    let Some(db) = database("open_alert_cannot_be_resolved_directly").await else {
        return;
    };
    let (mut store, _, _) = store(db.pool().clone()).await;
    let alert = open(
        &mut store,
        draft(NEW_CHANNEL, AlertSubject::Channel(channel(1)), 7),
    )
    .await;
    assert_eq!(
        resolve(&mut store, alert.id).await,
        Err(AlertActionError::NotAcknowledged(alert.id))
    );
    assert_eq!(read(&store, alert.id).await.state, AlertState::Open);
    // Acknowledging twice is unchanged; then it resolves.
    assert_eq!(acknowledge(&mut store, alert.id).await, Ok(Change::Applied));
    assert_eq!(
        acknowledge(&mut store, alert.id).await,
        Ok(Change::Unchanged)
    );
    assert_eq!(resolve(&mut store, alert.id).await, Ok(Change::Applied));
}

#[tokio::test(flavor = "multi_thread")]
async fn resolving_an_open_alert_is_refused() {
    let Some(db) = database("resolving_an_open_alert_is_refused").await else {
        return;
    };
    let (mut store, _, mut events) = store(db.pool().clone()).await;
    let alert = open(
        &mut store,
        draft(SUSPECTED, AlertSubject::Transmission(transmission(3)), 7),
    )
    .await;
    drain(&mut events);
    assert_eq!(
        resolve(&mut store, alert.id).await,
        Err(AlertActionError::NotAcknowledged(alert.id))
    );
    assert_eq!(read(&store, alert.id).await, alert);
    assert_eq!(drain(&mut events), Vec::new());
    let unknown = AlertId::from_ulid(1 << 100);
    assert_eq!(
        resolve(&mut store, unknown).await,
        Err(AlertActionError::UnknownAlert(unknown))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rule_disabled_suppresses_its_active_alerts() {
    let Some(db) = database("rule_disabled_suppresses_its_active_alerts").await else {
        return;
    };
    let (mut store, _, _) = store(db.pool().clone()).await;
    let open_one = open(
        &mut store,
        draft(NEW_CHANNEL, AlertSubject::Channel(channel(1)), 7),
    )
    .await;
    let acked = open(
        &mut store,
        draft(NEW_CHANNEL, AlertSubject::Channel(channel(2)), 7),
    )
    .await;
    assert_eq!(acknowledge(&mut store, acked.id).await, Ok(Change::Applied));
    assert_eq!(store.rule_disabled(NEW_CHANNEL, ts(20)).await, Ok(2));
    for id in [open_one.id, acked.id] {
        assert_eq!(
            read(&store, id).await.state,
            AlertState::Suppressed {
                at: ts(20),
                reason: SuppressReason::RuleDisabled,
            }
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn rule_disabled_leaves_other_rules_and_resolved_alerts() {
    let Some(db) = database("rule_disabled_leaves_other_rules_and_resolved_alerts").await else {
        return;
    };
    let (mut store, _, _) = store(db.pool().clone()).await;
    let resolved = open(
        &mut store,
        draft(NEW_CHANNEL, AlertSubject::Channel(channel(1)), 7),
    )
    .await;
    assert_eq!(
        acknowledge(&mut store, resolved.id).await,
        Ok(Change::Applied)
    );
    assert_eq!(resolve(&mut store, resolved.id).await, Ok(Change::Applied));
    let other = open(
        &mut store,
        draft(SUSPECTED, AlertSubject::Transmission(transmission(1)), 7),
    )
    .await;
    let before = (
        read(&store, resolved.id).await,
        read(&store, other.id).await,
    );
    assert_eq!(store.rule_disabled(NEW_CHANNEL, ts(20)).await, Ok(0));
    assert_eq!(
        (
            read(&store, resolved.id).await,
            read(&store, other.id).await
        ),
        before
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn sanction_suppresses_open_and_acknowledged_channel_alerts() {
    let Some(db) = database("sanction_suppresses_open_and_acknowledged_channel_alerts").await
    else {
        return;
    };
    let (mut store, _, _) = store(db.pool().clone()).await;
    let open_one = open(
        &mut store,
        draft(NEW_CHANNEL, AlertSubject::Channel(channel(1)), 7),
    )
    .await;
    let acked = open(
        &mut store,
        draft(
            BuiltinRule::UnreviewedTraffic.id(),
            AlertSubject::Channel(channel(1)),
            7,
        ),
    )
    .await;
    assert_eq!(acknowledge(&mut store, acked.id).await, Ok(Change::Applied));
    assert_eq!(store.channel_sanctioned(channel(1), ts(30)).await, Ok(2));
    for id in [open_one.id, acked.id] {
        assert_eq!(
            read(&store, id).await.state,
            AlertState::Suppressed {
                at: ts(30),
                reason: SuppressReason::ChannelSanctioned,
            }
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sanction_leaves_transmission_alerts_and_resolved_alerts() {
    let Some(db) = database("sanction_leaves_transmission_alerts_and_resolved_alerts").await else {
        return;
    };
    let (mut store, _, _) = store(db.pool().clone()).await;
    let resolved = open(
        &mut store,
        draft(NEW_CHANNEL, AlertSubject::Channel(channel(1)), 7),
    )
    .await;
    assert_eq!(
        acknowledge(&mut store, resolved.id).await,
        Ok(Change::Applied)
    );
    assert_eq!(resolve(&mut store, resolved.id).await, Ok(Change::Applied));
    let about = open(
        &mut store,
        draft(SUSPECTED, AlertSubject::Transmission(transmission(1)), 7),
    )
    .await;
    let elsewhere = open(
        &mut store,
        draft(NEW_CHANNEL, AlertSubject::Channel(channel(2)), 7),
    )
    .await;
    let agent_alert = open(
        &mut store,
        draft(SUSPECTED, AlertSubject::Agent(agent(1)), 7),
    )
    .await;
    let before = [
        read(&store, resolved.id).await,
        read(&store, about.id).await,
        read(&store, elsewhere.id).await,
        read(&store, agent_alert.id).await,
    ];
    assert_eq!(store.channel_sanctioned(channel(1), ts(30)).await, Ok(0));
    let after = [
        read(&store, resolved.id).await,
        read(&store, about.id).await,
        read(&store, elsewhere.id).await,
        read(&store, agent_alert.id).await,
    ];
    assert_eq!(after, before);
}

#[tokio::test(flavor = "multi_thread")]
async fn sanction_suppresses_superseded_channel_alerts() {
    let Some(db) = database("sanction_suppresses_superseded_channel_alerts").await else {
        return;
    };
    let (mut store, directory, _) = store(db.pool().clone()).await;
    let old = open(
        &mut store,
        draft(NEW_CHANNEL, AlertSubject::Channel(channel(1)), 7),
    )
    .await;
    let unrelated = open(
        &mut store,
        draft(NEW_CHANNEL, AlertSubject::Channel(channel(3)), 7),
    )
    .await;
    assert!(directory.supersede(channel(1), channel(2)).is_ok());
    // Later traffic raises alerts on the superseding channel: a new key.
    let new = open(
        &mut store,
        draft(NEW_CHANNEL, AlertSubject::Channel(channel(2)), 8),
    )
    .await;
    assert_eq!(store.channel_sanctioned(channel(2), ts(30)).await, Ok(2));
    for id in [old.id, new.id] {
        assert!(matches!(
            read(&store, id).await.state,
            AlertState::Suppressed { .. }
        ));
    }
    assert_eq!(read(&store, unrelated.id).await.state, AlertState::Open);
}

/// A promotion that sanctions a channel and supersedes others reaches
/// triage as a sanction of the promoted channel: the alerts stored under
/// the channels it superseded are suppressed with its own.
#[tokio::test(flavor = "multi_thread")]
async fn pg_sanction_after_promotion() {
    let Some(db) = database("pg_sanction_after_promotion").await else {
        return;
    };
    let (mut store, directory, mut events) = store(db.pool().clone()).await;
    let a = open(
        &mut store,
        draft(NEW_CHANNEL, AlertSubject::Channel(channel(4)), 7),
    )
    .await;
    let b = open(
        &mut store,
        draft(
            BuiltinRule::UnreviewedTraffic.id(),
            AlertSubject::Channel(channel(5)),
            7,
        ),
    )
    .await;
    let t = open(
        &mut store,
        draft(SUSPECTED, AlertSubject::Transmission(transmission(4)), 7),
    )
    .await;
    assert!(directory.supersede(channel(4), channel(9)).is_ok());
    assert!(directory.supersede(channel(5), channel(9)).is_ok());
    drain(&mut events);
    // Sanctioning a superseded id resolves it too.
    assert_eq!(store.channel_sanctioned(channel(4), ts(40)).await, Ok(2));
    let changed: Vec<BusEvent> = drain(&mut events);
    assert_eq!(changed.len(), 4);
    for id in [a.id, b.id] {
        assert_eq!(
            read(&store, id).await.state,
            AlertState::Suppressed {
                at: ts(40),
                reason: SuppressReason::ChannelSanctioned,
            }
        );
    }
    assert_eq!(read(&store, t.id).await.state, AlertState::Open);
}

#[tokio::test(flavor = "multi_thread")]
async fn false_detection_suppresses_transmission_alerts() {
    let Some(db) = database("false_detection_suppresses_transmission_alerts").await else {
        return;
    };
    let (mut store, _, _) = store(db.pool().clone()).await;
    let first = open(
        &mut store,
        draft(SUSPECTED, AlertSubject::Transmission(transmission(1)), 7),
    )
    .await;
    let user = store
        .create(
            crosstalk_spec::aggregates::alert::RuleName::new("q")
                .unwrap_or_else(|_| panic!("name")),
            crosstalk_spec::aggregates::alert::UserRule::SemanticQuery {
                text: crosstalk_spec::aggregates::alert::RuleQueryText::new("wiki")
                    .unwrap_or_else(|_| panic!("text")),
                threshold: crosstalk_memory::model::build::similarity(0.5)
                    .unwrap_or_else(|| panic!("threshold")),
            },
            Vec::new(),
            operator(1),
            ts(2_000),
        )
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    let second = open(
        &mut store,
        draft(user, AlertSubject::Transmission(transmission(1)), 7),
    )
    .await;
    assert_eq!(
        acknowledge(&mut store, second.id).await,
        Ok(Change::Applied)
    );
    let other = open(
        &mut store,
        draft(SUSPECTED, AlertSubject::Transmission(transmission(2)), 7),
    )
    .await;
    let suppressed = store
        .transmission_judged(
            transmission(1),
            Some(Verdict::FalseDetection),
            VerdictRevision::FIRST,
            ts(50),
        )
        .await;
    assert_eq!(suppressed, Ok(2));
    for id in [first.id, second.id] {
        assert_eq!(
            read(&store, id).await.state,
            AlertState::Suppressed {
                at: ts(50),
                reason: SuppressReason::OperatorRejected,
            }
        );
    }
    assert_eq!(read(&store, other.id).await.state, AlertState::Open);
    // While the verdict holds, drafts about the transmission open nothing.
    let rejected = triage(
        &mut store,
        draft(SUSPECTED, AlertSubject::Transmission(transmission(1)), 60),
    )
    .await;
    assert_eq!(rejected, TriageOutcome::OperatorRejected);
}

#[tokio::test(flavor = "multi_thread")]
async fn verdict_withdrawal_reopens_nothing() {
    let Some(db) = database("verdict_withdrawal_reopens_nothing").await else {
        return;
    };
    let (mut store, _, _) = store(db.pool().clone()).await;
    let revision = |n: u32| {
        VerdictRevision::new(std::num::NonZeroU32::new(n).unwrap_or(std::num::NonZeroU32::MIN))
    };
    let alert = open(
        &mut store,
        draft(SUSPECTED, AlertSubject::Transmission(transmission(1)), 7),
    )
    .await;
    let judged = store
        .transmission_judged(
            transmission(1),
            Some(Verdict::FalseDetection),
            revision(2),
            ts(10),
        )
        .await;
    assert_eq!(judged, Ok(1));
    let suppressed = read(&store, alert.id).await;
    // A stale revision, a withdrawal and a Genuine verdict change no alert.
    for (verdict, n) in [
        (Some(Verdict::Genuine), 1),
        (None, 3),
        (Some(Verdict::Genuine), 4),
    ] {
        let changed = store
            .transmission_judged(transmission(1), verdict, revision(n), ts(20))
            .await;
        assert_eq!(changed, Ok(0));
        assert_eq!(read(&store, alert.id).await, suppressed);
    }
    // Later drafts open alerts again.
    let reopened = triage(
        &mut store,
        draft(SUSPECTED, AlertSubject::Transmission(transmission(1)), 30),
    )
    .await;
    assert!(matches!(reopened, TriageOutcome::Opened(_)));
    // A Genuine verdict on an open alert changes nothing either.
    let genuine = store
        .transmission_judged(transmission(1), Some(Verdict::Genuine), revision(5), ts(40))
        .await;
    assert_eq!(genuine, Ok(0));
}
