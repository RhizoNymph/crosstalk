//! `InMemoryAlertStore`: rules, triage, suppression and the alert
//! lifecycle.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU16;
use std::sync::Arc;

use crosstalk_spec::aggregates::alert::{
    AlertDraft, AlertRevision, AlertRuleConfig, AlertState, AlertSubject, BuiltinRule, RuleName,
    RuleQueryText, RuleStatus, SuppressReason, TriageOutcome, UserRule, WatchedTopics,
};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::derived::flow::verdict::{Verdict, VerdictRevision};
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::ids::{AlertId, AlertRuleId};
use crosstalk_spec::interfaces::l6_analysis::{AlertRuleStore, AlertTriage, TopicCatalog};
use crosstalk_spec::interfaces::l8_surface::{AlertFilter, AlertStateKind};
use crosstalk_spec::paging::{PageRequest, PageSize};
use crosstalk_spec::support::{Change, NonEmpty};

use super::support::{at, fit_ready};
use crate::analysis::alerts::triage::AlertActionError;
use crate::analysis::alerts::{AlertStoreConfig, InMemoryAlertStore};
use crate::analysis::aliases::StaticDirectory;
use crate::analysis::catalog::{InMemoryTopicCatalog, TopicVersions};
use crate::analysis::fakes::{FakeEmbedder, fake_model};
use crate::analysis::support::{ManualClock, Published};
use crate::model::build::{
    catalog, channel, operator, similarity, sink, topic_id, transmission, ts,
};

pub(super) type Store = InMemoryAlertStore<FakeEmbedder, StaticDirectory>;

pub(super) struct World {
    pub(super) store: Store,
    pub(super) directory: StaticDirectory,
    pub(super) clock: ManualClock,
    pub(super) catalog: InMemoryTopicCatalog,
}

pub(super) fn embedder(name: &str) -> FakeEmbedder {
    FakeEmbedder::new(fake_model(name, NonZeroU16::new(8).unwrap()), 64)
}

pub(super) fn world_with(embedder: FakeEmbedder) -> World {
    let clock = ManualClock::new(ts(1_000));
    let directory = StaticDirectory::new();
    let config = AlertStoreConfig {
        rules: AlertRuleConfig {
            default_remap_threshold: similarity(0.8).unwrap(),
        },
        sinks: BTreeSet::from([sink(1), sink(2)]),
        builtins: BTreeMap::from([(
            BuiltinRule::SanctionedUnused,
            (RuleStatus::Disabled, vec![]),
        )]),
    };
    let store =
        InMemoryAlertStore::new(config, embedder, directory.clone(), Arc::new(clock.clone()));
    let catalog = catalog(2, 0.5, clock.clone()).unwrap();
    World {
        store,
        directory,
        clock,
        catalog,
    }
}

pub(super) fn world() -> World {
    world_with(embedder("fake"))
}

pub(super) fn draft(rule: AlertRuleId, subject: AlertSubject, raised: u64) -> AlertDraft {
    AlertDraft {
        rule,
        subject,
        raised_at: at(raised),
    }
}

pub(super) fn traffic() -> AlertRuleId {
    BuiltinRule::UnreviewedTraffic.id()
}

pub(super) fn on_channel(n: u64) -> AlertSubject {
    AlertSubject::Channel(channel(n))
}

pub(super) fn on_transmission(n: u64) -> AlertSubject {
    AlertSubject::Transmission(transmission(n))
}

pub(super) fn name(text: &str) -> RuleName {
    RuleName::new(text).unwrap()
}

pub(super) async fn open(
    store: &mut Store,
    rule: AlertRuleId,
    subject: AlertSubject,
    raised: u64,
) -> AlertId {
    match store.triage(draft(rule, subject, raised)).await.unwrap() {
        TriageOutcome::Opened(alert) => alert.id,
        other => panic!("expected an opened alert, got {other:?}"),
    }
}

pub(super) fn state_of(store: &Store, id: AlertId) -> AlertState {
    store.alert(id).unwrap().state
}

/// Version 1 with topics 1 and 2, made current for rules.
pub(super) async fn version_one(world: &World) -> TopicModelVersion {
    let v1 = fit_ready(
        &world.catalog,
        10,
        &[(1, [1.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0])],
    );
    let lineage = stored_lineage(&world.catalog, TopicModelVersion(0)).await;
    world
        .store
        .topic_version_ready(&lineage, world.catalog.topic_ids(v1))
        .unwrap();
    v1
}

pub(super) async fn stored_lineage(
    catalog: &InMemoryTopicCatalog,
    from: TopicModelVersion,
) -> crosstalk_spec::aggregates::topic_history::TopicLineage {
    catalog.lineage(from).await.unwrap().unwrap()
}

pub(super) fn watch(version: TopicModelVersion, topics: &[u64]) -> UserRule {
    UserRule::WatchedTopic {
        topics: WatchedTopics {
            version,
            topics: NonEmpty::from_vec(topics.iter().copied().map(topic_id).collect()).unwrap(),
        },
        remap_threshold: None,
    }
}

pub(super) fn semantic(text: &str) -> UserRule {
    UserRule::SemanticQuery {
        text: RuleQueryText::new(text).unwrap(),
        threshold: similarity(0.5).unwrap(),
    }
}

#[tokio::test]
async fn opened_alert_copies_draft_and_starts_open() {
    // analysis.triage.opened-initial
    let mut world = world();
    let outcome = world
        .store
        .triage(draft(traffic(), on_channel(1), 5))
        .await
        .unwrap();
    let TriageOutcome::Opened(alert) = outcome else {
        panic!("expected an opened alert");
    };
    assert_eq!(alert.rule, traffic());
    assert_eq!(alert.subject, on_channel(1));
    assert_eq!(alert.raised_at, at(5));
    assert_eq!(alert.occurrences, 1);
    assert_eq!(alert.state, AlertState::Open);
    assert_eq!(
        world.store.alert_revision(alert.id),
        Some(AlertRevision::OPENED)
    );
    let published = world.store.drain_published();
    assert!(
        published.contains(&Published::Insight(InsightEvent::AlertOpened(
            alert.clone()
        )))
    );
    assert!(published.contains(&Published::Changed(Changed::Alert(alert.id))));
}

#[tokio::test]
async fn draft_folds_into_open_alert() {
    // analysis.triage.dedup-iff-active
    let mut world = world();
    let id = open(&mut world.store, traffic(), on_channel(1), 5).await;
    assert_eq!(
        world.store.triage(draft(traffic(), on_channel(1), 9)).await,
        Ok(TriageOutcome::Deduplicated { into: id })
    );
    // Another subject or rule opens its own alert.
    open(&mut world.store, traffic(), on_channel(2), 9).await;
    open(
        &mut world.store,
        BuiltinRule::NewChannel.id(),
        on_channel(1),
        9,
    )
    .await;
}

#[tokio::test]
async fn draft_folds_into_acknowledged_alert() {
    let mut world = world();
    let id = open(&mut world.store, traffic(), on_channel(1), 5).await;
    world.store.acknowledge(id, operator(1), at(6)).unwrap();
    assert_eq!(
        world.store.triage(draft(traffic(), on_channel(1), 9)).await,
        Ok(TriageOutcome::Deduplicated { into: id })
    );
}

#[tokio::test]
async fn draft_after_resolved_opens_new_alert() {
    let mut world = world();
    let id = open(&mut world.store, traffic(), on_channel(1), 5).await;
    world.store.acknowledge(id, operator(1), at(6)).unwrap();
    world.store.resolve(id, operator(1), at(7), None).unwrap();
    let again = open(&mut world.store, traffic(), on_channel(1), 9).await;
    assert_ne!(again, id);
}

#[tokio::test]
async fn draft_after_suppressed_opens_new_alert() {
    let mut world = world();
    let id = open(&mut world.store, traffic(), on_channel(1), 5).await;
    world.store.channel_sanctioned(channel(1)).await.unwrap();
    let again = open(&mut world.store, traffic(), on_channel(1), 9).await;
    assert_ne!(again, id);
}

#[tokio::test]
async fn dedup_increments_occurrences_by_one() {
    // analysis.triage.dedup-increments
    let mut world = world();
    let id = open(&mut world.store, traffic(), on_channel(1), 5).await;
    for expected in 2..=4 {
        world
            .store
            .triage(draft(traffic(), on_channel(1), 9))
            .await
            .unwrap();
        assert_eq!(world.store.alert(id).unwrap().occurrences, expected);
    }
    assert_eq!(
        world
            .store
            .alert_revision(id)
            .map(|revision| revision.get().get()),
        Some(4)
    );
}

#[tokio::test]
async fn dedup_preserves_state_and_raised_at() {
    let mut world = world();
    let id = open(&mut world.store, traffic(), on_channel(1), 5).await;
    world.store.acknowledge(id, operator(7), at(6)).unwrap();
    let before = world.store.alert(id).unwrap();
    world
        .store
        .triage(draft(traffic(), on_channel(1), 50))
        .await
        .unwrap();
    let after = world.store.alert(id).unwrap();
    assert_eq!(after.raised_at, before.raised_at);
    assert_eq!(after.state, before.state);
    assert_eq!(after.rule, before.rule);
    assert_eq!(after.subject, before.subject);
    assert_eq!(after.occurrences, before.occurrences + 1);
}

#[tokio::test]
async fn open_alert_cannot_be_resolved_directly() {
    // analysis.alert.state-transitions
    let mut world = world();
    let id = open(&mut world.store, traffic(), on_channel(1), 5).await;
    assert_eq!(
        world.store.resolve(id, operator(1), at(6), None),
        Err(AlertActionError::NotAcknowledged(id))
    );
    assert_eq!(state_of(&world.store, id), AlertState::Open);
    assert_eq!(
        world.store.acknowledge(id, operator(1), at(6)),
        Ok(Change::Applied)
    );
    assert_eq!(
        world.store.acknowledge(id, operator(2), at(7)),
        Ok(Change::Unchanged)
    );
    assert_eq!(
        state_of(&world.store, id),
        AlertState::Acknowledged {
            by: operator(1),
            at: at(6)
        }
    );
}

#[tokio::test]
async fn resolved_alert_rejects_every_action() {
    let mut world = world();
    let id = open(&mut world.store, traffic(), on_channel(1), 5).await;
    world.store.acknowledge(id, operator(1), at(6)).unwrap();
    world
        .store
        .resolve(id, operator(1), at(7), Some("fixed".to_owned()))
        .unwrap();
    let resolved = world.store.alert(id).unwrap();
    assert_eq!(
        world.store.acknowledge(id, operator(1), at(8)),
        Err(AlertActionError::NotActive(id))
    );
    assert_eq!(
        world.store.resolve(id, operator(1), at(8), None),
        Err(AlertActionError::NotActive(id))
    );
    world.store.channel_sanctioned(channel(1)).await.unwrap();
    world.store.rule_disabled(traffic()).await.unwrap();
    assert_eq!(world.store.alert(id).unwrap(), resolved);
}

#[tokio::test]
async fn suppressed_alert_rejects_every_action() {
    let mut world = world();
    let id = open(&mut world.store, traffic(), on_channel(1), 5).await;
    world.clock.set(at(77));
    world.store.rule_disabled(traffic()).await.unwrap();
    let suppressed = world.store.alert(id).unwrap();
    assert_eq!(
        suppressed.state,
        AlertState::Suppressed {
            at: at(77),
            reason: SuppressReason::RuleDisabled
        }
    );
    assert_eq!(
        world.store.acknowledge(id, operator(1), at(80)),
        Err(AlertActionError::NotActive(id))
    );
    assert_eq!(
        world.store.resolve(id, operator(1), at(80), None),
        Err(AlertActionError::NotActive(id))
    );
    world.store.channel_sanctioned(channel(1)).await.unwrap();
    assert_eq!(world.store.alert(id).unwrap(), suppressed);
    assert_eq!(
        world
            .store
            .acknowledge(AlertId::from_ulid(1), operator(1), at(80)),
        Err(AlertActionError::UnknownAlert(AlertId::from_ulid(1)))
    );
}

#[tokio::test]
async fn rule_disabled_suppresses_its_active_alerts() {
    // analysis.triage.rule-disabled-suppresses
    let mut world = world();
    let open_one = open(&mut world.store, traffic(), on_channel(1), 5).await;
    let acked = open(&mut world.store, traffic(), on_channel(2), 5).await;
    world.store.acknowledge(acked, operator(1), at(6)).unwrap();
    assert_eq!(world.store.rule_disabled(traffic()).await, Ok(2));
    for id in [open_one, acked] {
        assert!(matches!(
            state_of(&world.store, id),
            AlertState::Suppressed {
                reason: SuppressReason::RuleDisabled,
                ..
            }
        ));
    }
    assert_eq!(world.store.rule_disabled(traffic()).await, Ok(0));
}

#[tokio::test]
async fn rule_disabled_leaves_other_rules_and_resolved_alerts() {
    let mut world = world();
    let other = open(
        &mut world.store,
        BuiltinRule::NewChannel.id(),
        on_channel(1),
        5,
    )
    .await;
    let resolved = open(&mut world.store, traffic(), on_channel(1), 5).await;
    world
        .store
        .acknowledge(resolved, operator(1), at(6))
        .unwrap();
    world
        .store
        .resolve(resolved, operator(1), at(7), None)
        .unwrap();
    assert_eq!(world.store.rule_disabled(traffic()).await, Ok(0));
    assert_eq!(state_of(&world.store, other), AlertState::Open);
    assert!(matches!(
        state_of(&world.store, resolved),
        AlertState::Resolved { .. }
    ));
}

#[tokio::test]
async fn sanction_suppresses_open_and_acknowledged_channel_alerts() {
    // analysis.triage.sanction-suppresses
    let mut world = world();
    let a = open(&mut world.store, traffic(), on_channel(1), 5).await;
    let b = open(
        &mut world.store,
        BuiltinRule::NewChannel.id(),
        on_channel(1),
        5,
    )
    .await;
    world.store.acknowledge(b, operator(1), at(6)).unwrap();
    let elsewhere = open(&mut world.store, traffic(), on_channel(2), 5).await;
    assert_eq!(world.store.channel_sanctioned(channel(1)).await, Ok(2));
    for id in [a, b] {
        assert!(matches!(
            state_of(&world.store, id),
            AlertState::Suppressed {
                reason: SuppressReason::ChannelSanctioned,
                ..
            }
        ));
    }
    assert_eq!(state_of(&world.store, elsewhere), AlertState::Open);
}

#[tokio::test]
async fn sanction_leaves_transmission_alerts_and_resolved_alerts() {
    let mut world = world();
    let content = open(
        &mut world.store,
        BuiltinRule::SuspectedTransmission.id(),
        on_transmission(1),
        5,
    )
    .await;
    assert_eq!(world.store.channel_sanctioned(channel(1)).await, Ok(0));
    assert_eq!(state_of(&world.store, content), AlertState::Open);
}

#[tokio::test]
async fn sanction_suppresses_superseded_channel_alerts() {
    // analysis.alert.sanction-covers-superseded
    let mut world = world();
    let old = open(&mut world.store, traffic(), on_channel(2), 5).await;
    let other = open(&mut world.store, traffic(), on_channel(3), 5).await;
    world.directory.supersede(channel(2), channel(1)).unwrap();
    assert_eq!(world.store.channel_sanctioned(channel(1)).await, Ok(1));
    assert!(matches!(
        state_of(&world.store, old),
        AlertState::Suppressed { .. }
    ));
    assert_eq!(state_of(&world.store, other), AlertState::Open);
    // Dedup compares stored subjects: traffic on the superseding channel
    // opens its own alert.
    open(&mut world.store, traffic(), on_channel(1), 9).await;
}

#[tokio::test]
async fn false_detection_suppresses_transmission_alerts() {
    // analysis.triage.false-detection-suppresses
    let mut world = world();
    let a = open(
        &mut world.store,
        BuiltinRule::SuspectedTransmission.id(),
        on_transmission(1),
        5,
    )
    .await;
    let b = open(&mut world.store, traffic(), on_transmission(1), 5).await;
    let other = open(&mut world.store, traffic(), on_transmission(2), 5).await;
    assert_eq!(
        world
            .store
            .transmission_judged(
                transmission(1),
                Some(Verdict::FalseDetection),
                VerdictRevision::FIRST
            )
            .await,
        Ok(2)
    );
    for id in [a, b] {
        assert!(matches!(
            state_of(&world.store, id),
            AlertState::Suppressed {
                reason: SuppressReason::OperatorRejected,
                ..
            }
        ));
    }
    assert_eq!(state_of(&world.store, other), AlertState::Open);
}

#[tokio::test]
async fn rejected_subject_opens_nothing() {
    // analysis.triage.rejected-subject-opens-nothing, sequentially
    let mut world = world();
    world
        .store
        .transmission_judged(
            transmission(1),
            Some(Verdict::FalseDetection),
            VerdictRevision::FIRST,
        )
        .await
        .unwrap();
    assert_eq!(
        world
            .store
            .triage(draft(traffic(), on_transmission(1), 5))
            .await,
        Ok(TriageOutcome::OperatorRejected)
    );
    assert!(world.store.all_alerts().is_empty());
}

#[tokio::test]
async fn verdict_withdrawal_reopens_nothing() {
    // analysis.triage.verdict-reopens-nothing
    let mut world = world();
    let id = open(&mut world.store, traffic(), on_transmission(1), 5).await;
    let second = VerdictRevision::FIRST.next().unwrap();
    let third = second.next().unwrap();
    world
        .store
        .transmission_judged(transmission(1), Some(Verdict::FalseDetection), second)
        .await
        .unwrap();
    let suppressed = world.store.alert(id).unwrap();
    // A stale false detection, a withdrawal and a genuine verdict change
    // no alert.
    assert_eq!(
        world
            .store
            .transmission_judged(
                transmission(1),
                Some(Verdict::FalseDetection),
                VerdictRevision::FIRST
            )
            .await,
        Ok(0)
    );
    assert_eq!(
        world
            .store
            .transmission_judged(transmission(1), None, third)
            .await,
        Ok(0)
    );
    assert_eq!(world.store.alert(id).unwrap(), suppressed);
    // Later drafts open alerts again.
    let reopened = open(&mut world.store, traffic(), on_transmission(1), 9).await;
    assert_ne!(reopened, id);
    let fourth = third.next().unwrap();
    assert_eq!(
        world
            .store
            .transmission_judged(transmission(1), Some(Verdict::Genuine), fourth)
            .await,
        Ok(0)
    );
    assert_eq!(state_of(&world.store, reopened), AlertState::Open);
}

#[tokio::test]
async fn disabled_rule_triage_is_rule_inactive() {
    // analysis.rule.only-enabled-drafts (triage's re-check) and
    // analysis.rule.disable-no-late-alerts, sequentially
    let mut world = world();
    let id = open(&mut world.store, traffic(), on_channel(1), 5).await;
    assert_eq!(
        world.store.set_enabled(traffic(), false, operator(1)).await,
        Ok(Change::Applied)
    );
    assert!(matches!(
        state_of(&world.store, id),
        AlertState::Suppressed { .. }
    ));
    assert_eq!(
        world.store.triage(draft(traffic(), on_channel(1), 9)).await,
        Ok(TriageOutcome::RuleInactive)
    );
    assert_eq!(
        world
            .store
            .triage(draft(BuiltinRule::SanctionedUnused.id(), on_channel(1), 9))
            .await,
        Ok(TriageOutcome::RuleInactive)
    );
    assert_eq!(
        world
            .store
            .triage(draft(AlertRuleId::from_ulid(1 << 90), on_channel(1), 9))
            .await,
        Ok(TriageOutcome::RuleInactive)
    );
    world
        .store
        .set_enabled(traffic(), true, operator(1))
        .await
        .unwrap();
    open(&mut world.store, traffic(), on_channel(1), 10).await;
}

#[tokio::test]
async fn alerts_page_filters_by_state_and_resolved_channel() {
    let mut world = world();
    let a = open(&mut world.store, traffic(), on_channel(2), 5).await;
    let b = open(&mut world.store, traffic(), on_transmission(1), 5).await;
    let c = open(&mut world.store, traffic(), on_channel(3), 5).await;
    world.store.acknowledge(c, operator(1), at(6)).unwrap();
    world.directory.supersede(channel(2), channel(1)).unwrap();
    let route_of = |id| (id == transmission(1)).then_some(Route::Channel(channel(2)));
    let page = PageRequest {
        size: PageSize::new(1).unwrap(),
        after: None,
    };
    let filter = AlertFilter {
        states: vec![AlertStateKind::Open],
        channel: Some(channel(1)),
    };
    let mut seen = Vec::new();
    let mut request = page;
    loop {
        let page = world
            .store
            .alerts_page(&filter, route_of, &request)
            .unwrap();
        let (items, next) = page.into_parts();
        seen.extend(items.iter().map(|alert| alert.id));
        match next {
            Some(cursor) => request.after = Some(cursor),
            None => break,
        }
    }
    assert_eq!(seen, vec![b, a]);
    let acknowledged = AlertFilter {
        states: vec![AlertStateKind::Acknowledged],
        channel: None,
    };
    let first = PageRequest {
        size: PageSize::new(5).unwrap(),
        after: None,
    };
    let page = world
        .store
        .alerts_page(&acknowledged, route_of, &first)
        .unwrap();
    assert_eq!(
        page.items()
            .iter()
            .map(|alert| alert.id)
            .collect::<Vec<_>>(),
        vec![c]
    );
}

#[tokio::test]
async fn alert_changes_are_announced_with_revisions() {
    // analysis.alert.change-announced (store half)
    let mut world = world();
    let id = open(&mut world.store, traffic(), on_channel(1), 5).await;
    world
        .store
        .triage(draft(traffic(), on_channel(1), 6))
        .await
        .unwrap();
    world.store.acknowledge(id, operator(1), at(7)).unwrap();
    let changes: Vec<_> = world
        .store
        .drain_published()
        .into_iter()
        .filter_map(|event| match event {
            Published::Insight(InsightEvent::AlertChanged { alert, revision }) => {
                Some((alert.occurrences, revision.get().get()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(changes, vec![(2, 2), (2, 3)]);
}
