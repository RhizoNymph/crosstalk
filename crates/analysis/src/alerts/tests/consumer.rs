//! The `alerts` stage end to end over the Postgres store: drafts from
//! detections, suppression by sanction and verdict, rule upkeep on a ready
//! topic version and on start, and skipping a redelivered envelope.

use std::num::NonZeroU64;

use crosstalk_memory::analysis::fakes::{FakeRuleContext, fake_model};
use crosstalk_memory::model::build::{agent, catalog, channel, raw, resource, transmission, ts};
use crosstalk_memory::support::Outbox;
use crosstalk_spec::aggregates::alert::{
    AlertState, AlertSubject, BuiltinRule, SuppressReason, TriageOutcome,
};
use crosstalk_spec::derived::flow::channel::Seed;
use crosstalk_spec::derived::flow::channel::policy::{Decision, Policy, PolicyAuthor};
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::derived::flow::verdict::{Verdict, VerdictRevision};
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::EventId;
use crosstalk_spec::interfaces::l6_analysis::alerts::AlertReads;

use super::super::consumer::AlertsStage;
use super::reads::page;
use super::store;
use crate::pg::testing::database;

fn envelope(n: u64, event: BusEvent) -> Envelope {
    Envelope {
        id: EventId::from_ulid(raw(n)),
        at: ts(n),
        event,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn stage_drafts_triages_and_suppresses() {
    let Some(db) = database("stage_drafts_triages_and_suppresses").await else {
        return;
    };
    let (store, _, _) = store(db.pool().clone()).await;
    let mut context = FakeRuleContext::default();
    context
        .policies
        .insert(channel(1), Policy::Unreviewed(None));
    let topics = catalog(2, 0.5, Outbox::none()).unwrap_or_else(|| panic!("catalog"));
    let mut stage = AlertsStage::new(store.clone(), context, topics);
    let other = fake_model("other", std::num::NonZeroU16::MIN);
    assert_eq!(stage.start(&other).await, Ok(Vec::new()));

    let discovered = envelope(
        10,
        BusEvent::Detect(DetectEvent::ChannelDiscovered {
            channel: channel(1),
            seed: Seed {
                resource: resource(1),
                first_transmission: transmission(1),
                opened_at: ts(1),
            },
        }),
    );
    let handled = stage
        .handle(&discovered)
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    assert!(
        matches!(handled.triaged.as_slice(), [TriageOutcome::Opened(alert)] if alert.rule == BuiltinRule::NewChannel.id())
    );
    // The same envelope again is skipped.
    assert!(
        stage
            .handle(&discovered)
            .await
            .is_ok_and(|handled| handled.skipped)
    );

    let confirmed = envelope(
        11,
        BusEvent::Detect(DetectEvent::TransmissionConfirmed {
            transmission: transmission(1),
            from: agent(1),
            to: agent(2),
            route: Route::Channel(channel(1)),
            at: ts(11),
            matched_bytes: NonZeroU64::MIN,
        }),
    );
    let handled = stage
        .handle(&confirmed)
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(handled.triaged.len(), 1);

    let sanctioned = envelope(
        12,
        BusEvent::Insight(InsightEvent::PolicyChanged {
            channel: channel(1),
            policy: Policy::Sanctioned(Decision {
                by: PolicyAuthor::Config,
                at: ts(12),
                note: None,
            }),
        }),
    );
    assert_eq!(
        stage
            .handle(&sanctioned)
            .await
            .map(|handled| handled.suppressed),
        Ok(2)
    );

    let verdict = envelope(
        13,
        BusEvent::Detect(DetectEvent::VerdictSet {
            transmission: transmission(1),
            verdict: Some(Verdict::FalseDetection),
            revision: VerdictRevision::FIRST,
            by: crosstalk_memory::model::build::operator(1),
            at: ts(13),
        }),
    );
    assert_eq!(
        stage
            .handle(&verdict)
            .await
            .map(|handled| handled.suppressed),
        Ok(0)
    );

    let alerts = store
        .alerts(&Default::default(), &page(10, None))
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(alerts.items().len(), 2);
    for alert in alerts.items() {
        assert_eq!(alert.subject, AlertSubject::Channel(channel(1)));
        assert_eq!(
            alert.state,
            AlertState::Suppressed {
                at: ts(12),
                reason: SuppressReason::ChannelSanctioned
            }
        );
    }
}
