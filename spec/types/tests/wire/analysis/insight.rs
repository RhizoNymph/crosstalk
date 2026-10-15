//! The insight bus events (L6 to L8) on the wire, each inside the full
//! `Envelope` a node publishes to NATS:
//! `{"id": .., "at": .., "event": {"type": "insight", "data": {"type": .., "data": ..}}}`.
//! One golden per variant; the exhaustive `golden_name` match is the
//! reminder to add a new variant's golden.

use std::num::NonZeroU64;

use serde_json::{Value, json};

use super::super::harness::{assert_golden, assert_rejected};
use super::super::{ULID_A, ULID_B, ULID_C, id};
use super::{at, operator, sim, topic, version};
use crate::aggregates::alert::{
    Alert, AlertRevision, AlertRuleDef, AlertState, AlertSubject, ContentRule, RuleName,
    RuleRevision, RuleStatus, SuppressReason, TopicWatch, WatchedTopics,
};
use crate::aggregates::edge::{EdgeKey, TopicSlot};
use crate::derived::flow::channel::policy::{Decision, Policy, PolicyAuthor};
use crate::derived::flow::transmission::{Classification, DelegationDirection, Route};
use crate::events::insight::{ClassificationCause, InsightEvent};
use crate::events::{BusEvent, Envelope};
use crate::ids::{AgentId, AlertId, AlertRuleId, ChannelId, EventId, SinkId, TransmissionId};
use crate::support::{NonEmpty, TimeWindow, Watermark};

const AREA: &str = "insight";

fn agent(text: &str) -> AgentId {
    id(AgentId::from_ulid_text, text)
}

fn channel() -> ChannelId {
    id(ChannelId::from_ulid_text, ULID_C)
}

fn envelope(event: InsightEvent) -> Envelope {
    Envelope {
        id: id(EventId::from_ulid_text, ULID_C),
        at: at("12:00:00"),
        event: BusEvent::Insight(event),
    }
}

fn alert(occurrences: u32, state: AlertState) -> Alert {
    Alert {
        id: id(AlertId::from_ulid_text, ULID_A),
        rule: id(AlertRuleId::from_ulid_text, ULID_B),
        subject: AlertSubject::Transmission(id(TransmissionId::from_ulid_text, ULID_C)),
        raised_at: at("11:58:00"),
        occurrences,
        state,
    }
}

/// A watched-topic rule a re-fit just made stale.
fn stale_rule() -> AlertRuleDef {
    AlertRuleDef::load(
        id(AlertRuleId::from_ulid_text, ULID_B),
        RuleName::new("Credential handoffs").expect("a short name"),
        (operator(), at("09:15:00")),
        ContentRule::WatchedTopic {
            watch: TopicWatch::Stale {
                last: WatchedTopics {
                    version: version(2),
                    topics: NonEmpty::new(topic(0)),
                },
                unmapped_in: version(3),
                unmapped: NonEmpty::new(topic(0)),
            },
            remap_threshold: sim(0.75),
        },
        RuleStatus::Enabled,
        vec![id(SinkId::from_ulid_text, ULID_A)],
    )
    .expect("a generated id")
}

fn golden_name(event: &InsightEvent) -> &'static str {
    match event {
        InsightEvent::TransmissionClassified { .. } => "transmission_classified",
        InsightEvent::TopicVersionReady { .. } => "topic_version_ready",
        InsightEvent::TopicVersionActivated { .. } => "topic_version_activated",
        InsightEvent::TopicVersionDropped { .. } => "topic_version_dropped",
        InsightEvent::WatermarkAdvanced(_) => "watermark_advanced",
        InsightEvent::EdgeUpdated(_) => "edge_updated",
        InsightEvent::AlertOpened(_) => "alert_opened",
        InsightEvent::AlertChanged { .. } => "alert_changed",
        InsightEvent::AlertRuleChanged { .. } => "alert_rule_changed",
        InsightEvent::PolicyChanged { .. } => "policy_changed",
    }
}

/// One event of every variant.
fn every_event() -> Vec<InsightEvent> {
    let bucket = TimeWindow::new(at("11:00:00"), at("12:00:00")).expect("an hour");
    vec![
        InsightEvent::TransmissionClassified {
            cause: ClassificationCause::Confirmation,
            transmission: id(TransmissionId::from_ulid_text, ULID_C),
            from: agent(ULID_A),
            to: agent(ULID_B),
            route: Route::Channel(channel()),
            at: at("11:57:42"),
            matched_bytes: NonZeroU64::new(512).expect("non-zero"),
            classification: Classification {
                version: version(3),
                topic: Some(topic(3)),
                watched: true,
            },
        },
        InsightEvent::TopicVersionReady {
            version: version(3),
            transmissions: 48_213,
        },
        InsightEvent::TopicVersionActivated {
            version: version(3),
            previous: version(2),
        },
        InsightEvent::TopicVersionDropped {
            version: version(1),
        },
        InsightEvent::WatermarkAdvanced(Watermark(at("11:59:00"))),
        InsightEvent::EdgeUpdated(
            EdgeKey::new(
                agent(ULID_A),
                agent(ULID_B),
                Route::Delegation(DelegationDirection::ParentToChild),
                TopicSlot {
                    version: version(3),
                    topic: None,
                },
                bucket,
            )
            .expect("two agents"),
        ),
        InsightEvent::AlertOpened(alert(1, AlertState::Open)),
        InsightEvent::AlertChanged {
            alert: alert(
                3,
                AlertState::Suppressed {
                    at: at("11:59:30"),
                    reason: SuppressReason::OperatorRejected,
                },
            ),
            revision: AlertRevision::OPENED
                .next()
                .and_then(AlertRevision::next)
                .and_then(AlertRevision::next)
                .expect("4 fits"),
        },
        InsightEvent::AlertRuleChanged {
            rule: stale_rule(),
            revision: RuleRevision::CREATED.next().expect("2 fits"),
        },
        InsightEvent::PolicyChanged {
            channel: channel(),
            policy: Policy::Sanctioned(Decision {
                by: PolicyAuthor::Operator(operator()),
                at: at("11:59:50"),
                note: Some("the wiki is how the planner briefs the coder".into()),
            }),
        },
    ]
}

#[test]
fn insight_events_golden_in_envelopes() {
    let events = every_event();
    let mut names: Vec<&str> = events.iter().map(golden_name).collect();
    for event in events {
        assert_golden(AREA, golden_name(&event), &envelope(event));
    }
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), 10, "one event of each variant");
}

#[test]
fn classification_causes_golden() {
    fn declared(cause: ClassificationCause) -> ClassificationCause {
        match cause {
            ClassificationCause::Confirmation | ClassificationCause::Refit => cause,
        }
    }
    let causes = [
        ClassificationCause::Confirmation,
        ClassificationCause::Refit,
    ]
    .map(declared);
    assert_golden(AREA, "classification_causes", &causes.to_vec());
}

/// The envelope of the fixture event `name` as JSON, changed by `edit` on the
/// insight event's `data`.
fn event_json(name: &str, edit: impl FnOnce(&mut Value)) -> String {
    let event = every_event()
        .into_iter()
        .find(|event| golden_name(event) == name)
        .expect("a fixture event");
    let mut json = serde_json::to_value(envelope(event)).expect("an envelope encodes");
    edit(&mut json["event"]["data"]["data"]);
    json.to_string()
}

/// A node that does not know a variant or field fails the delivery, and a
/// rule or revision its constructor refuses is a decode error on the bus as
/// in a response.
#[test]
fn insight_events_refuse_unknown_and_invalid_payloads() {
    let unknown = event_json("topic_version_dropped", |_| {}).replace(
        r#""type":"topic_version_dropped""#,
        r#""type":"topic_version_archived""#,
    );
    assert_rejected::<Envelope>(&unknown, "unknown variant `topic_version_archived`");
    assert_rejected::<Envelope>(
        &event_json("topic_version_ready", |data| {
            data["fitted_topics"] = json!(15)
        }),
        "unknown field `fitted_topics`",
    );
    assert_rejected::<Envelope>(
        &event_json("alert_rule_changed", |data| data["revision"] = json!(0)),
        "invalid value: integer `0`",
    );
    assert_rejected::<Envelope>(
        &event_json("alert_changed", |data| data["revision"] = json!(0)),
        "invalid value: integer `0`",
    );
    assert_rejected::<Envelope>(
        &event_json("alert_rule_changed", |data| {
            data["rule"]["id"] = json!("00000000000000000000000003");
        }),
        "invalid alert rule: Reserved",
    );
    assert_rejected::<Envelope>(
        &event_json("transmission_classified", |data| {
            data["cause"] = json!("backfill");
        }),
        "unknown variant `backfill`",
    );
    assert_rejected::<ClassificationCause>(r#""backfill""#, "unknown variant `backfill`");
}
