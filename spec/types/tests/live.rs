use std::collections::HashSet;
use std::num::NonZeroU32;
use std::time::Duration;

use crate::aggregates::projection::ProjectionToken;
use crate::aggregates::topic::TopicModelVersion;
use crate::events::changed::Changed;
use crate::events::{BusEvent, Subject};
use crate::ids::{AlertId, AlertRuleId};
use crate::interfaces::l8_surface::Permission;
use crate::interfaces::l8_surface::live::{
    FeedEpoch, FeedWindow, FloorAboveHead, InvalidLiveConfig, LiveConfig, LiveCursor, LiveItem,
    Resume, ResumePlan, ResyncReason, UiEvent,
};
use crate::support::Watermark;
use crate::tests::fixtures::{agent, at, channel};
use crate::tests::operators::caller;

/// One notification of every variant, with the event it becomes.
fn every_change() -> Vec<(Changed, UiEvent)> {
    let alert = AlertId::from_ulid(1);
    let rule = AlertRuleId::from_ulid(2);
    let version = TopicModelVersion(3);
    let token = ProjectionToken::new(version, 1);
    let watermark = Watermark(at(60));
    vec![
        (Changed::Alert(alert), UiEvent::AlertChanged { id: alert }),
        (
            Changed::Channel(channel(1)),
            UiEvent::ChannelChanged { id: channel(1) },
        ),
        (
            Changed::Agent(agent(1)),
            UiEvent::AgentChanged { id: agent(1) },
        ),
        (Changed::Rule(rule), UiEvent::RuleChanged { id: rule }),
        (
            Changed::Watermark(watermark),
            UiEvent::Watermark { at: watermark },
        ),
        (
            Changed::TopicVersion(version),
            UiEvent::TopicVersionReady { version },
        ),
        (
            Changed::Projection(token),
            UiEvent::ProjectionReady { id: token },
        ),
    ]
}

#[test]
fn every_notification_becomes_the_event_naming_the_same_id() {
    let changes = every_change();
    for (changed, event) in &changes {
        assert_eq!(UiEvent::from(*changed), *event);
    }
    let events: HashSet<_> = changes.iter().map(|(_, event)| *event).collect();
    assert_eq!(events.len(), changes.len());
}

#[test]
fn notifications_travel_on_one_subject() {
    for (changed, _) in every_change() {
        assert_eq!(BusEvent::Changed(changed).subject(), Subject::Changed);
    }
}

#[test]
fn only_projection_ready_needs_content() {
    for (_, event) in every_change() {
        let expected = if matches!(event, UiEvent::ProjectionReady { .. }) {
            Permission::Content
        } else {
            Permission::View
        };
        assert_eq!(event.required_permission(), expected, "{event:?}");
    }
}

#[test]
fn viewer_sees_every_event_but_projection_ready() {
    let viewer = caller(1, &[Permission::View]);
    let reader = caller(2, &[Permission::View, Permission::Content]);
    for (_, event) in every_change() {
        let projection = matches!(event, UiEvent::ProjectionReady { .. });
        assert_eq!(event.visible_to(&viewer), !projection, "{event:?}");
        assert!(event.visible_to(&reader), "{event:?}");
    }
    let auditor = caller(3, &[Permission::Audit]);
    for (_, event) in every_change() {
        assert!(!event.visible_to(&auditor), "{event:?}");
    }
}

#[test]
fn cursor_round_trips_through_its_encoding() {
    let cursor = LiveCursor {
        epoch: FeedEpoch(7),
        seq: 42,
    };
    assert_eq!(cursor.encode(), "7-42");
    assert_eq!(LiveCursor::decode(&cursor.encode()), Some(cursor));
    let extreme = LiveCursor {
        epoch: FeedEpoch(u64::MAX),
        seq: 0,
    };
    assert_eq!(LiveCursor::decode(&extreme.encode()), Some(extreme));
}

#[test]
fn cursor_decoding_rejects_anything_else() {
    for text in [
        "",
        "7",
        "7-",
        "-42",
        "+7-42",
        "7-+42",
        "7-42-1",
        "a-1",
        "7 -42",
        "18446744073709551616-1",
    ] {
        assert_eq!(LiveCursor::decode(text), None, "{text:?}");
    }
}

#[test]
fn last_event_id_becomes_resume_point() {
    let cursor = LiveCursor {
        epoch: FeedEpoch(1),
        seq: 3,
    };
    assert_eq!(Resume::from_last_event_id(None), Resume::Fresh);
    assert_eq!(
        Resume::from_last_event_id(Some("1-3")),
        Resume::From(cursor)
    );
    assert_eq!(
        Resume::from_last_event_id(Some("garbage")),
        Resume::Unreadable
    );
}

#[test]
fn feed_window_rejects_floor_above_head() {
    assert_eq!(FeedWindow::new(FeedEpoch(1), 11, 10), Err(FloorAboveHead));
    assert!(FeedWindow::new(FeedEpoch(1), 10, 10).is_ok());
    assert!(FeedWindow::new(FeedEpoch(1), 0, 0).is_ok());
}

fn from(epoch: u64, seq: u64) -> Resume {
    Resume::From(LiveCursor {
        epoch: FeedEpoch(epoch),
        seq,
    })
}

#[test]
fn resume_replays_while_every_later_entry_is_retained() {
    let window = FeedWindow::new(FeedEpoch(1), 10, 20).expect("floor below head");
    assert_eq!(window.resume(from(1, 10)), ResumePlan::Replay { after: 10 });
    assert_eq!(window.resume(from(1, 15)), ResumePlan::Replay { after: 15 });
    assert_eq!(window.resume(from(1, 20)), ResumePlan::Replay { after: 20 });
    assert_eq!(
        window.head(),
        LiveCursor {
            epoch: FeedEpoch(1),
            seq: 20
        }
    );
}

#[test]
fn resume_resyncs_instead_of_losing_entries() {
    let window = FeedWindow::new(FeedEpoch(1), 10, 20).expect("floor below head");
    assert_eq!(
        window.resume(from(1, 9)),
        ResumePlan::Resync(ResyncReason::Expired)
    );
    assert_eq!(
        window.resume(from(2, 15)),
        ResumePlan::Resync(ResyncReason::OtherEpoch)
    );
    assert_eq!(
        window.resume(from(1, 21)),
        ResumePlan::Resync(ResyncReason::AheadOfHead)
    );
    assert_eq!(
        window.resume(Resume::Unreadable),
        ResumePlan::Resync(ResyncReason::Unreadable)
    );
    assert_eq!(window.resume(Resume::Fresh), ResumePlan::Live);
}

#[test]
fn live_config_requires_heartbeat_within_retention() {
    let buffer = NonZeroU32::new(256).expect("not zero");
    assert_eq!(
        LiveConfig::new(buffer, Duration::ZERO, Duration::from_secs(60)),
        Err(InvalidLiveConfig::ZeroHeartbeat)
    );
    assert_eq!(
        LiveConfig::new(buffer, Duration::from_secs(60), Duration::from_secs(60)),
        Err(InvalidLiveConfig::RetentionTooShort)
    );
    let config = LiveConfig::new(buffer, Duration::from_secs(15), Duration::from_secs(600))
        .expect("valid limits");
    assert_eq!(config.buffer(), buffer);
    assert_eq!(config.heartbeat(), Duration::from_secs(15));
    assert_eq!(config.retention(), Duration::from_secs(600));
}

#[test]
fn every_item_carries_its_cursor() {
    let cursor = LiveCursor {
        epoch: FeedEpoch(1),
        seq: 5,
    };
    let items = [
        LiveItem::Event {
            cursor,
            event: UiEvent::TopicVersionReady {
                version: TopicModelVersion(1),
            },
        },
        LiveItem::Resync {
            cursor,
            reason: ResyncReason::Expired,
        },
        LiveItem::Heartbeat { cursor },
    ];
    for item in items {
        assert_eq!(item.cursor(), cursor);
    }
}
