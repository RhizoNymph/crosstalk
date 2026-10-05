//! The live feed: committed changes publish the stores' `Changed`
//! notifications, a subscription resumes as `FeedWindow::resume` plans,
//! events reach only callers who may receive them, heartbeats carry the
//! newest cursor, and a slow stream ends `Lagged`.

use std::num::NonZeroU32;
use std::time::Duration;

use crosstalk_spec::aggregates::alert::{AlertState, AlertSubject};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::channel::policy::Policy;
use crosstalk_spec::derived::flow::channel::policy::PolicyKind;
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::interfaces::l8_surface::live::{
    FeedEpoch, LiveConfig, LiveCursor, LiveEnd, LiveFeed, LiveItem, LiveStream, Resume,
    ResyncReason, UiEvent,
};
use crosstalk_spec::interfaces::l8_surface::{
    ActionOutcome, OperatorAction, OperatorActions, Permission, QueryApi, QueryError,
};

use super::super::FixtureBackend;
use super::super::live::FeedStream;
use super::super::world::ChannelKey;
use super::actions_support::{agent, channel, find_alert, merge};
use super::reads_support::params;
use super::{caller, fresh, researcher, week};

const WAIT: Duration = Duration::from_secs(5);

fn config(buffer: u32, heartbeat_ms: u64, retention_ms: u64) -> LiveConfig {
    LiveConfig::new(
        NonZeroU32::new(buffer).expect("buffer"),
        Duration::from_millis(heartbeat_ms),
        Duration::from_millis(retention_ms),
    )
    .expect("config")
}

/// A fresh world whose heartbeats never get in the way.
fn quiet() -> FixtureBackend {
    fresh().with_live_config(config(256, 600_000, 1_200_000))
}

async fn item(stream: &mut FeedStream) -> Result<LiveItem, LiveEnd> {
    tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("an item in time")
}

async fn event(stream: &mut FeedStream) -> (LiveCursor, UiEvent) {
    match item(stream).await {
        Ok(LiveItem::Event { cursor, event }) => (cursor, event),
        other => panic!("expected an event, got {other:?}"),
    }
}

/// Every event the stream holds now, until it would wait.
async fn drain(stream: &mut FeedStream) -> Vec<UiEvent> {
    let mut out = Vec::new();
    while let Ok(next) = tokio::time::timeout(Duration::from_millis(50), stream.next()).await {
        match next {
            Ok(LiveItem::Event { event, .. }) => out.push(event),
            other => panic!("expected events, got {other:?}"),
        }
    }
    out
}

async fn open_alert(b: &FixtureBackend) -> crosstalk_spec::ids::AlertId {
    find_alert(b, |a| matches!(a.state, AlertState::Open)).await
}

#[tokio::test]
async fn subscribing_needs_view() {
    let b = quiet();
    assert_eq!(
        b.subscribe(&caller(&[Permission::Triage]), Resume::Fresh)
            .await
            .err(),
        Some(QueryError::Forbidden {
            missing: Permission::View
        })
    );
}

#[tokio::test]
async fn actions_publish_what_their_stores_changed() {
    let b = quiet();
    let c = researcher();
    let mut stream = b.subscribe(&c, Resume::Fresh).await.expect("subscribe");

    let alert = open_alert(&b).await;
    b.act(&c, OperatorAction::Acknowledge { alert })
        .await
        .expect("ack");
    let (first, ack) = event(&mut stream).await;
    assert_eq!(ack, UiEvent::AlertChanged { id: alert });
    assert_eq!(first.epoch, b.feed_epoch());
    assert_eq!(first.seq, 1);

    // Unchanged and refused calls publish nothing.
    let again = b
        .act(&c, OperatorAction::Acknowledge { alert })
        .await
        .expect("ack");
    assert_eq!(again, ActionOutcome::Unchanged);
    assert!(
        b.act(
            &caller(&[Permission::View]),
            OperatorAction::Acknowledge { alert }
        )
        .await
        .is_err()
    );
    assert!(drain(&mut stream).await.is_empty());

    // A verdict names its transmission.
    let tx = b
        .world
        .transmissions
        .iter()
        .find(|t| t.transmission.state.judgeable().is_ok())
        .expect("judgeable")
        .transmission
        .id;
    b.act(
        &c,
        OperatorAction::SetVerdict {
            transmission: tx,
            verdict: Some(Verdict::Genuine),
            note: None,
        },
    )
    .await
    .expect("verdict");
    assert_eq!(
        event(&mut stream).await.1,
        UiEvent::VerdictChanged { id: tx }
    );

    // Sanctioning names the channel and every alert it suppressed.
    let unused = {
        let state = b.state.read().await;
        state
            .alerts
            .iter()
            .filter(|a| matches!(a.state, AlertState::Open))
            .find_map(|a| match a.subject {
                AlertSubject::Channel(id) => state
                    .channels
                    .get(&id)
                    .filter(|record| {
                        record.channel().origin.supersession().is_none()
                            && !matches!(record.channel().policy, Policy::Sanctioned(_))
                    })
                    .map(|_| id),
                _ => None,
            })
            .expect("an open alert about a channel not yet sanctioned")
    };
    b.act(
        &c,
        OperatorAction::SetPolicy {
            channel: unused,
            policy: PolicyKind::Sanctioned,
            note: None,
        },
    )
    .await
    .expect("policy");
    let events = drain(&mut stream).await;
    assert_eq!(
        events.first(),
        Some(&UiEvent::ChannelChanged { id: unused })
    );
    let suppressed: Vec<_> = b
        .state
        .read()
        .await
        .alerts
        .iter()
        .filter(|a| matches!(a.state, AlertState::Suppressed { at, .. } if at == super::super::clock::NOW))
        .map(|a| UiEvent::AlertChanged { id: a.id })
        .collect();
    assert!(!suppressed.is_empty(), "the channel had open alerts");
    for alert in suppressed {
        assert!(events.contains(&alert), "{alert:?}");
    }
}

#[tokio::test]
async fn promotions_and_merges_name_every_id_they_repoint() {
    let b = quiet();
    let c = researcher();
    let mut stream = b.subscribe(&c, Resume::Fresh).await.expect("subscribe");
    let (wiki, talk) = (
        channel(&b, ChannelKey::HijackedWiki),
        channel(&b, ChannelKey::WikiTalk),
    );
    b.act(
        &c,
        OperatorAction::PromoteChannel {
            channel: wiki,
            pattern: crosstalk_spec::derived::flow::resource::ResourcePattern::UrlPrefix {
                host: crosstalk_spec::derived::flow::resource::Host("wiki.example.org".to_owned()),
                path_prefix: "/wiki".to_owned(),
            },
            policy: PolicyKind::Unsanctioned,
            note: None,
        },
    )
    .await
    .expect("promote");
    let events = drain(&mut stream).await;
    assert_eq!(
        events[..2],
        [
            UiEvent::ChannelChanged { id: wiki },
            UiEvent::ChannelChanged { id: talk }
        ]
    );

    let ActionOutcome::Merged(merge_id) = b.act(&c, merge(&b, "pi2", "pi1")).await.expect("merge")
    else {
        panic!("merged");
    };
    let mut merged = drain(&mut stream).await;
    let mut expected: Vec<_> = ["pi2", "pi1", "al2", "al3"]
        .into_iter()
        .map(|key| UiEvent::AgentChanged { id: agent(&b, key) })
        .collect();
    merged.sort_by_key(|e| format!("{e:?}"));
    expected.sort_by_key(|e| format!("{e:?}"));
    assert_eq!(merged, expected, "source, target and the repointed aliases");

    b.act(&c, OperatorAction::Unmerge { merge: merge_id })
        .await
        .expect("unmerge");
    let mut unmerged = drain(&mut stream).await;
    unmerged.sort_by_key(|e| format!("{e:?}"));
    assert_eq!(unmerged, expected, "source, former target and the restored");
}

#[tokio::test]
async fn pins_name_their_versions_and_fits_reach_content_callers_only() {
    let b = quiet();
    let c = researcher();
    let viewer = caller(&[Permission::View]);
    let mut all = b.subscribe(&c, Resume::Fresh).await.expect("subscribe");
    let mut view_only = b
        .subscribe(&viewer, Resume::Fresh)
        .await
        .expect("subscribe");

    b.act(
        &c,
        OperatorAction::PinTopicVersion {
            version: TopicModelVersion(2),
        },
    )
    .await
    .expect("pin");
    let pinned = UiEvent::TopicVersionReady {
        version: TopicModelVersion(2),
    };
    assert_eq!(event(&mut all).await.1, pinned);
    assert_eq!(event(&mut view_only).await.1, pinned);

    let week = week();
    let id = b
        .fit_projection(&c, week.window, &week.topology_filter(), params(5, 200))
        .await
        .expect("fit");
    let (cursor, ready) = event(&mut all).await;
    assert_eq!(ready, UiEvent::ProjectionReady { id });
    assert!(
        drain(&mut view_only).await.is_empty(),
        "a projection event needs Content"
    );
    // Passed over, it still advances the stream: a resume from the
    // viewer's last cursor replays nothing it may see.
    let mut resumed = b
        .subscribe(
            &viewer,
            Resume::From(LiveCursor {
                seq: cursor.seq - 1,
                ..cursor
            }),
        )
        .await
        .expect("resume");
    assert!(drain(&mut resumed).await.is_empty());
}

#[tokio::test]
async fn resuming_replays_or_resyncs_as_the_window_plans() {
    let b = quiet();
    let c = researcher();
    let epoch = b.feed_epoch();
    let first = open_alert(&b).await;
    b.act(&c, OperatorAction::Acknowledge { alert: first })
        .await
        .expect("ack");
    let second = open_alert(&b).await;
    b.act(&c, OperatorAction::Acknowledge { alert: second })
        .await
        .expect("ack");

    let mut replay = b
        .subscribe(&c, Resume::From(LiveCursor { epoch, seq: 1 }))
        .await
        .expect("replay");
    assert_eq!(
        event(&mut replay).await,
        (
            LiveCursor { epoch, seq: 2 },
            UiEvent::AlertChanged { id: second }
        )
    );
    // Live after the replay, without repeating it.
    let third = open_alert(&b).await;
    b.act(&c, OperatorAction::Acknowledge { alert: third })
        .await
        .expect("ack");
    assert_eq!(
        event(&mut replay).await,
        (
            LiveCursor { epoch, seq: 3 },
            UiEvent::AlertChanged { id: third }
        )
    );

    let resync = async |resume| {
        let mut stream = b.subscribe(&c, resume).await.expect("subscribe");
        item(&mut stream).await
    };
    let head = LiveCursor { epoch, seq: 3 };
    assert_eq!(
        resync(Resume::From(LiveCursor {
            epoch: FeedEpoch(epoch.0 + 1),
            seq: 1
        }))
        .await,
        Ok(LiveItem::Resync {
            cursor: head,
            reason: ResyncReason::OtherEpoch
        })
    );
    assert_eq!(
        resync(Resume::From(LiveCursor { epoch, seq: 9 })).await,
        Ok(LiveItem::Resync {
            cursor: head,
            reason: ResyncReason::AheadOfHead
        })
    );
    assert_eq!(
        resync(Resume::from_last_event_id(Some("not-a-cursor"))).await,
        Ok(LiveItem::Resync {
            cursor: head,
            reason: ResyncReason::Unreadable
        })
    );
}

#[tokio::test]
async fn entries_past_retention_resync() {
    let b = fresh().with_live_config(config(256, 40, 80));
    let c = researcher();
    let epoch = b.feed_epoch();
    let alert = open_alert(&b).await;
    b.act(&c, OperatorAction::Acknowledge { alert })
        .await
        .expect("ack");
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut stream = b
        .subscribe(&c, Resume::From(LiveCursor { epoch, seq: 0 }))
        .await
        .expect("subscribe");
    assert_eq!(
        item(&mut stream).await,
        Ok(LiveItem::Resync {
            cursor: LiveCursor { epoch, seq: 1 },
            reason: ResyncReason::Expired
        })
    );
    // Heartbeats follow, carrying the newest cursor passed.
    assert_eq!(
        item(&mut stream).await,
        Ok(LiveItem::Heartbeat {
            cursor: LiveCursor { epoch, seq: 1 }
        })
    );
}

#[tokio::test]
async fn a_stream_that_falls_behind_ends_lagged() {
    let b = fresh().with_live_config(config(2, 600_000, 1_200_000));
    let c = researcher();
    let mut slow = b.subscribe(&c, Resume::Fresh).await.expect("subscribe");
    for _ in 0..4 {
        let alert = open_alert(&b).await;
        b.act(&c, OperatorAction::Acknowledge { alert })
            .await
            .expect("ack");
    }
    assert_eq!(item(&mut slow).await, Err(LiveEnd::Lagged));
    assert_eq!(item(&mut slow).await, Err(LiveEnd::Lagged), "closed");
    // A new subscription from the log is unaffected.
    let mut caught_up = b
        .subscribe(
            &c,
            Resume::From(LiveCursor {
                epoch: b.feed_epoch(),
                seq: 0,
            }),
        )
        .await
        .expect("resume");
    assert_eq!(drain(&mut caught_up).await.len(), 4);
}
