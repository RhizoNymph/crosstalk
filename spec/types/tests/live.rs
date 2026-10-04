use std::collections::HashSet;
use std::num::{NonZeroU32, NonZeroU64};
use std::time::Duration;

use crate::aggregates::alert::{Alert, AlertRevision, AlertState, AlertSubject};
use crate::aggregates::edge::{EdgeKey, RouteKind, TopicSlot, TopologyFilter};
use crate::aggregates::topic::TopicModelVersion;
use crate::derived::flow::channel::policy::{Decision, PolicyAuthor, PolicyDecision, PolicyKind};
use crate::derived::flow::evidence::CoAccess;
use crate::derived::flow::transmission::{DelegationDirection, DirectCarrier, Route};
use crate::events::insight::InsightEvent;
use crate::events::{BusEvent, Subject};
use crate::ids::{AlertId, AlertRuleId, OperatorId, TopicId};
use crate::interfaces::l8_surface::live::{
    ChannelChange, FeedEpoch, FeedWindow, FloorAboveHead, InvalidLiveConfig, LiveConfig,
    LiveCursor, LiveItem, LiveScope, LiveUpdate, LiveUpdateKind, NoUpdateKinds, Resume, ResumePlan,
    ResyncReason, ScopeKeys, UpdateKinds,
};
use crate::interfaces::l8_surface::{Caller, Permission};
use crate::support::TimeWindow;
use crate::tests::fixtures::{
    access, agent, at, channel, read_access, resource, transmission, write_access,
};

fn alert() -> Alert {
    Alert {
        id: AlertId::from_ulid(1),
        rule: AlertRuleId::from_ulid(1),
        subject: AlertSubject::Channel(channel(1)),
        raised_at: at(9),
        occurrences: 1,
        state: AlertState::Open,
    }
}

fn edge_key() -> EdgeKey {
    EdgeKey::new(
        agent(1),
        agent(2),
        Route::Channel(channel(1)),
        TopicSlot {
            version: TopicModelVersion(1),
            topic: Some(TopicId::from_ulid(1)),
        },
        TimeWindow::new(at(0), at(60)).expect("non-empty"),
    )
    .expect("different agents")
}

fn co_access() -> CoAccess {
    CoAccess::new(
        &write_access(1, agent(1), resource(1), 1),
        &read_access(2, agent(2), resource(1), 2),
        Duration::from_secs(60),
    )
    .expect("valid co-access")
}

/// One update of every variant.
fn every_update() -> Vec<LiveUpdate> {
    vec![
        LiveUpdate::AlertOpened(alert()),
        LiveUpdate::AlertChanged {
            alert: alert(),
            revision: AlertRevision::OPENED.next().expect("2 fits"),
        },
        LiveUpdate::EdgeUpdated(edge_key()),
        LiveUpdate::ChannelDiscovered {
            channel: channel(1),
            first_access: access(1),
        },
        LiveUpdate::ChannelChanged {
            channel: channel(1),
            change: ChannelChange::CrossAccessed {
                co_access: co_access(),
                reader: agent(2),
            },
        },
        LiveUpdate::PolicyChanged {
            channel: channel(1),
            decision: PolicyDecision {
                kind: PolicyKind::Sanctioned,
                decision: Decision {
                    by: PolicyAuthor::Operator(OperatorId::from_ulid(1)),
                    at: at(5),
                    note: None,
                },
            },
        },
        LiveUpdate::TransmissionConfirmed {
            transmission: transmission(1),
            from: agent(1),
            to: agent(2),
            route: Route::Channel(channel(1)),
            at: at(8),
            matched_bytes: NonZeroU64::MIN,
        },
        LiveUpdate::TopicVersionActivated {
            version: TopicModelVersion(2),
        },
    ]
}

#[test]
fn every_kind_is_listed_once_and_has_an_update() {
    let listed: HashSet<LiveUpdateKind> = LiveUpdateKind::ALL.into_iter().collect();
    assert_eq!(listed.len(), LiveUpdateKind::ALL.len());
    let produced: HashSet<LiveUpdateKind> = every_update().iter().map(LiveUpdate::kind).collect();
    assert_eq!(produced, listed);
}

#[test]
fn only_transmissions_need_content() {
    for kind in LiveUpdateKind::ALL {
        let expected = if kind == LiveUpdateKind::TransmissionConfirmed {
            Permission::Content
        } else {
            Permission::View
        };
        assert_eq!(kind.required_permission(), expected, "{kind:?}");
    }
}

#[test]
fn every_kind_has_a_bus_source() {
    for kind in LiveUpdateKind::ALL {
        assert!(!kind.sources().is_empty(), "{kind:?}");
    }
    assert!(
        LiveUpdateKind::ChannelChanged
            .sources()
            .contains(&Subject::TransmissionConfirmed)
    );
}

#[test]
fn new_insight_events_have_their_own_subjects() {
    let changed = BusEvent::Insight(InsightEvent::AlertChanged {
        alert: alert(),
        revision: AlertRevision::OPENED,
    });
    let activated = BusEvent::Insight(InsightEvent::TopicVersionActivated {
        version: TopicModelVersion(2),
    });
    assert_eq!(changed.subject(), Subject::AlertChanged);
    assert_eq!(activated.subject(), Subject::TopicVersionActivated);
}

#[test]
fn alert_revisions_count_from_one() {
    assert_eq!(AlertRevision::OPENED.get(), NonZeroU32::MIN);
    let second = AlertRevision::OPENED.next().expect("2 fits");
    assert_eq!(second.get().get(), 2);
    assert!(second > AlertRevision::OPENED);
    assert_eq!(AlertRevision::new(NonZeroU32::MAX).next(), None);
}

#[test]
fn update_kinds_reject_empty() {
    assert_eq!(UpdateKinds::new([]), Err(NoUpdateKinds));
}

#[test]
fn update_kinds_hold_what_was_asked() {
    let kinds = UpdateKinds::new([
        LiveUpdateKind::EdgeUpdated,
        LiveUpdateKind::AlertOpened,
        LiveUpdateKind::EdgeUpdated,
    ])
    .expect("not empty");
    assert!(kinds.contains(LiveUpdateKind::AlertOpened));
    assert!(kinds.contains(LiveUpdateKind::EdgeUpdated));
    assert!(!kinds.contains(LiveUpdateKind::TransmissionConfirmed));
    assert_eq!(
        kinds.iter().collect::<Vec<_>>(),
        vec![LiveUpdateKind::AlertOpened, LiveUpdateKind::EdgeUpdated]
    );
    let all = UpdateKinds::all();
    assert_eq!(all.iter().collect::<Vec<_>>(), LiveUpdateKind::ALL.to_vec());
}

#[test]
fn missing_permission_names_what_the_caller_lacks() {
    let viewer = Caller {
        operator: OperatorId::from_ulid(1),
        permissions: vec![Permission::View],
    };
    let nobody = Caller {
        operator: OperatorId::from_ulid(2),
        permissions: Vec::new(),
    };
    let reader = Caller {
        operator: OperatorId::from_ulid(3),
        permissions: vec![Permission::View, Permission::Content],
    };
    let view_kinds = UpdateKinds::new([LiveUpdateKind::AlertOpened, LiveUpdateKind::EdgeUpdated])
        .expect("not empty");
    assert_eq!(view_kinds.missing_permission(&viewer), None);
    assert_eq!(
        view_kinds.missing_permission(&nobody),
        Some(Permission::View)
    );
    assert_eq!(
        UpdateKinds::all().missing_permission(&viewer),
        Some(Permission::Content)
    );
    assert_eq!(UpdateKinds::all().missing_permission(&reader), None);
}

fn edge_scope() -> LiveScope {
    LiveScope::Scoped(ScopeKeys::routed(
        vec![agent(1), agent(2)],
        &Route::Channel(channel(1)),
        Some(TopicId::from_ulid(1)),
    ))
}

#[test]
fn routed_keys_carry_channel_only_for_channel_routes() {
    let on_channel = ScopeKeys::routed(Vec::new(), &Route::Channel(channel(3)), None);
    assert_eq!(on_channel.channel, Some(channel(3)));
    assert_eq!(on_channel.route, Some(RouteKind::Channel));
    let cases = [
        (
            Route::Delegation(DelegationDirection::ChildToParent),
            RouteKind::Delegation,
        ),
        (Route::Direct(DirectCarrier::UserTurn), RouteKind::Direct),
        (Route::Unobserved, RouteKind::Unobserved),
    ];
    for (route, kind) in cases {
        let keys = ScopeKeys::routed(Vec::new(), &route, None);
        assert_eq!(keys.channel, None);
        assert_eq!(keys.route, Some(kind));
    }
    let channel_keys = ScopeKeys::channel(channel(4), Vec::new());
    assert_eq!(channel_keys.channel, Some(channel(4)));
    assert_eq!(channel_keys.route, Some(RouteKind::Channel));
}

#[test]
fn empty_filter_admits_everything() {
    let filter = TopologyFilter::default();
    assert!(LiveScope::Global.admitted_by(&filter));
    assert!(edge_scope().admitted_by(&filter));
    assert!(LiveScope::Scoped(ScopeKeys::default()).admitted_by(&filter));
}

#[test]
fn global_updates_pass_any_filter() {
    let filter = TopologyFilter {
        agents: vec![agent(9)],
        channels: vec![channel(9)],
        route_kinds: vec![RouteKind::Direct],
        topics: vec![TopicId::from_ulid(9)],
    };
    assert!(LiveScope::Global.admitted_by(&filter));
    assert!(!edge_scope().admitted_by(&filter));
}

#[test]
fn agent_filter_matches_sender_or_reader() {
    let sender = TopologyFilter {
        agents: vec![agent(1)],
        ..TopologyFilter::default()
    };
    let reader = TopologyFilter {
        agents: vec![agent(2), agent(7)],
        ..TopologyFilter::default()
    };
    let other = TopologyFilter {
        agents: vec![agent(7)],
        ..TopologyFilter::default()
    };
    assert!(edge_scope().admitted_by(&sender));
    assert!(edge_scope().admitted_by(&reader));
    assert!(!edge_scope().admitted_by(&other));
    let channel_only = LiveScope::Scoped(ScopeKeys::channel(channel(1), Vec::new()));
    assert!(!channel_only.admitted_by(&sender));
}

#[test]
fn channel_filter_keeps_only_those_channels() {
    let filter = TopologyFilter {
        channels: vec![channel(1)],
        ..TopologyFilter::default()
    };
    assert!(edge_scope().admitted_by(&filter));
    assert!(LiveScope::Scoped(ScopeKeys::channel(channel(1), Vec::new())).admitted_by(&filter));
    assert!(!LiveScope::Scoped(ScopeKeys::channel(channel(2), Vec::new())).admitted_by(&filter));
    let delegated = LiveScope::Scoped(ScopeKeys::routed(
        vec![agent(1), agent(2)],
        &Route::Delegation(DelegationDirection::ParentToChild),
        None,
    ));
    assert!(!delegated.admitted_by(&filter));
}

#[test]
fn route_kind_filter_matches_route() {
    let direct = TopologyFilter {
        route_kinds: vec![RouteKind::Direct],
        ..TopologyFilter::default()
    };
    let channel_kind = TopologyFilter {
        route_kinds: vec![RouteKind::Channel],
        ..TopologyFilter::default()
    };
    assert!(!edge_scope().admitted_by(&direct));
    assert!(edge_scope().admitted_by(&channel_kind));
    let agent_alert = LiveScope::Scoped(ScopeKeys {
        agents: vec![agent(1)],
        ..ScopeKeys::default()
    });
    assert!(!agent_alert.admitted_by(&channel_kind));
}

#[test]
fn topic_filter_never_matches_a_missing_topic() {
    let filter = TopologyFilter {
        topics: vec![TopicId::from_ulid(1)],
        ..TopologyFilter::default()
    };
    assert!(edge_scope().admitted_by(&filter));
    let unclassified = LiveScope::Scoped(ScopeKeys::routed(
        vec![agent(1), agent(2)],
        &Route::Channel(channel(1)),
        None,
    ));
    assert!(!unclassified.admitted_by(&filter));
}

#[test]
fn filter_fields_combine_with_and() {
    let filter = TopologyFilter {
        agents: vec![agent(1)],
        channels: vec![channel(2)],
        ..TopologyFilter::default()
    };
    assert!(!edge_scope().admitted_by(&filter));
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
        LiveItem::Update {
            cursor,
            update: LiveUpdate::TopicVersionActivated {
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
