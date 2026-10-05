//! The list and search requests of `l8_surface/lists.rs` (`ChannelFilter`
//! with each origin filter, `AlertRuleFilter`, `SearchRequest`), the topic
//! page, the overview's counts and the sinks report.

use std::num::NonZeroU16;

use super::super::harness::{
    assert_golden, assert_rejected, assert_request_golden, assert_round_trips,
};
use super::super::{ULID_A, ULID_B, ULID_C, id, ts};
use crate::aggregates::alert::RuleStatus;
use crate::aggregates::edge::EdgeTotals;
use crate::aggregates::node::CanonicalOriginKind;
use crate::aggregates::topic::{Embedding, EmbeddingModel, Topic, TopicModelVersion};
use crate::derived::flow::channel::confirmation::ListingKind;
use crate::derived::flow::channel::detection::DetectionKind;
use crate::ids::{SinkId, TopicId};
use crate::interfaces::l8_surface::lists::{
    AlertRuleFilter, ChannelFilter, OriginFilter, SearchMode, SearchRequest, TopicPage,
};
use crate::interfaces::l8_surface::overview::{OverviewCounts, QueueCounts};
use crate::interfaces::l8_surface::{PolicyKind, SinkError, SinkInfo, SinkKind};
use crate::paging::{Page, PageSize};
use crate::support::{Finite, NonBlank, TimeWindow};

const AREA: &str = "surface_actions/lists";

fn window() -> TimeWindow {
    TimeWindow::new(
        ts("2026-10-04T00:00:00.000000Z"),
        ts("2026-10-04T12:00:00.000000Z"),
    )
    .expect("twelve hours")
}

/// A channel filter for each origin filter, with its golden name.
fn every_channel_filter() -> Vec<(&'static str, ChannelFilter)> {
    let origins = [
        OriginFilter::InForce(Vec::new()),
        OriginFilter::WithSuperseded(vec![
            CanonicalOriginKind::Promoted,
            CanonicalOriginKind::Discovered,
        ]),
        OriginFilter::Superseded,
    ];
    origins
        .into_iter()
        .map(|origin| match origin {
            OriginFilter::InForce(_) => ("channel_filter_in_force", ChannelFilter::default()),
            OriginFilter::WithSuperseded(_) => (
                "channel_filter_with_superseded",
                ChannelFilter {
                    origin,
                    listings: vec![ListingKind::Confirmed, ListingKind::Declaration],
                    detections: vec![DetectionKind::Active, DetectionKind::Dormant],
                    policies: vec![PolicyKind::Unreviewed],
                    window: Some(window()),
                },
            ),
            OriginFilter::Superseded => (
                "channel_filter_superseded",
                ChannelFilter {
                    origin,
                    listings: Vec::new(),
                    detections: Vec::new(),
                    policies: Vec::new(),
                    window: None,
                },
            ),
        })
        .collect()
}

#[test]
fn channel_filters_golden_with_every_origin_filter() {
    for (name, filter) in every_channel_filter() {
        assert_request_golden(AREA, name, &filter);
    }
}

#[test]
fn alert_rule_filters_and_searches_golden() {
    let evaluating = AlertRuleFilter {
        statuses: vec![RuleStatus::Enabled],
        stale: Some(false),
    };
    assert_request_golden(AREA, "alert_rule_filter", &evaluating);
    assert_request_golden(
        AREA,
        "alert_rule_filter_everything",
        &AlertRuleFilter::default(),
    );

    fn mode(mode: SearchMode) -> SearchMode {
        match mode {
            SearchMode::Text | SearchMode::Semantic | SearchMode::Hybrid => mode,
        }
    }
    let modes = [SearchMode::Text, SearchMode::Semantic, SearchMode::Hybrid].map(mode);
    // A client that has not picked a mode searches both ways.
    assert_eq!(SearchMode::default(), SearchMode::Hybrid);
    assert_golden(AREA, "search_modes", &modes.to_vec());
    let search = SearchRequest {
        mode: SearchMode::Hybrid,
        text: NonBlank::new("deploy key").expect("not blank"),
    };
    assert_request_golden(AREA, "search_request", &search);
}

#[test]
fn topic_page_golden() {
    let model = EmbeddingModel {
        name: "nomic-embed-text-v1.5".into(),
        dimension: NonZeroU16::new(2).unwrap_or(NonZeroU16::MIN),
    };
    let topic = Topic {
        id: id(TopicId::from_ulid_text, ULID_A),
        version: TopicModelVersion(4),
        label: "credentials and deploy keys".into(),
        terms: vec![
            ("ssh".into(), Finite::new(0.5).expect("finite")),
            ("deploy".into(), Finite::new(0.25).expect("finite")),
        ],
        centroid: Embedding::new(model, vec![0.6, 0.8]).expect("unit norm"),
        fitted_at: ts("2026-10-02T03:00:00.000000Z"),
    };
    let page = TopicPage {
        version: TopicModelVersion(4),
        page: Page::last(PageSize::new(50).expect("valid"), vec![topic]).expect("fits"),
    };
    assert_golden(AREA, "topic_page", &page);
}

#[test]
fn overview_counts_golden() {
    let counts = OverviewCounts {
        activity: EdgeTotals {
            topic_version: TopicModelVersion(4),
            transmissions: 5_012,
            matched_bytes: 2_310_448,
            active_channels: 9,
        },
        queues: QueueCounts {
            open_alerts: 41,
            unreviewed_channels: 3,
            unconfirmed_channels: Some(2),
        },
    };
    assert_golden(AREA, "overview_counts", &counts);
    // Under `UnconfirmedChannels::Exclude`: not counted, not "none".
    let confirmed_only = QueueCounts {
        open_alerts: 41,
        unreviewed_channels: 2,
        unconfirmed_channels: None,
    };
    assert_golden(AREA, "queue_counts_confirmed_only", &confirmed_only);
    assert_round_trips(&QueueCounts::default());
}

/// `QueryApi::sinks`: one sink per `last_delivery` form (never delivered,
/// succeeded, failed each way) and of every kind.
#[test]
fn sinks_golden_with_every_delivery_form() {
    fn kind(kind: SinkKind) -> SinkKind {
        match kind {
            SinkKind::Webhook | SinkKind::Slack | SinkKind::Log => kind,
        }
    }
    let deliveries = [
        None,
        Some(Ok(ts("2026-10-04T12:31:07.250000Z"))),
        Some(Err(SinkError::Unreachable {
            reason: "connect timeout after 5s".into(),
        })),
        Some(Err(SinkError::Rejected { status: 403 })),
    ];
    let sinks: Vec<SinkInfo> = deliveries
        .into_iter()
        .map(|last_delivery| {
            let (sink, kind, name) = match &last_delivery {
                None => (ULID_A, kind(SinkKind::Log), "local-log"),
                Some(Ok(_)) => (ULID_B, kind(SinkKind::Slack), "#agent-alerts"),
                Some(Err(SinkError::Unreachable { .. })) => {
                    (ULID_C, kind(SinkKind::Webhook), "soc-webhook")
                }
                Some(Err(SinkError::Rejected { .. })) => {
                    (ULID_A, kind(SinkKind::Webhook), "pager-webhook")
                }
            };
            SinkInfo {
                id: id(SinkId::from_ulid_text, sink),
                kind,
                name: name.into(),
                last_delivery,
            }
        })
        .collect();
    assert_golden(AREA, "sinks", &sinks);
}

#[test]
fn list_requests_refuse_unknown_fields_and_variants() {
    let origin = r#"{"type": "in_force", "data": []}"#;
    assert_rejected::<ChannelFilter>(
        &format!(
            r#"{{"origin": {origin}, "detections": [], "policies": [], "window": null, "superseded": true}}"#
        ),
        "unknown field `superseded`",
    );
    assert_rejected::<ChannelFilter>(
        r#"{"origin": {"type": "everything"}, "detections": [], "policies": [], "window": null}"#,
        "unknown variant `everything`",
    );
    assert_rejected::<ChannelFilter>(
        &format!(
            r#"{{"origin": {origin}, "detections": ["hot"], "policies": [], "window": null}}"#
        ),
        "unknown variant `hot`",
    );
    assert_rejected::<AlertRuleFilter>(
        r#"{"statuses": ["enabled"], "stale": null, "kind": "watched_topic"}"#,
        "unknown field `kind`",
    );
    assert_rejected::<AlertRuleFilter>(
        r#"{"statuses": ["stale"], "stale": null}"#,
        "unknown variant `stale`",
    );
    assert_rejected::<SearchRequest>(
        r#"{"mode": "text", "text": "   "}"#,
        "invalid non-blank text: Blank",
    );
    assert_rejected::<SearchRequest>(
        r#"{"mode": "fuzzy", "text": "deploy key"}"#,
        "unknown variant `fuzzy`",
    );
    assert_rejected::<SearchRequest>(
        r#"{"mode": "text", "text": "deploy key", "vector": [0.1]}"#,
        "unknown field `vector`",
    );
}

#[test]
fn reports_refuse_unknown_fields_and_other_delivery_forms() {
    let sink = |last_delivery: &str| {
        format!(
            r#"{{"id": "{ULID_A}", "kind": "slack", "name": "agent-alerts", "last_delivery": {last_delivery}}}"#
        )
    };
    // serde's own form of a `Result` is not the wire's.
    assert_rejected::<SinkInfo>(
        &sink(r#"{"Ok": "2026-10-04T12:31:07.250000Z"}"#),
        r#"expected "type" or "data""#,
    );
    assert_rejected::<SinkInfo>(
        &sink(r#"{"type": "retrying", "data": 3}"#),
        "unknown variant `retrying`",
    );
    assert_rejected::<SinkInfo>(
        &sink(
            r#"{"type": "failed", "data": {"type": "rejected", "data": {"status": 500, "body": ""}}}"#,
        ),
        "unknown field `body`",
    );
    assert_rejected::<SinkKind>(r#""email""#, "unknown variant `email`");
    assert_rejected::<OverviewCounts>(
        r#"{"activity": {"topic_version": 4, "transmissions": 1, "matched_bytes": 1, "active_channels": 1},
            "queues": {"open_alerts": 0, "unreviewed_channels": 0, "dead_letters": 0}}"#,
        "unknown field `dead_letters`",
    );
}

/// Every `ListingKind` a channel list filter can name, in declaration order.
#[test]
fn listing_kinds_golden() {
    fn declared(kind: ListingKind) -> ListingKind {
        match kind {
            ListingKind::Confirmed | ListingKind::Unconfirmed | ListingKind::Declaration => kind,
        }
    }
    let kinds = [
        ListingKind::Confirmed,
        ListingKind::Unconfirmed,
        ListingKind::Declaration,
    ]
    .map(declared);
    assert_golden(AREA, "listing_kinds", &kinds.to_vec());
}
