//! Projections on the wire: `fit_projection` (a `ProjectionParams` request),
//! `projection_status` and `projections` (`ProjectionInfo` with its spec and
//! status), the points an export lists, and `QueryApi::projection`, whose
//! JSON half is the `ProjectionInfo` and whose frame is binary
//! (`application/octet-stream`, `ProjectionFrame::encode`).

use serde_json::{Value, json};

use super::super::harness::{assert_golden, assert_rejected, assert_request_golden};
use super::super::{ULID_A, ULID_B, id, ts};
use super::{at, model, operator, topic, version};
use crate::aggregates::edge::RouteKind;
use crate::aggregates::filter::UnconfirmedChannels;
use crate::aggregates::filter::{FalseDetections, TopicVersionSelector, TopologyFilter};
use crate::aggregates::projection::frame::{FrameHeader, ProjectionFrame};
use crate::aggregates::projection::{
    FitFailure, Fitted, PointParts, PointRoute, ProjectedPoint, Projection, ProjectionInfo,
    ProjectionLimit, ProjectionParams, ProjectionSpec, ProjectionStatus,
};
use crate::ids::{AgentId, ChannelId, ProjectionId, TransmissionId};
use crate::paging::{Page, PageSize, ProjectionList};
use crate::support::{Finite, TimeWindow, Watermark};

const AREA: &str = "projections";

fn projection_id(text: &str) -> ProjectionId {
    id(ProjectionId::from_ulid_text, text)
}

fn params(limit: u32) -> ProjectionParams {
    ProjectionParams::new(
        ProjectionLimit::new(limit).expect("a limit in range"),
        ProjectionParams::DEFAULT_NEIGHBORS,
        ProjectionParams::DEFAULT_MIN_DIST_MILLI,
        42,
    )
    .expect("default UMAP parameters")
}

/// The filter as the client sent it: on the current version, which the
/// spec pins.
fn filter() -> TopologyFilter {
    TopologyFilter {
        agents: Vec::new(),
        channels: vec![id(ChannelId::from_ulid_text, ULID_B)],
        route_kinds: vec![RouteKind::Channel],
        topics: vec![topic(0)],
        topic_version: TopicVersionSelector::Current,
        false_detections: FalseDetections::Exclude,
        unconfirmed_channels: UnconfirmedChannels::Include,
    }
}

fn spec(limit: u32) -> ProjectionSpec {
    let window = TimeWindow::new(at("10:00:00"), at("11:00:00")).expect("an hour");
    ProjectionSpec::new(window, filter(), version(2), params(limit), model())
}

fn queued(text: &str, limit: u32) -> ProjectionInfo {
    ProjectionInfo::queued(projection_id(text), spec(limit), operator(), at("11:05:00"))
}

fn fitted(matching: u64, points: u32) -> Fitted {
    Fitted {
        started_at: at("11:06:00"),
        fitted_at: at("11:08:30"),
        watermark: Watermark(at("11:00:00")),
        matching,
        points,
    }
}

fn ready(text: &str, limit: u32, matching: u64, points: u32) -> ProjectionInfo {
    queued(text, limit)
        .start(at("11:06:00"))
        .and_then(|job| job.complete(fitted(matching, points)))
        .expect("queued, fitting, ready")
}

/// One job in every status, both failures (before and after it started).
fn every_status() -> Vec<(&'static str, ProjectionInfo)> {
    fn declared(info: ProjectionInfo) -> ProjectionInfo {
        match info.status() {
            ProjectionStatus::Queued
            | ProjectionStatus::Fitting { .. }
            | ProjectionStatus::Ready(_)
            | ProjectionStatus::Failed { .. }
            | ProjectionStatus::Expired { .. } => info,
        }
    }
    let fitting = queued(ULID_A, 5_000)
        .start(at("11:06:00"))
        .expect("queued to fitting");
    [
        ("projection_queued", queued(ULID_A, 5_000)),
        ("projection_fitting", fitting.clone()),
        ("projection_ready", ready(ULID_A, 5_000, 12_000, 5_000)),
        (
            "projection_failed",
            fitting
                .fail(
                    at("11:07:00"),
                    FitFailure::TooFewPoints { needed: 16, got: 9 },
                )
                .expect("fitting to failed"),
        ),
        (
            "projection_failed_before_start",
            queued(ULID_A, 5_000)
                .fail(
                    at("11:06:00"),
                    FitFailure::VersionNotRetained {
                        version: version(2),
                    },
                )
                .expect("queued to failed"),
        ),
        (
            "projection_expired",
            ready(ULID_A, 5_000, 12_000, 5_000)
                .expire(ts("2027-04-02T11:08:30.000000Z"))
                .expect("ready to expired"),
        ),
    ]
    .into_iter()
    .map(|(golden, info)| (golden, declared(info)))
    .collect()
}

fn point(transmission: &str, topic_n: Option<usize>, x: f32, y: f32) -> ProjectedPoint {
    ProjectedPoint::new(PointParts {
        transmission: id(TransmissionId::from_ulid_text, transmission),
        from: id(AgentId::from_ulid_text, ULID_A),
        to: id(AgentId::from_ulid_text, ULID_B),
        route: PointRoute::Channel(id(ChannelId::from_ulid_text, ULID_B)),
        topic: topic_n.map(topic),
        confirmed_at: at("10:42:17"),
        x: Finite::new(x).expect("finite"),
        y: Finite::new(y).expect("finite"),
    })
    .expect("a point between two agents")
}

fn points() -> Vec<ProjectedPoint> {
    vec![
        point(ULID_A, Some(0), 1.25, -0.5),
        point(ULID_B, None, -3.75, 2.0),
    ]
}

/// What a client sends to `fit_projection`: every parameter, the seed
/// included. The window and filter travel beside it.
#[test]
fn projection_params_golden_as_a_request() {
    assert_request_golden(AREA, "projection_params", &params(5_000));
}

/// `projection_status`: the job in every status.
#[test]
fn projection_infos_golden_in_every_status() {
    for (golden, info) in every_status() {
        assert_golden(AREA, golden, &info);
    }
}

/// `projections`: newest first.
#[test]
fn projections_page_golden() {
    let page: Page<ProjectionInfo, ProjectionList> = Page::last(
        PageSize::new(50).expect("a valid size"),
        vec![queued(ULID_B, 2_000), ready(ULID_A, 5_000, 12_000, 5_000)],
    )
    .expect("two jobs fit");
    assert_golden(AREA, "projections_page", &page);
}

/// An export's point rows hold these; an outlier has no topic.
#[test]
fn projected_points_golden() {
    assert_golden(AREA, "projected_points", &points());
}

/// A point's route: its kind, and a channel route's channel. Adjacently
/// tagged, so only a channel route carries data.
#[test]
fn point_routes_golden_with_every_variant() {
    fn declared(route: PointRoute) -> PointRoute {
        match route {
            PointRoute::Channel(_)
            | PointRoute::Delegation
            | PointRoute::Direct
            | PointRoute::Unobserved => route,
        }
    }
    let routes: Vec<PointRoute> = [
        PointRoute::Channel(id(ChannelId::from_ulid_text, ULID_B)),
        PointRoute::Delegation,
        PointRoute::Direct,
        PointRoute::Unobserved,
    ]
    .into_iter()
    .map(declared)
    .collect();
    assert_golden(AREA, "point_routes", &routes);
    assert_rejected::<PointRoute>(r#"{"type": "channel"}"#, "missing field `data`");
    assert_rejected::<PointRoute>(
        r#"{"type": "channel", "data": "not a ulid"}"#,
        "invalid ULID",
    );
    // A bare route kind, the shape a point once carried, is not a route.
    assert_rejected::<PointRoute>(r#""channel""#, "expected adjacently tagged enum PointRoute");
    assert_rejected::<PointRoute>(
        &format!(r#"{{"type": "direct", "data": "{ULID_B}"}}"#),
        "invalid type",
    );
}

/// `QueryApi::projection` answers in two halves: the job record as JSON
/// (`projection_status`) and the frame as bytes. A client decodes both and
/// joins them with `Projection::new`, which checks that they agree, so the
/// pair it renders is exactly what the store holds.
#[test]
fn a_projection_travels_as_info_json_and_frame_bytes() {
    let info = ready(ULID_A, 5_000, 2, 2);
    let header = FrameHeader {
        projection: info.id(),
        topic_version: info.spec().topic_version(),
        watermark: Watermark(at("11:00:00")),
        limit: info.spec().params().limit(),
        matching: 2,
    };
    let frame = ProjectionFrame::from_points(header, &points()).expect("a valid frame");
    let projection = Projection::new(info.clone(), frame.clone()).expect("they agree");

    let info_json = serde_json::to_string(projection.info()).expect("info encodes");
    let bytes = projection.frame().encode();
    let decoded = Projection::new(
        serde_json::from_str(&info_json).expect("info decodes"),
        ProjectionFrame::decode(&bytes).expect("frame decodes"),
    )
    .expect("they still agree");
    assert_eq!(decoded, projection);
    assert_eq!(decoded.frame().points().collect::<Vec<_>>(), points());

    // A frame from another job does not join.
    let other = ready(ULID_B, 5_000, 2, 2);
    assert!(Projection::new(other, frame).is_err());
}

/// A spec decodes through `ProjectionSpec::new`, which pins the filter to
/// the resolved version: a stored spec never says `current`.
#[test]
fn projection_specs_decode_pinned() {
    let mut json = serde_json::to_value(spec(5_000)).expect("a spec encodes");
    assert_eq!(
        json["filter"]["topic_version"],
        json!({"type": "pinned", "data": 2})
    );
    json["filter"]["topic_version"] = json!({"type": "current"});
    let decoded: ProjectionSpec = serde_json::from_value(json).expect("a spec decodes");
    assert_eq!(decoded, spec(5_000));
    assert_eq!(
        decoded.filter().topic_version,
        TopicVersionSelector::Pinned(version(2))
    );
}

fn params_json(limit: u32, neighbors: u16, min_dist_milli: u16) -> String {
    json!({
        "limit": limit,
        "neighbors": neighbors,
        "min_dist_milli": min_dist_milli,
        "seed": 42,
    })
    .to_string()
}

#[test]
fn projection_params_refuse_what_their_constructors_refuse() {
    assert_rejected::<ProjectionLimit>("0", "invalid projection limit: Zero");
    assert_rejected::<ProjectionLimit>(
        "100001",
        "invalid projection limit: AboveMax { max: 100000, got: 100001 }",
    );
    assert_rejected::<ProjectionParams>(&params_json(0, 15, 100), "invalid projection limit: Zero");
    assert_rejected::<ProjectionParams>(
        &params_json(5_000, 1, 100),
        "invalid projection params: Neighbors { min: 2, max: 200, got: 1 }",
    );
    assert_rejected::<ProjectionParams>(
        &params_json(5_000, 201, 100),
        "invalid projection params: Neighbors",
    );
    assert_rejected::<ProjectionParams>(
        &params_json(5_000, 15, 1_001),
        "invalid projection params: MinDist { max_milli: 1000, got_milli: 1001 }",
    );
    // UMAP's float `min_dist` is not a field: thousandths are, so a recorded
    // spec reproduces exactly.
    assert_rejected::<ProjectionParams>(
        r#"{"limit": 5000, "neighbors": 15, "min_dist_milli": 100, "seed": 42, "min_dist": 0.1}"#,
        "unknown field `min_dist`",
    );
    assert_rejected::<ProjectionParams>(
        r#"{"limit": 5000, "neighbors": 15, "min_dist_milli": 100}"#,
        "missing field `seed`",
    );
}

/// `every_status()`'s `golden` as JSON, changed by `edit`.
fn info_json(golden: &str, edit: impl FnOnce(&mut Value)) -> String {
    let (_, info) = every_status()
        .into_iter()
        .find(|(name, _)| *name == golden)
        .expect("a fixture status");
    let mut json = serde_json::to_value(info).expect("info encodes");
    edit(&mut json);
    json.to_string()
}

#[test]
fn projection_infos_refuse_what_their_constructor_refuses() {
    let refused = |json: String, reason: &str| {
        assert_rejected::<ProjectionInfo>(&json, &format!("invalid projection info: {reason}"));
    };
    refused(
        info_json("projection_fitting", |json| {
            json["requested_at"] = json!("2026-10-04T11:06:01.000000Z");
        }),
        "TimestampsOutOfOrder",
    );
    refused(
        info_json("projection_expired", |json| {
            json["status"]["data"]["expired_at"] = json!("2026-10-04T11:08:00.000000Z");
        }),
        "TimestampsOutOfOrder",
    );
    refused(
        info_json("projection_ready", |json| {
            json["status"]["data"]["watermark"] = json!("2026-10-04T11:06:01.000000Z");
        }),
        "WatermarkAfterStart",
    );
    refused(
        info_json("projection_ready", |json| {
            json["status"]["data"]["points"] = json!(4_999);
        }),
        "WrongCount { expected: 5000, got: 4999 }",
    );
}

#[test]
fn projections_refuse_unknown_shapes_and_non_finite_points() {
    assert_rejected::<ProjectionInfo>(
        &info_json("projection_queued", |json| json["priority"] = json!(1)),
        "unknown field `priority`",
    );
    assert_rejected::<ProjectionStatus>(r#"{"type": "cancelled"}"#, "unknown variant `cancelled`");
    assert_rejected::<ProjectionStatus>(
        r#"{"type": "fitting", "data": {"started_at": "2026-10-04T11:06:00.000000Z", "worker": 3}}"#,
        "unknown field `worker`",
    );
    let mut fit = serde_json::to_value(fitted(12_000, 5_000)).expect("a fit encodes");
    fit["duration_ms"] = json!(150_000);
    assert_rejected::<Fitted>(&fit.to_string(), "unknown field `duration_ms`");

    let point_json = |edit: &dyn Fn(&mut Value)| {
        let mut json = serde_json::to_value(points().remove(0)).expect("a point encodes");
        edit(&mut json);
        json.to_string()
    };
    for (axis, value) in [("x", "1e39"), ("y", "-1e39")] {
        let number: Value = serde_json::from_str(value).expect("a JSON number");
        assert_rejected::<ProjectedPoint>(
            &point_json(&|json| json[axis] = number.clone()),
            "invalid finite number",
        );
    }
    assert_rejected::<ProjectedPoint>(
        &point_json(&|json| json["z"] = json!(0.0)),
        "unknown field `z`",
    );
}
