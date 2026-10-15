//! Series (`QueryApi::series`): the grid and grouping a client sends, and
//! the series it gets back in every grouping.

use std::num::NonZeroU64;

use serde_json::{Value, json};

use super::super::harness::{assert_golden, assert_rejected, assert_request_golden};
use super::super::ts;
use super::{AREA, a, array, at, b, c, edited, hour, object, topic, version, wiki};
use crate::aggregates::edge::{RouteKind, Weighting};
use crate::aggregates::series::{
    BucketWidth, Series, SeriesEdge, SeriesGrid, SeriesGrouping, SeriesGroups, SeriesStep,
    TopologySeries,
};
use crate::aggregates::watermark::Watermarked;
use crate::derived::flow::transmission::{DirectCarrier, Route};
use crate::support::Watermark;

const MINUTE: u64 = 60_000_000;

fn minutes(count: u64) -> NonZeroU64 {
    NonZeroU64::new(count * MINUTE).unwrap_or(NonZeroU64::MIN)
}

/// Minute buckets.
fn bucket() -> BucketWidth {
    BucketWidth::from_micros(minutes(1))
}

/// Quarter-hour points over the hour: four of them.
fn grid() -> SeriesGrid {
    let step = SeriesStep::new(bucket(), minutes(15)).expect("15 minutes of 1-minute buckets");
    SeriesGrid::new(hour(), step).expect("the hour is four whole steps")
}

fn series(groups: SeriesGroups) -> TopologySeries {
    TopologySeries::new(grid(), Weighting::Transmissions, version(), groups)
        .expect("one value per point, distinct keys, none all zero")
}

/// The fixture story's two edges, one series each.
fn by_edge() -> SeriesGroups {
    SeriesGroups::ByEdge(vec![
        Series {
            key: SeriesEdge {
                from: a(),
                to: b(),
                route: Route::Channel(wiki()),
            },
            values: vec![1, 0, 2, 0],
        },
        Series {
            key: SeriesEdge {
                from: b(),
                to: c(),
                route: Route::Direct(DirectCarrier::UserTurn),
            },
            values: vec![0, 0, 0, 1],
        },
    ])
}

/// One series answer per grouping, through an exhaustive match.
fn every_grouping() -> Vec<(&'static str, SeriesGroups)> {
    fn name(groups: &SeriesGroups) -> &'static str {
        match groups {
            SeriesGroups::Total(_) => "series_total",
            SeriesGroups::ByTopic(_) => "series_by_topic",
            SeriesGroups::ByRouteKind(_) => "series_by_route_kind",
            SeriesGroups::ByEdge(_) => "series_by_edge",
        }
    }
    [
        SeriesGroups::Total(vec![1, 0, 2, 1]),
        SeriesGroups::ByTopic(vec![
            Series {
                key: Some(topic()),
                values: vec![1, 0, 2, 0],
            },
            Series {
                key: None,
                values: vec![0, 0, 0, 1],
            },
        ]),
        SeriesGroups::ByRouteKind(vec![
            Series {
                key: RouteKind::Channel,
                values: vec![1, 0, 2, 0],
            },
            Series {
                key: RouteKind::Direct,
                values: vec![0, 0, 0, 1],
            },
        ]),
        by_edge(),
    ]
    .into_iter()
    .map(|groups| (name(&groups), groups))
    .collect()
}

#[test]
fn series_goldens_in_every_grouping() {
    for (name, groups) in every_grouping() {
        assert_golden(
            AREA,
            name,
            &Watermarked {
                watermark: Watermark(ts("2026-10-04T12:58:00.000000Z")),
                value: series(groups),
            },
        );
    }
}

/// The request half of `QueryApi::series`: the grid and every grouping.
#[test]
fn series_request_goldens() {
    assert_request_golden(AREA, "series_grid", &grid());
    fn name(grouping: SeriesGrouping) -> &'static str {
        match grouping {
            SeriesGrouping::Total => "series_grouping_total",
            SeriesGrouping::Topic => "series_grouping_topic",
            SeriesGrouping::RouteKind => "series_grouping_route_kind",
            SeriesGrouping::Edge => "series_grouping_edge",
        }
    }
    for grouping in [
        SeriesGrouping::Total,
        SeriesGrouping::Topic,
        SeriesGrouping::RouteKind,
        SeriesGrouping::Edge,
    ] {
        assert_request_golden(AREA, name(grouping), &grouping);
    }
    assert_golden(AREA, "bucket_width", &bucket());
}

#[test]
fn series_grids_refuse_what_their_constructor_refuses() {
    let request = |start: &str, end: &str, step: u64| {
        format!(
            r#"{{"window": {{"start": "{start}", "end": "{end}"}}, "step": {{"bucket": {MINUTE}, "micros": {step}}}}}"#
        )
    };
    assert_rejected::<SeriesGrid>(
        &request(
            "2026-10-04T12:00:30.000000Z",
            "2026-10-04T13:00:30.000000Z",
            15 * MINUTE,
        ),
        "invalid series grid: UnalignedStart",
    );
    assert_rejected::<SeriesGrid>(
        &request(
            "2026-10-04T12:00:00.000000Z",
            "2026-10-04T12:20:00.000000Z",
            15 * MINUTE,
        ),
        "invalid series grid: PartialStep { remainder_micros: 300000000 }",
    );
    // 10 001 one-minute points.
    assert_rejected::<SeriesGrid>(
        &request(
            "2026-10-04T00:00:00.000000Z",
            "2026-10-10T22:41:00.000000Z",
            MINUTE,
        ),
        "invalid series grid: TooManyPoints { points: 10001 }",
    );
    assert_rejected::<SeriesGrid>(
        &request(
            "2026-10-04T12:00:00.000000Z",
            "2026-10-04T13:00:00.000000Z",
            90_000_000,
        ),
        "invalid series step: NotBucketMultiple",
    );
    assert_rejected::<SeriesGrid>(
        &request(
            "2026-10-04T12:00:00.000000Z",
            "2026-10-04T12:00:00.000000Z",
            MINUTE,
        ),
        "invalid time window: EmptyWindow",
    );
    // The point count follows from the window and step; it is not sent.
    assert_rejected::<SeriesGrid>(
        &edited(&grid(), |json| {
            object(json, "").insert("points".into(), json!(4));
        }),
        "unknown field `points`",
    );
    assert_rejected::<SeriesStep>(
        &format!(
            r#"{{"bucket": {MINUTE}, "micros": {}, "points": 4}}"#,
            15 * MINUTE
        ),
        "unknown field `points`",
    );
    assert_rejected::<BucketWidth>("0", "invalid value");
    assert_rejected::<SeriesGrouping>(r#""channel""#, "unknown variant `channel`");
}

/// Each rule `TopologySeries::new` checks, broken in a golden series'
/// JSON, refused on decode.
#[test]
fn topology_series_refuse_what_their_constructor_refuses() {
    let edges = series(by_edge());
    let refused = |reason: &str, edit: &dyn Fn(&mut Value)| {
        assert_rejected::<TopologySeries>(&edited(&edges, edit), reason);
    };
    refused(
        "invalid topology series: WrongPointCount { expected: 4, got: 3 }",
        &|json| {
            array(json, "/groups/data/0/values").pop();
        },
    );
    refused("invalid topology series: DuplicateKey", &|json| {
        *at(json, "/groups/data/1/key") = at(json, "/groups/data/0/key").clone();
    });
    refused("invalid topology series: ZeroSeries", &|json| {
        *at(json, "/groups/data/1/values") = json!([0, 0, 0, 0]);
    });
    refused("invalid topology series: SelfEdge", &|json| {
        *at(json, "/groups/data/1/key/to") = at(json, "/groups/data/1/key/from").clone();
    });
    refused("unknown field `total`", &|json| {
        object(json, "").insert("total".into(), json!(4));
    });
    refused("unknown variant `by_channel`", &|json| {
        *at(json, "/groups/type") = json!("by_channel");
    });
    let total = series(SeriesGroups::Total(vec![1, 0, 2, 1]));
    assert_rejected::<TopologySeries>(
        &edited(&total, |json| {
            array(json, "/groups/data").push(json!(0));
        }),
        "invalid topology series: WrongPointCount { expected: 4, got: 5 }",
    );
    // A grid decoded inside the series is checked too.
    assert_rejected::<TopologySeries>(
        &edited(&total, |json| {
            *at(json, "/grid/step/micros") = json!(7 * MINUTE);
        }),
        "invalid series grid: PartialStep",
    );
}
