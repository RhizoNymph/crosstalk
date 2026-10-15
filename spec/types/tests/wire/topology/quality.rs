//! Detection quality (`QueryApi::detection_quality`): verdicts tallied
//! against the detector's calls.

use serde_json::{Value, json};

use super::super::harness::{assert_golden, assert_rejected};
use super::{AREA, array, at, edited, hour, object};
use crate::aggregates::edge::RouteKind;
use crate::aggregates::quality::{DetectionQuality, MatchClass, QualityMatch, QualityRow};

fn every_match_class() -> Vec<MatchClass> {
    fn declared(class: MatchClass) -> MatchClass {
        match class {
            MatchClass::Exact
            | MatchClass::Normalized
            | MatchClass::Decoded
            | MatchClass::Semantic => class,
        }
    }
    [
        MatchClass::Exact,
        MatchClass::Normalized,
        MatchClass::Decoded,
        MatchClass::Semantic,
    ]
    .into_iter()
    .map(declared)
    .collect()
}

/// One row per detector call, through an exhaustive match: confirmed by an
/// exact match, suspected, and discarded.
fn rows() -> Vec<QualityRow> {
    fn declared(call: QualityMatch) -> QualityMatch {
        match call {
            QualityMatch::Content(_) | QualityMatch::Suspected | QualityMatch::Discarded => call,
        }
    }
    let row = |route_kind, call, counts: (u64, u64, u64)| QualityRow {
        route_kind,
        match_kind: declared(call),
        genuine: counts.0,
        false_detection: counts.1,
        unlabeled: counts.2,
    };
    vec![
        row(
            RouteKind::Channel,
            QualityMatch::Content(MatchClass::Exact),
            (12, 1, 4),
        ),
        row(RouteKind::Channel, QualityMatch::Suspected, (2, 0, 7)),
        row(RouteKind::Channel, QualityMatch::Discarded, (0, 3, 1)),
        row(
            RouteKind::Direct,
            QualityMatch::Content(MatchClass::Semantic),
            (1, 2, 0),
        ),
    ]
}

fn quality() -> DetectionQuality {
    DetectionQuality::new(hour(), rows()).expect("distinct, non-empty rows")
}

#[test]
fn detection_quality_goldens() {
    assert_golden(AREA, "detection_quality", &quality());
    assert_golden(AREA, "match_classes", &every_match_class());
}

#[test]
fn detection_quality_decodes_through_its_constructor() {
    let reversed = edited(&quality(), |json| {
        array(json, "/rows").reverse();
    });
    let decoded: DetectionQuality = serde_json::from_str(&reversed)
        .unwrap_or_else(|error| panic!("rows in any order decode: {error}"));
    assert_eq!(decoded, quality());
    let refused = |reason: &str, edit: &dyn Fn(&mut Value)| {
        assert_rejected::<DetectionQuality>(&edited(&quality(), edit), reason);
    };
    refused(
        "invalid detection quality: DuplicateRow { route_kind: Channel, match_kind: Suspected }",
        &|json| {
            let row = at(json, "/rows/1").clone();
            array(json, "/rows").push(row);
        },
    );
    refused(
        "invalid detection quality: EmptyRow { route_kind: Channel, match_kind: Discarded }",
        &|json| {
            *at(json, "/rows/2/false_detection") = json!(0);
            *at(json, "/rows/2/unlabeled") = json!(0);
        },
    );
    refused("unknown field `precision`", &|json| {
        object(json, "/rows/0").insert("precision".into(), json!(0.92));
    });
    refused("unknown variant `expired`", &|json| {
        *at(json, "/rows/2/match_kind") = json!({"type": "expired"});
    });
    refused("unknown variant `fuzzy`", &|json| {
        *at(json, "/rows/0/match_kind/data") = json!("fuzzy");
    });
}
