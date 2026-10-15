//! Topics on the wire: `QueryApi::topic_versions` (the version history with
//! retention and pins), `topic_sizes` and `topic_lineage` (each taking a
//! `TopicModelVersion` from the client), and the topics `topics` pages.

use std::num::NonZeroU64;

use serde_json::{Value, json};

use super::super::harness::{assert_golden, assert_rejected, assert_request_golden};
use super::super::{ULID_A, ULID_B};
use super::{at, embedding, model, operator, sim, topic, version};
use crate::aggregates::edge::EdgeStats;
use crate::aggregates::retention::{Pin, Retention};
use crate::aggregates::topic::{Embedding, EmbeddingModel, Topic, TopicModelVersion};
use crate::aggregates::topic_history::{
    CompletedFit, FitRecord, LineageEntry, LineageLink, TopicLineage, TopicSize, TopicSizes,
    TopicVersionHistory, TopicVersionInfo, TopicVersionStatus,
};
use crate::aggregates::watermark::Watermarked;
use crate::support::{Finite, TimeWindow, Watermark};
use crate::wire::decode_request;

const AREA: &str = "topics";

fn fit(started: &str, fitted: &str, ready: &str, topics: u32) -> CompletedFit {
    CompletedFit {
        started_at: at(started),
        fitted_at: at(fitted),
        ready_at: at(ready),
        topics,
    }
}

fn info(n: u32, status: TopicVersionStatus, retention: Retention) -> TopicVersionInfo {
    TopicVersionInfo::with_retention(version(n), status, retention)
        .expect("a valid fixture version")
}

/// Versions 0 to 4, one in every status and every retention: 0 the
/// unfitted model, superseded by 2; 1 fitted but overtaken by 2 before it
/// was activated, and dropped; 2 active and pinned; 3 ready; 4 fitting.
fn versions() -> [TopicVersionInfo; 5] {
    fn declared(status: TopicVersionStatus) -> TopicVersionStatus {
        match status {
            TopicVersionStatus::Fitting { .. }
            | TopicVersionStatus::Ready { .. }
            | TopicVersionStatus::Active {
                fit: FitRecord::Unfitted | FitRecord::Fitted(_),
                ..
            }
            | TopicVersionStatus::Superseded { .. } => status,
        }
    }
    let superseded = |fit, activated_at| {
        declared(TopicVersionStatus::Superseded {
            fit,
            activated_at,
            by: version(2),
            superseded_at: at("10:00:00"),
        })
    };
    [
        info(
            0,
            superseded(FitRecord::Unfitted, Some(at("00:00:00"))),
            Retention::UNPINNED,
        ),
        info(
            1,
            superseded(
                FitRecord::Fitted(fit("06:00:00", "06:05:00", "06:20:00", 12)),
                None,
            ),
            Retention::Dropped { at: at("11:00:00") },
        ),
        info(
            2,
            declared(TopicVersionStatus::Active {
                fit: FitRecord::Fitted(fit("08:30:00", "08:40:00", "09:00:00", 14)),
                activated_at: at("10:00:00"),
            }),
            Retention::Retained {
                pin: Some(Pin {
                    by: operator(),
                    at: at("10:30:00"),
                }),
            },
        ),
        info(
            3,
            declared(TopicVersionStatus::Ready {
                fit: fit("11:00:00", "11:10:00", "11:30:00", 15),
            }),
            Retention::UNPINNED,
        ),
        info(
            4,
            declared(TopicVersionStatus::Fitting {
                started_at: at("12:00:00"),
            }),
            Retention::UNPINNED,
        ),
    ]
}

fn history() -> TopicVersionHistory {
    TopicVersionHistory::new(versions().to_vec()).expect("a valid history")
}

fn stats(transmissions: u64, matched_bytes: u64) -> EdgeStats {
    EdgeStats {
        transmissions: NonZeroU64::new(transmissions).expect("non-zero"),
        matched_bytes: NonZeroU64::new(matched_bytes).expect("non-zero"),
    }
}

fn sizes(window: Option<TimeWindow>) -> TopicSizes {
    TopicSizes::new(
        version(2),
        window,
        vec![
            TopicSize {
                topic: topic(0),
                stats: Some(stats(14, 5_120)),
            },
            TopicSize {
                topic: topic(1),
                stats: None,
            },
        ],
        Some(stats(3, 640)),
    )
    .expect("distinct topics")
}

fn link(n: usize, similarity: f32) -> LineageLink {
    LineageLink {
        topic: topic(n),
        similarity: sim(similarity),
    }
}

fn lineage() -> TopicLineage {
    TopicLineage::new(
        version(2),
        version(3),
        sim(0.3),
        vec![
            LineageEntry::new(topic(0), Some(link(3, 0.91)), vec![link(4, 0.42)])
                .expect("in lineage order"),
            LineageEntry::new(topic(1), Some(link(4, 0.35)), Vec::new()).expect("one link"),
        ],
    )
    .expect("a forward lineage above its floor")
}

fn weight(value: f32) -> Finite {
    Finite::new(value).expect("a finite weight")
}

fn sample_topic() -> Topic {
    Topic {
        id: topic(3),
        version: version(3),
        label: "credential handoffs".into(),
        terms: vec![
            ("token".into(), weight(0.42)),
            ("vault".into(), weight(0.31)),
            ("paste".into(), weight(0.125)),
        ],
        centroid: embedding(model()),
        fitted_at: at("11:10:00"),
    }
}

/// `topic_sizes` and `topic_lineage` take a version from the client;
/// `topic_sizes` takes `None` for the active one.
#[test]
fn topic_model_versions_golden_as_requests() {
    assert_request_golden(AREA, "topic_model_version", &version(3));
    assert_request_golden(
        AREA,
        "topic_model_version_active",
        &None::<TopicModelVersion>,
    );
    assert_eq!(decode_request::<TopicModelVersion>(b"0"), Ok(version(0)));
}

#[test]
fn topic_version_history_golden_in_every_status_and_retention() {
    assert_golden(AREA, "topic_version_history", &history());
}

#[test]
fn topic_sizes_golden_over_a_window_and_all_time() {
    let watermark = Watermark(at("11:00:00"));
    let window = TimeWindow::new(at("10:00:00"), at("11:00:00")).expect("an hour");
    assert_golden(
        AREA,
        "topic_sizes_window",
        &Watermarked {
            watermark,
            value: sizes(Some(window)),
        },
    );
    assert_golden(
        AREA,
        "topic_sizes_all_time",
        &Watermarked {
            watermark,
            value: sizes(None),
        },
    );
}

#[test]
fn topic_lineage_golden() {
    assert_golden(AREA, "topic_lineage", &Some(lineage()));
    assert_golden(AREA, "topic_lineage_pending", &None::<TopicLineage>);
    // A successor with no topics: no best link for anything.
    let to_empty = TopicLineage::new(
        version(3),
        version(4),
        sim(0.3),
        vec![LineageEntry::new(topic(3), None, Vec::new()).expect("no links")],
    )
    .expect("forward");
    assert_golden(AREA, "topic_lineage_to_empty", &Some(to_empty));
}

#[test]
fn topics_and_embeddings_golden() {
    assert_golden(AREA, "topic", &sample_topic());
    assert_golden(AREA, "embedding", &embedding(model()));
}

fn to_json<T: serde::Serialize>(value: &T) -> Value {
    serde_json::to_value(value).expect("a fixture encodes")
}

/// `versions()[n]` as JSON, changed by `edit`.
fn version_json(n: usize, edit: impl FnOnce(&mut Value)) -> String {
    let mut json = to_json(&versions()[n]);
    edit(&mut json);
    json.to_string()
}

#[test]
fn topic_version_infos_refuse_what_their_constructor_refuses() {
    let refused = |json: String, reason: &str| {
        assert_rejected::<TopicVersionInfo>(
            &json,
            &format!("invalid topic version info: {reason}"),
        );
    };
    refused(
        version_json(3, |json| json["version"] = json!(0)),
        "VersionZeroFitted",
    );
    refused(
        version_json(0, |json| json["version"] = json!(7)),
        "UnfittedNonZero",
    );
    refused(
        version_json(0, |json| {
            json["status"]["data"]["activated_at"] = Value::Null
        }),
        "UnfittedNeverActivated",
    );
    refused(
        version_json(1, |json| json["status"]["data"]["by"] = json!(1)),
        "SupersededByOlder",
    );
    refused(
        version_json(3, |json| {
            json["status"]["data"]["fit"]["ready_at"] = json!("2026-10-04T10:59:59.000000Z");
        }),
        "TimestampsOutOfOrder",
    );
    // Dropped before it was superseded, and pinned before it was ready.
    refused(
        version_json(1, |json| {
            json["retention"]["data"]["at"] = json!("2026-10-04T09:59:59.000000Z");
        }),
        "TimestampsOutOfOrder",
    );
    refused(
        version_json(2, |json| {
            json["retention"]["data"]["pin"]["at"] = json!("2026-10-04T08:59:59.000000Z");
        }),
        "TimestampsOutOfOrder",
    );
    refused(
        version_json(4, |json| {
            json["retention"] = to_json(&versions()[2].retention())
        }),
        "PinnedWhileFitting",
    );
    refused(
        version_json(2, |json| {
            json["retention"] = to_json(&versions()[1].retention())
        }),
        "DroppedNotSuperseded",
    );
}

#[test]
fn topic_version_histories_refuse_what_their_constructor_refuses() {
    let refused = |versions: Vec<Value>, reason: &str| {
        assert_rejected::<TopicVersionHistory>(
            &json!({ "versions": versions }).to_string(),
            &format!("invalid topic version history: {reason}"),
        );
    };
    let all: Vec<Value> = versions().iter().map(to_json).collect();
    let pick =
        |indices: &[usize]| -> Vec<Value> { indices.iter().map(|&i| all[i].clone()).collect() };
    refused(pick(&[1, 2, 3, 4]), "MissingVersionZero");
    refused(pick(&[0, 2, 1, 3]), "NotAscending");
    refused(pick(&[0, 1]), "NoActive");
    let also_active = version_json(3, |json| {
        json["status"] = json!({
            "type": "active",
            "data": {
                "fit": {"type": "fitted", "data": json["status"]["data"]["fit"].clone()},
                "activated_at": "2026-10-04T11:40:00.000000Z",
            },
        });
    });
    let also_active: Value = serde_json::from_str(&also_active).expect("JSON");
    refused(
        vec![all[0].clone(), all[1].clone(), all[2].clone(), also_active],
        "SeveralActive",
    );
    let ready_before_active = json!({
        "version": 1,
        "status": {"type": "ready", "data": {"fit": all[1]["status"]["data"]["fit"]["data"].clone()}},
        "retention": {"type": "retained", "data": {"pin": null}},
    });
    refused(
        vec![all[0].clone(), ready_before_active, all[2].clone()],
        "StatusOutOfPlace { version: TopicModelVersion(1) }",
    );
    let wrong_by = version_json(0, |json| json["status"]["data"]["by"] = json!(3));
    let wrong_by: Value = serde_json::from_str(&wrong_by).expect("JSON");
    refused(
        vec![wrong_by, all[1].clone(), all[2].clone(), all[3].clone()],
        "WrongSupersessor { version: TopicModelVersion(0) }",
    );
    // The active index is not on the wire.
    assert_rejected::<TopicVersionHistory>(
        &json!({ "versions": all, "active": 2 }).to_string(),
        "unknown field `active`",
    );
}

#[test]
fn topic_sizes_and_lineage_refuse_what_their_constructors_refuse() {
    let mut duplicate = to_json(&sizes(None));
    duplicate["topics"][1]["topic"] = json!(ULID_A);
    assert_rejected::<TopicSizes>(
        &duplicate.to_string(),
        "invalid topic sizes: DuplicateTopic",
    );

    let entry = |best: Value, others: Value| {
        json!({"topic": ULID_A, "best": best, "others": others}).to_string()
    };
    let link_json =
        |topic: &str, similarity: f32| json!({"topic": topic, "similarity": similarity});
    assert_rejected::<LineageEntry>(
        &entry(Value::Null, json!([link_json(ULID_B, 0.5)])),
        "invalid lineage entry: OthersWithoutBest",
    );
    assert_rejected::<LineageEntry>(
        &entry(link_json(ULID_B, 0.5), json!([link_json(ULID_A, 0.75)])),
        "invalid lineage entry: OutOfOrder",
    );
    assert_rejected::<LineageEntry>(
        &entry(link_json(ULID_B, 0.75), json!([link_json(ULID_B, 0.5)])),
        "invalid lineage entry: DuplicateSuccessor",
    );
    assert_rejected::<LineageLink>(
        &format!(r#"{{"topic": "{ULID_B}", "similarity": 1e39}}"#),
        "invalid similarity",
    );

    let lineage_json = |edit: &dyn Fn(&mut Value)| {
        let mut json = to_json(&lineage());
        edit(&mut json);
        json.to_string()
    };
    assert_rejected::<TopicLineage>(
        &lineage_json(&|json| json["to"] = json!(2)),
        "invalid topic lineage: NotForward",
    );
    assert_rejected::<TopicLineage>(
        &lineage_json(&|json| json["entries"][1]["topic"] = json["entries"][0]["topic"].clone()),
        "invalid topic lineage: DuplicateEntry",
    );
    assert_rejected::<TopicLineage>(
        &lineage_json(&|json| json["floor"] = json!(0.5)),
        "invalid topic lineage: BelowFloor",
    );
}

#[test]
fn topics_refuse_non_finite_weights_and_bad_embeddings() {
    for weight in ["1e39", "-1e39"] {
        let mut json = to_json(&sample_topic());
        json["terms"][0][1] = serde_json::from_str(weight).expect("a JSON number");
        assert_rejected::<Topic>(&json.to_string(), "invalid finite number");
    }
    assert_rejected::<Finite>("1e39", "invalid finite number: NotFinite(inf)");
    assert_rejected::<Finite>("null", "invalid type: null");
    let mut json = to_json(&sample_topic());
    json["size"] = json!(12);
    assert_rejected::<Topic>(&json.to_string(), "unknown field `size`");

    let embedding_json = |values: &str| {
        format!(
            r#"{{"model": {{"name": "nomic-embed-text-v1.5", "dimension": 4}}, "values": {values}}}"#
        )
    };
    assert_rejected::<Embedding>(
        &embedding_json("[0.6, 0.8]"),
        "invalid embedding: WrongDimension { expected: 4, got: 2 }",
    );
    assert_rejected::<Embedding>(
        &embedding_json("[1.0, 1.0, 0.0, 0.0]"),
        "invalid embedding: NotNormalized",
    );
    // Too large for an `f32`: decodes to infinity, whose norm is not 1.
    assert_rejected::<Embedding>(
        &embedding_json("[1e39, 0.0, 0.0, 0.0]"),
        "invalid embedding: NotNormalized { norm: inf }",
    );
    let mut json = to_json(&embedding(model()));
    json["norm"] = json!(1.0);
    assert_rejected::<Embedding>(&json.to_string(), "unknown field `norm`");
    assert_rejected::<EmbeddingModel>(
        r#"{"name": "nomic-embed-text-v1.5", "dimension": 0}"#,
        "invalid value: integer `0`",
    );
}

#[test]
fn topic_versions_refuse_unknown_shapes() {
    assert_rejected::<TopicModelVersion>("-1", "invalid value: integer `-1`");
    assert_rejected::<TopicModelVersion>(r#""3""#, "invalid type: string");
    assert_rejected::<TopicVersionStatus>(
        r#"{"type": "retired", "data": {"started_at": "2026-10-04T12:00:00.000000Z"}}"#,
        "unknown variant `retired`",
    );
    assert_rejected::<FitRecord>(r#"{"type": "refitted"}"#, "unknown variant `refitted`");
    assert_rejected::<Retention>(
        r#"{"type": "archived", "data": {"at": "2026-10-04T12:00:00.000000Z"}}"#,
        "unknown variant `archived`",
    );
    let mut pin = to_json(&Pin {
        by: operator(),
        at: at("10:30:00"),
    });
    pin["until"] = json!("2026-11-04T10:30:00.000000Z");
    assert_rejected::<Pin>(&pin.to_string(), "unknown field `until`");
    let mut size = to_json(&TopicSize {
        topic: topic(0),
        stats: None,
    });
    size["share"] = json!(0.5);
    assert_rejected::<TopicSize>(&size.to_string(), "unknown field `share`");
}
