//! Export on the wire: `QueryApi::export` (an `ExportRequest` in), the
//! header, rows and trailer it streams, each JSONL line, and an export's
//! audit events. `ExportRecord` itself is left to the audit area, whose
//! caller field is changing shape.

use std::num::NonZeroU16;
use std::path::PathBuf;

use serde_json::{Value, json};

use super::super::harness::{
    BLESS, assert_golden, assert_rejected, assert_request_golden, golden_root,
};
use super::super::{ULID_A, ULID_B, id, ts};
use super::fixtures::{
    coder, confirmed_at, day, edited, field, nz, operator, planner, read_at, topic, tx, wiki,
};
use crate::aggregates::edge::{EdgeSelector, RouteKind, TopologyFilter};
use crate::aggregates::filter::TopicVersionSelector;
use crate::aggregates::projection::{
    Fitted, PointParts, PointRoute, ProjectedPoint, ProjectionLimit, ProjectionParams,
    ProjectionSpec,
};
use crate::aggregates::quality::{MatchClass, QualityMatch};
use crate::aggregates::topic::{EmbeddingModel, TopicModelVersion};
use crate::derived::flow::access::AccessKind;
use crate::derived::flow::transmission::Route;
use crate::derived::flow::verdict::{Verdict, VerdictRevision};
use crate::ids::{ExportId, ProjectionId};
use crate::interfaces::l8_surface::evidence::MatchQuotes;
use crate::interfaces::l8_surface::excerpt::{Excerpt, ExcerptWindow, Excerpted};
use crate::interfaces::l8_surface::export::rows::{
    AccessRow, EdgeRow, LabelContent, MatchText, PointRow, TopicContent, TopicRow,
    TransmissionContent, TransmissionRow, VerdictRow,
};
use crate::interfaces::l8_surface::export::{
    ExportBasis, ExportDataset, ExportDatasetKind, ExportEnd, ExportEvent, ExportFailure,
    ExportFormat, ExportHeader, ExportHeaderParts, ExportLine, ExportRequest, ExportRow,
    ExportScope, ExportSealer, ExportTrailer, GatewayVersion, Incomplete, JsonlError,
    JsonlErrorKind, JsonlExport, RowHasher, RowRefused, SourceFailure, read_jsonl, settled_window,
};
use crate::interfaces::l8_surface::summary::{
    Delivery, SummaryState, TopicUnder, TransmissionSummary,
};
use crate::interfaces::l8_surface::{Permission, QueryError};
use crate::support::{Blake3, ByteRange, Finite, NonEmpty, TimeWindow, Watermark};
use crate::wire::DecodeErrorKind;

const AREA: &str = "surface_reads/export";

/// The topic-model version every scoped export here resolves to.
const V: TopicModelVersion = TopicModelVersion(4);

/// A stand-in for BLAKE3: FNV-1a over what it is fed, in four lanes. The
/// goldens pin the trailer's shape, not the hash function; the digest in
/// them is this stand-in's.
#[derive(Debug, Default)]
struct StandInHasher {
    fed: Vec<u8>,
}

impl RowHasher for StandInHasher {
    fn update(&mut self, bytes: &[u8]) {
        self.fed.extend_from_slice(bytes);
    }

    fn finalize(&self) -> Blake3 {
        let mut out = [0_u8; 32];
        for (chunk, lane) in out.chunks_mut(8).zip(0_u64..) {
            let mut hash: u64 = 0xcbf2_9ce4_8422_2325 ^ lane;
            for byte in &self.fed {
                hash ^= u64::from(*byte);
                hash = hash.wrapping_mul(0x0100_0000_01b3);
            }
            chunk.copy_from_slice(&hash.to_le_bytes());
        }
        Blake3::from_bytes(out)
    }
}

// ── Requests ───────────────────────────────────────────────────────────────

fn scope() -> ExportScope {
    ExportScope {
        window: day(),
        filter: TopologyFilter::default(),
    }
}

fn projection() -> ProjectionId {
    id(ProjectionId::from_ulid_text, ULID_B)
}

/// The dataset of each kind, through an exhaustive match.
fn dataset(kind: ExportDatasetKind) -> ExportDataset {
    match kind {
        ExportDatasetKind::Transmissions => ExportDataset::Transmissions(scope()),
        ExportDatasetKind::Edges => ExportDataset::Edges(scope()),
        ExportDatasetKind::Accesses => ExportDataset::Accesses(scope()),
        ExportDatasetKind::Topics => ExportDataset::Topics(scope()),
        ExportDatasetKind::Projection => ExportDataset::Projection(projection()),
        ExportDatasetKind::Verdicts => ExportDataset::Verdicts(day()),
    }
}

fn request(kind: ExportDatasetKind, include_content: bool) -> ExportRequest {
    ExportRequest::new(dataset(kind), ExportFormat::Jsonl, include_content)
        .expect("content only where the dataset has content columns")
}

fn kind_name(kind: ExportDatasetKind) -> String {
    serde_json::to_value(kind)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .expect("a kind is a string")
}

/// Every dataset without content, and with it where it has content
/// columns.
#[test]
fn export_requests_golden_for_every_dataset() {
    for kind in ExportDatasetKind::ALL {
        let name = kind_name(kind);
        assert_request_golden(
            AREA,
            &format!("export_request_{name}"),
            &request(kind, false),
        );
        if kind.has_content_columns() {
            assert_request_golden(
                AREA,
                &format!("export_request_{name}_with_content"),
                &request(kind, true),
            );
        }
    }
    let parquet = ExportRequest::new(
        dataset(ExportDatasetKind::Edges),
        ExportFormat::Parquet,
        false,
    )
    .expect("edges without content");
    assert_request_golden(AREA, "export_request_edges_parquet", &parquet);
}

#[test]
fn export_requests_are_decoded_through_their_constructor() {
    for kind in [ExportDatasetKind::Accesses, ExportDatasetKind::Verdicts] {
        let name = kind_name(kind);
        let json = edited(&request(kind, false), |json| {
            *field(json, "include_content") = Value::Bool(true);
        });
        assert_rejected::<ExportRequest>(
            &json,
            &format!("invalid export request: NoContentColumns {{ dataset: {kind:?} }}"),
        );
        assert!(json.contains(&name));
    }
    let edges = request(ExportDatasetKind::Edges, false);
    assert_rejected::<ExportRequest>(
        &edited(&edges, |json| json["compress"] = Value::Bool(true)),
        "unknown field `compress`",
    );
    assert_rejected::<ExportRequest>(
        &edited(&edges, |json| *field(json, "format") = "csv".into()),
        "unknown variant `csv`",
    );
    assert_rejected::<ExportRequest>(
        &edited(&edges, |json| {
            *field(json, "dataset") = json!({"type": "messages", "data": null});
        }),
        "unknown variant `messages`",
    );
    assert_rejected::<ExportScope>(
        &edited(&scope(), |json| json["limit"] = 10.into()),
        "unknown field `limit`",
    );
}

// ── Header ─────────────────────────────────────────────────────────────────

fn export_id() -> ExportId {
    id(ExportId::from_ulid_text, ULID_A)
}

fn watermark() -> Watermark {
    Watermark(ts("2026-10-04T12:00:00.000000Z"))
}

fn model() -> EmbeddingModel {
    EmbeddingModel {
        name: "text-embedding-3-small".into(),
        dimension: NonZeroU16::new(1536).expect("non-zero"),
    }
}

fn parts(request: ExportRequest, basis: ExportBasis, rows: u64) -> ExportHeaderParts {
    ExportHeaderParts {
        id: export_id(),
        request,
        by: operator(),
        started_at: ts("2026-10-04T12:05:30.120000Z"),
        watermark: watermark(),
        basis,
        embedding_model: model(),
        gateway: GatewayVersion::new("0.4.1").expect("non-blank"),
        rows,
    }
}

fn scoped_basis() -> ExportBasis {
    ExportBasis::Scoped {
        topic_version: V,
        filter: TopologyFilter::default().pinned(V),
        settled: settled_window(day(), watermark()),
    }
}

fn spec() -> ProjectionSpec {
    let params = ProjectionParams::new(
        ProjectionLimit::new(5_000).expect("within the maximum"),
        ProjectionParams::DEFAULT_NEIGHBORS,
        ProjectionParams::DEFAULT_MIN_DIST_MILLI,
        42,
    )
    .expect("valid params");
    let window = TimeWindow::new(
        ts("2026-10-03T00:00:00.000000Z"),
        ts("2026-10-04T00:00:00.000000Z"),
    )
    .expect("a day");
    ProjectionSpec::new(window, TopologyFilter::default(), V, params, model())
}

fn fitted() -> Fitted {
    Fitted {
        started_at: ts("2026-10-04T00:10:00.000000Z"),
        fitted_at: ts("2026-10-04T00:12:31.904000Z"),
        watermark: Watermark(ts("2026-10-04T00:05:00.000000Z")),
        matching: 1_830,
        points: 1_830,
    }
}

/// A header of each basis: scoped (for `kind`'s scoped dataset), verdicts
/// and projection.
fn header(kind: ExportDatasetKind, include_content: bool, rows: u64) -> ExportHeader {
    let basis = match kind {
        ExportDatasetKind::Transmissions
        | ExportDatasetKind::Edges
        | ExportDatasetKind::Accesses
        | ExportDatasetKind::Topics => scoped_basis(),
        ExportDatasetKind::Verdicts => ExportBasis::Verdicts {
            settled: settled_window(day(), watermark()),
        },
        ExportDatasetKind::Projection => ExportBasis::Projection {
            projection: projection(),
            spec: spec(),
            fitted: fitted(),
        },
    };
    ExportHeader::new(parts(request(kind, include_content), basis, rows))
        .expect("the basis is the request's")
}

#[test]
fn export_headers_golden_for_every_basis() {
    fn declared(header: ExportHeader) -> ExportHeader {
        match header.basis() {
            ExportBasis::Scoped { .. }
            | ExportBasis::Verdicts { .. }
            | ExportBasis::Projection { .. } => header,
        }
    }
    assert_golden(
        AREA,
        "export_header_scoped",
        &declared(header(ExportDatasetKind::Transmissions, true, 2)),
    );
    assert_golden(
        AREA,
        "export_header_verdicts",
        &declared(header(ExportDatasetKind::Verdicts, false, 12)),
    );
    assert_golden(
        AREA,
        "export_header_projection",
        &declared(header(ExportDatasetKind::Projection, false, 1_830)),
    );
}

#[test]
fn export_headers_are_decoded_through_their_constructor() {
    let scoped = header(ExportDatasetKind::Transmissions, false, 2);
    let reject = |edit: &dyn Fn(&mut Value), reason: &str| {
        assert_rejected::<ExportHeader>(&edited(&scoped, edit), reason);
    };
    reject(
        &|json| *field(json, "watermark") = "2026-10-04T12:06:00.000000Z".into(),
        "invalid export header: WatermarkAfterStart",
    );
    let verdicts_basis =
        serde_json::to_value(ExportBasis::Verdicts { settled: None }).expect("encodes");
    reject(
        &|json| *field(json, "basis") = verdicts_basis.clone(),
        "invalid export header: BasisForOtherDataset { dataset: Transmissions }",
    );
    let pinned_3 =
        serde_json::to_value(TopicVersionSelector::Pinned(TopicModelVersion(3))).expect("encodes");
    reject(
        &|json| json["request"]["dataset"]["data"]["filter"]["topic_version"] = pinned_3.clone(),
        "invalid export header: VersionMismatch",
    );
    let current = serde_json::to_value(TopicVersionSelector::Current).expect("encodes");
    reject(
        &|json| json["basis"]["data"]["filter"]["topic_version"] = current.clone(),
        "invalid export header: FilterNotPinned",
    );
    reject(
        &|json| json["basis"]["data"]["settled"] = Value::Null,
        "invalid export header: SettledWindow",
    );
    reject(
        &|json| json["requested_by"] = json!(ULID_B),
        "unknown field `requested_by`",
    );
    let projection = header(ExportDatasetKind::Projection, false, 1_830);
    let other = id(ProjectionId::from_ulid_text, ULID_A);
    assert_rejected::<ExportHeader>(
        &edited(&projection, |json| {
            json["basis"]["data"]["projection"] = serde_json::to_value(other).expect("encodes");
        }),
        "invalid export header: OtherProjection",
    );
    assert_rejected::<ExportHeader>(
        &edited(&projection, |json| *field(json, "rows") = 1_829.into()),
        "invalid export header: PlannedRows { planned: 1829, points: 1830 }",
    );
    assert_rejected::<ExportBasis>(
        r#"{"type": "everything", "data": null}"#,
        "unknown variant `everything`",
    );
    assert_rejected::<GatewayVersion>(r#""   ""#, "invalid non-blank text: Blank");
}

// ── Rows ───────────────────────────────────────────────────────────────────

fn hour() -> TimeWindow {
    TimeWindow::new(
        ts("2026-10-04T09:00:00.000000Z"),
        ts("2026-10-04T10:00:00.000000Z"),
    )
    .expect("an hour")
}

fn matched_only(text: &str) -> Excerpted {
    let end = u32::try_from(text.len()).expect("short");
    let range = ByteRange::new(0, end).expect("not empty");
    Excerpted::Shown(Excerpt::cut(text, range, ExcerptWindow::MATCH_ONLY).expect("fits"))
}

fn transmission_row(content: bool) -> TransmissionRow {
    let summary = TransmissionSummary {
        id: tx(),
        to: coder(),
        route: Route::Channel(wiki()),
        opened_at: ts("2026-10-04T09:16:40.002513Z"),
        state: SummaryState::Classified {
            delivery: Delivery {
                from: planner(),
                confirmed_at: confirmed_at(),
                matched_bytes: nz(47),
            },
            topic: TopicUnder::Topic(topic()),
            verdict: Some(Verdict::Genuine),
        },
    };
    let content = content.then(|| TransmissionContent {
        topic_label: Some("release planning".into()),
        matches: NonEmpty::new(MatchText {
            class: MatchClass::Exact,
            quotes: MatchQuotes {
                origin: matched_only("ship v2.4 on Friday after QA signs off Thursday"),
                read: Excerpted::BodyDropped {
                    message: read_at().part.message,
                },
            },
        }),
    });
    TransmissionRow::new(summary, MatchClass::Exact, content).expect("a confirmed summary")
}

fn access_row(op: AccessKind, accesses: u64) -> AccessRow {
    let agent = match op {
        AccessKind::Write => planner(),
        AccessKind::Read => coder(),
    };
    AccessRow {
        agent,
        channel: wiki(),
        op,
        bucket: hour(),
        accesses: nz(accesses),
    }
}

/// A row of each dataset, through an exhaustive match.
fn row(kind: ExportDatasetKind) -> ExportRow {
    match kind {
        ExportDatasetKind::Transmissions => {
            ExportRow::Transmission(Box::new(transmission_row(true)))
        }
        ExportDatasetKind::Edges => ExportRow::Edge(EdgeRow {
            edge: EdgeSelector::new(planner(), coder(), Route::Channel(wiki()))
                .expect("two agents"),
            topic: Some(topic()),
            bucket: hour(),
            transmissions: nz(3),
            matched_bytes: nz(141),
            content: Some(LabelContent {
                topic_label: Some("release planning".into()),
            }),
        }),
        ExportDatasetKind::Accesses => ExportRow::Access(access_row(AccessKind::Read, 5)),
        ExportDatasetKind::Topics => ExportRow::Topic(TopicRow {
            topic: topic(),
            transmissions: 9,
            matched_bytes: 1_204,
            content: Some(TopicContent {
                label: "release planning".into(),
                terms: vec![
                    ("release".into(), Finite::new(0.42).expect("finite")),
                    ("friday".into(), Finite::new(0.17).expect("finite")),
                ],
            }),
        }),
        ExportDatasetKind::Projection => ExportRow::Point(PointRow {
            index: 0,
            point: ProjectedPoint::new(PointParts {
                transmission: tx(),
                from: planner(),
                to: coder(),
                route: PointRoute::Channel(wiki()),
                topic: None,
                confirmed_at: confirmed_at(),
                x: Finite::new(3.25).expect("finite"),
                y: Finite::new(-1.5).expect("finite"),
            })
            .expect("a point between two agents"),
            content: None,
        }),
        ExportDatasetKind::Verdicts => ExportRow::Verdict(VerdictRow {
            transmission: tx(),
            route_kind: RouteKind::Channel,
            call: QualityMatch::Content(MatchClass::Exact),
            revision: VerdictRevision::FIRST,
            verdict: Some(Verdict::Genuine),
            by: operator(),
            at: ts("2026-10-04T11:03:12.000000Z"),
            note: Some("confirmed with the planner".into()),
        }),
    }
}

#[test]
fn export_rows_golden_for_every_dataset() {
    for kind in ExportDatasetKind::ALL {
        let row = row(kind);
        assert_eq!(row.kind(), kind);
        assert_golden(AREA, &format!("export_row_{}", kind_name(kind)), &row);
    }
    assert_golden(
        AREA,
        "export_row_transmissions_without_content",
        &ExportRow::Transmission(Box::new(transmission_row(false))),
    );
}

#[test]
fn export_rows_refuse_what_their_constructors_refuse() {
    let row = transmission_row(false);
    assert_rejected::<TransmissionRow>(
        &edited(&row, |json| {
            json["summary"]["state"] = json!({"type": "detected"});
        }),
        "invalid transmission row: NotConfirmed(Detected)",
    );
    assert_rejected::<TransmissionRow>(
        &edited(&row, |json| {
            json["delivery"] = json["summary"]["state"]["data"]["delivery"].clone();
        }),
        "unknown field `delivery`",
    );
    assert_rejected::<ExportRow>(
        r#"{"type": "message", "data": null}"#,
        "unknown variant `message`",
    );
    assert_rejected::<AccessRow>(
        &edited(&access_row(AccessKind::Write, 1), |json| {
            *field(json, "accesses") = 0.into();
        }),
        "nonzero",
    );
}

// ── Trailer ────────────────────────────────────────────────────────────────

/// The access rows of the small export: the planner's write bucket, then
/// the coder's read bucket, in key order.
fn access_rows() -> Vec<ExportRow> {
    vec![
        ExportRow::Access(access_row(AccessKind::Write, 2)),
        ExportRow::Access(access_row(AccessKind::Read, 5)),
    ]
}

fn sealer(planned: u64) -> ExportSealer<StandInHasher> {
    ExportSealer::new(
        &header(ExportDatasetKind::Accesses, false, planned),
        StandInHasher::default(),
    )
}

/// The trailer of every way an export ends, named after it.
fn every_end() -> Vec<(&'static str, ExportTrailer)> {
    let [write, read] = <[ExportRow; 2]>::try_from(access_rows()).expect("two rows");
    let complete = {
        let mut sealer = sealer(2);
        for row in [&write, &read] {
            sealer.push(row).expect("in order, within the plan");
        }
        sealer.finish()
    };
    let after_one = || {
        let mut sealer = sealer(2);
        sealer.push(&write).expect("the first row");
        sealer
    };
    let store = after_one().fail(SourceFailure::Store {
        reason: "edge store: connection reset".into(),
    });
    let not_retained = after_one().fail(SourceFailure::VersionNotRetained { version: V });
    let short = after_one().finish();
    let refused = {
        let mut sealer = after_one();
        let edge = row(ExportDatasetKind::Edges);
        assert!(sealer.push(&edge).is_err());
        sealer.finish()
    };
    let trailers = [
        ("export_trailer_complete", complete),
        ("export_trailer_store", store),
        ("export_trailer_version_not_retained", not_retained),
        ("export_trailer_count_mismatch", short),
        ("export_trailer_invalid_row", refused),
    ];
    for (name, trailer) in &trailers {
        let declared = match trailer.end() {
            ExportEnd::Complete => "export_trailer_complete",
            ExportEnd::Failed(ExportFailure::Store { .. }) => "export_trailer_store",
            ExportEnd::Failed(ExportFailure::VersionNotRetained { .. }) => {
                "export_trailer_version_not_retained"
            }
            ExportEnd::Failed(ExportFailure::CountMismatch { .. }) => {
                "export_trailer_count_mismatch"
            }
            ExportEnd::Failed(ExportFailure::InvalidRow { .. }) => "export_trailer_invalid_row",
        };
        assert_eq!(*name, declared);
    }
    trailers.into()
}

#[test]
fn export_trailers_golden_for_every_end() {
    for (name, trailer) in every_end() {
        assert_golden(AREA, name, &trailer);
    }
}

#[test]
fn row_refusals_golden_in_every_variant() {
    fn declared(refused: RowRefused) -> RowRefused {
        match refused {
            RowRefused::OtherDataset { .. }
            | RowRefused::ContentMismatch { .. }
            | RowRefused::OutOfOrder
            | RowRefused::BeyondPlan { .. }
            | RowRefused::AfterRefusal => refused,
        }
    }
    let every = [
        RowRefused::OtherDataset {
            expected: ExportDatasetKind::Accesses,
            got: ExportDatasetKind::Edges,
        },
        RowRefused::ContentMismatch { requested: false },
        RowRefused::OutOfOrder,
        RowRefused::BeyondPlan { planned: 2 },
        RowRefused::AfterRefusal,
    ]
    .map(declared);
    assert_golden(AREA, "row_refused_every_variant", &every.to_vec());
}

/// A trailer is checked for what the sealer guarantees about it alone.
#[test]
fn export_trailers_refuse_what_no_sealer_builds() {
    let trailer = |rows: u64, failure: Value| {
        json!({
            "export": ULID_A,
            "rows": rows,
            "digest": "00".repeat(32),
            "end": {"type": "failed", "data": failure},
        })
        .to_string()
    };
    assert_rejected::<ExportTrailer>(
        &trailer(
            1,
            json!({"type": "count_mismatch", "data": {"planned": 2, "produced": 2}}),
        ),
        "invalid export trailer: CountMismatch { rows: 1, planned: 2, produced: 2 }",
    );
    assert_rejected::<ExportTrailer>(
        &trailer(
            2,
            json!({"type": "count_mismatch", "data": {"planned": 2, "produced": 2}}),
        ),
        "invalid export trailer: CountMismatch { rows: 2, planned: 2, produced: 2 }",
    );
    assert_rejected::<ExportTrailer>(
        &trailer(
            3,
            json!({"type": "invalid_row", "data": {"index": 1, "refused": {"type": "out_of_order"}}}),
        ),
        "invalid export trailer: RefusalIndex { rows: 3, index: 1 }",
    );
    assert_rejected::<ExportTrailer>(
        &trailer(
            1,
            json!({"type": "invalid_row", "data": {"index": 1, "refused": {"type": "after_refusal"}}}),
        ),
        "invalid export trailer: NotFirstRefusal",
    );
    assert_rejected::<ExportTrailer>(
        &trailer(
            1,
            json!({"type": "invalid_row", "data": {"index": 1, "refused": {"type": "beyond_plan", "data": {"planned": 4}}}}),
        ),
        "invalid export trailer: NotFirstRefusal",
    );
    let (_, complete) = every_end().swap_remove(0);
    assert_rejected::<ExportTrailer>(
        &edited(&complete, |json| json["planned"] = 2.into()),
        "unknown field `planned`",
    );
    assert_rejected::<ExportFailure>(
        r#"{"type": "timeout", "data": null}"#,
        "unknown variant `timeout`",
    );
}

// ── Audit events ───────────────────────────────────────────────────────────

#[test]
fn export_events_golden_in_every_variant() {
    fn declared(event: ExportEvent) -> ExportEvent {
        match event {
            ExportEvent::Refused(_)
            | ExportEvent::Started(_)
            | ExportEvent::Ended(_)
            | ExportEvent::Abandoned { .. } => event,
        }
    }
    let (_, complete) = every_end().swap_remove(0);
    let every = vec![
        ExportEvent::Refused(QueryError::Forbidden {
            missing: Permission::Content,
        }),
        ExportEvent::Started(Box::new(header(ExportDatasetKind::Accesses, false, 2))),
        ExportEvent::Ended(complete),
        ExportEvent::Abandoned {
            export: export_id(),
            rows: 1,
        },
    ]
    .into_iter()
    .map(declared)
    .collect::<Vec<_>>();
    assert_golden(AREA, "export_event_every_variant", &every);
    assert_rejected::<ExportEvent>(
        r#"{"type": "paused", "data": null}"#,
        "unknown variant `paused`",
    );
}

// ── JSONL framing ──────────────────────────────────────────────────────────

fn jsonl_golden_path() -> PathBuf {
    golden_root().join(AREA).join("export_complete.jsonl")
}

/// The lines of the small export: its header, two access rows and the
/// complete trailer.
fn complete_export() -> Vec<ExportLine> {
    let (_, complete) = every_end().swap_remove(0);
    std::iter::once(ExportLine::Header(Box::new(header(
        ExportDatasetKind::Accesses,
        false,
        2,
    ))))
    .chain(access_rows().into_iter().map(ExportLine::Row))
    .chain(std::iter::once(ExportLine::Trailer(complete)))
    .collect()
}

fn jsonl(lines: &[ExportLine]) -> String {
    lines
        .iter()
        .map(|line| serde_json::to_string(line).expect("a line encodes") + "\n")
        .collect()
}

/// A complete JSONL export, byte for byte: one compact JSON object per
/// line, each ended by `\n`.
#[test]
fn a_complete_jsonl_export_golden() {
    let lines = complete_export();
    let body = jsonl(&lines);
    let path = jsonl_golden_path();
    if std::env::var(BLESS).is_ok_and(|value| value == "1") {
        let dir = path.parent().expect("a parent");
        std::fs::create_dir_all(dir).unwrap_or_else(|error| panic!("create {dir:?}: {error}"));
        std::fs::write(&path, &body).unwrap_or_else(|error| panic!("write {path:?}: {error}"));
    }
    let golden = std::fs::read_to_string(&path).unwrap_or_else(|error| {
        panic!("golden {path:?} unreadable ({error}); run with {BLESS}=1 to write it")
    });
    assert_eq!(body, golden, "the JSONL export differs from its golden");
    assert_eq!(golden.lines().count(), 4);
    assert!(!golden.contains("\n\n") && !golden.contains('\r'));
    for line in golden.lines() {
        let value: Value = serde_json::from_str(line).expect("each line is JSON");
        assert_eq!(
            value.as_object().map(|object| object.len()),
            Some(2),
            "{line}"
        );
    }

    let read = read_jsonl(golden.as_bytes()).expect("the framing holds");
    let [
        ExportLine::Header(header),
        ExportLine::Row(first),
        ExportLine::Row(second),
        ExportLine::Trailer(trailer),
    ] = <[ExportLine; 4]>::try_from(lines).expect("four lines")
    else {
        panic!("a header, two rows and a trailer");
    };
    assert_eq!(
        read,
        JsonlExport {
            header: *header,
            rows: vec![first, second],
            trailer: Some(trailer),
        }
    );
    assert_eq!(read.verify(StandInHasher::default()), Ok(()));
}

/// Every way a body can be cut short reads as an export without a trailer.
#[test]
fn a_cut_off_jsonl_export_has_no_trailer() {
    let body = jsonl(&complete_export());
    let without_newline = body.strip_suffix('\n').expect("ends with a newline");
    let cut_in_trailer = &body[..body.len() - 20];
    for cut in [without_newline, cut_in_trailer] {
        let read = read_jsonl(cut.as_bytes()).expect("the lines before the cut are framed");
        assert_eq!(read.rows.len(), 2);
        assert_eq!(read.trailer, None);
        assert_eq!(
            read.verify(StandInHasher::default()),
            Err(Incomplete::NoTrailer)
        );
    }
    let header_only = body.split_inclusive('\n').next().expect("a header line");
    let read = read_jsonl(header_only.as_bytes()).expect("a header alone is framed");
    assert!(read.rows.is_empty() && read.trailer.is_none());
    let cut_in_header = &header_only[..header_only.len() - 2];
    assert_eq!(
        read_jsonl(cut_in_header.as_bytes()),
        Err(JsonlError {
            line: 1,
            kind: JsonlErrorKind::NoHeader,
        })
    );
}

#[test]
fn jsonl_framing_errors() {
    let lines = complete_export();
    let line = |index: usize| jsonl(&lines[index..=index]);
    let kind = |body: String| read_jsonl(body.as_bytes()).map_err(|error| (error.line, error.kind));
    assert_eq!(kind(String::new()), Err((1, JsonlErrorKind::NoHeader)));
    assert_eq!(
        kind(line(1) + &line(0)),
        Err((1, JsonlErrorKind::HeaderNotFirst))
    );
    assert_eq!(
        kind(line(0) + &line(1) + &line(0)),
        Err((3, JsonlErrorKind::SecondHeader))
    );
    assert_eq!(
        kind(jsonl(&lines) + &line(2)),
        Err((5, JsonlErrorKind::AfterTrailer))
    );
    match kind(line(0) + "\n" + &line(1)) {
        Err((2, JsonlErrorKind::Undecodable(error))) => {
            assert_eq!(error.kind, DecodeErrorKind::Eof);
        }
        other => panic!("a blank line is undecodable, not {other:?}"),
    }
    match kind(line(0) + r#"{"type":"footer","data":null}"# + "\n") {
        Err((2, JsonlErrorKind::Undecodable(error))) => {
            assert_eq!(error.kind, DecodeErrorKind::Data);
            assert!(
                error.reason.contains("unknown variant `footer`"),
                "{}",
                error.reason
            );
        }
        other => panic!("an unknown line type is undecodable, not {other:?}"),
    }
}
