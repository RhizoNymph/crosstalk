//! Export requests, headers, rows read from stored values, limits, plan
//! errors and audit records. The stream, the sealer and verification are
//! in `export_stream`.

use std::collections::HashMap;
use std::num::{NonZeroU16, NonZeroU64};

use crate::aggregates::edge::{RouteKind, TopologyFilter};
use crate::aggregates::filter::{TopicVersionSelector, VersionUnavailable};
use crate::aggregates::projection::frame::{FrameHeader, ProjectionFrame};
use crate::aggregates::projection::{
    Fitted, PointParts, PointRoute, ProjectedPoint, Projection, ProjectionInfo, ProjectionLimit,
    ProjectionParams, ProjectionSpec, ProjectionStatus,
};
use crate::aggregates::quality::{MatchClass, QualityMatch};
use crate::aggregates::topic::{EmbeddingModel, TopicModelVersion};
use crate::aliases::NoAliases;
use crate::derived::flow::transmission::{Route, Transmission, TransmissionState};
use crate::derived::flow::verdict::{TransmissionVerdict, Verdict, VerdictLog, VerdictRevision};
use crate::ids::{AuditId, ExportId, OperatorId, ProjectionId, TopicId};
use crate::interfaces::l6_analysis::ProjectionStoreError;
use crate::interfaces::l8_surface::audit::{AuditAuthor, AuditBody, AuditEntry, AuditSubject};
use crate::interfaces::l8_surface::export::rows::{
    InvalidTransmissionRow, PointRow, TransmissionRow, VerdictRow, VerdictRowsError,
    projection_rows, verdict_rows,
};
use crate::interfaces::l8_surface::export::{
    ExportBasis, ExportDataset, ExportDatasetKind, ExportEvent, ExportFormat, ExportHeader,
    ExportHeaderParts, ExportLimits, ExportPlanError, ExportRecord, ExportRequest, ExportRow,
    ExportScope, GatewayVersion, InvalidExportRecord, InvalidExportRequest, InvalidHeader,
    settled_window,
};
use crate::interfaces::l8_surface::export::{
    ExportFormats, InvalidExportFormats, UnsupportedFormat,
};
use crate::interfaces::l8_surface::summary::{TopicUnder, TransmissionSummary};
use crate::interfaces::l8_surface::{ConflictKind, InputError, Permission, QueryError};
use crate::support::{Finite, TimeWindow, Watermark};
use crate::tests::fixtures::{agent, at, transmission};
use crate::tests::operators::caller;
use crate::tests::verdicts::{confirmed, every_state};

pub(super) const V: TopicModelVersion = TopicModelVersion(3);

pub(super) fn window(start: u64, end: u64) -> TimeWindow {
    TimeWindow::new(at(start), at(end)).expect("non-empty fixture window")
}

pub(super) fn scope() -> ExportScope {
    ExportScope {
        window: window(0, 1_500),
        filter: TopologyFilter::default(),
    }
}

pub(super) fn projection_id() -> ProjectionId {
    ProjectionId::from_ulid(77)
}

fn every_dataset() -> Vec<ExportDataset> {
    vec![
        ExportDataset::Transmissions(scope()),
        ExportDataset::Edges(scope()),
        ExportDataset::Accesses(scope()),
        ExportDataset::Topics(scope()),
        ExportDataset::Projection(projection_id()),
        ExportDataset::Verdicts(window(0, 1_500)),
    ]
}

pub(super) fn request(dataset: ExportDataset, include_content: bool) -> ExportRequest {
    ExportRequest::new(dataset, ExportFormat::Jsonl, include_content).expect("valid request")
}

pub(super) fn model() -> EmbeddingModel {
    EmbeddingModel {
        name: "embed".into(),
        dimension: NonZeroU16::new(4).expect("non-zero"),
    }
}

pub(super) fn operator() -> OperatorId {
    OperatorId::from_ulid(1)
}

/// A header started at 2,000 under watermark 1,000 by operator 1.
pub(super) fn parts(request: ExportRequest, basis: ExportBasis, rows: u64) -> ExportHeaderParts {
    ExportHeaderParts {
        id: ExportId::from_ulid(9),
        request,
        by: operator(),
        started_at: at(2_000),
        watermark: Watermark(at(1_000)),
        basis,
        embedding_model: model(),
        gateway: GatewayVersion::new("0.4.1").expect("non-blank"),
        rows,
    }
}

/// The basis of a `scope()` export resolved to `V` under watermark 1,000.
pub(super) fn scoped_basis() -> ExportBasis {
    ExportBasis::Scoped {
        topic_version: V,
        filter: TopologyFilter::default().pinned(V),
        settled: Some(window(0, 1_000)),
    }
}

/// A header for `dataset` (a scoped one or verdicts) planning `rows` rows.
pub(super) fn header(dataset: ExportDataset, include_content: bool, rows: u64) -> ExportHeader {
    let basis = match dataset {
        ExportDataset::Verdicts(_) => ExportBasis::Verdicts {
            settled: Some(window(0, 1_000)),
        },
        _ => scoped_basis(),
    };
    ExportHeader::new(parts(request(dataset, include_content), basis, rows)).expect("valid header")
}

// ── Projection fixtures ────────────────────────────────────────────────────

fn spec() -> ProjectionSpec {
    let params = ProjectionParams::new(
        ProjectionLimit::new(10).expect("in range"),
        ProjectionParams::DEFAULT_NEIGHBORS,
        ProjectionParams::DEFAULT_MIN_DIST_MILLI,
        7,
    )
    .expect("valid params");
    ProjectionSpec::new(
        window(0, 500),
        TopologyFilter::default(),
        V,
        params,
        model(),
    )
}

fn fitted(points: u32) -> Fitted {
    Fitted {
        started_at: at(600),
        fitted_at: at(700),
        watermark: Watermark(at(500)),
        matching: u64::from(points),
        points,
    }
}

fn topic(n: u128) -> TopicId {
    TopicId::from_ulid(n)
}

fn point(n: u128, topic: Option<TopicId>) -> ProjectedPoint {
    ProjectedPoint::new(PointParts {
        transmission: transmission(n),
        from: agent(1),
        to: agent(2),
        route: PointRoute::Unobserved,
        topic,
        confirmed_at: at(100),
        x: Finite::new(0.25).expect("finite"),
        y: Finite::new(-2.0).expect("finite"),
    })
    .expect("a point between two agents")
}

pub(super) fn points() -> Vec<ProjectedPoint> {
    vec![
        point(3, Some(topic(5))),
        point(1, None),
        point(2, Some(topic(6))),
    ]
}

pub(super) fn projection() -> Projection {
    let points = points();
    let count = u32::try_from(points.len()).expect("small");
    let info = ProjectionInfo::new(
        projection_id(),
        spec(),
        operator(),
        at(550),
        ProjectionStatus::Ready(fitted(count)),
    )
    .expect("ready job");
    let frame = ProjectionFrame::from_points(
        FrameHeader {
            projection: projection_id(),
            topic_version: V,
            watermark: Watermark(at(500)),
            limit: spec().params().limit(),
            matching: u64::from(count),
        },
        &points,
    )
    .expect("valid frame");
    Projection::new(info, frame).expect("frame matches job")
}

fn projection_basis(points: u32) -> ExportBasis {
    ExportBasis::Projection {
        projection: projection_id(),
        spec: spec(),
        fitted: fitted(points),
    }
}

// ── Requests ───────────────────────────────────────────────────────────────

#[test]
fn content_columns_by_dataset() {
    let cases = [
        (ExportDatasetKind::Transmissions, true, false),
        (ExportDatasetKind::Edges, true, false),
        (ExportDatasetKind::Accesses, false, false),
        (ExportDatasetKind::Topics, true, false),
        (ExportDatasetKind::Projection, true, true),
        (ExportDatasetKind::Verdicts, false, false),
    ];
    assert_eq!(
        cases.iter().map(|case| case.0).collect::<Vec<_>>(),
        ExportDatasetKind::ALL.to_vec()
    );
    for (kind, columns, content_only) in cases {
        assert_eq!(kind.has_content_columns(), columns, "{kind:?}");
        assert_eq!(kind.is_content_only(), content_only, "{kind:?}");
    }
}

#[test]
fn dataset_codes_are_one_to_six_in_order() {
    let codes: Vec<u8> = ExportDatasetKind::ALL
        .iter()
        .map(|kind| kind.code())
        .collect();
    assert_eq!(codes, vec![1, 2, 3, 4, 5, 6]);
}

#[test]
fn every_dataset_reports_its_kind_and_scope() {
    for (dataset, kind) in every_dataset().into_iter().zip(ExportDatasetKind::ALL) {
        assert_eq!(dataset.kind(), kind);
        let scoped = !matches!(
            kind,
            ExportDatasetKind::Projection | ExportDatasetKind::Verdicts
        );
        assert_eq!(dataset.scope().is_some(), scoped, "{kind:?}");
    }
}

#[test]
fn request_refuses_content_for_datasets_without_content_columns() {
    for dataset in every_dataset() {
        let kind = dataset.kind();
        for format in [ExportFormat::Jsonl, ExportFormat::Parquet] {
            let without = ExportRequest::new(dataset.clone(), format, false);
            assert!(without.is_ok(), "{kind:?} without content");
            let with = ExportRequest::new(dataset.clone(), format, true);
            if kind.has_content_columns() {
                let with = with.expect("content columns exist");
                assert!(with.include_content());
                assert_eq!(with.format(), format);
                assert_eq!(*with.dataset(), dataset);
            } else {
                assert_eq!(
                    with,
                    Err(InvalidExportRequest::NoContentColumns { dataset: kind })
                );
            }
        }
    }
}

#[test]
fn required_permission_is_content_for_content_or_a_projection() {
    for dataset in every_dataset() {
        let kind = dataset.kind();
        let plain = request(dataset.clone(), false);
        let expected = if kind == ExportDatasetKind::Projection {
            Permission::Content
        } else {
            Permission::View
        };
        assert_eq!(plain.required_permission(), expected, "{kind:?}");
        if kind.has_content_columns() {
            assert_eq!(
                request(dataset, true).required_permission(),
                Permission::Content,
                "{kind:?} with content"
            );
        }
    }
}

// ── Settled window ─────────────────────────────────────────────────────────

#[test]
fn settled_window_cuts_at_the_watermark() {
    let w = Watermark(at(1_000));
    assert_eq!(settled_window(window(0, 500), w), Some(window(0, 500)));
    assert_eq!(settled_window(window(0, 1_000), w), Some(window(0, 1_000)));
    assert_eq!(
        settled_window(window(200, 1_500), w),
        Some(window(200, 1_000))
    );
    assert_eq!(settled_window(window(1_000, 1_500), w), None);
    assert_eq!(settled_window(window(1_200, 1_500), w), None);
}

// ── Headers ────────────────────────────────────────────────────────────────

#[test]
fn header_accepts_every_dataset_with_its_basis() {
    for dataset in every_dataset() {
        let kind = dataset.kind();
        let (basis, rows) = match kind {
            ExportDatasetKind::Projection => (projection_basis(3), 3),
            ExportDatasetKind::Verdicts => (
                ExportBasis::Verdicts {
                    settled: Some(window(0, 1_000)),
                },
                4,
            ),
            _ => (scoped_basis(), 4),
        };
        let header = ExportHeader::new(parts(request(dataset, false), basis.clone(), rows))
            .expect("matching basis");
        assert_eq!(*header.basis(), basis, "{kind:?}");
        assert_eq!(header.rows(), rows);
        assert_eq!(header.watermark(), Watermark(at(1_000)));
        assert_eq!(header.embedding_model(), &model());
        assert_eq!(header.gateway().as_str(), "0.4.1");
        assert_eq!(header.by(), operator());
        let version = match kind {
            ExportDatasetKind::Verdicts => None,
            _ => Some(V),
        };
        assert_eq!(header.basis().topic_version(), version, "{kind:?}");
    }
}

#[test]
fn header_rejects_a_basis_for_another_dataset() {
    let cases = [
        (ExportDataset::Edges(scope()), projection_basis(3)),
        (
            ExportDataset::Topics(scope()),
            ExportBasis::Verdicts {
                settled: Some(window(0, 1_000)),
            },
        ),
        (ExportDataset::Verdicts(window(0, 1_500)), scoped_basis()),
        (ExportDataset::Projection(projection_id()), scoped_basis()),
    ];
    for (dataset, basis) in cases {
        let kind = dataset.kind();
        assert_eq!(
            ExportHeader::new(parts(request(dataset, false), basis, 3)),
            Err(InvalidHeader::BasisForOtherDataset { dataset: kind })
        );
    }
}

#[test]
fn header_pins_the_requested_version() {
    let pinned = ExportScope {
        window: window(0, 1_500),
        filter: TopologyFilter {
            topic_version: TopicVersionSelector::Pinned(V),
            ..TopologyFilter::default()
        },
    };
    let ok = ExportHeader::new(parts(
        request(ExportDataset::Edges(pinned.clone()), false),
        scoped_basis(),
        1,
    ));
    assert!(ok.is_ok());
    let other = ExportBasis::Scoped {
        topic_version: TopicModelVersion(4),
        filter: TopologyFilter::default().pinned(TopicModelVersion(4)),
        settled: Some(window(0, 1_000)),
    };
    assert_eq!(
        ExportHeader::new(parts(
            request(ExportDataset::Edges(pinned), false),
            other,
            1
        )),
        Err(InvalidHeader::VersionMismatch {
            requested: V,
            resolved: TopicModelVersion(4),
        })
    );
}

#[test]
fn header_rejects_a_filter_other_than_the_request_pinned() {
    let unpinned = ExportBasis::Scoped {
        topic_version: V,
        filter: TopologyFilter::default(),
        settled: Some(window(0, 1_000)),
    };
    let altered = ExportBasis::Scoped {
        topic_version: V,
        filter: TopologyFilter {
            agents: vec![agent(4)],
            ..TopologyFilter::default()
        }
        .pinned(V),
        settled: Some(window(0, 1_000)),
    };
    for basis in [unpinned, altered] {
        assert_eq!(
            ExportHeader::new(parts(
                request(ExportDataset::Transmissions(scope()), false),
                basis,
                1
            )),
            Err(InvalidHeader::FilterNotPinned)
        );
    }
}

#[test]
fn header_rejects_a_settled_window_not_cut_at_the_watermark() {
    for settled in [None, Some(window(0, 1_500)), Some(window(0, 900))] {
        let scoped = ExportBasis::Scoped {
            topic_version: V,
            filter: TopologyFilter::default().pinned(V),
            settled,
        };
        assert_eq!(
            ExportHeader::new(parts(
                request(ExportDataset::Accesses(scope()), false),
                scoped,
                1
            )),
            Err(InvalidHeader::SettledWindow)
        );
        assert_eq!(
            ExportHeader::new(parts(
                request(ExportDataset::Verdicts(window(0, 1_500)), false),
                ExportBasis::Verdicts { settled },
                1
            )),
            Err(InvalidHeader::SettledWindow)
        );
    }
}

#[test]
fn header_with_nothing_settled_plans_no_rows() {
    let late = ExportScope {
        window: window(1_200, 1_500),
        filter: TopologyFilter::default(),
    };
    let basis = ExportBasis::Scoped {
        topic_version: V,
        filter: TopologyFilter::default().pinned(V),
        settled: None,
    };
    let header = ExportHeader::new(parts(
        request(ExportDataset::Transmissions(late), false),
        basis,
        0,
    ))
    .expect("an empty settled window is valid");
    assert_eq!(header.rows(), 0);
}

#[test]
fn header_rejects_a_watermark_after_its_start() {
    let mut late = parts(
        request(ExportDataset::Transmissions(scope()), false),
        scoped_basis(),
        1,
    );
    late.started_at = at(999);
    assert_eq!(
        ExportHeader::new(late),
        Err(InvalidHeader::WatermarkAfterStart)
    );
}

#[test]
fn projection_header_names_its_projection_and_plans_its_points() {
    let dataset = ExportDataset::Projection(projection_id());
    assert_eq!(
        ExportHeader::new(parts(
            request(dataset.clone(), false),
            projection_basis(3),
            2
        )),
        Err(InvalidHeader::PlannedRows {
            planned: 2,
            points: 3
        })
    );
    let other = ExportDataset::Projection(ProjectionId::from_ulid(78));
    assert_eq!(
        ExportHeader::new(parts(request(other, false), projection_basis(3), 3)),
        Err(InvalidHeader::OtherProjection)
    );
}

#[test]
fn gateway_version_is_not_blank() {
    assert!(GatewayVersion::new("  ").is_err());
    assert_eq!(
        GatewayVersion::new(" 1.2.3 ").expect("non-blank").as_str(),
        "1.2.3"
    );
}

// ── Formats ────────────────────────────────────────────────────────────────

#[test]
fn offered_formats_are_non_empty_and_distinct_in_offer_order() {
    assert_eq!(
        ExportFormats::new(Vec::new()),
        Err(InvalidExportFormats::Empty)
    );
    assert_eq!(
        ExportFormats::new(vec![
            ExportFormat::Parquet,
            ExportFormat::Jsonl,
            ExportFormat::Parquet
        ]),
        Err(InvalidExportFormats::Duplicate(ExportFormat::Parquet))
    );
    let both =
        ExportFormats::new(vec![ExportFormat::Parquet, ExportFormat::Jsonl]).expect("distinct");
    assert_eq!(
        both.as_slice(),
        &[ExportFormat::Parquet, ExportFormat::Jsonl]
    );
    assert_eq!(both.first(), ExportFormat::Parquet);
}

#[test]
fn a_format_the_gateway_does_not_write_is_refused() {
    let jsonl = ExportFormats::new(vec![ExportFormat::Jsonl]).expect("one format");
    assert!(jsonl.offers(ExportFormat::Jsonl));
    assert_eq!(jsonl.check(ExportFormat::Jsonl), Ok(()));
    assert!(!jsonl.offers(ExportFormat::Parquet));
    assert_eq!(
        jsonl.check(ExportFormat::Parquet),
        Err(UnsupportedFormat {
            format: ExportFormat::Parquet
        })
    );
    assert_eq!(
        QueryError::from(UnsupportedFormat {
            format: ExportFormat::Parquet
        }),
        QueryError::InvalidInput(InputError::UnsupportedFormat {
            format: ExportFormat::Parquet
        })
    );
}

// ── Limits and plan errors ─────────────────────────────────────────────────

#[test]
fn limits_refuse_more_rows_than_the_maximum() {
    let limits = ExportLimits::new(NonZeroU64::new(100).expect("non-zero"));
    assert_eq!(limits.check(0), Ok(()));
    assert_eq!(limits.check(100), Ok(()));
    assert_eq!(
        limits.check(101),
        Err(ConflictKind::ExportTooLarge {
            rows: 101,
            limit: 100
        })
    );
    assert_eq!(
        ExportLimits::default().max_rows().get(),
        ExportLimits::DEFAULT_MAX_ROWS
    );
}

#[test]
fn plan_errors_map_like_the_reads_they_repeat() {
    let projection = projection_id();
    let cases = [
        (
            ExportPlanError::Store {
                reason: "reset".into(),
            },
            QueryError::Store {
                reason: "reset".into(),
            },
        ),
        (
            ExportPlanError::Version(VersionUnavailable::Unknown(V)),
            QueryError::NotFound,
        ),
        (
            ExportPlanError::Version(VersionUnavailable::NotRetained(V)),
            QueryError::VersionNotRetained { version: V },
        ),
        (
            ExportPlanError::Version(VersionUnavailable::NotActivated(V)),
            QueryError::Conflict(ConflictKind::TopicVersionNotActivated { version: V }),
        ),
        (
            ExportPlanError::TopicsNotInVersion {
                version: V,
                topics: vec![topic(5)],
            },
            QueryError::Conflict(ConflictKind::TopicsNotInVersion {
                version: V,
                topics: vec![topic(5)],
            }),
        ),
        (
            ExportPlanError::UnalignedWindow,
            QueryError::InvalidInput(InputError::UnalignedWindow),
        ),
        (
            ExportPlanError::Projection(ProjectionStoreError::Unknown(projection)),
            QueryError::NotFound,
        ),
        (
            ExportPlanError::Projection(ProjectionStoreError::NotRetained(projection)),
            QueryError::ProjectionNotRetained { projection },
        ),
    ];
    for (error, expected) in cases {
        assert_eq!(QueryError::from(error.clone()), expected, "{error:?}");
    }
}

// ── Rows read from stored values ───────────────────────────────────────────

#[test]
fn projection_rows_are_the_stored_frame_in_order() {
    let projection = projection();
    let rows = projection_rows(&projection, None);
    let expected: Vec<ExportRow> = points()
        .into_iter()
        .zip(0_u32..)
        .map(|(point, index)| {
            ExportRow::Point(PointRow {
                index,
                point,
                content: None,
            })
        })
        .collect();
    assert_eq!(rows, expected);
    assert_eq!(
        rows,
        projection_rows(&projection, None),
        "same on every read"
    );
}

#[test]
fn projection_rows_with_content_carry_topic_labels() {
    let labels = HashMap::from([
        (topic(5), "billing".to_owned()),
        (topic(6), "auth".to_owned()),
    ]);
    let rows = projection_rows(&projection(), Some(&labels));
    let found: Vec<Option<String>> = rows
        .iter()
        .map(|row| match row {
            ExportRow::Point(PointRow {
                content: Some(content),
                ..
            }) => content.topic_label.clone(),
            other => panic!("not a point with content: {other:?}"),
        })
        .collect();
    assert_eq!(
        found,
        vec![Some("billing".to_owned()), None, Some("auth".to_owned())]
    );
}

fn judged(state: TransmissionState) -> (Transmission, VerdictLog) {
    let transmission = Transmission {
        id: transmission(4),
        to: agent(2),
        route: Route::Unobserved,
        opened_at: at(3),
        state,
    };
    let mut log = VerdictLog::new(transmission.id);
    if transmission.state.judgeable().is_ok() {
        for (verdict, when) in [
            (Some(Verdict::FalseDetection), 10),
            (None, 11),
            (Some(Verdict::Genuine), 12),
        ] {
            let record = TransmissionVerdict::new(
                &transmission,
                verdict,
                operator(),
                at(when),
                Some(format!("note {when}")),
            )
            .expect("judgeable");
            log.record(record).expect("same transmission");
        }
    }
    (transmission, log)
}

#[test]
fn verdict_rows_follow_the_log_with_the_detector_call() {
    let (transmission, log) = judged(TransmissionState::Confirmed(confirmed()));
    let rows = verdict_rows(&transmission, &log, NoAliases).expect("same transmission");
    let revision = |n| VerdictRevision::new(std::num::NonZeroU32::new(n).expect("non-zero"));
    let expected: Vec<ExportRow> = [
        (Some(Verdict::FalseDetection), 10, 1),
        (None, 11, 2),
        (Some(Verdict::Genuine), 12, 3),
    ]
    .into_iter()
    .map(|(verdict, when, n)| {
        ExportRow::Verdict(VerdictRow {
            transmission: transmission.id,
            route_kind: RouteKind::Unobserved,
            call: QualityMatch::Content(MatchClass::Exact),
            revision: revision(n),
            verdict,
            by: operator(),
            at: at(when),
            note: Some(format!("note {when}")),
        })
    })
    .collect();
    assert_eq!(rows, expected);
}

#[test]
fn verdict_rows_exist_only_for_judgeable_states() {
    for (state, judgeable) in every_state() {
        let (transmission, log) = judged(state);
        let rows = verdict_rows(&transmission, &log, NoAliases).expect("same transmission");
        assert_eq!(rows.len(), if judgeable { 3 } else { 0 });
    }
}

#[test]
fn verdict_rows_refuse_another_transmissions_log() {
    let (transmission, _) = judged(TransmissionState::Confirmed(confirmed()));
    let other = VerdictLog::new(crate::tests::fixtures::transmission(5));
    assert_eq!(
        verdict_rows(&transmission, &other, NoAliases),
        Err(VerdictRowsError::OtherTransmission)
    );
}

#[test]
fn transmission_rows_are_the_listed_summary_of_a_confirmed_transmission() {
    for (state, _) in every_state() {
        let (transmission, _) = judged(state);
        let verdict = |_| Some(Verdict::Genuine);
        let topic = |_| TopicUnder::Outlier;
        let summary = TransmissionSummary::of(&transmission, NoAliases, verdict, topic);
        let row = TransmissionRow::of(&transmission, NoAliases, verdict, topic, None);
        match transmission.state.confirmed() {
            Some(confirmed) => {
                let row = row.expect("a confirmed transmission");
                assert_eq!(row.summary(), &summary);
                assert_eq!(row.strongest(), MatchClass::strongest(confirmed));
                assert_eq!(Some(row.delivery()), summary.state.delivery());
            }
            None => assert_eq!(
                row,
                Err(InvalidTransmissionRow::NotConfirmed(summary.state.kind()))
            ),
        }
    }
}

// ── Audit records ──────────────────────────────────────────────────────────

fn content_request() -> ExportRequest {
    request(ExportDataset::Transmissions(scope()), true)
}

#[test]
fn export_record_is_forbidden_exactly_without_the_permission() {
    let viewer = caller(1, &[Permission::View]);
    let reader = caller(1, &[Permission::View, Permission::Content]);
    let forbidden = || {
        ExportEvent::Refused(QueryError::Forbidden {
            missing: Permission::Content,
        })
    };
    assert!(ExportRecord::new(viewer.clone(), content_request(), forbidden()).is_ok());
    assert_eq!(
        ExportRecord::new(reader.clone(), content_request(), forbidden()),
        Err(InvalidExportRecord::ForbiddenButPermitted {
            required: Permission::Content
        })
    );
    assert_eq!(
        ExportRecord::new(
            viewer.clone(),
            content_request(),
            ExportEvent::Refused(QueryError::Forbidden {
                missing: Permission::View
            })
        ),
        Err(InvalidExportRecord::WrongMissingPermission {
            required: Permission::Content
        })
    );
    assert_eq!(
        ExportRecord::new(
            viewer,
            content_request(),
            ExportEvent::Refused(QueryError::NotFound)
        ),
        Err(InvalidExportRecord::AttemptedWithoutPermission {
            required: Permission::Content
        })
    );
    assert!(
        ExportRecord::new(
            reader,
            content_request(),
            ExportEvent::Refused(QueryError::NotFound)
        )
        .is_ok()
    );
}

#[test]
fn started_record_holds_the_header_of_its_request_and_caller() {
    let reader = caller(1, &[Permission::View, Permission::Content]);
    let header = header(ExportDataset::Transmissions(scope()), true, 2);
    assert!(
        ExportRecord::new(
            reader.clone(),
            content_request(),
            ExportEvent::Started(Box::new(header.clone()))
        )
        .is_ok()
    );
    let other_request = request(ExportDataset::Transmissions(scope()), false);
    assert_eq!(
        ExportRecord::new(
            reader,
            other_request,
            ExportEvent::Started(Box::new(header.clone()))
        ),
        Err(InvalidExportRecord::HeaderMismatch)
    );
    let other_caller = caller(2, &[Permission::View, Permission::Content]);
    assert_eq!(
        ExportRecord::new(
            other_caller,
            content_request(),
            ExportEvent::Started(Box::new(header))
        ),
        Err(InvalidExportRecord::HeaderMismatch)
    );
}

#[test]
fn export_entries_are_authored_by_the_caller_and_name_the_export() {
    let reader = caller(1, &[Permission::View, Permission::Content]);
    let started = header(ExportDataset::Transmissions(scope()), true, 2);
    let entry = |event| AuditEntry {
        id: AuditId::from_ulid(1),
        at: at(2_000),
        body: AuditBody::Export(
            ExportRecord::new(reader.clone(), content_request(), event).expect("valid record"),
        ),
    };
    let export = ExportId::from_ulid(9);
    let refused = entry(ExportEvent::Refused(QueryError::NotFound));
    assert_eq!(refused.by(), AuditAuthor::Operator(operator()));
    assert_eq!(refused.subjects(), Vec::new());
    assert_eq!(
        entry(ExportEvent::Started(Box::new(started))).subjects(),
        vec![AuditSubject::Export(export)]
    );
    assert_eq!(
        entry(ExportEvent::Abandoned { export, rows: 1 }).subjects(),
        vec![AuditSubject::Export(export)]
    );
}

#[test]
fn projection_export_entries_name_the_projection() {
    let reader = caller(1, &[Permission::Content]);
    let dataset = ExportDataset::Projection(projection_id());
    let header = ExportHeader::new(parts(
        request(dataset.clone(), false),
        projection_basis(3),
        3,
    ))
    .expect("valid header");
    let record = ExportRecord::new(reader.clone(), request(dataset.clone(), false), {
        ExportEvent::Started(Box::new(header))
    })
    .expect("valid record");
    assert_eq!(
        record.subjects(),
        vec![
            AuditSubject::Export(ExportId::from_ulid(9)),
            AuditSubject::Projection(projection_id()),
        ]
    );
    let refused = ExportRecord::new(
        reader,
        request(dataset, false),
        ExportEvent::Refused(QueryError::ProjectionNotRetained {
            projection: projection_id(),
        }),
    )
    .expect("valid record");
    assert_eq!(
        refused.subjects(),
        vec![AuditSubject::Projection(projection_id())]
    );
}
