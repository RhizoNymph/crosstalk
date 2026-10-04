//! The mapping from store errors to `QueryError`.

use std::num::{NonZeroU16, NonZeroU64};

use crate::aggregates::filter::VersionUnavailable;
use crate::aggregates::projection::{FitFailure, ProjectionStatusKind};
use crate::aggregates::series::BucketWidth;
use crate::aggregates::topic::{EmbeddingModel, TopicModelVersion};
use crate::ids::{AuditId, EventId, ProjectionId, TopicId, TransmissionId};
use crate::interfaces::l2_transport::{BlobError, BusError, ConsumerGroup};
use crate::interfaces::l5_flow::verdicts::VerdictError;
use crate::interfaces::l6_analysis::{CatalogError, EmbedError, ProjectionStoreError, SearchError};
use crate::interfaces::l7_topology::EdgeQueryError;
use crate::interfaces::l8_surface::audit::AuditError;
use crate::interfaces::l8_surface::evidence::{EvidenceError, EvidenceRecord, InvalidEvidence};
use crate::interfaces::l8_surface::excerpt::{CutError, ExcerptError};
use crate::interfaces::l8_surface::{ConflictKind, InputError, QueryError};
use crate::observed::message::text::NoPartText;
use crate::tests::fixtures::{access, message, resource, span};

const V: TopicModelVersion = TopicModelVersion(4);

fn store() -> String {
    "connection reset".to_owned()
}

fn projection() -> ProjectionId {
    ProjectionId::from_ulid(8)
}

fn model(name: &str) -> EmbeddingModel {
    EmbeddingModel {
        name: name.into(),
        dimension: NonZeroU16::new(8).expect("non-zero"),
    }
}

fn width(micros: u64) -> BucketWidth {
    BucketWidth::from_micros(NonZeroU64::new(micros).expect("non-zero"))
}

#[test]
fn unavailable_versions_map_by_cause() {
    let cases = [
        (VersionUnavailable::Unknown(V), QueryError::NotFound),
        (
            VersionUnavailable::Fitting(V),
            QueryError::Conflict(ConflictKind::TopicVersionFitting { version: V }),
        ),
        (
            VersionUnavailable::NotActivated(V),
            QueryError::Conflict(ConflictKind::TopicVersionNotActivated { version: V }),
        ),
        (
            VersionUnavailable::NotRetained(V),
            QueryError::VersionNotRetained { version: V },
        ),
    ];
    for (error, expected) in cases {
        assert_eq!(QueryError::from(error), expected, "{error:?}");
    }
}

#[test]
fn edge_query_errors_map_to_typed_query_errors() {
    let topics = vec![TopicId::from_ulid(1)];
    let cases = [
        (
            EdgeQueryError::Store { reason: store() },
            QueryError::Store { reason: store() },
        ),
        (
            EdgeQueryError::UnalignedWindow,
            QueryError::InvalidInput(InputError::UnalignedWindow),
        ),
        (
            EdgeQueryError::BucketWidthMismatch {
                store: width(60),
                grid: width(30),
            },
            QueryError::InvalidInput(InputError::BucketWidthMismatch),
        ),
        (
            EdgeQueryError::Version(VersionUnavailable::NotRetained(V)),
            QueryError::VersionNotRetained { version: V },
        ),
        (
            EdgeQueryError::TopicsNotInVersion {
                version: V,
                topics: topics.clone(),
            },
            QueryError::Conflict(ConflictKind::TopicsNotInVersion { version: V, topics }),
        ),
        (EdgeQueryError::InvalidCursor, QueryError::InvalidCursor),
    ];
    for (error, expected) in cases {
        assert_eq!(QueryError::from(error.clone()), expected, "{error:?}");
    }
}

#[test]
fn search_errors_map_to_typed_query_errors() {
    let topics = vec![TopicId::from_ulid(2)];
    let cases = [
        (
            SearchError::Store { reason: store() },
            QueryError::Store { reason: store() },
        ),
        (
            SearchError::WrongModel {
                index: model("new"),
                query: model("old"),
            },
            QueryError::Conflict(ConflictKind::EmbeddingModelChanged),
        ),
        (
            SearchError::Version(VersionUnavailable::Fitting(V)),
            QueryError::Conflict(ConflictKind::TopicVersionFitting { version: V }),
        ),
        (
            SearchError::TopicsNotInVersion {
                version: V,
                topics: topics.clone(),
            },
            QueryError::Conflict(ConflictKind::TopicsNotInVersion { version: V, topics }),
        ),
        (SearchError::InvalidCursor, QueryError::InvalidCursor),
    ];
    for (error, expected) in cases {
        assert_eq!(QueryError::from(error.clone()), expected, "{error:?}");
    }
}

#[test]
fn embedding_a_search_maps_too_long_to_input() {
    assert_eq!(
        QueryError::from(EmbedError::TooLong { index: 0 }),
        QueryError::InvalidInput(InputError::QueryTooLong)
    );
    assert_eq!(
        QueryError::from(EmbedError::Model { reason: store() }),
        QueryError::Store { reason: store() }
    );
}

#[test]
fn catalog_errors_map_to_typed_query_errors() {
    let cases = [
        (
            CatalogError::Store { reason: store() },
            QueryError::Store { reason: store() },
        ),
        (CatalogError::UnknownVersion(V), QueryError::NotFound),
        (
            CatalogError::StillFitting(V),
            QueryError::Conflict(ConflictKind::TopicVersionFitting { version: V }),
        ),
        (CatalogError::InvalidCursor, QueryError::InvalidCursor),
    ];
    for (error, expected) in cases {
        assert_eq!(QueryError::from(error.clone()), expected, "{error:?}");
    }
}

#[test]
fn projection_store_errors_map_to_typed_query_errors() {
    let failure = FitFailure::TooFewPoints { needed: 16, got: 2 };
    let cases = [
        (
            ProjectionStoreError::Store { reason: store() },
            QueryError::Store { reason: store() },
        ),
        (
            ProjectionStoreError::Unknown(projection()),
            QueryError::NotFound,
        ),
        (
            ProjectionStoreError::NotReady {
                projection: projection(),
                status: ProjectionStatusKind::Fitting,
            },
            QueryError::Conflict(ConflictKind::ProjectionNotReady {
                projection: projection(),
                status: ProjectionStatusKind::Fitting,
            }),
        ),
        (
            ProjectionStoreError::Failed {
                projection: projection(),
                failure: failure.clone(),
            },
            QueryError::Conflict(ConflictKind::ProjectionFailed {
                projection: projection(),
                failure,
            }),
        ),
        (
            ProjectionStoreError::NotRetained(projection()),
            QueryError::ProjectionNotRetained {
                projection: projection(),
            },
        ),
        (
            ProjectionStoreError::QueueFull,
            QueryError::Conflict(ConflictKind::ProjectionQueueFull),
        ),
        (
            ProjectionStoreError::InvalidCursor,
            QueryError::InvalidCursor,
        ),
    ];
    for (error, expected) in cases {
        assert_eq!(QueryError::from(error.clone()), expected, "{error:?}");
    }
}

#[test]
fn dead_letter_bus_errors_keep_cursor_and_not_found_apart() {
    assert_eq!(
        QueryError::from(BusError::InvalidCursor),
        QueryError::InvalidCursor
    );
    assert_eq!(
        QueryError::from(BusError::UnknownDeadLetter {
            group: ConsumerGroup("analyze".into()),
            id: EventId::from_ulid(1),
        }),
        QueryError::NotFound
    );
    assert!(matches!(
        QueryError::from(BusError::Disconnected),
        QueryError::Store { .. }
    ));
}

#[test]
fn catalog_drops_map_to_version_not_retained() {
    assert_eq!(
        QueryError::from(CatalogError::VersionNotRetained(V)),
        QueryError::VersionNotRetained { version: V }
    );
}

#[test]
fn verdict_store_errors_map_to_typed_query_errors() {
    let transmission = TransmissionId::from_ulid(3);
    let cases = [
        (
            VerdictError::Store { reason: store() },
            QueryError::Store { reason: store() },
        ),
        (
            VerdictError::UnknownTransmission(transmission),
            QueryError::NotFound,
        ),
        (
            VerdictError::NotJudgeable(transmission),
            QueryError::Conflict(ConflictKind::TransmissionNotJudgeable { transmission }),
        ),
    ];
    for (error, expected) in cases {
        assert_eq!(QueryError::from(error.clone()), expected, "{error:?}");
    }
}

#[test]
fn audit_log_errors_map_to_typed_query_errors() {
    assert_eq!(
        QueryError::from(AuditError::Store { reason: store() }),
        QueryError::Store { reason: store() }
    );
    assert_eq!(
        QueryError::from(AuditError::InvalidCursor),
        QueryError::InvalidCursor
    );
    assert!(matches!(
        QueryError::from(AuditError::IdReused(AuditId::from_ulid(1))),
        QueryError::Store { .. }
    ));
}

#[test]
fn blob_errors_are_store_failures() {
    for error in [
        BlobError::Unavailable { reason: store() },
        BlobError::Corrupt(message(1)),
    ] {
        assert!(
            matches!(QueryError::from(error.clone()), QueryError::Store { .. }),
            "{error:?}"
        );
    }
}

/// One of every evidence error, behind exhaustive matches so a new variant
/// does not compile until it is listed.
fn every_evidence_error() -> Vec<EvidenceError> {
    let errors = vec![
        EvidenceError::Store { reason: store() },
        EvidenceError::Blob(BlobError::Unavailable { reason: store() }),
        EvidenceError::Blob(BlobError::Corrupt(message(1))),
        EvidenceError::Missing(EvidenceRecord::Span(span(1))),
        EvidenceError::Missing(EvidenceRecord::Access(access(1))),
        EvidenceError::Missing(EvidenceRecord::Resource(resource(1))),
        EvidenceError::Excerpt(ExcerptError::WrongMessage {
            expected: message(1),
            got: message(2),
        }),
        EvidenceError::Excerpt(ExcerptError::Part(NoPartText::NoSuchPart {
            index: 3,
            parts: 1,
        })),
        EvidenceError::Excerpt(ExcerptError::Part(NoPartText::NotText { index: 0 })),
        EvidenceError::Excerpt(ExcerptError::Cut(CutError::OutsideText { end: 9, len: 4 })),
        EvidenceError::Excerpt(ExcerptError::Cut(CutError::NotCharBoundary { at: 1 })),
        EvidenceError::Invalid(InvalidEvidence::ResourceMismatch {
            access: access(1),
            expected: resource(1),
            got: resource(2),
        }),
        EvidenceError::Invalid(InvalidEvidence::WrongAccess {
            asked: access(1),
            got: access(2),
        }),
    ];
    for error in &errors {
        match error {
            EvidenceError::Store { .. }
            | EvidenceError::Blob(BlobError::Unavailable { .. } | BlobError::Corrupt(_))
            | EvidenceError::Missing(
                EvidenceRecord::Span(_) | EvidenceRecord::Access(_) | EvidenceRecord::Resource(_),
            )
            | EvidenceError::Excerpt(
                ExcerptError::WrongMessage { .. }
                | ExcerptError::Part(NoPartText::NoSuchPart { .. } | NoPartText::NotText { .. })
                | ExcerptError::Cut(CutError::OutsideText { .. } | CutError::NotCharBoundary { .. }),
            )
            | EvidenceError::Invalid(
                InvalidEvidence::ResourceMismatch { .. } | InvalidEvidence::WrongAccess { .. },
            ) => {}
        }
    }
    errors
}

#[test]
fn evidence_errors_are_store_failures_naming_their_cause() {
    let mut reasons: Vec<String> = Vec::new();
    for error in every_evidence_error() {
        let QueryError::Store { reason } = QueryError::from(error.clone()) else {
            panic!("{error:?} is not a store failure");
        };
        assert!(!reasons.contains(&reason), "{error:?} shares its reason");
        reasons.push(reason);
    }
}

#[test]
fn evidence_blob_errors_map_as_blob_errors_do() {
    let blob = BlobError::Corrupt(message(4));
    assert_eq!(
        QueryError::from(EvidenceError::Blob(blob.clone())),
        QueryError::from(blob)
    );
    assert_eq!(
        QueryError::from(EvidenceError::Store { reason: store() }),
        QueryError::Store { reason: store() }
    );
}
