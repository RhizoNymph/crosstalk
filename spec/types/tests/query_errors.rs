//! The mapping from store errors to `QueryError`.

use std::num::{NonZeroU16, NonZeroU64};

use crate::aggregates::filter::VersionUnavailable;
use crate::aggregates::projection::{FitFailure, ProjectionStatusKind};
use crate::aggregates::series::BucketWidth;
use crate::aggregates::topic::{EmbeddingModel, TopicModelVersion};
use crate::ids::{EventId, ProjectionId, TopicId};
use crate::interfaces::l2_transport::{BusError, ConsumerGroup};
use crate::interfaces::l6_analysis::{CatalogError, EmbedError, ProjectionStoreError, SearchError};
use crate::interfaces::l7_topology::EdgeQueryError;
use crate::interfaces::l8_surface::{ConflictKind, InputError, QueryError};

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
