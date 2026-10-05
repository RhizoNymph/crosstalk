//! HTTP statuses for errors on data routes.

use crosstalk_spec::interfaces::l8_surface::{ConflictKind, QueryError};
use topcoat::router::error::{
    bad_request, forbidden, internal_server_error, not_found, service_unavailable,
};

use crate::error::UiError;

/// The status an error is answered with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorStatus {
    BadRequest,
    Forbidden,
    NotFound,
    /// The http backend's call never reached the gateway
    /// (`Unavailable`): 503, worth retrying.
    Unavailable,
    Internal,
}

/// The `Retry-After` of a 503: the gateway may be back by then.
const RETRY_AFTER_SECS: u64 = 5;

pub fn status_of(error: &UiError) -> ErrorStatus {
    match error {
        UiError::Field { .. } => ErrorStatus::BadRequest,
        UiError::Query(query) => match query {
            QueryError::Forbidden { .. } => ErrorStatus::Forbidden,
            QueryError::NotFound | QueryError::ProjectionNotRetained { .. } => {
                ErrorStatus::NotFound
            }
            // A linked view's version conflicts come from the URL (`v`,
            // `t`), and so does a projection not ready or failed (`p`): the
            // element shows why, like an invalid input.
            QueryError::VersionNotRetained { .. }
            | QueryError::InvalidInput(_)
            | QueryError::InvalidCursor
            | QueryError::Conflict(
                ConflictKind::TopicVersionFitting { .. }
                | ConflictKind::TopicVersionNotActivated { .. }
                | ConflictKind::TopicsNotInVersion { .. }
                | ConflictKind::ProjectionNotReady { .. }
                | ConflictKind::ProjectionFailed { .. },
            ) => ErrorStatus::BadRequest,
            QueryError::Unavailable { .. } => ErrorStatus::Unavailable,
            QueryError::Store { .. } | QueryError::Conflict(_) => ErrorStatus::Internal,
        },
    }
}

/// Converts an error into the router error for its status. A 400 carries
/// the error's message, so the element can show why; a 503 or 500 is
/// logged and carries nothing.
pub fn query_error(error: impl Into<UiError>) -> topcoat::Error {
    let error = error.into();
    match status_of(&error) {
        ErrorStatus::BadRequest => bad_request(error.to_string()).into(),
        ErrorStatus::Forbidden => forbidden().into(),
        ErrorStatus::NotFound => not_found().into(),
        ErrorStatus::Unavailable => {
            tracing::error!(error = %error, "data route: the gateway did not answer");
            service_unavailable(RETRY_AFTER_SECS).into()
        }
        ErrorStatus::Internal => {
            tracing::error!(error = %error, "data route backend failure");
            internal_server_error(error).into()
        }
    }
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::aggregates::topic::TopicModelVersion;
    use crosstalk_spec::ids::AgentId;
    use crosstalk_spec::ids::TopicId;
    use crosstalk_spec::interfaces::l8_surface::{InputError, Permission};

    use super::*;

    #[test]
    fn maps_every_error_kind() {
        let cases = [
            (
                UiError::Query(QueryError::Forbidden {
                    missing: Permission::Content,
                }),
                ErrorStatus::Forbidden,
            ),
            (UiError::Query(QueryError::NotFound), ErrorStatus::NotFound),
            (
                UiError::Query(QueryError::VersionNotRetained {
                    version: TopicModelVersion(2),
                }),
                ErrorStatus::BadRequest,
            ),
            (
                UiError::Query(QueryError::InvalidInput(InputError::UnalignedWindow)),
                ErrorStatus::BadRequest,
            ),
            (UiError::field("x", "bad"), ErrorStatus::BadRequest),
            (
                UiError::Query(QueryError::InvalidCursor),
                ErrorStatus::BadRequest,
            ),
            (
                UiError::Query(QueryError::Store {
                    reason: "down".to_owned(),
                }),
                ErrorStatus::Internal,
            ),
            (
                UiError::Query(QueryError::Unavailable {
                    kind: crosstalk_spec::interfaces::l8_surface::UnavailableKind::Timeout,
                    reason: "no response within 50 ms".to_owned(),
                }),
                ErrorStatus::Unavailable,
            ),
            (
                UiError::Query(QueryError::Conflict(ConflictKind::AgentMerged {
                    agent: AgentId::from_ulid(1),
                    into: AgentId::from_ulid(2),
                })),
                ErrorStatus::Internal,
            ),
        ];
        for (error, status) in cases {
            assert_eq!(status_of(&error), status, "{error:?}");
        }
    }

    #[test]
    fn unusable_projections_are_bad_requests() {
        use crosstalk_spec::aggregates::projection::{FitFailure, ProjectionStatusKind};
        use crosstalk_spec::ids::ProjectionId;
        let projection = ProjectionId::from_ulid(9);
        for conflict in [
            ConflictKind::ProjectionNotReady {
                projection,
                status: ProjectionStatusKind::Queued,
            },
            ConflictKind::ProjectionFailed {
                projection,
                failure: FitFailure::NonFiniteLayout,
            },
        ] {
            let error = UiError::Query(QueryError::Conflict(conflict));
            assert_eq!(status_of(&error), ErrorStatus::BadRequest, "{error:?}");
        }
        assert_eq!(
            status_of(&UiError::Query(QueryError::ProjectionNotRetained {
                projection
            })),
            ErrorStatus::NotFound
        );
    }

    #[test]
    fn linked_view_version_conflicts_are_bad_requests() {
        let version = TopicModelVersion(1);
        for conflict in [
            ConflictKind::TopicVersionFitting { version },
            ConflictKind::TopicVersionNotActivated { version },
            ConflictKind::TopicsNotInVersion {
                version,
                topics: vec![TopicId::from_ulid(3)],
            },
        ] {
            let error = UiError::Query(QueryError::Conflict(conflict));
            assert_eq!(status_of(&error), ErrorStatus::BadRequest, "{error:?}");
        }
    }
}
