//! HTTP statuses for errors on data routes.

use crosstalk_spec::interfaces::l8_surface::{ConflictKind, QueryError};
use topcoat::router::error::{bad_request, forbidden, internal_server_error, not_found};

use crate::error::UiError;

/// The status an error is answered with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorStatus {
    BadRequest,
    Forbidden,
    NotFound,
    Internal,
}

pub fn status_of(error: &UiError) -> ErrorStatus {
    match error {
        UiError::Field { .. } => ErrorStatus::BadRequest,
        UiError::Query(query) => match query {
            QueryError::Forbidden { .. } => ErrorStatus::Forbidden,
            QueryError::NotFound | QueryError::ProjectionNotRetained { .. } => {
                ErrorStatus::NotFound
            }
            // A linked view's version conflicts come from the URL (`v`,
            // `t`): the element shows why, like an invalid input.
            QueryError::VersionNotRetained { .. }
            | QueryError::InvalidInput(_)
            | QueryError::InvalidCursor
            | QueryError::Conflict(
                ConflictKind::TopicVersionFitting { .. }
                | ConflictKind::TopicVersionNotActivated { .. }
                | ConflictKind::TopicsNotInVersion { .. },
            ) => ErrorStatus::BadRequest,
            QueryError::Store { .. } | QueryError::Conflict(_) => ErrorStatus::Internal,
        },
    }
}

/// Converts an error into the router error for its status. A 400 carries
/// the error's message, so the element can show why; a 500 is logged and
/// carries nothing.
pub fn query_error(error: impl Into<UiError>) -> topcoat::Error {
    let error = error.into();
    match status_of(&error) {
        ErrorStatus::BadRequest => bad_request(error.to_string()).into(),
        ErrorStatus::Forbidden => forbidden().into(),
        ErrorStatus::NotFound => not_found().into(),
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
