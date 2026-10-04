//! HTTP statuses for backend errors on data routes.

use topcoat::router::error::{bad_request, forbidden, internal_server_error, not_found};

use crate::contract::errors::QueryError;

/// The status a [`QueryError`] is answered with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorStatus {
    BadRequest,
    Forbidden,
    NotFound,
    Internal,
}

pub fn status_of(error: &QueryError) -> ErrorStatus {
    match error {
        QueryError::Forbidden { .. } => ErrorStatus::Forbidden,
        QueryError::NotFound => ErrorStatus::NotFound,
        QueryError::VersionNotRetained { .. } | QueryError::InvalidInput(_) => {
            ErrorStatus::BadRequest
        }
        QueryError::Store { .. } | QueryError::Conflict(_) => ErrorStatus::Internal,
    }
}

/// Converts a backend error into the router error for its status. A 400
/// carries the error's message, so the element can show why; a 500 is
/// logged and carries nothing.
pub fn query_error(error: QueryError) -> topcoat::Error {
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
    use crosstalk_spec::interfaces::l8_surface::Permission;

    use super::*;
    use crate::contract::errors::{ConflictKind, InputError};

    #[test]
    fn maps_every_error_kind() {
        let cases = [
            (
                QueryError::Forbidden {
                    missing: Permission::Content,
                },
                ErrorStatus::Forbidden,
            ),
            (QueryError::NotFound, ErrorStatus::NotFound),
            (
                QueryError::VersionNotRetained {
                    version: TopicModelVersion(2),
                },
                ErrorStatus::BadRequest,
            ),
            (
                QueryError::InvalidInput(InputError::Field {
                    field: "x",
                    reason: "bad".to_owned(),
                }),
                ErrorStatus::BadRequest,
            ),
            (
                QueryError::Store {
                    reason: "down".to_owned(),
                },
                ErrorStatus::Internal,
            ),
            (
                QueryError::Conflict(ConflictKind::AgentMerged),
                ErrorStatus::Internal,
            ),
        ];
        for (error, status) in cases {
            assert_eq!(status_of(&error), status, "{error:?}");
        }
    }
}
