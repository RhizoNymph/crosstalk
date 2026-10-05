//! The status of every error: golden tables over every query and action
//! error, every conflict and every input error included.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use super::super::errors::{
    every_action_error, every_conflict, every_input_error, every_query_error,
};
use super::super::harness::assert_golden;
use super::AREA;
use crate::interfaces::l8_surface::http::{ErrorStatus, Status};
use crate::interfaces::l8_surface::{
    ActionError, ConflictKind, InputError, Permission, QueryError, UnavailableKind,
};
use crate::wire::{DecodeError, DecodeErrorKind};

/// One error and the status it is answered with.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StatusRow<E> {
    status: u16,
    error: E,
}

/// Every query error a server answers: each variant but the client-only
/// `Unavailable`, with `Conflict` and `InvalidInput` once per conflict and
/// input error.
fn every_query_error_expanded() -> Vec<QueryError> {
    let mut errors: Vec<QueryError> = every_query_error()
        .into_iter()
        .filter(|error| !error.is_client_only())
        .filter(|error| !matches!(error, QueryError::Conflict(_) | QueryError::InvalidInput(_)))
        .collect();
    errors.extend(every_conflict().into_iter().map(QueryError::Conflict));
    errors.extend(
        every_input_error()
            .into_iter()
            .map(QueryError::InvalidInput),
    );
    errors
}

fn every_action_error_expanded() -> Vec<ActionError> {
    let mut errors: Vec<ActionError> = every_action_error()
        .into_iter()
        .filter(|error| !error.is_client_only())
        .filter(|error| {
            !matches!(
                error,
                ActionError::Conflict(_) | ActionError::InvalidInput(_)
            )
        })
        .collect();
    errors.extend(every_conflict().into_iter().map(ActionError::Conflict));
    errors.extend(
        every_input_error()
            .into_iter()
            .map(ActionError::InvalidInput),
    );
    errors
}

fn rows<E: ErrorStatus + Clone>(errors: Vec<E>) -> Vec<StatusRow<E>> {
    errors
        .into_iter()
        .map(|error| StatusRow {
            status: error.status().code(),
            error,
        })
        .collect()
}

/// The status of every error, pinned: a change of status is a golden diff.
#[test]
fn error_status_tables_golden() {
    assert_golden(
        AREA,
        "query_error_statuses",
        &rows(every_query_error_expanded()),
    );
    assert_golden(
        AREA,
        "action_error_statuses",
        &rows(every_action_error_expanded()),
    );
}

/// An action error is answered exactly as the query error it converts to,
/// so a client reads both with one table.
#[test]
fn an_action_error_has_its_query_error_status() {
    for error in every_action_error_expanded() {
        assert_eq!(
            error.status(),
            QueryError::from(error.clone()).status(),
            "{error:?}"
        );
    }
}

/// An undecodable request is a 400 whatever the decoder's kind, and no
/// other input error is.
#[test]
fn an_undecodable_request_is_a_bad_request() {
    for kind in [
        DecodeErrorKind::Syntax,
        DecodeErrorKind::Data,
        DecodeErrorKind::Eof,
    ] {
        let error = DecodeError {
            kind,
            reason: "expected value at line 1 column 1".into(),
        };
        assert_eq!(QueryError::from(error.clone()).status(), Status::BadRequest);
        assert_eq!(ActionError::from(error).status(), Status::BadRequest);
    }
    for input in every_input_error() {
        let expected = match input {
            InputError::MalformedRequest { .. } => Status::BadRequest,
            _ => Status::UnprocessableContent,
        };
        assert_eq!(
            QueryError::InvalidInput(input.clone()).status(),
            expected,
            "{input:?}"
        );
    }
}

/// The statuses the distinctions rest on: 403 is a caller without the
/// permission (401 is reserved for no caller), 409 a state conflict, 410
/// data retention dropped, 429 a full fit queue, 503 a failed store.
#[test]
fn statuses_tell_the_cases_apart() {
    let forbidden = QueryError::Forbidden {
        missing: Permission::Content,
    };
    assert_eq!(forbidden.status().code(), 403);
    assert_eq!(QueryError::NotFound.status().code(), 404);
    assert_eq!(QueryError::InvalidCursor.status().code(), 400);
    for conflict in every_conflict() {
        let expected = match conflict {
            ConflictKind::ProjectionQueueFull => 429,
            _ => 409,
        };
        assert_eq!(
            QueryError::Conflict(conflict.clone()).status().code(),
            expected,
            "{conflict:?}"
        );
    }
    for error in every_query_error() {
        if matches!(
            error,
            QueryError::VersionNotRetained { .. } | QueryError::ProjectionNotRetained { .. }
        ) {
            assert_eq!(error.status().code(), 410, "{error:?}");
        }
        if matches!(error, QueryError::Store { .. }) {
            assert_eq!(error.status().code(), 503, "{error:?}");
        }
        assert_ne!(error.status(), Status::Unauthorized, "{error:?}");
    }
}

/// Each status has its own code, and every error status is a 4xx or 5xx.
#[test]
fn status_codes_are_distinct() {
    let codes: HashSet<u16> = Status::ALL.iter().map(|status| status.code()).collect();
    assert_eq!(codes.len(), Status::ALL.len());
    for error in every_query_error_expanded() {
        assert!(error.status().code() >= 400, "{error:?}");
    }
}

/// The client-only `Unavailable` is never served: what a server answers
/// for every error (`served`) is not client-only, keeps the error's status,
/// and is the error itself unless it was `Unavailable`, which is `Store`
/// with the same reason. So the status tables, which list what a server
/// answers, hold no `Unavailable`.
#[test]
fn a_server_never_serves_a_client_only_error() {
    let mut queries = every_query_error_expanded();
    let mut actions = every_action_error_expanded();
    for kind in UnavailableKind::ALL {
        let reason = format!("{kind:?}");
        queries.push(QueryError::Unavailable {
            kind,
            reason: reason.clone(),
        });
        actions.push(ActionError::Unavailable {
            kind,
            reason: reason.clone(),
        });
        assert_eq!(
            QueryError::Unavailable {
                kind,
                reason: reason.clone()
            }
            .served(),
            QueryError::Store {
                reason: reason.clone()
            }
        );
        assert_eq!(
            ActionError::Unavailable {
                kind,
                reason: reason.clone()
            }
            .served(),
            ActionError::Store { reason }
        );
    }
    for error in queries {
        let served = error.clone().served();
        assert!(!served.is_client_only(), "{error:?}");
        assert_eq!(served.status(), error.status(), "{error:?}");
        assert_eq!(
            served.status() == Status::ServiceUnavailable,
            matches!(served, QueryError::Store { .. }),
            "{error:?}"
        );
        if !error.is_client_only() {
            assert_eq!(served, error);
        }
    }
    for error in actions {
        let served = error.clone().served();
        assert!(!served.is_client_only(), "{error:?}");
        assert_eq!(served.status(), error.status(), "{error:?}");
        if !error.is_client_only() {
            assert_eq!(served, error);
        }
    }
    for row in rows(every_query_error_expanded()) {
        assert!(!row.error.is_client_only(), "{row:?}");
    }
    for row in rows(every_action_error_expanded()) {
        assert!(!row.error.is_client_only(), "{row:?}");
    }
}
