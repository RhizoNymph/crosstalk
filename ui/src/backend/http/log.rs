//! Structured logs of the http backend's failed calls.
//!
//! The client returns a call that never reached the gateway (the network,
//! a timeout, a cut body, a `401`) as the client-only `Unavailable { kind,
//! reason }`, and a response the binding does not describe as
//! `Store { reason }` (`crosstalk_client`'s error mapping), so the page
//! shows the existing "the gateway's store failed" state and the log says
//! why:
//!
//! | Error | Level | Meaning |
//! | --- | --- | --- |
//! | `Unavailable` | error | unreachable, timed out, cut off, or `401` (bad token): `kind` says which |
//! | `Store` | error | the gateway's store failed, or answered off the binding |
//! | `Forbidden` | warn | `403`: the token's operator lacks a permission |
//! | anything else | debug | an answer about the request (not found, invalid input, a conflict) |

use std::fmt::Debug;

use crosstalk_spec::interfaces::l8_surface::live::LiveEnd;
use crosstalk_spec::interfaces::l8_surface::{ActionError, QueryError};

/// How a failed call reads in the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    /// The call did not get an answer from the surface.
    Unavailable,
    /// The surface refused the caller.
    Refused,
    /// The surface answered about the request.
    Answered,
}

/// An error a call over HTTP returns.
pub trait CallError: Debug {
    fn severity(&self) -> Severity;
}

impl CallError for QueryError {
    fn severity(&self) -> Severity {
        match self {
            Self::Store { .. } | Self::Unavailable { .. } => Severity::Unavailable,
            Self::Forbidden { .. } => Severity::Refused,
            _ => Severity::Answered,
        }
    }
}

impl CallError for ActionError {
    fn severity(&self) -> Severity {
        match self {
            Self::Store { .. } | Self::Unavailable { .. } => Severity::Unavailable,
            Self::Forbidden { .. } => Severity::Refused,
            _ => Severity::Answered,
        }
    }
}

/// `result`, logged if it failed.
pub fn outcome<T, E: CallError>(method: &'static str, result: Result<T, E>) -> Result<T, E> {
    if let Err(error) = &result {
        match error.severity() {
            Severity::Unavailable => {
                tracing::error!(backend = "http", method, error = ?error, "gateway call failed");
            }
            Severity::Refused => {
                tracing::warn!(backend = "http", method, error = ?error, "gateway refused the call");
            }
            Severity::Answered => {
                tracing::debug!(backend = "http", method, error = ?error, "gateway answered an error");
            }
        }
    }
    result
}

/// Logs why a live stream over HTTP ended: the client has already
/// reconnected as far as its policy allows.
pub fn live_ended(end: LiveEnd) {
    match end {
        LiveEnd::SessionEnded => tracing::warn!(
            backend = "http",
            end = ?end,
            "live feed ended: the gateway refused the token"
        ),
        LiveEnd::ShuttingDown => tracing::warn!(
            backend = "http",
            end = ?end,
            "live feed ended: the gateway shut down"
        ),
        LiveEnd::Unreachable => tracing::warn!(
            backend = "http",
            end = ?end,
            "live feed ended: the gateway could not be reached again"
        ),
        LiveEnd::Lagged => tracing::warn!(
            backend = "http",
            end = ?end,
            "live feed ended: the stream fell behind"
        ),
    }
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::interfaces::l8_surface::{Permission, UnavailableKind};

    use super::*;

    #[test]
    fn store_failures_are_unavailable_and_forbidden_is_refused() {
        let store = QueryError::Store {
            reason: "no caller".to_owned(),
        };
        assert_eq!(store.severity(), Severity::Unavailable);
        let forbidden = ActionError::Forbidden {
            missing: Permission::Triage,
        };
        assert_eq!(forbidden.severity(), Severity::Refused);
        assert_eq!(QueryError::NotFound.severity(), Severity::Answered);
        assert!(outcome("alert", Err::<(), _>(QueryError::NotFound)).is_err());
    }

    #[test]
    fn every_unavailable_kind_is_unavailable() {
        for kind in UnavailableKind::ALL {
            let query = QueryError::Unavailable {
                kind,
                reason: "sending the request: refused".to_owned(),
            };
            assert_eq!(query.severity(), Severity::Unavailable, "{kind:?}");
            let action = ActionError::Unavailable {
                kind,
                reason: "no caller: expired".to_owned(),
            };
            assert_eq!(action.severity(), Severity::Unavailable, "{kind:?}");
        }
    }
}
