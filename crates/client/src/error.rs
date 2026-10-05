//! What can go wrong in a call, and how it becomes the surface's own error
//! type for the trait methods.
//!
//! **The status mapping, in reverse.** The surface answers every error
//! with `ErrorStatus::status` of the error and the error's wire JSON
//! (`http_api.md`, Status mapping). The client decodes the body as the
//! route's error type and accepts it only when that error's status is the
//! status received ([`decode_error`]): a body whose error belongs to
//! another status is [`ClientError::StatusMismatch`], not trusted. A `401`
//! is the binding's [`AuthError`]. Anything without an L8 body (a proxy's
//! `502`, an HTML page) is [`ClientError::UnexpectedResponse`].
//!
//! **Into the trait's error.** `QueryApi` and `OperatorActions` return the
//! surface's `QueryError` and `ActionError`, which have no variant for "no
//! caller" or "the network failed", because the in-process surface has
//! neither. The trait methods therefore return the decoded error as is,
//! a request the client could not encode as `InvalidInput(MalformedRequest)`
//! (what the server would answer it with), and everything else as
//! `Store { reason }`: the call did not reach a store that answered, and
//! retrying may succeed. The reason names the cause, and a page that needs
//! to tell "sign in again" apart reads [`ClientError`] from the inherent
//! methods instead.

use std::fmt::Debug;

use crosstalk_spec::interfaces::l8_surface::http::{AuthError, EncodeError, ErrorStatus, Route};
use crosstalk_spec::interfaces::l8_surface::{ActionError, InputError, QueryError};
use crosstalk_spec::wire::DecodeErrorKind;
use serde::de::DeserializeOwned;

/// An error type a route answers with: decoded from the body, with the
/// status the binding gives it.
pub trait ApiError: DeserializeOwned + ErrorStatus + Debug + Send + 'static {}

impl ApiError for QueryError {}

impl ApiError for ActionError {}

/// Why the HTTP exchange itself failed: nothing the surface decided.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// The request could not be built (an invalid header value).
    #[error("building the request: {0}")]
    Build(#[source] hyper::http::Error),
    /// Connecting, sending, or reading the response head failed.
    #[error("sending the request: {0}")]
    Send(#[source] hyper_util::client::legacy::Error),
    /// Reading the body failed: the connection was cut or reset.
    #[error("reading the body: {0}")]
    Body(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// No response, or no bytes of a stream, within the configured time.
    #[error("no response within {millis} ms")]
    Timeout { millis: u128 },
    /// A body or one line longer than the configured limit.
    #[error("a response over {limit} bytes")]
    TooLarge { limit: usize },
}

/// Why a call failed, for an error type `E` the route answers with.
#[derive(Debug, thiserror::Error)]
pub enum ClientError<E: Debug> {
    /// The surface refused the call: the error it answered with, at the
    /// status the binding gives that error.
    #[error("the surface answered {0:?}")]
    Api(E),
    /// `401`: the request had no caller.
    #[error("no caller: {0:?}")]
    Unauthenticated(AuthError),
    /// The call does not follow the route table.
    #[error("the call does not follow the route table: {0:?}")]
    Encode(EncodeError),
    #[error(transparent)]
    Transport(#[from] TransportError),
    /// An error body whose error the binding answers with another status.
    #[error(
        "{route:?} answered {status} with {error:?}, which the binding answers with {expected}"
    )]
    StatusMismatch {
        route: Route,
        status: u16,
        expected: u16,
        error: E,
    },
    /// A response the binding does not describe: a status with no L8
    /// body, a success of the wrong content type, or a body that does not
    /// decode as the route's result.
    #[error("{route:?} answered {status}: {reason}")]
    UnexpectedResponse {
        route: Route,
        status: u16,
        reason: String,
    },
}

impl<E: Debug> ClientError<E> {
    pub(crate) fn unexpected(route: Route, status: u16, reason: impl Into<String>) -> Self {
        Self::UnexpectedResponse {
            route,
            status,
            reason: reason.into(),
        }
    }
}

/// The error a non-success response carries: a 401's `AuthError`, else the
/// route's error type, accepted only at its own status.
pub(crate) fn decode_error<E: ApiError>(route: Route, status: u16, body: &[u8]) -> ClientError<E> {
    if status == 401 {
        return match serde_json::from_slice::<AuthError>(body) {
            Ok(error) => ClientError::Unauthenticated(error),
            Err(error) => {
                ClientError::unexpected(route, status, format!("not an AuthError: {error}"))
            }
        };
    }
    match serde_json::from_slice::<E>(body) {
        Ok(error) => {
            let expected = error.status().code();
            if expected == status {
                ClientError::Api(error)
            } else {
                ClientError::StatusMismatch {
                    route,
                    status,
                    expected,
                    error,
                }
            }
        }
        Err(error) => ClientError::unexpected(route, status, format!("no error body: {error}")),
    }
}

fn malformed(error: &EncodeError) -> InputError {
    InputError::MalformedRequest {
        kind: DecodeErrorKind::Data,
        reason: format!("the client could not encode the request: {error:?}"),
    }
}

impl From<ClientError<QueryError>> for QueryError {
    fn from(error: ClientError<QueryError>) -> Self {
        match error {
            ClientError::Api(error) => error,
            ClientError::Encode(encode) => Self::InvalidInput(malformed(&encode)),
            other => Self::Store {
                reason: other.to_string(),
            },
        }
    }
}

impl From<ClientError<ActionError>> for ActionError {
    fn from(error: ClientError<ActionError>) -> Self {
        match error {
            ClientError::Api(error) => error,
            ClientError::Encode(encode) => Self::InvalidInput(malformed(&encode)),
            other => Self::Store {
                reason: other.to_string(),
            },
        }
    }
}
