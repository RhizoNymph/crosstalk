//! The status each error is answered with.
//!
//! The body of an error response is always the error's wire JSON
//! ([`QueryError`], [`ActionError`], or [`AuthError`] for
//! a 401), so the status is for intermediaries, logs and clients that
//! branch before decoding; the JSON says exactly what happened.
//!
//! | Error | Status |
//! | --- | --- |
//! | `InvalidInput(MalformedRequest)`, `InvalidCursor` | 400 Bad Request: the request could not be read, or names a page the surface did not issue |
//! | no credential, an invalid one, or one for an operator config does not define | 401 Unauthorized, with `WWW-Authenticate` ([`AuthError`]) |
//! | `Forbidden` | 403 Forbidden: authenticated, without the permission |
//! | `NotFound`, and a path no route serves | 404 Not Found |
//! | `Conflict(_)`, except `ProjectionQueueFull` | 409 Conflict: well-formed, but the state does not allow it now |
//! | `VersionNotRetained`, `ProjectionNotRetained` | 410 Gone: the data existed and retention dropped it for good |
//! | `InvalidInput(_)` other than `MalformedRequest` | 422 Unprocessable Content: read, but invalid whatever the state |
//! | `Conflict(ProjectionQueueFull)` | 429 Too Many Requests: the fit queue is full; retry later |
//! | `Store` | 503 Service Unavailable: a store or the bus failed; retrying may succeed |
//! | `Unavailable` (client-only) | never answered: a server answers `served()` of the error, `Store` with the same reason, at 503 |
//!
//! `Unavailable` has a status, 503, so a client that relays the error
//! (the UI, to its browser) can still branch on it, and so serving it as
//! `Store` keeps its status. It is not in the server's status table
//! (`http/query_error_statuses.json`): a server never answers it, and the
//! client refuses an `unavailable` body as a response the binding does not
//! describe.
//!
//! Every match below is exhaustive with no wildcard, over the error enums
//! and over every [`ConflictKind`] and [`InputError`], so a new variant
//! does not compile until its status is decided. An action error is
//! answered exactly as the query error `QueryError::from` makes of it.

use super::super::{ActionError, ConflictKind, InputError, QueryError};
use super::AuthError;

/// The statuses the surface answers with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Status {
    Ok,
    Accepted,
    NotModified,
    BadRequest,
    Unauthorized,
    Forbidden,
    NotFound,
    Conflict,
    Gone,
    UnprocessableContent,
    TooManyRequests,
    ServiceUnavailable,
}

impl Status {
    pub const ALL: [Self; 12] = [
        Self::Ok,
        Self::Accepted,
        Self::NotModified,
        Self::BadRequest,
        Self::Unauthorized,
        Self::Forbidden,
        Self::NotFound,
        Self::Conflict,
        Self::Gone,
        Self::UnprocessableContent,
        Self::TooManyRequests,
        Self::ServiceUnavailable,
    ];

    /// The status code.
    pub const fn code(self) -> u16 {
        match self {
            Self::Ok => 200,
            Self::Accepted => 202,
            Self::NotModified => 304,
            Self::BadRequest => 400,
            Self::Unauthorized => 401,
            Self::Forbidden => 403,
            Self::NotFound => 404,
            Self::Conflict => 409,
            Self::Gone => 410,
            Self::UnprocessableContent => 422,
            Self::TooManyRequests => 429,
            Self::ServiceUnavailable => 503,
        }
    }
}

impl From<Status> for u16 {
    fn from(status: Status) -> Self {
        status.code()
    }
}

/// An error answered over HTTP: its status. The body is the error's JSON.
pub trait ErrorStatus {
    fn status(&self) -> Status;
}

impl ErrorStatus for QueryError {
    fn status(&self) -> Status {
        match self {
            Self::Store { .. } | Self::Unavailable { .. } => Status::ServiceUnavailable,
            Self::NotFound => Status::NotFound,
            Self::Forbidden { .. } => Status::Forbidden,
            Self::VersionNotRetained { .. } | Self::ProjectionNotRetained { .. } => Status::Gone,
            Self::Conflict(kind) => conflict_status(kind),
            Self::InvalidInput(input) => input_status(input),
            Self::InvalidCursor => Status::BadRequest,
        }
    }
}

impl ErrorStatus for ActionError {
    fn status(&self) -> Status {
        match self {
            Self::Store { .. } | Self::Unavailable { .. } => Status::ServiceUnavailable,
            Self::NotFound => Status::NotFound,
            Self::Forbidden { .. } => Status::Forbidden,
            Self::Conflict(kind) => conflict_status(kind),
            Self::InvalidInput(input) => input_status(input),
        }
    }
}

/// Always 401: the request has no caller ([`super::auth`]).
impl ErrorStatus for AuthError {
    fn status(&self) -> Status {
        Status::Unauthorized
    }
}

/// 409 for every conflict but a full fit queue, which is capacity rather
/// than state the client can change: 429, retried later as is.
pub fn conflict_status(kind: &ConflictKind) -> Status {
    match kind {
        ConflictKind::AlertNotActive { .. }
        | ConflictKind::AlertNotAcknowledged { .. }
        | ConflictKind::AgentMerged { .. }
        | ConflictKind::MergeAlreadyReverted { .. }
        | ConflictKind::MergeIntoSelf { .. }
        | ConflictKind::ChannelSuperseded { .. }
        | ConflictKind::ChannelNotDiscovered { .. }
        | ConflictKind::PatternOverlaps { .. }
        | ConflictKind::RuleNotEditable { .. }
        | ConflictKind::RuleStale { .. }
        | ConflictKind::TopicVersionNotCurrent { .. }
        | ConflictKind::TransmissionNotJudgeable { .. }
        | ConflictKind::TopicVersionFitting { .. }
        | ConflictKind::TopicVersionDropped { .. }
        | ConflictKind::TopicVersionNotActivated { .. }
        | ConflictKind::TopicsNotInVersion { .. }
        | ConflictKind::EmbeddingModelChanged
        | ConflictKind::ProjectionNotReady { .. }
        | ConflictKind::ProjectionFailed { .. }
        | ConflictKind::ExportTooLarge { .. } => Status::Conflict,
        ConflictKind::ProjectionQueueFull => Status::TooManyRequests,
    }
}

/// 400 for input that could not be read as the route's types; 422 for
/// input that was read and is invalid whatever the state.
pub fn input_status(input: &InputError) -> Status {
    match input {
        InputError::MalformedRequest { .. } => Status::BadRequest,
        InputError::UnalignedWindow
        | InputError::BucketWidthMismatch
        | InputError::PatternMissesSeed
        | InputError::UnknownTopics
        | InputError::UnknownSink { .. }
        | InputError::QueryTooLong
        | InputError::SelfMerge
        | InputError::EmptySelection
        | InputError::ExcerptContextTooLong { .. }
        | InputError::TooManyIds { .. }
        | InputError::UnsupportedFormat { .. } => Status::UnprocessableContent,
    }
}
