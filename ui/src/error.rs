//! Why a page, shard or data route failed: the surface's typed
//! [`QueryError`], or a value from the browser the UI rejected before asking
//! the surface anything.
//!
//! The surface's errors carry no text (`crosstalk-spec` has no
//! dependencies), so [`describe`] words every variant here, once, for every
//! page that shows an error.

use std::fmt;

use crosstalk_spec::aggregates::projection::{FitFailure, ProjectionStatusKind};
use crosstalk_spec::interfaces::l8_surface::{
    ActionError, ConflictKind, InputError, Permission, QueryError,
};

use crate::url::ulid::UlidId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UiError {
    /// The surface refused or failed the request.
    Query(QueryError),
    /// A query key, form field or shard argument that did not validate.
    Field { field: &'static str, reason: String },
}

impl UiError {
    pub fn field(field: &'static str, reason: impl fmt::Display) -> Self {
        Self::Field {
            field,
            reason: reason.to_string(),
        }
    }
}

impl From<QueryError> for UiError {
    fn from(error: QueryError) -> Self {
        Self::Query(error)
    }
}

impl From<ActionError> for UiError {
    fn from(error: ActionError) -> Self {
        Self::Query(QueryError::from(error))
    }
}

impl fmt::Display for UiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Query(error) => f.write_str(&describe(error)),
            Self::Field { field, reason } => write!(f, "{field}: {reason}"),
        }
    }
}

impl std::error::Error for UiError {}

/// A query error in words.
pub fn describe(error: &QueryError) -> String {
    match error {
        QueryError::Store { reason } => format!("the gateway's store failed: {reason}"),
        QueryError::NotFound => "not found".to_owned(),
        QueryError::Forbidden { missing } => {
            format!("this needs the {} permission", permission_name(*missing))
        }
        QueryError::VersionNotRetained { version } => format!(
            "topic model version {} is no longer retained; pick a newer version",
            version.0
        ),
        QueryError::Conflict(kind) => conflict(kind),
        QueryError::InvalidInput(input) => self::input(input),
        QueryError::InvalidCursor => {
            "that page link has expired; start again from the first page".to_owned()
        }
        QueryError::ProjectionNotRetained { projection } => format!(
            "projection {} was fitted too long ago and its points are gone; fit it again",
            projection.to_ulid()
        ),
    }
}

pub fn permission_name(permission: Permission) -> &'static str {
    match permission {
        Permission::View => "View",
        Permission::Content => "Content",
        Permission::Govern => "Govern",
        Permission::Triage => "Triage",
        Permission::Operate => "Operate",
        Permission::Audit => "Audit",
    }
}

fn conflict(kind: &ConflictKind) -> String {
    match kind {
        ConflictKind::AlertNotActive { alert } => format!(
            "the alert is not in a state that allows this: alert {} is no longer open or acknowledged",
            alert.to_ulid()
        ),
        ConflictKind::AgentMerged { agent, into } => format!(
            "agent {} is merged into {}; act on that agent instead",
            agent.to_ulid(),
            into.to_ulid()
        ),
        ConflictKind::MergeAlreadyReverted { merge } => {
            format!("merge {} was already reverted", merge.to_ulid())
        }
        ConflictKind::MergeIntoSelf { canonical, .. } => format!(
            "the merge target resolves to the source agent: both are {}",
            canonical.to_ulid()
        ),
        ConflictKind::ChannelSuperseded { channel, by } => format!(
            "the channel is superseded: {} resolves to {}; act on that channel instead",
            channel.to_ulid(),
            by.to_ulid()
        ),
        ConflictKind::ChannelNotDiscovered { channel } => format!(
            "channel {} is already declared; only discovered channels can be promoted",
            channel.to_ulid()
        ),
        ConflictKind::PatternOverlaps { existing } => format!(
            "the pattern overlaps declared channel {}",
            existing.to_ulid()
        ),
        ConflictKind::RuleNotEditable { rule } => format!(
            "rule {} cannot be edited this way: built-in rules can only be enabled or disabled, and a rule keeps its kind",
            rule.to_ulid()
        ),
        ConflictKind::RuleStale { rule } => format!(
            "rule {} is stale; update it to retarget and enable it",
            rule.to_ulid()
        ),
        ConflictKind::TopicVersionNotCurrent { requested, current } => format!(
            "watched topics must be of the current topic version {} (not {})",
            current.0, requested.0
        ),
        ConflictKind::TransmissionNotJudgeable { transmission } => format!(
            "transmission {} has nothing to judge yet",
            transmission.to_ulid()
        ),
        ConflictKind::TopicVersionFitting { version } => {
            format!("topic model version {} is still being fitted", version.0)
        }
        ConflictKind::TopicVersionDropped { version } => {
            format!("topic model version {} has been dropped", version.0)
        }
        ConflictKind::TopicVersionNotActivated { version } => format!(
            "topic model version {} was never activated, so views cannot use it",
            version.0
        ),
        ConflictKind::TopicsNotInVersion { version, topics } => format!(
            "{} filtered topic(s) are not in topic model version {}; clear the topic filter or pick their version",
            topics.len(),
            version.0
        ),
        ConflictKind::EmbeddingModelChanged => {
            "the embedding model changed; run the search again".to_owned()
        }
        ConflictKind::ProjectionNotReady { projection, status } => format!(
            "projection {} is {}",
            projection.to_ulid(),
            match status {
                ProjectionStatusKind::Queued => "queued",
                ProjectionStatusKind::Fitting => "still fitting",
                ProjectionStatusKind::Ready => "ready",
                ProjectionStatusKind::Failed => "failed",
                ProjectionStatusKind::Expired => "expired",
            }
        ),
        ConflictKind::ProjectionFailed {
            projection,
            failure,
        } => format!(
            "projection {} failed: {}",
            projection.to_ulid(),
            fit_failure(failure)
        ),
        ConflictKind::ProjectionQueueFull => {
            "too many projections are fitting; try again shortly".to_owned()
        }
        ConflictKind::ExportTooLarge { rows, limit } => format!(
            "the export would hold {rows} rows, more than the limit of {limit}; narrow the window or the filter"
        ),
    }
}

/// Why a projection's fit failed, in words.
pub fn fit_failure(failure: &FitFailure) -> String {
    match failure {
        FitFailure::TooFewPoints { needed, got } => {
            format!("{got} points sampled, it needs more than {needed}")
        }
        FitFailure::VersionNotRetained { version } => format!(
            "topic model version {} was dropped before the fit read its sample",
            version.0
        ),
        FitFailure::EmbeddingModelUnavailable { model } => {
            format!("embedding model {} is no longer available", model.name)
        }
        FitFailure::NonFiniteLayout => "the layout had a non-finite coordinate".to_owned(),
    }
}

fn input(error: &InputError) -> String {
    match error {
        InputError::UnalignedWindow => {
            "the window does not start and end on bucket boundaries".to_owned()
        }
        InputError::BucketWidthMismatch => {
            "the series grid was built for another bucket width".to_owned()
        }
        InputError::PatternMissesSeed => {
            "the pattern does not cover the channel's seed resource".to_owned()
        }
        InputError::UnknownTopics => {
            "the rule names topics or a version that do not exist".to_owned()
        }
        InputError::UnknownSink { sink } => format!("sink {} is not configured", sink.to_ulid()),
        InputError::QueryTooLong => "the text is too long to embed".to_owned(),
        InputError::SelfMerge => "an agent cannot be merged into itself".to_owned(),
        InputError::EmptySelection => "the selection holds no transmission".to_owned(),
        InputError::ExcerptContextTooLong { max, got } => {
            format!("excerpt context of {got} bytes is over the maximum of {max}")
        }
        InputError::TooManyIds { max, got } => {
            format!("{got} ids asked for at once, more than the maximum of {max}")
        }
    }
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::aggregates::topic::TopicModelVersion;
    use crosstalk_spec::ids::AgentId;

    use super::*;

    #[test]
    fn every_error_has_words() {
        let errors = [
            QueryError::NotFound,
            QueryError::InvalidCursor,
            QueryError::Forbidden {
                missing: Permission::Audit,
            },
            QueryError::VersionNotRetained {
                version: TopicModelVersion(1),
            },
            QueryError::Conflict(ConflictKind::AgentMerged {
                agent: AgentId::from_ulid(1),
                into: AgentId::from_ulid(2),
            }),
            QueryError::InvalidInput(InputError::EmptySelection),
            QueryError::InvalidInput(InputError::ExcerptContextTooLong { max: 8, got: 9 }),
        ];
        for error in errors {
            assert!(!describe(&error).is_empty(), "{error:?}");
        }
        assert_eq!(
            describe(&QueryError::Forbidden {
                missing: Permission::Audit
            }),
            "this needs the Audit permission"
        );
    }

    #[test]
    fn fields_name_themselves() {
        assert_eq!(
            UiError::field("note", "too long").to_string(),
            "note: too long"
        );
        let from_action: UiError = ActionError::NotFound.into();
        assert_eq!(from_action, UiError::Query(QueryError::NotFound));
    }
}
