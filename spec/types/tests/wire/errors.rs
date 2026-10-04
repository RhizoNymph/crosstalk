//! Query and action errors on the wire. Each golden lists one value of
//! every variant; the exhaustive matches are the reminder to add a new
//! variant to its golden.

use std::num::NonZeroU16;

use super::harness::{assert_golden, assert_rejected};
use super::{ULID_A, ULID_B, ULID_C, id};
use crate::aggregates::projection::{FitFailure, ProjectionStatusKind};
use crate::aggregates::topic::{EmbeddingModel, TopicModelVersion};
use crate::ids::{
    AgentId, AlertId, AlertRuleId, ChannelId, MergeId, ProjectionId, SinkId, TopicId,
    TransmissionId,
};
use crate::interfaces::l8_surface::{
    ActionError, ConflictKind, InputError, Permission, QueryError,
};
use crate::wire::DecodeErrorKind;

const AREA: &str = "errors";

fn version(n: u32) -> TopicModelVersion {
    TopicModelVersion(n)
}

fn projection() -> ProjectionId {
    id(ProjectionId::from_ulid_text, ULID_C)
}

fn model() -> EmbeddingModel {
    EmbeddingModel {
        name: "nomic-embed-text-v1.5".into(),
        dimension: NonZeroU16::new(768).unwrap_or(NonZeroU16::MIN),
    }
}

fn every_fit_failure() -> Vec<FitFailure> {
    fn declared(failure: FitFailure) -> FitFailure {
        match failure {
            FitFailure::TooFewPoints { .. }
            | FitFailure::VersionNotRetained { .. }
            | FitFailure::EmbeddingModelUnavailable { .. }
            | FitFailure::NonFiniteLayout => failure,
        }
    }
    [
        FitFailure::TooFewPoints { needed: 16, got: 9 },
        FitFailure::VersionNotRetained {
            version: version(4),
        },
        FitFailure::EmbeddingModelUnavailable { model: model() },
        FitFailure::NonFiniteLayout,
    ]
    .into_iter()
    .map(declared)
    .collect()
}

fn every_conflict() -> Vec<ConflictKind> {
    fn declared(kind: ConflictKind) -> ConflictKind {
        match kind {
            ConflictKind::AlertNotActive { .. }
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
            | ConflictKind::ProjectionQueueFull
            | ConflictKind::ExportTooLarge { .. } => kind,
        }
    }
    let agent = |text| id(AgentId::from_ulid_text, text);
    let channel = |text| id(ChannelId::from_ulid_text, text);
    [
        ConflictKind::AlertNotActive {
            alert: id(AlertId::from_ulid_text, ULID_A),
        },
        ConflictKind::AgentMerged {
            agent: agent(ULID_A),
            into: agent(ULID_B),
        },
        ConflictKind::MergeAlreadyReverted {
            merge: id(MergeId::from_ulid_text, ULID_A),
        },
        ConflictKind::MergeIntoSelf {
            from: agent(ULID_A),
            into: agent(ULID_B),
            canonical: agent(ULID_C),
        },
        ConflictKind::ChannelSuperseded {
            channel: channel(ULID_A),
            by: channel(ULID_B),
        },
        ConflictKind::ChannelNotDiscovered {
            channel: channel(ULID_A),
        },
        ConflictKind::PatternOverlaps {
            existing: channel(ULID_B),
        },
        ConflictKind::RuleNotEditable {
            rule: AlertRuleId::from_ulid(1),
        },
        ConflictKind::RuleStale {
            rule: id(AlertRuleId::from_ulid_text, ULID_A),
        },
        ConflictKind::TopicVersionNotCurrent {
            requested: version(3),
            current: version(4),
        },
        ConflictKind::TransmissionNotJudgeable {
            transmission: id(TransmissionId::from_ulid_text, ULID_A),
        },
        ConflictKind::TopicVersionFitting {
            version: version(5),
        },
        ConflictKind::TopicVersionDropped {
            version: version(1),
        },
        ConflictKind::TopicVersionNotActivated {
            version: version(5),
        },
        ConflictKind::TopicsNotInVersion {
            version: version(4),
            topics: vec![
                id(TopicId::from_ulid_text, ULID_A),
                id(TopicId::from_ulid_text, ULID_B),
            ],
        },
        ConflictKind::EmbeddingModelChanged,
        ConflictKind::ProjectionNotReady {
            projection: projection(),
            status: ProjectionStatusKind::Fitting,
        },
        ConflictKind::ProjectionFailed {
            projection: projection(),
            failure: FitFailure::TooFewPoints { needed: 16, got: 9 },
        },
        ConflictKind::ProjectionQueueFull,
        ConflictKind::ExportTooLarge {
            rows: 2_000_001,
            limit: 2_000_000,
        },
    ]
    .into_iter()
    .map(declared)
    .collect()
}

fn every_input_error() -> Vec<InputError> {
    fn declared(input: InputError) -> InputError {
        match input {
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
            | InputError::MalformedRequest { .. } => input,
        }
    }
    [
        InputError::UnalignedWindow,
        InputError::BucketWidthMismatch,
        InputError::PatternMissesSeed,
        InputError::UnknownTopics,
        InputError::UnknownSink {
            sink: id(SinkId::from_ulid_text, ULID_A),
        },
        InputError::QueryTooLong,
        InputError::SelfMerge,
        InputError::EmptySelection,
        InputError::ExcerptContextTooLong {
            max: 2048,
            got: 4096,
        },
        InputError::TooManyIds {
            max: 1000,
            got: 1001,
        },
        InputError::MalformedRequest {
            kind: DecodeErrorKind::Data,
            reason: "unknown field `stats`, expected `states` or `channel` at line 1 column 8"
                .into(),
        },
    ]
    .into_iter()
    .map(declared)
    .collect()
}

fn every_query_error() -> Vec<QueryError> {
    fn declared(error: QueryError) -> QueryError {
        match error {
            QueryError::Store { .. }
            | QueryError::NotFound
            | QueryError::Forbidden { .. }
            | QueryError::VersionNotRetained { .. }
            | QueryError::Conflict(_)
            | QueryError::InvalidInput(_)
            | QueryError::InvalidCursor
            | QueryError::ProjectionNotRetained { .. } => error,
        }
    }
    [
        QueryError::Store {
            reason: "connection reset by peer".into(),
        },
        QueryError::NotFound,
        QueryError::Forbidden {
            missing: Permission::Content,
        },
        QueryError::VersionNotRetained {
            version: version(2),
        },
        QueryError::Conflict(ConflictKind::RuleStale {
            rule: id(AlertRuleId::from_ulid_text, ULID_A),
        }),
        QueryError::InvalidInput(InputError::UnalignedWindow),
        QueryError::InvalidCursor,
        QueryError::ProjectionNotRetained {
            projection: projection(),
        },
    ]
    .into_iter()
    .map(declared)
    .collect()
}

fn every_action_error() -> Vec<ActionError> {
    fn declared(error: ActionError) -> ActionError {
        match error {
            ActionError::Store { .. }
            | ActionError::NotFound
            | ActionError::Forbidden { .. }
            | ActionError::Conflict(_)
            | ActionError::InvalidInput(_) => error,
        }
    }
    [
        ActionError::Store {
            reason: "serialization failure".into(),
        },
        ActionError::NotFound,
        ActionError::Forbidden {
            missing: Permission::Govern,
        },
        ActionError::Conflict(ConflictKind::AlertNotActive {
            alert: id(AlertId::from_ulid_text, ULID_A),
        }),
        ActionError::InvalidInput(InputError::SelfMerge),
    ]
    .into_iter()
    .map(declared)
    .collect()
}

#[test]
fn errors_golden_with_every_variant() {
    assert_golden(AREA, "query_errors", &every_query_error());
    assert_golden(AREA, "action_errors", &every_action_error());
    assert_golden(AREA, "conflict_kinds", &every_conflict());
    assert_golden(AREA, "input_errors", &every_input_error());
    assert_golden(AREA, "fit_failures", &every_fit_failure());
    assert_golden(AREA, "permissions", &Permission::ALL.to_vec());
    let statuses = [
        ProjectionStatusKind::Queued,
        ProjectionStatusKind::Fitting,
        ProjectionStatusKind::Ready,
        ProjectionStatusKind::Failed,
        ProjectionStatusKind::Expired,
    ];
    assert_golden(AREA, "projection_status_kinds", &statuses.to_vec());
}

/// An action error is a query error with the same JSON, so a client reads
/// both with one decoder.
#[test]
fn action_errors_encode_as_their_query_error() {
    for error in every_action_error() {
        let as_query = QueryError::from(error.clone());
        assert_eq!(
            serde_json::to_value(&error).ok(),
            serde_json::to_value(&as_query).ok(),
            "{error:?}"
        );
    }
}

#[test]
fn errors_refuse_unknown_variants_and_fields() {
    assert_rejected::<QueryError>(r#"{"type": "timeout"}"#, "unknown variant `timeout`");
    assert_rejected::<QueryError>(
        r#"{"type": "forbidden", "data": {"missing": "admin"}}"#,
        "unknown variant `admin`",
    );
    assert_rejected::<QueryError>(
        r#"{"type": "store", "data": {"reason": "x", "retry_after": 5}}"#,
        "unknown field `retry_after`",
    );
    // Not every query error is an action error.
    assert_rejected::<ActionError>(
        r#"{"type": "invalid_cursor"}"#,
        "unknown variant `invalid_cursor`",
    );
    assert_rejected::<ConflictKind>(
        r#"{"type": "projection_failed", "data": {"projection": "01J9Z3N4P5Q6R7S8T9V0W1X2Y3",
            "failure": {"type": "embedding_model_unavailable",
                        "data": {"model": {"name": "m", "dimension": 0}}}}}"#,
        "invalid value",
    );
    assert_rejected::<InputError>(
        r#"{"type": "malformed_request", "data": {"kind": "io", "reason": "x"}}"#,
        "unknown variant `io`",
    );
}
