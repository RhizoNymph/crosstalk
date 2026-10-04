//! Typed query errors (item 21).

use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::interfaces::l8_surface::Permission;

/// Replaces `crosstalk_spec::interfaces::l8_surface::QueryError`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum QueryError {
    #[error("store failed: {reason}")]
    Store { reason: String },
    #[error("not found")]
    NotFound,
    #[error("missing permission {missing:?}")]
    Forbidden { missing: Permission },
    #[error("topic model version {} is no longer retained", version.0)]
    VersionNotRetained { version: TopicModelVersion },
    #[error("conflict: {0}")]
    Conflict(ConflictKind),
    #[error("invalid input: {0}")]
    InvalidInput(InputError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ConflictKind {
    #[error("the agent is merged; act on its canonical agent")]
    AgentMerged,
    #[error("the channel is superseded")]
    ChannelSuperseded,
    #[error("the channel is not discovered")]
    ChannelNotDiscovered,
    #[error("the pattern does not cover the channel's seed")]
    PatternMissesSeed,
    #[error("the merge was already reverted")]
    MergeReverted,
    #[error("the transmission has nothing to judge yet")]
    NotJudgeable,
    #[error("the alert is not in a state that allows this")]
    AlertState,
    #[error("built-in rules can only be enabled or disabled")]
    BuiltinRule,
    #[error("the rule is stale; update it to re-target and enable it")]
    RuleStale,
    #[error("the merge target resolves to the source agent")]
    MergeIntoSelf,
    #[error("watched topics must belong to the current topic version")]
    TopicVersionNotCurrent,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InputError {
    #[error("{field}: {reason}")]
    Field {
        field: &'static str,
        reason: String,
    },
}
