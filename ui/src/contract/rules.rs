//! Alert rule management and sinks (item 18).
//!
//! Built-in rules exist once each and can only be enabled or disabled.
//! Operator rules can be created and edited. No rule is ever deleted, so
//! alerts always reference a rule that exists.

use crosstalk_spec::aggregates::topic::{Embedding, EmbeddingModel, TopicModelVersion};
use crosstalk_spec::ids::{AlertRuleId, OperatorId, TopicId};
use crosstalk_spec::interfaces::l8_surface::SinkError;
use crosstalk_spec::support::{NonEmpty, Similarity, Timestamp};

use super::SinkId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BuiltinRule {
    NewChannel,
    UnreviewedTraffic,
    UnsanctionedTraffic,
    SanctionedUnused,
    SuspectedTransmission,
}

impl BuiltinRule {
    pub const ALL: [Self; 5] = [
        Self::NewChannel,
        Self::UnreviewedTraffic,
        Self::UnsanctionedTraffic,
        Self::SanctionedUnused,
        Self::SuspectedTransmission,
    ];
}

/// The text of a semantic query rule: trimmed, non-empty, at most
/// [`QueryText::MAX_CHARS`] characters.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueryText(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidQueryText {
    #[error("the query text is empty")]
    Empty,
    #[error("the query text is longer than {max} characters", max = QueryText::MAX_CHARS)]
    TooLong,
}

impl QueryText {
    pub const MAX_CHARS: usize = 1000;

    pub fn new(raw: &str) -> Result<Self, InvalidQueryText> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(InvalidQueryText::Empty);
        }
        if trimmed.chars().count() > Self::MAX_CHARS {
            return Err(InvalidQueryText::TooLong);
        }
        Ok(Self(trimmed.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// An operator rule as an operator submits it (`CreateRule`, `UpdateRule`).
/// A semantic query is text: the gateway embeds it with its current model
/// and stores the resulting [`UserRule`], so the UI never handles
/// embeddings.
#[derive(Debug, Clone, PartialEq)]
pub enum UserRuleSpec {
    WatchedTopic {
        version: TopicModelVersion,
        topics: NonEmpty<TopicId>,
        remap_threshold: Similarity,
    },
    SemanticQuery {
        text: QueryText,
        threshold: Similarity,
    },
}

/// An operator rule as stored.
#[derive(Debug, Clone, PartialEq)]
pub enum UserRule {
    WatchedTopic {
        version: TopicModelVersion,
        topics: NonEmpty<TopicId>,
        remap_threshold: Similarity,
    },
    /// The text is kept so the rule can be shown and edited; the embedding
    /// is of that text under `model`, computed by the gateway.
    SemanticQuery {
        text: QueryText,
        model: EmbeddingModel,
        embedding: Embedding,
        threshold: Similarity,
    },
}

impl UserRule {
    /// What an operator would submit to get this rule again: the edit
    /// form's starting point.
    pub fn spec(&self) -> UserRuleSpec {
        match self {
            Self::WatchedTopic {
                version,
                topics,
                remap_threshold,
            } => UserRuleSpec::WatchedTopic {
                version: *version,
                topics: topics.clone(),
                remap_threshold: *remap_threshold,
            },
            Self::SemanticQuery {
                text, threshold, ..
            } => UserRuleSpec::SemanticQuery {
                text: text.clone(),
                threshold: *threshold,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RuleKind {
    Builtin(BuiltinRule),
    User(UserRule),
}

/// A trimmed, non-empty rule name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RuleName(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("rule name is empty")]
pub struct EmptyRuleName;

impl RuleName {
    pub fn new(raw: &str) -> Result<Self, EmptyRuleName> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(EmptyRuleName);
        }
        Ok(Self(trimmed.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StaleReason {
    /// These topics of a watched-topic rule had no match in the new version.
    TopicsUnmapped { topics: NonEmpty<TopicId> },
    /// The embedder changed; the semantic query must be re-embedded.
    EmbeddingModelChanged { now: EmbeddingModel },
}

/// Replaces `crosstalk_spec::aggregates::alert::RuleStatus`, adding the
/// reason a rule is stale.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuleStatus {
    Enabled,
    Disabled,
    Stale(StaleReason),
}

/// What an operator can set. `Stale` is reached only by the system.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorRuleStatus {
    Enabled,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleAuthor {
    Config,
    Operator(OperatorId),
}

/// Replaces `crosstalk_spec::aggregates::alert::AlertRuleDef`.
#[derive(Debug, Clone, PartialEq)]
pub struct RuleDef {
    pub id: AlertRuleId,
    pub name: RuleName,
    pub rule: RuleKind,
    pub status: RuleStatus,
    pub created: (RuleAuthor, Timestamp),
    /// Where its alerts are delivered. Empty means every sink.
    pub sinks: Vec<SinkId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SinkKind {
    Webhook,
    Slack,
    Log,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkInfo {
    pub id: SinkId,
    pub kind: SinkKind,
    pub name: String,
    pub last_delivery: Option<Result<Timestamp, SinkError>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_text_is_trimmed_and_bounded() {
        assert_eq!(
            QueryText::new("  api keys ").map(|t| t.as_str().to_owned()),
            Ok("api keys".to_owned())
        );
        assert_eq!(QueryText::new(" \n "), Err(InvalidQueryText::Empty));
        let long = "x".repeat(QueryText::MAX_CHARS + 1);
        assert_eq!(QueryText::new(&long), Err(InvalidQueryText::TooLong));
        assert!(QueryText::new(&"é".repeat(QueryText::MAX_CHARS)).is_ok());
    }
}
