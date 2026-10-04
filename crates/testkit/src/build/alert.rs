//! Alerts and alert rules.

use std::num::NonZeroU16;
use std::time::Duration;

use crosstalk_spec::aggregates::alert::{
    Alert, AlertRuleDef, AlertState, AlertSubject, BuiltinRule, ContentRule, QueryWatch, RuleName,
    RuleQueryText, RuleStatus, SemanticQuery, SuppressReason, TopicWatch, WatchedTopics,
};
use crosstalk_spec::aggregates::topic::{Embedding, EmbeddingModel, TopicModelVersion};
use crosstalk_spec::ids::{AlertId, AlertRuleId, OperatorId, SinkId, TopicId};
use crosstalk_spec::support::{NonEmpty, Similarity, Timestamp};

use crate::build::error::BuildError;
use crate::ids::Ids;
use crate::time::{T0, after};

/// How long after an alert is raised the builder acknowledges, resolves or
/// suppresses it.
pub const HANDLED_AFTER: Duration = Duration::from_secs(60);

/// Which state a built alert is in.
#[derive(Debug, Clone, PartialEq, Eq)]
enum State {
    Open,
    Acknowledged,
    Resolved { note: Option<String> },
    Suppressed { reason: SuppressReason },
}

/// Builds an [`Alert`]. The default is an open `NewChannel` alert on a
/// fresh channel, raised at [`T0`], seen once. Acknowledged, resolved and
/// suppressed states are stamped [`HANDLED_AFTER`] the raise, by a fresh
/// operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlertBuilder {
    id: AlertId,
    rule: AlertRuleId,
    subject: AlertSubject,
    raised_at: Timestamp,
    occurrences: u32,
    operator: OperatorId,
    state: State,
}

impl AlertBuilder {
    pub fn new(ids: &mut Ids) -> Self {
        Self {
            id: ids.alert(),
            rule: BuiltinRule::NewChannel.id(),
            subject: AlertSubject::Channel(ids.channel()),
            raised_at: T0,
            occurrences: 1,
            operator: ids.operator(),
            state: State::Open,
        }
    }

    pub fn id(&self) -> AlertId {
        self.id
    }

    pub fn with_id(mut self, id: AlertId) -> Self {
        self.id = id;
        self
    }

    pub fn rule(mut self, rule: AlertRuleId) -> Self {
        self.rule = rule;
        self
    }

    pub fn builtin(self, rule: BuiltinRule) -> Self {
        self.rule(rule.id())
    }

    pub fn subject(mut self, subject: AlertSubject) -> Self {
        self.subject = subject;
        self
    }

    pub fn raised_at(mut self, at: Timestamp) -> Self {
        self.raised_at = at;
        self
    }

    /// How many drafts it holds; at least 1.
    pub fn occurrences(mut self, occurrences: u32) -> Self {
        self.occurrences = occurrences.max(1);
        self
    }

    /// The operator who acknowledges or resolves it.
    pub fn by(mut self, operator: OperatorId) -> Self {
        self.operator = operator;
        self
    }

    pub fn open(mut self) -> Self {
        self.state = State::Open;
        self
    }

    pub fn acknowledged(mut self) -> Self {
        self.state = State::Acknowledged;
        self
    }

    pub fn resolved(mut self, note: Option<&str>) -> Self {
        self.state = State::Resolved {
            note: note.map(str::to_owned),
        };
        self
    }

    pub fn suppressed(mut self, reason: SuppressReason) -> Self {
        self.state = State::Suppressed { reason };
        self
    }

    pub fn build(self) -> Alert {
        let at = after(self.raised_at, HANDLED_AFTER);
        let state = match self.state {
            State::Open => AlertState::Open,
            State::Acknowledged => AlertState::Acknowledged {
                by: self.operator,
                at,
            },
            State::Resolved { note } => AlertState::Resolved {
                by: self.operator,
                at,
                note,
            },
            State::Suppressed { reason } => AlertState::Suppressed { at, reason },
        };
        Alert {
            id: self.id,
            rule: self.rule,
            subject: self.subject,
            raised_at: self.raised_at,
            occurrences: self.occurrences,
            state,
        }
    }
}

/// A built-in rule, enabled, delivering nowhere.
pub fn builtin_rule(rule: BuiltinRule) -> AlertRuleDef {
    AlertRuleDef::builtin(rule, RuleStatus::Enabled, Vec::new())
}

/// The embedding model user rules default to: a two-dimensional test model.
pub fn test_model(name: &str) -> EmbeddingModel {
    EmbeddingModel {
        name: name.to_owned(),
        dimension: NonZeroU16::new(2).unwrap_or(NonZeroU16::MIN),
    }
}

/// What a built user rule watches.
#[derive(Debug, Clone, PartialEq)]
enum Watch {
    Topics {
        version: TopicModelVersion,
        topics: NonEmpty<TopicId>,
        threshold: f32,
        /// Unmapped in this version: stale.
        stale_in: Option<TopicModelVersion>,
    },
    Query {
        text: String,
        threshold: f32,
        model: EmbeddingModel,
        /// The embedder's current model, if it differs: stale.
        current: Option<EmbeddingModel>,
    },
}

/// Builds a user [`AlertRuleDef`] through [`AlertRuleDef::load`], in any
/// status and staleness. The default watches one fresh topic of version 1
/// with a 0.8 remap threshold, created by a fresh operator at [`T0`],
/// enabled and current, delivering nowhere.
#[derive(Debug, Clone, PartialEq)]
pub struct UserRuleBuilder {
    id: AlertRuleId,
    name: String,
    created: (OperatorId, Timestamp),
    watch: Watch,
    status: RuleStatus,
    sinks: Vec<SinkId>,
}

impl UserRuleBuilder {
    /// A watched-topic rule.
    pub fn watched_topic(ids: &mut Ids) -> Self {
        Self {
            id: ids.rule(),
            name: "watch topic".to_owned(),
            created: (ids.operator(), T0),
            watch: Watch::Topics {
                version: TopicModelVersion(1),
                topics: NonEmpty::new(ids.topic()),
                threshold: 0.8,
                stale_in: None,
            },
            status: RuleStatus::Enabled,
            sinks: Vec::new(),
        }
    }

    /// A semantic-query rule: "credentials leaving the sandbox" at 0.75,
    /// embedded with the two-dimensional `test-embedder`.
    pub fn semantic_query(ids: &mut Ids) -> Self {
        Self {
            id: ids.rule(),
            name: "semantic query".to_owned(),
            created: (ids.operator(), T0),
            watch: Watch::Query {
                text: "credentials leaving the sandbox".to_owned(),
                threshold: 0.75,
                model: test_model("test-embedder"),
                current: None,
            },
            status: RuleStatus::Enabled,
            sinks: Vec::new(),
        }
    }

    pub fn id(&self) -> AlertRuleId {
        self.id
    }

    pub fn with_id(mut self, id: AlertRuleId) -> Self {
        self.id = id;
        self
    }

    /// The rule's name, checked as [`RuleName`] at build.
    pub fn name(mut self, name: &str) -> Self {
        self.name = name.to_owned();
        self
    }

    pub fn created(mut self, by: OperatorId, at: Timestamp) -> Self {
        self.created = (by, at);
        self
    }

    pub fn status(mut self, status: RuleStatus) -> Self {
        self.status = status;
        self
    }

    pub fn disabled(self) -> Self {
        self.status(RuleStatus::Disabled)
    }

    pub fn sinks(mut self, sinks: Vec<SinkId>) -> Self {
        self.sinks = sinks;
        self
    }

    /// Watch `topics` of `version`. Turns a semantic rule into a
    /// watched-topic one.
    pub fn topics(mut self, version: TopicModelVersion, topics: NonEmpty<TopicId>) -> Self {
        let threshold = match &self.watch {
            Watch::Topics { threshold, .. } => *threshold,
            Watch::Query { .. } => 0.8,
        };
        self.watch = Watch::Topics {
            version,
            topics,
            threshold,
            stale_in: None,
        };
        self
    }

    /// Query `text`. Turns a watched-topic rule into a semantic one.
    pub fn query(mut self, text: &str) -> Self {
        self.watch = match self.watch {
            Watch::Query {
                threshold,
                model,
                current,
                ..
            } => Watch::Query {
                text: text.to_owned(),
                threshold,
                model,
                current,
            },
            Watch::Topics { .. } => Watch::Query {
                text: text.to_owned(),
                threshold: 0.75,
                model: test_model("test-embedder"),
                current: None,
            },
        };
        self
    }

    /// The remap threshold (topics) or match threshold (query), checked as a
    /// [`Similarity`] at build.
    pub fn threshold(mut self, value: f32) -> Self {
        match &mut self.watch {
            Watch::Topics { threshold, .. } | Watch::Query { threshold, .. } => *threshold = value,
        }
        self
    }

    /// Stale: a watched-topic rule whose first topic went unmapped in
    /// `version`, or a semantic rule whose embedder now uses another model.
    pub fn stale(mut self, version: TopicModelVersion) -> Self {
        match &mut self.watch {
            Watch::Topics { stale_in, .. } => *stale_in = Some(version),
            Watch::Query { current, .. } => *current = Some(test_model("test-embedder-next")),
        }
        self
    }

    pub fn build(self) -> Result<AlertRuleDef, BuildError> {
        let name = RuleName::new(&self.name)?;
        let content = match self.watch {
            Watch::Topics {
                version,
                topics,
                threshold,
                stale_in,
            } => {
                let first = *topics.first();
                let last = WatchedTopics { version, topics };
                ContentRule::WatchedTopic {
                    watch: match stale_in {
                        None => TopicWatch::Current(last),
                        Some(unmapped_in) => TopicWatch::Stale {
                            last,
                            unmapped_in,
                            unmapped: NonEmpty::new(first),
                        },
                    },
                    remap_threshold: Similarity::new(threshold)?,
                }
            }
            Watch::Query {
                text,
                threshold,
                model,
                current,
            } => {
                let query = SemanticQuery {
                    text: RuleQueryText::new(&text)?,
                    embedding: Embedding::new(model.clone(), unit_vector(&model))?,
                };
                ContentRule::SemanticQuery {
                    watch: QueryWatch::under(query, current.as_ref().unwrap_or(&model)),
                    threshold: Similarity::new(threshold)?,
                }
            }
        };
        Ok(AlertRuleDef::load(
            self.id,
            name,
            self.created,
            content,
            self.status,
            self.sinks,
        )?)
    }
}

/// The unit vector along the first axis, in `model`'s dimension.
fn unit_vector(model: &EmbeddingModel) -> Vec<f32> {
    let mut values = vec![0.0; usize::from(model.dimension.get())];
    if let Some(first) = values.first_mut() {
        *first = 1.0;
    }
    values
}
