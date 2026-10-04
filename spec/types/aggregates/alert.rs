//! Alert rules and alerts.
//!
//! **Rules.** Five [`BuiltinRule`]s exist exactly once each, from the
//! moment the rule set exists: an operator can enable or disable them, and
//! nothing else. Their ids are fixed ([`BuiltinRule::id`]) and an
//! [`AlertRuleSet`] holds each in its own slot, so a second instance cannot
//! be represented. Operators create and edit user rules, `WatchedTopic` and
//! `SemanticQuery` ([`UserRule`]); the server assigns their ids. No rule is
//! ever deleted, so every alert's rule can always be shown.
//!
//! A user rule can go stale when what it was written against is replaced: a
//! topic re-fit leaves a watched topic without a close successor
//! ([`TopicWatch::Stale`]), or the embedder's model changes under a semantic
//! query ([`QueryWatch::Stale`]). [`AlertRuleDef::stale_reason`] says which.
//! Staleness is separate from the operator's enabled or disabled
//! [`RuleStatus`] and no operator action can set it. A rule evaluates only
//! when enabled and current ([`AlertRuleDef::evaluates`]). Updating a stale
//! rule retargets it to the current topic version or embedding model and
//! enables it ([`AlertRuleDef::update`]).
//!
//! A rule keeps its kind for life, so its alerts keep their meaning.
//! Updating a rule leaves its alerts as they are.
//!
//! **Alerts.** A rule evaluation produces an [`AlertDraft`] (the lifecycle's
//! `Fired`). Triage either opens an [`Alert`] or folds the draft into an
//! active (open or acknowledged) alert with the same rule and subject, so
//! `Deduplicated` is a [`TriageOutcome`], not a stored state. A draft whose
//! rule stopped evaluating before triage opens nothing (`RuleInactive`).
//! Alerts are delivered to the sinks their rule lists.
//!
//! Sanctioning a channel suppresses the active alerts whose subject is that
//! channel; alerts about transmissions on it stay, because content can be
//! worth flagging on a sanctioned channel. Disabling a rule suppresses its
//! active alerts.
//!
//! A `FalseDetection` verdict on a transmission suppresses every active
//! alert whose subject is that transmission, whatever its rule, with reason
//! [`SuppressReason::OperatorRejected`], and while it is the transmission's
//! current verdict triage opens nothing about it
//! ([`TriageOutcome::OperatorRejected`]). Withdrawing the verdict, or
//! replacing it with `Genuine`, reopens nothing: later drafts open alerts
//! again. A `Genuine` verdict changes no alert.
//!
//! ```text
//! draft ─triage─┬─▶ Open ─acknowledge─▶ Acknowledged ─resolve─▶ Resolved
//!               │     └──────┬──────────────┘
//!               │   sanctioned, rule disabled, or transmission
//!               │   judged a false detection
//!               │            ▼
//!               │       Suppressed
//!               ├─▶ deduplicated into an existing alert
//!               └─▶ nothing: rule inactive, or subject judged a false detection
//! ```
//!
//! Every stored change to an alert (a deduplicated occurrence, a
//! suppression, an acknowledgement, a resolution) bumps its
//! [`AlertRevision`] by one and publishes `AlertChanged` with the alert after
//! the change. Changes to one alert are compare-and-set on its revision, so
//! revisions are consecutive and a reader that keeps the highest revision it
//! has seen ends with the stored alert, whatever order the events arrive in.
//! Rules follow the same scheme with [`RuleRevision`] and `AlertRuleChanged`.

use std::num::NonZeroU32;

use crate::aggregates::topic::{Embedding, EmbeddingModel, TopicModelVersion};
use crate::aggregates::topic_history::{RemapError, TopicLineage};
use crate::ids::{
    AgentId, AlertId, AlertRuleId, ChannelId, OperatorId, SinkId, TopicId, TransmissionId,
};
use crate::support::{Change, DisplayText, NonBlank, NonEmpty, Similarity, Timestamp};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlertRuleKind {
    NewChannel,
    UnreviewedTraffic,
    UnsanctionedTraffic,
    SanctionedUnused,
    SuspectedTransmission,
    WatchedTopic,
    SemanticQuery,
}

/// A rule that takes no parameters and exists exactly once.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BuiltinRule {
    /// A channel was discovered that no config declared.
    NewChannel,
    /// Confirmed traffic on a channel whose policy is unreviewed.
    UnreviewedTraffic,
    /// Confirmed traffic on a channel whose policy is unsanctioned.
    UnsanctionedTraffic,
    /// A declared channel whose policy is sanctioned saw no traffic within
    /// its idle window. Flow reports every unused declared channel; this rule
    /// checks the policy.
    SanctionedUnused,
    /// A transmission was left with access-pattern evidence only.
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

    /// The rule's fixed id: 1 to 5. Ids with a zero ULID timestamp are
    /// reserved ([`AlertRuleId`]s below `1 << 80`); no generated id has one.
    pub const fn id(self) -> AlertRuleId {
        AlertRuleId::from_ulid(self.index() as u128 + 1)
    }

    /// The built-in rule with this id, if any.
    pub fn from_id(id: AlertRuleId) -> Option<Self> {
        Self::ALL.into_iter().find(|rule| rule.id() == id)
    }

    pub fn kind(self) -> AlertRuleKind {
        match self {
            Self::NewChannel => AlertRuleKind::NewChannel,
            Self::UnreviewedTraffic => AlertRuleKind::UnreviewedTraffic,
            Self::UnsanctionedTraffic => AlertRuleKind::UnsanctionedTraffic,
            Self::SanctionedUnused => AlertRuleKind::SanctionedUnused,
            Self::SuspectedTransmission => AlertRuleKind::SuspectedTransmission,
        }
    }

    /// The name the UI shows.
    pub fn name(self) -> &'static str {
        match self {
            Self::NewChannel => "New channel",
            Self::UnreviewedTraffic => "Traffic on unreviewed channel",
            Self::UnsanctionedTraffic => "Traffic on unsanctioned channel",
            Self::SanctionedUnused => "Sanctioned channel unused",
            Self::SuspectedTransmission => "Suspected transmission",
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::NewChannel => 0,
            Self::UnreviewedTraffic => 1,
            Self::UnsanctionedTraffic => 2,
            Self::SanctionedUnused => 3,
            Self::SuspectedTransmission => 4,
        }
    }
}

/// Whether `id` is in the range reserved for built-in rules.
pub fn is_reserved_rule_id(id: AlertRuleId) -> bool {
    id.as_ulid() >> 80 == 0
}

/// A user rule's name: trimmed, non-empty, at most 80 characters and free
/// of control characters.
pub type RuleName = DisplayText<80>;

/// Topics of one topic-model version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchedTopics {
    pub version: TopicModelVersion,
    pub topics: NonEmpty<TopicId>,
}

/// A user rule as an operator writes it in `CreateRule` and `UpdateRule`.
/// The rule store resolves it into a [`RuleDefinition`].
#[derive(Debug, Clone, PartialEq)]
pub enum UserRule {
    /// `topics.version` must be the current topic-model version and every
    /// topic must exist in it. `None` for `remap_threshold` takes
    /// [`AlertRuleConfig::default_remap_threshold`].
    WatchedTopic {
        topics: WatchedTopics,
        remap_threshold: Option<Similarity>,
    },
    /// The store embeds `text` with the current embedding model.
    SemanticQuery {
        text: NonBlank,
        threshold: Similarity,
    },
}

impl UserRule {
    /// "Watch this topic": one topic of `version`, with the configured
    /// remap threshold.
    pub fn watch_topic(version: TopicModelVersion, topic: TopicId) -> Self {
        Self::WatchedTopic {
            topics: WatchedTopics {
                version,
                topics: NonEmpty::new(topic),
            },
            remap_threshold: None,
        }
    }

    pub fn kind(&self) -> AlertRuleKind {
        match self {
            Self::WatchedTopic { .. } => AlertRuleKind::WatchedTopic,
            Self::SemanticQuery { .. } => AlertRuleKind::SemanticQuery,
        }
    }
}

/// Configuration for user rules.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AlertRuleConfig {
    /// The remap threshold of a watched-topic rule created without one.
    pub default_remap_threshold: Similarity,
}

/// A semantic query and its embedding. The embedding carries the model it
/// was made with ([`Embedding::model`]), so the two cannot disagree.
#[derive(Debug, Clone, PartialEq)]
pub struct SemanticQuery {
    pub text: NonBlank,
    pub embedding: Embedding,
}

impl SemanticQuery {
    pub fn model(&self) -> &EmbeddingModel {
        self.embedding.model()
    }
}

/// A user rule resolved by the store and current by construction: a
/// watched-topic rule on the current topic version with its threshold
/// filled in, or a semantic query embedded with the current model.
#[derive(Debug, Clone, PartialEq)]
pub enum RuleDefinition {
    WatchedTopic {
        topics: WatchedTopics,
        remap_threshold: Similarity,
    },
    SemanticQuery {
        query: SemanticQuery,
        threshold: Similarity,
    },
}

impl RuleDefinition {
    pub fn kind(&self) -> AlertRuleKind {
        match self {
            Self::WatchedTopic { .. } => AlertRuleKind::WatchedTopic,
            Self::SemanticQuery { .. } => AlertRuleKind::SemanticQuery,
        }
    }
}

/// Whether a watched-topic rule still names topics that mean something.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicWatch {
    Current(WatchedTopics),
    /// A re-fit to `unmapped_in` left a topic without a close counterpart.
    /// The rule evaluates nothing until an operator updates it, which is the
    /// only way back to `Current`. Its active alerts stay active: they were
    /// valid when raised. Produced only by
    /// [`TopicLineage::remap`](crate::aggregates::topic_history::TopicLineage::remap).
    Stale {
        last: WatchedTopics,
        unmapped_in: TopicModelVersion,
        /// The topics of `last` whose best link into `unmapped_in` is absent
        /// or below the rule's threshold, in the rule's order.
        unmapped: NonEmpty<TopicId>,
    },
}

/// Whether a semantic rule's query can still be compared with new
/// embeddings.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryWatch {
    Current(SemanticQuery),
    /// The embedder now uses `model`, and `last` was embedded with another.
    /// Embeddings from different models are never compared, so the rule
    /// evaluates nothing until an operator updates it, which re-embeds the
    /// text. Produced only by [`QueryWatch::under`].
    Stale {
        last: SemanticQuery,
        model: EmbeddingModel,
    },
}

impl QueryWatch {
    /// `query` under the embedder's `current` model: current when it was
    /// embedded with that model, else stale.
    pub fn under(query: SemanticQuery, current: &EmbeddingModel) -> Self {
        if query.model() == current {
            Self::Current(query)
        } else {
            Self::Stale {
                last: query,
                model: current.clone(),
            }
        }
    }
}

/// Why a rule is stale, for the UI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StaleReason {
    /// A re-fit to `version` left `topics` without a successor at or above
    /// the rule's remap threshold.
    TopicsUnmapped {
        version: TopicModelVersion,
        topics: NonEmpty<TopicId>,
    },
    /// The query was embedded with `from` and the embedder now uses `to`.
    EmbeddingModelChanged {
        from: EmbeddingModel,
        to: EmbeddingModel,
    },
}

/// A stored user rule's content.
#[derive(Debug, Clone, PartialEq)]
pub enum ContentRule {
    /// Topic ids only mean something within one topic-model version. When a
    /// new version becomes ready (`TopicVersionReady`), the rule's topics are
    /// remapped over the lineage from its version ([`AlertRuleDef::remap`],
    /// which is [`TopicLineage::remap`], the same lineage the UI shows); a
    /// rule with any topic left unmapped becomes [`TopicWatch::Stale`]
    /// instead of silently watching the wrong topics.
    WatchedTopic {
        watch: TopicWatch,
        remap_threshold: Similarity,
    },
    /// The query is compared with each confirmed transmission's embedding.
    /// When the embedder's model changes it becomes [`QueryWatch::Stale`]
    /// ([`AlertRuleDef::embedding_model_changed`]).
    SemanticQuery {
        watch: QueryWatch,
        threshold: Similarity,
    },
}

impl From<RuleDefinition> for ContentRule {
    fn from(definition: RuleDefinition) -> Self {
        match definition {
            RuleDefinition::WatchedTopic {
                topics,
                remap_threshold,
            } => Self::WatchedTopic {
                watch: TopicWatch::Current(topics),
                remap_threshold,
            },
            RuleDefinition::SemanticQuery { query, threshold } => Self::SemanticQuery {
                watch: QueryWatch::Current(query),
                threshold,
            },
        }
    }
}

impl ContentRule {
    pub fn kind(&self) -> AlertRuleKind {
        match self {
            Self::WatchedTopic { .. } => AlertRuleKind::WatchedTopic,
            Self::SemanticQuery { .. } => AlertRuleKind::SemanticQuery,
        }
    }

    pub fn stale_reason(&self) -> Option<StaleReason> {
        match self {
            Self::WatchedTopic {
                watch:
                    TopicWatch::Stale {
                        unmapped_in,
                        unmapped,
                        ..
                    },
                ..
            } => Some(StaleReason::TopicsUnmapped {
                version: *unmapped_in,
                topics: unmapped.clone(),
            }),
            Self::SemanticQuery {
                watch: QueryWatch::Stale { last, model },
                ..
            } => Some(StaleReason::EmbeddingModelChanged {
                from: last.model().clone(),
                to: model.clone(),
            }),
            Self::WatchedTopic {
                watch: TopicWatch::Current(_),
                ..
            }
            | Self::SemanticQuery {
                watch: QueryWatch::Current(_),
                ..
            } => None,
        }
    }
}

/// A rule: built in, or written by an operator.
#[derive(Debug, Clone, PartialEq)]
pub enum AlertRule {
    Builtin(BuiltinRule),
    User {
        name: RuleName,
        /// Who created it, and when.
        created: (OperatorId, Timestamp),
        content: ContentRule,
    },
}

impl AlertRule {
    pub fn kind(&self) -> AlertRuleKind {
        match self {
            Self::Builtin(rule) => rule.kind(),
            Self::User { content, .. } => content.kind(),
        }
    }

    /// Only user rules can be stale.
    pub fn stale_reason(&self) -> Option<StaleReason> {
        match self {
            Self::Builtin(_) => None,
            Self::User { content, .. } => content.stale_reason(),
        }
    }

    pub fn is_stale(&self) -> bool {
        self.stale_reason().is_some()
    }
}

/// What an operator set. Staleness is separate ([`AlertRule::stale_reason`]),
/// so a rule can be disabled and stale at once, and enabling a stale rule
/// does not make it evaluate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleStatus {
    Enabled,
    Disabled,
}

impl RuleStatus {
    pub fn of(enabled: bool) -> Self {
        if enabled {
            Self::Enabled
        } else {
            Self::Disabled
        }
    }
}

/// A stored rule. Built only through [`AlertRuleDef::builtin`] and
/// [`AlertRuleDef::user`], so a built-in rule's id is always its fixed id,
/// and a user rule's never is. Its transitions never change its id or kind.
#[derive(Debug, Clone, PartialEq)]
pub struct AlertRuleDef {
    id: AlertRuleId,
    rule: AlertRule,
    pub status: RuleStatus,
    /// Where its alerts are delivered.
    pub sinks: Vec<SinkId>,
}

/// A user rule given an id reserved for built-in rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReservedRuleId(pub AlertRuleId);

/// An update of a built-in rule, or one that would change a rule's kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotEditable {
    pub rule: AlertRuleId,
}

/// A remap of a rule that does not watch topics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleRemapError {
    NotWatchedTopic,
    Remap(RemapError),
}

impl AlertRuleDef {
    pub fn builtin(rule: BuiltinRule, status: RuleStatus, sinks: Vec<SinkId>) -> Self {
        Self {
            id: rule.id(),
            rule: AlertRule::Builtin(rule),
            status,
            sinks,
        }
    }

    /// A new user rule: enabled and current.
    pub fn user(
        id: AlertRuleId,
        name: RuleName,
        created: (OperatorId, Timestamp),
        definition: RuleDefinition,
        sinks: Vec<SinkId>,
    ) -> Result<Self, ReservedRuleId> {
        if is_reserved_rule_id(id) {
            return Err(ReservedRuleId(id));
        }
        Ok(Self {
            id,
            rule: AlertRule::User {
                name,
                created,
                content: definition.into(),
            },
            status: RuleStatus::Enabled,
            sinks,
        })
    }

    /// A user rule as stored, in any status and staleness: what the store
    /// reads back. Refuses a reserved id like [`AlertRuleDef::user`].
    pub fn load(
        id: AlertRuleId,
        name: RuleName,
        created: (OperatorId, Timestamp),
        content: ContentRule,
        status: RuleStatus,
        sinks: Vec<SinkId>,
    ) -> Result<Self, ReservedRuleId> {
        if is_reserved_rule_id(id) {
            return Err(ReservedRuleId(id));
        }
        Ok(Self {
            id,
            rule: AlertRule::User {
                name,
                created,
                content,
            },
            status,
            sinks,
        })
    }

    pub fn id(&self) -> AlertRuleId {
        self.id
    }

    pub fn rule(&self) -> &AlertRule {
        &self.rule
    }

    pub fn kind(&self) -> AlertRuleKind {
        self.rule.kind()
    }

    /// The built-in rule's fixed name or the user rule's name.
    pub fn name(&self) -> &str {
        match &self.rule {
            AlertRule::Builtin(rule) => rule.name(),
            AlertRule::User { name, .. } => name.as_str(),
        }
    }

    /// Who created a user rule, and when. `None` for a built-in rule.
    pub fn created(&self) -> Option<(OperatorId, Timestamp)> {
        match &self.rule {
            AlertRule::Builtin(_) => None,
            AlertRule::User { created, .. } => Some(*created),
        }
    }

    pub fn stale_reason(&self) -> Option<StaleReason> {
        self.rule.stale_reason()
    }

    /// Whether the rule produces drafts: enabled and not stale.
    pub fn evaluates(&self) -> bool {
        self.status == RuleStatus::Enabled && !self.rule.is_stale()
    }

    /// Enable or disable. Staleness is untouched, so enabling a stale rule
    /// leaves it stale.
    pub fn set_enabled(&mut self, enabled: bool) -> Change {
        let status = RuleStatus::of(enabled);
        if self.status == status {
            return Change::Unchanged;
        }
        self.status = status;
        Change::Applied
    }

    /// Replace a user rule's name, definition and sinks, keeping its id,
    /// kind and creator. The rule is current afterwards; a stale rule is
    /// also enabled. Its status is otherwise kept. `Unchanged` when a
    /// current rule already has this name, definition and sinks. Refuses a
    /// built-in rule and a definition of another kind, changing nothing.
    pub fn update(
        &mut self,
        name: RuleName,
        definition: RuleDefinition,
        sinks: Vec<SinkId>,
    ) -> Result<Change, NotEditable> {
        let not_editable = NotEditable { rule: self.id };
        let AlertRule::User {
            name: current_name,
            content,
            ..
        } = &mut self.rule
        else {
            return Err(not_editable);
        };
        if content.kind() != definition.kind() {
            return Err(not_editable);
        }
        let was_stale = content.stale_reason().is_some();
        let content_new = ContentRule::from(definition);
        if !was_stale && *current_name == name && *content == content_new && self.sinks == sinks {
            return Ok(Change::Unchanged);
        }
        *current_name = name;
        *content = content_new;
        self.sinks = sinks;
        if was_stale {
            self.status = RuleStatus::Enabled;
        }
        Ok(Change::Applied)
    }

    /// `TopicVersionReady`: carry a current watched-topic rule on
    /// `lineage.from()` over to `lineage.to()` with [`TopicLineage::remap`].
    /// `Unchanged` for a stale rule, which stays stale whatever later fits
    /// show. Status is never touched. Refuses a rule that does not watch
    /// topics, or one on another version, changing nothing.
    pub fn remap(&mut self, lineage: &TopicLineage) -> Result<Change, RuleRemapError> {
        let AlertRule::User {
            content:
                ContentRule::WatchedTopic {
                    watch,
                    remap_threshold,
                },
            ..
        } = &mut self.rule
        else {
            return Err(RuleRemapError::NotWatchedTopic);
        };
        let TopicWatch::Current(topics) = watch else {
            return Ok(Change::Unchanged);
        };
        *watch = lineage
            .remap(topics, *remap_threshold)
            .map_err(RuleRemapError::Remap)?;
        Ok(Change::Applied)
    }

    /// The embedder now uses `current`: a current semantic rule embedded
    /// with another model becomes stale. `Unchanged` for every other rule,
    /// including one already stale. Status is never touched.
    pub fn embedding_model_changed(&mut self, current: &EmbeddingModel) -> Change {
        let AlertRule::User {
            content: ContentRule::SemanticQuery { watch, .. },
            ..
        } = &mut self.rule
        else {
            return Change::Unchanged;
        };
        match watch {
            QueryWatch::Current(query) if query.model() != current => {
                *watch = QueryWatch::under(query.clone(), current);
                Change::Applied
            }
            QueryWatch::Current(_) | QueryWatch::Stale { .. } => Change::Unchanged,
        }
    }
}

/// Every rule. Holds each built-in rule in its own slot from construction,
/// and the user rules by id. Nothing is ever removed, and a built-in rule
/// can never be inserted, so each built-in exists exactly once.
#[derive(Debug, Clone, PartialEq)]
pub struct AlertRuleSet {
    builtins: [AlertRuleDef; 5],
    user: std::collections::BTreeMap<AlertRuleId, AlertRuleDef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertError {
    /// The rule is built in; its slot already exists.
    Builtin(BuiltinRule),
    /// A rule with this id already exists.
    DuplicateId(AlertRuleId),
}

impl AlertRuleSet {
    /// Every built-in rule with the status and sinks `settings` gives it,
    /// and no user rules.
    pub fn new(settings: impl Fn(BuiltinRule) -> (RuleStatus, Vec<SinkId>)) -> Self {
        Self {
            builtins: BuiltinRule::ALL.map(|rule| {
                let (status, sinks) = settings(rule);
                AlertRuleDef::builtin(rule, status, sinks)
            }),
            user: std::collections::BTreeMap::new(),
        }
    }

    /// Add a user rule. Refuses a built-in rule and a taken id.
    pub fn insert(&mut self, rule: AlertRuleDef) -> Result<(), InsertError> {
        if let AlertRule::Builtin(builtin) = rule.rule {
            return Err(InsertError::Builtin(builtin));
        }
        if self.user.contains_key(&rule.id) {
            return Err(InsertError::DuplicateId(rule.id));
        }
        self.user.insert(rule.id, rule);
        Ok(())
    }

    pub fn builtin(&self, rule: BuiltinRule) -> &AlertRuleDef {
        &self.builtins[rule.index()]
    }

    pub fn get(&self, id: AlertRuleId) -> Option<&AlertRuleDef> {
        match BuiltinRule::from_id(id) {
            Some(rule) => Some(self.builtin(rule)),
            None => self.user.get(&id),
        }
    }

    /// The rule to change in place. Its transitions keep its id and kind.
    pub fn get_mut(&mut self, id: AlertRuleId) -> Option<&mut AlertRuleDef> {
        match BuiltinRule::from_id(id) {
            Some(rule) => Some(&mut self.builtins[rule.index()]),
            None => self.user.get_mut(&id),
        }
    }

    /// Built-in rules in [`BuiltinRule::ALL`] order, then user rules by id.
    pub fn iter(&self) -> impl Iterator<Item = &AlertRuleDef> {
        self.builtins.iter().chain(self.user.values())
    }
}

/// How many stored changes a rule has had: 1 when created, one more per
/// change (an update, an enable or disable, going stale). Changes to one
/// rule are compare-and-set on its revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RuleRevision(NonZeroU32);

impl RuleRevision {
    pub const CREATED: Self = Self(NonZeroU32::MIN);

    pub const fn new(revision: NonZeroU32) -> Self {
        Self(revision)
    }

    pub const fn get(self) -> NonZeroU32 {
        self.0
    }

    /// `None` once the counter is exhausted; the store rejects that change.
    pub fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlertSubject {
    Channel(ChannelId),
    Transmission(TransmissionId),
    Agent(AgentId),
}

/// A rule's output, before triage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlertDraft {
    pub rule: AlertRuleId,
    pub subject: AlertSubject,
    pub raised_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriageOutcome {
    Opened(Alert),
    /// An active (open or acknowledged) alert with the same rule and subject
    /// already exists; its occurrence count goes up instead.
    Deduplicated {
        into: AlertId,
    },
    /// The draft's rule no longer evaluates (disabled or stale by the time
    /// the draft was triaged), so nothing was opened. Closes the race between
    /// an evaluation and a disable.
    RuleInactive,
    /// The draft's subject is a transmission whose current verdict, as
    /// triage holds it, is `FalseDetection`, so nothing was opened. Closes
    /// the race between an evaluation and the verdict's suppression.
    OperatorRejected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Alert {
    pub id: AlertId,
    pub rule: AlertRuleId,
    pub subject: AlertSubject,
    pub raised_at: Timestamp,
    pub occurrences: u32,
    pub state: AlertState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlertState {
    Open,
    Acknowledged {
        by: OperatorId,
        at: Timestamp,
    },
    Resolved {
        by: OperatorId,
        at: Timestamp,
        note: Option<String>,
    },
    /// The condition stopped being alert-worthy: the channel was sanctioned,
    /// the rule disabled, or the transmission judged a false detection.
    Suppressed {
        at: Timestamp,
        reason: SuppressReason,
    },
}

/// How many stored changes an alert has had: 1 when opened, one more per
/// change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AlertRevision(NonZeroU32);

impl AlertRevision {
    /// The revision `AlertOpened` carries.
    pub const OPENED: Self = Self(NonZeroU32::MIN);

    pub const fn new(revision: NonZeroU32) -> Self {
        Self(revision)
    }

    pub const fn get(self) -> NonZeroU32 {
        self.0
    }

    /// The revision after one more change. `None` once the counter is
    /// exhausted; the store rejects that change.
    pub fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuppressReason {
    ChannelSanctioned,
    RuleDisabled,
    /// An operator judged the transmission the alert is about a
    /// `FalseDetection`. A later withdrawal does not reopen the alert.
    OperatorRejected,
}
