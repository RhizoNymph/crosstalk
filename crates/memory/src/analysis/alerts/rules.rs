//! `AlertRuleStore` on the reference alert store, and the rule changes the
//! `alerts` consumer makes: remapping watched topics on `TopicVersionReady`
//! and marking semantic rules stale when the embedding model changes.

use std::collections::BTreeSet;

use crosstalk_spec::aggregates::alert::{
    AlertRule, AlertRuleDef, ContentRule, NotEditable, RuleDefinition, RuleName, SemanticQuery,
    SuppressReason, UserRule,
};
use crosstalk_spec::aggregates::topic::EmbeddingModel;
use crosstalk_spec::aggregates::topic_history::TopicLineage;
use crosstalk_spec::ids::{AlertRuleId, OperatorId, SinkId, TopicId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_spec::interfaces::l6_analysis::{AlertRuleStore, EmbedError, Embedder, RuleError};
use crosstalk_spec::support::{Change, NonEmpty, Timestamp};

use super::{AlertStoreConfig, AlertsState, CommitRefused, InMemoryAlertStore};
use crate::analysis::support::lock;

fn store_error(error: CommitRefused) -> RuleError {
    RuleError::Store {
        reason: error.to_string(),
    }
}

impl AlertsState {
    /// Resolve `rule` into a definition current now: a watched-topic rule
    /// must name the current version and topics in it, and takes the
    /// configured threshold when it has none; a semantic rule takes the
    /// query embedded before the lock was taken.
    fn resolve(
        &self,
        config: &AlertStoreConfig,
        rule: UserRule,
        embedded: Option<SemanticQuery>,
    ) -> Result<RuleDefinition, RuleError> {
        match rule {
            UserRule::WatchedTopic {
                topics,
                remap_threshold,
            } => {
                if topics.version != self.version {
                    return Err(RuleError::TopicVersionNotCurrent {
                        requested: topics.version,
                        current: self.version,
                    });
                }
                let mut unknown: Vec<TopicId> = Vec::new();
                for topic in topics.topics.iter() {
                    if !self.topics.contains(topic) && !unknown.contains(topic) {
                        unknown.push(*topic);
                    }
                }
                if let Some(unknown) = NonEmpty::from_vec(unknown) {
                    return Err(RuleError::UnknownTopics(unknown));
                }
                Ok(RuleDefinition::WatchedTopic {
                    topics,
                    remap_threshold: remap_threshold
                        .unwrap_or(config.rules.default_remap_threshold),
                })
            }
            UserRule::SemanticQuery { threshold, .. } => {
                let query = embedded.ok_or_else(|| RuleError::Store {
                    reason: "a semantic rule reached the store unembedded".to_owned(),
                })?;
                Ok(RuleDefinition::SemanticQuery { query, threshold })
            }
        }
    }
}

fn check_sinks(config: &AlertStoreConfig, sinks: &[SinkId]) -> Result<(), RuleError> {
    match sinks.iter().find(|sink| !config.sinks.contains(sink)) {
        Some(sink) => Err(RuleError::UnknownSink(*sink)),
        None => Ok(()),
    }
}

impl<E, D> InMemoryAlertStore<E, D>
where
    E: Embedder + Send + Sync,
    D: AgentDirectory + ChannelDirectory + Send + Sync,
{
    /// A semantic rule's query embedded with the embedder's current model;
    /// `None` for a watched-topic rule.
    async fn embed_if_semantic(&self, rule: &UserRule) -> Result<Option<SemanticQuery>, RuleError> {
        let UserRule::SemanticQuery { text, .. } = rule else {
            return Ok(None);
        };
        let embeddings = self
            .embedder
            .embed(&[text.as_str()])
            .await
            .map_err(RuleError::Embed)?;
        let embedding = embeddings.into_iter().next().ok_or_else(|| {
            RuleError::Embed(EmbedError::Model {
                reason: "the embedder returned no embedding".to_owned(),
            })
        })?;
        Ok(Some(SemanticQuery {
            text: text.clone(),
            embedding,
        }))
    }

    /// The consumer starting: every current semantic rule embedded with a
    /// model other than the embedder's is marked stale before any event is
    /// evaluated.
    pub fn start(&self) -> Result<Vec<AlertRuleId>, CommitRefused> {
        self.embedding_model_changed(&self.embedder.model())
    }

    /// `TopicVersionReady` for `lineage.to()`, whose topics are `topics`:
    /// every watched-topic rule current on `lineage.from()` is carried over
    /// with `AlertRuleDef::remap` (current on the new version, or stale),
    /// and the new version becomes the one rules must name. Returns the
    /// rules that changed. A version not newer than the current one changes
    /// nothing (a redelivery).
    pub fn topic_version_ready(
        &self,
        lineage: &TopicLineage,
        topics: impl IntoIterator<Item = TopicId>,
    ) -> Result<Vec<AlertRuleId>, CommitRefused> {
        let mut state = lock(&self.state);
        if lineage.to() <= state.version {
            return Ok(Vec::new());
        }
        let mut remapped = Vec::new();
        for rule in state.rules.iter() {
            let mut next = rule.clone();
            if next
                .remap(lineage)
                .is_ok_and(|change| change == Change::Applied)
            {
                remapped.push(next);
            }
        }
        state.check_rule_revisions(remapped.iter().map(AlertRuleDef::id))?;
        let changed = remapped.iter().map(AlertRuleDef::id).collect();
        for rule in remapped {
            state.commit_rule(rule)?;
        }
        state.version = lineage.to();
        state.topics = topics.into_iter().collect::<BTreeSet<_>>();
        Ok(changed)
    }

    /// The embedder now uses `model`: every current semantic rule embedded
    /// with another model becomes stale, keeping its status. Returns the
    /// rules that changed.
    pub fn embedding_model_changed(
        &self,
        model: &EmbeddingModel,
    ) -> Result<Vec<AlertRuleId>, CommitRefused> {
        let mut state = lock(&self.state);
        let mut stale = Vec::new();
        for rule in state.rules.iter() {
            let mut next = rule.clone();
            if next.embedding_model_changed(model) == Change::Applied {
                stale.push(next);
            }
        }
        state.check_rule_revisions(stale.iter().map(AlertRuleDef::id))?;
        let changed = stale.iter().map(AlertRuleDef::id).collect();
        for rule in stale {
            state.commit_rule(rule)?;
        }
        state.model = model.clone();
        Ok(changed)
    }
}

impl AlertsState {
    /// Refuses when any of `rules` has no next revision.
    fn check_rule_revisions(
        &self,
        mut rules: impl Iterator<Item = AlertRuleId>,
    ) -> Result<(), CommitRefused> {
        let exhausted = rules.any(|id| {
            self.rule_revisions
                .get(&id)
                .is_some_and(|revision| revision.next().is_none())
        });
        if exhausted {
            Err(CommitRefused::RevisionExhausted)
        } else {
            Ok(())
        }
    }
}

/// Whether `rule` is a user rule of `kind`'s kind, so an update can apply.
fn editable(rule: &AlertRuleDef, update: &UserRule) -> bool {
    match rule.rule() {
        AlertRule::Builtin(_) => false,
        AlertRule::User { content, .. } => match (content, update) {
            (ContentRule::WatchedTopic { .. }, UserRule::WatchedTopic { .. })
            | (ContentRule::SemanticQuery { .. }, UserRule::SemanticQuery { .. }) => true,
            (ContentRule::WatchedTopic { .. }, UserRule::SemanticQuery { .. })
            | (ContentRule::SemanticQuery { .. }, UserRule::WatchedTopic { .. }) => false,
        },
    }
}

impl<E, D> AlertRuleStore for InMemoryAlertStore<E, D>
where
    E: Embedder + Send + Sync,
    D: AgentDirectory + ChannelDirectory + Send + Sync,
{
    async fn create(
        &mut self,
        name: RuleName,
        rule: UserRule,
        sinks: Vec<SinkId>,
        by: OperatorId,
        at: Timestamp,
    ) -> Result<AlertRuleId, RuleError> {
        let embedded = self.embed_if_semantic(&rule).await?;
        let mut state = lock(&self.state);
        let definition = state.resolve(&self.config, rule, embedded)?;
        check_sinks(&self.config, &sinks)?;
        let id = AlertRuleId::from_ulid(state.rule_ids.next_raw());
        let rule = AlertRuleDef::user(id, name, (by, at), definition, sinks).map_err(|error| {
            RuleError::Store {
                reason: format!("generated a reserved rule id: {error:?}"),
            }
        })?;
        state.commit_rule(rule).map_err(store_error)?;
        Ok(id)
    }

    async fn update(
        &mut self,
        id: AlertRuleId,
        name: RuleName,
        rule: UserRule,
        sinks: Vec<SinkId>,
        _by: OperatorId,
    ) -> Result<Change, RuleError> {
        {
            let state = lock(&self.state);
            let stored = state.rules.get(id).ok_or(RuleError::UnknownRule(id))?;
            if !editable(stored, &rule) {
                return Err(RuleError::NotEditable(NotEditable { rule: id }));
            }
        }
        let embedded = self.embed_if_semantic(&rule).await?;
        let mut state = lock(&self.state);
        let mut stored = state
            .rules
            .get(id)
            .cloned()
            .ok_or(RuleError::UnknownRule(id))?;
        let definition = state.resolve(&self.config, rule, embedded)?;
        check_sinks(&self.config, &sinks)?;
        let change = stored
            .update(name, definition, sinks)
            .map_err(RuleError::NotEditable)?;
        if change == Change::Applied {
            state.commit_rule(stored).map_err(store_error)?;
        }
        Ok(change)
    }

    async fn set_enabled(
        &mut self,
        id: AlertRuleId,
        enabled: bool,
        _by: OperatorId,
    ) -> Result<Change, RuleError> {
        let at = self.clock.now();
        let mut state = lock(&self.state);
        let mut stored = state
            .rules
            .get(id)
            .cloned()
            .ok_or(RuleError::UnknownRule(id))?;
        let change = stored.set_enabled(enabled).map_err(RuleError::Stale)?;
        if change == Change::Unchanged {
            return Ok(change);
        }
        state
            .check_rule_revisions(std::iter::once(id))
            .map_err(store_error)?;
        if !enabled {
            state
                .suppress(|alert| alert.rule == id, SuppressReason::RuleDisabled, at)
                .map_err(store_error)?;
        }
        state.commit_rule(stored).map_err(store_error)?;
        Ok(change)
    }
}
