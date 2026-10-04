//! `AlertRuleStore` on [`PgAlertStore`], and `AlertRuleMaintenance`, the
//! rule changes the `alerts` consumer makes.
//!
//! A user rule is resolved inside the transaction that stores it, against
//! the version and topics the consumer last made current
//! (`TopicVersionNotCurrent`, `UnknownTopics`), then its sinks are checked
//! against config (`UnknownSink`). A semantic query is embedded before the
//! transaction (`Embed`), since an embedder call must not run inside one.
//! The checks run in the reference store's order, so both refuse alike.

use crosstalk_spec::aggregates::alert::{
    AlertRule, AlertRuleDef, ContentRule, NotEditable, RuleDefinition, RuleName, SemanticQuery,
    SuppressReason, UserRule,
};
use crosstalk_spec::aggregates::topic::EmbeddingModel;
use crosstalk_spec::aggregates::topic_history::TopicLineage;
use crosstalk_spec::ids::{AlertRuleId, OperatorId, SinkId, TopicId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_spec::interfaces::l6_analysis::alerts::AlertRuleMaintenance;
use crosstalk_spec::interfaces::l6_analysis::{AlertRuleStore, EmbedError, Embedder, RuleError};
use crosstalk_spec::support::{Change, NonEmpty, Timestamp};
use crosstalk_store::{TxError, retry_serializable};

use super::store::{self, RuleState, abort, finish};
use super::{AlertStoreConfig, PgAlertStore, SubjectFacts};
use crate::pg::EventSink;
use crate::pg::outbox::{self, Pending};

/// Resolve `rule` against what is current: a watched-topic rule must name
/// the current version and topics in it, and takes the configured
/// threshold when it has none; a semantic rule takes its embedded query.
fn resolve(
    config: &AlertStoreConfig,
    state: &RuleState,
    rule: UserRule,
    embedded: Option<SemanticQuery>,
) -> Result<RuleDefinition, RuleError> {
    match rule {
        UserRule::WatchedTopic {
            topics,
            remap_threshold,
        } => {
            if topics.version != state.version {
                return Err(RuleError::TopicVersionNotCurrent {
                    requested: topics.version,
                    current: state.version,
                });
            }
            let mut unknown: Vec<TopicId> = Vec::new();
            for topic in topics.topics.iter() {
                if !state.topics.contains(topic) && !unknown.contains(topic) {
                    unknown.push(*topic);
                }
            }
            if let Some(unknown) = NonEmpty::from_vec(unknown) {
                return Err(RuleError::UnknownTopics(unknown));
            }
            Ok(RuleDefinition::WatchedTopic {
                topics,
                remap_threshold: remap_threshold.unwrap_or(config.rules.default_remap_threshold),
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

fn check_sinks(config: &AlertStoreConfig, sinks: &[SinkId]) -> Result<(), RuleError> {
    match sinks.iter().find(|sink| !config.sinks.contains(sink)) {
        Some(sink) => Err(RuleError::UnknownSink(*sink)),
        None => Ok(()),
    }
}

/// Whether an update of kind `update` can apply to `rule`.
fn editable(rule: &AlertRuleDef, update: &UserRule) -> bool {
    match rule.rule() {
        AlertRule::Builtin(_) => false,
        AlertRule::User { content, .. } => matches!(
            (content, update),
            (
                ContentRule::WatchedTopic { .. },
                UserRule::WatchedTopic { .. }
            ) | (
                ContentRule::SemanticQuery { .. },
                UserRule::SemanticQuery { .. }
            )
        ),
    }
}

impl<E, D, F, S> PgAlertStore<E, D, F, S>
where
    E: Embedder + Send + Sync,
    D: AgentDirectory + ChannelDirectory + Send + Sync,
    F: SubjectFacts,
    S: EventSink,
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

    /// Apply `change` to every stored rule; store and announce the ones it
    /// changed, then `after`. Returns them ascending.
    async fn maintain(
        &self,
        change: impl Fn(&mut AlertRuleDef) -> bool + Clone + Send + Sync + 'static,
        after: Maintenance,
    ) -> Result<Vec<AlertRuleId>, RuleError> {
        let (changed, pending) = finish(
            retry_serializable(&self.pool, &self.retry, |conn| {
                let change = change.clone();
                let after = after.clone();
                Box::pin(async move {
                    if let Maintenance::Version { lineage, .. } = &after {
                        let state = store::rule_state(conn).await.map_err(abort)?;
                        if lineage.to() <= state.version {
                            return Ok((Vec::new(), Pending::default()));
                        }
                    }
                    let mut changed = Vec::new();
                    let mut events = Vec::new();
                    for (mut rule, revision) in store::rules(conn).await.map_err(abort)? {
                        if change(&mut rule) {
                            events.extend(
                                store::save_rule(conn, &rule, Some(revision))
                                    .await
                                    .map_err(abort)?,
                            );
                            changed.push(rule.id());
                        }
                    }
                    match &after {
                        Maintenance::Version { lineage, topics } => {
                            store::set_rule_version(conn, lineage.to(), topics)
                                .await
                                .map_err(abort)?;
                        }
                        Maintenance::Model(model) => {
                            store::set_model(conn, model).await.map_err(abort)?;
                        }
                    }
                    let pending = outbox::append(conn, events).await.map_err(abort)?;
                    Ok((changed, pending))
                })
            })
            .await,
        )?;
        self.deliver(pending).await;
        Ok(changed)
    }
}

/// What a maintenance call records once its rules are stored.
#[derive(Debug, Clone)]
enum Maintenance {
    Version {
        lineage: TopicLineage,
        topics: Vec<TopicId>,
    },
    Model(EmbeddingModel),
}

impl<E, D, F, S> AlertRuleMaintenance for PgAlertStore<E, D, F, S>
where
    E: Embedder + Send + Sync,
    D: AgentDirectory + ChannelDirectory + Send + Sync,
    F: SubjectFacts,
    S: EventSink,
{
    /// A version not newer than the current one changes nothing (a
    /// redelivery).
    async fn topic_version_ready(
        &mut self,
        lineage: &TopicLineage,
        topics: &[TopicId],
    ) -> Result<Vec<AlertRuleId>, RuleError> {
        let remap_over = lineage.clone();
        self.maintain(
            move |rule| {
                rule.remap(&remap_over)
                    .is_ok_and(|change| change == Change::Applied)
            },
            Maintenance::Version {
                lineage: lineage.clone(),
                topics: topics.to_vec(),
            },
        )
        .await
    }

    async fn embedding_model_changed(
        &mut self,
        model: &EmbeddingModel,
    ) -> Result<Vec<AlertRuleId>, RuleError> {
        let current = model.clone();
        self.maintain(
            move |rule| rule.embedding_model_changed(&current) == Change::Applied,
            Maintenance::Model(model.clone()),
        )
        .await
    }
}

impl<E, D, F, S> AlertRuleStore for PgAlertStore<E, D, F, S>
where
    E: Embedder + Send + Sync,
    D: AgentDirectory + ChannelDirectory + Send + Sync,
    F: SubjectFacts,
    S: EventSink,
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
        let id = self.rule_id(at).map_err(store::fail)?;
        let config = std::sync::Arc::clone(&self.config);
        let pending = finish(
            retry_serializable(&self.pool, &self.retry, |conn| {
                let (config, name, rule, sinks, embedded) = (
                    std::sync::Arc::clone(&config),
                    name.clone(),
                    rule.clone(),
                    sinks.clone(),
                    embedded.clone(),
                );
                Box::pin(async move {
                    let state = store::rule_state(conn).await.map_err(abort)?;
                    let definition =
                        resolve(&config, &state, rule, embedded).map_err(TxError::Abort)?;
                    check_sinks(&config, &sinks).map_err(TxError::Abort)?;
                    let rule = AlertRuleDef::user(id, name, (by, at), definition, sinks).map_err(
                        |error| {
                            TxError::Abort(RuleError::Store {
                                reason: format!("minted a reserved rule id: {error:?}"),
                            })
                        },
                    )?;
                    let events = store::save_rule(conn, &rule, None).await.map_err(abort)?;
                    outbox::append(conn, events).await.map_err(abort)
                })
            })
            .await,
        )?;
        self.deliver(pending).await;
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
        // Checked before embedding, so a refused update calls no embedder;
        // checked again in the transaction, which decides.
        {
            let mut conn = self.pool.acquire().await.map_err(store::fail)?;
            let (stored, _) = store::rule(&mut conn, id)
                .await
                .map_err(store::fail)?
                .ok_or(RuleError::UnknownRule(id))?;
            if !editable(&stored, &rule) {
                return Err(RuleError::NotEditable(NotEditable { rule: id }));
            }
        }
        let embedded = self.embed_if_semantic(&rule).await?;
        let config = std::sync::Arc::clone(&self.config);
        let (change, pending) = finish(
            retry_serializable(&self.pool, &self.retry, |conn| {
                let (config, name, rule, sinks, embedded) = (
                    std::sync::Arc::clone(&config),
                    name.clone(),
                    rule.clone(),
                    sinks.clone(),
                    embedded.clone(),
                );
                Box::pin(async move {
                    let (mut stored, revision) = store::rule(conn, id)
                        .await
                        .map_err(abort)?
                        .ok_or(TxError::Abort(RuleError::UnknownRule(id)))?;
                    let state = store::rule_state(conn).await.map_err(abort)?;
                    let definition =
                        resolve(&config, &state, rule, embedded).map_err(TxError::Abort)?;
                    check_sinks(&config, &sinks).map_err(TxError::Abort)?;
                    let change = stored
                        .update(name, definition, sinks)
                        .map_err(|error| TxError::Abort(RuleError::NotEditable(error)))?;
                    let events = match change {
                        Change::Applied => store::save_rule(conn, &stored, Some(revision))
                            .await
                            .map_err(abort)?,
                        Change::Unchanged => Vec::new(),
                    };
                    let pending = outbox::append(conn, events).await.map_err(abort)?;
                    Ok((change, pending))
                })
            })
            .await,
        )?;
        self.deliver(pending).await;
        Ok(change)
    }

    async fn set_enabled(
        &mut self,
        id: AlertRuleId,
        enabled: bool,
        _by: OperatorId,
        at: Timestamp,
    ) -> Result<Change, RuleError> {
        let (change, pending) = finish(
            retry_serializable(&self.pool, &self.retry, |conn| {
                Box::pin(async move {
                    let (mut stored, revision) = store::rule(conn, id)
                        .await
                        .map_err(abort)?
                        .ok_or(TxError::Abort(RuleError::UnknownRule(id)))?;
                    let change = stored
                        .set_enabled(enabled)
                        .map_err(|stale| TxError::Abort(RuleError::Stale(stale)))?;
                    if change == Change::Unchanged {
                        return Ok((change, Pending::default()));
                    }
                    if revision.next().is_none() {
                        return Err(abort(crate::pg::StorageFailure::RevisionExhausted(
                            format!("rule {id:?}"),
                        )));
                    }
                    let mut events = Vec::new();
                    if !enabled {
                        let active = store::active(conn, store::Active::Rule(id))
                            .await
                            .map_err(abort)?;
                        events.extend(
                            store::suppress(conn, active, SuppressReason::RuleDisabled, at)
                                .await
                                .map_err(abort)?,
                        );
                    }
                    events.extend(
                        store::save_rule(conn, &stored, Some(revision))
                            .await
                            .map_err(abort)?,
                    );
                    let pending = outbox::append(conn, events).await.map_err(abort)?;
                    Ok((change, pending))
                })
            })
            .await,
        )?;
        self.deliver(pending).await;
        Ok(change)
    }
}
