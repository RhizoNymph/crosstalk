//! L6 alerts on Postgres: [`PgAlertStore`], one store implementing the
//! spec's `AlertRuleStore`, `AlertTriage`, `AlertRuleMaintenance`,
//! `AlertActions` and `AlertReads` over schema `analysis`
//! (`migrations/0002_alerts.sql`); [`eval`], the rules' evaluation
//! (`AlertRuleEval`); and [`consumer`], the `alerts` bus consumer.
//!
//! - **Writes** run in `SERIALIZABLE` transactions with bounded retries
//!   (`crosstalk_store::retry_serializable`), so every call is one
//!   transaction over the rules, the alerts and triage's verdict copy, and
//!   concurrent calls leave a state some serial order would. A partial
//!   unique index keeps at most one active alert per (rule, subject).
//! - **Decisions** are the spec's own transitions (`AlertRuleDef::update`,
//!   `set_enabled`, `remap`, `embedding_model_changed`,
//!   `CurrentVerdict::observe`, the alert lifecycle), applied to the rows
//!   the transaction read, so the store and the reference decide alike.
//! - **Events.** Every stored change bumps the rule's or alert's revision
//!   by one and appends `AlertRuleChanged` / `AlertOpened` /
//!   `AlertChanged` with `Changed::Rule` / `Changed::Alert` to the outbox
//!   in the same transaction; they reach the [`EventSink`] after the
//!   commit ([`crate::pg::outbox`]). A change that would exhaust a revision
//!   counter is refused as a store failure before anything changes.
//! - **Reads** run in one snapshot each. The alerts list leaves out the
//!   alerts readers do not show (`AlertSubject::shown`) and matches its
//!   channel filter through [`SubjectFacts`], what L5 and L3 know about a
//!   subject at the read.
//! - **Ids** of rules and alerts are ULIDs minted at the time of what they
//!   name (a rule's `at`, a draft's `raised_at`), so newest first is
//!   descending id; a rule id is never in the reserved built-in range.

pub mod consumer;
pub mod eval;
mod facts;
mod reads;
mod rules;
mod store;
mod triage;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};

use crosstalk_spec::aggregates::alert::{
    AlertRuleConfig, AlertRuleDef, BuiltinRule, RuleRevision, RuleStatus,
};
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::ids::mint::{SeededRandom, UlidGenerator};
use crosstalk_spec::ids::{AlertId, AlertRuleId, SinkId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_spec::interfaces::l6_analysis::Embedder;
use crosstalk_spec::support::Timestamp;
use crosstalk_store::{SerializableRetry, retry_serializable};
use sqlx::PgPool;

use crate::pg::outbox::{self, Pending};
use crate::pg::{CursorKey, EventSink, StorageFailure};

pub use facts::{FactsError, FlowFacts, NoFacts, SubjectFacts};
pub use store::Failure;

/// The alert store's configuration.
#[derive(Debug, Clone, PartialEq)]
pub struct AlertStoreConfig {
    pub rules: AlertRuleConfig,
    /// The configured sinks: every sink a rule lists must be one of these.
    pub sinks: BTreeSet<SinkId>,
    /// Each built-in rule's status and sinks when it is first provisioned;
    /// a rule not listed is enabled with no sinks.
    pub builtins: BTreeMap<BuiltinRule, (RuleStatus, Vec<SinkId>)>,
}

/// What the store is built from besides the pool and config.
pub struct AlertStoreParts<E, D, F, S> {
    /// Embeds semantic rules' queries; its model is the one current when
    /// the store is first provisioned.
    pub embedder: E,
    /// Resolves channels through supersession (sanction suppression, the
    /// alerts list's channel filter).
    pub directory: D,
    /// What other layers know about an alert's subject at a read.
    pub facts: F,
    /// Where committed changes are published.
    pub sink: Arc<S>,
    /// Mints rule and alert ids at the times passed in.
    pub ids: UlidGenerator<SeededRandom>,
    pub cursor_key: CursorKey,
    pub retry: SerializableRetry,
}

/// The Postgres alert store. Clones share the pool, the sink and the id
/// generator.
pub struct PgAlertStore<E, D, F, S> {
    pool: PgPool,
    config: Arc<AlertStoreConfig>,
    embedder: Arc<E>,
    directory: D,
    facts: F,
    sink: Arc<S>,
    ids: Arc<Mutex<UlidGenerator<SeededRandom>>>,
    cursor_key: CursorKey,
    retry: SerializableRetry,
}

impl<E, D: Clone, F: Clone, S> Clone for PgAlertStore<E, D, F, S> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            config: Arc::clone(&self.config),
            embedder: Arc::clone(&self.embedder),
            directory: self.directory.clone(),
            facts: self.facts.clone(),
            sink: Arc::clone(&self.sink),
            ids: Arc::clone(&self.ids),
            cursor_key: self.cursor_key,
            retry: self.retry,
        }
    }
}

impl<E, D, F, S> std::fmt::Debug for PgAlertStore<E, D, F, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgAlertStore")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// The earliest time a rule id is minted at: ids stamped in millisecond 0
/// are reserved for built-in rules.
const FIRST_RULE_MILLI: Timestamp = Timestamp::from_micros(1_000);

/// The events of a stored rule change.
pub(crate) fn rule_events(rule: &AlertRuleDef, revision: RuleRevision) -> Vec<BusEvent> {
    vec![
        BusEvent::Insight(InsightEvent::AlertRuleChanged {
            rule: rule.clone(),
            revision,
        }),
        BusEvent::Changed(Changed::Rule(rule.id())),
    ]
}

impl<E, D, F, S> PgAlertStore<E, D, F, S>
where
    E: Embedder + Send + Sync,
    D: AgentDirectory + ChannelDirectory + Send + Sync,
    F: SubjectFacts,
    S: EventSink,
{
    /// The store over `pool` (L6's migrations applied). The first open
    /// provisions every built-in rule from `config` (each publishes
    /// `AlertRuleChanged` at `RuleRevision::CREATED`), makes topic-model
    /// version 0 current and records the embedder's model; a later open
    /// keeps what is stored. Events an earlier run left in the outbox are
    /// published first.
    pub async fn open(
        pool: PgPool,
        config: AlertStoreConfig,
        parts: AlertStoreParts<E, D, F, S>,
    ) -> Result<Self, StorageFailure> {
        let store = Self {
            pool,
            config: Arc::new(config),
            embedder: Arc::new(parts.embedder),
            directory: parts.directory,
            facts: parts.facts,
            sink: parts.sink,
            ids: Arc::new(Mutex::new(parts.ids)),
            cursor_key: parts.cursor_key,
            retry: parts.retry,
        };
        store.flush_outbox().await?;
        let model = crate::pg::codec::to_json("embedding model", &store.embedder.model())?;
        let builtins: Vec<(usize, AlertRuleDef)> = BuiltinRule::ALL
            .into_iter()
            .enumerate()
            .map(|(index, rule)| {
                let (status, sinks) = store
                    .config
                    .builtins
                    .get(&rule)
                    .cloned()
                    .unwrap_or((RuleStatus::Enabled, Vec::new()));
                (index, AlertRuleDef::builtin(rule, status, sinks))
            })
            .collect();
        let pending = retry_serializable(&store.pool, &store.retry, |conn| {
            let model = model.clone();
            let builtins = builtins.clone();
            Box::pin(async move {
                sqlx::query(
                    "INSERT INTO analysis.alert_rule_state (topic_version, topics, model) \
                     VALUES (0, '[]', $1) ON CONFLICT (singleton) DO NOTHING",
                )
                .bind(model)
                .execute(&mut *conn)
                .await?;
                let mut events = Vec::new();
                for (index, rule) in builtins {
                    let json = crate::pg::codec::to_json("alert rule", &rule)
                        .map_err(|error| StorageFailure::from(error).into_tx(|failure| failure))?;
                    let inserted = sqlx::query(
                        "INSERT INTO analysis.alert_rules (id, builtin, definition, revision) \
                         VALUES ($1, $2, $3, 1) ON CONFLICT (id) DO NOTHING",
                    )
                    .bind(crate::pg::codec::id_text(rule.id()))
                    .bind(i16::try_from(index).unwrap_or(i16::MAX))
                    .bind(json)
                    .execute(&mut *conn)
                    .await?;
                    if inserted.rows_affected() == 1 {
                        events.extend(rule_events(&rule, RuleRevision::CREATED));
                    }
                }
                outbox::append(conn, events)
                    .await
                    .map_err(|error| StorageFailure::from(error).into_tx(|failure| failure))
            })
        })
        .await
        .map_err(StorageFailure::from)?;
        store.deliver(pending).await;
        Ok(store)
    }

    /// Publish what an earlier run left in the outbox. Returns how many
    /// events it published.
    pub async fn flush_outbox(&self) -> Result<usize, StorageFailure> {
        Ok(outbox::flush(&self.pool, self.sink.as_ref()).await?)
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub(crate) async fn deliver(&self, pending: Pending) {
        outbox::deliver(&self.pool, self.sink.as_ref(), pending).await;
    }

    /// A fresh rule id stamped `at` (never in the reserved range).
    pub(crate) fn rule_id(&self, at: Timestamp) -> Result<AlertRuleId, StorageFailure> {
        let at = at.max(FIRST_RULE_MILLI);
        let mut ids = self.ids.lock().unwrap_or_else(PoisonError::into_inner);
        ids.mint_at(at).map_err(StorageFailure::Ids)
    }

    /// A fresh alert id stamped `at`.
    pub(crate) fn alert_id(&self, at: Timestamp) -> Result<AlertId, StorageFailure> {
        let mut ids = self.ids.lock().unwrap_or_else(PoisonError::into_inner);
        ids.mint_at(at).map_err(StorageFailure::Ids)
    }
}
