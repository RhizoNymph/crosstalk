//! The `alerts` consumer group: evaluates the rules on the events the spec
//! names, triages the drafts, and keeps the rules and alerts in step with
//! the events that change them. Built like the gateway pipeline's stages
//! (`crosstalk_gateway::pipeline`): the wiring subscribes [`SUBJECTS`]
//! under [`group`] before anything publishes, then spawns
//! [`AlertsStage::run`] over the subscription.
//!
//! ```text
//! start: AlertRuleMaintenance::embedding_model_changed(embedder's model)
//! ChannelDiscovered, TransmissionConfirmed, DeclaredChannelUnused,
//! TransmissionSuspected, TransmissionClassified
//!     ─▶ every evaluating rule ─RuleEvaluator::evaluate(envelope, context)─▶ drafts ─▶ AlertTriage::triage
//! PolicyChanged (Sanctioned), ChannelPromoted (Sanctioned) ─▶ AlertTriage::channel_sanctioned(channel, envelope time)
//! VerdictSet ─▶ AlertTriage::transmission_judged(transmission, verdict, revision, the verdict's time)
//! TopicVersionReady(v) ─▶ TopicCatalog lineage from v's predecessor, v's topics
//!     ─▶ AlertRuleMaintenance::topic_version_ready
//! ```
//!
//! The store publishes what it decides (`AlertOpened`, `AlertChanged`,
//! `AlertRuleChanged`, `Changed`) after each commit, so the stage itself
//! publishes nothing. A delivery is acked once handled and nacked when a
//! store call fails, so the bus redelivers it; deduplication makes a
//! redelivered detection fold into the alert it opened (its occurrences
//! count the redelivery), and the stage skips an envelope it already
//! handled while it runs.

use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crosstalk_spec::aggregates::alert::{AlertRuleDef, RuleStatus, TriageOutcome};
use crosstalk_spec::aggregates::topic::{Embedding, EmbeddingModel, TopicModelVersion};
use crosstalk_spec::derived::flow::channel::policy::{Policy, PolicyKind};
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::ids::{AlertRuleId, ChannelId, EventId, TopicId, TransmissionId};
use crosstalk_spec::interfaces::l2_transport::{ConsumerGroup, Subscription};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_spec::interfaces::l5_flow::channels::ChannelReads;
use crosstalk_spec::interfaces::l6_analysis::alerts::{
    AlertReadError, AlertReads, AlertRuleMaintenance,
};
use crosstalk_spec::interfaces::l6_analysis::{
    AlertRuleEval, AlertTriage, CatalogError, RuleContext, RuleError, TopicCatalog, TriageError,
};
use crosstalk_spec::interfaces::l8_surface::lists::AlertRuleFilter;
use crosstalk_spec::paging::{PageRequest, PageSize};

use super::eval::RuleEvaluator;

/// The consumer group the stage reads with.
pub const GROUP: &str = "alerts";

/// The group as the bus names it.
pub fn group() -> ConsumerGroup {
    ConsumerGroup(GROUP.to_owned())
}

/// Every subject the stage handles.
pub const SUBJECTS: [Subject; 9] = [
    Subject::ChannelDiscovered,
    Subject::TransmissionConfirmed,
    Subject::DeclaredChannelUnused,
    Subject::TransmissionSuspected,
    Subject::TransmissionClassified,
    Subject::PolicyChanged,
    Subject::ChannelPromoted,
    Subject::VerdictSet,
    Subject::TopicVersionReady,
];

/// How long a failed delivery waits before the bus redelivers it (clamped
/// to the group's retry policy by the bus).
const RETRY_AFTER: Duration = Duration::from_millis(500);

/// How many handled envelope ids the stage remembers to skip redeliveries.
const REMEMBERED: usize = 4096;

/// Why an envelope could not be handled. The delivery is nacked.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StageError {
    #[error("reading the rules: {0:?}")]
    Reads(AlertReadError),
    #[error("triage: {0:?}")]
    Triage(TriageError),
    #[error("rule upkeep: {0:?}")]
    Rules(RuleError),
    #[error("topic catalog: {0:?}")]
    Catalog(CatalogError),
}

/// What handling one envelope did.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Handled {
    /// The triage outcome of each draft, in rule list order.
    pub triaged: Vec<TriageOutcome>,
    /// Alerts suppressed by a sanction or a false-detection verdict.
    pub suppressed: u32,
    /// Rules a topic version or model change made stale or remapped.
    pub rules_changed: Vec<AlertRuleId>,
    /// The envelope was already handled, or carries nothing for this group.
    pub skipped: bool,
}

/// The stage's counters, for health endpoints.
#[derive(Debug, Default)]
pub struct AlertsStats {
    handled: AtomicU64,
    drafts: AtomicU64,
    opened: AtomicU64,
    failed: AtomicU64,
}

/// A reading of [`AlertsStats`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AlertsCounts {
    pub handled: u64,
    pub drafts: u64,
    pub opened: u64,
    pub failed: u64,
}

impl AlertsStats {
    pub fn snapshot(&self) -> AlertsCounts {
        AlertsCounts {
            handled: self.handled.load(Ordering::Relaxed),
            drafts: self.drafts.load(Ordering::Relaxed),
            opened: self.opened.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
        }
    }
}

/// The `alerts` stage over an alert store `A`, a rule context `C` and the
/// topic catalog `K`.
pub struct AlertsStage<A, C, K> {
    store: A,
    context: C,
    catalog: K,
    stats: Arc<AlertsStats>,
    seen: BTreeSet<EventId>,
    order: VecDeque<EventId>,
}

impl<A, C, K> AlertsStage<A, C, K>
where
    A: AlertTriage + AlertRuleMaintenance + AlertReads + Send,
    C: RuleContext + Send,
    K: TopicCatalog + Send + Sync,
{
    pub fn new(store: A, context: C, catalog: K) -> Self {
        Self {
            store,
            context,
            catalog,
            stats: Arc::new(AlertsStats::default()),
            seen: BTreeSet::new(),
            order: VecDeque::new(),
        }
    }

    pub fn stats(&self) -> Arc<AlertsStats> {
        Arc::clone(&self.stats)
    }

    pub fn store(&self) -> &A {
        &self.store
    }

    /// Before evaluating any event: the embedder now uses `model`, so every
    /// current semantic rule embedded with another becomes stale.
    pub async fn start(&mut self, model: &EmbeddingModel) -> Result<Vec<AlertRuleId>, StageError> {
        self.store
            .embedding_model_changed(model)
            .await
            .map_err(StageError::Rules)
    }

    /// The rules that evaluate, as evaluators, in list order.
    async fn evaluators(&self) -> Result<Vec<RuleEvaluator>, StageError> {
        let filter = AlertRuleFilter {
            statuses: vec![RuleStatus::Enabled],
            stale: Some(false),
        };
        let size = PageSize::new(100).map_err(|error| {
            StageError::Reads(AlertReadError::Store {
                reason: format!("page size: {error:?}"),
            })
        })?;
        let mut request = PageRequest { size, after: None };
        let mut rules: Vec<AlertRuleDef> = Vec::new();
        loop {
            let (items, next) = self
                .store
                .rules(&filter, &request)
                .await
                .map_err(StageError::Reads)?
                .into_parts();
            rules.extend(items);
            match next {
                Some(cursor) => request.after = Some(cursor),
                None => break,
            }
        }
        Ok(rules.into_iter().map(RuleEvaluator::new).collect())
    }

    async fn evaluate(&mut self, envelope: &Envelope) -> Result<Handled, StageError> {
        let mut handled = Handled::default();
        for evaluator in self.evaluators().await? {
            let Some(draft) = evaluator.evaluate(envelope, &self.context).await else {
                continue;
            };
            self.stats.drafts.fetch_add(1, Ordering::Relaxed);
            let outcome = self.store.triage(draft).await.map_err(StageError::Triage)?;
            if let TriageOutcome::Opened(alert) = &outcome {
                self.stats.opened.fetch_add(1, Ordering::Relaxed);
                tracing::info!(group = GROUP, alert = %alert.id.ulid_text(), rule = %alert.rule.ulid_text(), "alert opened");
            }
            handled.triaged.push(outcome);
        }
        Ok(handled)
    }

    async fn sanctioned(
        &mut self,
        channel: ChannelId,
        envelope: &Envelope,
    ) -> Result<Handled, StageError> {
        let suppressed = self
            .store
            .channel_sanctioned(channel, envelope.at)
            .await
            .map_err(StageError::Triage)?;
        Ok(Handled {
            suppressed,
            ..Handled::default()
        })
    }

    /// `TopicVersionReady` for `version`: carry the rules over the lineage
    /// from its predecessor.
    async fn version_ready(&mut self, version: TopicModelVersion) -> Result<Handled, StageError> {
        let history = self.catalog.versions().await.map_err(StageError::Catalog)?;
        let predecessor = history
            .versions()
            .iter()
            .map(|info| info.version())
            .take_while(|known| *known != version)
            .last();
        let Some(predecessor) = predecessor else {
            tracing::warn!(
                group = GROUP,
                version = version.0,
                "topic version without a predecessor; no rule to carry over"
            );
            return Ok(Handled {
                skipped: true,
                ..Handled::default()
            });
        };
        let Some(lineage) = self
            .catalog
            .lineage(predecessor)
            .await
            .map_err(StageError::Catalog)?
            .filter(|lineage| lineage.to() == version)
        else {
            tracing::warn!(
                group = GROUP,
                version = version.0,
                predecessor = predecessor.0,
                "no lineage into the ready version; rules left as they are"
            );
            return Ok(Handled {
                skipped: true,
                ..Handled::default()
            });
        };
        let topics = self.topics(version).await?;
        let rules_changed = self
            .store
            .topic_version_ready(&lineage, &topics)
            .await
            .map_err(StageError::Rules)?;
        Ok(Handled {
            rules_changed,
            ..Handled::default()
        })
    }

    async fn topics(&self, version: TopicModelVersion) -> Result<Vec<TopicId>, StageError> {
        let size = PageSize::new(500).map_err(|error| {
            StageError::Catalog(CatalogError::Store {
                reason: format!("page size: {error:?}"),
            })
        })?;
        let mut request = PageRequest { size, after: None };
        let mut topics = Vec::new();
        loop {
            let (items, next) = self
                .catalog
                .topics(version, &request)
                .await
                .map_err(StageError::Catalog)?
                .into_parts();
            topics.extend(items.into_iter().map(|topic| topic.id));
            match next {
                Some(cursor) => request.after = Some(cursor),
                None => return Ok(topics),
            }
        }
    }

    /// Handle one envelope.
    pub async fn handle(&mut self, envelope: &Envelope) -> Result<Handled, StageError> {
        if self.seen.contains(&envelope.id) {
            return Ok(Handled {
                skipped: true,
                ..Handled::default()
            });
        }
        let handled = match &envelope.event {
            BusEvent::Detect(
                DetectEvent::ChannelDiscovered { .. }
                | DetectEvent::TransmissionConfirmed { .. }
                | DetectEvent::DeclaredChannelUnused { .. }
                | DetectEvent::TransmissionSuspected { .. },
            )
            | BusEvent::Insight(InsightEvent::TransmissionClassified { .. }) => {
                self.evaluate(envelope).await?
            }
            BusEvent::Insight(InsightEvent::PolicyChanged {
                channel,
                policy: Policy::Sanctioned(_),
            }) => self.sanctioned(*channel, envelope).await?,
            BusEvent::Detect(DetectEvent::ChannelPromoted {
                channel, policy, ..
            }) if policy.kind == PolicyKind::Sanctioned => {
                self.sanctioned(*channel, envelope).await?
            }
            BusEvent::Detect(DetectEvent::VerdictSet {
                transmission,
                verdict,
                revision,
                at,
                ..
            }) => {
                let suppressed = self
                    .store
                    .transmission_judged(*transmission, *verdict, *revision, *at)
                    .await
                    .map_err(StageError::Triage)?;
                Handled {
                    suppressed,
                    ..Handled::default()
                }
            }
            BusEvent::Insight(InsightEvent::TopicVersionReady { version, .. }) => {
                self.version_ready(*version).await?
            }
            _ => Handled {
                skipped: true,
                ..Handled::default()
            },
        };
        self.remember(envelope.id);
        self.stats.handled.fetch_add(1, Ordering::Relaxed);
        Ok(handled)
    }

    fn remember(&mut self, id: EventId) {
        if self.seen.insert(id) {
            self.order.push_back(id);
        }
        while self.order.len() > REMEMBERED {
            if let Some(oldest) = self.order.pop_front() {
                self.seen.remove(&oldest);
            }
        }
    }

    /// Mark rules stale for `model` (`start`), then handle every delivery
    /// of `subscription` until the bus shuts down. A failed start is
    /// retried before any event is evaluated.
    pub async fn run<S: Subscription>(mut self, mut subscription: S, model: EmbeddingModel) {
        tracing::info!(group = GROUP, model = %model.name, "alerts consumer started");
        loop {
            match self.start(&model).await {
                Ok(stale) => {
                    tracing::info!(
                        group = GROUP,
                        stale = stale.len(),
                        "semantic rules checked against the embedding model"
                    );
                    break;
                }
                Err(error) => {
                    tracing::error!(group = GROUP, error = %error, "marking stale semantic rules failed; retrying");
                    tokio::time::sleep(RETRY_AFTER).await;
                }
            }
        }
        while let Some(next) = subscription.next().await {
            let delivery = match next {
                Ok(delivery) => delivery,
                Err(error) => {
                    tracing::warn!(group = GROUP, error = ?error, "undecodable delivery skipped");
                    continue;
                }
            };
            let event = delivery.envelope.id.ulid_text();
            match self.handle(&delivery.envelope).await {
                Ok(handled) => {
                    tracing::debug!(group = GROUP, event = %event, drafts = handled.triaged.len(), suppressed = handled.suppressed, "envelope handled");
                    if let Err(error) = subscription.ack(delivery.id).await {
                        tracing::warn!(group = GROUP, event = %event, error = ?error, "ack failed; the bus will redeliver");
                    }
                }
                Err(error) => {
                    self.stats.failed.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(group = GROUP, event = %event, attempt = delivery.attempt.get(), error = %error, "handling an envelope failed");
                    if let Err(error) = subscription
                        .nack(delivery.id, RETRY_AFTER, error.to_string())
                        .await
                    {
                        tracing::warn!(group = GROUP, event = %event, error = ?error, "nack failed");
                    }
                }
            }
        }
        tracing::info!(group = GROUP, "alerts consumer stopped");
    }
}

/// Where a rule context reads transmission embeddings: the search index
/// under its current model.
pub trait EmbeddingSource: Send + Sync {
    fn embedding(
        &self,
        transmission: TransmissionId,
    ) -> impl Future<Output = Option<Embedding>> + Send;
}

impl<D, T> EmbeddingSource for crate::search::PgSearchIndex<D, T>
where
    D: AgentDirectory + ChannelDirectory + Send + Sync,
    T: crate::search::TopicAssignments,
{
    async fn embedding(&self, transmission: TransmissionId) -> Option<Embedding> {
        match crate::search::PgSearchIndex::embedding(self, transmission).await {
            Ok(embedding) => embedding,
            Err(error) => {
                tracing::warn!(group = GROUP, transmission = %transmission.ulid_text(), error = ?error, "reading an embedding failed; the rule sees none");
                None
            }
        }
    }
}

/// The wiring's `RuleContext`: a channel's policy from L5's channel reads,
/// read for the channel it resolves to through supersession, and a
/// transmission's embedding from the search index.
#[derive(Debug, Clone)]
pub struct FlowContext<R, I> {
    pub channels: R,
    pub embeddings: I,
}

impl<R, I> RuleContext for FlowContext<R, I>
where
    R: ChannelReads + ChannelDirectory + Sync,
    I: EmbeddingSource,
{
    async fn channel_policy(&self, channel: ChannelId) -> Option<Policy> {
        let canonical = ChannelDirectory::canonical(&self.channels, channel);
        match self.channels.channel(canonical).await {
            Ok(stored) => stored.map(|stored| stored.channel().policy.clone()),
            Err(error) => {
                tracing::warn!(group = GROUP, channel = %canonical.ulid_text(), error = ?error, "reading a channel's policy failed; the rule sees none");
                None
            }
        }
    }

    async fn transmission_embedding(&self, transmission: TransmissionId) -> Option<Embedding> {
        self.embeddings.embedding(transmission).await
    }
}
