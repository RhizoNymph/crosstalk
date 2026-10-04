//! `check_alert_rule_store` and `check_alert_triage`: the alert store
//! against the reference.
//!
//! Rule and alert ids are the store's to assign, so the harness maps the
//! subject's ids to the reference's by creation order (built-in rule ids
//! are fixed and map to themselves) and compares everything with the
//! subject's ids translated. After every operation it also checks
//! `analysis.triage.one-active-per-key`: at most one open or acknowledged
//! alert per (rule, subject).

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU16;
use std::sync::Arc;

use proptest::prelude::*;

use crosstalk_spec::aggregates::alert::{
    Alert, AlertDraft, AlertRule, AlertRuleConfig, AlertRuleDef, AlertSubject, BuiltinRule,
    NotEditable, RuleName, RuleQueryText, RuleStatus, StaleRule, TriageOutcome, UserRule,
    WatchedTopics,
};
use crosstalk_spec::aggregates::topic::{EmbeddingModel, Topic, TopicModelVersion};
use crosstalk_spec::aggregates::topic_history::TopicLineage;
use crosstalk_spec::derived::flow::verdict::{Verdict, VerdictRevision};
use crosstalk_spec::ids::{AlertId, AlertRuleId, ChannelId, OperatorId, TopicId};
use crosstalk_spec::interfaces::l6_analysis::{
    AlertRuleStore, AlertTriage, RuleError, TriageError,
};
use crosstalk_spec::support::{Change, NonEmpty, Timestamp};

use super::search::harness_model;
use crate::analysis::alerts::triage::AlertActionError;
use crate::analysis::alerts::{AlertStoreConfig, CommitRefused, InMemoryAlertStore, is_active};
use crate::analysis::aliases::{AliasError, StaticDirectory};
use crate::analysis::fakes::{FakeEmbedder, fake_model};
use crate::analysis::lineage::lineage_between;
use crate::analysis::support::{Clock, ManualClock};
use crate::model::build::{
    channel, operator, raw, similarity, sink, topic, transmission, ts, unit,
};
use crate::model::{Divergence, HarnessConfig, ModelMismatch, holds, run, same};

/// An alert store with the `alerts` consumer's rule changes, the surface's
/// acknowledge and resolve, full reads, the supersession table it resolves
/// through and its clock.
pub trait AlertStoreSubject: AlertRuleStore + AlertTriage {
    fn topic_version_ready(
        &self,
        lineage: &TopicLineage,
        topics: Vec<TopicId>,
    ) -> impl Future<Output = Result<Vec<AlertRuleId>, CommitRefused>> + Send;

    fn embedding_model_changed(
        &self,
        model: &EmbeddingModel,
    ) -> impl Future<Output = Result<Vec<AlertRuleId>, CommitRefused>> + Send;

    fn acknowledge(
        &self,
        alert: AlertId,
        by: OperatorId,
        at: Timestamp,
    ) -> impl Future<Output = Result<Change, AlertActionError>> + Send;

    fn resolve(
        &self,
        alert: AlertId,
        by: OperatorId,
        at: Timestamp,
        note: Option<String>,
    ) -> impl Future<Output = Result<Change, AlertActionError>> + Send;

    /// Every rule, in any order.
    fn rules(&self) -> impl Future<Output = Vec<AlertRuleDef>> + Send;

    /// Every alert, in any order.
    fn alerts(&self) -> impl Future<Output = Vec<Alert>> + Send;

    fn supersede(&self, channel: ChannelId, by: ChannelId) -> Result<(), AliasError>;

    /// Set the clock suppressions are stamped with.
    fn set_now(&self, at: Timestamp);
}

/// What `make` builds a subject from.
#[derive(Debug, Clone)]
pub struct AlertWorld {
    pub config: AlertStoreConfig,
    pub embedder: FakeEmbedder,
}

/// The reference alert store with its directory and clock.
#[derive(Clone)]
pub struct ReferenceAlerts {
    pub store: InMemoryAlertStore<FakeEmbedder, StaticDirectory>,
    pub directory: StaticDirectory,
    pub clock: ManualClock,
}

impl ReferenceAlerts {
    pub fn new(world: AlertWorld) -> Self {
        let directory = StaticDirectory::new();
        let clock = ManualClock::new(ts(0));
        let shared: Arc<dyn Clock> = Arc::new(clock.clone());
        Self {
            store: InMemoryAlertStore::new(world.config, world.embedder, directory.clone(), shared),
            directory,
            clock,
        }
    }
}

impl AlertRuleStore for ReferenceAlerts {
    fn create(
        &mut self,
        name: RuleName,
        rule: UserRule,
        sinks: Vec<crosstalk_spec::ids::SinkId>,
        by: OperatorId,
        at: Timestamp,
    ) -> impl Future<Output = Result<AlertRuleId, RuleError>> + Send {
        self.store.create(name, rule, sinks, by, at)
    }

    fn update(
        &mut self,
        id: AlertRuleId,
        name: RuleName,
        rule: UserRule,
        sinks: Vec<crosstalk_spec::ids::SinkId>,
        by: OperatorId,
    ) -> impl Future<Output = Result<Change, RuleError>> + Send {
        self.store.update(id, name, rule, sinks, by)
    }

    fn set_enabled(
        &mut self,
        id: AlertRuleId,
        enabled: bool,
        by: OperatorId,
    ) -> impl Future<Output = Result<Change, RuleError>> + Send {
        self.store.set_enabled(id, enabled, by)
    }
}

impl AlertTriage for ReferenceAlerts {
    fn triage(
        &mut self,
        draft: AlertDraft,
    ) -> impl Future<Output = Result<TriageOutcome, TriageError>> + Send {
        self.store.triage(draft)
    }

    fn channel_sanctioned(
        &mut self,
        channel: ChannelId,
    ) -> impl Future<Output = Result<u32, TriageError>> + Send {
        self.store.channel_sanctioned(channel)
    }

    fn rule_disabled(
        &mut self,
        rule: AlertRuleId,
    ) -> impl Future<Output = Result<u32, TriageError>> + Send {
        self.store.rule_disabled(rule)
    }

    fn transmission_judged(
        &mut self,
        transmission: crosstalk_spec::ids::TransmissionId,
        verdict: Option<Verdict>,
        revision: VerdictRevision,
    ) -> impl Future<Output = Result<u32, TriageError>> + Send {
        self.store
            .transmission_judged(transmission, verdict, revision)
    }
}

impl AlertStoreSubject for ReferenceAlerts {
    async fn topic_version_ready(
        &self,
        lineage: &TopicLineage,
        topics: Vec<TopicId>,
    ) -> Result<Vec<AlertRuleId>, CommitRefused> {
        self.store.topic_version_ready(lineage, topics)
    }

    async fn embedding_model_changed(
        &self,
        model: &EmbeddingModel,
    ) -> Result<Vec<AlertRuleId>, CommitRefused> {
        self.store.embedding_model_changed(model)
    }

    async fn acknowledge(
        &self,
        alert: AlertId,
        by: OperatorId,
        at: Timestamp,
    ) -> Result<Change, AlertActionError> {
        self.store.acknowledge(alert, by, at)
    }

    async fn resolve(
        &self,
        alert: AlertId,
        by: OperatorId,
        at: Timestamp,
        note: Option<String>,
    ) -> Result<Change, AlertActionError> {
        self.store.resolve(alert, by, at, note)
    }

    async fn rules(&self) -> Vec<AlertRuleDef> {
        self.store.all_rules()
    }

    async fn alerts(&self) -> Vec<Alert> {
        self.store.all_alerts()
    }

    fn supersede(&self, channel: ChannelId, by: ChannelId) -> Result<(), AliasError> {
        self.directory.supersede(channel, by)
    }

    fn set_now(&self, at: Timestamp) {
        self.clock.set(at);
    }
}

/// The world every alert harness case starts from: sinks 1 and 2
/// configured, every built-in enabled, remap threshold 0.8, an embedder of
/// model "fake" refusing texts over 40 characters.
pub fn alert_world() -> Option<AlertWorld> {
    Some(AlertWorld {
        config: AlertStoreConfig {
            rules: AlertRuleConfig {
                default_remap_threshold: similarity(0.8)?,
            },
            sinks: BTreeSet::from([sink(1), sink(2)]),
            builtins: BTreeMap::new(),
        },
        embedder: FakeEmbedder::new(fake_model("fake", NonZeroU16::new(8)?), 40),
    })
}

const WORDS: [&str; 5] = ["wiki", "deploy", "token", "page", "build"];

#[derive(Debug, Clone)]
pub enum AlertOp {
    CreateWatched {
        topics: Vec<u8>,
        stale_version: bool,
        threshold: Option<u8>,
        sinks: Vec<u64>,
    },
    CreateSemantic {
        words: Vec<u8>,
        sinks: Vec<u64>,
    },
    UpdateWatched {
        rule: u8,
        topics: Vec<u8>,
        sinks: Vec<u64>,
    },
    UpdateSemantic {
        rule: u8,
        words: Vec<u8>,
    },
    SetEnabled {
        rule: u8,
        enabled: bool,
    },
    Triage {
        rule: u8,
        subject: u8,
        n: u64,
        raised: u64,
    },
    Sanctioned {
        channel: u64,
    },
    RuleDisabled {
        rule: u8,
    },
    Judged {
        transmission: u64,
        verdict: u8,
        revision: u32,
    },
    VersionReady {
        topics: Vec<(i8, i8, i8)>,
    },
    ModelChanged {
        other: bool,
    },
    Acknowledge {
        alert: u8,
    },
    Resolve {
        alert: u8,
    },
    Supersede {
        channel: u64,
        by: u64,
    },
    Tick {
        micros: u64,
    },
}

fn rule_ops() -> impl Strategy<Value = AlertOp> {
    let direction = (-2i8..3, -2i8..3, -2i8..3);
    prop_oneof![
        4 => (prop::collection::vec(0u8..4, 1..3), prop::bool::weighted(0.1), prop::option::of(0u8..10), prop::collection::vec(0u64..4, 0..2))
            .prop_map(|(topics, stale_version, threshold, sinks)| AlertOp::CreateWatched { topics, stale_version, threshold, sinks }),
        2 => (prop::collection::vec(0u8..5, 1..12), prop::collection::vec(0u64..3, 0..2))
            .prop_map(|(words, sinks)| AlertOp::CreateSemantic { words, sinks }),
        2 => (0u8..9, prop::collection::vec(0u8..4, 1..3), prop::collection::vec(0u64..3, 0..2))
            .prop_map(|(rule, topics, sinks)| AlertOp::UpdateWatched { rule, topics, sinks }),
        1 => (0u8..9, prop::collection::vec(0u8..5, 1..4)).prop_map(|(rule, words)| AlertOp::UpdateSemantic { rule, words }),
        3 => (0u8..9, any::<bool>()).prop_map(|(rule, enabled)| AlertOp::SetEnabled { rule, enabled }),
        3 => (0u8..9, 0u8..3, 0u64..3, 0u64..100).prop_map(|(rule, subject, n, raised)| AlertOp::Triage { rule, subject, n, raised }),
        2 => prop::collection::vec(direction, 0..4).prop_map(|topics| AlertOp::VersionReady { topics }),
        1 => any::<bool>().prop_map(|other| AlertOp::ModelChanged { other }),
        1 => (1u64..50).prop_map(|micros| AlertOp::Tick { micros }),
    ]
}

fn triage_ops() -> impl Strategy<Value = AlertOp> {
    prop_oneof![
        6 => (0u8..9, 0u8..3, 0u64..3, 0u64..100).prop_map(|(rule, subject, n, raised)| AlertOp::Triage { rule, subject, n, raised }),
        2 => (0u64..3).prop_map(|channel| AlertOp::Sanctioned { channel }),
        1 => (0u8..9).prop_map(|rule| AlertOp::RuleDisabled { rule }),
        2 => (0u64..3, 0u8..3, 1u32..4).prop_map(|(transmission, verdict, revision)| AlertOp::Judged { transmission, verdict, revision }),
        2 => (0u8..8).prop_map(|alert| AlertOp::Acknowledge { alert }),
        2 => (0u8..8).prop_map(|alert| AlertOp::Resolve { alert }),
        1 => (0u64..3, 0u64..3).prop_map(|(channel, by)| AlertOp::Supersede { channel, by }),
        2 => (0u8..9, any::<bool>()).prop_map(|(rule, enabled)| AlertOp::SetEnabled { rule, enabled }),
        1 => (prop::collection::vec(0u8..4, 1..3), Just(false), Just(None), Just(Vec::new()))
            .prop_map(|(topics, stale_version, threshold, sinks)| AlertOp::CreateWatched { topics, stale_version, threshold, sinks }),
        1 => prop::collection::vec((-2i8..3, -2i8..3, -2i8..3), 0..4).prop_map(|topics| AlertOp::VersionReady { topics }),
        1 => (1u64..50).prop_map(|micros| AlertOp::Tick { micros }),
    ]
}

/// The subject's ids mapped to the reference's.
#[derive(Debug, Default)]
struct Ids {
    rules: Vec<(AlertRuleId, AlertRuleId)>,
    alerts: Vec<(AlertId, AlertId)>,
}

impl Ids {
    fn rule(&self, theirs: AlertRuleId) -> AlertRuleId {
        self.rules
            .iter()
            .find(|(subject, _)| *subject == theirs)
            .map_or(theirs, |(_, ours)| *ours)
    }

    fn alert(&self, theirs: AlertId) -> AlertId {
        self.alerts
            .iter()
            .find(|(subject, _)| *subject == theirs)
            .map_or(theirs, |(_, ours)| *ours)
    }

    /// The rule a generated index names: a built-in, a created rule, or an
    /// id neither store has.
    fn pick_rule(&self, index: u8) -> (AlertRuleId, AlertRuleId) {
        let index = usize::from(index);
        match BuiltinRule::ALL.get(index) {
            Some(rule) => (rule.id(), rule.id()),
            None => self
                .rules
                .get(index - BuiltinRule::ALL.len())
                .copied()
                .unwrap_or_else(|| {
                    let unknown = AlertRuleId::from_ulid(raw(999));
                    (unknown, unknown)
                }),
        }
    }

    fn pick_alert(&self, index: u8) -> (AlertId, AlertId) {
        self.alerts
            .get(usize::from(index))
            .copied()
            .unwrap_or_else(|| {
                let unknown = AlertId::from_ulid(raw(999));
                (unknown, unknown)
            })
    }

    fn translate_alert(&self, alert: &Alert) -> Alert {
        Alert {
            id: self.alert(alert.id),
            rule: self.rule(alert.rule),
            ..alert.clone()
        }
    }

    fn translate_rule_error(&self, error: RuleError) -> RuleError {
        match error {
            RuleError::UnknownRule(id) => RuleError::UnknownRule(self.rule(id)),
            RuleError::NotEditable(NotEditable { rule }) => RuleError::NotEditable(NotEditable {
                rule: self.rule(rule),
            }),
            RuleError::Stale(StaleRule { rule }) => RuleError::Stale(StaleRule {
                rule: self.rule(rule),
            }),
            other => other,
        }
    }

    fn translate_action(
        &self,
        result: Result<Change, AlertActionError>,
    ) -> Result<Change, AlertActionError> {
        result.map_err(|error| match error {
            AlertActionError::UnknownAlert(id) => AlertActionError::UnknownAlert(self.alert(id)),
            AlertActionError::NotActive(id) => AlertActionError::NotActive(self.alert(id)),
            AlertActionError::NotAcknowledged(id) => {
                AlertActionError::NotAcknowledged(self.alert(id))
            }
            other => other,
        })
    }
}

/// A rule without its id, as compared.
fn rule_view(
    rule: &AlertRuleDef,
    ids: &Ids,
) -> (
    AlertRuleId,
    AlertRule,
    RuleStatus,
    Vec<crosstalk_spec::ids::SinkId>,
) {
    (
        ids.rule(rule.id()),
        rule.rule().clone(),
        rule.status,
        rule.sinks.clone(),
    )
}

/// The topics of the version rules currently name, and its number.
#[derive(Debug, Clone)]
struct Version {
    number: TopicModelVersion,
    topics: Vec<Topic>,
}

fn topic_id_of(version: TopicModelVersion, k: u8) -> TopicId {
    TopicId::from_ulid(raw(u64::from(version.0) * 16 + u64::from(k)))
}

fn watched(version: TopicModelVersion, topics: &[u8], threshold: Option<u8>) -> Option<UserRule> {
    Some(UserRule::WatchedTopic {
        topics: WatchedTopics {
            version,
            topics: NonEmpty::from_vec(topics.iter().map(|k| topic_id_of(version, *k)).collect())?,
        },
        remap_threshold: threshold.and_then(|tenths| similarity(f32::from(tenths) / 10.0)),
    })
}

fn semantic(words: &[u8]) -> Option<UserRule> {
    let text = words
        .iter()
        .map(|word| WORDS[usize::from(*word) % WORDS.len()])
        .collect::<Vec<_>>()
        .join(" ");
    Some(UserRule::SemanticQuery {
        text: RuleQueryText::new(&text).ok()?,
        threshold: similarity(0.5)?,
    })
}

fn subject_of(kind: u8, n: u64) -> AlertSubject {
    match kind {
        0 => AlertSubject::Channel(channel(n)),
        1 => AlertSubject::Transmission(transmission(n)),
        _ => AlertSubject::Agent(crate::model::build::agent(n)),
    }
}

fn verdict_of(n: u8) -> Option<Verdict> {
    match n {
        0 => None,
        1 => Some(Verdict::Genuine),
        _ => Some(Verdict::FalseDetection),
    }
}

/// Apply one operation to both stores and compare the outcomes, keeping
/// the id maps.
async fn step_both<S: AlertStoreSubject>(
    step: usize,
    op: &AlertOp,
    subject: &mut S,
    reference: &mut ReferenceAlerts,
    ids: &mut Ids,
    version: &mut Version,
    now: Timestamp,
) -> Result<(), Divergence> {
    let label = format!("{op:?}");
    let name = RuleName::new("rule").map_err(|_| Divergence::new(step, "rule name"))?;
    match op {
        AlertOp::CreateWatched {
            topics,
            stale_version,
            threshold,
            sinks,
        } => {
            let named = if *stale_version {
                TopicModelVersion(version.number.0 + 7)
            } else {
                version.number
            };
            let Some(rule) = watched(named, topics, *threshold) else {
                return Ok(());
            };
            let sinks: Vec<_> = sinks.iter().copied().map(sink).collect();
            let theirs = subject
                .create(name.clone(), rule.clone(), sinks.clone(), operator(1), now)
                .await;
            let ours = reference.create(name, rule, sinks, operator(1), now).await;
            created(step, &label, theirs, ours, ids)
        }
        AlertOp::CreateSemantic { words, sinks } => {
            let Some(rule) = semantic(words) else {
                return Ok(());
            };
            let sinks: Vec<_> = sinks.iter().copied().map(sink).collect();
            let theirs = subject
                .create(name.clone(), rule.clone(), sinks.clone(), operator(1), now)
                .await;
            let ours = reference.create(name, rule, sinks, operator(1), now).await;
            created(step, &label, theirs, ours, ids)
        }
        AlertOp::UpdateWatched {
            rule,
            topics,
            sinks,
        } => {
            let (their_id, our_id) = ids.pick_rule(*rule);
            let Some(definition) = watched(version.number, topics, None) else {
                return Ok(());
            };
            let sinks: Vec<_> = sinks.iter().copied().map(sink).collect();
            let theirs = subject
                .update(
                    their_id,
                    name.clone(),
                    definition.clone(),
                    sinks.clone(),
                    operator(2),
                )
                .await;
            let ours = reference
                .update(our_id, name, definition, sinks, operator(2))
                .await;
            same(
                step,
                &label,
                &theirs.map_err(|error| ids.translate_rule_error(error)),
                &ours,
            )
        }
        AlertOp::UpdateSemantic { rule, words } => {
            let (their_id, our_id) = ids.pick_rule(*rule);
            let Some(definition) = semantic(words) else {
                return Ok(());
            };
            let theirs = subject
                .update(
                    their_id,
                    name.clone(),
                    definition.clone(),
                    Vec::new(),
                    operator(2),
                )
                .await;
            let ours = reference
                .update(our_id, name, definition, Vec::new(), operator(2))
                .await;
            same(
                step,
                &label,
                &theirs.map_err(|error| ids.translate_rule_error(error)),
                &ours,
            )
        }
        AlertOp::SetEnabled { rule, enabled } => {
            let (their_id, our_id) = ids.pick_rule(*rule);
            let theirs = subject.set_enabled(their_id, *enabled, operator(2)).await;
            let ours = reference.set_enabled(our_id, *enabled, operator(2)).await;
            same(
                step,
                &label,
                &theirs.map_err(|error| ids.translate_rule_error(error)),
                &ours,
            )
        }
        AlertOp::Triage {
            rule,
            subject: kind,
            n,
            raised,
        } => {
            let (their_id, our_id) = ids.pick_rule(*rule);
            let about = subject_of(*kind, *n);
            let theirs = subject
                .triage(AlertDraft {
                    rule: their_id,
                    subject: about,
                    raised_at: ts(*raised),
                })
                .await;
            let ours = reference
                .triage(AlertDraft {
                    rule: our_id,
                    subject: about,
                    raised_at: ts(*raised),
                })
                .await;
            if let (Ok(TriageOutcome::Opened(their_alert)), Ok(TriageOutcome::Opened(our_alert))) =
                (&theirs, &ours)
            {
                ids.alerts.push((their_alert.id, our_alert.id));
            }
            let theirs = theirs.map(|outcome| match outcome {
                TriageOutcome::Opened(alert) => TriageOutcome::Opened(ids.translate_alert(&alert)),
                TriageOutcome::Deduplicated { into } => TriageOutcome::Deduplicated {
                    into: ids.alert(into),
                },
                other => other,
            });
            same(step, &label, &theirs, &ours)
        }
        AlertOp::Sanctioned { channel: c } => {
            let theirs = subject.channel_sanctioned(channel(*c)).await;
            same(
                step,
                &label,
                &theirs,
                &reference.channel_sanctioned(channel(*c)).await,
            )
        }
        AlertOp::RuleDisabled { rule } => {
            let (their_id, our_id) = ids.pick_rule(*rule);
            let theirs = subject.rule_disabled(their_id).await;
            same(
                step,
                &label,
                &theirs,
                &reference.rule_disabled(our_id).await,
            )
        }
        AlertOp::Judged {
            transmission: n,
            verdict,
            revision,
        } => {
            let revision = VerdictRevision::new(
                std::num::NonZeroU32::new(*revision).unwrap_or(std::num::NonZeroU32::MIN),
            );
            let theirs = subject
                .transmission_judged(transmission(*n), verdict_of(*verdict), revision)
                .await;
            let ours = reference
                .transmission_judged(transmission(*n), verdict_of(*verdict), revision)
                .await;
            same(step, &label, &theirs, &ours)
        }
        AlertOp::VersionReady { topics } => {
            let next = TopicModelVersion(version.number.0 + 1);
            let model = harness_model();
            let made: Vec<Topic> = topics
                .iter()
                .enumerate()
                .filter_map(|(k, (x, y, z))| {
                    let centroid = unit(&model, f32::from(*x), f32::from(*y), f32::from(*z))?;
                    Some(topic(
                        topic_id_of(next, u8::try_from(k).ok()?),
                        next,
                        centroid,
                        now,
                    ))
                })
                .collect();
            let older: Vec<&Topic> = version.topics.iter().collect();
            let newer: Vec<&Topic> = made.iter().collect();
            let Some(floor) = similarity(0.5) else {
                return Ok(());
            };
            let Ok(lineage) = lineage_between(version.number, &older, next, &newer, floor) else {
                return Err(Divergence::new(
                    step,
                    "the harness built an invalid lineage",
                ));
            };
            let topic_ids: Vec<TopicId> = made.iter().map(|one| one.id).collect();
            let theirs = subject
                .topic_version_ready(&lineage, topic_ids.clone())
                .await;
            let ours = reference.topic_version_ready(&lineage, topic_ids).await;
            *version = Version {
                number: next,
                topics: made,
            };
            same(
                step,
                &label,
                &theirs.map(|changed| {
                    changed
                        .into_iter()
                        .map(|id| ids.rule(id))
                        .collect::<Vec<_>>()
                }),
                &ours,
            )
        }
        AlertOp::ModelChanged { other } => {
            let name = if *other { "fake-2" } else { "fake" };
            let Some(dimension) = NonZeroU16::new(8) else {
                return Ok(());
            };
            let model = fake_model(name, dimension);
            let theirs = subject.embedding_model_changed(&model).await;
            let ours = reference.embedding_model_changed(&model).await;
            same(
                step,
                &label,
                &theirs.map(|changed| {
                    changed
                        .into_iter()
                        .map(|id| ids.rule(id))
                        .collect::<Vec<_>>()
                }),
                &ours,
            )
        }
        AlertOp::Acknowledge { alert } => {
            let (their_id, our_id) = ids.pick_alert(*alert);
            let theirs =
                ids.translate_action(subject.acknowledge(their_id, operator(3), now).await);
            same(
                step,
                &label,
                &theirs,
                &reference.acknowledge(our_id, operator(3), now).await,
            )
        }
        AlertOp::Resolve { alert } => {
            let (their_id, our_id) = ids.pick_alert(*alert);
            let note = Some("done".to_owned());
            let theirs = ids.translate_action(
                subject
                    .resolve(their_id, operator(3), now, note.clone())
                    .await,
            );
            same(
                step,
                &label,
                &theirs,
                &reference.resolve(our_id, operator(3), now, note).await,
            )
        }
        AlertOp::Supersede { channel: c, by } => {
            let theirs = subject.supersede(channel(*c), channel(*by));
            same(
                step,
                &label,
                &theirs,
                &reference.supersede(channel(*c), channel(*by)),
            )
        }
        AlertOp::Tick { .. } => Ok(()),
    }
}

fn created(
    step: usize,
    label: &str,
    theirs: Result<AlertRuleId, RuleError>,
    ours: Result<AlertRuleId, RuleError>,
    ids: &mut Ids,
) -> Result<(), Divergence> {
    match (&theirs, &ours) {
        (Ok(their_id), Ok(our_id)) => {
            ids.rules.push((*their_id, *our_id));
            Ok(())
        }
        _ => same(
            step,
            label,
            &theirs
                .map(|_| ())
                .map_err(|error| ids.translate_rule_error(error)),
            &ours.map(|_| ()),
        ),
    }
}

/// Run `ops` against a subject from `make` and the reference.
fn check_alerts<S, F, Fut>(
    harness: HarnessConfig,
    make: F,
    strategy: impl Strategy<Value = Vec<AlertOp>>,
) -> Result<(), ModelMismatch>
where
    S: AlertStoreSubject,
    F: Fn(AlertWorld) -> Fut,
    Fut: Future<Output = S>,
{
    let world = alert_world().ok_or_else(|| ModelMismatch::Setup("alert world".to_owned()))?;
    run(harness, strategy, |runtime, ops| {
        runtime.block_on(async {
            let mut subject = make(world.clone()).await;
            let mut reference = ReferenceAlerts::new(world.clone());
            let mut ids = Ids::default();
            let mut version = Version {
                number: TopicModelVersion(0),
                topics: Vec::new(),
            };
            let mut micros = 1_000u64;
            for (step, op) in ops.iter().enumerate() {
                micros += match op {
                    AlertOp::Tick { micros } => *micros,
                    _ => 1,
                };
                let now = ts(micros);
                subject.set_now(now);
                reference.set_now(now);
                step_both(
                    step,
                    op,
                    &mut subject,
                    &mut reference,
                    &mut ids,
                    &mut version,
                    now,
                )
                .await?;
                let mut their_rules: Vec<_> = subject
                    .rules()
                    .await
                    .iter()
                    .map(|rule| rule_view(rule, &ids))
                    .collect();
                let mut our_rules: Vec<_> = reference
                    .rules()
                    .await
                    .iter()
                    .map(|rule| rule_view(rule, &Ids::default()))
                    .collect();
                their_rules.sort_by_key(|rule| rule.0);
                our_rules.sort_by_key(|rule| rule.0);
                same(step, "rules after the operation", &their_rules, &our_rules)?;
                let mut their_alerts: Vec<Alert> = subject
                    .alerts()
                    .await
                    .iter()
                    .map(|alert| ids.translate_alert(alert))
                    .collect();
                let mut our_alerts = reference.alerts().await;
                their_alerts.sort_by_key(|alert| alert.id);
                our_alerts.sort_by_key(|alert| alert.id);
                same(
                    step,
                    "alerts after the operation",
                    &their_alerts,
                    &our_alerts,
                )?;
                let mut active = std::collections::HashSet::new();
                for alert in their_alerts.iter().filter(|alert| is_active(&alert.state)) {
                    holds(step, active.insert((alert.rule, alert.subject)), || {
                        format!(
                            "two active alerts for ({:?}, {:?})",
                            alert.rule, alert.subject
                        )
                    })?;
                }
            }
            Ok(())
        })
    })
}

/// Random rule creation, updates, enabling, re-fits and model changes,
/// with triage in between, against the reference. `make` builds a fresh
/// subject from the world: config and embedder, version 0 current, the
/// clock at the epoch, no supersession.
pub fn check_alert_rule_store<S, F, Fut>(
    harness: HarnessConfig,
    make: F,
) -> Result<(), ModelMismatch>
where
    S: AlertStoreSubject,
    F: Fn(AlertWorld) -> Fut,
    Fut: Future<Output = S>,
{
    check_alerts(
        harness,
        make,
        prop::collection::vec(rule_ops(), 1..harness.max_ops),
    )
}

/// Random drafts, sanctions, disables, verdicts, acknowledgements and
/// resolutions against the reference. `make` as for
/// [`check_alert_rule_store`].
pub fn check_alert_triage<S, F, Fut>(harness: HarnessConfig, make: F) -> Result<(), ModelMismatch>
where
    S: AlertStoreSubject,
    F: Fn(AlertWorld) -> Fut,
    Fut: Future<Output = S>,
{
    check_alerts(
        harness,
        make,
        prop::collection::vec(triage_ops(), 1..harness.max_ops),
    )
}
