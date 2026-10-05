//! Rule evaluation: [`RuleEvaluator`], the spec's `AlertRuleEval` for one
//! stored rule, whatever its kind.
//!
//! A rule drafts only while it evaluates (`AlertRuleDef::evaluates`:
//! enabled and current), only for its kind's trigger, and always with the
//! envelope's time as `raised_at`, so the same envelope and context give
//! the same draft:
//!
//! | Kind | Trigger | Drafts when | Subject |
//! | --- | --- | --- | --- |
//! | `NewChannel` | `ChannelDiscovered` | always | the channel |
//! | `UnreviewedTraffic`, `UnsanctionedTraffic` | `TransmissionConfirmed` on a channel route | the channel's policy (`RuleContext::channel_policy`) is `Raise` of this kind | the route's channel |
//! | `SanctionedUnused` | `DeclaredChannelUnused` | the channel's policy is `Sanctioned` | the channel |
//! | `SuspectedTransmission` | `TransmissionSuspected` | always | the transmission |
//! | `WatchedTopic` | `TransmissionClassified` (cause `Confirmation`) | the classification's version is the rule's and its topic one of the rule's | the transmission |
//! | `SemanticQuery` | `TransmissionClassified` (cause `Confirmation`) | the transmission's embedding is at least `threshold` similar to the query | the transmission |
//!
//! The context resolves a channel through supersession before reading its
//! policy (see [`crate::alerts::consumer::FlowContext`]); a re-fit's
//! classification (cause `Refit`) drafts nothing, so re-classifying the
//! corpus never floods the inbox.

#[cfg(test)]
mod tests;

use crosstalk_spec::aggregates::alert::{
    AlertDraft, AlertRule, AlertRuleDef, AlertRuleKind, AlertSubject, BuiltinRule, ContentRule,
    QueryWatch, TopicWatch,
};
use crosstalk_spec::derived::flow::channel::policy::{Policy, TrafficVerdict};
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::insight::{ClassificationCause, InsightEvent};
use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::interfaces::l6_analysis::{AlertRuleEval, RuleContext};

use crate::search::similarity;

/// One stored rule, as an evaluator.
#[derive(Debug, Clone, PartialEq)]
pub struct RuleEvaluator {
    rule: AlertRuleDef,
}

impl RuleEvaluator {
    pub fn new(rule: AlertRuleDef) -> Self {
        Self { rule }
    }

    pub fn rule(&self) -> &AlertRuleDef {
        &self.rule
    }

    fn draft(&self, envelope: &Envelope, subject: AlertSubject) -> AlertDraft {
        AlertDraft {
            rule: self.rule.id(),
            subject,
            raised_at: envelope.at,
        }
    }
}

impl AlertRuleEval for RuleEvaluator {
    fn kind(&self) -> AlertRuleKind {
        self.rule.kind()
    }

    async fn evaluate(
        &self,
        envelope: &Envelope,
        context: &impl RuleContext,
    ) -> Option<AlertDraft> {
        if !self.rule.evaluates() {
            return None;
        }
        match self.rule.rule() {
            AlertRule::Builtin(rule) => self.builtin(*rule, envelope, context).await,
            AlertRule::User { content, .. } => self.content(content, envelope, context).await,
        }
    }
}

impl RuleEvaluator {
    async fn builtin(
        &self,
        rule: BuiltinRule,
        envelope: &Envelope,
        context: &impl RuleContext,
    ) -> Option<AlertDraft> {
        let BusEvent::Detect(event) = &envelope.event else {
            return None;
        };
        match (rule, event) {
            (BuiltinRule::NewChannel, DetectEvent::ChannelDiscovered { channel, .. }) => {
                Some(self.draft(envelope, AlertSubject::Channel(*channel)))
            }
            (
                BuiltinRule::UnreviewedTraffic | BuiltinRule::UnsanctionedTraffic,
                DetectEvent::TransmissionConfirmed {
                    route: Route::Channel(channel),
                    ..
                },
            ) => {
                let policy = context.channel_policy(*channel).await?;
                (policy.on_traffic() == TrafficVerdict::Raise(rule.kind()))
                    .then(|| self.draft(envelope, AlertSubject::Channel(*channel)))
            }
            (BuiltinRule::SanctionedUnused, DetectEvent::DeclaredChannelUnused { channel, .. }) => {
                let policy = context.channel_policy(*channel).await?;
                matches!(policy, Policy::Sanctioned(_))
                    .then(|| self.draft(envelope, AlertSubject::Channel(*channel)))
            }
            (
                BuiltinRule::SuspectedTransmission,
                DetectEvent::TransmissionSuspected { transmission, .. },
            ) => Some(self.draft(envelope, AlertSubject::Transmission(*transmission))),
            _ => None,
        }
    }

    async fn content(
        &self,
        content: &ContentRule,
        envelope: &Envelope,
        context: &impl RuleContext,
    ) -> Option<AlertDraft> {
        let BusEvent::Insight(InsightEvent::TransmissionClassified {
            cause: ClassificationCause::Confirmation,
            transmission,
            classification,
            ..
        }) = &envelope.event
        else {
            return None;
        };
        let subject = AlertSubject::Transmission(*transmission);
        match content {
            ContentRule::WatchedTopic {
                watch: TopicWatch::Current(watched),
                ..
            } => {
                let topic = classification.topic?;
                (classification.version == watched.version
                    && watched.topics.iter().any(|listed| *listed == topic))
                .then(|| self.draft(envelope, subject))
            }
            ContentRule::SemanticQuery {
                watch: QueryWatch::Current(query),
                threshold,
            } => {
                let embedding = context.transmission_embedding(*transmission).await?;
                let score = similarity(&query.embedding, &embedding)?;
                (score.get() >= threshold.get()).then(|| self.draft(envelope, subject))
            }
            ContentRule::WatchedTopic {
                watch: TopicWatch::Stale { .. },
                ..
            }
            | ContentRule::SemanticQuery {
                watch: QueryWatch::Stale { .. },
                ..
            } => None,
        }
    }
}
