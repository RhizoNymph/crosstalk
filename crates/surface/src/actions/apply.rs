//! Each action's effect: one call to the store that owns it, with the
//! caller's operator and the acceptance time stamped wherever the store
//! records an author or a time, and its refusal mapped through the one
//! `ActionError::from` of that store's error.

use crosstalk_spec::aggregates::retention::{Pin, PinChange};
use crosstalk_spec::derived::flow::channel::policy::{
    Decision, PolicyAuthor, PolicyDecision, PolicyKind, Recorded,
};
use crosstalk_spec::derived::flow::channel::promotion::Promotion;
use crosstalk_spec::derived::flow::resource::ResourcePattern;
use crosstalk_spec::derived::flow::verdict::VerdictRecorded;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::{ChannelId, EventId};
use crosstalk_spec::interfaces::l2_transport::{DeadLetterStore, EventBus};
use crosstalk_spec::interfaces::l3_reconstruction::IdentityResolver;
use crosstalk_spec::interfaces::l5_flow::channels::ChannelReads;
use crosstalk_spec::interfaces::l5_flow::verdicts::TransmissionVerdicts;
use crosstalk_spec::interfaces::l5_flow::{ChannelDirectory, ChannelRegistry};
use crosstalk_spec::interfaces::l6_analysis::alerts::AlertActions;
use crosstalk_spec::interfaces::l6_analysis::{AlertRuleStore, TopicCatalog};
use crosstalk_spec::interfaces::l8_surface::actions::SupersededChannels;
use crosstalk_spec::interfaces::l8_surface::{
    ActionError, ActionOutcome, Caller, ConflictKind, OperatorAction,
};
use crosstalk_spec::support::{Change, Timestamp};

use super::errors::{bus_action_error, store_failure};
use crate::service::Surface;
use crate::stores::SurfaceStores;

fn change(change: Change) -> ActionOutcome {
    match change {
        Change::Applied => ActionOutcome::Applied,
        Change::Unchanged => ActionOutcome::Unchanged,
    }
}

fn pin_change(change: PinChange) -> ActionOutcome {
    match change {
        PinChange::Changed => ActionOutcome::Applied,
        PinChange::Unchanged => ActionOutcome::Unchanged,
    }
}

impl<S: SurfaceStores> Surface<S> {
    /// The effect of `action` for `caller`, accepted at `at`. The caller's
    /// permission has been checked.
    pub(super) async fn apply(
        &self,
        caller: &Caller,
        action: &OperatorAction,
        at: Timestamp,
    ) -> Result<ActionOutcome, ActionError> {
        let by = caller.operator();
        match action.clone() {
            OperatorAction::SetPolicy {
                channel,
                policy,
                note,
            } => self.set_policy(caller, channel, policy, note, at).await,
            OperatorAction::MergeAgents(request) => {
                let record = self.stores.agents().clone().merge(request, at).await?;
                Ok(ActionOutcome::Merged(record.id()))
            }
            OperatorAction::Unmerge { merge } => {
                self.stores.agents().clone().unmerge(merge, by, at).await?;
                Ok(ActionOutcome::Applied)
            }
            OperatorAction::RenameAgent { agent, label } => {
                let changed = self.stores.agents().clone().rename(agent, label, by).await?;
                Ok(change(changed))
            }
            OperatorAction::PromoteChannel {
                channel,
                pattern,
                policy,
                note,
            } => self.promote(caller, channel, pattern, policy, note, at).await,
            OperatorAction::Acknowledge { alert } => {
                let changed = self
                    .stores
                    .alerts()
                    .clone()
                    .acknowledge(alert, by, at)
                    .await?;
                Ok(change(changed))
            }
            OperatorAction::Resolve { alert, note } => {
                let changed = self
                    .stores
                    .alerts()
                    .clone()
                    .resolve(alert, by, at, note)
                    .await?;
                Ok(change(changed))
            }
            OperatorAction::CreateRule { name, rule, sinks } => {
                let id = self
                    .stores
                    .alerts()
                    .clone()
                    .create(name, rule, sinks, by, at)
                    .await?;
                Ok(ActionOutcome::RuleCreated(id))
            }
            OperatorAction::SetVerdict {
                transmission,
                verdict,
                note,
            } => {
                let recorded = self
                    .stores
                    .transmissions()
                    .clone()
                    .set(transmission, verdict, by, at, note)
                    .await?;
                Ok(match recorded {
                    VerdictRecorded::Appended(_) => ActionOutcome::Applied,
                    VerdictRecorded::Unchanged => ActionOutcome::Unchanged,
                })
            }
            OperatorAction::UpdateRule {
                id,
                name,
                rule,
                sinks,
            } => {
                let changed = self
                    .stores
                    .alerts()
                    .clone()
                    .update(id, name, rule, sinks, by)
                    .await?;
                Ok(change(changed))
            }
            OperatorAction::SetRuleEnabled { id, enabled } => {
                let changed = self
                    .stores
                    .alerts()
                    .clone()
                    .set_enabled(id, enabled, by, at)
                    .await?;
                Ok(change(changed))
            }
            OperatorAction::ReplayDeadLetter { group, id } => {
                self.stores
                    .dead_letters()
                    .replay(&group, id)
                    .await
                    .map_err(bus_action_error)?;
                Ok(ActionOutcome::Applied)
            }
            OperatorAction::PinTopicVersion { version } => {
                let changed = self.stores.topics().pin(version, Pin { by, at }).await?;
                Ok(pin_change(changed))
            }
            OperatorAction::UnpinTopicVersion { version } => {
                let changed = self.stores.topics().unpin(version, at).await?;
                Ok(pin_change(changed))
            }
        }
    }

    /// `SetPolicy`: refuse an unknown or superseded channel, record the
    /// caller's decision in the channel's policy history (so the caller's
    /// next read shows it), then publish the decision as `PolicyChanged`
    /// for flow detection and alert triage. The registry announces the
    /// recorded decision with `Changed::Channel`; the surface publishes no
    /// `Changed` of its own.
    async fn set_policy(
        &self,
        caller: &Caller,
        channel: ChannelId,
        kind: PolicyKind,
        note: Option<String>,
        at: Timestamp,
    ) -> Result<ActionOutcome, ActionError> {
        let stores = &self.stores;
        if stores.channels().channel(channel).await?.is_none() {
            return Err(ActionError::NotFound);
        }
        let canonical = ChannelDirectory::canonical(stores.channels(), channel);
        if canonical != channel {
            return Err(ActionError::Conflict(ConflictKind::ChannelSuperseded {
                channel,
                by: canonical,
            }));
        }
        let decision = PolicyDecision {
            kind,
            decision: Decision {
                by: PolicyAuthor::Operator(caller.operator()),
                at,
                note,
            },
        };
        let policy = decision.policy();
        let recorded = stores
            .channels()
            .clone()
            .set_policy(channel, decision)
            .await?;
        let id: EventId = self.ids.mint().map_err(store_failure)?;
        let envelope = Envelope {
            id,
            at,
            event: BusEvent::Insight(InsightEvent::PolicyChanged { channel, policy }),
        };
        stores
            .bus()
            .publish(envelope)
            .await
            .map_err(bus_action_error)?;
        Ok(match recorded {
            Recorded::Current | Recorded::Superseded => ActionOutcome::Applied,
            Recorded::Duplicate => ActionOutcome::Unchanged,
        })
    }

    /// `PromoteChannel`: the caller's pattern and decision, both stamped
    /// with the caller and `at`, handed to the registry as one
    /// `Promotion`. Success names the same channel and every channel the
    /// registry reported it superseded.
    async fn promote(
        &self,
        caller: &Caller,
        channel: ChannelId,
        pattern: ResourcePattern,
        policy: PolicyKind,
        note: Option<String>,
        at: Timestamp,
    ) -> Result<ActionOutcome, ActionError> {
        let promotion = Promotion::new(pattern, policy, caller.operator(), at, note);
        let promoted = self
            .stores
            .channels()
            .clone()
            .promote(channel, promotion)
            .await?;
        Ok(ActionOutcome::ChannelPromoted {
            channel,
            superseded: SupersededChannels::new(promoted.superseded),
        })
    }
}
