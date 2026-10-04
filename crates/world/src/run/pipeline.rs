//! L3 and L5 writes: agents and merges, channels and their traffic,
//! transmissions, policies, the promotion and verdicts.

use crosstalk_spec::derived::flow::channel::policy::{PolicyAuthor, PolicyKind, Recorded};
use crosstalk_spec::derived::flow::verdict::{Verdict, VerdictRecorded};
use crosstalk_spec::interfaces::l3_reconstruction::agents::ActivityStore;
use crosstalk_spec::interfaces::l3_reconstruction::lifecycle::AgentLifecycle;
use crosstalk_spec::interfaces::l3_reconstruction::{ClaimStore, IdentityResolver};
use crosstalk_spec::interfaces::l5_flow::ChannelRegistry;
use crosstalk_spec::interfaces::l5_flow::channels::ChannelTraffic;
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::interfaces::l5_flow::verdicts::TransmissionVerdicts;
use crosstalk_spec::interfaces::l6_analysis::AlertTriage;
use crosstalk_spec::interfaces::l6_analysis::corpus::SearchCorpus;
use crosstalk_spec::interfaces::l7_topology::{AccessContribution, EdgeStore};
use crosstalk_spec::interfaces::l8_surface::actions::SupersededChannels;
use crosstalk_spec::interfaces::l8_surface::{ActionError, ActionOutcome, OperatorAction};
use crosstalk_spec::observed::agent::MergeAuthor;
use crosstalk_spec::support::Timestamp;

use crate::error::WorldError;
use crate::script::Op;
use crate::stores::WorldStores;

use super::{Runner, outcome};

impl<S: WorldStores> Runner<'_, S> {
    pub(super) async fn pipeline(&mut self, at: Timestamp, op: Op) -> Result<(), WorldError> {
        match op {
            Op::CreateAgent(agent) => self
                .stores
                .agents()
                .create(agent)
                .await
                .map_err(|e| WorldError::store("AgentLifecycle::create", at, e)),
            Op::Advance { agent, advance } => self
                .stores
                .agents()
                .advance(agent, advance)
                .await
                .map_err(|e| WorldError::store("AgentLifecycle::advance", at, e)),
            Op::Claim { agent, claim } => {
                ClaimStore::record(self.stores.agents(), agent, &claim, at)
                    .await
                    .map_err(|e| WorldError::store("ClaimStore::record", at, e))
            }
            Op::Activity { agent } => ActivityStore::record(self.stores.agents(), agent, at)
                .await
                .map_err(|e| WorldError::store("ActivityStore::record", at, e)),
            Op::Merge { key, request } => {
                let author = request.by();
                let action = OperatorAction::MergeAgents(request);
                let record = self
                    .stores
                    .agents()
                    .merge(request, at)
                    .await
                    .map_err(|e| WorldError::store("IdentityResolver::merge", at, e))?;
                self.ledger.merges.insert(key, record.id());
                if let MergeAuthor::Operator(by) = author {
                    self.audit(at, by, action, Ok(ActionOutcome::Merged(record.id())))
                        .await?;
                }
                Ok(())
            }
            Op::Unmerge { key, by } => {
                let merge = self.ledger.merge(key)?;
                self.stores
                    .agents()
                    .unmerge(merge, by, at)
                    .await
                    .map_err(|e| WorldError::store("IdentityResolver::unmerge", at, e))?;
                self.audit(
                    at,
                    by,
                    OperatorAction::Unmerge { merge },
                    Ok(ActionOutcome::Applied),
                )
                .await
            }
            Op::Rename { agent, label, by } => {
                let change = self
                    .stores
                    .agents()
                    .rename(agent, Some(label.clone()), by)
                    .await
                    .map_err(|e| WorldError::store("IdentityResolver::rename", at, e))?;
                let action = OperatorAction::RenameAgent {
                    agent,
                    label: Some(label),
                };
                self.audit(at, by, action, Ok(outcome(change))).await
            }
            Op::Discover {
                channel,
                resource,
                first_access,
            } => self
                .stores
                .channels()
                .discover(channel, resource, first_access)
                .await
                .map_err(|e| WorldError::store("ChannelTraffic::discover", at, e)),
            Op::AddResource { channel, resource } => self
                .stores
                .channels()
                .add_resource(channel, resource)
                .await
                .map_err(|e| WorldError::store("ChannelTraffic::add_resource", at, e)),
            Op::Access { access, channel } => {
                let contribution = AccessContribution {
                    access: access.id,
                    agent: access.agent,
                    channel,
                    op: access.op.kind(),
                    at: access.at,
                };
                self.stores
                    .channels()
                    .record_access(access)
                    .await
                    .map_err(|e| WorldError::store("ChannelTraffic::record_access", at, e))?;
                self.stores
                    .edges()
                    .apply_access(&contribution)
                    .await
                    .map_err(|e| WorldError::store("EdgeStore::apply_access", at, e))?;
                Ok(())
            }
            Op::Detection { channel, update } => self
                .stores
                .channels()
                .set_detection(channel, update)
                .await
                .map(drop)
                .map_err(|e| WorldError::store("ChannelTraffic::set_detection", at, e)),
            Op::Save(transmission) => self
                .stores
                .transmissions()
                .save(*transmission)
                .await
                .map_err(|e| WorldError::store("TransmissionStore::save", at, e)),
            Op::Confirm {
                channel,
                transmission,
            } => self
                .stores
                .channels()
                .confirm(channel, transmission, at)
                .await
                .map(drop)
                .map_err(|e| WorldError::store("ChannelTraffic::confirm", at, e)),
            Op::Policy { channel, decision } => self.policy(at, channel, decision).await,
            Op::Promote { channel, promotion } => self.promote(at, channel, *promotion).await,
            Op::ForbiddenPolicy {
                channel,
                policy,
                note,
                by,
            } => {
                let action = OperatorAction::SetPolicy {
                    channel,
                    policy,
                    note,
                };
                let missing = action.required_permission();
                self.audit(at, by, action, Err(ActionError::Forbidden { missing }))
                    .await
            }
            Op::Verdict {
                transmission,
                verdict,
                by,
                note,
            } => self.verdict(at, transmission, verdict, by, note).await,
            other => Err(WorldError::missing(format!("a pipeline op, not {other:?}"))),
        }
    }

    async fn policy(
        &mut self,
        at: Timestamp,
        channel: crosstalk_spec::ids::ChannelId,
        decision: crosstalk_spec::derived::flow::channel::policy::PolicyDecision,
    ) -> Result<(), WorldError> {
        let PolicyAuthor::Operator(by) = decision.decision.by else {
            return Err(WorldError::missing("an operator's policy decision"));
        };
        let kind = decision.kind;
        let action = OperatorAction::SetPolicy {
            channel,
            policy: kind,
            note: decision.decision.note.clone(),
        };
        let recorded = self
            .stores
            .channels()
            .set_policy(channel, decision)
            .await
            .map_err(|e| WorldError::store("ChannelRegistry::set_policy", at, e))?;
        let result = match recorded {
            Recorded::Current | Recorded::Superseded => ActionOutcome::Applied,
            Recorded::Duplicate => ActionOutcome::Unchanged,
        };
        self.audit(at, by, action, Ok(result)).await?;
        if kind == PolicyKind::Sanctioned && recorded == Recorded::Current {
            self.sanction(at, channel).await?;
        }
        Ok(())
    }

    async fn promote(
        &mut self,
        at: Timestamp,
        channel: crosstalk_spec::ids::ChannelId,
        promotion: crosstalk_spec::derived::flow::channel::promotion::Promotion,
    ) -> Result<(), WorldError> {
        let PolicyAuthor::Operator(by) = promotion.declaration().by else {
            return Err(WorldError::missing("the promotion's operator"));
        };
        let kind = promotion.decision().kind;
        let action = OperatorAction::PromoteChannel {
            channel,
            pattern: promotion.pattern().clone(),
            policy: kind,
            note: promotion.decision().decision.note.clone(),
        };
        let promoted = self
            .stores
            .channels()
            .promote(channel, promotion)
            .await
            .map_err(|e| WorldError::store("ChannelRegistry::promote", at, e))?;
        for superseded in &promoted.superseded {
            self.ledger.superseded.insert(*superseded, channel);
        }
        let result = ActionOutcome::ChannelPromoted {
            channel,
            superseded: SupersededChannels::new(promoted.superseded.iter().copied()),
        };
        self.audit(at, by, action, Ok(result)).await?;
        if kind == PolicyKind::Sanctioned {
            self.sanction(at, channel).await?;
        }
        Ok(())
    }

    /// `PolicyChanged` or `ChannelPromoted` carrying `Sanctioned`, as the
    /// alerts consumer handles it.
    async fn sanction(
        &mut self,
        at: Timestamp,
        channel: crosstalk_spec::ids::ChannelId,
    ) -> Result<(), WorldError> {
        self.stores
            .alerts()
            .channel_sanctioned(channel, at)
            .await
            .map_err(|e| WorldError::store("AlertTriage::channel_sanctioned", at, e))?;
        self.ledger.sanctioned(channel);
        Ok(())
    }

    async fn verdict(
        &mut self,
        at: Timestamp,
        transmission: crosstalk_spec::ids::TransmissionId,
        verdict: Option<Verdict>,
        by: crosstalk_spec::ids::OperatorId,
        note: Option<String>,
    ) -> Result<(), WorldError> {
        let recorded = self
            .stores
            .transmissions()
            .set(transmission, verdict, by, at, note.clone())
            .await
            .map_err(|e| WorldError::store("TransmissionVerdicts::set", at, e))?;
        let result = match recorded {
            VerdictRecorded::Appended(revision) => {
                self.stores
                    .edges()
                    .judge(transmission, verdict, revision)
                    .await
                    .map_err(|e| WorldError::store("EdgeStore::judge", at, e))?;
                self.stores
                    .search()
                    .judge(transmission, verdict, revision)
                    .await
                    .map_err(|e| WorldError::store("SearchCorpus::judge", at, e))?;
                self.stores
                    .alerts()
                    .transmission_judged(transmission, verdict, revision, at)
                    .await
                    .map_err(|e| WorldError::store("AlertTriage::transmission_judged", at, e))?;
                if verdict == Some(Verdict::FalseDetection) {
                    self.ledger.judged_false(transmission);
                }
                ActionOutcome::Applied
            }
            VerdictRecorded::Unchanged => ActionOutcome::Unchanged,
        };
        let action = OperatorAction::SetVerdict {
            transmission,
            verdict,
            note,
        };
        self.audit(at, by, action, Ok(result)).await
    }
}
