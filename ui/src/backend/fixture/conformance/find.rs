//! Finding things in the generated world that satisfy a scenario's facts.

use crosstalk_conformance::ProvisionError;
use crosstalk_spec::aggregates::alert::RuleStatus;
use crosstalk_spec::derived::flow::transmission::{Crossing, Route};
use crosstalk_spec::ids::{AgentId, AlertRuleId, ChannelId, MergeId, ResourceId, TransmissionId};

use crate::backend::fixture::clock::{BUCKET, WATERMARK};
use crate::backend::fixture::queries::Ctx;
use crate::backend::fixture::store::State;
use crate::backend::fixture::world::{ChannelKey, TxRecord, World, co_accesses, confirmed};

/// The world and its state, read while binding one scenario.
pub struct Find<'a> {
    pub world: &'a World,
    pub state: &'a State,
    ctx: Ctx<'a>,
    scenario: &'static str,
}

impl<'a> Find<'a> {
    pub fn new(world: &'a World, state: &'a State, scenario: &'static str) -> Self {
        Self {
            world,
            state,
            ctx: Ctx::new(world, state),
            scenario,
        }
    }

    /// A binding failure naming what was missing.
    pub fn missing(&self, what: &str) -> ProvisionError {
        ProvisionError::Failed {
            scenario: self.scenario,
            reason: format!("the fixture world has no {what}"),
        }
    }

    pub fn agent(&self, key: &str) -> Result<AgentId, ProvisionError> {
        self.world
            .scenario
            .agent(key)
            .ok_or_else(|| self.missing(&format!("agent {key}")))
    }

    pub fn channel(&self, key: ChannelKey) -> Result<ChannelId, ProvisionError> {
        self.world
            .scenario
            .channel(key)
            .ok_or_else(|| self.missing(&format!("channel {key:?}")))
    }

    /// The resource a discovered (or superseded, or promoted) channel was
    /// seeded with.
    pub fn seed(&self, key: ChannelKey) -> Result<ResourceId, ProvisionError> {
        let id = self.channel(key)?;
        self.state
            .channels
            .get(&id)
            .and_then(|record| match &record.channel().origin {
                crosstalk_spec::derived::flow::channel::ChannelOrigin::Discovered {
                    seed, ..
                }
                | crosstalk_spec::derived::flow::channel::ChannelOrigin::Superseded {
                    seed, ..
                } => Some(seed.resource),
                crosstalk_spec::derived::flow::channel::ChannelOrigin::Declared {
                    history:
                        crosstalk_spec::derived::flow::channel::DeclaredHistory::Promoted {
                            from, ..
                        },
                    ..
                } => Some(from.resource),
                crosstalk_spec::derived::flow::channel::ChannelOrigin::Declared { .. } => None,
            })
            .ok_or_else(|| self.missing(&format!("seed of {key:?}")))
    }

    /// The merge of `source` into `target` in the merge log.
    pub fn merge(&self, source: AgentId, target: AgentId) -> Result<MergeId, ProvisionError> {
        self.state
            .identity
            .merges()
            .iter()
            .find(|m| m.source() == source && m.target() == target)
            .map(|m| m.id())
            .ok_or_else(|| self.missing("merge"))
    }

    pub fn is_canonical(&self, id: AgentId) -> bool {
        !self.state.identity.is_merged(id)
    }

    /// The writer a transmission's evidence names: its sender once
    /// confirmed, else the writer of its first co-access.
    pub fn writer(&self, record: &TxRecord) -> Option<AgentId> {
        record.from.or_else(|| {
            co_accesses(&record.transmission.state)
                .first()
                .and_then(|co| self.world.access(co.write()))
                .map(|access| access.agent)
        })
    }

    /// Whether a transmission crosses between two agents now.
    pub fn crosses(&self, record: &TxRecord) -> bool {
        self.ctx.crossing(&record.transmission) == Crossing::Crosses
    }

    /// The newest transmission `keep` admits whose writer and reader are
    /// both canonical (so they bind to roles of canonical agents).
    pub fn tx(
        &self,
        what: &str,
        keep: impl Fn(&TxRecord) -> bool,
    ) -> Result<&'a TxRecord, ProvisionError> {
        self.world
            .transmissions
            .iter()
            .rev()
            .filter(|r| self.is_canonical(r.transmission.to))
            .filter(|r| self.writer(r).is_none_or(|w| self.is_canonical(w)))
            .find(|r| keep(r))
            .ok_or_else(|| self.missing(what))
    }

    /// The newest confirmed cross-agent transmission between canonical
    /// agents, settled before the watermark, that `keep` admits.
    pub fn confirmed(
        &self,
        what: &str,
        keep: impl Fn(&TxRecord) -> bool,
    ) -> Result<&'a TxRecord, ProvisionError> {
        self.tx(what, |r| {
            confirmed(&r.transmission.state).is_some_and(|c| c.at() < WATERMARK)
                && self.crosses(r)
                && keep(r)
        })
    }

    /// The newest confirmed cross-agent transmission routed through `key`.
    pub fn on_channel(&self, key: ChannelKey) -> Result<&'a TxRecord, ProvisionError> {
        let id = self.channel(key)?;
        self.confirmed(&format!("confirmed transmission on {key:?}"), |r| {
            r.transmission.route == Route::Channel(id)
        })
    }

    /// The current stale user rule.
    pub fn stale_rule(&self) -> Result<AlertRuleId, ProvisionError> {
        self.state
            .rules
            .iter()
            .find(|r| r.stale_reason().is_some() && r.status == RuleStatus::Enabled)
            .map(|r| r.id())
            .ok_or_else(|| self.missing("stale enabled rule"))
    }

    /// The verdict log's verdicts for a transmission, oldest first.
    pub fn verdicts(
        &self,
        id: TransmissionId,
    ) -> Vec<Option<crosstalk_spec::derived::flow::verdict::Verdict>> {
        self.state
            .verdicts
            .get(&id)
            .map(|log| log.records().iter().map(|r| r.verdict()).collect())
            .unwrap_or_default()
    }
}

/// Whether a confirmed transmission was confirmed in a later bucket than
/// it opened in.
pub fn confirmed_later(record: &TxRecord) -> bool {
    let width = BUCKET.as_micros().get();
    confirmed(&record.transmission.state).is_some_and(|c| {
        c.at().as_micros() / width > record.transmission.opened_at.as_micros() / width
    })
}
