//! Scenarios: worlds described as facts over roles, provisioned by each
//! implementation and checked through L8.
//!
//! - `roles`: typed symbolic names ([`AgentRole`], [`ChannelRole`], ...).
//! - [`facts`]: the vocabulary ([`Fact`]).
//! - [`Scenario`]: a named, validated set of facts. [`Scenario::compose`]
//!   unions scenarios into one world; roles carry their scenario's name, so
//!   parts never collide.
//! - [`Bindings`]: what a harness returns, each role bound to an id.
//! - [`named`]: the scenarios the suite's tests run against, built from the
//!   fixture world's cases (the hijacked wiki, impersonating claims, merges
//!   with vetoes, a stale rule after a re-fit, suspected-only traffic,
//!   declared and unused channels, and the rest).

mod bindings;
pub mod facts;
pub mod named;
mod roles;

use std::collections::{HashMap, HashSet};

pub use bindings::{Bindings, Unbound};
pub use facts::*;
pub use roles::{
    Agent, AgentRole, Channel, ChannelRole, Merge, MergeRole, Resource, ResourceRole, Role,
    RoleKey, RoleKind, Rule, RuleRole, Transmission, TransmissionRole,
};

/// A scenario that does not describe a possible world. Always a bug in
/// the scenario's definition.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ScenarioError {
    #[error("{0} is declared twice")]
    Redeclared(RoleKey),
    #[error("{0} is used but never declared")]
    Undeclared(RoleKey),
    #[error("{role} belongs to another scenario than {scenario}")]
    Foreign {
        role: RoleKey,
        scenario: &'static str,
    },
    #[error("{0} names one agent twice")]
    SameAgent(RoleKey),
    #[error("{0} is superseded by a promotion but was not discovered")]
    SupersedesUndiscovered(RoleKey),
    #[error("scenario {0} is composed twice")]
    RepeatedPart(&'static str),
    #[error("{0} carries transmissions but no channel holds it")]
    NoChannel(RoleKey),
    #[error("{0} has co-access evidence but a route other than a resource")]
    CoAccessOffResource(RoleKey),
}

/// A named, validated world description.
#[derive(Debug, Clone, PartialEq)]
pub struct Scenario {
    name: &'static str,
    /// The named scenarios composed into this one; itself alone for a
    /// scenario built from facts.
    parts: Vec<&'static str>,
    facts: Vec<Fact>,
}

impl Scenario {
    /// Starts a scenario named `name`; every role it declares must carry
    /// that name.
    pub fn build(name: &'static str) -> ScenarioBuilder {
        ScenarioBuilder {
            name,
            facts: Vec::new(),
        }
    }

    /// One world holding every fact of `parts`.
    pub fn compose(
        name: &'static str,
        parts: impl IntoIterator<Item = Scenario>,
    ) -> Result<Self, ScenarioError> {
        let mut seen = HashSet::new();
        let mut names = Vec::new();
        let mut facts = Vec::new();
        for part in parts {
            for inner in part.parts {
                if !seen.insert(inner) {
                    return Err(ScenarioError::RepeatedPart(inner));
                }
                names.push(inner);
            }
            facts.extend(part.facts);
        }
        let scenario = Self {
            name,
            parts: names,
            facts,
        };
        scenario.validate(None)?;
        Ok(scenario)
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    /// The named scenarios this world is made of.
    pub fn parts(&self) -> &[&'static str] {
        &self.parts
    }

    pub fn facts(&self) -> &[Fact] {
        &self.facts
    }

    pub fn agents(&self) -> impl Iterator<Item = &AgentFact> {
        self.facts.iter().filter_map(|f| match f {
            Fact::Agent(a) => Some(a),
            _ => None,
        })
    }

    pub fn agent(&self, role: AgentRole) -> Option<&AgentFact> {
        self.agents().find(|a| a.role == role)
    }

    pub fn resources(&self) -> impl Iterator<Item = &ResourceFact> {
        self.facts.iter().filter_map(|f| match f {
            Fact::Resource(r) => Some(r),
            _ => None,
        })
    }

    pub fn resource(&self, role: ResourceRole) -> Option<&ResourceFact> {
        self.resources().find(|r| r.role == role)
    }

    pub fn channels(&self) -> impl Iterator<Item = &ChannelFact> {
        self.facts.iter().filter_map(|f| match f {
            Fact::Channel(c) => Some(c),
            _ => None,
        })
    }

    pub fn channel(&self, role: ChannelRole) -> Option<&ChannelFact> {
        self.channels().find(|c| c.role == role)
    }

    /// The channel holding `resource`: the one discovered at it, else a
    /// declared one whose pattern matches it.
    pub fn channel_at(&self, resource: ResourceRole) -> Option<&ChannelFact> {
        let seeded = self.channels().find(|c| match &c.source {
            ChannelSource::Discovered { seed } => *seed == resource,
            ChannelSource::Declared { .. } => false,
        });
        let locator = self.resource(resource).map(|r| &r.locator);
        seeded.or_else(|| {
            self.channels().find(|c| match &c.source {
                ChannelSource::Declared { pattern } => locator.is_some_and(|l| pattern.matches(l)),
                ChannelSource::Discovered { .. } => false,
            })
        })
    }

    /// The channel a promotion superseded `channel` into, if any.
    pub fn superseded_by(&self, channel: ChannelRole) -> Option<ChannelRole> {
        self.promotions()
            .find(|p| p.supersedes.contains(&channel))
            .map(|p| p.channel)
    }

    /// The channel in force for `channel`: itself, or what superseded it.
    pub fn in_force(&self, channel: ChannelRole) -> ChannelRole {
        self.superseded_by(channel).unwrap_or(channel)
    }

    pub fn accesses(&self) -> impl Iterator<Item = &AccessFact> {
        self.facts.iter().filter_map(|f| match f {
            Fact::Access(a) => Some(a),
            _ => None,
        })
    }

    pub fn transmissions(&self) -> impl Iterator<Item = &TransmissionFact> {
        self.facts.iter().filter_map(|f| match f {
            Fact::Transmission(t) => Some(t),
            _ => None,
        })
    }

    pub fn transmission(&self, role: TransmissionRole) -> Option<&TransmissionFact> {
        self.transmissions().find(|t| t.role == role)
    }

    pub fn merges(&self) -> impl Iterator<Item = &MergeFact> {
        self.facts.iter().filter_map(|f| match f {
            Fact::Merge(m) => Some(m),
            _ => None,
        })
    }

    pub fn promotions(&self) -> impl Iterator<Item = &PromotionFact> {
        self.facts.iter().filter_map(|f| match f {
            Fact::Promotion(p) => Some(p),
            _ => None,
        })
    }

    pub fn policies(&self) -> impl Iterator<Item = &PolicyFact> {
        self.facts.iter().filter_map(|f| match f {
            Fact::Policy(p) => Some(p),
            _ => None,
        })
    }

    pub fn verdicts(&self) -> impl Iterator<Item = &VerdictFact> {
        self.facts.iter().filter_map(|f| match f {
            Fact::Verdicts(v) => Some(v),
            _ => None,
        })
    }

    pub fn dropped_bodies(&self) -> impl Iterator<Item = &BodyDroppedFact> {
        self.facts.iter().filter_map(|f| match f {
            Fact::BodyDropped(d) => Some(d),
            _ => None,
        })
    }

    pub fn has_topic_history(&self) -> bool {
        self.facts
            .iter()
            .any(|f| matches!(f, Fact::TopicHistory(_)))
    }

    pub fn stale_rules(&self) -> impl Iterator<Item = &StaleRuleFact> {
        self.facts.iter().filter_map(|f| match f {
            Fact::StaleRule(r) => Some(r),
            _ => None,
        })
    }

    pub fn dead_letters(&self) -> impl Iterator<Item = &DeadLetterFact> {
        self.facts.iter().filter_map(|f| match f {
            Fact::DeadLetters(d) => Some(d),
            _ => None,
        })
    }

    /// The canonical agent `role` resolves to through the scenario's
    /// standing (unreverted) merges.
    pub fn canonical(&self, role: AgentRole) -> AgentRole {
        let mut at = role;
        // Merges form chains; a scenario never holds a cycle (validated),
        // so this ends within the number of merges.
        for _ in 0..=self.merges().count() {
            match self.merges().find(|m| m.alias == at && !m.reverted) {
                Some(merge) => at = merge.into,
                None => return at,
            }
        }
        at
    }

    /// Whether the transmission crosses between two agents once the
    /// scenario's standing merges resolve (`Transmission::crossing`).
    pub fn crosses(&self, fact: &TransmissionFact) -> bool {
        fact.state
            .writer()
            .is_some_and(|w| self.canonical(w) != self.canonical(fact.reader))
    }

    /// Checks every reference and constraint; `own` restricts roles to one
    /// scenario's name.
    fn validate(&self, own: Option<&'static str>) -> Result<(), ScenarioError> {
        let mut declared: HashMap<RoleKey, ()> = HashMap::new();
        let mut declare = |key: RoleKey| -> Result<(), ScenarioError> {
            if let Some(scenario) = own
                && key.scenario != scenario
            {
                return Err(ScenarioError::Foreign {
                    role: key,
                    scenario,
                });
            }
            match declared.insert(key, ()) {
                Some(()) => Err(ScenarioError::Redeclared(key)),
                None => Ok(()),
            }
        };
        for fact in &self.facts {
            match fact {
                Fact::Agent(a) => declare(a.role.key())?,
                Fact::Resource(r) => declare(r.role.key())?,
                Fact::Channel(c) => declare(c.role.key())?,
                Fact::Transmission(t) => declare(t.role.key())?,
                Fact::Merge(m) => declare(m.role.key())?,
                Fact::StaleRule(r) => declare(r.rule.key())?,
                Fact::Access(_)
                | Fact::Promotion(_)
                | Fact::Policy(_)
                | Fact::Verdicts(_)
                | Fact::BodyDropped(_)
                | Fact::TopicHistory(_)
                | Fact::DeadLetters(_) => {}
            }
        }
        let known = |key: RoleKey| {
            if declared.contains_key(&key) {
                Ok(())
            } else {
                Err(ScenarioError::Undeclared(key))
            }
        };
        for fact in &self.facts {
            match fact {
                Fact::Agent(a) => {
                    if let Some(parent) = a.parent {
                        known(parent.key())?;
                    }
                }
                Fact::Resource(_) | Fact::TopicHistory(_) | Fact::DeadLetters(_) => {}
                Fact::Channel(c) => match c.source {
                    ChannelSource::Declared { .. } => {}
                    ChannelSource::Discovered { seed } => known(seed.key())?,
                },
                Fact::Access(a) => {
                    known(a.agent.key())?;
                    known(a.resource.key())?;
                }
                Fact::Transmission(t) => {
                    known(t.reader.key())?;
                    if let Some(writer) = t.state.writer() {
                        known(writer.key())?;
                        if writer == t.reader {
                            return Err(ScenarioError::SameAgent(t.role.key()));
                        }
                    }
                    match &t.route {
                        Some(Via::Resource(resource)) => {
                            known(resource.key())?;
                            if self.channel_at(*resource).is_none() {
                                return Err(ScenarioError::NoChannel(resource.key()));
                            }
                        }
                        Some(Via::Delegation(_) | Via::Direct | Via::Unobserved)
                            if t.state.is_co_access() =>
                        {
                            return Err(ScenarioError::CoAccessOffResource(t.role.key()));
                        }
                        Some(Via::Delegation(_) | Via::Direct | Via::Unobserved) | None => {}
                    }
                }
                Fact::Merge(m) => {
                    known(m.alias.key())?;
                    known(m.into.key())?;
                    if m.alias == m.into {
                        return Err(ScenarioError::SameAgent(m.role.key()));
                    }
                }
                Fact::Promotion(p) => {
                    known(p.channel.key())?;
                    for superseded in &p.supersedes {
                        known(superseded.key())?;
                        let discovered = self
                            .channel(*superseded)
                            .is_some_and(|c| matches!(c.source, ChannelSource::Discovered { .. }));
                        if !discovered {
                            return Err(ScenarioError::SupersedesUndiscovered(superseded.key()));
                        }
                    }
                }
                Fact::Policy(p) => known(p.channel.key())?,
                Fact::Verdicts(v) => known(v.transmission.key())?,
                Fact::BodyDropped(d) => known(d.transmission.key())?,
                Fact::StaleRule(_) => {}
            }
        }
        Ok(())
    }
}

/// Collects a scenario's facts; [`ScenarioBuilder::done`] validates them.
#[derive(Debug, Clone)]
pub struct ScenarioBuilder {
    name: &'static str,
    facts: Vec<Fact>,
}

impl ScenarioBuilder {
    pub fn fact(mut self, fact: impl Into<Fact>) -> Self {
        self.facts.push(fact.into());
        self
    }

    pub fn facts(mut self, facts: impl IntoIterator<Item = Fact>) -> Self {
        self.facts.extend(facts);
        self
    }

    pub fn done(self) -> Result<Scenario, ScenarioError> {
        let scenario = Scenario {
            name: self.name,
            parts: vec![self.name],
            facts: self.facts,
        };
        scenario.validate(Some(self.name))?;
        Ok(scenario)
    }
}
