//! The scenario vocabulary: facts at the level of what the gateway derives
//! from traffic.
//!
//! A fact says what is true of a world, never how it came to be: "this
//! agent wrote that resource and that agent read it, and the text matched"
//! is a confirmed transmission through a resource, whichever exchanges
//! carried it. An implementation provisions a fact however it can: the
//! gateway by writing the derived records into its stores through a test
//! hook (or, later, by replaying harness traffic that produces them), the
//! fixture by finding a generated thing that satisfies it. Facts name
//! roles, never ids.
//!
//! Each type admits only facts that can be true: a transmission's state
//! carries a writer exactly when its evidence names one, a merge names two
//! agents, a promotion names the channels it superseded.

use crosstalk_spec::derived::flow::channel::policy::PolicyKind;
use crosstalk_spec::derived::flow::resource::{Locator, ResourcePattern};
use crosstalk_spec::derived::flow::transmission::DelegationDirection;
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::observed::client::HarnessFamily;
use crosstalk_spec::support::NonEmpty;

use super::roles::{AgentRole, ChannelRole, MergeRole, ResourceRole, RuleRole, TransmissionRole};

/// One fact of a scenario.
#[derive(Debug, Clone, PartialEq)]
pub enum Fact {
    Agent(AgentFact),
    Resource(ResourceFact),
    Channel(ChannelFact),
    Access(AccessFact),
    Transmission(TransmissionFact),
    Merge(MergeFact),
    Promotion(PromotionFact),
    Policy(PolicyFact),
    Verdicts(VerdictFact),
    BodyDropped(BodyDroppedFact),
    TopicHistory(TopicHistoryFact),
    StaleRule(StaleRuleFact),
    DeadLetters(DeadLetterFact),
}

/// An agent the gateway resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentFact {
    pub role: AgentRole,
    /// The harness it really runs in; `None` lets the provisioner choose.
    pub harness: Option<HarnessFamily>,
    /// The harness families its traffic claimed, beyond its own. An agent
    /// that claims a family it does not run in is impersonating it.
    pub also_claims: Vec<HarnessFamily>,
    /// Its label, from config.
    pub label: Option<&'static str>,
    /// The agent that spawned it, for a sub-agent.
    pub parent: Option<AgentRole>,
    pub presence: Presence,
}

/// Whether the agent ever sent traffic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Presence {
    /// It sent traffic: it has claims and a last-seen time.
    Seen,
    /// Registered by config and never seen: no claims, no traffic.
    RegisteredOnly,
}

impl AgentFact {
    /// A seen agent running in `harness`, claiming only it.
    pub fn new(role: AgentRole, harness: HarnessFamily) -> Self {
        Self {
            harness: Some(harness),
            ..Self::any(role)
        }
    }

    /// A seen agent in whatever harness the provisioner chooses.
    pub fn any(role: AgentRole) -> Self {
        Self {
            role,
            harness: None,
            also_claims: Vec::new(),
            label: None,
            parent: None,
            presence: Presence::Seen,
        }
    }

    pub fn claiming(mut self, family: HarnessFamily) -> Self {
        self.also_claims.push(family);
        self
    }

    pub fn labelled(mut self, label: &'static str) -> Self {
        self.label = Some(label);
        self
    }

    pub fn child_of(mut self, parent: AgentRole) -> Self {
        self.parent = Some(parent);
        self
    }

    pub fn registered_only(mut self) -> Self {
        self.presence = Presence::RegisteredOnly;
        self
    }

    /// Every family its traffic is known to have claimed: its own first. A
    /// registered agent never seen claims nothing.
    pub fn claims(&self) -> Vec<HarnessFamily> {
        match self.presence {
            Presence::RegisteredOnly => Vec::new(),
            Presence::Seen => self
                .harness
                .iter()
                .chain(self.also_claims.iter())
                .cloned()
                .collect(),
        }
    }
}

/// A resource some tool call touched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceFact {
    pub role: ResourceRole,
    pub locator: Locator,
}

/// A channel and where it came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelFact {
    pub role: ChannelRole,
    pub source: ChannelSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelSource {
    /// Declared in config under `pattern`, before any traffic. Resources
    /// the pattern matches are the channel's.
    Declared { pattern: ResourcePattern },
    /// Discovered by the first cross-agent transmission through `seed`
    /// (INV-747): it exists because such a transmission does.
    Discovered { seed: ResourceRole },
}

/// An access that is part of no transmission: an agent using a resource
/// nobody else touched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessFact {
    pub agent: AgentRole,
    pub resource: ResourceRole,
    pub op: Op,
}

/// What an access did to its resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Op {
    Write,
    Read,
}

/// A transmission, by its reader, route and what the detector knows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransmissionFact {
    pub role: TransmissionRole,
    pub reader: AgentRole,
    /// `None` lets the provisioner choose a route the evidence allows: a
    /// resource for co-access evidence.
    pub route: Option<Via>,
    pub state: Evidence,
}

impl TransmissionFact {
    /// A confirmed transmission from `writer` to `reader`, settled.
    pub fn confirmed(role: TransmissionRole, writer: AgentRole, reader: AgentRole) -> Self {
        Self {
            role,
            reader,
            route: None,
            state: Evidence::Confirmed {
                writer,
                timing: Timing::Settled,
            },
        }
    }

    /// A transmission whose evidence is `state`, to `reader`.
    pub fn with(role: TransmissionRole, reader: AgentRole, state: Evidence) -> Self {
        Self {
            role,
            reader,
            route: None,
            state,
        }
    }

    pub fn via(mut self, route: Via) -> Self {
        self.route = Some(route);
        self
    }

    /// The resource it went through, when its route names one.
    pub fn resource(&self) -> Option<ResourceRole> {
        match self.route {
            Some(Via::Resource(resource)) => Some(resource),
            Some(Via::Delegation(_) | Via::Direct | Via::Unobserved) | None => None,
        }
    }
}

/// How the text travelled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Via {
    /// Written to `resource` by the writer, read from it by the reader: the
    /// route is the channel holding the resource.
    Resource(ResourceRole),
    /// Between a parent and a sub-agent it spawned.
    Delegation(DelegationDirection),
    /// Placed straight into the reader's context (a prompt or a tool result
    /// touching no extracted resource).
    Direct,
    /// The reader's output carries the text but none of its inputs did.
    Unobserved,
}

/// What the detector holds about a transmission. The writer is named
/// exactly when the evidence names a sender.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Evidence {
    /// Content matched: a confirmed transmission.
    Confirmed { writer: AgentRole, timing: Timing },
    /// Only the write-then-read pattern, no content match yet.
    Suspected { writer: AgentRole },
    /// A co-access opened it and the content window is still open.
    AwaitingContent { writer: AgentRole },
    /// Suspected, and the content window closed without a match.
    Discarded { writer: AgentRole },
    /// Detected, no sender named yet.
    Detected,
}

impl Evidence {
    /// The writer its evidence names, if any.
    pub fn writer(&self) -> Option<AgentRole> {
        match self {
            Self::Confirmed { writer, .. }
            | Self::Suspected { writer }
            | Self::AwaitingContent { writer }
            | Self::Discarded { writer } => Some(*writer),
            Self::Detected => None,
        }
    }

    pub fn is_confirmed(&self) -> bool {
        matches!(self, Self::Confirmed { .. })
    }

    /// Whether its evidence is a co-access through a resource.
    pub fn is_co_access(&self) -> bool {
        matches!(
            self,
            Self::Suspected { .. } | Self::AwaitingContent { .. } | Self::Discarded { .. }
        )
    }
}

/// When a confirmed transmission was confirmed, relative to its opening.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Timing {
    /// Anywhere settled, before the watermark.
    Settled,
    /// Settled, and confirmed in a later bucket than the one it opened in,
    /// so counting by confirmation time and by opening time disagree.
    ConfirmedInLaterBucket,
}

/// A merge of `alias` into `into` in the identity resolver's log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeFact {
    pub role: MergeRole,
    pub alias: AgentRole,
    pub into: AgentRole,
    pub by: MergeBy,
    /// Reverted by an operator, leaving a veto between the two.
    pub reverted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MergeBy {
    Resolver,
    Operator,
}

/// A past promotion of a discovered channel under `pattern`, superseding
/// the discovered channels the pattern covered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromotionFact {
    pub channel: ChannelRole,
    pub pattern: ResourcePattern,
    pub policy: PolicyKind,
    pub supersedes: Vec<ChannelRole>,
}

/// Operator policy decisions on a channel, oldest first, beyond what config
/// declared.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyFact {
    pub channel: ChannelRole,
    pub decisions: NonEmpty<PolicyKind>,
}

/// Operator verdicts on a transmission, oldest first; `None` withdraws.
#[derive(Debug, Clone, PartialEq)]
pub struct VerdictFact {
    pub transmission: TransmissionRole,
    pub verdicts: NonEmpty<Option<Verdict>>,
}

/// Content retention dropped one side's message body of a confirmed
/// transmission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BodyDroppedFact {
    pub transmission: TransmissionRole,
    pub side: BodySide,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BodySide {
    /// The sender's originating message.
    Sender,
    /// The reader's input.
    Reader,
}

/// The topic catalog's history: a version retention dropped, then an
/// older version still retained whose lineage to its successor leaves at
/// least one topic without a link at the default remap threshold, then the
/// active version. Every confirmed transmission is classified under the
/// active version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TopicHistoryFact;

/// An enabled watched-topic rule on the older retained version that the
/// re-fit left stale (`TopicsUnmapped`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StaleRuleFact {
    pub rule: RuleRole,
}

/// Dead letters in at least `groups` consumer groups.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DeadLetterFact {
    pub groups: std::num::NonZeroU8,
}

macro_rules! into_fact {
    ($($variant:ident($ty:ty)),* $(,)?) => {
        $(impl From<$ty> for Fact {
            fn from(fact: $ty) -> Self {
                Self::$variant(fact)
            }
        })*
    };
}

into_fact! {
    Agent(AgentFact),
    Resource(ResourceFact),
    Channel(ChannelFact),
    Access(AccessFact),
    Transmission(TransmissionFact),
    Merge(MergeFact),
    Promotion(PromotionFact),
    Policy(PolicyFact),
    Verdicts(VerdictFact),
    BodyDropped(BodyDroppedFact),
    TopicHistory(TopicHistoryFact),
    StaleRule(StaleRuleFact),
    DeadLetters(DeadLetterFact),
}
