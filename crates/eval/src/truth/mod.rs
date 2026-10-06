//! Ground truth: eval-owned, serialisable labels.
//!
//! An [`Expectation`] is one of:
//!
//! - a [`ExpectedTransmission`]: one agent's text must be found in another
//!   agent's input at a given reader exchange and location;
//! - an [`ExpectedAccess`]: the same, through a channel, but one the
//!   detector can only suspect: a co-access with no content to confirm it
//!   (a write that carries no spans, such as a `git push`; or content the
//!   sender never wrote to the resource, INV-963);
//! - a [`NegativeControl`]: a pair, exchange or location where a detector
//!   must **not** report a transmission (a rejected send, text both agents
//!   got from a shared source, harness boilerplate, a scripted sender);
//! - an [`AgentCluster`]: agent keys that name one agent, for identity tests;
//! - an [`Exemption`]: a reader exchange and location where a prediction is
//!   unjudged (content from a sender the dataset cannot name).
//!
//! Labels are built through checked constructors (and deserialised through
//! the same checks), and written as JSONL ([`jsonl`]) so reports can cite
//! them.

pub mod jsonl;
pub mod kinds;

use serde::{Deserialize, Serialize};

pub use kinds::{CarrierKind, MatchNeed, Tier};

use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::derived::flow::transmission::DelegationDirection;
use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::ids::ExchangeId;

use crate::keys::{AgentKey, SourceRef};
use crate::location::SpanLocationExt;

/// How the content is expected to travel: the spec's `Route`, with a channel
/// named by the canonical resource it was read through.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RouteExpectation {
    Channel { resource: Locator },
    Delegation { direction: DelegationDirection },
    Direct,
    Unobserved,
}

impl RouteExpectation {
    pub fn kind(&self) -> RouteKind {
        match self {
            Self::Channel { .. } => RouteKind::Channel,
            Self::Delegation { .. } => RouteKind::Delegation,
            Self::Direct => RouteKind::Direct,
            Self::Unobserved => RouteKind::Unobserved,
        }
    }
}

/// Text and where it sits in the reader's message: `text` is exactly the
/// bytes `at` cuts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpectedContent {
    pub text: String,
    pub at: SpanLocation,
}

/// The fields of an expected transmission, before checking.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransmissionLabel {
    pub from: AgentKey,
    pub to: AgentKey,
    /// The sender's exchange whose response holds the text, when the sender
    /// made one (a scripted sender makes none).
    pub sender_exchange: Option<ExchangeId>,
    /// The first exchange of the reader whose input carries the text.
    pub reader_exchange: ExchangeId,
    pub route: RouteExpectation,
    pub carrier: CarrierKind,
    pub content: ExpectedContent,
    pub needs: MatchNeed,
    pub tier: Tier,
    pub source: SourceRef,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidLabel {
    #[error("sender and reader are the same agent ({0})")]
    SelfTransmission(AgentKey),
    #[error("{from} and {to} are in different worlds")]
    CrossWorld { from: AgentKey, to: AgentKey },
    #[error("content text is {text} bytes but its location covers {location}")]
    ContentLength { text: usize, location: u32 },
    #[error("a negative control needs a reader exchange, a location or an origin")]
    Unbounded,
    #[error("an agent cluster needs at least two agents")]
    SmallCluster,
    #[error("a label's need is out of reach (undecodable or unobserved) exactly when its tier is")]
    Reach,
    #[error("an access-only label needs a channel route: access evidence names a resource")]
    AccessOffChannel,
}

fn check_pair(from: &AgentKey, to: &AgentKey) -> Result<(), InvalidLabel> {
    if from == to {
        return Err(InvalidLabel::SelfTransmission(from.clone()));
    }
    if from.world != to.world {
        return Err(InvalidLabel::CrossWorld {
            from: from.clone(),
            to: to.clone(),
        });
    }
    Ok(())
}

/// One transmission a detector should report. Built only through
/// [`ExpectedTransmission::new`]: sender and reader differ and share a world,
/// the content text is as long as its location, and it needs an
/// undecodable codec exactly when its tier is out of reach.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "TransmissionLabel", into = "TransmissionLabel")]
pub struct ExpectedTransmission(TransmissionLabel);

impl ExpectedTransmission {
    pub fn new(label: TransmissionLabel) -> Result<Self, InvalidLabel> {
        check_pair(&label.from, &label.to)?;
        let text = label.content.text.len();
        if u32::try_from(text).ok() != Some(label.content.at.len()) {
            return Err(InvalidLabel::ContentLength {
                text,
                location: label.content.at.len(),
            });
        }
        if label.needs.out_of_reach() != (label.tier == Tier::OutOfReach) {
            return Err(InvalidLabel::Reach);
        }
        Ok(Self(label))
    }

    pub fn label(&self) -> &TransmissionLabel {
        &self.0
    }
}

impl TryFrom<TransmissionLabel> for ExpectedTransmission {
    type Error = InvalidLabel;

    fn try_from(label: TransmissionLabel) -> Result<Self, Self::Error> {
        Self::new(label)
    }
}

impl From<ExpectedTransmission> for TransmissionLabel {
    fn from(expected: ExpectedTransmission) -> Self {
        expected.0
    }
}

/// A transmission a detector should see only as an access pattern: the
/// sender wrote a resource and the reader read it, but no content links
/// the two. The write carries no spans (a `git push`: its content is not
/// in the call, `WritePayload::Unseen`), or the reader got the sender's
/// content from a resource the sender never wrote (INV-963,
/// `flow.route.shared-upstream-stays-suspected`). The detector suspects
/// the channel on its co-access and never confirms it. Only access
/// evidence (a suspected or discarded prediction) finds the label, and it
/// is scored apart from content recall, under access-only recall.
///
/// Built only through [`ExpectedAccess::new`]: a valid
/// [`ExpectedTransmission`] whose route is a channel, since access evidence
/// always names a resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "TransmissionLabel", into = "TransmissionLabel")]
pub struct ExpectedAccess(ExpectedTransmission);

impl ExpectedAccess {
    pub fn new(label: TransmissionLabel) -> Result<Self, InvalidLabel> {
        if !matches!(label.route, RouteExpectation::Channel { .. }) {
            return Err(InvalidLabel::AccessOffChannel);
        }
        ExpectedTransmission::new(label).map(Self)
    }

    /// The transmission, for alignment.
    pub fn transmission(&self) -> &ExpectedTransmission {
        &self.0
    }

    pub fn label(&self) -> &TransmissionLabel {
        self.0.label()
    }
}

impl TryFrom<TransmissionLabel> for ExpectedAccess {
    type Error = InvalidLabel;

    fn try_from(label: TransmissionLabel) -> Result<Self, Self::Error> {
        Self::new(label)
    }
}

impl From<ExpectedAccess> for TransmissionLabel {
    fn from(expected: ExpectedAccess) -> Self {
        expected.0.0
    }
}

/// Why a negative control must not yield an edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NegativeReason {
    /// The sender tried to send it and the send failed: never delivered.
    RejectedSend,
    /// Both agents got the text from one source (a shared system prompt
    /// template, a shared database), not from each other.
    SharedSource,
    /// Harness text addressed to the reader, not written by the sender.
    Boilerplate,
    /// The sender is scripted and made no exchange, so nothing it "said"
    /// originated in an exchange the gateway could see.
    NoSenderExchange,
    /// The reader read back what it wrote itself: no other agent was
    /// involved. The only control whose sender and reader are one agent,
    /// so a detector that splits one agent in two is charged here.
    SelfRead,
    /// The reader read text it had already read earlier in the same
    /// session: the transmission is at the first read, not this one.
    Reread,
    /// The read found nothing (no page, or an empty one): nobody's text
    /// arrived in it.
    Miss,
}

/// The fields of a negative control, before checking.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NegativeLabel {
    pub from: AgentKey,
    pub to: AgentKey,
    /// The reader exchange the control covers; `None` covers every exchange
    /// of `to` (then `at` bounds it).
    pub reader_exchange: Option<ExchangeId>,
    /// The reader-side location the control covers; `None` covers the whole
    /// exchange.
    pub at: Option<SpanLocation>,
    /// The sender-side location the control covers: a prediction falls
    /// under it only when its matched span overlaps this (a rejected
    /// message's text). `None` puts no condition on the span.
    pub origin: Option<SpanLocation>,
    /// The text concerned, for reports (the rejected message, the shared
    /// passage).
    pub text: Option<String>,
    pub reason: NegativeReason,
    pub tier: Tier,
    pub source: SourceRef,
}

/// A place where a prediction from `from` to `to` is wrong. Built only
/// through [`NegativeControl::new`]: the pair is valid (sender and reader
/// may be one agent only for [`NegativeReason::SelfRead`]) and it names a
/// reader exchange, a location or an origin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "NegativeLabel", into = "NegativeLabel")]
pub struct NegativeControl(NegativeLabel);

impl NegativeControl {
    pub fn new(label: NegativeLabel) -> Result<Self, InvalidLabel> {
        match label.reason {
            NegativeReason::SelfRead if label.from == label.to => {}
            _ => check_pair(&label.from, &label.to)?,
        }
        if label.reader_exchange.is_none() && label.at.is_none() && label.origin.is_none() {
            return Err(InvalidLabel::Unbounded);
        }
        Ok(Self(label))
    }

    pub fn label(&self) -> &NegativeLabel {
        &self.0
    }
}

impl TryFrom<NegativeLabel> for NegativeControl {
    type Error = InvalidLabel;

    fn try_from(label: NegativeLabel) -> Result<Self, Self::Error> {
        Self::new(label)
    }
}

impl From<NegativeControl> for NegativeLabel {
    fn from(control: NegativeControl) -> Self {
        control.0
    }
}

/// Agent keys that are one agent, for identity tests. At least two keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "ClusterLabel", into = "ClusterLabel")]
pub struct AgentCluster(ClusterLabel);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterLabel {
    pub agents: Vec<AgentKey>,
    pub tier: Tier,
    pub source: SourceRef,
}

impl AgentCluster {
    pub fn new(label: ClusterLabel) -> Result<Self, InvalidLabel> {
        if label.agents.len() < 2 {
            return Err(InvalidLabel::SmallCluster);
        }
        Ok(Self(label))
    }

    pub fn label(&self) -> &ClusterLabel {
        &self.0
    }
}

impl TryFrom<ClusterLabel> for AgentCluster {
    type Error = InvalidLabel;

    fn try_from(label: ClusterLabel) -> Result<Self, Self::Error> {
        Self::new(label)
    }
}

impl From<AgentCluster> for ClusterLabel {
    fn from(cluster: AgentCluster) -> Self {
        cluster.0
    }
}

/// Why a place is exempt from judging.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExemptionReason {
    /// Content did arrive here from another agent, but the dataset does not
    /// know which one wrote it (a read whose write was never logged).
    UnknownSender,
}

/// A place in one reader exchange where any prediction is unjudged:
/// neither correct nor false, whatever the world's coverage. It keeps a
/// world `Complete` while admitting the few deliveries its truth could not
/// attribute. Every field is required, so an exemption is always bounded
/// to one reader exchange and one location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Exemption {
    pub to: AgentKey,
    pub reader_exchange: ExchangeId,
    pub at: SpanLocation,
    /// The text concerned, for reports.
    pub text: Option<String>,
    pub reason: ExemptionReason,
    pub tier: Tier,
    pub source: SourceRef,
}

/// One label.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "expect", content = "label", rename_all = "snake_case")]
pub enum Expectation {
    Transmission(ExpectedTransmission),
    /// A transmission only access evidence should find.
    AccessOnly(ExpectedAccess),
    NoTransmission(NegativeControl),
    AgentCluster(AgentCluster),
    /// Predictions here are unjudged.
    Unjudged(Exemption),
}
