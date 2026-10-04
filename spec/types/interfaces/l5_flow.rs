//! L5 flow detection: accesses, channels and transmissions. Consumer group
//! `flow`.
//!
//! Triggered by `ConversationDelta` (tool calls and results become
//! accesses), `ContentMatched` (confirms transmissions), the clock (evidence
//! windows and idle windows close) and `PolicyChanged`. The surface calls
//! it directly to promote a discovered channel (`ChannelRegistry::promote`)
//! and to dismiss a suspected transmission (`TransmissionReview::dismiss`).
//!
//! Policy: the flow consumer turns each `PolicyChanged` into a
//! [`PolicyDecision`] and records it with [`ChannelRegistry::set_policy`].
//! A `PolicyChanged` carrying `Policy::Unreviewed(None)` holds no decision;
//! it is a permanent failure (logged at warn and acked, not retried). Config
//! decisions take the same path: a declared channel's initial policy, when it
//! carries a decision, and every policy a config reload changes.
//!
//! Implementations:
//! - `ResourceExtractor`: `WebFetchExtractor`, `HttpToolExtractor`,
//!   `BashExtractor` (tree-sitter-bash), `FileToolExtractor`, `McpExtractor`,
//!   `UrlScanFallback`.
//! - `ChannelRegistry`: `PgChannelRegistry`.
//! - `Correlator`: `WindowedCorrelator`, which buffers evidence that arrives
//!   out of order. A content match can be processed before the access that
//!   opens its transmission, because they come from different consumer
//!   groups. A tool-result match whose call never yields an access opens a
//!   `Direct(ToolResult)` transmission when its window closes.
//!
//! The correlator chooses routes in the precedence order documented on
//! `Route`, using the `AgentDirectory` and agent parent links for
//! `Delegation`.
//!
//! Promotion (`ChannelRegistry::promote`) follows
//! [`promotion::plan`](crate::derived::flow::channel::promotion::plan): in
//! one transaction the channel becomes declared under the same id, the
//! operator's policy decision is recorded in its policy history, and every
//! other discovered channel whose seed the pattern matches becomes
//! superseded by it; then one `ChannelPromoted` is published. Superseded
//! channels are aliases: `ChannelDirectory` resolves them, every reader of
//! stored channel ids goes through it, and nothing stored is rewritten. A
//! `PolicyChanged` for a superseded channel (published before the
//! promotion committed) is a permanent failure: logged at warn with the
//! channel and its superseding channel, and acked.

use crate::aggregates::access::ResourceUsePage;
use crate::derived::flow::access::{Access, AccessKind, Extraction};
use crate::derived::flow::channel::policy::{
    Policy, PolicyAuthor, PolicyDecision, PolicyHistory, Recorded,
};
use crate::derived::flow::channel::promotion::{Promotion, PromotionRefusal};
use crate::derived::flow::evidence::CoAccess;
use crate::derived::flow::resource::{Locator, ResourcePattern};
use crate::derived::flow::transmission::{Confirmed, Dismissal, NonChannelRoute};
use crate::derived::provenance::matching::ContentMatch;
use crate::ids::{AgentId, ChannelId, TransmissionId};
use crate::observed::message::{ToolCall, ToolResult};
use crate::paging::{PageRequest, ResourceUseList};
use crate::support::{NonEmpty, TimeWindow, Timestamp};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractedAccess {
    pub kind: AccessKind,
    pub locator: Locator,
    pub via: Extraction,
}

pub trait ResourceExtractor {
    /// Whether this extractor understands the tool at all.
    fn handles(&self, call: &ToolCall) -> bool;

    /// Accesses implied by one call. `result` is present once the harness has
    /// sent it back (in the next request, or in the same response for
    /// server-side tools); reads are only recorded with a result.
    fn extract(
        &self,
        call: &ToolCall,
        result: Option<&ToolResult>,
    ) -> Result<Vec<ExtractedAccess>, ExtractError>;
}

/// Where a locator belongs. Never names a superseded channel: a resource of
/// a superseded channel is `Known` on the channel that superseded it, so a
/// superseded channel accepts no new resources or accesses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelLookup {
    /// Already a resource of this channel, or of a channel it superseded.
    Known(ChannelId),
    /// First sighting, but it matches a declared channel's pattern.
    Declared(ChannelId),
    /// Matches nothing: the caller creates a discovered channel.
    New,
}

/// The supersession table. Every reader of stored channel ids (routes,
/// accesses, edges, filters, alert subjects, graph nodes) resolves them
/// through it, as agents resolve through `AgentDirectory`.
pub trait ChannelDirectory {
    /// The channel `id` resolves to: the promoted channel that superseded
    /// it, else itself (`Channel::canonical`). One step: a superseding
    /// channel is declared, so never superseded, and `canonical` of a
    /// canonical id is that id.
    fn canonical(&self, id: ChannelId) -> ChannelId;
}

/// What an accepted promotion changed besides the channel's origin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Promoted {
    /// The channels it superseded, as in `PromotionPlan::superseded`.
    pub superseded: Vec<ChannelId>,
    /// Where the operator's policy decision landed in the channel's policy
    /// history: `Current` unless a later-timed decision already exists.
    pub policy: Recorded,
}

pub trait ChannelRegistry {
    async fn lookup(&self, locator: &Locator) -> Result<ChannelLookup, RegistryError>;

    /// Declare a channel from config. When `policy` carries a decision, it
    /// is the first entry of the channel's [`PolicyHistory`].
    async fn declare(
        &mut self,
        pattern: ResourcePattern,
        policy: Policy,
        by: PolicyAuthor,
    ) -> Result<ChannelId, RegistryError>;

    /// Record a decision in the channel's [`PolicyHistory`] and set the
    /// channel's policy to the history's current one, in one transaction.
    /// Idempotent: a redelivered decision returns `Recorded::Duplicate` and
    /// changes nothing. A decision older than the current one is kept in the
    /// history and returns `Recorded::Superseded`. A superseded channel
    /// takes no decisions: `Superseded { channel, by }`, changing nothing.
    async fn set_policy(
        &mut self,
        channel: ChannelId,
        decision: PolicyDecision,
    ) -> Result<Recorded, RegistryError>;

    /// Every decision recorded for the channel, oldest first.
    async fn policy_history(&self, channel: ChannelId) -> Result<PolicyHistory, RegistryError>;

    /// Promote the discovered channel `channel` with `promotion`, as
    /// [`promotion::plan`] decides, in one transaction: its origin becomes
    /// [`ChannelOrigin::promoted`] with the promotion's declaration (id,
    /// resources and detection unchanged); the promotion's policy decision is
    /// recorded as by [`ChannelRegistry::set_policy`]; every channel the plan
    /// supersedes becomes [`ChannelOrigin::Superseded`] by `channel` at the
    /// promotion time. Afterwards, lookups of unseen locators that match the
    /// pattern return `Declared(channel)`, and lookups of a superseded
    /// channel's resources return `Known(channel)`. Publishes one
    /// `ChannelPromoted` after commit.
    ///
    /// A refusal ([`PromotionRefusal`]) changes nothing.
    ///
    /// [`promotion::plan`]: crate::derived::flow::channel::promotion::plan
    /// [`ChannelOrigin::promoted`]: crate::derived::flow::channel::ChannelOrigin::promoted
    /// [`ChannelOrigin::Superseded`]: crate::derived::flow::channel::ChannelOrigin::Superseded
    async fn promote(
        &mut self,
        channel: ChannelId,
        promotion: Promotion,
    ) -> Result<Promoted, PromoteError>;

    /// The resources of `channel`'s canonical channel (its own and those of
    /// every channel it superseded) accessed within `window`, newest
    /// resource first, with their canonical writers and readers and how
    /// often each accessed it in the window
    /// ([`ResourceUse`](crate::aggregates::access::ResourceUse)). Agents are
    /// resolved through `AgentDirectory`, summing merged aliases.
    async fn resource_use(
        &self,
        channel: ChannelId,
        window: TimeWindow,
        page: &PageRequest<ResourceUseList>,
    ) -> Result<ResourceUsePage, RegistryError>;
}

/// What the correlator decided. The flow consumer applies these to stored
/// transmissions and publishes the matching events.
#[derive(Debug, Clone, PartialEq)]
pub enum TransmissionUpdate {
    /// A cross access on a channel: wait for content evidence.
    OpenChannel {
        transmission: TransmissionId,
        to: AgentId,
        channel: ChannelId,
        co_access: CoAccess,
    },
    /// Content evidence on a non-channel route: opens and confirms at once.
    OpenConfirmed {
        transmission: TransmissionId,
        to: AgentId,
        route: NonChannelRoute,
        confirmed: Confirmed,
    },
    /// A later match from the same sender to the same reader exchange.
    Extend {
        transmission: TransmissionId,
        content: ContentMatch,
    },
    Confirm {
        transmission: TransmissionId,
        confirmed: Confirmed,
    },
    Suspect {
        transmission: TransmissionId,
        co_access: NonEmpty<CoAccess>,
    },
    /// A suspected transmission's window expired: Discarded, `Expired`.
    Discard { transmission: TransmissionId },
    /// An operator dismissed a suspected transmission: Discarded,
    /// `Dismissed`.
    Dismiss {
        transmission: TransmissionId,
        dismissal: Dismissal,
    },
}

/// Owns the open-evidence windows. Runs in one task per flow shard and is
/// fed over a channel, so it takes `&mut self` and does no I/O.
pub trait Correlator {
    fn on_access(&mut self, access: &Access, channel: ChannelId) -> Vec<TransmissionUpdate>;

    fn on_match(
        &mut self,
        content: &ContentMatch,
        channel: Option<ChannelId>,
    ) -> Vec<TransmissionUpdate>;

    /// Close evidence windows and expire suspected transmissions up to `now`.
    fn on_tick(&mut self, now: Timestamp) -> Vec<TransmissionUpdate>;

    /// An operator dismissed `transmission`. Returns `Dismiss` and releases
    /// its state if it is suspected in this shard; otherwise `NotSuspected`
    /// (it was confirmed, discarded or is still awaiting content) or
    /// `UnknownTransmission`. Fed through the shard's input channel like
    /// every other input, so a dismissal and a late content match for the
    /// same transmission are applied in one order and exactly one wins.
    fn on_dismiss(
        &mut self,
        transmission: TransmissionId,
        dismissal: Dismissal,
    ) -> Result<TransmissionUpdate, DismissError>;
}

/// Operator review of transmissions, called by the surface. The
/// implementation routes each request to the correlator shard that owns the
/// transmission and waits for its answer.
pub trait TransmissionReview {
    /// Dismiss a suspected transmission. On success the flow consumer has
    /// stored it as Discarded with reason `Dismissed` and published one
    /// `TransmissionDismissed`.
    async fn dismiss(
        &self,
        transmission: TransmissionId,
        dismissal: Dismissal,
    ) -> Result<(), DismissError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtractError {
    /// The arguments were not valid JSON for this tool's schema.
    Arguments { reason: String },
    /// A shell or code argument failed to parse.
    Parse { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    Store {
        reason: String,
    },
    UnknownChannel(ChannelId),
    /// The pattern overlaps an existing declared channel's pattern.
    OverlappingDeclaration {
        existing: ChannelId,
    },
    /// A policy decision for a channel that `by` superseded.
    Superseded {
        channel: ChannelId,
        by: ChannelId,
    },
    /// A cursor the registry did not issue, or issued for another channel
    /// or window.
    InvalidCursor,
}

/// Why `ChannelRegistry::promote` failed. A refusal is the operator's to
/// fix; a store failure may succeed on retry. Neither changed anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromoteError {
    Store { reason: String },
    Refused(PromotionRefusal),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DismissError {
    Store {
        reason: String,
    },
    UnknownTransmission(TransmissionId),
    /// Only suspected transmissions can be dismissed.
    NotSuspected(TransmissionId),
}
