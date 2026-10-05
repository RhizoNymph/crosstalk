//! L5 flow detection: accesses, channels and transmissions. Consumer group
//! `flow`.
//!
//! Triggered by `ConversationDelta` (tool calls and results become
//! accesses), `ContentMatched` (confirms transmissions), the clock (evidence
//! windows and idle windows close) and `PolicyChanged`. The surface calls
//! it directly to promote a discovered channel (`ChannelRegistry::promote`),
//! and records operator verdicts on transmissions through
//! [`verdicts::TransmissionVerdicts`], which publishes `VerdictSet`. A
//! verdict never changes a transmission's state.
//!
//! Policy: the flow consumer turns each `PolicyChanged` into a
//! [`PolicyDecision`] and records it with [`ChannelRegistry::set_policy`].
//! A `PolicyChanged` carrying `Policy::Unreviewed(None)` holds no decision;
//! it is a permanent failure (logged at warn and acked, not retried). Config
//! decisions take the same path: a declared channel's initial policy, when it
//! carries a decision, and every policy a config reload changes.
//!
//! After every committed change to a stored channel (discovery, a
//! declaration, a new resource, any detection change including turning
//! dormant, a recorded policy decision) the registry publishes
//! `Changed::Channel` for it; after a promotion, for the promoted channel and every channel it
//! superseded ([`Changed::promotion`]). A `PolicyChanged` is announced to
//! the UI only this way, once recorded, never by the surface that published
//! it. The verdict store publishes `Changed::Verdict` for each appended
//! verdict record. A merge or unmerge changes no stored channel, though it
//! can change a channel's confirmation and listing at read time (hiding a
//! discovered channel or listing it again); the `Changed::Agent` the merge
//! publishes is what readers re-query channels on.
//!
//! [`Changed::promotion`]: crate::events::changed::Changed::promotion
//!
//! The flow consumer writes what it sees through
//! [`channels::ChannelTraffic`] (a resource, an access, a discovery, the
//! state of each channel transmission, a detection change) and stores each
//! transmission state the correlator decides through
//! [`transmissions::TransmissionStore`]. It publishes the events of its own
//! computation once the write commits: `AccessRecorded`,
//! `DeclaredChannelUnused` (the idle window closed),
//! `ChannelCrossAccessed`, `TransmissionConfirmed` and
//! `TransmissionSuspected` (the correlator's updates). Whether a discovery
//! creates a channel is the registry's decision, taken in its transaction,
//! so the registry publishes `ChannelDiscovered`
//! ([`channels::ChannelTraffic::discover`]). The surface reads stored
//! channels, with their cross-agent traffic, through
//! [`channels::ChannelReads`].
//!
//! **Discovery.** A resource is only a resource until a transmission
//! between two different agents goes through it. Its first sighting looks
//! the locator up ([`ChannelRegistry::lookup`]); a locator on no channel
//! and matching no declared pattern is [`ChannelLookup::NoChannel`]: the
//! resource is stored on no channel, its accesses are recorded on it
//! (`AccessRecorded` with no channel) and no channel is created, whatever
//! happens on it next, until the correlator pairs a write by one agent
//! with a later read by another ([`CoAccess`]) on that resource. That
//! co-access opens a channel transmission
//! ([`TransmissionUpdate::OpenChannel`] with [`OpensOn::Resource`]), and the
//! flow consumer then discovers the channel seeded by the resource and that
//! transmission ([`channels::ChannelTraffic::discover`]: `Active` since the
//! transmission opened, `Unreviewed`; the registry publishes
//! `ChannelDiscovered`, which raises `NewChannel`, and `Changed::Channel`),
//! stores the transmission routed through the channel `discover` returned
//! and records it as that channel's traffic
//! ([`channels::ChannelTraffic::record_transmission`]). Every channel
//! transmission is opened this way, by a co-access between two agents, so
//! a channel is never created by a resource only one agent touches, nor by
//! writes nobody else reads. Accesses recorded on the resource before
//! discovery count on the channel from then on, at read time (L7 buckets
//! accesses by resource). A declared channel's resources are on it from
//! their first sighting, but it too counts as carrying traffic only once a
//! cross-agent transmission goes through it (`DeclaredDetection::InUse`).
//!
//! **Resources of a discovered channel.** A discovered channel holds exactly
//! its seed resource: a resource joins a channel only through a declared
//! pattern (a declaration, or a promotion, which also supersedes the
//! discovered channels whose seeds its pattern matches). Any other resource
//! stays a resource until a cross-agent transmission through it discovers a
//! channel of its own, so no resource counts as a channel's traffic without
//! a cross-agent transmission through it or through the pattern an operator
//! or config declared.
//!
//! Implementations:
//! - `ResourceExtractor`: `WebFetchExtractor`, `HttpToolExtractor`,
//!   `BashExtractor` (tree-sitter-bash), `FileToolExtractor`, `McpExtractor`,
//!   `UrlScanFallback`.
//! - `ChannelRegistry`, `ChannelTraffic`, `ChannelReads`:
//!   `PgChannelRegistry`.
//! - `TransmissionStore`, `TransmissionVerdicts`: `PgTransmissionStore`.
//! - `Correlator`: `WindowedCorrelator`, which buffers evidence that arrives
//!   out of order. A content match can be processed before the access that
//!   opens its transmission, because they come from different consumer
//!   groups. A tool-result match whose call never yields an access opens a
//!   `Direct(ToolResult)` transmission when its window closes.
//!
//! The correlator chooses routes in the precedence order documented on
//! `Route`, using the `AgentDirectory` and agent parent links for
//! `Delegation`. Shards are keyed by canonical channel
//! (`ChannelDirectory`), or by resource for an access on a resource on no
//! channel. On `ChannelPromoted` the evidence a shard holds for a
//! superseded channel moves to the promoted channel's shard, so a write
//! recorded before a promotion and a read recorded after it still meet;
//! likewise, when a channel is discovered from a resource, the evidence
//! held for that resource moves to the new channel's shard.
//!
//! Promotion (`ChannelRegistry::promote`) follows
//! [`promotion::plan`](crate::derived::flow::channel::promotion::plan), run
//! with the promotion's declaration over the stored channels; the surface's
//! promotion preview reads `ChannelRegistry::promotion_coverage`, which runs
//! the same plan over the same channels and changes nothing. A promotion: in
//! one transaction the channel becomes declared under the same id, the
//! operator's policy decision is recorded in its policy history, and every
//! other discovered channel whose seed the pattern matches becomes
//! superseded by it; then one `ChannelPromoted` is published. Superseded
//! channels are aliases: `ChannelDirectory` resolves them, every reader of
//! stored channel ids goes through it, and nothing stored is rewritten. A
//! `PolicyChanged` for a superseded channel (published before the
//! promotion committed) is a permanent failure: logged at warn with the
//! channel and its superseding channel, and acked.
//!
//! **Detection follows resolution.** A cross-agent transmission, opened or
//! confirmed, advances the detection of the channel its route resolves to
//! (`ChannelDirectory::canonical`), never of a superseded one. A transmission opened on a channel before a
//! promotion superseded it keeps that channel id in its stored route, and
//! its content can arrive after the promotion (the `provenance` and `flow`
//! groups lag independently); when it is confirmed, the flow consumer
//! updates the superseding channel's `TrafficDetection` exactly as for a
//! confirmation routed to that channel (it becomes or stays `Active`, with
//! `last_transmission` naming the transmission) and publishes
//! `Changed::Channel` for it. The superseded channel's own detection stays
//! frozen as it was at the promotion. This is the read-time resolution
//! every reader applies: the route counts on the canonical channel in edges
//! and channel rows, and the canonical channel's policy judges the traffic,
//! so its detection is the one that records it.
//!
//! **Write outcomes.** A write is recorded only once its outcome is final
//! ([`WriteOutcome`]): the flow consumer holds a tool call that writes
//! until the call's result arrives (normally in the writer's next request
//! of the conversation; in the same response for a server tool) and then
//! extracts it with the result, or, at the first tick at or after
//! `CorrelationTiming::write_settles_at` of the call's exchange time, with
//! none, which makes the write `Unknown`
//! (`flow.correlator.write-held-until-outcome`). A result arriving after
//! that changes nothing: an access is recorded once. Every write is then
//! recorded and published as `AccessRecorded` whatever its outcome
//! (`flow.access.rejected-write-recorded`): an attempted write is a signal
//! in itself, and it counts as a write in resource use and access buckets.
//! The correlator pairs `Delivered` and `Unknown` writes, `Unknown` at lower
//! confidence (its outcome, as an access's `Extraction` is), and never a
//! `Rejected` one: `CoAccess::new` refuses it
//! (`flow.coaccess.write-not-rejected`), and a content match is never
//! attributed to it.
//!
//! **Which write a match came through.** A write's spans are the originated
//! spans in its arguments plus the writer's own earlier spans it relays
//! (`AccessOp::Write`), so a retry after a rejected write carries the
//! rejected write's spans. A content match on such a span links to every
//! write holding it; the rejected ones are left out by their outcome.
//!
//! **Shared upstream source.** A `ToolResult` content match whose reader's
//! call yielded an access on a resource, while the match's origin agent
//! has no write on that resource whose outcome pairs (`Delivered` or
//! `Unknown`, made before the read), is evidence that both agents quote one
//! upstream source (a repository file both read), not of a transmission:
//! it confirms nothing, and a channel transmission it would have confirmed
//! stays `AwaitingContent` and then `Suspected` on its co-access evidence
//! (`flow.route.shared-upstream-stays-suspected`). A tool result whose call
//! yielded no access is unaffected: it opens `Direct(ToolResult)` as
//! before (`flow.route.tool-result-without-access`). In SWE trajectory
//! corpora about 14.5% of an agent's novel shingles reappear in another
//! task on the same repository because both quote the same file, yet only
//! 0.18% of trajectory pairs share 20 or more of them; a per-agent-pair
//! minimum of shared spans is a possible later configuration knob if
//! evaluation shows this rule insufficient.
//!
//! **Timing.** The correlator is configured with a [`CorrelationTiming`]: it
//! pairs a write and a read within `correlation_window`, opens a channel
//! transmission `AwaitingContent` until `window_closes_at(read.at)`, keeps it
//! `Suspected` until `expires_at(since)`, and opens a pending tool-result
//! match as `Direct(ToolResult)` at `window_closes_at` of its exchange's
//! time. So every confirmation it emits while processing an input has a
//! `Confirmed::at` no earlier than the input's event time or its last tick
//! minus `settle_after`, whichever is earlier; L7's watermark depends on it.
//! Each shard records the last tick it processed, which L7 reads as
//! `PipelineFrontier::ticked_through`.
//!
//! [`CorrelationTiming`]: crate::derived::flow::timing::CorrelationTiming

pub mod channels;
pub mod transmissions;
pub mod verdicts;

use crate::aggregates::access::ResourceUsePage;
use crate::derived::flow::access::{Access, AccessKind, Extraction, WriteOutcome};
use crate::derived::flow::channel::Declaration;
use crate::derived::flow::channel::policy::{
    Policy, PolicyAuthor, PolicyDecision, PolicyHistory, Recorded,
};
use crate::derived::flow::channel::promotion::{Promotion, PromotionCoverage, PromotionRefusal};
use crate::derived::flow::evidence::CoAccess;
use crate::derived::flow::resource::{Locator, ResourcePattern};
use crate::derived::flow::transmission::{Confirmed, NonChannelRoute};
use crate::derived::provenance::matching::ContentMatch;
use crate::ids::{AgentId, ChannelId, ResourceId, TransmissionId};
use crate::observed::message::{ToolCall, ToolResult};
use crate::paging::{PageRequest, ResourceUseList};
use crate::support::{NonEmpty, TimeWindow, Timestamp};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractedAccess {
    pub op: ExtractedOp,
    pub locator: Locator,
    pub via: Extraction,
}

/// An extracted access's operation: a write carries its outcome, a read
/// none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtractedOp {
    Write(WriteOutcome),
    Read,
}

impl ExtractedOp {
    pub fn kind(self) -> AccessKind {
        match self {
            Self::Write(_) => AccessKind::Write,
            Self::Read => AccessKind::Read,
        }
    }
}

pub trait ResourceExtractor {
    /// Whether this extractor understands the tool at all.
    fn handles(&self, call: &ToolCall) -> bool;

    /// Accesses implied by one call. `result` is present once the harness has
    /// sent it back (in the next request, or in the same response for
    /// server-side tools); reads are only recorded with a result.
    ///
    /// Each write's [`WriteOutcome`] (`flow.extract.write-outcome-classified`):
    /// `Unknown` without a result (the consumer calls `extract` without one
    /// only once the write's settle window has closed); `Rejected` for a
    /// result whose `ToolOutcome` is `Error`; otherwise what the tool's
    /// content rule reads from the result's content (`Delivered` or
    /// `Rejected`, for a tool that reports refusal in its text, such as a
    /// message tool refusing an over-length message), or, for a tool with no
    /// content rule, `Delivered` for `Success` and `Unknown` for `Unknown`.
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
    /// On no channel yet (first sighting, or only ever a resource), and it
    /// matches a declared channel's pattern: the resource joins that
    /// channel.
    Declared(ChannelId),
    /// On no channel and matching no declared pattern: the resource is
    /// stored on no channel and its accesses are recorded on it alone.
    /// Nothing is created; a channel is discovered from the resource only
    /// when a cross-agent transmission goes through it
    /// ([`ChannelTraffic::discover`](channels::ChannelTraffic::discover)).
    NoChannel,
}

/// Where a co-access opens its channel transmission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpensOn {
    /// The canonical channel the resource is on.
    Channel(ChannelId),
    /// A resource on no channel: this transmission is the first cross-agent
    /// transmission through it, and the flow consumer discovers a channel
    /// from it ([`ChannelTraffic::discover`](channels::ChannelTraffic::discover))
    /// before storing the transmission routed through that channel.
    Resource(ResourceId),
}

/// What [`ChannelTraffic::discover`](channels::ChannelTraffic::discover)
/// found or made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Discovery {
    /// A new discovered channel, seeded by the resource and transmission.
    /// The registry published `ChannelDiscovered` and `Changed::Channel`
    /// from the transaction that created it.
    Created(ChannelId),
    /// The resource was already on this canonical channel (a concurrent
    /// discovery committed first), or a declared pattern claims it and it
    /// joined that channel: the transmission is routed through it and no
    /// `ChannelDiscovered` is published.
    Existing(ChannelId),
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
    /// Where a locator's resource belongs. Creates nothing.
    fn lookup(
        &self,
        locator: &Locator,
    ) -> impl Future<Output = Result<ChannelLookup, RegistryError>> + Send;

    /// Declare a channel from config, as `by` at `at` (the declaration's
    /// time), under a fresh id the registry assigns. When `policy` carries a
    /// decision, it is the first entry of the channel's [`PolicyHistory`].
    /// Publishes `Changed::Channel` for it. `OverlappingDeclaration` when
    /// the pattern overlaps a declared channel's, changing nothing. Resources
    /// already stored on no channel that the pattern matches stay where they
    /// are until their next sighting, when their lookup names this channel.
    fn declare(
        &mut self,
        pattern: ResourcePattern,
        policy: Policy,
        by: PolicyAuthor,
        at: Timestamp,
    ) -> impl Future<Output = Result<ChannelId, RegistryError>> + Send;

    /// Record a decision in the channel's [`PolicyHistory`] and set the
    /// channel's policy to the history's current one, in one transaction.
    /// Idempotent: a redelivered decision returns `Recorded::Duplicate` and
    /// changes nothing. A decision older than the current one is kept in the
    /// history and returns `Recorded::Superseded`. A superseded channel
    /// takes no decisions: `Superseded { channel, by }`, changing nothing.
    fn set_policy(
        &mut self,
        channel: ChannelId,
        decision: PolicyDecision,
    ) -> impl Future<Output = Result<Recorded, RegistryError>> + Send;

    /// Every decision recorded for the channel, oldest first.
    fn policy_history(
        &self,
        channel: ChannelId,
    ) -> impl Future<Output = Result<PolicyHistory, RegistryError>> + Send;

    /// Promote the discovered channel `channel` with `promotion`, as
    /// [`promotion::plan`] decides for `promotion.declaration()` over the
    /// stored channels, in one transaction: its origin becomes
    /// [`ChannelOrigin::promoted`] with the promotion's declaration (id,
    /// resources and detection unchanged); the promotion's policy decision is
    /// recorded as by [`ChannelRegistry::set_policy`]; every channel the plan
    /// supersedes becomes [`ChannelOrigin::Superseded`] by `channel` at the
    /// promotion time. Afterwards, lookups of locators on no channel that
    /// match the pattern return `Declared(channel)`, and lookups of a superseded
    /// channel's resources return `Known(channel)`. Publishes one
    /// `ChannelPromoted` after commit.
    ///
    /// A refusal ([`PromotionRefusal`]) changes nothing.
    ///
    /// [`promotion::plan`]: crate::derived::flow::channel::promotion::plan
    /// [`ChannelOrigin::promoted`]: crate::derived::flow::channel::ChannelOrigin::promoted
    /// [`ChannelOrigin::Superseded`]: crate::derived::flow::channel::ChannelOrigin::Superseded
    fn promote(
        &mut self,
        channel: ChannelId,
        promotion: Promotion,
    ) -> impl Future<Output = Result<Promoted, PromoteError>> + Send;

    /// What `promote` would do now for a promotion with `declaration`,
    /// changing nothing: exactly [`promotion::coverage`] over the channels
    /// `promote` would plan over, read in one snapshot, where a channel's
    /// held resources are its seed resource and every resource stored on it.
    /// A refusal is `Refused` with the `PromotionRefusal` that `promote`
    /// would return in the same state; on success the coverage's superseded
    /// channels are the ones `promote` would report in
    /// [`Promoted::superseded`].
    ///
    /// [`promotion::coverage`]: crate::derived::flow::channel::promotion::coverage
    fn promotion_coverage(
        &self,
        channel: ChannelId,
        declaration: &Declaration,
    ) -> impl Future<Output = Result<PromotionCoverage, PromoteError>> + Send;

    /// The resources of `channel`'s canonical channel (its own and those of
    /// every channel it superseded) accessed within `window`, newest
    /// resource first, with their canonical writers and readers and how
    /// often each accessed it in the window
    /// ([`ResourceUse`](crate::aggregates::access::ResourceUse)). Agents are
    /// resolved through `AgentDirectory`, summing merged aliases.
    fn resource_use(
        &self,
        channel: ChannelId,
        window: TimeWindow,
        page: &PageRequest<ResourceUseList>,
    ) -> impl Future<Output = Result<ResourceUsePage, RegistryError>> + Send;
}

/// What the correlator decided. The flow consumer applies these to stored
/// transmissions and publishes the matching events.
#[derive(Debug, Clone, PartialEq)]
pub enum TransmissionUpdate {
    /// A cross access (a write by one agent, a read by another): wait for
    /// content evidence. On a resource on no channel, the consumer first
    /// discovers the channel the transmission is routed through (module
    /// docs, "Discovery").
    OpenChannel {
        transmission: TransmissionId,
        to: AgentId,
        on: OpensOn,
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
    /// Content evidence for an open channel transmission. Its channel's
    /// detection is advanced on the channel the stored route resolves to,
    /// even when the route names a channel superseded since it opened
    /// (module docs, "Detection follows resolution").
    Confirm {
        transmission: TransmissionId,
        confirmed: Confirmed,
    },
    Suspect {
        transmission: TransmissionId,
        co_access: NonEmpty<CoAccess>,
    },
    /// A suspected transmission's window expired: Discarded.
    Discard { transmission: TransmissionId },
}

/// Owns the open-evidence windows. Runs in one task per flow shard and is
/// fed over a channel, so it takes `&mut self` and does no I/O.
pub trait Correlator {
    /// `channel` is the canonical channel the access's resource is on, or
    /// `None` for a resource on no channel (correlated by the resource).
    fn on_access(&mut self, access: &Access, channel: Option<ChannelId>)
    -> Vec<TransmissionUpdate>;

    /// `channel` is the canonical channel the read's resource is on when
    /// the match is processed, or `None` when the match was not carried by
    /// a tool result reading a channel's resource.
    fn on_match(
        &mut self,
        content: &ContentMatch,
        channel: Option<ChannelId>,
    ) -> Vec<TransmissionUpdate>;

    /// Close evidence windows and expire suspected transmissions up to `now`.
    fn on_tick(&mut self, now: Timestamp) -> Vec<TransmissionUpdate>;
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

/// Why `ChannelRegistry::promote` (or `promotion_coverage`) failed. A
/// refusal is the operator's to fix; a store failure may succeed on retry.
/// Neither changed anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromoteError {
    Store { reason: String },
    Refused(PromotionRefusal),
}
