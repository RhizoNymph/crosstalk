//! L5 flow detection: accesses, channels and transmissions. Consumer group
//! `flow`.
//!
//! Triggered by `ConversationDelta` (tool calls and results become
//! accesses), `ContentMatched` (confirms transmissions), the clock (evidence
//! windows and idle windows close) and `PolicyChanged`. The surface calls
//! it directly to promote a discovered channel (`ChannelRegistry::promote`)
//! and to dismiss a suspected transmission (`TransmissionReview::dismiss`).
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

use crate::derived::flow::access::{Access, AccessKind, Extraction};
use crate::derived::flow::channel::policy::{Policy, PolicyAuthor};
use crate::derived::flow::evidence::CoAccess;
use crate::derived::flow::resource::{Locator, ResourcePattern};
use crate::derived::flow::transmission::{Confirmed, Dismissal, NonChannelRoute};
use crate::derived::provenance::matching::ContentMatch;
use crate::ids::{AgentId, ChannelId, OperatorId, TransmissionId};
use crate::observed::message::{ToolCall, ToolResult};
use crate::support::{NonEmpty, Timestamp};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelLookup {
    /// Already a resource of this channel.
    Known(ChannelId),
    /// First sighting, but it matches a declared channel's pattern.
    Declared(ChannelId),
    /// Matches nothing: the caller creates a discovered channel.
    New,
}

pub trait ChannelRegistry {
    async fn lookup(&self, locator: &Locator) -> Result<ChannelLookup, RegistryError>;

    async fn declare(
        &mut self,
        pattern: ResourcePattern,
        policy: Policy,
        by: PolicyAuthor,
    ) -> Result<ChannelId, RegistryError>;

    async fn set_policy(&mut self, channel: ChannelId, policy: Policy)
    -> Result<(), RegistryError>;

    /// Attach `pattern` to the discovered channel `channel`
    /// ([`ChannelOrigin::promoted`](crate::derived::flow::channel::ChannelOrigin::promoted)),
    /// declared by operator `by` at `at`. Its id, resources, policy and
    /// detection are unchanged, and later lookups of unseen locators that
    /// match the pattern return `Declared(channel)`. Resources already on
    /// other channels stay there (`Known` wins over `Declared`).
    ///
    /// Rejects, changing nothing: an unknown channel, a channel already
    /// declared (`NotDiscovered`), a pattern that does not match the
    /// channel's seed locator (`PatternMissesSeed`), and a pattern that
    /// overlaps another declared channel's (`OverlappingDeclaration`).
    async fn promote(
        &mut self,
        channel: ChannelId,
        pattern: ResourcePattern,
        by: OperatorId,
        at: Timestamp,
    ) -> Result<(), RegistryError>;
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
    /// Promotion of a channel that is already declared.
    NotDiscovered(ChannelId),
    /// A promotion pattern that does not match the channel's own seed.
    PatternMissesSeed,
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
