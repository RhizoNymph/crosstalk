//! Transmissions: one agent-to-agent communication.
//!
//! ```text
//!            ┌──────── match (non-channel route) ───────────┐
//! Detected ──┤                                              ▼
//!            └─ await evidence ─▶ AwaitingContent ─match─▶ Confirmed ─classify─▶ Classified ─aggregate─▶ Aggregated
//!                                      │                    ▲
//!                               window closes          late match
//!                                      ▼                    │
//!                                  Suspected ───────────────┘
//!                                      └─ expire ─▶ Discarded
//! ```
//!
//! **Identity.** A transmission is one (reader exchange, sender, route).
//! Further content matches for the same triple extend the confirmed
//! transmission ([`Confirmed::extend`]); a new reader exchange opens a new
//! transmission.
//!
//! **Route precedence.** When several routes could explain a match, the
//! first that applies wins: `Delegation` (sender and reader are parent and
//! child), then `Channel` (the content arrived in a tool result for an access
//! on a channel), then `Direct`, then `Unobserved`.
//!
//! **Policy.** A channel-routed transmission is judged by the channel policy
//! in force when it was confirmed, on its canonical channel: one opened on a
//! channel that was superseded before confirmation is judged by the
//! superseding channel's policy.
//!
//! **Superseded channels.** A stored `Route::Channel` keeps the channel the
//! access resolved to when it was recorded; readers resolve it through
//! supersession ([`Route::resolved`]).
//!
//! **Discarded is final.** A suspected transmission is discarded only when
//! its window expires ([`TransmissionState::expire`]). Content evidence that
//! arrives afterwards opens a new transmission through the normal path; the
//! discarded one is never revived. Operator verdicts on transmissions are a
//! separate axis.
//!
//! The sender is unknown until content evidence arrives, so it lives inside
//! [`Confirmed`], not on the transmission itself.

use std::num::NonZeroU64;

use crate::aggregates::topic::TopicModelVersion;
use crate::aliases::Aliases;
use crate::derived::flow::evidence::CoAccess;
use crate::derived::provenance::matching::ContentMatch;
use crate::ids::{AgentId, ChannelId, TopicId, TransmissionId};
use crate::observed::message::ToolName;
use crate::support::{NonEmpty, Timestamp};

#[derive(Debug, Clone, PartialEq)]
pub struct Transmission {
    pub id: TransmissionId,
    /// The reader.
    pub to: AgentId,
    pub route: Route,
    pub opened_at: Timestamp,
    pub state: TransmissionState,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Route {
    /// Through a shared resource: written by the sender, read by the reader
    /// through a tool call the gateway resolved to a channel.
    Channel(ChannelId),
    /// Between a parent agent and a sub-agent it spawned (Claude Code's
    /// Task/Agent tool, Codex multi-agent, oh-my-pi tasks).
    Delegation(DelegationDirection),
    /// Placed into the reader's context by something the gateway sees but
    /// that is not a resource: a prompt, or a tool whose call touches no
    /// extracted resource.
    Direct(DirectCarrier),
    /// The reader's own output contains the sender's text, but none of the
    /// reader's visible inputs did: a channel the gateway cannot see.
    Unobserved,
}

impl Route {
    /// The route with its channel resolved through supersession. Stored
    /// transmissions keep the channel they were routed through; readers
    /// resolve it, so a transmission on a superseded channel counts on the
    /// channel that superseded it.
    pub fn resolved(&self, aliases: impl Aliases) -> Self {
        match self {
            Self::Channel(channel) => Self::Channel(aliases.channel(*channel)),
            Self::Delegation(_) | Self::Direct(_) | Self::Unobserved => self.clone(),
        }
    }
}

/// A route that needs no channel: what a transmission opened and confirmed
/// in one step can have.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NonChannelRoute {
    Delegation(DelegationDirection),
    Direct(DirectCarrier),
    Unobserved,
}

impl From<NonChannelRoute> for Route {
    fn from(route: NonChannelRoute) -> Self {
        match route {
            NonChannelRoute::Delegation(direction) => Route::Delegation(direction),
            NonChannelRoute::Direct(carrier) => Route::Direct(carrier),
            NonChannelRoute::Unobserved => Route::Unobserved,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DelegationDirection {
    /// The parent's task prompt became the child's first user turn.
    ParentToChild,
    /// The child's final message came back as the parent's tool result.
    ChildToParent,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DirectCarrier {
    UserTurn,
    SystemPrompt,
    ToolResult(ToolName),
}

#[derive(Debug, Clone, PartialEq)]
pub enum TransmissionState {
    Detected,
    AwaitingContent {
        co_access: CoAccess,
        /// `CorrelationTiming::window_closes_at` of the read's time.
        window_closes_at: Timestamp,
    },
    /// Only access-pattern evidence.
    Suspected {
        co_access: NonEmpty<CoAccess>,
        /// The `window_closes_at` it was suspected at. It expires at
        /// `CorrelationTiming::expires_at(since)`.
        since: Timestamp,
    },
    Confirmed(Confirmed),
    Classified {
        confirmed: Confirmed,
        classification: Classification,
    },
    /// Counted into its edge. Final.
    Aggregated {
        confirmed: Confirmed,
        classification: Classification,
    },
    /// Suspected, but no content evidence arrived before expiry. Final.
    Discarded {
        at: Timestamp,
        co_access: NonEmpty<CoAccess>,
    },
}

/// A transition that applies only to a suspected transmission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotSuspected;

impl TransmissionState {
    /// Suspected to Discarded at `at`, keeping its co-accesses. Any other
    /// state is left unchanged.
    pub fn expire(&mut self, at: Timestamp) -> Result<(), NotSuspected> {
        match self {
            Self::Suspected { co_access, .. } => {
                *self = Self::Discarded {
                    at,
                    co_access: co_access.clone(),
                };
                Ok(())
            }
            Self::Detected
            | Self::AwaitingContent { .. }
            | Self::Confirmed(_)
            | Self::Classified { .. }
            | Self::Aggregated { .. }
            | Self::Discarded { .. } => Err(NotSuspected),
        }
    }

    /// The content evidence, in `Confirmed`, `Classified` and `Aggregated`.
    pub fn confirmed(&self) -> Option<&Confirmed> {
        match self {
            Self::Confirmed(confirmed)
            | Self::Classified { confirmed, .. }
            | Self::Aggregated { confirmed, .. } => Some(confirmed),
            Self::Detected
            | Self::AwaitingContent { .. }
            | Self::Suspected { .. }
            | Self::Discarded { .. } => None,
        }
    }

    /// The co-access records this state holds, in stored order: none while
    /// `Detected`, the one being waited on, the suspected or discarded
    /// ones, or a confirmed transmission's `Confirmed::co_access`.
    pub fn co_accesses(&self) -> Vec<CoAccess> {
        match self {
            Self::Detected => Vec::new(),
            Self::AwaitingContent { co_access, .. } => vec![*co_access],
            Self::Suspected { co_access, .. } | Self::Discarded { co_access, .. } => {
                co_access.iter().copied().collect()
            }
            Self::Confirmed(confirmed)
            | Self::Classified { confirmed, .. }
            | Self::Aggregated { confirmed, .. } => confirmed.co_access().to_vec(),
        }
    }
}

/// A transmission backed by at least one content match.
///
/// Built only through [`Confirmed::new`] and grown only through
/// [`Confirmed::extend`], which require every match to share one origin
/// agent (the sender) and one reader. When a reader's input echoes two
/// writers, that is two transmissions.
#[derive(Debug, Clone, PartialEq)]
pub struct Confirmed {
    from: AgentId,
    content: NonEmpty<ContentMatch>,
    co_access: Vec<CoAccess>,
    at: Timestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MixedMatches {
    SeveralOrigins,
    SeveralReaders,
}

impl Confirmed {
    pub fn new(
        content: NonEmpty<ContentMatch>,
        co_access: Vec<CoAccess>,
        at: Timestamp,
    ) -> Result<Self, MixedMatches> {
        let first = content.first();
        let (from, reader) = (first.origin_agent(), first.reader());
        if content.iter().any(|m| m.origin_agent() != from) {
            return Err(MixedMatches::SeveralOrigins);
        }
        if content.iter().any(|m| m.reader() != reader) {
            return Err(MixedMatches::SeveralReaders);
        }
        Ok(Self {
            from,
            content,
            co_access,
            at,
        })
    }

    /// Add a later match from the same sender to the same reader.
    pub fn extend(&mut self, content: ContentMatch) -> Result<(), MixedMatches> {
        if content.origin_agent() != self.from {
            return Err(MixedMatches::SeveralOrigins);
        }
        if content.reader() != self.content.first().reader() {
            return Err(MixedMatches::SeveralReaders);
        }
        self.content.push(content);
        Ok(())
    }

    pub fn from(&self) -> AgentId {
        self.from
    }

    pub fn content(&self) -> &NonEmpty<ContentMatch> {
        &self.content
    }

    pub fn co_access(&self) -> &[CoAccess] {
        &self.co_access
    }

    /// When the reader received the content: the `started_at` of the
    /// reader exchange holding the first content match. For a channel
    /// transmission that is the exchange whose tool result made the read, so
    /// it is the read's time. Edges are bucketed by it and query windows are
    /// tested against it, so a late match adds to an earlier bucket; see
    /// [`crate::aggregates::watermark`].
    pub fn at(&self) -> Timestamp {
        self.at
    }

    /// Bytes of the sender's originated text that reached the reader. Never
    /// zero: every match covers at least one byte.
    pub fn matched_bytes(&self) -> NonZeroU64 {
        let total: u64 = self
            .content
            .iter()
            .map(|m| u64::from(m.matched_bytes().get()))
            .sum();
        NonZeroU64::new(total).unwrap_or(NonZeroU64::MIN)
    }
}

/// A topic assignment under one topic-model version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Classification {
    pub version: TopicModelVersion,
    /// `None` when the topic model marks it an outlier.
    pub topic: Option<TopicId>,
    pub watched: bool,
}
