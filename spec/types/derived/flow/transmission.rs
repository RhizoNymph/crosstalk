//! Transmissions: one agent-to-agent communication.
//!
//! ```text
//!            ┌──────── match (direct route) ────────────────┐
//! Detected ──┤                                              ▼
//!            └─ await evidence ─▶ AwaitingContent ─match─▶ Confirmed ─classify─▶ Classified ─aggregate─▶ Aggregated
//!                                      │                    ▲
//!                               window closes          late match
//!                                      ▼                    │
//!                                  Suspected ───────────────┘
//!                                      └─ expire ─▶ Discarded
//! ```
//!
//! The sender is unknown until content evidence arrives, so it lives inside
//! [`Confirmed`], not on the transmission itself.

use crate::derived::flow::evidence::CoAccess;
use crate::derived::provenance::matching::ContentMatch;
use crate::ids::{AgentId, ChannelId, TopicId, TransmissionId};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Route {
    /// Through a shared resource: the content arrived as a tool result.
    Channel(ChannelId),
    /// No resource in between: the content was placed in the reader's
    /// prompt by an orchestrator or human.
    Direct(DirectCarrier),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DirectCarrier {
    UserTurn,
    SystemPrompt,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TransmissionState {
    Detected,
    AwaitingContent {
        co_access: CoAccess,
        window_closes_at: Timestamp,
    },
    /// Only access-pattern evidence.
    Suspected {
        co_access: NonEmpty<CoAccess>,
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

/// A transmission backed by at least one content match.
///
/// Built only through [`Confirmed::new`], which requires every match to
/// share one origin agent (the sender) and one reader. When a reader's input
/// echoes two writers, that is two transmissions.
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

    pub fn from(&self) -> AgentId {
        self.from
    }

    pub fn content(&self) -> &NonEmpty<ContentMatch> {
        &self.content
    }

    pub fn co_access(&self) -> &[CoAccess] {
        &self.co_access
    }

    pub fn at(&self) -> Timestamp {
        self.at
    }

    /// Bytes of the sender's originated text that reached the reader.
    pub fn matched_bytes(&self) -> u64 {
        self.content
            .iter()
            .map(|m| u64::from(m.matched_bytes()))
            .sum()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Classification {
    /// `None` when the topic model marks it an outlier.
    pub topic: Option<TopicId>,
    pub watched: bool,
}
