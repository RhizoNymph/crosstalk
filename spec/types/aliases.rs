//! Read-time alias resolution.
//!
//! Two kinds of id can come to stand for another after records naming them
//! were stored. Both are resolved when a record is read, never by rewriting
//! it:
//!
//! | Id | Aliased by | Resolved through |
//! | --- | --- | --- |
//! | `AgentId` | a merge (L3) | `AgentDirectory::canonical` |
//! | `ChannelId` | a promotion that supersedes it (L5) | `ChannelDirectory::canonical` |
//!
//! Every reader that matches stored ids against a request or groups records
//! by id takes an [`Aliases`]: the view filter
//! ([`TopologyFilter::admits`](crate::aggregates::filter::TopologyFilter::admits)),
//! routes ([`Route::resolved`](crate::derived::flow::transmission::Route::resolved)),
//! alert subjects ([`AlertSubject::resolved`](crate::aggregates::alert::AlertSubject::resolved)),
//! and graph nodes and edges. Both resolutions are one step: a merged
//! agent's target is never merged, and a superseding channel is a promoted
//! channel, which is declared and so never superseded. Resolving a resolved
//! id returns it unchanged.

use crate::ids::{AgentId, ChannelId};

/// The canonical form of agent and channel ids at the time of a read.
pub trait Aliases {
    /// The canonical agent: `id` unless it was merged.
    fn agent(&self, id: AgentId) -> AgentId;

    /// The canonical channel: `id` unless a promotion superseded it.
    fn channel(&self, id: ChannelId) -> ChannelId;
}

/// Merges only: a store in which no channel is superseded. Every channel is
/// its own canonical channel.
impl<F: Fn(AgentId) -> AgentId> Aliases for F {
    fn agent(&self, id: AgentId) -> AgentId {
        self(id)
    }

    fn channel(&self, id: ChannelId) -> ChannelId {
        id
    }
}

/// Merges and supersessions. Implementations build it from the
/// `AgentDirectory` and `ChannelDirectory` they read through.
#[derive(Debug, Clone, Copy)]
pub struct Resolve<A, C> {
    pub agents: A,
    pub channels: C,
}

impl<A, C> Aliases for Resolve<A, C>
where
    A: Fn(AgentId) -> AgentId,
    C: Fn(ChannelId) -> ChannelId,
{
    fn agent(&self, id: AgentId) -> AgentId {
        (self.agents)(id)
    }

    fn channel(&self, id: ChannelId) -> ChannelId {
        (self.channels)(id)
    }
}

/// No merges and no supersessions: every id is canonical.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NoAliases;

impl Aliases for NoAliases {
    fn agent(&self, id: AgentId) -> AgentId {
        id
    }

    fn channel(&self, id: ChannelId) -> ChannelId {
        id
    }
}
