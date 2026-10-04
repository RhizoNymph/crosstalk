//! Change notifications: which stored entity a surface query returns has
//! changed, by id only.
//!
//! The store that owns an entity publishes [`Changed`] after every committed
//! change to it, whichever component or action made the change, and never
//! before the change is visible to the queries that read that store. The
//! live feed (L8) turns each one into a `UiEvent`, and the UI re-queries.
//! Because a notification carries no state, notifications for one entity
//! may arrive in any order or more than once: the re-query after the last
//! one returns the stored state.
//!
//! | Variant | Published by | After |
//! | --- | --- | --- |
//! | `Alert` | L6 alert store, L8 actions | open, deduplicate, suppress (sanction, rule disabled, false detection), acknowledge, resolve |
//! | `Channel` | L5 `ChannelRegistry` | discovery, declaration (config), a new resource, every detection change (observed, candidate, active, dormant, unused), a recorded policy decision (config or `PolicyChanged`), a promotion: the promoted channel and every channel it superseded ([`Changed::promotion`]) |
//! | `Agent` | L3 identity resolver | creation (traffic or config), a state change, a merge (source, target and every agent it repointed), an unmerge (the source, the agent it was merged into, every restored agent), a rename (label set or cleared) |
//! | `Rule` | L6 `AlertRuleStore` | create (operator or config), update, enable or disable, turning stale |
//! | `Verdict` | L5 `TransmissionVerdicts` | a verdict record appended to the transmission's log (set or withdrawn) |
//! | `Watermark` | L7 `EdgeStore` | the watermark advancing |
//! | `TopicVersion` | L6 `TopicCatalog` | a status change of that version (ready, active, superseded), and a retention change (pinned, unpinned, dropped) |
//! | `Projection` | L6 `ProjectionStore` | a projection job becoming ready or failed, or its frame expiring |
//!
//! A store that changes nothing (a redelivery, an `Unchanged` action)
//! publishes nothing. Merged agents and superseded channels are not
//! resolved here: a notification names the stored id that changed, and the
//! re-query resolves it ([`crate::aliases`]).

use crate::aggregates::topic::TopicModelVersion;
use crate::events::Subject;
use crate::ids::{AgentId, AlertId, AlertRuleId, ChannelId, ProjectionId, TransmissionId};
use crate::support::Watermark;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Changed {
    Alert(AlertId),
    Channel(ChannelId),
    Agent(AgentId),
    Rule(AlertRuleId),
    /// A verdict was set on, or withdrawn from, this transmission.
    Verdict(TransmissionId),
    /// The edge store's watermark advanced to this value.
    Watermark(Watermark),
    TopicVersion(TopicModelVersion),
    /// This stored projection job became ready or failed, or its frame
    /// expired.
    Projection(ProjectionId),
}

impl Changed {
    pub fn subject(&self) -> Subject {
        Subject::Changed
    }

    /// What L5 publishes after a promotion commits (`ChannelPromoted`): the
    /// promoted channel, then every channel it superseded, in order. Each
    /// superseded channel changed too: it now resolves to `channel`, and
    /// its resources and alerts show under it.
    pub fn promotion(channel: ChannelId, superseded: &[ChannelId]) -> Vec<Self> {
        std::iter::once(channel)
            .chain(superseded.iter().copied())
            .map(Self::Channel)
            .collect()
    }
}
