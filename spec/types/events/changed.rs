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
//! | `Alert` | L6 alert store, L8 actions | open, deduplicate, suppress, acknowledge, resolve |
//! | `Channel` | L5 `ChannelRegistry` | discovery, declaration (config), a new resource, every detection change (observed, candidate, active, dormant, unused), a recorded policy decision (config or `PolicyChanged`), promotion |
//! | `Agent` | L3 identity resolver | creation (traffic or config), a state change, a merge (source and target), an unmerge (the agent, the agent it was merged into, every restored agent), a label set or cleared |
//! | `Rule` | L6 `AlertRuleStore` | create (operator or config), update, status change, turning stale |
//! | `Watermark` | L7 `EdgeStore` | the watermark advancing |
//! | `TopicVersion` | L6 `TopicCatalog` | a status change of that version (ready, active, superseded) |
//! | `Projection` | L6 `ProjectionStore` | a projection job becoming ready or failed, or its frame expiring |

use crate::aggregates::topic::TopicModelVersion;
use crate::events::Subject;
use crate::ids::{AgentId, AlertId, AlertRuleId, ChannelId, ProjectionId};
use crate::support::Watermark;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Changed {
    Alert(AlertId),
    Channel(ChannelId),
    Agent(AgentId),
    Rule(AlertRuleId),
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
}
