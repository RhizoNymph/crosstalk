//! Export, audit and operators (items 11 and 12). Stored projections are
//! the spec's (`aggregates::projection`).

use crosstalk_spec::ids::{AgentId, ChannelId, OperatorId, TransmissionId};
use crosstalk_spec::interfaces::l8_surface::Permission;
use crosstalk_spec::support::{TimeWindow, Timestamp};

use super::actions::{ActionOutcome, OperatorAction};
use crate::url::scope::Scope;
use crosstalk_spec::ids::{AuditId, MergeId, ProjectionId};
use crosstalk_spec::interfaces::l8_surface::QueryError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportDataset {
    Transmissions,
    Edges,
    Accesses,
    Topics,
    Projection(ProjectionId),
    Verdicts,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    Jsonl,
    Parquet,
}

/// An export (item 11). `include_content` needs `Content`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportRequest {
    pub dataset: ExportDataset,
    pub scope: Scope,
    pub format: ExportFormat,
    pub include_content: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Actor {
    Config,
    Operator(OperatorId),
}

#[derive(Debug, Clone, PartialEq)]
pub enum AuditedAction {
    Operator(OperatorAction),
    /// A change applied from configuration, described by the gateway.
    Config {
        summary: String,
    },
}

/// What became of an audited action. An applied operator action carries
/// what it created (`RuleCreated`, `ChannelPromoted`, `Merged`); config
/// changes are `Applied(ActionOutcome::Applied)`.
#[derive(Debug, Clone, PartialEq)]
pub enum AuditOutcome {
    Applied(ActionOutcome),
    Rejected(QueryError),
}

/// One entry of the append-only audit log (item 12).
#[derive(Debug, Clone, PartialEq)]
pub struct AuditEntry {
    pub id: AuditId,
    pub at: Timestamp,
    pub by: Actor,
    pub action: AuditedAction,
    /// The entity the entry is about: the action's target, or what it
    /// created when the action names none (a created rule). `None` for
    /// actions about no entity (dead-letter replays, most config changes).
    pub subject: Option<AuditSubject>,
    pub outcome: AuditOutcome,
}

/// What an audit entry is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditSubject {
    Agent(AgentId),
    Channel(ChannelId),
    Transmission(TransmissionId),
    Rule(crosstalk_spec::ids::AlertRuleId),
    Alert(crosstalk_spec::ids::AlertId),
    Merge(MergeId),
}

/// Restricts the audit log. `subject` keeps the entries about that entity
/// or created it, resolving agent aliases and channel supersession: an
/// agent's entries include the merges it took part in, a channel's the
/// promotion that declared it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AuditFilter {
    pub operators: Vec<OperatorId>,
    pub subject: Option<AuditSubject>,
    pub window: Option<TimeWindow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Operator {
    pub id: OperatorId,
    pub name: String,
    pub permissions: Vec<Permission>,
}
