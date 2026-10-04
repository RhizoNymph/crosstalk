//! Stored projections, detection quality, export, audit and operators
//! (items 8 to 12).

use std::num::{NonZeroU16, NonZeroU32};

use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::aggregates::topic::EmbeddingModel;
use crosstalk_spec::ids::{AgentId, ChannelId, OperatorId, TopicId, TransmissionId};
use crosstalk_spec::interfaces::l8_surface::Permission;
use crosstalk_spec::support::{TimeWindow, Timestamp};

use super::actions::{ActionOutcome, OperatorAction};
use super::errors::QueryError;
use super::scope::Scope;
use super::{AuditId, MergeId, ProjectionId};

/// UMAP parameters. `min_dist` is checked to lie in `[0, 1]`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProjectionParams {
    pub neighbors: NonZeroU16,
    min_dist: f32,
    pub seed: u64,
    /// At most this many transmissions are sampled, deterministically from
    /// `seed`.
    pub sample_limit: NonZeroU32,
}

#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
#[error("min_dist {0} is outside [0, 1]")]
pub struct InvalidMinDist(pub f32);

impl ProjectionParams {
    pub fn new(
        neighbors: NonZeroU16,
        min_dist: f32,
        seed: u64,
        sample_limit: NonZeroU32,
    ) -> Result<Self, InvalidMinDist> {
        if !(0.0..=1.0).contains(&min_dist) {
            return Err(InvalidMinDist(min_dist));
        }
        Ok(Self {
            neighbors,
            min_dist,
            seed,
            sample_limit,
        })
    }

    pub fn min_dist(&self) -> f32 {
        self.min_dist
    }
}

/// Everything needed to reproduce a projection.
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionMeta {
    pub id: ProjectionId,
    pub scope: Scope,
    pub params: ProjectionParams,
    pub embedding_model: EmbeddingModel,
    pub fitted_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProjectionJob {
    Queued,
    Running { done: u32, total: u32 },
    Ready(ProjectionMeta),
    Failed { reason: String },
}

/// Category of one projected point, as indexes into the tables of
/// [`ProjectionPoints`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PointCategories {
    pub sender: u32,
    pub reader: u32,
    pub route: RouteKind,
    /// Into `channels`; `None` unless channel-routed.
    pub channel: Option<u32>,
    /// Into `topics`; `None` for outliers.
    pub topic: Option<u32>,
}

/// A stored projection in columnar form (item 8). Built only through
/// [`ProjectionPoints::new`]: columns have equal length and every index is
/// inside its table.
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionPoints {
    meta: ProjectionMeta,
    transmissions: Vec<TransmissionId>,
    xs: Vec<f32>,
    ys: Vec<f32>,
    categories: Vec<PointCategories>,
    agents: Vec<AgentId>,
    channels: Vec<ChannelId>,
    topics: Vec<TopicId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidPoints {
    #[error("columns have different lengths")]
    RaggedColumns,
    #[error("point {0} has an index outside its table")]
    IndexOutOfRange(usize),
    #[error("point {0} has a non-finite coordinate")]
    NonFinite(usize),
}

impl ProjectionPoints {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        meta: ProjectionMeta,
        transmissions: Vec<TransmissionId>,
        xs: Vec<f32>,
        ys: Vec<f32>,
        categories: Vec<PointCategories>,
        agents: Vec<AgentId>,
        channels: Vec<ChannelId>,
        topics: Vec<TopicId>,
    ) -> Result<Self, InvalidPoints> {
        let n = transmissions.len();
        if xs.len() != n || ys.len() != n || categories.len() != n {
            return Err(InvalidPoints::RaggedColumns);
        }
        let inside = |i: Option<u32>, len: usize| i.is_none_or(|i| (i as usize) < len);
        for (i, c) in categories.iter().enumerate() {
            if !inside(Some(c.sender), agents.len())
                || !inside(Some(c.reader), agents.len())
                || !inside(c.channel, channels.len())
                || !inside(c.topic, topics.len())
            {
                return Err(InvalidPoints::IndexOutOfRange(i));
            }
            if !xs[i].is_finite() || !ys[i].is_finite() {
                return Err(InvalidPoints::NonFinite(i));
            }
        }
        Ok(Self {
            meta,
            transmissions,
            xs,
            ys,
            categories,
            agents,
            channels,
            topics,
        })
    }

    pub fn meta(&self) -> &ProjectionMeta {
        &self.meta
    }

    pub fn len(&self) -> usize {
        self.transmissions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.transmissions.is_empty()
    }

    pub fn transmissions(&self) -> &[TransmissionId] {
        &self.transmissions
    }

    pub fn xs(&self) -> &[f32] {
        &self.xs
    }

    pub fn ys(&self) -> &[f32] {
        &self.ys
    }

    pub fn categories(&self) -> &[PointCategories] {
        &self.categories
    }

    pub fn agents(&self) -> &[AgentId] {
        &self.agents
    }

    pub fn channels(&self) -> &[ChannelId] {
        &self.channels
    }

    pub fn topics(&self) -> &[TopicId] {
        &self.topics
    }
}

/// Labelled and unlabelled detections per route and match kind (item 10).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualityRow {
    pub route: RouteKind,
    pub match_kind: MatchKindName,
    pub genuine: u64,
    pub false_detection: u64,
    pub unlabeled: u64,
}

/// A `MatchKind` without its payload, for grouping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MatchKindName {
    Exact,
    Normalized,
    Decoded,
    Semantic,
}

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
