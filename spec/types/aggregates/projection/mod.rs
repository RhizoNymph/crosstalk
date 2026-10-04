//! Stored, versioned 2-D projections (UMAP) of transmission embeddings.
//!
//! UMAP is randomized and depends on the points it is fitted on, so a
//! projection is computed once and stored, and a cited view is read back
//! exactly. An operator asks for one with `QueryApi::fit_projection`; it
//! becomes a background job, and `QueryApi::projection` returns the stored
//! result, the same bytes on every read.
//!
//! **What is recorded.** A [`ProjectionSpec`] holds everything the fit was
//! asked to do: the window, the [`TopologyFilter`] with its topic version
//! resolved and pinned, the [`ProjectionParams`] (sample size, UMAP
//! neighbours and minimum distance, and the seed) and the embedding model.
//! The sample itself is stored: the frame lists the transmissions it holds.
//! A [`Fitted`] record adds when the sample was read and the [`Watermark`]
//! at that moment, so a citation says which buckets were settled.
//!
//! **Sampling.** When the fit starts, the job reads every transmission
//! confirmed in the window that the filter admits under the pinned version
//! (agents resolved through `AgentDirectory` at that moment) and that has an
//! embedding from the spec's model. That count is `matching`. If more match
//! than the sample size, it keeps the `limit` with the smallest sample key,
//! `BLAKE3(key = derive_key("crosstalk projection sample v1", seed as u64
//! LE), transmission ULID as u128 LE)`, ties to the smaller id. Points are
//! stored in ascending sample-key order, which is also the order the
//! embeddings are passed to UMAP, so the same sample, params and seed give
//! the same layout. For two fits with the same seed and sample size, a
//! transmission sampled by the wider one is sampled by any narrower one
//! that still admits it (bottom-k selection).
//!
//! **Points are frozen at fit time.** A stored point carries the canonical
//! agents, route kind, topic (under the pinned version) and confirmation
//! time as they were when the sample was read. Later merges, verdicts and
//! re-fits do not change a stored projection; a client that needs current
//! canonical agents resolves the frame's agent tables through the agent
//! list, where a merged agent names its canonical one.
//!
//! **Lifecycle.**
//!
//! ```text
//! Queued ──start──▶ Fitting ──complete──▶ Ready ──expire──▶ Expired
//!   │  ◀──requeue──    │
//!   └──────fail────────┴──▶ Failed
//! ```
//!
//! A fitter that dies mid-fit leaves its job `Fitting`; the store returns it
//! to `Queued` (`requeue`) once its lease lapses, so a crash never fails a
//! job. Only deterministic problems fail one ([`FitFailure`]).
//!
//! **Retention.** A projection's [`ProjectionInfo`] (spec, requester and
//! status) is kept for as long as the audit log. Its frame is kept for
//! `projection.frame_retention_days` after it was fitted (default 180), then
//! dropped, and the projection becomes `Expired`: reading it returns
//! `ProjectionNotRetained`, while its spec still says exactly what it was and
//! can be fitted again with the same seed. The catalog keeps every
//! version's topics (`TopicCatalog::topics`), so a frame's topic ids always
//! resolve to labels.
//!
//! **On the wire.** [`ProjectionParams`] is a request (`fit_projection`);
//! [`ProjectionInfo`], with its [`ProjectionSpec`] and
//! [`ProjectionStatus`], is a response (`projection_status`,
//! `projections`) and never a request, because the surface stamps its
//! requester and times. A [`Projection`] has no JSON form: its frame is
//! binary. `QueryApi::projection` is answered in two halves: the job record
//! is the JSON `ProjectionInfo` that `projection_status` returns, and the
//! frame is `application/octet-stream`, the bytes of
//! [`ProjectionFrame::encode`]. The UI reads `projection_status` (JSON)
//! and, once the status is `ready`, fetches the frame bytes, decodes them
//! with [`ProjectionFrame::decode`] and joins the two with
//! [`Projection::new`], which refuses a frame that does not belong to the
//! job. An error on the frame request (`NotFound`,
//! `Conflict(ProjectionNotReady)`, `Conflict(ProjectionFailed)`,
//! `ProjectionNotRetained`) is a `QueryError` in JSON like any other. The
//! UMAP minimum distance is held in thousandths, so no float of a spec is
//! on the wire; a [`ProjectedPoint`]'s coordinates are
//! [`Finite`].
//!
//! [`TopologyFilter`]: crate::aggregates::filter::TopologyFilter

pub mod frame;

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use crate::aggregates::edge::{RouteKind, TopicSlot};
use crate::aggregates::filter::TopologyFilter;
use crate::aggregates::topic::{EmbeddingModel, TopicModelVersion};
use crate::ids::{AgentId, OperatorId, ProjectionId, TopicId, TransmissionId};
use crate::support::{Finite, TimeWindow, Timestamp, Watermark};
use crate::wire::{Rejected, WireRequest};

use frame::ProjectionFrame;

/// The most points a projection may hold: `1..=ProjectionLimit::MAX`. On
/// the wire, the number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct ProjectionLimit(NonZeroU32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidProjectionLimit {
    Zero,
    AboveMax { max: u32, got: u32 },
}

impl TryFrom<u32> for ProjectionLimit {
    type Error = Rejected<InvalidProjectionLimit>;

    fn try_from(limit: u32) -> Result<Self, Self::Error> {
        Self::new(limit).map_err(|error| Rejected::new("projection limit", error))
    }
}

impl From<ProjectionLimit> for u32 {
    fn from(limit: ProjectionLimit) -> Self {
        limit.0.get()
    }
}

impl ProjectionLimit {
    /// Bounds a fit to minutes and a frame to about 5 MB (48 bytes per point
    /// plus its tables), which a browser canvas still renders interactively.
    pub const MAX: u32 = 100_000;

    pub fn new(limit: u32) -> Result<Self, InvalidProjectionLimit> {
        let limit = NonZeroU32::new(limit).ok_or(InvalidProjectionLimit::Zero)?;
        if limit.get() > Self::MAX {
            return Err(InvalidProjectionLimit::AboveMax {
                max: Self::MAX,
                got: limit.get(),
            });
        }
        Ok(Self(limit))
    }

    pub fn get(self) -> NonZeroU32 {
        self.0
    }
}

/// How to fit one projection. Every field is recorded with the projection.
///
/// Built only through [`ProjectionParams::new`]. The minimum distance is
/// held in thousandths, so a recorded spec reproduces exactly whatever it
/// was serialized through.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawProjectionParams")]
pub struct ProjectionParams {
    limit: ProjectionLimit,
    neighbors: u16,
    min_dist_milli: u16,
    seed: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidParams {
    Neighbors { min: u16, max: u16, got: u16 },
    MinDist { max_milli: u16, got_milli: u16 },
}

/// [`ProjectionParams`]'s fields, decoded without the checks.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawProjectionParams {
    limit: ProjectionLimit,
    neighbors: u16,
    min_dist_milli: u16,
    seed: u64,
}

impl TryFrom<RawProjectionParams> for ProjectionParams {
    type Error = Rejected<InvalidParams>;

    fn try_from(raw: RawProjectionParams) -> Result<Self, Self::Error> {
        Self::new(raw.limit, raw.neighbors, raw.min_dist_milli, raw.seed)
            .map_err(|error| Rejected::new("projection params", error))
    }
}

/// A client picks every parameter of a fit, the seed included.
impl WireRequest for ProjectionParams {}

impl ProjectionParams {
    pub const MIN_NEIGHBORS: u16 = 2;
    pub const MAX_NEIGHBORS: u16 = 200;
    pub const DEFAULT_NEIGHBORS: u16 = 15;
    /// UMAP's `min_dist` ranges over `0.0..=1.0`.
    pub const MAX_MIN_DIST_MILLI: u16 = 1_000;
    pub const DEFAULT_MIN_DIST_MILLI: u16 = 100;

    /// `seed` is any value; a client wanting a fresh layout picks one at
    /// random. It keys both the sample and UMAP's random state.
    pub fn new(
        limit: ProjectionLimit,
        neighbors: u16,
        min_dist_milli: u16,
        seed: u64,
    ) -> Result<Self, InvalidParams> {
        if !(Self::MIN_NEIGHBORS..=Self::MAX_NEIGHBORS).contains(&neighbors) {
            return Err(InvalidParams::Neighbors {
                min: Self::MIN_NEIGHBORS,
                max: Self::MAX_NEIGHBORS,
                got: neighbors,
            });
        }
        if min_dist_milli > Self::MAX_MIN_DIST_MILLI {
            return Err(InvalidParams::MinDist {
                max_milli: Self::MAX_MIN_DIST_MILLI,
                got_milli: min_dist_milli,
            });
        }
        Ok(Self {
            limit,
            neighbors,
            min_dist_milli,
            seed,
        })
    }

    pub fn limit(self) -> ProjectionLimit {
        self.limit
    }

    /// UMAP's `n_neighbors`.
    pub fn neighbors(self) -> u16 {
        self.neighbors
    }

    pub fn min_dist_milli(self) -> u16 {
        self.min_dist_milli
    }

    /// UMAP's `min_dist`.
    pub fn min_dist(self) -> f32 {
        f32::from(self.min_dist_milli) / 1_000.0
    }

    pub fn seed(self) -> u64 {
        self.seed
    }
}

/// What a projection was asked to fit, with the topic version resolved.
///
/// Built only through [`ProjectionSpec::new`], which pins the filter's
/// selector to the resolved version, so a stored spec never says `Current`.
/// Decoding goes through it too, so a decoded spec's filter is pinned. It is
/// server-stamped (the version and model resolved when the request was
/// accepted) and never a request (`wire/authority.rs`): a client asks with
/// `ProjectionParams`, a window and a filter, and only the gateway writes a
/// spec, so decoding one normalizes rather than refusing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", from = "RawProjectionSpec")]
pub struct ProjectionSpec {
    window: TimeWindow,
    filter: TopologyFilter,
    topic_version: TopicModelVersion,
    params: ProjectionParams,
    embedding_model: EmbeddingModel,
}

/// [`ProjectionSpec`]'s fields, decoded before [`ProjectionSpec::new`] pins
/// the filter.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawProjectionSpec {
    window: TimeWindow,
    filter: TopologyFilter,
    topic_version: TopicModelVersion,
    params: ProjectionParams,
    embedding_model: EmbeddingModel,
}

impl From<RawProjectionSpec> for ProjectionSpec {
    fn from(raw: RawProjectionSpec) -> Self {
        Self::new(
            raw.window,
            raw.filter,
            raw.topic_version,
            raw.params,
            raw.embedding_model,
        )
    }
}

impl ProjectionSpec {
    /// `topic_version` is what the request's selector resolved to when it
    /// was accepted; `embedding_model` is the model current at that time.
    pub fn new(
        window: TimeWindow,
        filter: TopologyFilter,
        topic_version: TopicModelVersion,
        params: ProjectionParams,
        embedding_model: EmbeddingModel,
    ) -> Self {
        Self {
            window,
            filter: filter.pinned(topic_version),
            topic_version,
            params,
            embedding_model,
        }
    }

    pub fn window(&self) -> TimeWindow {
        self.window
    }

    /// Always `Pinned(self.topic_version())`.
    pub fn filter(&self) -> &TopologyFilter {
        &self.filter
    }

    pub fn topic_version(&self) -> TopicModelVersion {
        self.topic_version
    }

    pub fn params(&self) -> ProjectionParams {
        self.params
    }

    pub fn embedding_model(&self) -> &EmbeddingModel {
        &self.embedding_model
    }
}

/// Why a fit failed. Only deterministic outcomes: fitting the same spec
/// again before anything changes fails the same way. Store and worker
/// failures are retried, never recorded here.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum FitFailure {
    /// Fewer sampled points than UMAP needs (more than `neighbors`).
    TooFewPoints { needed: u32, got: u64 },
    /// The pinned version's assignments were dropped before the fit read
    /// its sample.
    VersionNotRetained { version: TopicModelVersion },
    /// The spec's embedding model has been replaced and its vectors dropped.
    EmbeddingModelUnavailable { model: EmbeddingModel },
    /// UMAP produced a non-finite coordinate.
    NonFiniteLayout,
}

/// A completed fit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Fitted {
    /// When the fit started and read its sample.
    pub started_at: Timestamp,
    /// When the frame was stored.
    pub fitted_at: Timestamp,
    /// The aggregate watermark when the sample was read.
    pub watermark: Watermark,
    /// Transmissions the window, filter and model admitted, before sampling.
    pub matching: u64,
    /// Points stored: `min(matching, limit)`.
    pub points: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ProjectionStatus {
    Queued,
    Fitting {
        started_at: Timestamp,
    },
    Ready(Fitted),
    /// `started_at` is `None` when the job failed before it started (its
    /// version was dropped while it was queued).
    Failed {
        started_at: Option<Timestamp>,
        failed_at: Timestamp,
        failure: FitFailure,
    },
    /// The frame was dropped after the retention period.
    Expired {
        fitted: Fitted,
        expired_at: Timestamp,
    },
}

/// On the wire, a string: `"fitting"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionStatusKind {
    Queued,
    Fitting,
    Ready,
    Failed,
    Expired,
}

impl ProjectionStatus {
    pub fn kind(&self) -> ProjectionStatusKind {
        match self {
            Self::Queued => ProjectionStatusKind::Queued,
            Self::Fitting { .. } => ProjectionStatusKind::Fitting,
            Self::Ready(_) => ProjectionStatusKind::Ready,
            Self::Failed { .. } => ProjectionStatusKind::Failed,
            Self::Expired { .. } => ProjectionStatusKind::Expired,
        }
    }
}

/// One projection job and what came of it.
///
/// Built only through [`ProjectionInfo::new`] (or [`ProjectionInfo::queued`]
/// and the transitions): its timestamps never go backwards (requested,
/// started, fitted or failed, expired), a fit's watermark is no later than
/// its start, and a fit holds exactly `min(matching, limit)` points.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawProjectionInfo")]
pub struct ProjectionInfo {
    id: ProjectionId,
    spec: ProjectionSpec,
    requested_by: OperatorId,
    requested_at: Timestamp,
    status: ProjectionStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidProjectionInfo {
    TimestampsOutOfOrder,
    WatermarkAfterStart,
    /// `points` is not `min(matching, limit)`.
    WrongCount {
        expected: u64,
        got: u32,
    },
}

/// [`ProjectionInfo`]'s fields, decoded without the checks. Decoding goes
/// through [`ProjectionInfo::new`].
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawProjectionInfo {
    id: ProjectionId,
    spec: ProjectionSpec,
    requested_by: OperatorId,
    requested_at: Timestamp,
    status: ProjectionStatus,
}

impl TryFrom<RawProjectionInfo> for ProjectionInfo {
    type Error = Rejected<InvalidProjectionInfo>;

    fn try_from(raw: RawProjectionInfo) -> Result<Self, Self::Error> {
        Self::new(
            raw.id,
            raw.spec,
            raw.requested_by,
            raw.requested_at,
            raw.status,
        )
        .map_err(|error| Rejected::new("projection info", error))
    }
}

/// A transition the lifecycle does not allow, or one that would break a
/// [`ProjectionInfo`] invariant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidTransition {
    NotAllowed {
        from: ProjectionStatusKind,
        to: ProjectionStatusKind,
    },
    /// `complete` with a fit whose start differs from the job's.
    StartMismatch,
    Info(InvalidProjectionInfo),
}

impl ProjectionInfo {
    pub fn new(
        id: ProjectionId,
        spec: ProjectionSpec,
        requested_by: OperatorId,
        requested_at: Timestamp,
        status: ProjectionStatus,
    ) -> Result<Self, InvalidProjectionInfo> {
        let mut times = vec![requested_at];
        let mut fitted = None;
        match &status {
            ProjectionStatus::Queued => {}
            ProjectionStatus::Fitting { started_at } => times.push(*started_at),
            ProjectionStatus::Ready(fit) => {
                times.extend([fit.started_at, fit.fitted_at]);
                fitted = Some(*fit);
            }
            ProjectionStatus::Failed {
                started_at,
                failed_at,
                ..
            } => times.extend(started_at.iter().copied().chain([*failed_at])),
            ProjectionStatus::Expired {
                fitted: fit,
                expired_at,
            } => {
                times.extend([fit.started_at, fit.fitted_at, *expired_at]);
                fitted = Some(*fit);
            }
        }
        if times.windows(2).any(|pair| pair[0] > pair[1]) {
            return Err(InvalidProjectionInfo::TimestampsOutOfOrder);
        }
        if let Some(fit) = fitted {
            if fit.watermark.0 > fit.started_at {
                return Err(InvalidProjectionInfo::WatermarkAfterStart);
            }
            let expected = fit
                .matching
                .min(u64::from(spec.params().limit().get().get()));
            if u64::from(fit.points) != expected {
                return Err(InvalidProjectionInfo::WrongCount {
                    expected,
                    got: fit.points,
                });
            }
        }
        Ok(Self {
            id,
            spec,
            requested_by,
            requested_at,
            status,
        })
    }

    /// A job just accepted by `fit_projection`.
    pub fn queued(
        id: ProjectionId,
        spec: ProjectionSpec,
        requested_by: OperatorId,
        requested_at: Timestamp,
    ) -> Self {
        Self {
            id,
            spec,
            requested_by,
            requested_at,
            status: ProjectionStatus::Queued,
        }
    }

    pub fn id(&self) -> ProjectionId {
        self.id
    }

    pub fn spec(&self) -> &ProjectionSpec {
        &self.spec
    }

    pub fn requested_by(&self) -> OperatorId {
        self.requested_by
    }

    pub fn requested_at(&self) -> Timestamp {
        self.requested_at
    }

    pub fn status(&self) -> &ProjectionStatus {
        &self.status
    }

    /// `Queued` to `Fitting`: a fitter claimed the job.
    pub fn start(self, at: Timestamp) -> Result<Self, InvalidTransition> {
        match self.status {
            ProjectionStatus::Queued => self.with(ProjectionStatus::Fitting { started_at: at }),
            _ => Err(self.not_allowed(ProjectionStatusKind::Fitting)),
        }
    }

    /// `Fitting` to `Queued`: the fitter's lease lapsed.
    pub fn requeue(self) -> Result<Self, InvalidTransition> {
        match self.status {
            ProjectionStatus::Fitting { .. } => self.with(ProjectionStatus::Queued),
            _ => Err(self.not_allowed(ProjectionStatusKind::Queued)),
        }
    }

    /// `Fitting` to `Ready`, with the fit that started at the job's start.
    pub fn complete(self, fit: Fitted) -> Result<Self, InvalidTransition> {
        match self.status {
            ProjectionStatus::Fitting { started_at } if started_at == fit.started_at => {
                self.with(ProjectionStatus::Ready(fit))
            }
            ProjectionStatus::Fitting { .. } => Err(InvalidTransition::StartMismatch),
            _ => Err(self.not_allowed(ProjectionStatusKind::Ready)),
        }
    }

    /// `Queued` or `Fitting` to `Failed`.
    pub fn fail(self, at: Timestamp, failure: FitFailure) -> Result<Self, InvalidTransition> {
        let started_at = match self.status {
            ProjectionStatus::Queued => None,
            ProjectionStatus::Fitting { started_at } => Some(started_at),
            _ => return Err(self.not_allowed(ProjectionStatusKind::Failed)),
        };
        self.with(ProjectionStatus::Failed {
            started_at,
            failed_at: at,
            failure,
        })
    }

    /// `Ready` to `Expired`: the frame was dropped.
    pub fn expire(self, at: Timestamp) -> Result<Self, InvalidTransition> {
        match self.status {
            ProjectionStatus::Ready(fitted) => self.with(ProjectionStatus::Expired {
                fitted,
                expired_at: at,
            }),
            _ => Err(self.not_allowed(ProjectionStatusKind::Expired)),
        }
    }

    fn with(self, status: ProjectionStatus) -> Result<Self, InvalidTransition> {
        Self::new(
            self.id,
            self.spec,
            self.requested_by,
            self.requested_at,
            status,
        )
        .map_err(InvalidTransition::Info)
    }

    fn not_allowed(&self, to: ProjectionStatusKind) -> InvalidTransition {
        InvalidTransition::NotAllowed {
            from: self.status.kind(),
            to,
        }
    }
}

/// One point of a projection: a row of a [`ProjectionFrame`]. On the wire
/// (an export's point rows) `x` and `y` are JSON numbers, finite by type, so
/// they always encode and a non-finite one is a decode error.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ProjectedPoint {
    pub transmission: TransmissionId,
    /// Canonical sender when the sample was read.
    pub from: AgentId,
    /// Canonical reader when the sample was read. Equal to `from` when the
    /// two agents had been merged by then; the topology graph drops such
    /// transmissions.
    pub to: AgentId,
    pub route: RouteKind,
    /// The topic under the spec's topic version; `None` for an outlier.
    pub topic: Option<TopicId>,
    /// `Confirmed::at`.
    pub confirmed_at: Timestamp,
    pub x: Finite,
    pub y: Finite,
}

/// A ready projection as `QueryApi::projection` returns it: its job record
/// and its stored frame.
///
/// Not serialized: on the wire the record is the JSON [`ProjectionInfo`]
/// and the frame its binary encoding, served separately (see the module
/// docs), and a client rebuilds the pair with [`Projection::new`].
///
/// Built only through [`Projection::new`]: the job is `Ready` and the
/// frame's header agrees with it (id, topic version, watermark, sample size,
/// matching and point counts).
#[derive(Debug, Clone, PartialEq)]
pub struct Projection {
    info: ProjectionInfo,
    frame: ProjectionFrame,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionMismatch {
    NotReady(ProjectionStatusKind),
    Id,
    TopicVersion,
    Watermark,
    Limit,
    Counts,
}

impl Projection {
    pub fn new(info: ProjectionInfo, frame: ProjectionFrame) -> Result<Self, ProjectionMismatch> {
        let ProjectionStatus::Ready(fitted) = info.status else {
            return Err(ProjectionMismatch::NotReady(info.status.kind()));
        };
        let header = frame.header();
        if header.projection != info.id {
            return Err(ProjectionMismatch::Id);
        }
        if header.topic_version != info.spec.topic_version() {
            return Err(ProjectionMismatch::TopicVersion);
        }
        if header.watermark != fitted.watermark {
            return Err(ProjectionMismatch::Watermark);
        }
        if header.limit != info.spec.params().limit() {
            return Err(ProjectionMismatch::Limit);
        }
        if header.matching != fitted.matching || frame.count() != fitted.points {
            return Err(ProjectionMismatch::Counts);
        }
        Ok(Self { info, frame })
    }

    pub fn info(&self) -> &ProjectionInfo {
        &self.info
    }

    pub fn frame(&self) -> &ProjectionFrame {
        &self.frame
    }

    pub fn watermark(&self) -> Watermark {
        self.frame.header().watermark
    }

    pub fn topic_version(&self) -> TopicModelVersion {
        self.frame.header().topic_version
    }

    /// Whether the points are a sample of fewer than `matching`.
    pub fn is_sampled(&self) -> bool {
        u64::from(self.frame.count()) < self.frame.header().matching
    }

    /// A point's topic slot under this projection's version.
    pub fn slot(&self, point: &ProjectedPoint) -> TopicSlot {
        TopicSlot {
            version: self.topic_version(),
            topic: point.topic,
        }
    }
}
