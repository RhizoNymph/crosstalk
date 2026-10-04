//! The 2-D projection (UMAP) of transmission embeddings.
//!
//! A layout is fitted with each topic-model version (the same UMAP the topic
//! model clusters on, reduced to two dimensions) and later transmissions
//! classified under that version are placed into it. A [`ProjectionToken`]
//! names one layout. Within one token, a transmission's coordinates and topic
//! never change, so a client can merge points from several queries that
//! carry the same token, and must drop what it holds when the token changes.
//!
//! A projection holds the transmissions placed in the layout, confirmed in
//! the query window and admitted by the [`TopologyFilter`]. Each point
//! carries what a client needs to colour and link it without a lookup per
//! point: canonical sender and reader, route kind, topic and confirmation
//! time.
//!
//! **Sampling.** At most [`ProjectionLimit`] points are returned. When more
//! transmissions match, the projection keeps the `limit` whose sample key
//! (a BLAKE3 of the transmission id keyed by the token) is smallest. The
//! sample is therefore fixed per token and stable under narrowing: a point
//! returned for a filter and window is also returned for any narrower
//! filter or sub-window that still admits it.
//!
//! [`TopologyFilter`]: crate::aggregates::filter::TopologyFilter

use std::collections::HashSet;
use std::num::NonZeroU32;

use crate::aggregates::edge::{RouteKind, TopicSlot};
use crate::aggregates::topic::TopicModelVersion;
use crate::ids::{AgentId, TopicId, TransmissionId};
use crate::support::Timestamp;

/// Names one fitted layout: the topic-model version it was fitted with and
/// a revision within that version (a layout can be re-fitted without a new
/// topic model). Opaque to clients beyond equality.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProjectionToken {
    topic_version: TopicModelVersion,
    revision: u32,
}

impl ProjectionToken {
    pub const fn new(topic_version: TopicModelVersion, revision: u32) -> Self {
        Self {
            topic_version,
            revision,
        }
    }

    /// The version every point's topic is under.
    pub const fn topic_version(self) -> TopicModelVersion {
        self.topic_version
    }

    pub const fn revision(self) -> u32 {
        self.revision
    }
}

/// The most points a projection may return: `1..=ProjectionLimit::MAX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProjectionLimit(NonZeroU32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidProjectionLimit {
    Zero,
    AboveMax { max: u32, got: u32 },
}

impl ProjectionLimit {
    /// Bounds one response to a few megabytes and what a browser canvas
    /// renders interactively.
    pub const MAX: u32 = 50_000;

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

/// One transmission in a projection.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProjectedPoint {
    pub transmission: TransmissionId,
    /// Canonical sender at query time.
    pub from: AgentId,
    /// Canonical reader at query time. Equal to `from` when the two agents
    /// have since been merged; the topology graph drops such transmissions.
    pub to: AgentId,
    pub route: RouteKind,
    /// The topic under the projection's topic version; `None` for an outlier.
    /// The full slot is [`Projection::slot`].
    pub topic: Option<TopicId>,
    /// `Confirmed::at`.
    pub confirmed_at: Timestamp,
    pub x: f32,
    pub y: f32,
}

/// The points of one layout for one window and filter.
///
/// Built only through [`Projection::new`]: it holds exactly
/// `min(matching, limit)` points, no transmission twice, and only finite
/// coordinates. Every point's topic is under the token's topic version,
/// which the type holds once.
#[derive(Debug, Clone, PartialEq)]
pub struct Projection {
    token: ProjectionToken,
    matching: u64,
    points: Vec<ProjectedPoint>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidProjection {
    /// `points` does not hold `min(matching, limit)` points.
    WrongCount {
        expected: u64,
        got: usize,
    },
    Duplicate(TransmissionId),
    NonFinite(TransmissionId),
}

impl Projection {
    /// `matching` counts every transmission the window and filter admit,
    /// before sampling.
    pub fn new(
        token: ProjectionToken,
        limit: ProjectionLimit,
        matching: u64,
        points: Vec<ProjectedPoint>,
    ) -> Result<Self, InvalidProjection> {
        let expected = matching.min(u64::from(limit.get().get()));
        if u64::try_from(points.len()).ok() != Some(expected) {
            return Err(InvalidProjection::WrongCount {
                expected,
                got: points.len(),
            });
        }
        let mut seen = HashSet::with_capacity(points.len());
        for point in &points {
            if !seen.insert(point.transmission) {
                return Err(InvalidProjection::Duplicate(point.transmission));
            }
            if !(point.x.is_finite() && point.y.is_finite()) {
                return Err(InvalidProjection::NonFinite(point.transmission));
            }
        }
        Ok(Self {
            token,
            matching,
            points,
        })
    }

    pub fn token(&self) -> ProjectionToken {
        self.token
    }

    pub fn topic_version(&self) -> TopicModelVersion {
        self.token.topic_version()
    }

    /// Transmissions admitted before sampling.
    pub fn matching(&self) -> u64 {
        self.matching
    }

    /// Whether `points` is a sample of fewer than `matching` transmissions.
    pub fn is_sampled(&self) -> bool {
        u64::try_from(self.points.len()).is_ok_and(|len| len < self.matching)
    }

    pub fn points(&self) -> &[ProjectedPoint] {
        &self.points
    }

    /// A point's topic slot under this projection's version.
    pub fn slot(&self, point: &ProjectedPoint) -> TopicSlot {
        TopicSlot {
            version: self.topic_version(),
            topic: point.topic,
        }
    }
}
