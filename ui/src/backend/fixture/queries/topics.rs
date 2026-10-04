//! The topic catalog's reads: the version history, topic sizes, lineage
//! and a version's topics, failing as `TopicCatalog` does (its
//! `CatalogError`s become the surface's errors through the spec's `From`).
//!
//! Sizes count topic assignments, not graph edges: every confirmed
//! transmission with an assignment under the version, by `Confirmed::at`,
//! self-edges included. A dropped version's all-time sizes are frozen at
//! its drop: the transmissions confirmed by then.

use std::collections::HashMap;
use std::num::NonZeroU64;

use crosstalk_spec::aggregates::edge::EdgeStats;
use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::aggregates::retention::Retention;
use crosstalk_spec::aggregates::topic::{Assignment, TopicModelVersion};
use crosstalk_spec::aggregates::topic_history::{
    TopicLineage, TopicSize, TopicSizes, TopicVersionHistory, TopicVersionInfo,
    TopicVersionStatusKind,
};
use crosstalk_spec::aggregates::watermark::Watermarked;
use crosstalk_spec::ids::TopicId;
use crosstalk_spec::interfaces::l6_analysis::CatalogError;
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::interfaces::l8_surface::lists::TopicPage;
use crosstalk_spec::paging::{PageRequest, TopicList};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use crate::backend::Result;
use crate::backend::fixture::world::confirmed;

use super::Ctx;
use super::graph::{store_error, watermarked};
use super::page::{Key, digest, paginate, pinned, versioned};

const TOPICS: &str = "topics";

pub fn versions(ctx: &Ctx) -> TopicVersionHistory {
    ctx.world.topics.history.clone()
}

/// A version whose fit has returned: unknown is `NotFound`, fitting
/// `Conflict(TopicVersionFitting)`.
fn fitted<'a>(ctx: &'a Ctx<'a>, version: TopicModelVersion) -> Result<&'a TopicVersionInfo> {
    let info = ctx
        .world
        .topics
        .info(version)
        .ok_or(CatalogError::UnknownVersion(version))?;
    if info.status().kind() == TopicVersionStatusKind::Fitting {
        return Err(CatalogError::StillFitting(version).into());
    }
    Ok(info)
}

/// Transmissions and matched bytes, summed.
#[derive(Debug, Clone, Copy, Default)]
struct Tally {
    transmissions: u64,
    matched_bytes: u64,
}

impl Tally {
    fn add(&mut self, matched_bytes: NonZeroU64) {
        self.transmissions += 1;
        self.matched_bytes += matched_bytes.get();
    }

    /// `None` when nothing was counted.
    fn stats(self) -> Option<EdgeStats> {
        Some(EdgeStats {
            transmissions: NonZeroU64::new(self.transmissions)?,
            matched_bytes: NonZeroU64::new(self.matched_bytes)?,
        })
    }
}

pub fn sizes(
    ctx: &Ctx,
    version: Option<TopicModelVersion>,
    window: Option<TimeWindow>,
) -> Result<Watermarked<TopicSizes>> {
    let topics = &ctx.world.topics;
    let version = version.unwrap_or_else(|| topics.active());
    let info = fitted(ctx, version)?;
    let frozen: Option<Timestamp> = match (info.retention(), window) {
        (Retention::Dropped { .. }, Some(_)) => {
            return Err(CatalogError::VersionNotRetained(version).into());
        }
        (Retention::Dropped { at }, None) => Some(at),
        (Retention::Retained { .. }, _) => None,
    };
    let mut tallies: HashMap<Option<TopicId>, Tally> = HashMap::new();
    for record in &ctx.world.transmissions {
        let (Some(confirmation), Some(assignment)) = (
            confirmed(&record.transmission.state),
            record.assignment(version),
        ) else {
            continue;
        };
        let at = confirmation.at();
        if window.is_some_and(|w| !w.contains(at)) || frozen.is_some_and(|cut| at > cut) {
            continue;
        }
        let topic = match assignment {
            Assignment::Topic { topic, .. } => Some(topic),
            Assignment::Outlier => None,
        };
        tallies
            .entry(topic)
            .or_default()
            .add(confirmation.matched_bytes());
    }
    let mut of_version: Vec<TopicId> = topics.topics_of(version).map(|t| t.id).collect();
    of_version.sort_unstable_by(|a, b| b.cmp(a));
    let sizes = of_version
        .into_iter()
        .map(|topic| TopicSize {
            topic,
            stats: tallies.get(&Some(topic)).and_then(|t| t.stats()),
        })
        .collect();
    let outliers = tallies.get(&None).and_then(|t| t.stats());
    TopicSizes::new(version, window, sizes, outliers)
        .map(watermarked)
        .map_err(|e| store_error("topic sizes", e))
}

pub fn lineage(ctx: &Ctx, from: TopicModelVersion) -> Result<Option<TopicLineage>> {
    let topics = &ctx.world.topics;
    topics
        .info(from)
        .ok_or(CatalogError::UnknownVersion(from))?;
    Ok(topics.lineage(from).cloned())
}

/// A page of a version's topics, newest id first. A later page reads the
/// version its cursor pinned; a cursor of another version is
/// `InvalidCursor`.
pub fn topics(
    ctx: &Ctx,
    selector: TopicVersionSelector,
    page: &PageRequest<TopicList>,
) -> Result<TopicPage> {
    let version = match (selector, pinned(TOPICS, page)?) {
        (TopicVersionSelector::Pinned(asked), Some(cursor)) if asked != cursor => {
            return Err(QueryError::InvalidCursor);
        }
        (_, Some(cursor)) => cursor,
        (TopicVersionSelector::Pinned(asked), None) => asked,
        (TopicVersionSelector::Current, None) => ctx.world.topics.active(),
    };
    fitted(ctx, version)?;
    let items: Vec<(Key, _)> = ctx
        .world
        .topics
        .topics_of(version)
        .map(|topic| ((0, u128::MAX - topic.id.as_ulid()), topic.clone()))
        .collect();
    let page = paginate(&versioned(TOPICS, version), digest(&version), items, page)?;
    Ok(TopicPage { version, page })
}
