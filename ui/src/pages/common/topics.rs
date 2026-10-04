//! Topic-model reads shared by pages and data routes: the version views
//! default to, every topic of a version, and per-topic trends.

use std::collections::HashMap;
use std::num::NonZeroU32;

use crosstalk_spec::aggregates::edge::Weighting;
use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::aggregates::series::{SeriesGrouping, SeriesGroups};
use crosstalk_spec::aggregates::topic::{Topic, TopicModelVersion};
use crosstalk_spec::ids::TopicId;
use crosstalk_spec::interfaces::l8_surface::lists::TopicPage;
use crosstalk_spec::interfaces::l8_surface::{Caller, QueryError};
use crosstalk_spec::paging::{PageRequest, PageSize, TopicList};
use crosstalk_spec::support::TimeWindow;

use crate::contract::present::Present;
use crate::data::timeline::timeline_grid;
use crate::error::UiError;
use crate::url::scope::ViewFilter;
use crosstalk_spec::interfaces::l8_surface::QueryApi;

/// Trend points over a view's window.
pub const TREND_BUCKETS: NonZeroU32 = match NonZeroU32::new(24) {
    Some(n) => n,
    None => NonZeroU32::MIN,
};

/// Pages read before a topic listing is cut short. A version holds tens
/// of topics; this only bounds a backend that never stops paging.
const MAX_PAGES: usize = 100;

/// The version a view without one reads: the history's active version, the
/// one graphs and series read. Needs `View`.
pub async fn default_version<B: QueryApi>(
    backend: &B,
    caller: &Caller,
) -> Result<TopicModelVersion, QueryError> {
    Ok(backend.topic_versions(caller).await?.active().version())
}

/// Every topic of the version `selector` names, newest id first, and that
/// version: `topics` followed to its last page. Needs `Content`.
pub async fn all_topics<B: QueryApi>(
    backend: &B,
    caller: &Caller,
    selector: TopicVersionSelector,
) -> Result<(TopicModelVersion, Vec<Topic>), QueryError> {
    let mut request = PageRequest::<TopicList> {
        size: crate::pages::common::paging::size(PageSize::MAX),
        after: None,
    };
    let mut topics = Vec::new();
    for _ in 0..MAX_PAGES {
        let TopicPage { version, page } = backend.topics(caller, selector, &request).await?;
        let (items, next) = page.into_parts();
        topics.extend(items);
        match next {
            Some(cursor) => request.after = Some(cursor),
            None => return Ok((version, topics)),
        }
    }
    Err(QueryError::Store {
        reason: format!("topics did not end within {MAX_PAGES} pages"),
    })
}

/// Each topic's transmissions per step of one grid; `None` is the
/// outliers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Trends {
    points: usize,
    by_topic: HashMap<Option<TopicId>, Vec<u64>>,
}

impl Trends {
    #[cfg(test)]
    pub fn new(points: usize, by_topic: HashMap<Option<TopicId>, Vec<u64>>) -> Self {
        Self { points, by_topic }
    }

    /// A topic's values; zeros when it counted nothing (a series query
    /// leaves such topics out), empty when no trend was read.
    pub fn of(&self, topic: Option<TopicId>) -> Vec<u64> {
        self.by_topic
            .get(&topic)
            .cloned()
            .unwrap_or_else(|| vec![0; self.points])
    }
}

/// Transmissions per step of a [`TREND_BUCKETS`]-point grid over `window`
/// for each topic of `version` (and its outliers), from one `series`
/// grouped by topic with no filter but the version, so a trend follows
/// what the version's topic sizes count (though graph-counted: self-edges
/// left out).
pub async fn topic_trends<B: QueryApi + Present>(
    backend: &B,
    caller: &Caller,
    window: TimeWindow,
    version: TopicModelVersion,
) -> Result<Trends, UiError> {
    let grid = timeline_grid(window, backend.bucket_width(), TREND_BUCKETS)
        .map_err(|e| UiError::field("window", e))?;
    let series = backend
        .series(
            caller,
            grid,
            Weighting::Transmissions,
            SeriesGrouping::Topic,
            &ViewFilter::default().pinned(version),
        )
        .await?;
    let points = usize::try_from(grid.points().get()).unwrap_or(usize::MAX);
    match series.value.groups() {
        SeriesGroups::ByTopic(groups) => Ok(Trends {
            points,
            by_topic: groups
                .iter()
                .map(|series| (series.key, series.values.clone()))
                .collect(),
        }),
        other => Err(UiError::Query(QueryError::Store {
            reason: format!("a topic series came back grouped by {:?}", other.grouping()),
        })),
    }
}
