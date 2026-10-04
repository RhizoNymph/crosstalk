//! The topic history (versions, sizes, lineage) and a version's topics.

use std::collections::BTreeSet;

use crosstalk_spec::aggregates::filter::{TopicVersionSelector, VersionUnavailable};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::topic_history::{TopicLineage, TopicSizes, TopicVersionHistory};
use crosstalk_spec::aggregates::watermark::Watermarked;
use crosstalk_spec::ids::TopicId;
use crosstalk_spec::interfaces::l6_analysis::TopicCatalog;
use crosstalk_spec::interfaces::l7_topology::EdgeStore;
use crosstalk_spec::interfaces::l8_surface::lists::TopicPage;
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission, QueryError};
use crosstalk_spec::paging::{PageRequest, TopicList};
use crosstalk_spec::support::TimeWindow;

use crate::service::{Surface, largest_page, require};
use crate::stores::SurfaceStores;

impl<S: SurfaceStores> Surface<S> {
    pub(crate) async fn topic_versions_query(
        &self,
        caller: &Caller,
    ) -> Result<TopicVersionHistory, QueryError> {
        require(caller, Permission::View)?;
        Ok(self.stores.topics().versions().await?)
    }

    /// `TopicCatalog::sizes` of `version` (the active version when `None`),
    /// with L7's watermark read first.
    pub(crate) async fn topic_sizes_query(
        &self,
        caller: &Caller,
        version: Option<TopicModelVersion>,
        window: Option<TimeWindow>,
    ) -> Result<Watermarked<TopicSizes>, QueryError> {
        require(caller, Permission::View)?;
        let watermark = self.stores.edges().watermark().await?;
        let version = match version {
            Some(version) => version,
            None => self.stores.topics().versions().await?.active().version(),
        };
        let value = self.stores.topics().sizes(version, window).await?;
        Ok(Watermarked { watermark, value })
    }

    pub(crate) async fn topic_lineage_query(
        &self,
        caller: &Caller,
        from: TopicModelVersion,
    ) -> Result<Option<TopicLineage>, QueryError> {
        require(caller, Permission::View)?;
        Ok(self.stores.topics().lineage(from).await?)
    }

    /// A version's topics: `Current` is the active version, a pinned one any
    /// version whose fit has returned (the catalog refuses the others).
    pub(crate) async fn topics_query(
        &self,
        caller: &Caller,
        version: TopicVersionSelector,
        page: &PageRequest<TopicList>,
    ) -> Result<TopicPage, QueryError> {
        require(caller, Permission::Content)?;
        let version = match version {
            TopicVersionSelector::Current => {
                self.stores.topics().versions().await?.active().version()
            }
            TopicVersionSelector::Pinned(version) => version,
        };
        let page = self.stores.topics().topics(version, page).await?;
        Ok(TopicPage { version, page })
    }

    /// The version `selector` resolves to as a linked view resolves it,
    /// retention deciding what is retained, and that version's topic ids.
    pub(crate) async fn resolve_version(
        &self,
        selector: TopicVersionSelector,
    ) -> Result<(TopicModelVersion, BTreeSet<TopicId>), QueryError> {
        let history = self.stores.topics().versions().await?;
        let version = resolve_in(&history, selector)?;
        let topics = self.topic_ids(version).await?;
        Ok((version, topics))
    }

    /// Every topic id of `version`: a full `TopicCatalog::topics` traversal.
    pub(crate) async fn topic_ids(
        &self,
        version: TopicModelVersion,
    ) -> Result<BTreeSet<TopicId>, QueryError> {
        let mut request = PageRequest {
            size: largest_page()?,
            after: None,
        };
        let mut topics = BTreeSet::new();
        loop {
            let page = self.stores.topics().topics(version, &request).await?;
            let (items, next) = page.into_parts();
            topics.extend(items.into_iter().map(|topic| topic.id));
            match next {
                Some(next) => request.after = Some(next),
                None => return Ok(topics),
            }
        }
    }
}

/// `selector` resolved against `history`, a version retained exactly when
/// retention has not dropped it.
pub(crate) fn resolve_in(
    history: &TopicVersionHistory,
    selector: TopicVersionSelector,
) -> Result<TopicModelVersion, QueryError> {
    selector
        .resolve(history, |version| retained(history, version))
        .map_err(QueryError::from)
}

/// Whether retention still keeps `version`'s data.
pub(crate) fn retained(history: &TopicVersionHistory, version: TopicModelVersion) -> bool {
    history
        .get(version)
        .is_some_and(|info| info.retention().is_retained())
}

/// `VersionNotRetained` for a version retention has dropped since a cursor
/// pinned it.
pub(crate) fn still_retained(
    history: &TopicVersionHistory,
    version: TopicModelVersion,
) -> Result<(), QueryError> {
    if retained(history, version) {
        Ok(())
    } else if history.get(version).is_some() {
        Err(VersionUnavailable::NotRetained(version).into())
    } else {
        Err(VersionUnavailable::Unknown(version).into())
    }
}
