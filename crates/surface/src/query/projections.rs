//! Projection jobs: fitting one (recorded and returned at once), their
//! status and list, and a ready projection's stored frame.

use crosstalk_spec::aggregates::edge::TopologyFilter;
use crosstalk_spec::aggregates::projection::{
    Projection, ProjectionInfo, ProjectionParams, ProjectionSpec,
};
use crosstalk_spec::ids::ProjectionId;
use crosstalk_spec::interfaces::l6_analysis::{Embedder, ProjectionStore};
use crosstalk_spec::interfaces::l8_surface::{Caller, ConflictKind, Permission, QueryError};
use crosstalk_spec::paging::{Page, PageRequest, ProjectionList};
use crosstalk_spec::support::TimeWindow;

use crate::service::{Surface, require};
use crate::stores::SurfaceStores;

impl<S: SurfaceStores> Surface<S> {
    /// Resolve and pin the filter's version as a linked view does, refuse
    /// topics outside it, and record a queued job with the current
    /// embedding model, the params, the caller and the time. Returns the new
    /// job's id without waiting for the fit.
    pub(crate) async fn fit_projection_query(
        &self,
        caller: &Caller,
        window: TimeWindow,
        filter: &TopologyFilter,
        params: ProjectionParams,
    ) -> Result<ProjectionId, QueryError> {
        require(caller, Permission::Content)?;
        let (version, topics) = self.resolve_version(filter.topic_version).await?;
        let outside = filter.topics_outside(version, |topic| {
            topics.contains(&topic).then_some(version)
        });
        if !outside.is_empty() {
            return Err(QueryError::Conflict(ConflictKind::TopicsNotInVersion {
                version,
                topics: outside,
            }));
        }
        let model = self.stores.embedder().model();
        let spec = ProjectionSpec::new(window, filter.clone(), version, params, model);
        let id: ProjectionId = self.mint()?;
        let job = ProjectionInfo::queued(id, spec, caller.operator(), self.now());
        self.stores.projections().clone().enqueue(job).await?;
        Ok(id)
    }

    pub(crate) async fn projection_status_query(
        &self,
        caller: &Caller,
        id: ProjectionId,
    ) -> Result<ProjectionInfo, QueryError> {
        require(caller, Permission::Content)?;
        self.stores
            .projections()
            .info(id)
            .await?
            .ok_or(QueryError::NotFound)
    }

    pub(crate) async fn projections_query(
        &self,
        caller: &Caller,
        page: &PageRequest<ProjectionList>,
    ) -> Result<Page<ProjectionInfo, ProjectionList>, QueryError> {
        require(caller, Permission::Content)?;
        Ok(self.stores.projections().list(page).await?)
    }

    pub(crate) async fn projection_query(
        &self,
        caller: &Caller,
        id: ProjectionId,
    ) -> Result<Projection, QueryError> {
        require(caller, Permission::Content)?;
        Ok(self.stores.projections().projection(id).await?)
    }
}
