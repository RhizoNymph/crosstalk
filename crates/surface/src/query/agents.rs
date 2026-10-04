//! The agent queries: rows (L3's profiles joined with L7's traffic), one
//! agent's detail following merges, and names in batches.

use std::collections::BTreeMap;

use crosstalk_spec::aggregates::agents::{AgentDetail, AgentName, AgentRow};
use crosstalk_spec::aggregates::watermark::Watermarked;
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::ids::AgentId;
use crosstalk_spec::interfaces::l3_reconstruction::agents::AgentReads;
use crosstalk_spec::interfaces::l7_topology::EdgeStore;
use crosstalk_spec::interfaces::l8_surface::lists::AgentFilter;
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission, QueryError};
use crosstalk_spec::paging::{AgentList, Page, PageRequest};
use crosstalk_spec::support::TimeWindow;

use crate::service::{Surface, page_of, require};
use crate::stores::SurfaceStores;

impl<S: SurfaceStores> Surface<S> {
    /// A page of L3's canonical profiles, each with its traffic in `window`
    /// from one `EdgeStore::agent_traffic` call over the page's agents,
    /// whose watermark the page carries.
    pub(crate) async fn agents_query(
        &self,
        caller: &Caller,
        filter: &AgentFilter,
        window: TimeWindow,
        page: &PageRequest<AgentList>,
    ) -> Result<Watermarked<Page<AgentRow, AgentList>>, QueryError> {
        require(caller, Permission::View)?;
        let window = self.aligned(window)?;
        let listed = self.stores.agents().list(filter, page).await?;
        let (profiles, next) = listed.into_parts();
        let ids: Vec<AgentId> = profiles.iter().map(|profile| profile.id()).collect();
        let traffic = self.stores.edges().agent_traffic(window, &ids).await?;
        let rows = profiles
            .into_iter()
            .map(|profile| AgentRow {
                traffic: traffic
                    .value
                    .get(&profile.id())
                    .copied()
                    .unwrap_or_default(),
                profile,
            })
            .collect();
        Ok(Watermarked {
            watermark: traffic.watermark,
            value: page_of(page.size, rows, next)?,
        })
    }

    pub(crate) async fn agent_query(
        &self,
        caller: &Caller,
        id: AgentId,
        window: TimeWindow,
    ) -> Result<Option<Watermarked<AgentDetail>>, QueryError> {
        require(caller, Permission::View)?;
        let window = self.aligned(window)?;
        let Some(cluster) = self.stores.agents().cluster(id).await? else {
            return Ok(None);
        };
        let canonical = cluster.profile().id();
        let traffic = self
            .stores
            .edges()
            .agent_traffic(window, &[canonical])
            .await?;
        let counts = traffic.value.get(&canonical).copied().unwrap_or_default();
        Ok(Some(Watermarked {
            watermark: traffic.watermark,
            value: AgentDetail {
                cluster,
                traffic: counts,
            },
        }))
    }

    pub(crate) async fn agent_names_query(
        &self,
        caller: &Caller,
        ids: &IdBatch<AgentId>,
    ) -> Result<BTreeMap<AgentId, AgentName>, QueryError> {
        require(caller, Permission::View)?;
        Ok(self.stores.agents().names(ids).await?)
    }
}
