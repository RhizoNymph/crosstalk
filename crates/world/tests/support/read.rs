//! Whole-list reads through the store read traits, following cursors.

#![allow(dead_code)]

use crosstalk_spec::aggregates::agents::AgentProfile;
use crosstalk_spec::aggregates::alert::{Alert, AlertRuleDef};
use crosstalk_spec::aggregates::projection::ProjectionInfo;
use crosstalk_spec::derived::flow::channel::Channel;
use crosstalk_spec::interfaces::l2_transport::{DeadLetter, DeadLetterStore};
use crosstalk_spec::interfaces::l3_reconstruction::agents::AgentReads;
use crosstalk_spec::interfaces::l5_flow::channels::ChannelReads;
use crosstalk_spec::interfaces::l6_analysis::ProjectionStore;
use crosstalk_spec::interfaces::l6_analysis::alerts::AlertReads;
use crosstalk_spec::interfaces::l8_surface::AlertFilter;
use crosstalk_spec::interfaces::l8_surface::audit::{AuditEntry, AuditFilter, AuditLog};
use crosstalk_spec::interfaces::l8_surface::lists::AgentFilter;
use crosstalk_spec::interfaces::l8_surface::lists::{AlertRuleFilter, ChannelFilter};
use crosstalk_spec::paging::{Page, PageRequest, PageSize};

use super::Seeded;

/// Follows `read`'s cursors from the first page to the last.
async fn collect<T, L, E, F, Fut>(mut read: F) -> Result<Vec<T>, String>
where
    T: Clone,
    E: std::fmt::Debug,
    F: FnMut(PageRequest<L>) -> Fut,
    Fut: std::future::Future<Output = Result<Page<T, L>, E>>,
{
    let size = PageSize::new(PageSize::MAX).map_err(|e| format!("{e:?}"))?;
    let mut out = Vec::new();
    let mut after = None;
    loop {
        let request = PageRequest { size, after };
        let page = read(request).await.map_err(|e| format!("{e:?}"))?;
        let (items, next) = page.into_parts();
        out.extend(items);
        match next {
            Some(cursor) => after = Some(cursor),
            None => return Ok(out),
        }
    }
}

pub async fn agents(seeded: &Seeded) -> Result<Vec<AgentProfile>, String> {
    let filter = AgentFilter::default();
    collect(|page| {
        let store = seeded.stores.agents.clone();
        let filter = filter.clone();
        async move { store.list(&filter, &page).await }
    })
    .await
}

pub async fn channels(seeded: &Seeded, filter: ChannelFilter) -> Result<Vec<Channel>, String> {
    let listed = collect(|page| {
        let store = seeded.stores.channels.clone();
        let filter = filter.clone();
        async move { store.channels(&filter, &page).await }
    })
    .await?;
    Ok(listed.into_iter().map(|read| read.into_parts().0).collect())
}

pub async fn alerts(seeded: &Seeded) -> Result<Vec<Alert>, String> {
    collect(|page| {
        let store = seeded.stores.alerts.clone();
        async move { store.alerts(&AlertFilter::default(), &page).await }
    })
    .await
}

pub async fn rules(seeded: &Seeded) -> Result<Vec<AlertRuleDef>, String> {
    collect(|page| {
        let store = seeded.stores.alerts.clone();
        async move { store.rules(&AlertRuleFilter::default(), &page).await }
    })
    .await
}

pub async fn audit(seeded: &Seeded) -> Result<Vec<AuditEntry>, String> {
    collect(|page| {
        let store = seeded.stores.audit.clone();
        async move { store.query(&AuditFilter::default(), &page).await }
    })
    .await
}

pub async fn letters(seeded: &Seeded) -> Result<Vec<DeadLetter>, String> {
    collect(|page| {
        let store = seeded.stores.letters.clone();
        async move { store.list(None, &page).await }
    })
    .await
}

pub async fn jobs(seeded: &Seeded) -> Result<Vec<ProjectionInfo>, String> {
    collect(|page| {
        let store = seeded.stores.projections.clone();
        async move { store.list(&page).await }
    })
    .await
}
