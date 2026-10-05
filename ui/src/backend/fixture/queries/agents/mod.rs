//! The agent reads: `agents` (canonical rows, newest agent first, filtered
//! by `AgentFilter::matches`), `agent` (one cluster, an alias redirected to
//! its canonical agent) and `agent_names` (`AgentReads::names`). Traffic is
//! the agent's node counts in the default-filter topology for the window
//! ([`profile::traffic`]), so rows agree with the graph.

pub mod profile;

use std::collections::HashMap;

use crosstalk_spec::aggregates::agents::filter::AgentFilter;
use crosstalk_spec::aggregates::agents::{
    AgentCluster, AgentClusterParts, AgentDetail, AgentLookup, AgentName, AgentRow,
};
use crosstalk_spec::aggregates::watermark::Watermarked;
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::ids::AgentId;
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::paging::{AgentList, Page, PageRequest};
use crosstalk_spec::support::TimeWindow;

use crate::backend::Result;

use super::graph::{aligned, watermarked};
use super::{Ctx, page};

/// A page of the canonical agents `filter` admits, newest agent first
/// (`AgentId` descending), each with its traffic in `window`. Cursors are
/// bound to the filter only: the window never changes which rows are
/// listed.
pub fn list(
    ctx: &Ctx,
    filter: &AgentFilter,
    window: TimeWindow,
    request: &PageRequest<AgentList>,
) -> Result<Watermarked<Page<AgentRow, AgentList>>> {
    aligned(window)?;
    let traffic = profile::traffic(ctx, window)?;
    let mut items = Vec::new();
    for id in ctx.canonical_agents() {
        let Some(profile) = profile::profile(ctx, id)? else {
            continue;
        };
        if filter.matches(&profile, ctx.aliases()) {
            let row = AgentRow {
                profile,
                traffic: traffic.get(&id).copied().unwrap_or_default(),
            };
            items.push(((0, u128::MAX - id.as_ulid()), row));
        }
    }
    page::paginate("agents", page::digest(filter), items, request).map(watermarked)
}

/// The detail of the canonical agent `id` resolves to, with its traffic in
/// `window`; `None` for an unknown id.
pub fn one(ctx: &Ctx, id: AgentId, window: TimeWindow) -> Result<Option<Watermarked<AgentDetail>>> {
    aligned(window)?;
    let Some(asked) = ctx.state.identity.agent(id) else {
        return Ok(None);
    };
    let canonical = ctx.agent(asked.id);
    let lookup = if canonical == id {
        AgentLookup::Canonical
    } else {
        AgentLookup::Redirected { from: id }
    };
    let Some(profile) = profile::profile(ctx, canonical)? else {
        return Err(store("no profile for a canonical agent", canonical));
    };
    let agent = ctx
        .state
        .identity
        .agent(canonical)
        .cloned()
        .ok_or_else(|| store("unknown canonical agent", canonical))?;
    let members = ctx.members(canonical);
    let in_cluster = |a: AgentId| members.contains(&a);
    let aliases = members
        .iter()
        .filter(|m| **m != canonical)
        .filter_map(|m| ctx.state.identity.agent(*m).cloned())
        .collect();
    let children = ctx
        .canonical_agents()
        .filter(|c| *c != canonical && profile::parent(ctx, *c) == Some(canonical))
        .collect();
    let merges = ctx
        .state
        .identity
        .merges()
        .iter()
        .filter(|m| {
            in_cluster(m.source())
                || in_cluster(m.target())
                || m.repointed().iter().copied().any(in_cluster)
        })
        .cloned()
        .collect();
    let vetoes = ctx
        .state
        .identity
        .vetoes()
        .iter()
        .filter(|v| in_cluster(v.a()) || in_cluster(v.b()))
        .copied()
        .collect();
    let cluster = AgentCluster::new(AgentClusterParts {
        profile,
        agent,
        aliases,
        children,
        merges,
        vetoes,
        lookup,
    })
    .map_err(|e| store("cluster", (canonical, e)))?;
    let traffic = profile::traffic(ctx, window)?
        .get(&canonical)
        .copied()
        .unwrap_or_default();
    Ok(Some(watermarked(AgentDetail { cluster, traffic })))
}

/// For each id of the batch that names a stored agent, merged or not, the
/// name of its canonical agent, keyed by the id asked for.
pub fn names(ctx: &Ctx, ids: &IdBatch<AgentId>) -> Result<HashMap<AgentId, AgentName>> {
    let mut names = HashMap::with_capacity(ids.len());
    for id in ids.ids() {
        if ctx.state.identity.agent(*id).is_none() {
            continue;
        }
        let canonical = ctx.agent(*id);
        let name = ctx
            .state
            .identity
            .agent(canonical)
            .and_then(AgentName::of)
            .ok_or_else(|| store("canonical agent without a name", canonical))?;
        names.insert(*id, name);
    }
    Ok(names)
}

fn store(what: &str, detail: impl std::fmt::Debug) -> QueryError {
    QueryError::Store {
        reason: format!("fixture agents: {what}: {detail:?}"),
    }
}
