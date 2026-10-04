//! What the topology drawer shows for a selection, loaded and owned.
//!
//! The shard hands this module its arguments as text (the view state's
//! canonical query, the selection, the transmissions cursor); every one is
//! validated here before the backend is called.

use std::num::NonZeroU32;

use crosstalk_spec::aggregates::edge::{
    EdgeSelector, EdgeTransmission, RouteKind, TopologyGraph, WeightedEdge, Weighting,
};
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::{AgentId, ChannelId, TransmissionId};
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission, PolicyKind};
use crosstalk_spec::observed::client::HarnessClaim;
use topcoat::context::Cx;

use crate::app::backend;
use crate::backend::Backend;
use crate::components::{
    agent_name, format_bytes, format_share, format_time, format_time_short, href,
};
use crate::contract::agents::AgentStateKind;
use crate::error::UiError;
use crate::pages::channels::list::Activity;
use crate::pages::channels::model::origin_kind;
use crate::pages::common::action::require;
use crate::pages::common::form::invalid;
use crate::pages::common::links::{agent_url, channel_url, transmission_url};
use crate::pages::common::lookup::{AgentNames, agent_names};
use crate::pages::common::paging::{Count, parse_cursor};
use crate::pages::common::transmissions::summary_name;
use crate::pages::common::transmissions::{
    ChannelNames, Named, channel_names, route_channel, route_text,
};
use crate::pages::topology::selection::Selection;
use crate::pages::view::state_from_query;
use crate::url::view_state::ViewState;
use crosstalk_spec::aggregates::node::CanonicalOriginKind;
use crosstalk_spec::derived::flow::channel::detection::DetectionKind;
use crosstalk_spec::paging::PageRequest;

/// Transmissions per drawer page: the drawer is narrow and short.
pub const DRAWER_PAGE: NonZeroU32 = match NonZeroU32::new(12) {
    Some(n) => n,
    None => NonZeroU32::MIN,
};

/// Edges listed in the empty drawer and on agent and channel cards.
const LISTED_EDGES: usize = 8;

/// An edge in a list, selectable from the drawer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeItem {
    /// The selection value that selects it.
    pub code: String,
    pub from: String,
    pub to: String,
    pub route_kind: RouteKind,
    pub route: String,
    pub share: String,
    pub transmissions: u64,
}

/// One transmission the edge counts. Its sender, reader and route are the
/// edge's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeRow {
    pub id: TransmissionId,
    /// The evidence page.
    pub url: String,
    /// `Confirmed::at`.
    pub confirmed: String,
    pub matched: String,
}

impl EdgeRow {
    pub fn new(row: &EdgeTransmission, state: &ViewState) -> Self {
        Self {
            id: row.transmission,
            url: transmission_url(row.transmission, state),
            confirmed: format_time_short(row.confirmed_at),
            matched: format_bytes(row.matched_bytes.get()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeStatsView {
    pub transmissions: u64,
    pub matched: String,
    pub share: String,
    /// What the share is a share of.
    pub of: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgePanel {
    pub from: Named,
    pub to: Named,
    pub route_kind: RouteKind,
    pub route: String,
    pub route_url: Option<String>,
    /// `None` when the edge carries nothing under the current filter.
    pub stats: Option<EdgeStatsView>,
    pub rows: Vec<EdgeRow>,
    pub next: Option<String>,
    pub paged: bool,
    /// The topology filtered to the edge's two agents.
    pub focus_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentPanel {
    pub name: String,
    pub url: String,
    pub state: AgentStateKind,
    pub claims: Vec<HarnessClaim>,
    pub parent: Option<Named>,
    pub transmissions_in: u64,
    pub transmissions_out: u64,
    pub last_seen: String,
    pub edges: Vec<EdgeItem>,
    pub focus_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelPanel {
    pub name: String,
    pub url: String,
    pub origin: CanonicalOriginKind,
    pub detection: DetectionKind,
    pub policy: PolicyKind,
    /// Counted in the view's window; a superseded channel's on its channel
    /// in force.
    pub activity: Activity,
    pub edges: Vec<EdgeItem>,
    pub focus_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Drawer {
    Empty {
        heaviest: Vec<EdgeItem>,
    },
    Edge(EdgePanel),
    Agent(AgentPanel),
    Channel(ChannelPanel),
    /// The selection names nothing the backend knows.
    Missing(&'static str),
}

fn weighting_noun(weighting: Weighting) -> &'static str {
    match weighting {
        Weighting::Transmissions => "transmissions",
        Weighting::MatchedBytes => "matched bytes",
    }
}

/// The edges matching `keep`, heaviest first, as list items.
pub fn edge_items(
    edges: &[WeightedEdge],
    keep: impl Fn(&WeightedEdge) -> bool,
    agents: &AgentNames,
    channels: &ChannelNames,
) -> Vec<EdgeItem> {
    let mut picked: Vec<&WeightedEdge> = edges.iter().filter(|e| keep(e)).collect();
    picked.sort_by(|a, b| {
        b.share
            .get()
            .total_cmp(&a.share.get())
            .then_with(|| b.stats.transmissions.cmp(&a.stats.transmissions))
    });
    picked
        .into_iter()
        .take(LISTED_EDGES)
        .map(|e| EdgeItem {
            code: Selection::edge(e.from, e.to, &e.route).encode(),
            from: agents.name(e.from),
            to: agents.name(e.to),
            route_kind: RouteKind::from(&e.route),
            route: route_text(&e.route, channels),
            share: format_share(e.share.get()),
            transmissions: e.stats.transmissions.get(),
        })
        .collect()
}

/// The same view filtered to `agents` and `channels` (empty: unchanged),
/// with the selection kept.
fn focus_url(
    state: &ViewState,
    agents: Vec<AgentId>,
    channels: Vec<ChannelId>,
    sel: &str,
) -> String {
    let mut next = state.clone();
    if !agents.is_empty() {
        next.scope.filter.agents = agents;
    }
    if !channels.is_empty() {
        next.scope.filter.channels = channels;
    }
    href(super::super::PATH, &next, &[("sel", sel)])
}

async fn names_for(
    cx: &Cx,
    caller: &Caller,
    edges: &[&WeightedEdge],
) -> (AgentNames, ChannelNames) {
    let agents = agent_names(
        cx,
        caller,
        edges
            .iter()
            .flat_map(|e| [e.from, e.to])
            .collect::<Vec<_>>(),
    )
    .await;
    let channels = channel_names(
        cx,
        caller,
        edges
            .iter()
            .filter_map(|e| route_channel(&e.route))
            .collect::<Vec<_>>(),
    )
    .await;
    (agents, channels)
}

/// The listed edges among `edges` that `keep` selects, with their names.
async fn listed(
    cx: &Cx,
    caller: &Caller,
    graph: &TopologyGraph,
    keep: impl Fn(&WeightedEdge) -> bool,
) -> Vec<EdgeItem> {
    let mut chosen: Vec<&WeightedEdge> = graph.edges.iter().filter(|e| keep(e)).collect();
    chosen.sort_by(|a, b| b.share.get().total_cmp(&a.share.get()));
    chosen.truncate(LISTED_EDGES);
    let (agents, channels) = names_for(cx, caller, &chosen).await;
    let owned: Vec<WeightedEdge> = chosen.into_iter().cloned().collect();
    edge_items(&owned, |_| true, &agents, &channels)
}

/// Validates the shard's arguments and loads the drawer.
pub async fn load(
    cx: &Cx,
    caller: &Caller,
    state: &str,
    sel: &str,
    cursor: &str,
) -> Result<(ViewState, Drawer), UiError> {
    require(caller, Permission::View)?;
    let state = state_from_query(cx, state).await?;
    let selection = Selection::parse(sel).map_err(|e| invalid("sel", e))?;
    let cursor = parse_cursor(Some(cursor).filter(|c| !c.is_empty()))?;
    let backend = backend(cx);
    let view = backend
        .topology(
            caller,
            state.scope.window,
            state.weighting,
            &state.scope.topology_filter(),
        )
        .await?
        .value;
    let drawer = match selection {
        Selection::None => Drawer::Empty {
            heaviest: listed(cx, caller, &view, |_| true).await,
        },
        Selection::Edge { from, to, route } => {
            let page = PageRequest {
                after: cursor.clone(),
                size: crate::pages::common::paging::size(DRAWER_PAGE.items()),
            };
            let selector = EdgeSelector::new(from, to, route.clone())
                .map_err(|_| invalid("sel", "an edge joins two different agents"))?;
            let listed = backend
                .edge_transmissions(
                    caller,
                    &selector,
                    state.scope.window,
                    &state.scope.topology_filter(),
                    &page,
                )
                .await?
                .value
                .page;
            let names = agent_names(cx, caller, vec![from, to]).await;
            let channels = channel_names(cx, caller, route_channel(&route)).await;
            let stats = view
                .edges
                .iter()
                .find(|e| e.from == from && e.to == to && e.route == route)
                .map(|e| EdgeStatsView {
                    transmissions: e.stats.transmissions.get(),
                    matched: format_bytes(e.stats.matched_bytes.get()),
                    share: format_share(e.share.get()),
                    of: weighting_noun(state.weighting),
                });
            Drawer::Edge(EdgePanel {
                from: Named {
                    url: agent_url(from, &state),
                    name: names.name(from),
                },
                to: Named {
                    url: agent_url(to, &state),
                    name: names.name(to),
                },
                route_kind: RouteKind::from(&route),
                route: route_text(&route, &channels),
                route_url: route_channel(&route).map(|c| channel_url(c, &state)),
                stats,
                rows: listed
                    .items()
                    .iter()
                    .map(|row| EdgeRow::new(row, &state))
                    .collect(),
                next: listed.next().map(|c| c.token().to_owned()),
                paged: cursor.is_some(),
                focus_url: focus_url(&state, vec![from, to], Vec::new(), sel),
            })
        }
        Selection::Agent(id) => match backend.agent(caller, id).await? {
            None => Drawer::Missing("No agent has this id."),
            Some(detail) => {
                let summary = detail.summary;
                let canonical = summary.id;
                let parent = match summary.parent {
                    Some(parent) => {
                        let names = agent_names(cx, caller, [parent]).await;
                        Some(Named {
                            url: agent_url(parent, &state),
                            name: names.name(parent),
                        })
                    }
                    None => None,
                };
                Drawer::Agent(AgentPanel {
                    name: agent_name(&summary),
                    url: agent_url(canonical, &state),
                    state: summary.state,
                    claims: summary.claims.into_iter().map(|c| c.claim).collect(),
                    parent,
                    transmissions_in: summary.transmissions_in,
                    transmissions_out: summary.transmissions_out,
                    last_seen: format_time(summary.last_seen),
                    edges: listed(cx, caller, &view, |e| {
                        e.from == canonical || e.to == canonical
                    })
                    .await,
                    focus_url: focus_url(&state, vec![canonical], Vec::new(), sel),
                })
            }
        },
        Selection::Channel(id) => match backend
            .channel(caller, id, Some(state.scope.window))
            .await?
        {
            None => Drawer::Missing("No channel has this id."),
            Some(row) => {
                let row = row.value;
                let channel = row.channel();
                let canonical = channel.id;
                Drawer::Channel(ChannelPanel {
                    name: summary_name(&row),
                    url: channel_url(canonical, &state),
                    origin: origin_kind(&channel.origin),
                    detection: channel.origin.detection_kind(),
                    policy: channel.policy.kind(),
                    activity: Activity::of(&row),
                    edges: listed(cx, caller, &view, |e| e.route == Route::Channel(canonical))
                        .await,
                    focus_url: focus_url(&state, Vec::new(), vec![canonical], sel),
                })
            }
        },
    };
    Ok((state, drawer))
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use crosstalk_spec::aggregates::edge::EdgeStats;
    use crosstalk_spec::derived::flow::transmission::DelegationDirection;
    use crosstalk_spec::support::Share;

    use super::*;

    fn edge(from: u128, to: u128, share: f64, route: Route) -> WeightedEdge {
        WeightedEdge {
            from: AgentId::from_ulid(from),
            to: AgentId::from_ulid(to),
            route,
            stats: EdgeStats {
                transmissions: NonZeroU64::new(3).expect("n"),
                matched_bytes: NonZeroU64::new(2048).expect("n"),
            },
            share: Share::new(share).expect("share"),
        }
    }

    #[test]
    fn edge_items_are_heaviest_first_and_selectable() {
        let edges = vec![
            edge(1, 2, 0.1, Route::Unobserved),
            edge(2, 3, 0.6, Route::Channel(ChannelId::from_ulid(9))),
            edge(
                1,
                3,
                0.3,
                Route::Delegation(DelegationDirection::ParentToChild),
            ),
        ];
        let agents = AgentNames::from_pairs([(AgentId::from_ulid(2), "pi-scraper".to_owned())]);
        let channels =
            ChannelNames::from_pairs([(ChannelId::from_ulid(9), "wiki.example.org".to_owned())]);
        let items = edge_items(
            &edges,
            |e| e.from != AgentId::from_ulid(9),
            &agents,
            &channels,
        );
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].from, "pi-scraper");
        assert_eq!(items[0].route, "wiki.example.org");
        assert_eq!(items[0].share, "60.0%");
        assert_eq!(
            Selection::parse(&items[0].code),
            Ok(Selection::edge(
                AgentId::from_ulid(2),
                AgentId::from_ulid(3),
                &Route::Channel(ChannelId::from_ulid(9))
            ))
        );
        assert_eq!(items[2].route_kind, RouteKind::Unobserved);
        let only = edge_items(
            &edges,
            |e| e.from == AgentId::from_ulid(1),
            &agents,
            &channels,
        );
        assert_eq!(only.len(), 2);
    }

    #[test]
    fn focus_links_narrow_the_filter_and_keep_the_selection() {
        let state = crate::components::href::tests::state();
        let url = focus_url(&state, vec![AgentId::from_ulid(1)], Vec::new(), "agent:X");
        assert!(url.starts_with("/topology?"));
        assert!(url.contains("&a=00000000000000000000000001"));
        assert!(url.ends_with("&sel=agent:X"));
    }
}
