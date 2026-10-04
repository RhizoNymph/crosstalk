//! `/topology`: the communication graph with its filter, time brush, agent
//! and channel lists and selection drawer.
//!
//! The graph (`<ct-topology>`) and the time brush (`<ct-timebrush>`) load
//! their payloads from `/data/`. The selection lives in three places kept
//! in step: the `sel` signal (bound to the graph's `data-highlight`, read
//! by the drawer shard and the lists' `aria-pressed`), the graph's own
//! `value`, and the URL's `sel` key (rewritten with
//! `history.replaceState`). The graph and the lists both set the signal
//! ([`lists`]), so selecting from either is the same. A page load starts the
//! signal from the URL, so a selected edge is citeable. The time brush
//! navigates to the same view with the brushed window: its payload's bucket
//! edges are all bucket boundaries, so a brushed window is aligned and the
//! URL it navigates to is canonical.

pub mod drawer;
pub mod filters;
pub mod lists;
pub mod query;
pub mod selection;

use crate::contract::present::Present;
use std::time::Duration;

use crosstalk_spec::aggregates::edge::{TopologyGraph, Weighting};
use crosstalk_spec::aggregates::node::GraphNode;
use crosstalk_spec::aggregates::series::BucketWidth;
use crosstalk_spec::aggregates::watermark::Watermarked;
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission};
use crosstalk_spec::support::{TimeWindow, Timestamp};
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::error::see_other;
use topcoat::router::{page, parse_query_params, query_params};
use topcoat::runtime::{Event, signal};
use topcoat::view::{View, component, view};

use self::drawer::topology_drawer;
use self::filters::{FilterChoices, chips, clear_href, filter_chips, filter_form, load_choices};
use self::lists::model::{GraphLists, load as load_lists};
use self::lists::{ListTab, graph_lists, selection_sync};
use self::query::{RawTopologyQuery, TopologyQuery, submitted_filter};
use crate::app::{backend, caller};
use crate::components::{Tab, error_panel, format_time, href, segmented};
use crate::data::elements::{TIMEBRUSH_JS, TOPOLOGY_JS};
use crate::error::UiError;
use crate::pages::common::action::{require, status_of};
use crate::pages::common::form::{FormFields, invalid};
use crate::pages::view::view_state;
use crate::url::scope::{align_down, align_up};
use crate::url::view_state::{GraphMode, ViewState, format_time as rfc3339};
use crosstalk_spec::interfaces::l8_surface::QueryApi;

pub const PATH: &str = "/topology";

/// The time brush shows at least this much history before the data's end.
const CONTEXT: Duration = Duration::from_secs(7 * 24 * 3600);
const HOUR: u64 = 3_600_000_000;

/// The window the time brush draws: the last week of data, widened to
/// include the view's window and snapped outward to bucket boundaries, in
/// hourly buckets (at most 1000). The view's window is aligned, so the
/// brush's bucket edges include its ends.
pub fn brush_window(window: TimeWindow, now: Timestamp, bucket: BucketWidth) -> (TimeWindow, u32) {
    let span = u64::try_from(CONTEXT.as_micros()).unwrap_or(u64::MAX);
    let start = align_down(
        window
            .start()
            .min(Timestamp::from_micros(now.as_micros().saturating_sub(span))),
        bucket,
    );
    let end = align_up(window.end().max(now), bucket);
    let context = TimeWindow::new(start, end).unwrap_or(window);
    let micros = context.end().as_micros() - context.start().as_micros();
    let hours = micros.div_ceil(HOUR).clamp(1, 1000);
    (context, u32::try_from(hours).unwrap_or(1000))
}

/// The `/data/timeline` URL for the brush.
pub fn timeline_src(state: &ViewState, now: Timestamp, bucket: BucketWidth) -> String {
    let (window, buckets) = brush_window(state.scope.window, now, bucket);
    let mut context = state.clone();
    context.scope.window = window;
    href(
        "/data/timeline",
        &context,
        &[("buckets", &buckets.to_string())],
    )
}

fn page_pairs(query: &TopologyQuery) -> Vec<(&'static str, String)> {
    query.pairs()
}

fn borrowed<'a>(pairs: &'a [(&'static str, String)]) -> Vec<(&'static str, &'a str)> {
    pairs.iter().map(|(k, v)| (*k, v.as_str())).collect()
}

/// Headline numbers of the filtered graph.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Summary {
    agents: usize,
    edges: usize,
    transmissions: u64,
    watermark: String,
}

/// The agents-mode graph of the view: the header's numbers, and the lists'
/// agents in agents mode.
async fn agents_graph(
    cx: &Cx,
    caller: &Caller,
    state: &ViewState,
) -> std::result::Result<Watermarked<TopologyGraph>, UiError> {
    require(caller, Permission::View)?;
    Ok(backend(cx)
        .topology(
            caller,
            state.scope.window,
            state.weighting,
            &state.scope.topology_filter(),
        )
        .await?)
}

fn summary(graph: &Watermarked<TopologyGraph>) -> Summary {
    let value = &graph.value;
    Summary {
        agents: value
            .nodes()
            .iter()
            .filter(|n| matches!(n, GraphNode::Agent(_)))
            .count(),
        edges: value.edges().len(),
        transmissions: value.edges().iter().fold(0u64, |sum, e| {
            sum.saturating_add(e.stats.transmissions.get())
        }),
        watermark: format_time(graph.watermark.at()),
    }
}

#[page("/topology")]
async fn topology_get(cx: &Cx) -> Result<impl View> {
    let state = view_state(cx).await?;
    let form: FormFields = parse_query_params(cx).unwrap_or_default();
    let query = query_params::<RawTopologyQuery>(cx)
        .map_err(|e| invalid("query", e))
        .and_then(TopologyQuery::parse);
    let failure = match (submitted_filter(&form), &query) {
        (Ok(Some(filter)), Ok(query)) => {
            let mut next = state.clone();
            next.scope.filter = filter;
            let pairs = page_pairs(query);
            return Err(see_other(href(PATH, &next, &borrowed(&pairs))).into());
        }
        (Err(error), _) => Some(error),
        (Ok(_), Err(error)) => Some(error.clone()),
        (Ok(None), Ok(_)) => None,
    };
    let query = query.unwrap_or_default();
    Ok(view! { topology_page(state: state, query: query, failure: failure) })
}

#[component]
async fn topology_page(
    cx: &Cx,
    state: ViewState,
    query: TopologyQuery,
    failure: Option<UiError>,
) -> Result<impl View> {
    let caller = caller(cx);
    let graph = agents_graph(cx, &caller, &state).await;
    let lists = match &graph {
        Ok(graph) => load_lists(cx, &caller, &state, &graph.value).await,
        Err(error) => Err(error.clone()),
    };
    let loaded = graph.as_ref().map(summary).map_err(Clone::clone);
    let choices = match &loaded {
        Ok(_) => load_choices(cx, &caller, &state).await,
        Err(_) => FilterChoices::default(),
    };
    let pairs = page_pairs(&query);
    let extra = borrowed(&pairs);
    let chip_list = chips(PATH, &state, &extra, &choices);
    let clear = clear_href(PATH, &state, &extra);
    let failed_status = failure.as_ref().map(status_of);
    let headline = loaded.as_ref().ok().cloned();

    Ok(view! {
        <script type="module" src=(TOPOLOGY_JS)></script>
        <script type="module" src=(TIMEBRUSH_JS)></script>
        if let Some(status) = failed_status {
            (status)
        }
        header_bar(state: &state, query: &query, headline: headline)
        if let Some(error) = failure {
            <div class="mb-3">error_panel(error: &error)</div>
        }
        match loaded {
            Err(error) => {
                (status_of(&error))
                error_panel(error: &error)
            },
            Ok(_) => {
                <div class="mb-3 space-y-2">
                    filter_form(action: PATH.to_owned(), state: &state, extra: pairs.clone(), choices: choices)
                    filter_chips(chips: chip_list, clear: clear)
                </div>
                workspace(state: &state, query: &query, lists: lists)
            },
        }
    })
}

/// Title, window, headline numbers and the mode, weighting and collapse
/// toggles.
#[component]
async fn header_bar(
    state: &ViewState,
    query: &TopologyQuery,
    headline: Option<Summary>,
) -> Result<impl View> {
    let pairs = page_pairs(query);
    let extra = borrowed(&pairs);
    let mode = [
        (GraphMode::Agents, "Agents"),
        (GraphMode::Channels, "Channels"),
    ]
    .map(|(g, label)| Tab {
        label: label.to_owned(),
        href: href(PATH, &state.with_graph(g), &extra),
        active: state.graph == g,
    })
    .to_vec();
    let weighting = [
        (Weighting::Transmissions, "Transmissions"),
        (Weighting::MatchedBytes, "Matched bytes"),
    ]
    .map(|(w, label)| {
        let mut next = state.clone();
        next.weighting = w;
        Tab {
            label: label.to_owned(),
            href: href(PATH, &next, &extra),
            active: state.weighting == w,
        }
    })
    .to_vec();
    let collapse_pairs = page_pairs(&query.with_collapse(!query.collapse));
    let collapse_href = href(PATH, state, &borrowed(&collapse_pairs));
    let collapse = query.collapse;
    let window_text = format!(
        "{} → {}",
        format_time(state.scope.window.start()),
        format_time(state.scope.window.end())
    );
    let counts = headline.as_ref().map(|s| {
        format!(
            " · {} agents · {} edges · {} transmissions · ",
            s.agents, s.edges, s.transmissions
        )
    });
    let watermark = headline.map(|s| format!("final up to {}", s.watermark));
    Ok(view! {
        <header class="mb-3 flex flex-wrap items-end justify-between gap-3">
            <div>
                <h1 class="text-lg font-semibold">"Topology"</h1>
                <p class="text-xs text-zinc-500">
                    (window_text)
                    (counts.unwrap_or_default())
                    if let Some(watermark) = watermark {
                        <span title="Buckets before this time are final">(watermark)</span>
                    }
                </p>
            </div>
            <div class="flex flex-wrap items-center gap-2">
                segmented(label: "Graph mode", items: mode)
                segmented(label: "Weighting", items: weighting)
                <a
                    href=(collapse_href)
                    class=(if collapse { TOGGLE_ON } else { TOGGLE_OFF })
                    aria-pressed=(if collapse { "true" } else { "false" })
                >"Collapse sub-agents"</a>
            </div>
        </header>
    })
}

const TOGGLE_ON: &str = "rounded border border-sky-600 bg-sky-50 px-2 py-0.5 text-xs text-sky-800 dark:border-sky-500 dark:bg-sky-950 dark:text-sky-200";
const TOGGLE_OFF: &str = "rounded border border-zinc-300 px-2 py-0.5 text-xs text-zinc-600 hover:bg-zinc-100 dark:border-zinc-700 dark:text-zinc-400 dark:hover:bg-zinc-800";

/// The graph, the time brush, the agent and channel lists and the drawer,
/// sharing the selection signal. The graph's `data-highlight` shows the
/// list item under the pointer (`hover`) when there is one, else the
/// selection.
#[component]
async fn workspace(
    cx: &Cx,
    state: &ViewState,
    query: &TopologyQuery,
    lists: std::result::Result<GraphLists, UiError>,
) -> Result<impl View> {
    let collapse = query.collapse;
    let topology_src = href("/data/topology", state, &[]);
    let backend = backend(cx);
    let now = backend
        .now(&caller(cx))
        .await
        .unwrap_or(state.scope.window.end());
    let brush_src = timeline_src(state, now, backend.bucket_width());
    let brush_from = rfc3339(state.scope.window.start());
    let brush_to = rfc3339(state.scope.window.end());
    let state_query = state.to_query();
    let initial = query.sel.encode();
    let selected = initial.clone();
    let initial_tab = ListTab::for_selection(&query.sel).code();
    let sel = signal(cx, move || initial);
    let cursor = signal(cx, String::new);
    let hover = signal(cx, String::new);
    let tab = signal(cx, move || initial_tab.to_owned());
    let (drawer_sel, drawer_cursor) = (sel.clone(), cursor.clone());
    Ok(view! {
        <div class="grid grid-cols-1 items-start gap-4 lg:grid-cols-[minmax(0,1fr)_18rem] xl:grid-cols-[minmax(0,1fr)_24rem]">
            <div class="min-w-0 lg:col-start-1 lg:row-start-1 xl:row-span-2">
                <ct-topology
                    class="block h-[36rem] rounded border border-zinc-200 dark:border-zinc-800"
                    data-src=(topology_src)
                    data-collapse=(collapse.then_some("true"))
                    :data-highlight=$(if hover.get().is_empty() { sel.get() } else { hover.get() })
                    @change=$(|e: Event| {
                        let v = e.target.value;
                        sel.set(v.to_owned());
                        cursor.set("".to_owned());
                        hover.set("".to_owned());
                        let next_tab = if v.starts_with("channel:") { "channels".to_owned() } else if v.starts_with("agent:") { "agents".to_owned() } else { tab.get() };
                        tab.set(next_tab);
                    })
                ></ct-topology>
                <ct-timebrush
                    class="mt-2 block h-24 rounded border border-zinc-200 dark:border-zinc-800"
                    data-src=(brush_src)
                    data-from=(brush_from)
                    data-to=(brush_to)
                    @change=$(|e: Event| {
                        let _brushed = e.target.value;
                        raw!("((value) => { const v = String(value); const i = v.indexOf('/'); if (i < 0) return; const enc = (s) => encodeURIComponent(s).replace(/%3A/g, ':'); const kept = location.search.slice(1).split('&').filter((p) => { const k = p.split('=')[0]; return p !== '' && k !== 'from' && k !== 'to'; }); location.assign(location.pathname + '?from=' + enc(v.slice(0, i)) + '&to=' + enc(v.slice(i + 1)) + (kept.length > 0 ? '&' + kept.join('&') : '')); })(${_brushed})");
                    })
                ></ct-timebrush>
                <p class="mt-1 text-[11px] text-zinc-500">"Drag on the time brush to choose a window; click an edge or node, or an agent or channel in the lists, to inspect it."</p>
            </div>
            <div class="min-w-0 lg:col-start-2 lg:row-start-1">
                match lists {
                    Ok(lists) => graph_lists(lists: lists, selected: selected, sel: &sel, cursor: &cursor, hover: &hover, tab: &tab),
                    Err(error) => error_panel(error: &error),
                }
            </div>
            selection_sync(sel: &sel)
            <aside class="min-w-0 lg:col-span-2 xl:col-span-1 xl:col-start-2 xl:row-start-2">
                topology_drawer(state: state_query, sel: drawer_sel, cursor: drawer_cursor)
            </aside>
        </div>
    })
}

#[cfg(test)]
pub mod tests;
