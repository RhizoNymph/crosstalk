//! `/`: the landing page. Counts for the view's window (transmissions,
//! active channels, open alerts, the review queue), the heaviest edges
//! (each opening the topology with that edge selected), the newest open
//! alerts, and a link into every section carrying the view state.

pub mod model;

use crosstalk_spec::interfaces::l8_surface::AlertStateKind;
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::page;
use topcoat::view::{View, component, view};

use self::model::{Tile, load};
use crate::app::caller;
use crate::components::form::{LINK, SECTION, SECTION_TITLE};
use crate::components::table::{ROW, TD, TD_MUTED, TD_NUM};
use crate::components::{
    data_table, empty_state, error_panel, format_time, href, kind_badge, route_badge,
};
use crate::contract::errors::QueryError;
use crate::pages::alerts::model::AlertRow;
use crate::pages::common::action::status_of;
use crate::pages::topology::drawer::model::EdgeItem;
use crate::pages::view::view_state;
use crate::url::view_state::ViewState;

/// The sections the landing page links into, with what each is for.
const SECTIONS: [(&str, &str, &str); 9] = [
    ("/topology", "Topology", "Who talks to whom, through what"),
    (
        "/explore",
        "Explore",
        "Search and the projection of transmitted text",
    ),
    (
        "/topics",
        "Topics",
        "What transmissions are about, per model version",
    ),
    ("/channels", "Channels", "Shared resources and their policy"),
    ("/agents", "Agents", "Identities, sub-agents and merges"),
    ("/alerts", "Alerts", "The inbox and the rules behind it"),
    (
        "/export",
        "Export",
        "Research datasets and detection quality",
    ),
    ("/audit", "Audit", "Every operator and config action"),
    ("/pipeline", "Pipeline", "Dead letters and replay"),
];

#[page("/")]
async fn overview_get(cx: &Cx) -> Result<impl View> {
    let state = view_state(cx).await?;
    Ok(view! { overview_page(state: state) })
}

#[component]
async fn overview_page(cx: &Cx, state: ViewState) -> Result<impl View> {
    let caller = caller(cx);
    let loaded = load(cx, &caller, &state).await;
    let final_up_to = loaded
        .as_ref()
        .ok()
        .and_then(|o| o.watermark.clone())
        .map(|w| format!(" · final up to {w}"))
        .unwrap_or_default();
    let window = format!(
        "{} → {}",
        format_time(state.scope.window.start()),
        format_time(state.scope.window.end())
    );
    let sections: Vec<(String, &str, &str)> = SECTIONS
        .iter()
        .map(|(path, label, what)| (href(path, &state, &[]), *label, *what))
        .collect();
    let alerts_url = href("/alerts", &state, &[]);
    let topology_url = href("/topology", &state, &[]);
    Ok(view! {
        <header class="mb-4">
            <h1 class="text-lg font-semibold">"Overview"</h1>
            <p class="text-sm text-zinc-500">
                "Agent-to-agent communication seen by the gateway, " (window)
                (final_up_to)
                "."
            </p>
        </header>
        match loaded {
            Err(error) => {
                (status_of(&error))
                error_panel(error: &error)
            },
            Ok(summary) => {
                <div class="mb-6 grid grid-cols-2 gap-3 lg:grid-cols-4">
                    for tile in summary.tiles {
                        stat_tile(tile: tile)
                    }
                </div>
                <div class="grid gap-6 lg:grid-cols-2">
                    heaviest_edges(edges: summary.edges, topology_url: topology_url)
                    newest_alerts(alerts: summary.alerts, inbox_url: alerts_url)
                </div>
            },
        }
        <section class=(SECTION)>
            <h2 class=(SECTION_TITLE)>"Sections"</h2>
            <ul class="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
                for (url, label, what) in sections {
                    <li>
                        <a href=(url) class="block rounded border border-zinc-200 px-3 py-2 hover:border-sky-500 hover:bg-zinc-50 dark:border-zinc-800 dark:hover:border-sky-600 dark:hover:bg-zinc-900">
                            <span class="block text-sm font-medium">(label)</span>
                            <span class="block text-xs text-zinc-500">(what)</span>
                        </a>
                    </li>
                }
            </ul>
        </section>
    })
}

#[component]
async fn stat_tile(tile: Tile) -> Result<impl View> {
    Ok(view! {
        <a href=(tile.href) class="block rounded border border-zinc-200 px-3 py-2.5 hover:border-sky-500 dark:border-zinc-800 dark:hover:border-sky-600">
            <span class="block text-[11px] font-semibold uppercase tracking-wide text-zinc-500">(tile.label)</span>
            match tile.value {
                Ok(value) => <span class="block text-2xl font-semibold tabular-nums">(value)</span>,
                Err(error) => <span class="block text-xs text-red-700 dark:text-red-300" title=(error.to_string())>"unavailable"</span>,
            }
            <span class="block text-xs text-zinc-500">(tile.detail)</span>
        </a>
    })
}

#[component]
async fn heaviest_edges(
    edges: std::result::Result<Vec<(EdgeItem, String)>, QueryError>,
    topology_url: String,
) -> Result<impl View> {
    let empty = edges.as_ref().is_ok_and(Vec::is_empty);
    Ok(view! {
        <section class=(SECTION)>
            <h2 class=(SECTION_TITLE)>
                "Heaviest edges"
                <a class=(format!("{LINK} ml-2 normal-case tracking-normal font-normal")) href=(topology_url)>"open the topology"</a>
            </h2>
            match edges {
                Err(error) => error_panel(error: &error),
                Ok(_) if empty => empty_state(message: "No confirmed transmissions in this window."),
                Ok(edges) => data_table(
                    headers: &["Sender → reader", "Route", "Transmissions", "Share"],
                    for (edge, url) in edges {
                        <tr class=(ROW)>
                            <td class=(TD)>
                                <a class=(LINK) href=(url)>(edge.from) <span class="text-zinc-400">" → "</span> (edge.to)</a>
                            </td>
                            <td class=(TD)>
                                <div class="flex min-w-0 max-w-56 items-center gap-1.5">
                                    route_badge(kind: edge.route_kind)
                                    <span class="truncate text-xs text-zinc-500">(edge.route)</span>
                                </div>
                            </td>
                            <td class=(TD_NUM)>(edge.transmissions)</td>
                            <td class=(TD_NUM)>(edge.share)</td>
                        </tr>
                    }
                ),
            }
        </section>
    })
}

#[component]
async fn newest_alerts(
    alerts: std::result::Result<Vec<AlertRow>, QueryError>,
    inbox_url: String,
) -> Result<impl View> {
    let empty = alerts.as_ref().is_ok_and(Vec::is_empty);
    Ok(view! {
        <section class=(SECTION)>
            <h2 class=(SECTION_TITLE)>
                "Newest open alerts"
                <a class=(format!("{LINK} ml-2 normal-case tracking-normal font-normal")) href=(inbox_url)>"open the inbox"</a>
            </h2>
            match alerts {
                Err(error) => error_panel(error: &error),
                Ok(_) if empty => empty_state(message: "No open alerts."),
                Ok(rows) => data_table(
                    headers: &["Rule", "Subject", "Occurrences", "Raised"],
                    for row in rows {
                        <tr class=(ROW)>
                            <td class=(TD)>
                                <span class="mr-1.5">kind_badge(value: AlertStateKind::Open)</span>
                                (row.rule)
                            </td>
                            <td class=(TD)><a class=(LINK) href=(row.subject_url)>(row.subject_label)</a></td>
                            <td class=(TD_NUM)>(row.occurrences)</td>
                            <td class=(TD_MUTED)>(row.raised)</td>
                        </tr>
                    }
                ),
            }
        </section>
    })
}

#[cfg(test)]
mod tests {
    use topcoat::router::StatusCode;

    use crate::pages::topology::tests::fixture_state;
    use crate::testing::get;

    #[tokio::test]
    async fn the_landing_page_summarises_the_window() {
        let reply = get("/").await;
        assert_eq!(reply.status, StatusCode::TEMPORARY_REDIRECT);
        let reply = get(&format!("/?{}", fixture_state().to_query())).await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        let body = &reply.body;
        for label in [
            "Transmissions",
            "Active channels",
            "Open alerts",
            "Review queue",
        ] {
            assert!(body.contains(label), "{label}");
        }
        assert!(!body.contains("unavailable"), "{body}");
        assert!(body.contains("Heaviest edges"));
        assert!(
            body.contains("&amp;sel=edge:"),
            "edges open the topology selected"
        );
        assert!(body.contains("Newest open alerts"));
        assert!(body.contains("href=\"/channels?from=2026-10-02T00:00:00Z"));
        assert!(body.contains("tab=review"));
        assert!(body.contains("final up to 2026-10-02 23:50:00 UTC"));
    }
}
