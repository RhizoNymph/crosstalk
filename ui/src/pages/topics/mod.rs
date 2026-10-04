//! `/topics`: the topic model's versions (`topic_versions`), the topics of
//! one version (`topics`) with their terms, size over the view's window
//! (`topic_sizes`) and trend (`series` grouped by topic), and the lineage
//! to the next version (`topic_lineage`): which topics `TopicLineage::remap`
//! carries over at the rule form's default threshold, and which
//! watched-topic rules it leaves stale.
//!
//! `ver` picks the version shown (default: the view's `v`); a link sets the
//! view's `v` to it so every page reads that version. A version retention
//! dropped keeps its topics and lineage but has no sizes over a window: its
//! table shows the typed error (409). Topic labels and terms come from
//! message text, so the page needs `Content`; without it the page says so.

pub mod model;

use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::aggregates::topic::{Topic, TopicModelVersion};
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission, QueryError};
use crosstalk_spec::support::Similarity;
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::{page, query_params};
use topcoat::view::{View, component, view};

use self::model::{RemapRow, TopicRow, VersionTab, remap_rows, topic_rows, version_tabs};
use crate::app::{backend, caller, can};
use crate::backend::Backend;
use crate::components::form::{FACET, LINK, PANEL, SECTION, SECTION_TITLE};
use crate::components::sparkline::sparkline;
use crate::components::table::{ROW, TD, TD_NUM};
use crate::components::{
    Tab, data_table, empty_state, error_panel, format_time, href, page_header, segmented,
};
use crate::error::UiError;
use crate::pages::alerts::rules::form::DEFAULT_REMAP;
use crate::pages::common::action::{require, status_of};
use crate::pages::common::form::invalid;
use crate::pages::common::rules::all_rules;
use crate::pages::common::topics::{Trends, all_topics, topic_trends};
use crate::pages::common::transmissions::Named;
use crate::pages::explore::topics::watch_url;
use crate::pages::view::view_state;
use crate::url::view_state::ViewState;

pub const PATH: &str = "/topics";

#[query_params]
struct RawTopicsQuery {
    ver: Option<String>,
}

/// The `ver` key: a version number.
pub fn parse_version(
    text: Option<&str>,
) -> std::result::Result<Option<TopicModelVersion>, UiError> {
    text.map(|t| {
        t.parse::<u32>()
            .map(TopicModelVersion)
            .map_err(|_| invalid("ver", "not a topic model version"))
    })
    .transpose()
}

/// The selected version's topics in the window.
struct Table {
    rows: Vec<TopicRow>,
    /// "final up to …" for the sizes.
    watermark: String,
}

struct Loaded {
    tabs: Vec<VersionTab>,
    selected: TopicModelVersion,
    /// The selected version's topics; an error when the version is
    /// unknown, or dropped (no sizes over a window).
    topics: std::result::Result<Table, UiError>,
    /// The lineage to the next version: its number and rows.
    remap: Option<(u32, Vec<RemapRow>)>,
}

/// The remap threshold the table maps at: the rule form's default.
fn default_threshold() -> std::result::Result<Similarity, UiError> {
    DEFAULT_REMAP
        .parse::<f32>()
        .ok()
        .and_then(|value| Similarity::new(value).ok())
        .ok_or_else(|| {
            invalid(
                "remap_threshold",
                "the default threshold is not a similarity",
            )
        })
}

/// The rows of the selected version's `topics`: sizes over the window,
/// then trends (a version never activated has sizes but no series; it
/// shows no trend).
async fn table(
    cx: &Cx,
    caller: &Caller,
    state: &ViewState,
    (selected, topics): (TopicModelVersion, &[Topic]),
    watched_version: TopicModelVersion,
) -> std::result::Result<Table, UiError> {
    let backend = backend(cx);
    let sizes = backend
        .topic_sizes(caller, Some(selected), Some(state.scope.window))
        .await?;
    let trends = match topic_trends(backend, caller, state.scope.window, selected).await {
        Ok(trends) => trends,
        Err(error) => {
            tracing::warn!(error = %error, version = selected.0, "topic trends unavailable");
            Trends::default()
        }
    };
    let rows = topic_rows(topics, &sizes.value, &trends, |id| {
        (selected == watched_version).then(|| watch_url(id, state))
    });
    Ok(Table {
        rows,
        watermark: format!("final up to {}", format_time(sizes.watermark.at())),
    })
}

async fn load(
    cx: &Cx,
    caller: &Caller,
    state: &ViewState,
    selected: TopicModelVersion,
) -> std::result::Result<Loaded, UiError> {
    let backend = backend(cx);
    let history = backend.topic_versions(caller).await?;
    let tabs = version_tabs(&history, state.scope.topic_version);
    // New watched-topic rules name the version the rule form picks topics
    // from: the active one.
    let active = history.active().version();
    let topics: std::result::Result<Vec<Topic>, UiError> = if history.get(selected).is_some() {
        all_topics(backend, caller, TopicVersionSelector::Pinned(selected))
            .await
            .map(|(_, topics)| topics)
            .map_err(UiError::from)
    } else {
        Err(UiError::Query(QueryError::NotFound))
    };
    let table = match &topics {
        Ok(topics) => table(cx, caller, state, (selected, topics), active).await,
        Err(error) => Err(error.clone()),
    };
    let remap = match backend.topic_lineage(caller, selected).await {
        Ok(Some(lineage)) => {
            let from = topics?;
            let (_, next) =
                all_topics(backend, caller, TopicVersionSelector::Pinned(lineage.to())).await?;
            let rules = all_rules(backend, caller).await?;
            let rows = remap_rows(&lineage, &from, &next, &rules, default_threshold()?, state);
            Some((lineage.to().0, rows))
        }
        Ok(None) | Err(QueryError::NotFound) => None,
        Err(error) => return Err(error.into()),
    };
    Ok(Loaded {
        tabs,
        selected,
        topics: table,
        remap,
    })
}

#[page("/topics")]
async fn topics_get(cx: &Cx) -> Result<impl View> {
    let state = view_state(cx).await?;
    let selected = query_params::<RawTopicsQuery>(cx)
        .map_err(|e| invalid("query", e))
        .and_then(|q| parse_version(q.ver.as_deref()))
        .map(|v| v.unwrap_or(state.scope.topic_version));
    Ok(view! { topics_page(state: state, selected: selected) })
}

#[component]
async fn topics_page(
    cx: &Cx,
    state: ViewState,
    selected: std::result::Result<TopicModelVersion, UiError>,
) -> Result<impl View> {
    let caller = caller(cx);
    let allowed = require(&caller, Permission::View).and(selected);
    let content = can(&caller, Permission::Content);
    Ok(view! {
        page_header(title: "Topics", subtitle: "What transmissions are about, per topic-model version, and how topics carried over after a re-fit.")
        match allowed {
            Err(error) => {
                (status_of(&error))
                error_panel(error: &error)
            },
            Ok(_) if !content => {
                <div class=(PANEL)>
                    <p class="text-sm">"Topics need the Content permission: their labels and terms come from message text."</p>
                </div>
            },
            Ok(selected) => topics_body(state: state, selected: selected),
        }
    })
}

/// The version picker and the link that makes the shown version the
/// view's.
struct Picker {
    tabs: Vec<Tab>,
    detail: String,
    /// `None` when the shown version already is the view's.
    use_url: Option<String>,
}

fn picker(state: &ViewState, tabs: &[VersionTab], selected: TopicModelVersion) -> Picker {
    let detail = tabs
        .iter()
        .find(|t| t.version == selected.0)
        .map(|t| t.detail.clone())
        .unwrap_or_default();
    let mut use_state = state.clone();
    use_state.scope.topic_version = selected;
    Picker {
        tabs: tabs
            .iter()
            .map(|t| Tab {
                label: format!(
                    "{}{}{}{}",
                    t.label,
                    if t.pinned { " · pinned" } else { "" },
                    if t.newest { " · newest" } else { "" },
                    if t.dropped { " · dropped" } else { "" }
                ),
                href: href(PATH, state, &[("ver", &t.version.to_string())]),
                active: t.version == selected.0,
            })
            .collect(),
        detail,
        use_url: (selected != state.scope.topic_version).then(|| href(PATH, &use_state, &[])),
    }
}

#[component]
async fn topics_body(cx: &Cx, state: ViewState, selected: TopicModelVersion) -> Result<impl View> {
    let caller = caller(cx);
    let loaded = load(cx, &caller, &state, selected).await;
    let picked = loaded
        .as_ref()
        .ok()
        .map(|l| picker(&state, &l.tabs, l.selected));
    let rules_url = href("/alerts/rules", &state, &[]);
    Ok(view! {
        if let Some(picked) = picked {
            <div class="mb-4 flex flex-wrap items-center gap-3">
                <span class=(FACET)>"Version"</span>
                segmented(label: "Topic model version", items: picked.tabs)
                <span class="text-xs text-zinc-500">(picked.detail)</span>
                match picked.use_url {
                    Some(url) => <a class=(format!("{LINK} text-xs")) href=(url)>"Use v" (selected.0) " in every view"</a>,
                    None => <span class="text-xs text-zinc-500">"· the version every view reads"</span>,
                }
            </div>
        }
        match loaded {
            Err(error) => {
                (status_of(&error))
                error_panel(error: &error)
            },
            Ok(loaded) => {
                topic_table(topics: loaded.topics)
                if let Some((to, rows)) = loaded.remap {
                    remap_table(from: selected.0, to: to, rows: rows, rules_url: rules_url)
                }
            },
        }
    })
}

#[component]
async fn topic_table(topics: std::result::Result<Table, UiError>) -> Result<impl View> {
    let empty = topics.as_ref().is_ok_and(|t| t.rows.is_empty());
    let watermark = topics
        .as_ref()
        .map(|t| t.watermark.clone())
        .unwrap_or_default();
    Ok(view! {
        <section class=(SECTION)>
            <h2 class=(SECTION_TITLE)>
                "Topics in the window"
                <span class="ml-2 font-normal normal-case tracking-normal">(watermark)</span>
            </h2>
            match topics {
                Err(error) => {
                    (status_of(&error))
                    error_panel(error: &error)
                },
                Ok(_) if empty => empty_state(message: "This version has no topics: it was never fitted to traffic."),
                Ok(Table { rows, .. }) => data_table(
                    headers: &["Topic", "Top terms", "Transmissions", "Trend", ""],
                    for row in rows {
                        <tr class=(ROW)>
                            <td class=(format!("{TD} font-medium"))>(row.label.clone())</td>
                            <td class=(TD)>
                                <div class="flex flex-wrap gap-1">
                                    for (term, weight) in row.terms {
                                        <span class="inline-flex items-baseline gap-1 rounded bg-zinc-100 px-1.5 py-0.5 font-mono text-[11px] dark:bg-zinc-800">
                                            (term) <span class="text-zinc-500">(weight)</span>
                                        </span>
                                    }
                                </div>
                            </td>
                            <td class=(TD_NUM)>(row.transmissions)</td>
                            <td class=(TD)>sparkline(values: row.trend, label: format!("{} over the window", row.label))</td>
                            <td class=(TD)>
                                if let Some(url) = row.watch {
                                    <a class=(format!("{LINK} text-xs")) href=(url)>"Watch"</a>
                                }
                            </td>
                        </tr>
                    }
                ),
            }
        </section>
    })
}

#[component]
async fn remap_table(
    from: u32,
    to: u32,
    rows: Vec<RemapRow>,
    rules_url: String,
) -> Result<impl View> {
    let unmapped = rows.iter().filter(|r| r.to.is_none()).count();
    let empty = rows.is_empty();
    Ok(view! {
        <section class=(SECTION)>
            <h2 class=(SECTION_TITLE)>
                "Remap v" (from) " → v" (to)
                <a class=(format!("{LINK} ml-2 normal-case tracking-normal font-normal")) href=(rules_url)>"alert rules"</a>
            </h2>
            if unmapped > 0 {
                <p class="mb-2 text-xs text-amber-800 dark:text-amber-200">
                    (unmapped) " of these topics found no match in v" (to) ". Watched-topic rules on them go stale until updated."
                </p>
            }
            if empty {
                empty_state(message: "No topics to carry over.")
            } else {
                data_table(
                    headers: &["Topic", "Maps to", "Similarity", "Stale rules"],
                    for row in rows {
                        <tr class=(if row.to.is_none() { "bg-amber-50 dark:bg-amber-950/40" } else { ROW })>
                            <td class=(TD)>(row.from)</td>
                            match row.to {
                                Some((label, similarity)) => {
                                    <td class=(TD)>(label)</td>
                                    <td class=(TD_NUM)>(similarity)</td>
                                },
                                None => {
                                    <td class=(TD)><span class="text-xs font-medium text-amber-800 dark:text-amber-200">"unmapped"</span></td>
                                    <td class=(TD_NUM)>"—"</td>
                                },
                            }
                            <td class=(TD)>
                                stale_rules(rules: row.stale_rules)
                            </td>
                        </tr>
                    }
                )
            }
        </section>
    })
}

#[component]
async fn stale_rules(rules: Vec<Named>) -> Result<impl View> {
    Ok(view! {
        <div class="flex flex-wrap gap-2 text-xs">
            for rule in rules {
                <a class=(LINK) href=(rule.url)>(rule.name)</a>
            }
        </div>
    })
}

#[cfg(test)]
mod tests {
    use topcoat::router::StatusCode;

    use super::*;
    use crate::pages::topology::tests::fixture_state;
    use crate::testing::get;

    fn url(extra: &str) -> String {
        format!("{PATH}?{}{extra}", fixture_state().to_query())
    }

    #[test]
    fn versions_parse_as_numbers() {
        assert_eq!(parse_version(None), Ok(None));
        assert_eq!(parse_version(Some("1")), Ok(Some(TopicModelVersion(1))));
        assert!(parse_version(Some("v1")).is_err());
    }

    #[tokio::test]
    async fn the_view_version_shows_its_topics_with_terms_and_watch_links() {
        assert_eq!(get(PATH).await.status, StatusCode::TEMPORARY_REDIRECT);
        let reply = get(&url("")).await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        assert!(reply.body.contains("v1 · pinned"));
        assert!(reply.body.contains("v2 · newest"));
        assert!(reply.body.contains("Credentials and API keys"));
        assert!(reply.body.contains(">Watch</a>"));
        assert!(reply.body.contains("<polyline"));
        assert!(reply.body.contains("final up to 2026-10-02 23:50:00 UTC"));
        assert!(
            !reply.body.contains("Remap v2"),
            "the newest version has no successor"
        );
    }

    #[tokio::test]
    async fn an_older_version_shows_its_remap_and_stale_rules() {
        let reply = get(&url("&ver=1")).await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        assert!(reply.body.contains("Remap v1 → v2"));
        assert!(reply.body.contains("Engineering chatter"));
        assert!(reply.body.contains("unmapped"));
        assert!(
            reply.body.contains("href=\"/alerts/rules/"),
            "the stale rule is linked"
        );
        assert!(reply.body.contains("Use v1 in every view"));
        assert!(
            !reply.body.contains(">Watch</a>"),
            "new rules target the newest version"
        );
    }

    #[tokio::test]
    async fn dropped_and_unknown_versions() {
        // v0 (unfitted) was dropped by retention when v2 was activated: no
        // sizes over a window, but its lineage to v1 is still listed.
        let reply = get(&url("&ver=0")).await;
        assert_eq!(reply.status, StatusCode::CONFLICT, "{}", reply.body);
        assert!(reply.body.contains("v0 · dropped"));
        assert!(reply.body.contains("no longer retained"));
        assert!(reply.body.contains("Remap v0 → v1"));
        assert!(reply.body.contains("No topics to carry over."));
        let reply = get(&url("&ver=9")).await;
        assert_eq!(reply.status, StatusCode::NOT_FOUND, "{}", reply.body);
        let reply = get(&url("&ver=x")).await;
        assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
    }
}
