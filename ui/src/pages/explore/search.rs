//! Search: the form and the paged hits, each with its score, snippet,
//! sender → reader and route, linking to the evidence page.

use std::collections::HashMap;

use crosstalk_spec::ids::TransmissionId;
use crosstalk_spec::interfaces::l8_surface::Caller;
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::view::{View, component, view};

use super::query::{ExploreQuery, MODES, mode_code, mode_label};
use crate::app::backend;
use crate::backend::Backend;
use crate::components::form::{BUTTON_PRIMARY, INPUT};
use crate::components::{
    PageLinks, empty_state, error_panel, pagination, route_badge, state_inputs,
};
use crate::contract::errors::QueryError;
use crate::contract::graph::TransmissionSelector;
use crate::contract::lists::{Cursor, PageRequest};
use crate::contract::search::SearchRequest;
use crate::pages::common::links::transmission_url;
use crate::pages::common::transmissions::{TransmissionRow, rows};
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;

#[derive(Debug, Clone, PartialEq)]
pub struct HitRow {
    pub id: TransmissionId,
    pub url: String,
    pub score: f32,
    pub snippet: String,
    /// Sender, reader and route, when the hit is in the view's scope.
    pub row: Option<TransmissionRow>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Hits {
    pub rows: Vec<HitRow>,
    pub next: Option<Cursor>,
    pub current: Option<Cursor>,
}

impl Hits {
    /// The hits' ids as the projection's `data-highlight`.
    pub fn highlight(&self) -> String {
        self.rows
            .iter()
            .map(|h| h.id.to_ulid())
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// The page of hits for the query's text, or `None` without a text.
pub async fn load_hits(
    cx: &Cx,
    caller: &Caller,
    query: &ExploreQuery,
    state: &ViewState,
    page: PageRequest,
) -> std::result::Result<Option<Hits>, QueryError> {
    let Some(text) = &query.text else {
        return Ok(None);
    };
    let request = SearchRequest {
        text: text.clone(),
        mode: query.mode,
    };
    let backend = backend(cx);
    let found = backend
        .search(caller, &request, &state.scope, &page)
        .await?;
    let ids: Vec<TransmissionId> = found.items.iter().map(|h| h.transmission).collect();
    let summaries = if ids.is_empty() {
        Vec::new()
    } else {
        backend
            .transmissions(
                caller,
                &state.scope,
                &TransmissionSelector::Ids(ids.clone()),
                &PageRequest::first(page.limit),
            )
            .await?
            .items
    };
    let mut by_id: HashMap<TransmissionId, TransmissionRow> = rows(cx, caller, &summaries, state)
        .await
        .into_iter()
        .map(|r| (r.id, r))
        .collect();
    Ok(Some(Hits {
        rows: found
            .items
            .into_iter()
            .map(|hit| HitRow {
                id: hit.transmission,
                url: transmission_url(hit.transmission, state),
                score: hit.score.get(),
                snippet: hit.snippet,
                row: by_id.remove(&hit.transmission),
            })
            .collect(),
        next: found.next,
        current: page.cursor,
    }))
}

/// The search form: text and mode, keeping the view state and the page's
/// projection and colouring.
#[component]
pub async fn search_form(state: &ViewState, query: &ExploreQuery) -> Result<impl View> {
    let text = query
        .text
        .as_ref()
        .map(|t| t.as_str().to_owned())
        .unwrap_or_default();
    let keep: Vec<(&str, String)> = query
        .pairs()
        .into_iter()
        .filter(|(k, v)| (*k == "p" || *k == "cb") && !v.is_empty())
        .collect();
    let modes: Vec<(&str, &str, bool)> = MODES
        .iter()
        .map(|m| (mode_code(*m), mode_label(*m), *m == query.mode))
        .collect();
    Ok(view! {
        <form method="get" action="/explore" class="flex gap-1.5" role="search">
            state_inputs(state: state)
            for (name, value) in keep {
                <input type="hidden" name=(name) value=(value)>
            }
            <input
                type="search"
                name="q"
                value=(text)
                maxlength="500"
                placeholder="Search transmitted text"
                class=(format!("{INPUT} min-w-0 flex-1"))
                aria-label="Search text"
            >
            <select name="m" class=(INPUT) aria-label="Search mode">
                for (code, label, chosen) in modes {
                    <option value=(code) selected=(chosen)>(label)</option>
                }
            </select>
            <button type="submit" class=(BUTTON_PRIMARY)>"Search"</button>
        </form>
    })
}

fn score_width(score: f32) -> String {
    format!("width: {:.0}%", (score.clamp(0.0, 1.0) * 100.0).round())
}

/// The hits of one page, best first.
#[component]
pub async fn hit_list(
    hits: Option<std::result::Result<Hits, QueryError>>,
    links: PageLinks,
) -> Result<impl View> {
    let none = matches!(&hits, Some(Ok(h)) if h.rows.is_empty());
    Ok(view! {
        match hits {
            None => <p class="mt-3 text-xs text-zinc-500">"Search the text that travelled between agents. Hits light up in the projection."</p>,
            Some(Err(error)) => <div class="mt-3">error_panel(error: &error)</div>,
            Some(Ok(_)) if none => <div class="mt-3">empty_state(message: "No transmission in the view matches this search.")</div>,
            Some(Ok(hits)) => {
                <ol class="mt-3 max-h-[calc(100vh-14rem)] divide-y overflow-y-auto divide-zinc-100 rounded border border-zinc-200 dark:divide-zinc-800 dark:border-zinc-800" data-hits=(hits.rows.len())>
                    for hit in hits.rows {
                        <li class="px-2.5 py-2 text-xs">
                            <div class="mb-1 flex items-center gap-2">
                                <span class="h-1.5 w-14 shrink-0 overflow-hidden rounded-full bg-zinc-200 dark:bg-zinc-800" title="score">
                                    <span class="block h-full bg-sky-600 dark:bg-sky-400" style=(score_width(hit.score))></span>
                                </span>
                                <span class="tabular-nums text-zinc-500">(format!("{:.2}", hit.score))</span>
                                match hit.row {
                                    Some(row) => {
                                        route_badge(kind: row.route_kind)
                                        <span class="min-w-0 truncate text-zinc-600 dark:text-zinc-400">
                                            (row.from.map(|f| f.name).unwrap_or_else(|| "unknown".to_owned()))
                                            " → " (row.to.name)
                                        </span>
                                        <span class="ml-auto shrink-0 text-zinc-500">(row.opened)</span>
                                    },
                                    None => "",
                                }
                            </div>
                            <a class="block leading-snug text-zinc-800 hover:text-sky-700 dark:text-zinc-200 dark:hover:text-sky-300" href=(hit.url)>(hit.snippet)</a>
                        </li>
                    }
                </ol>
                pagination(links: links)
            },
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn highlight_lists_hit_ids_in_order() {
        let hit = |n| HitRow {
            id: TransmissionId::from_ulid(n),
            url: String::new(),
            score: 0.5,
            snippet: String::new(),
            row: None,
        };
        let hits = Hits {
            rows: vec![hit(2), hit(1)],
            next: None,
            current: None,
        };
        assert_eq!(
            hits.highlight(),
            "00000000000000000000000002,00000000000000000000000001"
        );
        assert_eq!(score_width(0.834), "width: 83%");
        assert_eq!(score_width(1.5), "width: 100%");
    }
}
