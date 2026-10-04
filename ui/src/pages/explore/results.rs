//! The projection's selection, resolved on the server: a shard that
//! re-renders when the selection signal (or its page) changes.
//!
//! A point shows that transmission; a lasso is resolved against the stored
//! projection ([`Polygon::transmissions`]) and lists the transmissions
//! inside it, paged, from `transmissions_by_id`. Rows are read under the
//! projection's own topic version, since that is what its points were
//! sampled under. The shard checks `View` and `Content` and validates every
//! argument itself.

use std::num::NonZeroU32;

use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission};
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::runtime::{Signal, shard};
use topcoat::view::{View, view};

use super::lasso::ProjectionSelection;
use crate::app::{backend, caller};
use crate::components::form::{LINK, SMALL_BUTTON};
use crate::components::{empty_state, error_panel, kind_badge, route_badge};
use crate::error::UiError;
use crate::pages::common::action::require;
use crate::pages::common::form::invalid;
use crate::pages::common::paging::{Count, parse_cursor};
use crate::pages::common::transmissions::{
    TransmissionRow, rows, summaries_by_id, transmission_table,
};
use crate::pages::view::state_from_query;
use crate::url::ulid::UlidId;
use crosstalk_spec::ids::ProjectionId;
use crosstalk_spec::interfaces::l8_surface::QueryApi;
use crosstalk_spec::paging::PageRequest;

pub const RESULTS_PAGE: NonZeroU32 = match NonZeroU32::new(15) {
    Some(n) => n,
    None => NonZeroU32::MIN,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Results {
    /// Nothing selected.
    Idle,
    Point(Option<Box<TransmissionRow>>),
    Lasso {
        selected: usize,
        of: usize,
        rows: Vec<TransmissionRow>,
        next: Option<String>,
        paged: bool,
    },
}

/// Validates the shard's arguments and resolves the selection.
pub async fn load(
    cx: &Cx,
    caller: &Caller,
    state: &str,
    projection: &str,
    selection: &str,
    cursor: &str,
) -> std::result::Result<Results, UiError> {
    require(caller, Permission::View)?;
    require(caller, Permission::Content)?;
    let state = state_from_query(cx, state).await?;
    let selection = ProjectionSelection::parse(selection).map_err(|e| invalid("ps", e))?;
    let cursor = parse_cursor(Some(cursor).filter(|c| !c.is_empty()))?;
    if selection == ProjectionSelection::None {
        return Ok(Results::Idle);
    }
    let id = ProjectionId::parse_ulid(projection).map_err(|e| invalid("p", e))?;
    let backend = backend(cx);
    let projection = backend.projection(caller, id).await?;
    let version = TopicVersionSelector::Pinned(projection.topic_version());
    let of = usize::try_from(projection.frame().count()).unwrap_or(usize::MAX);
    match selection {
        ProjectionSelection::None => Ok(Results::Idle),
        ProjectionSelection::Point(tx) => {
            let listed = summaries_by_id(
                cx,
                caller,
                vec![tx],
                version,
                &crate::pages::common::paging::first(RESULTS_PAGE),
            )
            .await?
            .page;
            let row = rows(cx, caller, listed.items(), &state)
                .await
                .into_iter()
                .next();
            Ok(Results::Point(row.map(Box::new)))
        }
        ProjectionSelection::Lasso(polygon) => {
            let ids = polygon.transmissions(&projection);
            let selected = ids.len();
            if ids.is_empty() {
                return Ok(Results::Lasso {
                    selected,
                    of,
                    rows: Vec::new(),
                    next: None,
                    paged: false,
                });
            }
            let page = PageRequest {
                after: cursor.clone(),
                size: crate::pages::common::paging::size(RESULTS_PAGE.items()),
            };
            let listed = summaries_by_id(cx, caller, ids, version, &page).await?.page;
            Ok(Results::Lasso {
                selected,
                of,
                rows: rows(cx, caller, listed.items(), &state).await,
                next: listed.next().map(|c| c.token().to_owned()),
                paged: cursor.is_some(),
            })
        }
    }
}

#[shard]
pub async fn projection_results(
    cx: &Cx,
    state: String,
    projection: String,
    sel: Signal<String>,
    cursor: Signal<String>,
) -> Result<impl View> {
    let caller = caller(cx);
    let selected = sel.get();
    let at = cursor.get();
    let loaded = load(cx, &caller, &state, &projection, &selected, &at).await;
    Ok(view! {
        <div data-results=(selected.clone())>
            match loaded {
                Err(error) => error_panel(error: &error),
                Ok(Results::Idle) => <p class="text-xs text-zinc-500">"Click a point, or shift-drag a lasso around a cluster, to list its transmissions here."</p>,
                Ok(Results::Point(None)) => empty_state(message: "No stored transmission has this id any more."),
                Ok(Results::Point(Some(row))) => {
                    <div class="rounded border border-zinc-200 p-3 text-sm dark:border-zinc-800">
                        <div class="mb-1 text-[11px] font-semibold uppercase tracking-wide text-zinc-500">"Selected point"</div>
                        <p class="flex flex-wrap items-center gap-2">
                            <a class=(LINK) href=(row.url.clone())>"Transmission " <span class="font-mono">(row.short)</span></a>
                            kind_badge(value: row.state)
                            if let Some(verdict) = row.verdict {
                                kind_badge(value: verdict)
                            }
                        </p>
                        <p class="mt-1 text-xs">
                            match row.from {
                                Some(from) => <a class=(LINK) href=(from.url)>(from.name)</a>,
                                None => <span class="italic text-zinc-500">"unknown"</span>,
                            }
                            <span class="px-1 text-zinc-400">"→"</span>
                            <a class=(LINK) href=(row.to.url)>(row.to.name)</a>
                        </p>
                        <p class="mt-1 flex items-center gap-1.5 text-xs text-zinc-500">
                            route_badge(kind: row.route_kind)
                            (row.route) " · " (row.opened) " · " (row.matched_bytes)
                        </p>
                        <a class=(format!("{LINK} mt-2 inline-block text-xs")) href=(row.url)>"Open evidence →"</a>
                    </div>
                },
                Ok(Results::Lasso { selected: count, of, rows, next, paged }) => {
                    <div class="mb-2 flex items-baseline justify-between text-xs">
                        <span><span class="font-semibold tabular-nums">(count)</span> " of " (of) " points inside the lasso"</span>
                        <div class="flex gap-2">
                            if paged {
                                <button type="button" class=(SMALL_BUTTON) @click=$(|_e| cursor.set("".to_owned()))>"« First"</button>
                            }
                            if let Some(next) = next {
                                <button type="button" class=(SMALL_BUTTON) @click=$(|_e| cursor.set(next.to_owned()))>"Next »"</button>
                            }
                        </div>
                    </div>
                    if count == 0 {
                        empty_state(message: "The lasso holds no points.")
                    } else {
                        transmission_table(rows: rows)
                    }
                },
            }
        </div>
    })
}
