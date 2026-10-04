//! `/explore`: search and the UMAP projection, linked.
//!
//! Search hits (left) light up in the projection (`data-highlight`); a
//! point or lasso in the projection (`value`) sets the `ps` signal, which
//! the results shard resolves on the server and the URL keeps
//! (`history.replaceState`). Colour-by is a signal bound to
//! `data-color-by`, also mirrored into the URL (`cb`). Without a projection
//! id (`p`) the page offers to fit one for the view's scope; posting
//! `action=fit` stores it and redirects here with `p`. Every part of the
//! page reads message content, so it needs `Content`; without it the page
//! says so instead of failing.

pub mod fit;
pub mod lasso;
pub mod query;
pub mod results;
pub mod search;
pub mod topics;

use crosstalk_spec::interfaces::l8_surface::Permission;
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::content::Form;
use topcoat::router::{page, query_params};
use topcoat::runtime::{Event, signal};
use topcoat::view::{View, component, view};

use self::fit::fit_form;
use self::query::{ColorBy, ExploreQuery, RawExploreQuery};
use self::results::projection_results;
use self::search::{Hits, hit_list, load_hits, search_form};
use self::topics::{load_topics, topic_sidebar};
use crate::app::{backend, caller, can};
use crate::backend::Backend;
use crate::components::form::{INPUT, LABEL, PANEL};
use crate::components::{PageLinks, error_panel, flash_banner, format_time, href, page_header};
use crate::contract::research::ProjectionJob;
use crate::data::elements::PROJECTION_JS;
use crate::error::UiError;
use crate::pages::common::action::{Failure, done, error_for, fields_for, require, status_of};
use crate::pages::common::flash::{Flash, flash};
use crate::pages::common::form::{FormFields, invalid};
use crate::pages::common::paging::Count;
use crate::pages::common::paging::page_request;
use crate::pages::view::view_state;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;
use crosstalk_spec::ids::ProjectionId;
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::paging::{PageRequest, SearchList};

pub const PATH: &str = "/explore";

/// The forms on this page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExploreForm {
    Fit,
}

/// The projection panel's state.
#[derive(Debug, Clone, PartialEq)]
pub enum ProjectionPanel {
    NotFitted,
    Missing,
    Unavailable(UiError),
    Pending(String),
    Failed(String),
    Ready {
        id: ProjectionId,
        meta: String,
        /// The projection was fitted for another window, filter or version.
        stale: bool,
    },
}

/// The panel for the query's projection.
pub fn panel(
    job: Option<std::result::Result<ProjectionJob, UiError>>,
    state: &ViewState,
    id: Option<ProjectionId>,
) -> ProjectionPanel {
    match (job, id) {
        (None, _) | (_, None) => ProjectionPanel::NotFitted,
        (Some(Err(UiError::Query(QueryError::NotFound))), _) => ProjectionPanel::Missing,
        (Some(Err(error)), _) => ProjectionPanel::Unavailable(error),
        (Some(Ok(ProjectionJob::Queued)), _) => {
            ProjectionPanel::Pending("Queued for fitting.".to_owned())
        }
        (Some(Ok(ProjectionJob::Running { done, total })), _) => {
            ProjectionPanel::Pending(format!("Fitting: {done} of {total} done."))
        }
        (Some(Ok(ProjectionJob::Failed { reason })), _) => ProjectionPanel::Failed(reason),
        (Some(Ok(ProjectionJob::Ready(meta))), Some(id)) => ProjectionPanel::Ready {
            id,
            meta: format!(
                "{} · {} neighbours · min distance {} · seed {} · sample ≤ {} · fitted {}",
                meta.embedding_model.name,
                meta.params.neighbors,
                meta.params.min_dist(),
                meta.params.seed,
                meta.params.sample_limit,
                format_time(meta.fitted_at)
            ),
            stale: meta.scope != state.scope,
        },
    }
}

fn borrowed<'a>(pairs: &'a [(&'static str, String)]) -> Vec<(&'static str, &'a str)> {
    pairs.iter().map(|(k, v)| (*k, v.as_str())).collect()
}

fn parse_query(cx: &Cx) -> std::result::Result<ExploreQuery, UiError> {
    query_params::<RawExploreQuery>(cx)
        .map_err(|e| invalid("query", e))
        .and_then(ExploreQuery::parse)
}

/// Search hits per page: the list sits beside the projection.
const SEARCH_PAGE: std::num::NonZeroU32 = match std::num::NonZeroU32::new(20) {
    Some(n) => n,
    None => std::num::NonZeroU32::MIN,
};

fn search_page(cx: &Cx) -> std::result::Result<PageRequest<SearchList>, UiError> {
    page_request(cx).map(|page| PageRequest {
        size: crate::pages::common::paging::size(SEARCH_PAGE.items()),
        ..page
    })
}

#[page("/explore")]
async fn explore_get(cx: &Cx) -> Result<impl View> {
    let state = view_state(cx).await?;
    let query = parse_query(cx).and_then(|q| search_page(cx).map(|page| (q, page)));
    let flash = flash(cx);
    Ok(view! { explore_page(state: state, query: query, flash: flash, failure: None) })
}

#[page(POST "/explore")]
async fn explore_post(cx: &Cx, Form(fields): Form<FormFields>) -> Result<impl View> {
    let state = view_state(cx).await?;
    let query = parse_query(cx);
    let failure = match fields.text("action") {
        Some("fit") => {
            let caller = caller(cx);
            let fitted = async {
                let query = query.as_ref().map_err(Clone::clone)?;
                let params = fit::parse(&fields)?;
                require(&caller, Permission::Content)?;
                let id = backend(cx)
                    .fit_projection(&caller, &state.scope, params)
                    .await?;
                Ok::<_, UiError>((query.clone(), id))
            }
            .await;
            match fitted {
                Ok((mut next, id)) => {
                    next.projection = Some(id);
                    let pairs = next.pairs();
                    return Err(done(
                        PATH,
                        &state,
                        &borrowed(&pairs),
                        Flash::ProjectionFitted,
                    ));
                }
                Err(error) => Failure::new(Some(ExploreForm::Fit), error, fields),
            }
        }
        _ => Failure::new(None, invalid("action", "unknown action"), fields),
    };
    let query = query.and_then(|q| search_page(cx).map(|page| (q, page)));
    Ok(view! { explore_page(state: state, query: query, flash: None, failure: Some(failure)) })
}

#[component]
async fn explore_page(
    cx: &Cx,
    state: ViewState,
    query: std::result::Result<(ExploreQuery, PageRequest<SearchList>), UiError>,
    flash: Option<Flash>,
    failure: Option<Failure<ExploreForm>>,
) -> Result<impl View> {
    let caller = caller(cx);
    let allowed = require(&caller, Permission::View);
    let content = can(&caller, Permission::Content);
    let failed_status = failure.as_ref().map(Failure::status);
    let fit_error = error_for(failure.as_ref(), ExploreForm::Fit);
    let fit_fields = fields_for(failure.as_ref(), ExploreForm::Fit);
    let other_error = failure
        .as_ref()
        .filter(|f| f.form.is_none())
        .map(|f| f.error.clone());
    let subtitle = format!(
        "Search and the projection of transmitted text, {} → {}.",
        format_time(state.scope.window.start()),
        format_time(state.scope.window.end())
    );
    Ok(view! {
        if let Some(status) = failed_status {
            (status)
        }
        page_header(title: "Explore", subtitle: &subtitle)
        if let Some(flash) = flash {
            flash_banner(message: flash.message())
        }
        if let Some(error) = other_error {
            <div class="mb-4">error_panel(error: &error)</div>
        }
        match (allowed, query) {
            (Err(error), _) | (Ok(()), Err(error)) => {
                (status_of(&error))
                error_panel(error: &error)
            },
            (Ok(()), Ok(_)) if !content => {
                <div class=(PANEL)>
                    <p class="text-sm">"Explore needs the Content permission: search, the projection and topic labels all come from message text."</p>
                    <p class="mt-1 text-xs text-zinc-500">"The topology, channels and agents pages show the same traffic without content."</p>
                </div>
            },
            (Ok(()), Ok((query, page))) => {
                <script type="module" src=(PROJECTION_JS)></script>
                explore_body(state: state, query: query, page: page, fit_error: fit_error, fit_fields: fit_fields)
            },
        }
    })
}

#[component]
async fn explore_body(
    cx: &Cx,
    state: ViewState,
    query: ExploreQuery,
    page: PageRequest<SearchList>,
    fit_error: Option<UiError>,
    fit_fields: Option<FormFields>,
) -> Result<impl View> {
    let caller = caller(cx);
    let backend = backend(cx);
    let hits: Option<std::result::Result<Hits, UiError>> =
        load_hits(cx, &caller, &query, &state, page)
            .await
            .transpose();
    let pairs = query.pairs();
    let links = match &hits {
        Some(Ok(h)) => PageLinks::new(
            PATH,
            &state,
            &borrowed(&pairs),
            h.current.as_ref(),
            h.next.as_ref(),
        ),
        _ => PageLinks::default(),
    };
    let highlight = match &hits {
        Some(Ok(h)) => h.highlight(),
        _ => String::new(),
    };
    let job = match query.projection {
        Some(id) => Some(
            backend
                .projection_job(&caller, id)
                .await
                .map_err(UiError::from),
        ),
        None => None,
    };
    let panel = panel(job, &state, query.projection);
    let topics = load_topics(cx, &caller, &state).await;
    let topics_url = href("/topics", &state, &[]);
    let action = href(PATH, &state, &borrowed(&pairs));
    Ok(view! {
        <div class="grid gap-4 xl:grid-cols-[20rem_minmax(0,1fr)_14rem]">
            <section class="min-w-0">
                search_form(state: &state, query: &query)
                hit_list(hits: hits, links: links)
            </section>
            <section class="min-w-0">
                projection_section(state: &state, query: &query, panel: panel, highlight: highlight, action: action, fit_error: fit_error, fit_fields: fit_fields)
            </section>
            <aside class="min-w-0">
                topic_sidebar(rows: topics, version: state.scope.topic_version.0, topics_url: topics_url)
            </aside>
        </div>
    })
}

#[component]
async fn projection_section(
    cx: &Cx,
    state: &ViewState,
    query: &ExploreQuery,
    panel: ProjectionPanel,
    highlight: String,
    action: String,
    fit_error: Option<UiError>,
    fit_fields: Option<FormFields>,
) -> Result<impl View> {
    let initial_color = query.color.code().to_owned();
    let color = signal(cx, move || initial_color);
    let initial_selection = query.selection_text.clone();
    let sel = signal(cx, move || initial_selection);
    let cursor = signal(cx, String::new);
    let colors: Vec<(&str, &str, bool)> = ColorBy::ALL
        .iter()
        .map(|c| (c.code(), c.label(), *c == query.color))
        .collect();
    let state_query = state.to_query();
    Ok(view! {
        match panel {
            ProjectionPanel::Ready { id, meta, stale } => {
                let src = format!("/data/projection/{}", id.to_ulid());
                let projection = id.to_ulid();
                <div class="mb-2 flex flex-wrap items-center justify-between gap-2">
                    <p class="min-w-0 truncate text-xs text-zinc-500" title=(meta.clone())>(meta)</p>
                    <label class="flex items-center gap-1.5 text-xs">
                        <span class=(LABEL)>"Colour by"</span>
                        <select
                            class=(format!("{INPUT} py-0.5 text-xs"))
                            @change=$(|e: Event| {
                                let v = e.target.value;
                                color.set(v.to_owned());
                                raw!("((value) => { const v = String(value); const kept = location.search.slice(1).split('&').filter((p) => p !== '' && p.split('=')[0] !== 'cb'); if (v !== 'topic') kept.push('cb=' + encodeURIComponent(v)); history.replaceState(history.state, '', location.pathname + '?' + kept.join('&')); })(${v})");
                            })
                        >
                            for (code, label, chosen) in colors {
                                <option value=(code) selected=(chosen)>(label)</option>
                            }
                        </select>
                    </label>
                </div>
                if stale {
                    <div class="mb-2 rounded border border-amber-300 bg-amber-50 px-3 py-2 text-xs text-amber-900 dark:border-amber-800 dark:bg-amber-950 dark:text-amber-100">
                        "This projection was fitted for a different window, filter or topic version. Search hits outside its sample are not drawn."
                        <div class="mt-2">fit_form(action: action.clone(), submit: "Fit for this view", retained: fit_fields.clone(), error: fit_error.clone())</div>
                    </div>
                }
                <ct-projection
                    class="block h-[30rem] rounded border border-zinc-200 dark:border-zinc-800"
                    data-src=(src)
                    data-highlight=(highlight)
                    :data-color-by=$(color.get())
                    @change=$(|e: Event| {
                        let v = e.target.value;
                        sel.set(v.to_owned());
                        cursor.set("".to_owned());
                        raw!("((value) => { const v = String(value); const kept = location.search.slice(1).split('&').filter((p) => p !== '' && p.split('=')[0] !== 'ps'); if (v !== '') kept.push('ps=' + encodeURIComponent(v).replace(/%3A/g, ':').replace(/%2C/g, ',').replace(/%3B/g, ';')); history.replaceState(history.state, '', location.pathname + '?' + kept.join('&')); })(${v})");
                    })
                ></ct-projection>
                <div class="mt-3">
                    projection_results(state: state_query, projection: projection, sel: sel, cursor: cursor)
                </div>
            },
            other => {
                <div class=(PANEL)>
                    match other {
                        ProjectionPanel::Missing => <p class="mb-2 text-sm">"No stored projection has this id. Fit one for this view."</p>,
                        ProjectionPanel::Unavailable(error) => <div class="mb-2">error_panel(error: &error)</div>,
                        ProjectionPanel::Pending(status) => <p class="mb-2 text-sm">(status) " Reload to check again."</p>,
                        ProjectionPanel::Failed(reason) => <p class="mb-2 text-sm text-red-700 dark:text-red-300">"Fitting failed: " (reason)</p>,
                        _ => {
                            <p class="text-sm font-medium">"No projection yet"</p>
                            <p class="mb-3 text-xs text-zinc-500">"A projection places this view's transmissions by the similarity of their text. It is stored with its parameters and seed, so the link reproduces it."</p>
                        },
                    }
                    fit_form(action: action, submit: "Fit projection", retained: fit_fields, error: fit_error)
                </div>
            },
        }
    })
}

#[cfg(test)]
mod tests;
