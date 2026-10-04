//! `/export`: choose a dataset, topic version, format and whether to
//! include content, over the view's window and filter; and the detection
//! quality summary for the window.
//!
//! The backend has no export call yet (contract item 11). A valid
//! submission is answered with 501 and the exact `ExportRequest` that would
//! be sent, so the form, its validation and its permissions are in place
//! for when it does.

pub mod quality;
pub mod request;

use crosstalk_spec::interfaces::l8_surface::Permission;
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::content::Form;
use topcoat::router::{StatusCode, page};
use topcoat::view::{View, component, view};

use self::quality::{quality_lines, quality_section};
use self::request::{DatasetChoice, FORMATS, describe, format_code, format_label, parse};
use crate::app::{backend, caller, can};
use crate::backend::Backend;
use crate::components::form::{BUTTON_PRIMARY, INPUT, LABEL, PANEL, SECTION, SECTION_TITLE};
use crate::components::{error_panel, format_time, href, page_header};
use crate::contract::errors::QueryError;
use crate::contract::research::ExportRequest;
use crate::pages::common::action::{require, status_of};
use crate::pages::common::form::FormFields;
use crate::pages::view::view_state;
use crate::url::view_state::ViewState;

pub const PATH: &str = "/export";

/// What a post produced.
#[derive(Debug, Clone, PartialEq)]
pub enum Submitted {
    /// Valid, but the backend cannot export yet.
    Unavailable(ExportRequest),
    Invalid(QueryError, FormFields),
}

#[page("/export")]
async fn export_get(cx: &Cx) -> Result<impl View> {
    let state = view_state(cx).await?;
    Ok(view! { export_page(state: state, submitted: None) })
}

#[page(POST "/export")]
async fn export_post(cx: &Cx, Form(fields): Form<FormFields>) -> Result<impl View> {
    let state = view_state(cx).await?;
    let caller = caller(cx);
    let submitted =
        match require(&caller, Permission::View).and_then(|()| parse(&fields, &state, &caller)) {
            Ok(request) => Submitted::Unavailable(request),
            Err(error) => Submitted::Invalid(error, fields),
        };
    Ok(view! { export_page(state: state, submitted: Some(submitted)) })
}

#[component]
async fn export_page(cx: &Cx, state: ViewState, submitted: Option<Submitted>) -> Result<impl View> {
    let caller = caller(cx);
    let allowed = require(&caller, Permission::View);
    let content = can(&caller, Permission::Content);
    let backend = backend(cx);
    let quality = match &allowed {
        Ok(()) => backend
            .detection_quality(&caller, state.scope.window)
            .await
            .map(|rows| quality_lines(&rows)),
        Err(error) => Err(error.clone()),
    };
    // Versions to export under; listing them reads topic metadata, which
    // needs Content. Without it the view's version is the only choice.
    let versions: Vec<u32> = if content {
        match backend.topic_versions(&caller).await {
            Ok(versions) => versions.iter().map(|v| v.version.0).collect(),
            Err(error) => {
                tracing::warn!(%error, "topic versions unavailable");
                vec![state.scope.topic_version.0]
            }
        }
    } else {
        vec![state.scope.topic_version.0]
    };
    let (status, outcome, retained) = match submitted {
        None => (None, None, None),
        Some(Submitted::Unavailable(request)) => {
            (Some(StatusCode::NOT_IMPLEMENTED), Some(Ok(request)), None)
        }
        Some(Submitted::Invalid(error, fields)) => {
            (Some(status_of(&error)), Some(Err(error)), Some(fields))
        }
    };
    let window = format!(
        "{} → {}",
        format_time(state.scope.window.start()),
        format_time(state.scope.window.end())
    );
    Ok(view! {
        if let Some(status) = status {
            (status)
        }
        page_header(title: "Export", subtitle: "Research datasets over the current window and filter, with their topic version recorded.")
        match allowed {
            Err(error) => {
                (status_of(&error))
                error_panel(error: &error)
            },
            Ok(()) => {
                match outcome {
                    Some(Ok(request)) => outcome_panel(request: request),
                    Some(Err(error)) => <div class="mb-4">error_panel(error: &error)</div>,
                    None => "",
                }
                <section class=(SECTION)>
                    <h2 class=(SECTION_TITLE)>"Dataset"</h2>
                    export_form(state: &state, window: window, versions: versions, content: content, retained: retained)
                </section>
                quality_section(lines: quality)
            },
        }
    })
}

#[component]
async fn outcome_panel(request: ExportRequest) -> Result<impl View> {
    let lines = describe(&request);
    let exact = format!("{request:#?}");
    Ok(view! {
        <div class="mb-4 rounded border border-amber-300 bg-amber-50 p-3 text-sm text-amber-900 dark:border-amber-800 dark:bg-amber-950 dark:text-amber-100" role="status">
            <p class="font-medium">"Export is not available from this backend yet."</p>
            <p class="mb-2 text-xs">"This request is valid; it is what would be sent once the gateway offers export:"</p>
            <dl class="grid grid-cols-[auto_1fr] gap-x-4 gap-y-0.5 text-xs">
                for (label, value) in lines {
                    <dt class="opacity-70">(label)</dt>
                    <dd class="font-mono">(value)</dd>
                }
            </dl>
            <details class="mt-2 text-xs">
                <summary class="cursor-pointer opacity-70">"As sent"</summary>
                <pre class="mt-1 overflow-auto whitespace-pre font-mono text-[11px]">(exact)</pre>
            </details>
        </div>
    })
}

#[component]
async fn export_form(
    state: &ViewState,
    window: String,
    versions: Vec<u32>,
    content: bool,
    retained: Option<FormFields>,
) -> Result<impl View> {
    let pick = |key: &str| {
        retained
            .as_ref()
            .and_then(|f| f.text(key))
            .map(str::to_owned)
    };
    let dataset = pick("dataset").unwrap_or_else(|| DatasetChoice::Transmissions.code().to_owned());
    let version = pick("version").unwrap_or_else(|| state.scope.topic_version.0.to_string());
    let format = pick("format").unwrap_or_else(|| "jsonl".to_owned());
    let projection = pick("projection").unwrap_or_default();
    let include = content && pick("content").is_some();
    let datasets: Vec<(&str, &str, bool)> = DatasetChoice::ALL
        .iter()
        .map(|d| (d.code(), d.label(), d.code() == dataset))
        .collect();
    let versions: Vec<(String, bool, bool)> = versions
        .into_iter()
        .map(|v| {
            (
                v.to_string(),
                v.to_string() == version,
                v == state.scope.topic_version.0,
            )
        })
        .collect();
    let formats: Vec<(&str, &str, bool)> = FORMATS
        .iter()
        .map(|f| (format_code(*f), format_label(*f), format_code(*f) == format))
        .collect();
    let action = href(PATH, state, &[]);
    let filter_note = if state.scope.filter == Default::default() {
        "no filter".to_owned()
    } else {
        "the view's filter".to_owned()
    };
    let topology = href("/topology", state, &[]);
    Ok(view! {
        <form method="post" action=(action) class=(format!("{PANEL} space-y-3"))>
            <p class="text-xs text-zinc-500">
                "Window " <span class="text-zinc-800 dark:text-zinc-200">(window)</span> " with " (filter_note) ". "
                <a class="text-sky-700 hover:underline dark:text-sky-400" href=(topology)>"Change it on the topology"</a>
            </p>
            <div class="flex flex-wrap items-end gap-3">
                <label class="block">
                    <span class=(LABEL)>"Dataset"</span>
                    <select name="dataset" class=(INPUT)>
                        for (code, label, chosen) in datasets {
                            <option value=(code) selected=(chosen)>(label)</option>
                        }
                    </select>
                </label>
                <label class="block">
                    <span class=(LABEL)>"Projection id (projection only)"</span>
                    <input type="text" name="projection" value=(projection) maxlength="26" class=(format!("{INPUT} w-64 font-mono text-xs")) placeholder="01J…">
                </label>
                <label class="block">
                    <span class=(LABEL)>"Topic version"</span>
                    <select name="version" class=(INPUT)>
                        for (v, chosen, in_view) in versions {
                            <option value=(v.clone()) selected=(chosen)>"v" (v) (if in_view { " (view)" } else { "" })</option>
                        }
                    </select>
                </label>
                <label class="block">
                    <span class=(LABEL)>"Format"</span>
                    <select name="format" class=(INPUT)>
                        for (code, label, chosen) in formats {
                            <option value=(code) selected=(chosen)>(label)</option>
                        }
                    </select>
                </label>
                <label class=(if content { "flex items-center gap-1.5 pb-1 text-sm" } else { "flex items-center gap-1.5 pb-1 text-sm text-zinc-400" }) title=(if content { "" } else { "Needs the Content permission" })>
                    <input type="checkbox" name="content" value="1" checked=(include) disabled=(!content)>
                    "Include message content"
                </label>
                <button type="submit" class=(BUTTON_PRIMARY)>"Export"</button>
            </div>
            if !content {
                <p class="text-xs text-zinc-500">"Content can be included only with the Content permission."</p>
            }
        </form>
    })
}

#[cfg(test)]
mod tests {
    use topcoat::router::StatusCode;

    use super::*;
    use crate::pages::topology::tests::fixture_state;
    use crate::testing::{get, post};

    fn url(extra: &str) -> String {
        format!("{PATH}?{}{extra}", fixture_state().to_query())
    }

    #[tokio::test]
    async fn the_form_and_quality_table_render() {
        assert_eq!(get(PATH).await.status, StatusCode::TEMPORARY_REDIRECT);
        let reply = get(&url("")).await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        assert!(reply.body.contains("name=\"dataset\""));
        assert!(reply.body.contains("v2 (view)"));
        assert!(reply.body.contains("Include message content"));
        assert!(reply.body.contains("Detection quality in this window"));
        assert!(reply.body.contains(">total</span>"));
        assert!(reply.body.contains("decoded"));
    }

    #[tokio::test]
    async fn valid_posts_show_the_request_that_would_be_sent() {
        let reply = post(
            &url("&r=channel"),
            "dataset=edges&version=1&format=parquet&content=1",
        )
        .await;
        assert_eq!(reply.status, StatusCode::NOT_IMPLEMENTED, "{}", reply.body);
        assert!(
            reply
                .body
                .contains("Export is not available from this backend yet.")
        );
        assert!(reply.body.contains(">edges</dd>"));
        assert!(reply.body.contains(">v1</dd>"));
        assert!(reply.body.contains(">channel</dd>"));
        assert!(reply.body.contains("ExportRequest"));
    }

    #[tokio::test]
    async fn invalid_posts_keep_the_input() {
        let reply = post(&url(""), "dataset=projection&format=parquet").await;
        assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(
            reply
                .body
                .contains("projection: a projection export needs its id")
        );
        assert!(reply.body.contains("<option value=\"parquet\" selected"));
    }
}
