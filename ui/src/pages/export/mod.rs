//! `/export`: choose a dataset, topic version, format and whether to
//! include content, over the view's window and filter, and download it;
//! and the detection quality summary for the window.
//!
//! `POST /export` validates the form into the spec's `ExportRequest`
//! ([`request::parse`]), calls `QueryApi::export` and answers with the
//! JSON Lines download ([`jsonl`]). A refused post (invalid input, a
//! missing permission, a conflict) is rewritten to `GET /export` carrying
//! the error and the submitted fields ([`Rejected`]), so the page renders
//! the error with the input kept, under the status the error gives.

pub mod jsonl;
pub mod quality;
pub mod request;

use crosstalk_spec::interfaces::l8_surface::Permission;
use crosstalk_spec::interfaces::l8_surface::export::ExportFormats;
use topcoat::Result;
use topcoat::context::{Cx, try_request_context};
use topcoat::router::content::Form;
use topcoat::router::error::rewrite;
use topcoat::router::header::{CONTENT_LENGTH, CONTENT_TYPE};
use topcoat::router::request::{headers, uri};
use topcoat::router::{Body, Method, StatusCode, page, route};
use topcoat::view::{View, component, view};

use self::jsonl::{Download, download};
use self::quality::{quality_lines, quality_section};
use self::request::{DatasetChoice, FORMATS, format_code, format_label, parse};
use crate::app::{backend, caller, can, present};
use crate::components::form::{BUTTON_PRIMARY, INPUT, LABEL, PANEL, SECTION, SECTION_TITLE};
use crate::components::{error_panel, format_time, href, page_header};
use crate::error::UiError;
use crate::pages::common::action::{require, status_of};
use crate::pages::common::form::FormFields;
use crate::pages::view::view_state;
use crate::url::view_state::ViewState;
use crosstalk_spec::interfaces::l8_surface::QueryApi;

pub const PATH: &str = "/export";

/// A refused export post, carried to the page that shows it.
#[derive(Debug, Clone, PartialEq)]
pub struct Rejected {
    pub error: UiError,
    pub fields: FormFields,
}

#[page("/export")]
async fn export_get(cx: &Cx) -> Result<impl View> {
    let state = view_state(cx).await?;
    let rejected = try_request_context::<Rejected>(cx).cloned();
    Ok(view! { export_page(state: state, rejected: rejected) })
}

/// Validates, exports and downloads; a refusal is shown on the page.
#[route(POST "/export")]
async fn export_post(cx: &Cx, Form(fields): Form<FormFields>) -> Result<Download> {
    let state = view_state(cx).await?;
    let caller = caller(cx);
    let backend = backend(cx);
    let exported = async {
        require(&caller, Permission::View)?;
        let writes = present(cx).await.map_err(|e| UiError::from(e.clone()))?;
        let request = parse(&fields, &state, &caller, writes.export_formats.as_slice())?;
        let export = backend.export(&caller, &request).await?;
        download(export).await
    }
    .await;
    match exported {
        Ok(download) => {
            tracing::info!(operator = ?caller.operator(), file = download.filename(), "export downloaded");
            Ok(download)
        }
        Err(error) => {
            tracing::info!(operator = ?caller.operator(), error = ?error, "export refused");
            Err(show(cx, Rejected { error, fields }))
        }
    }
}

/// Rewrites the post to the page at the same URL, carrying the refusal.
fn show(cx: &Cx, rejected: Rejected) -> topcoat::Error {
    let target = uri(cx)
        .path_and_query()
        .map_or_else(|| PATH.to_owned(), |pq| pq.as_str().to_owned());
    let mut kept = headers(cx).clone();
    kept.remove(CONTENT_TYPE);
    kept.remove(CONTENT_LENGTH);
    rewrite(target, Body::empty())
        .method(Method::GET)
        .headers(kept)
        .with(rejected)
        .into()
}

#[component]
async fn export_page(cx: &Cx, state: ViewState, rejected: Option<Rejected>) -> Result<impl View> {
    let caller = caller(cx);
    // The formats the backend writes, from the request's present (which
    // needs View, like the page).
    let allowed = match require(&caller, Permission::View) {
        Ok(()) => present(cx)
            .await
            .map(|present| present.export_formats.clone())
            .map_err(|e| UiError::from(e.clone())),
        Err(error) => Err(error),
    };
    let content = can(&caller, Permission::Content);
    let backend = backend(cx);
    let quality = match &allowed {
        Ok(_) => backend
            .detection_quality(&caller, state.scope.window)
            .await
            .map(|quality| quality_lines(quality.rows()))
            .map_err(UiError::from),
        Err(error) => Err(error.clone()),
    };
    // Versions to export under: those whose data retention still keeps
    // (the history needs only View). If it cannot be read, the view's
    // version is the only choice.
    let versions: Vec<u32> = match backend.topic_versions(&caller).await {
        Ok(history) => history
            .versions()
            .iter()
            .filter(|info| info.retention().is_retained())
            .map(|info| info.version().0)
            .collect(),
        Err(error) => {
            tracing::warn!(error = ?error, "topic versions unavailable");
            vec![state.scope.topic_version.0]
        }
    };
    let (status, refusal, retained): (Option<StatusCode>, _, _) = match rejected {
        None => (None, None, None),
        Some(Rejected { error, fields }) => (Some(status_of(&error)), Some(error), Some(fields)),
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
            Ok(writes) => {
                if let Some(error) = refusal {
                    <div class="mb-4">error_panel(error: &error)</div>
                }
                <section class=(SECTION)>
                    <h2 class=(SECTION_TITLE)>"Dataset"</h2>
                    export_form(state: &state, window: window, versions: versions, content: content, writes: writes, retained: retained)
                </section>
                quality_section(lines: quality)
            },
        }
    })
}

#[component]
async fn export_form(
    state: &ViewState,
    window: String,
    versions: Vec<u32>,
    content: bool,
    writes: ExportFormats,
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
    // Formats the backend does not write are shown, disabled, so the
    // choice is visible and never refused after the fact.
    let formats: Vec<(&str, String, bool, bool)> = FORMATS
        .iter()
        .map(|f| {
            let available = writes.offers(*f);
            let label = if available {
                format_label(*f).to_owned()
            } else {
                format!("{} (not available on this backend)", format_label(*f))
            };
            (format_code(*f), label, format_code(*f) == format, available)
        })
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
                        for (code, label, chosen, available) in formats {
                            <option value=(code) selected=(chosen) disabled=(!available)>(label)</option>
                        }
                    </select>
                </label>
                <label class=(if content { "flex items-center gap-1.5 pb-1 text-sm" } else { "flex items-center gap-1.5 pb-1 text-sm text-zinc-400" }) title=(if content { "" } else { "Needs the Content permission" })>
                    <input type="checkbox" name="content" value="1" checked=(include) disabled=(!content)>
                    "Include message content"
                </label>
                <button type="submit" class=(BUTTON_PRIMARY)>"Export"</button>
            </div>
            <p class="text-xs text-zinc-500">"Downloads one JSON object per line: a header (what was selected, the topic version and the watermark), one line per row, and a trailer with the row count and digest. Accesses and verdicts have no content columns; a projection needs the Content permission."</p>
            if !content {
                <p class="text-xs text-zinc-500">"Content can be included only with the Content permission."</p>
            }
        </form>
    })
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::aggregates::projection::ProjectionStatusKind;
    use crosstalk_spec::paging::PageRequest;
    use serde_json::Value;
    use topcoat::router::StatusCode;

    use super::*;
    use crate::pages::topology::tests::fixture_state;
    use crate::testing::{Session, get, post, world};
    use crate::url::ulid::UlidId;

    fn url(extra: &str) -> String {
        format!("{PATH}?{}{extra}", fixture_state().to_query())
    }

    /// The whole generated week, under v2.
    fn week_url() -> String {
        let mut state = fixture_state();
        state.scope.window = crosstalk_spec::support::TimeWindow::new(
            crosstalk_spec::support::Timestamp::from_micros(1_790_380_800_000_000),
            state.scope.window.end(),
        )
        .expect("week");
        format!("{PATH}?{}", state.to_query())
    }

    fn lines(body: &str) -> Vec<Value> {
        body.lines()
            .map(|line| serde_json::from_str(line).expect("a JSON line"))
            .collect()
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
        assert!(
            reply
                .body
                .contains("Parquet (not available on this backend)")
        );
    }

    #[tokio::test]
    async fn valid_posts_download_the_export_as_json_lines() {
        let reply = post(
            &url("&r=channel"),
            "dataset=edges&version=2&format=jsonl&content=1",
        )
        .await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        assert_eq!(
            reply
                .headers
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("application/x-ndjson")
        );
        let lines = lines(&reply.body);
        let (Some(header), Some(trailer)) = (lines.first(), lines.last()) else {
            panic!("header and trailer lines");
        };
        assert_eq!(header["type"], "header");
        assert_eq!(header["dataset"], "edges");
        assert_eq!(header["include_content"], true);
        assert_eq!(header["selection"]["filter"]["route_kinds"][0], "channel");
        assert_eq!(header["basis"]["topic_version"], 2);
        let export = header["export"].as_str().expect("export id");
        assert_eq!(
            reply
                .headers
                .get("content-disposition")
                .and_then(|v| v.to_str().ok()),
            Some(format!("attachment; filename=\"crosstalk-edges-{export}.jsonl\"").as_str())
        );
        let rows = &lines[1..lines.len() - 1];
        assert!(!rows.is_empty());
        assert!(rows.iter().all(|row| row["type"] == "row"
            && row["dataset"] == "edges"
            && row["route_kind"] == "channel"));
        assert_eq!(header["rows"].as_u64(), u64::try_from(rows.len()).ok());
        assert_eq!(trailer["type"], "trailer");
        assert_eq!(trailer["export"], export);
        assert_eq!(trailer["rows"], header["rows"]);
        assert_eq!(trailer["end"]["status"], "complete");
        assert_eq!(trailer["digest"].as_str().map(str::len), Some(64));
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

    #[tokio::test]
    async fn refused_exports_show_the_error_with_the_input_kept() {
        let parquet = post(&url(""), "dataset=verdicts&format=parquet").await;
        assert_eq!(
            parquet.status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{}",
            parquet.body
        );
        assert!(parquet.body.contains("this backend cannot write Parquet"));

        let content = post(&url(""), "dataset=accesses&format=jsonl&content=1").await;
        assert_eq!(content.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(
            content
                .body
                .contains("accesses exports have no content columns")
        );

        let large = post(&week_url(), "dataset=accesses&format=jsonl").await;
        assert_eq!(large.status, StatusCode::CONFLICT, "{}", large.body);
        assert!(large.body.contains("<option value=\"accesses\" selected"));

        let queued = world()
            .projections(
                &crate::testing::operator().caller(),
                &PageRequest {
                    size: crate::pages::common::paging::size(50),
                    after: None,
                },
            )
            .await
            .expect("jobs")
            .into_parts()
            .0
            .into_iter()
            .find(|info| info.status().kind() == ProjectionStatusKind::Queued)
            .expect("a queued job");
        let form = format!("dataset=projection&projection={}", queued.id().to_ulid());
        let not_ready = post(&url(""), &form).await;
        assert_eq!(not_ready.status, StatusCode::CONFLICT, "{}", not_ready.body);
        assert!(
            not_ready.body.contains(&queued.id().to_ulid()),
            "input kept"
        );
    }

    #[tokio::test]
    async fn exports_appear_in_the_audit_log() {
        let session = Session::new();
        let reply = session
            .post(&url(""), "dataset=verdicts&format=jsonl")
            .await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        let refused = session
            .post(
                &url(""),
                "dataset=projection&projection=01J9ZQ3W8D0000000000000001",
            )
            .await;
        assert_eq!(refused.status, StatusCode::NOT_FOUND, "{}", refused.body);
        let audit = session
            .get(&format!("/audit?{}&span=all", fixture_state().to_query()))
            .await;
        assert_eq!(audit.status, StatusCode::OK, "{}", audit.body);
        assert!(audit.body.contains("started exporting"), "{}", audit.body);
        assert!(audit.body.contains("finished exporting"));
        assert!(audit.body.contains("asked to export"));
    }
}
