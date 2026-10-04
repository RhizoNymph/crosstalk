//! Reading the shared view state from a request.

use std::time::Duration;

use crosstalk_spec::support::{TimeWindow, Timestamp};
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::error::{bad_request, forbidden, internal_server_error, redirect};
use topcoat::router::parse_query_params;
use topcoat::router::request::uri;

use crate::app::{backend, caller};
use crate::backend::Backend;
use crate::contract::errors::{InputError, QueryError};
use crate::url::view_state::{Defaults, RawViewState, ViewState};

/// The default window: the 24 hours before the backend's present.
const DEFAULT_SPAN: Duration = Duration::from_secs(24 * 3600);

/// What a view state without a window or version defaults to: the last 24
/// hours before the backend's `now`, and its current topic version. Shared
/// by pages and data routes.
pub async fn defaults(cx: &Cx) -> std::result::Result<Defaults, QueryError> {
    let backend = backend(cx);
    let caller = caller(cx);
    let end = backend.now(&caller).await?;
    let topic_version = backend.current_topic_version(&caller).await?;
    let span = u64::try_from(DEFAULT_SPAN.as_micros()).unwrap_or(u64::MAX);
    let start = Timestamp::from_micros(end.as_micros().saturating_sub(span));
    let window = TimeWindow::new(start, end).map_err(|_| {
        QueryError::InvalidInput(InputError::Field {
            field: "window",
            reason: "the backend's present leaves an empty default window".to_owned(),
        })
    })?;
    Ok(Defaults {
        window,
        topic_version,
    })
}

/// The router error for defaults that could not be read.
pub fn defaults_error(error: QueryError) -> topcoat::Error {
    match error {
        QueryError::Forbidden { .. } => forbidden().into(),
        other => {
            tracing::error!(error = %other, "view defaults unavailable");
            internal_server_error(other).into()
        }
    }
}

/// The request's view state when its query carries a complete, valid one;
/// `None` otherwise. Never redirects: for the layout's navigation links.
pub async fn current_state(cx: &Cx) -> Option<ViewState> {
    let raw: RawViewState = parse_query_params(cx).ok()?;
    let parsed = ViewState::parse(&raw, defaults(cx).await.ok()?).ok()?;
    parsed.complete.then_some(parsed.state)
}

/// The request's view state. An incomplete URL is redirected to its
/// canonical form, so every URL a page is shown under reproduces it.
pub async fn view_state(cx: &Cx) -> Result<ViewState> {
    let raw: RawViewState =
        parse_query_params(cx).map_err(|e| bad_request(format!("query: {e}")))?;
    let defaults = defaults(cx).await.map_err(defaults_error)?;
    let parsed = ViewState::parse(&raw, defaults).map_err(|e| bad_request(e.to_string()))?;
    if !parsed.complete {
        let path = uri(cx).path().to_owned();
        return Err(redirect(format!("{path}?{}", parsed.state.to_query())).into());
    }
    Ok(parsed.state)
}
