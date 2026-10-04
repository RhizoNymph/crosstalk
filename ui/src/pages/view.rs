//! Reading the shared view state from a request.

use std::time::Duration;

use crosstalk_spec::support::{TimeWindow, Timestamp};
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::error::{bad_request, forbidden, internal_server_error, redirect};
use topcoat::router::parse_query_params;
use topcoat::router::request::uri;

use topcoat::router::content::Form;

use crate::app::{backend, caller};
use crate::contract::present::Present;
use crate::data::query::parse_strict;
use crate::error::UiError;
use crate::pages::common::form::invalid;
use crate::pages::common::topics::default_version;
use crate::url::scope::{align_down, align_up};
use crate::url::view_state::{Defaults, RawViewState, ViewState};
use crosstalk_spec::interfaces::l8_surface::QueryError;

/// The default window: the 24 hours before the backend's present, ending
/// on the bucket boundary at or after it.
const DEFAULT_SPAN: Duration = Duration::from_secs(24 * 3600);

/// What a view state without a window or version defaults to: the last 24
/// hours before the backend's `now` (on bucket boundaries), and the topic
/// history's active version. Shared by pages and data routes.
pub async fn defaults(cx: &Cx) -> std::result::Result<Defaults, UiError> {
    let backend = backend(cx);
    let caller = caller(cx);
    let bucket = backend.bucket_width();
    let end = align_up(backend.now(&caller).await?, bucket);
    let topic_version = default_version(backend, &caller).await?;
    let span = u64::try_from(DEFAULT_SPAN.as_micros()).unwrap_or(u64::MAX);
    let start = align_down(
        Timestamp::from_micros(end.as_micros().saturating_sub(span)),
        bucket,
    );
    let window = TimeWindow::new(start, end).map_err(|_| {
        UiError::field(
            "window",
            "the backend's present leaves an empty default window",
        )
    })?;
    Ok(Defaults {
        window,
        topic_version,
        bucket,
    })
}

/// The router error for defaults that could not be read.
pub fn defaults_error(error: UiError) -> topcoat::Error {
    match error {
        UiError::Query(QueryError::Forbidden { .. }) => forbidden().into(),
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

/// A view state handed to a shard as its canonical query string. A shard's
/// endpoint does not see the page URL, so pages pass their state along;
/// like any shard argument it is user input, parsed strictly (every
/// required key) and reported as `InvalidInput` on the `state` field.
pub async fn state_from_query(cx: &Cx, query: &str) -> std::result::Result<ViewState, UiError> {
    let Form(raw) =
        Form::<RawViewState>::from_bytes(query.as_bytes()).map_err(|e| invalid("state", e))?;
    let defaults = defaults(cx).await?;
    parse_strict(&raw, defaults).map_err(|e| invalid("state", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::components::href::tests::state;
    use crate::testing::cx;

    #[tokio::test]
    async fn shard_state_round_trips_and_rejects_partial_queries() {
        let cx = cx();
        let mut expected = state();
        expected.scope.filter.route_kinds =
            vec![crosstalk_spec::aggregates::edge::RouteKind::Channel];
        let parsed = state_from_query(&cx, &expected.to_query())
            .await
            .expect("parse");
        assert_eq!(parsed, expected);
        assert!(
            state_from_query(&cx, "from=2026-10-02T00:00:00Z")
                .await
                .is_err()
        );
        assert!(
            state_from_query(&cx, &format!("{}&w=edges", "from=a"))
                .await
                .is_err()
        );
    }
}
