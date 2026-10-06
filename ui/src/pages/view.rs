//! Reading the shared view state from a request.

use std::time::Duration;

use crosstalk_spec::support::{TimeWindow, Timestamp};
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::error::{bad_request, forbidden, internal_server_error, redirect};
use topcoat::router::parse_query_params;
use topcoat::router::request::uri;

use topcoat::router::content::Form;

use crate::app::{backend, caller, present};
use crate::data::query::parse_strict;
use crate::error::UiError;
use crate::pages::common::form::invalid;
use crate::pages::common::topics::default_version;
use crate::pages::gateway::{gateway_down, render};
use crate::url::follow::FollowSpan;
use crate::url::scope::{align_down, align_up};
use crate::url::view_state::{Defaults, KEYS, RawViewState, ViewState};
use crosstalk_spec::interfaces::l8_surface::QueryError;

/// The default window: the 24 hours before the backend's present, ending
/// on the bucket boundary at or after it.
const DEFAULT_SPAN: Duration = Duration::from_secs(24 * 3600);

/// What a view state without a window or version defaults to: the last 24
/// hours before the backend's view end (its present's `now`, on bucket
/// boundaries), and the topic history's active version. Shared by pages
/// and data routes; the present is the request's one read.
///
/// A followed window ends at the present itself, aligned up
/// ([`Defaults::follow_end`]). That is the view end on every backend but a
/// fixture replay, whose default window stays on the data's end while its
/// present moves: a followed replay slides with the present.
pub async fn defaults(cx: &Cx) -> std::result::Result<Defaults, UiError> {
    let backend = backend(cx);
    let caller = caller(cx);
    let present = present(cx).await.map_err(|e| UiError::Query(e.clone()))?;
    let bucket = present.bucket_width;
    let end = align_up(backend.view_end(present), bucket);
    let follow_end = align_up(present.now, bucket);
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
        follow_end,
        topic_version,
        bucket,
    })
}

/// The router error for defaults that could not be read: `Forbidden` is
/// 403, anything else 500. Data routes answer with it; pages go through
/// [`page_defaults_error`].
pub fn defaults_error(error: UiError) -> topcoat::Error {
    match error {
        UiError::Query(QueryError::Forbidden { .. }) => forbidden().into(),
        other => {
            tracing::error!(error = %other, "view defaults unavailable");
            internal_server_error(other).into()
        }
    }
}

/// [`defaults_error`] for a page: a gateway the http backend cannot reach,
/// or that refused its token, is the full-page gateway state
/// (`pages::gateway`) instead of a 500.
pub fn page_defaults_error(cx: &Cx, error: UiError) -> topcoat::Error {
    match &error {
        UiError::Query(query) => match gateway_down(cx, query) {
            Some(down) => render(cx, down),
            None => defaults_error(error),
        },
        UiError::Field { .. } => defaults_error(error),
    }
}

/// The request's view state when its query carries a complete, valid one;
/// `None` otherwise. Never redirects: for the layout's navigation links.
pub async fn current_state(cx: &Cx) -> Option<ViewState> {
    let raw: RawViewState = parse_query_params(cx).ok()?;
    let parsed = ViewState::parse(&raw, defaults(cx).await.ok()?).ok()?;
    parsed.complete.then_some(parsed.state)
}

/// How a page treats `follow`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Window {
    /// The page shows a fixed window. A URL without one gets the last 24
    /// hours; a followed URL (from a navigation link) is redirected to the
    /// window it resolves to now, keeping the page's own keys.
    Pinned,
    /// The page may follow the present (`/` and `/topology`). A URL
    /// without a window follows the last [`FollowSpan::DEFAULT`].
    Follows,
}

/// The request's view state on a page with a pinned window
/// ([`Window::Pinned`]).
pub async fn view_state(cx: &Cx) -> Result<ViewState> {
    page_state(cx, Window::Pinned).await
}

/// The request's view state on a page that may follow
/// ([`Window::Follows`]).
pub async fn followed_view_state(cx: &Cx) -> Result<ViewState> {
    page_state(cx, Window::Follows).await
}

/// The request's view state. An incomplete URL is redirected to its
/// canonical form, so every URL a page is shown under reproduces it.
async fn page_state(cx: &Cx, window: Window) -> Result<ViewState> {
    let mut raw: RawViewState =
        parse_query_params(cx).map_err(|e| bad_request(format!("query: {e}")))?;
    // A defaulted window is never canonical, even with every other key.
    let defaulted =
        window == Window::Follows && raw.from.is_none() && raw.to.is_none() && raw.follow.is_none();
    if defaulted {
        raw.follow = Some(FollowSpan::DEFAULT.as_str().to_owned());
    }
    let defaults = defaults(cx).await.map_err(|e| page_defaults_error(cx, e))?;
    let parsed = ViewState::parse(&raw, defaults).map_err(|e| bad_request(e.to_string()))?;
    let path = uri(cx).path().to_owned();
    if window == Window::Pinned && parsed.state.follow.is_some() {
        let own = own_pairs(uri(cx).query().unwrap_or(""));
        let pinned = parsed.state.pinned().to_query();
        let query = if own.is_empty() {
            pinned
        } else {
            format!("{pinned}&{own}")
        };
        return Err(redirect(format!("{path}?{query}")).into());
    }
    if defaulted || !parsed.complete {
        return Err(redirect(format!("{path}?{}", parsed.state.to_query())).into());
    }
    Ok(parsed.state)
}

/// The pairs of a raw query that are not view-state keys, as written.
fn own_pairs(query: &str) -> String {
    query
        .split('&')
        .filter(|pair| {
            let key = pair.split_once('=').map_or(*pair, |(key, _)| key);
            !pair.is_empty() && !KEYS.contains(&key)
        })
        .collect::<Vec<_>>()
        .join("&")
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

    #[test]
    fn own_pairs_drop_every_view_state_key() {
        assert_eq!(
            own_pairs("follow=1d&v=2&tab=review&w=tx&g=agents&cursor=abc&a=x&sel=agent:1"),
            "tab=review&cursor=abc&sel=agent:1"
        );
        assert_eq!(own_pairs("follow=1d&v=2&w=tx&g=agents"), "");
        assert_eq!(own_pairs(""), "");
    }

    #[tokio::test]
    async fn a_shard_refuses_a_followed_state() {
        let cx = cx();
        assert!(
            state_from_query(&cx, "follow=1d&v=1&w=tx&g=agents")
                .await
                .is_err()
        );
    }

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
