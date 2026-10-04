//! Reading the shared view state from a request.

use std::time::Duration;

use crosstalk_spec::support::{TimeWindow, Timestamp};
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::error::{bad_request, redirect};
use topcoat::router::parse_query_params;
use topcoat::router::request::uri;

use topcoat::router::content::Form;

use crate::app::backend;
use crate::contract::errors::QueryError;
use crate::data::query::parse_strict;
use crate::pages::common::form::invalid;
use crate::url::view_state::{Defaults, RawViewState, ViewState};

/// The default window: the 24 hours before the data's watermark.
const DEFAULT_SPAN: Duration = Duration::from_secs(24 * 3600);

pub fn defaults(cx: &Cx) -> Result<Defaults> {
    let backend = backend(cx);
    let end = backend.now();
    let span = u64::try_from(DEFAULT_SPAN.as_micros()).unwrap_or(u64::MAX);
    let start = Timestamp::from_micros(end.as_micros().saturating_sub(span));
    let window = TimeWindow::new(start, end).map_err(|_| bad_request("empty default window"))?;
    Ok(Defaults {
        window,
        topic_version: backend.current_topic_version(),
    })
}

/// The request's view state when its query carries a complete, valid one;
/// `None` otherwise. Never redirects: for the layout's navigation links.
pub fn current_state(cx: &Cx) -> Option<ViewState> {
    let raw: RawViewState = parse_query_params(cx).ok()?;
    let parsed = ViewState::parse(&raw, defaults(cx).ok()?).ok()?;
    parsed.complete.then_some(parsed.state)
}

/// The request's view state. An incomplete URL is redirected to its
/// canonical form, so every URL a page is shown under reproduces it.
pub fn view_state(cx: &Cx) -> Result<ViewState> {
    let raw: RawViewState =
        parse_query_params(cx).map_err(|e| bad_request(format!("query: {e}")))?;
    let parsed = ViewState::parse(&raw, defaults(cx)?).map_err(|e| bad_request(e.to_string()))?;
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
pub fn state_from_query(cx: &Cx, query: &str) -> std::result::Result<ViewState, QueryError> {
    let Form(raw) =
        Form::<RawViewState>::from_bytes(query.as_bytes()).map_err(|e| invalid("state", e))?;
    let defaults = defaults(cx).map_err(|e| QueryError::Store {
        reason: e.to_string(),
    })?;
    parse_strict(&raw, defaults).map_err(|e| invalid("state", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::components::href::tests::state;
    use crate::testing::cx;

    #[test]
    fn shard_state_round_trips_and_rejects_partial_queries() {
        let cx = cx();
        let mut expected = state();
        expected.scope.filter.route_kinds =
            vec![crosstalk_spec::aggregates::edge::RouteKind::Channel];
        let parsed = state_from_query(&cx, &expected.to_query()).expect("parse");
        assert_eq!(parsed, expected);
        assert!(state_from_query(&cx, "from=2026-10-02T00:00:00Z").is_err());
        assert!(state_from_query(&cx, &format!("{}&w=edges", "from=a")).is_err());
    }
}
