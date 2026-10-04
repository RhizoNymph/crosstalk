//! Reading the shared view state from a request.

use std::time::Duration;

use crosstalk_spec::support::{TimeWindow, Timestamp};
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::error::{bad_request, redirect};
use topcoat::router::parse_query_params;
use topcoat::router::request::uri;

use crate::app::backend;
use crate::url::view_state::{Defaults, RawViewState, ViewState};

/// The default window: the 24 hours before the data's watermark.
const DEFAULT_SPAN: Duration = Duration::from_secs(24 * 3600);

fn defaults(cx: &Cx) -> Result<Defaults> {
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
