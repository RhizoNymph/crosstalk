//! Strict query parsing for data routes.
//!
//! Pages redirect an incomplete view-state URL to its canonical form; a data
//! route cannot, since the element asked for exactly that URL and a
//! redirect would silently change what it shows. Here every required key
//! must be present, and the values are validated by the same
//! [`ViewState::parse`] with the same defaults as `pages::view`.

use std::num::NonZeroU32;
use std::time::Duration;

use crosstalk_spec::support::{TimeWindow, Timestamp};
use topcoat::context::Cx;
use topcoat::router::error::bad_request;
use topcoat::router::parse_query_params;

use crate::app::backend;
use crate::url::view_state::{Defaults, RawViewState, ViewState, ViewStateError};

/// The default window: the 24 hours before the data's watermark (as in
/// `pages::view`).
const DEFAULT_SPAN: Duration = Duration::from_secs(24 * 3600);

/// Timeline bucket count when `buckets` is absent.
pub const DEFAULT_BUCKETS: NonZeroU32 = NonZeroU32::new(96).expect("96 is non-zero");
/// The most buckets a timeline request may ask for.
pub const MAX_BUCKETS: u32 = 1000;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StrictViewStateError {
    #[error("incomplete view state: missing {}", .0.join(", "))]
    Missing(Vec<&'static str>),
    #[error(transparent)]
    Invalid(#[from] ViewStateError),
}

/// Parses a view state that must carry every required key.
pub fn parse_strict(
    raw: &RawViewState,
    defaults: Defaults,
) -> Result<ViewState, StrictViewStateError> {
    let missing: Vec<&'static str> = [
        ("from", raw.from.is_none()),
        ("to", raw.to.is_none()),
        ("v", raw.v.is_none()),
        ("w", raw.w.is_none()),
        ("g", raw.g.is_none()),
    ]
    .into_iter()
    .filter_map(|(key, absent)| absent.then_some(key))
    .collect();
    if !missing.is_empty() {
        return Err(StrictViewStateError::Missing(missing));
    }
    let parsed = ViewState::parse(raw, defaults)?;
    Ok(parsed.state)
}

fn defaults(cx: &Cx) -> topcoat::Result<Defaults> {
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

/// The request's view state; a 400 if it is incomplete or invalid.
pub fn view_state(cx: &Cx) -> topcoat::Result<ViewState> {
    let raw: RawViewState =
        parse_query_params(cx).map_err(|e| bad_request(format!("query: {e}")))?;
    parse_strict(&raw, defaults(cx)?).map_err(|e| bad_request(e.to_string()).into())
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BucketsError {
    #[error("buckets: expected an integer from 1 to {MAX_BUCKETS}")]
    OutOfRange,
}

/// Parses the `buckets` key: absent means [`DEFAULT_BUCKETS`].
pub fn parse_buckets(text: Option<&str>) -> Result<NonZeroU32, BucketsError> {
    let Some(text) = text else {
        return Ok(DEFAULT_BUCKETS);
    };
    text.trim()
        .parse::<u32>()
        .ok()
        .filter(|n| *n <= MAX_BUCKETS)
        .and_then(NonZeroU32::new)
        .ok_or(BucketsError::OutOfRange)
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize)]
struct RawBuckets {
    buckets: Option<String>,
}

/// The request's `buckets` key; a 400 if out of range.
pub fn buckets(cx: &Cx) -> topcoat::Result<NonZeroU32> {
    let raw: RawBuckets = parse_query_params(cx).map_err(|e| bad_request(format!("query: {e}")))?;
    parse_buckets(raw.buckets.as_deref()).map_err(|e| bad_request(e.to_string()).into())
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::aggregates::topic::TopicModelVersion;

    use super::*;

    fn defaults() -> Defaults {
        Defaults {
            window: TimeWindow::new(
                Timestamp::from_micros(1_000_000),
                Timestamp::from_micros(2_000_000),
            )
            .expect("window"),
            topic_version: TopicModelVersion(1),
        }
    }

    fn complete() -> RawViewState {
        RawViewState {
            from: Some("2026-10-02T00:00:00Z".to_owned()),
            to: Some("2026-10-03T00:00:00Z".to_owned()),
            v: Some("3".to_owned()),
            w: Some("tx".to_owned()),
            g: Some("channels".to_owned()),
            ..RawViewState::default()
        }
    }

    #[test]
    fn complete_query_parses() {
        let state = parse_strict(&complete(), defaults()).expect("parse");
        assert_eq!(state.scope.topic_version, TopicModelVersion(3));
        assert_eq!(
            state.to_query(),
            "from=2026-10-02T00:00:00Z&to=2026-10-03T00:00:00Z&v=3&w=tx&g=channels"
        );
    }

    #[test]
    fn incomplete_query_names_missing_keys() {
        let raw = RawViewState {
            v: None,
            g: None,
            ..complete()
        };
        assert_eq!(
            parse_strict(&raw, defaults()),
            Err(StrictViewStateError::Missing(vec!["v", "g"]))
        );
        assert_eq!(
            parse_strict(&RawViewState::default(), defaults()),
            Err(StrictViewStateError::Missing(vec![
                "from", "to", "v", "w", "g"
            ]))
        );
    }

    #[test]
    fn invalid_values_are_reported() {
        let raw = RawViewState {
            w: Some("edges".to_owned()),
            ..complete()
        };
        assert_eq!(
            parse_strict(&raw, defaults()),
            Err(StrictViewStateError::Invalid(ViewStateError::Weighting))
        );
    }

    #[test]
    fn buckets_are_bounded() {
        assert_eq!(parse_buckets(None), Ok(DEFAULT_BUCKETS));
        assert_eq!(parse_buckets(Some("1")).map(NonZeroU32::get), Ok(1));
        assert_eq!(
            parse_buckets(Some("1000")).map(NonZeroU32::get),
            Ok(MAX_BUCKETS)
        );
        for bad in ["0", "1001", "-3", "many", ""] {
            assert_eq!(parse_buckets(Some(bad)), Err(BucketsError::OutOfRange));
        }
    }
}
