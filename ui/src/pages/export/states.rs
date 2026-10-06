//! Which transmission states a transmissions export holds, as the export
//! page's URL carries them (`ExportStates`).
//!
//! ```text
//! /export?<view>                                  the default: confirmed, classified, aggregated
//! /export?<view>&states=suspected,confirmed,…     an explicit set, codes in ascending state order
//! /export?<view>&states_form=1&state=…&state=…    the page's states form ─▶ redirected to the canonical URL
//! ```
//!
//! The default keeps the export page's URL as it was, and an explicit set
//! is written one way only (the spec's wire codes, in ascending
//! `TransmissionStateKind` order, the order `ExportStates::iter` gives),
//! so an export view stays citeable and reproducible. No state at all, an
//! unknown code, `detected` or a repeat are field errors.

use crosstalk_spec::interfaces::l8_surface::export::{ExportStates, InvalidExportStates};
use crosstalk_spec::interfaces::l8_surface::summary::TransmissionStateKind;

use crate::components::href::decode_component;
use crate::error::UiError;
use crate::pages::common::form::invalid;

/// The canonical URL key: a comma-separated list of codes.
pub const KEY: &str = "states";
/// One checkbox of the page's states form.
pub const PICK: &str = "state";
/// Marks a submission of the page's states form (so that ticking nothing
/// is told apart from the default).
pub const FORM: &str = "states_form";

/// The URL and wire code of a state.
pub fn code(state: TransmissionStateKind) -> &'static str {
    match state {
        TransmissionStateKind::Detected => "detected",
        TransmissionStateKind::AwaitingContent => "awaiting_content",
        TransmissionStateKind::Suspected => "suspected",
        TransmissionStateKind::Discarded => "discarded",
        TransmissionStateKind::Confirmed => "confirmed",
        TransmissionStateKind::Classified => "classified",
        TransmissionStateKind::Aggregated => "aggregated",
    }
}

/// How the export form names a state; the unconfirmed ones say so.
pub fn label(state: TransmissionStateKind) -> &'static str {
    match state {
        TransmissionStateKind::Detected => "Detected",
        TransmissionStateKind::AwaitingContent => "Awaiting content (unconfirmed)",
        TransmissionStateKind::Suspected => "Suspected (unconfirmed)",
        TransmissionStateKind::Discarded => "Discarded (unconfirmed)",
        TransmissionStateKind::Confirmed => "Confirmed",
        TransmissionStateKind::Classified => "Classified",
        TransmissionStateKind::Aggregated => "Aggregated",
    }
}

/// Whether `state` carries a confirmation.
pub fn is_confirmed(state: TransmissionStateKind) -> bool {
    ExportStates::CONFIRMED.contains(&state)
}

/// The URL value of `states`: `None` for the default, else the codes in
/// ascending state order.
pub fn canonical(states: &ExportStates) -> Option<String> {
    (!states.is_confirmed()).then(|| states.iter().map(code).collect::<Vec<_>>().join(","))
}

/// The states asked for, and where to send the browser when the URL does
/// not say them canonically.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Requested {
    pub states: ExportStates,
    /// `Some(canonical)` when the URL is to be rewritten: the canonical
    /// value of [`KEY`], or `None` inside to drop the key (the default).
    pub redirect: Option<Option<String>>,
}

fn field(reason: impl std::fmt::Display) -> UiError {
    invalid(KEY, reason)
}

fn parse_code(text: &str) -> Result<TransmissionStateKind, UiError> {
    ExportStates::ALL
        .into_iter()
        .chain([TransmissionStateKind::Detected])
        .find(|state| code(*state) == text)
        .ok_or_else(|| field(format!("unknown transmission state {text:?}")))
}

fn checked(states: Vec<TransmissionStateKind>) -> Result<ExportStates, UiError> {
    ExportStates::new(states).map_err(|error| match error {
        InvalidExportStates::Empty => field("choose at least one transmission state"),
        InvalidExportStates::Detected => field("detected transmissions are never exported"),
        InvalidExportStates::Duplicate(state) => field(format!("{} is named twice", code(state))),
    })
}

/// The states a URL's query asks for (`None`: no query).
pub fn from_query(query: Option<&str>) -> Result<Requested, UiError> {
    let pairs: Vec<(String, String)> = query
        .unwrap_or_default()
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            (decode_component(key), decode_component(value))
        })
        .collect();
    let values = |key: &str| -> Vec<&str> {
        pairs
            .iter()
            .filter(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
            .collect()
    };
    if !values(FORM).is_empty() {
        let picked = values(PICK)
            .into_iter()
            .map(parse_code)
            .collect::<Result<Vec<_>, _>>()?;
        let states = checked(picked)?;
        let redirect = Some(canonical(&states));
        return Ok(Requested { states, redirect });
    }
    match values(KEY).as_slice() {
        [] => Ok(Requested {
            states: ExportStates::confirmed(),
            redirect: None,
        }),
        [text] => {
            let listed = if text.is_empty() {
                Vec::new()
            } else {
                text.split(',')
                    .map(parse_code)
                    .collect::<Result<Vec<_>, _>>()?
            };
            let states = checked(listed)?;
            let canonical = canonical(&states);
            let redirect = (canonical.as_deref() != Some(*text)).then_some(canonical);
            Ok(Requested { states, redirect })
        }
        _ => Err(field("give the states once")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn states(list: &[TransmissionStateKind]) -> ExportStates {
        ExportStates::new(list.to_vec()).expect("states")
    }

    fn field_of(result: Result<Requested, UiError>) -> Option<String> {
        match result {
            Err(UiError::Field { field, reason }) => Some(format!("{field}: {reason}")),
            _ => None,
        }
    }

    #[test]
    fn no_states_in_the_url_is_the_default_and_stays_put() {
        let requested = from_query(Some("from=a&to=b")).expect("default");
        assert_eq!(requested.states, ExportStates::confirmed());
        assert_eq!(requested.redirect, None);
        assert_eq!(from_query(None).expect("default").redirect, None);
    }

    #[test]
    fn the_canonical_list_is_ascending_and_absent_for_the_default() {
        assert_eq!(canonical(&ExportStates::confirmed()), None);
        assert_eq!(
            canonical(&ExportStates::all()).as_deref(),
            Some("awaiting_content,suspected,confirmed,classified,aggregated,discarded")
        );
        let requested = from_query(Some("states=suspected,confirmed")).expect("states");
        assert_eq!(
            requested.states,
            states(&[
                TransmissionStateKind::Suspected,
                TransmissionStateKind::Confirmed
            ])
        );
        assert_eq!(requested.redirect, None, "already canonical");
    }

    #[test]
    fn a_non_canonical_list_is_redirected() {
        let reordered = from_query(Some("states=confirmed,suspected")).expect("states");
        assert_eq!(
            reordered.redirect,
            Some(Some("suspected,confirmed".to_owned()))
        );
        let default = from_query(Some("states=aggregated,confirmed,classified")).expect("states");
        assert_eq!(default.states, ExportStates::confirmed());
        assert_eq!(default.redirect, Some(None), "the default drops the key");
    }

    #[test]
    fn the_states_form_is_redirected_to_its_canonical_url() {
        let picked =
            from_query(Some("states_form=1&state=discarded&state=suspected&from=x")).expect("ok");
        assert_eq!(
            picked.states,
            states(&[
                TransmissionStateKind::Suspected,
                TransmissionStateKind::Discarded
            ])
        );
        assert_eq!(
            picked.redirect,
            Some(Some("suspected,discarded".to_owned()))
        );
        let default = from_query(Some(
            "states_form=1&state=confirmed&state=classified&state=aggregated",
        ))
        .expect("ok");
        assert_eq!(default.redirect, Some(None));
    }

    #[test]
    fn no_state_unknown_detected_and_repeats_are_field_errors() {
        assert_eq!(
            field_of(from_query(Some("states_form=1"))).as_deref(),
            Some("states: choose at least one transmission state")
        );
        assert_eq!(
            field_of(from_query(Some("states="))).as_deref(),
            Some("states: choose at least one transmission state")
        );
        assert!(field_of(from_query(Some("states=everything"))).is_some());
        assert_eq!(
            field_of(from_query(Some("states=detected"))).as_deref(),
            Some("states: detected transmissions are never exported")
        );
        assert!(field_of(from_query(Some("states=suspected,suspected"))).is_some());
        assert!(field_of(from_query(Some("states=suspected&states=confirmed"))).is_some());
    }

    #[test]
    fn the_unconfirmed_states_are_labelled_so() {
        for state in ExportStates::ALL {
            assert_eq!(
                label(state).contains("(unconfirmed)"),
                !is_confirmed(state),
                "{state:?}"
            );
        }
    }
}
