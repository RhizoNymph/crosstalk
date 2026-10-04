//! Fitting a projection of the view's window and filter: the form and its
//! parameters, checked as the spec's `ProjectionParams` and
//! `ProjectionLimit` check them.
//!
//! Defaults: 15 neighbours, minimum distance 0.1, seed 42 and a sample of at
//! most 5,000 transmissions. The seed is part of the stored projection, so
//! the same window, filter and parameters reproduce the same points.

use crosstalk_spec::aggregates::projection::{ProjectionLimit, ProjectionParams};
use topcoat::Result;
use topcoat::view::{View, component, view};

use crate::components::error_panel;
use crate::components::form::{BUTTON_PRIMARY, INPUT, LABEL};
use crate::error::UiError;
use crate::pages::common::form::{FormFields, invalid};

pub const DEFAULT_NEIGHBORS: &str = "15";
pub const DEFAULT_MIN_DIST: &str = "0.1";
pub const DEFAULT_SEED: &str = "42";
pub const DEFAULT_SAMPLE: &str = "5000";

/// Fractional digits a minimum distance may carry: the spec records it in
/// thousandths.
const MIN_DIST_DECIMALS: usize = 3;

fn field<'a>(fields: &'a FormFields, key: &str, default: &'a str) -> &'a str {
    fields.text(key).unwrap_or(default)
}

/// `0` to `1` with at most three decimals, in thousandths.
fn min_dist_milli(text: &str) -> std::result::Result<u16, UiError> {
    let bad = || {
        invalid(
            "min_dist",
            format!(
                "{text:?} is not a number from 0 to 1 with at most {MIN_DIST_DECIMALS} decimals"
            ),
        )
    };
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    let digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    if whole.is_empty() || !digits(whole) || !digits(fraction) || fraction.len() > MIN_DIST_DECIMALS
    {
        return Err(bad());
    }
    let whole: u16 = whole.parse().map_err(|_| bad())?;
    let padded = format!("{fraction:0<MIN_DIST_DECIMALS$}");
    let thousandths: u16 = padded.parse().map_err(|_| bad())?;
    let milli = whole
        .checked_mul(1_000)
        .and_then(|w| w.checked_add(thousandths))
        .ok_or_else(bad)?;
    if milli > ProjectionParams::MAX_MIN_DIST_MILLI {
        return Err(bad());
    }
    Ok(milli)
}

/// The parameters a fit form asks for; blank fields take the defaults.
pub fn parse(fields: &FormFields) -> std::result::Result<ProjectionParams, UiError> {
    let (min, max) = (
        ProjectionParams::MIN_NEIGHBORS,
        ProjectionParams::MAX_NEIGHBORS,
    );
    let neighbors = field(fields, "neighbors", DEFAULT_NEIGHBORS)
        .parse::<u16>()
        .ok()
        .filter(|n| (min..=max).contains(n))
        .ok_or_else(|| invalid("neighbors", format!("expected {min} to {max}")))?;
    let min_dist = min_dist_milli(field(fields, "min_dist", DEFAULT_MIN_DIST))?;
    let seed = field(fields, "seed", DEFAULT_SEED)
        .parse::<u64>()
        .map_err(|_| invalid("seed", "expected a non-negative integer"))?;
    let limit = field(fields, "sample_limit", DEFAULT_SAMPLE)
        .parse::<u32>()
        .ok()
        .and_then(|n| ProjectionLimit::new(n).ok())
        .ok_or_else(|| {
            invalid(
                "sample_limit",
                format!("expected 1 to {}", ProjectionLimit::MAX),
            )
        })?;
    ProjectionParams::new(limit, neighbors, min_dist, seed)
        .map_err(|e| invalid("params", format!("{e:?}")))
}

/// The fit form, posting `action=fit` to `action`.
#[component]
pub async fn fit_form(
    action: String,
    submit: &str,
    retained: Option<FormFields>,
    error: Option<UiError>,
) -> Result<impl View> {
    let value = |key: &str, default: &str| {
        retained
            .as_ref()
            .and_then(|f| f.text(key))
            .unwrap_or(default)
            .to_owned()
    };
    let neighbors = value("neighbors", DEFAULT_NEIGHBORS);
    let min_dist = value("min_dist", DEFAULT_MIN_DIST);
    let seed = value("seed", DEFAULT_SEED);
    let sample = value("sample_limit", DEFAULT_SAMPLE);
    let small = format!("{INPUT} w-20 py-0.5 text-xs");
    Ok(view! {
        <form method="post" action=(action) class="space-y-2">
            <input type="hidden" name="action" value="fit">
            <div class="flex flex-wrap items-end gap-2">
                <label class="block">
                    <span class=(LABEL)>"Neighbours"</span>
                    <input type="number" name="neighbors" min="2" max="200" value=(neighbors) class=(small.clone())>
                </label>
                <label class="block">
                    <span class=(LABEL)>"Min distance"</span>
                    <input type="number" name="min_dist" min="0" max="1" step="0.001" value=(min_dist) class=(small.clone())>
                </label>
                <label class="block">
                    <span class=(LABEL)>"Seed"</span>
                    <input type="number" name="seed" min="0" value=(seed) class=(small.clone())>
                </label>
                <label class="block">
                    <span class=(LABEL)>"Sample"</span>
                    <input type="number" name="sample_limit" min="1" max="100000" value=(sample) class=(small)>
                </label>
                <button type="submit" class=(format!("{BUTTON_PRIMARY} py-0.5 text-xs"))>(submit)</button>
            </div>
            if let Some(error) = error {
                error_panel(error: &error)
            }
        </form>
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimum_distances_are_thousandths() {
        assert_eq!(min_dist_milli("0"), Ok(0));
        assert_eq!(min_dist_milli("1"), Ok(1_000));
        assert_eq!(min_dist_milli("0.1"), Ok(100));
        assert_eq!(min_dist_milli("0.05"), Ok(50));
        assert_eq!(min_dist_milli("0.125"), Ok(125));
        assert!(min_dist_milli("1.001").is_err());
        assert!(min_dist_milli(".5").is_err());
    }

    #[test]
    fn blank_fields_take_the_defaults() {
        let params = parse(&FormFields::default()).expect("defaults");
        assert_eq!(params.neighbors(), 15);
        assert_eq!(params.min_dist_milli(), 100);
        assert_eq!(params.seed(), 42);
        assert_eq!(params.limit().get().get(), 5000);
        let custom = FormFields::from_pairs(&[
            ("neighbors", "30"),
            ("min_dist", "0.5"),
            ("seed", "7"),
            ("sample_limit", "200"),
        ]);
        let params = parse(&custom).expect("custom");
        assert_eq!((params.neighbors(), params.seed()), (30, 7));
        assert_eq!(params.min_dist_milli(), 500);
    }

    #[test]
    fn out_of_range_fields_are_named() {
        for (key, value) in [
            ("neighbors", "0"),
            ("neighbors", "201"),
            ("min_dist", "1.5"),
            ("min_dist", "NaN"),
            ("seed", "-1"),
            ("sample_limit", "0"),
            ("sample_limit", "100001"),
            ("neighbors", "1"),
            ("min_dist", "0.0005"),
            ("min_dist", "-0.1"),
            ("min_dist", "1e-1"),
        ] {
            let fields = FormFields::from_pairs(&[(key, value)]);
            assert!(
                matches!(
                    parse(&fields),
                    Err(UiError::Field { field, .. }) if field == key
                ),
                "{key}={value}"
            );
        }
    }
}
