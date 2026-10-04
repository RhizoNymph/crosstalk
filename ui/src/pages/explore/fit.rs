//! Fitting a projection of the view's scope: the form and its parameters.
//!
//! Defaults: 15 neighbours, minimum distance 0.1, seed 42 and a sample of at
//! most 5,000 transmissions. The seed is part of the stored projection, so
//! the same scope and parameters reproduce the same points.

use std::num::{NonZeroU16, NonZeroU32};

use topcoat::Result;
use topcoat::view::{View, component, view};

use crate::components::error_panel;
use crate::components::form::{BUTTON_PRIMARY, INPUT, LABEL};
use crate::contract::errors::QueryError;
use crate::contract::research::ProjectionParams;
use crate::pages::common::form::{FormFields, invalid};

pub const DEFAULT_NEIGHBORS: &str = "15";
pub const DEFAULT_MIN_DIST: &str = "0.1";
pub const DEFAULT_SEED: &str = "42";
pub const DEFAULT_SAMPLE: &str = "5000";

const MAX_NEIGHBORS: u16 = 200;
const MAX_SAMPLE: u32 = 100_000;

fn field<'a>(fields: &'a FormFields, key: &str, default: &'a str) -> &'a str {
    fields.text(key).unwrap_or(default)
}

/// The parameters a fit form asks for; blank fields take the defaults.
pub fn parse(fields: &FormFields) -> std::result::Result<ProjectionParams, QueryError> {
    let neighbors = field(fields, "neighbors", DEFAULT_NEIGHBORS)
        .parse::<u16>()
        .ok()
        .filter(|n| *n <= MAX_NEIGHBORS)
        .and_then(NonZeroU16::new)
        .ok_or_else(|| invalid("neighbors", format!("expected 1 to {MAX_NEIGHBORS}")))?;
    let min_dist_text = field(fields, "min_dist", DEFAULT_MIN_DIST);
    let min_dist = min_dist_text
        .parse::<f32>()
        .ok()
        .filter(|v| v.is_finite())
        .ok_or_else(|| invalid("min_dist", format!("{min_dist_text:?} is not a number")))?;
    let seed = field(fields, "seed", DEFAULT_SEED)
        .parse::<u64>()
        .map_err(|_| invalid("seed", "expected a non-negative integer"))?;
    let sample_limit = field(fields, "sample_limit", DEFAULT_SAMPLE)
        .parse::<u32>()
        .ok()
        .filter(|n| *n <= MAX_SAMPLE)
        .and_then(NonZeroU32::new)
        .ok_or_else(|| invalid("sample_limit", format!("expected 1 to {MAX_SAMPLE}")))?;
    ProjectionParams::new(neighbors, min_dist, seed, sample_limit)
        .map_err(|e| invalid("min_dist", e))
}

/// The fit form, posting `action=fit` to `action`.
#[component]
pub async fn fit_form(
    action: String,
    submit: &str,
    retained: Option<FormFields>,
    error: Option<QueryError>,
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
                    <input type="number" name="neighbors" min="1" max="200" value=(neighbors) class=(small.clone())>
                </label>
                <label class="block">
                    <span class=(LABEL)>"Min distance"</span>
                    <input type="number" name="min_dist" min="0" max="1" step="0.05" value=(min_dist) class=(small.clone())>
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
    use crate::contract::errors::InputError;

    #[test]
    fn blank_fields_take_the_defaults() {
        let params = parse(&FormFields::default()).expect("defaults");
        assert_eq!(params.neighbors.get(), 15);
        assert!((params.min_dist() - 0.1).abs() < f32::EPSILON);
        assert_eq!(params.seed, 42);
        assert_eq!(params.sample_limit.get(), 5000);
        let custom = FormFields::from_pairs(&[
            ("neighbors", "30"),
            ("min_dist", "0.5"),
            ("seed", "7"),
            ("sample_limit", "200"),
        ]);
        let params = parse(&custom).expect("custom");
        assert_eq!((params.neighbors.get(), params.seed), (30, 7));
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
        ] {
            let fields = FormFields::from_pairs(&[(key, value)]);
            assert!(
                matches!(
                    parse(&fields),
                    Err(QueryError::InvalidInput(InputError::Field { field, .. })) if field == key
                ),
                "{key}={value}"
            );
        }
    }
}
