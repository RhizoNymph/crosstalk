//! A trend as a small inline SVG line, scaled to its own maximum.

use topcoat::Result;
use topcoat::view::{View, component, view};

/// Width and height of the drawing, in SVG user units.
pub const WIDTH: f64 = 96.0;
pub const HEIGHT: f64 = 20.0;

/// The `points` of a polyline drawing `values` left to right in a
/// `WIDTH × HEIGHT` box, the maximum touching the top (with a 1-unit inset
/// so the stroke is not clipped). Empty for no values; one value is a flat
/// line. Coordinates have at most two decimals.
pub fn points(values: &[u64]) -> String {
    let max = values.iter().copied().max().unwrap_or(0);
    let n = values.len();
    let x_at = |i: usize| match n {
        0 | 1 => 0.0,
        _ => WIDTH * i as f64 / (n - 1) as f64,
    };
    let y_at = |v: u64| {
        let ratio = if max == 0 { 0.0 } else { v as f64 / max as f64 };
        (HEIGHT - 1.0) - ratio * (HEIGHT - 2.0)
    };
    let mut out: Vec<String> = values
        .iter()
        .enumerate()
        .map(|(i, v)| format!("{},{}", trim(x_at(i)), trim(y_at(*v))))
        .collect();
    if let [only] = values {
        out.push(format!("{},{}", trim(WIDTH), trim(y_at(*only))));
    }
    out.join(" ")
}

/// A number with at most two decimals and no trailing zeros.
fn trim(value: f64) -> String {
    let text = format!("{value:.2}");
    let text = text.trim_end_matches('0').trim_end_matches('.');
    if text == "-0" {
        "0".to_owned()
    } else {
        text.to_owned()
    }
}

/// A sparkline of `values` (oldest first), drawn in the link colour.
#[component]
pub async fn sparkline(values: Vec<u64>, label: String) -> Result<impl View> {
    let line = points(&values);
    let view_box = format!("0 0 {} {}", trim(WIDTH), trim(HEIGHT));
    Ok(view! {
        <svg
            class="inline-block h-5 w-24 align-middle text-sky-600 dark:text-sky-400"
            viewBox=(view_box)
            preserveAspectRatio="none"
            role="img"
            aria-label=(label)
        >
            <polyline
                points=(line)
                fill="none"
                stroke="currentColor"
                stroke-width="1.25"
                stroke-linejoin="round"
                vector-effect="non-scaling-stroke"
            ></polyline>
        </svg>
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scales_to_the_maximum() {
        assert_eq!(points(&[0, 5, 10]), "0,19 48,10 96,1");
    }

    #[test]
    fn all_zero_is_a_flat_line_at_the_bottom() {
        assert_eq!(points(&[0, 0]), "0,19 96,19");
    }

    #[test]
    fn one_value_spans_the_width_and_none_is_empty() {
        assert_eq!(points(&[3]), "0,1 96,1");
        assert_eq!(points(&[]), "");
    }

    #[tokio::test]
    async fn renders_an_svg_polyline() {
        use crate::testing::{cx, render};
        let cx = &cx();
        let html = render(
            view! { cx => sparkline(values: vec![1, 2], label: "trend".to_owned()) },
            cx,
        )
        .await;
        assert!(html.contains("<polyline"), "{html}");
        assert!(html.contains("points=\"0,10 96,1\""), "{html}");
    }
}
