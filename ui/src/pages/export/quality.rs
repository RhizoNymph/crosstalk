//! Detection quality for the window (the spec's `DetectionQuality`): every
//! judgeable transmission opened in it, per route kind and detector call
//! (confirmed by its strongest match class, suspected, discarded), by
//! current verdict, with precision for confirmed evidence.

use topcoat::Result;
use topcoat::view::{View, component, view};

use crate::components::form::{SECTION, SECTION_TITLE};
use crate::components::table::{ROW, TD, TD_NUM};
use crate::components::{data_table, empty_state, error_panel, route_badge};
use crate::error::UiError;
use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::aggregates::quality::{MatchClass, QualityMatch, QualityRow};

/// The detector's call in words.
pub fn match_kind_name(kind: QualityMatch) -> &'static str {
    match kind {
        QualityMatch::Content(MatchClass::Exact) => "exact",
        QualityMatch::Content(MatchClass::Normalized) => "normalized",
        QualityMatch::Content(MatchClass::Decoded) => "decoded",
        QualityMatch::Content(MatchClass::Semantic) => "semantic",
        QualityMatch::Suspected => "suspected (access only)",
        QualityMatch::Discarded => "discarded",
    }
}

/// Genuine over labelled, when anything is labelled.
pub fn precision(genuine: u64, false_detection: u64) -> Option<f64> {
    let labelled = genuine + false_detection;
    (labelled > 0).then(|| genuine as f64 / labelled as f64)
}

#[derive(Debug, Clone, PartialEq)]
pub struct QualityLine {
    /// `None` on the totals line.
    pub route: Option<RouteKind>,
    pub match_kind: String,
    pub genuine: u64,
    pub false_detection: u64,
    pub unlabeled: u64,
    /// Over confirmed evidence only: a verdict on a suspected or discarded
    /// transmission judges a call the detector did not make.
    pub precision: String,
}

fn percent(precision: Option<f64>) -> String {
    precision.map_or_else(|| "—".to_owned(), |p| format!("{:.0}%", p * 100.0))
}

fn confirmed(row: &QualityRow) -> bool {
    matches!(row.match_kind, QualityMatch::Content(_))
}

/// One line per row, then the totals. Precision is shown for confirmed
/// rows, and on the totals line over the confirmed rows.
pub fn quality_lines(rows: &[QualityRow]) -> Vec<QualityLine> {
    let mut lines: Vec<QualityLine> = rows
        .iter()
        .map(|r| QualityLine {
            route: Some(r.route_kind),
            match_kind: match_kind_name(r.match_kind).to_owned(),
            genuine: r.genuine,
            false_detection: r.false_detection,
            unlabeled: r.unlabeled,
            precision: percent(
                confirmed(r)
                    .then(|| precision(r.genuine, r.false_detection))
                    .flatten(),
            ),
        })
        .collect();
    if !rows.is_empty() {
        let sum = |keep: fn(&QualityRow) -> bool, f: fn(&QualityRow) -> u64| {
            rows.iter().filter(|r| keep(r)).map(f).sum::<u64>()
        };
        let all = |_: &QualityRow| true;
        lines.push(QualityLine {
            route: None,
            match_kind: "all".to_owned(),
            genuine: sum(all, |r| r.genuine),
            false_detection: sum(all, |r| r.false_detection),
            unlabeled: sum(all, |r| r.unlabeled),
            precision: percent(precision(
                sum(confirmed, |r| r.genuine),
                sum(confirmed, |r| r.false_detection),
            )),
        });
    }
    lines
}

#[component]
pub async fn quality_section(
    lines: std::result::Result<Vec<QualityLine>, UiError>,
) -> Result<impl View> {
    let empty = lines.as_ref().is_ok_and(Vec::is_empty);
    Ok(view! {
        <section class=(format!("{SECTION} max-w-4xl"))>
            <h2 class=(SECTION_TITLE)>"Detection quality in this window"</h2>
            <p class="mb-2 text-xs text-zinc-500">"Transmissions opened in this window that the detector made a call on, by route and call: confirmed (by its strongest match), suspected or discarded. Precision is genuine over labelled, for confirmed evidence; unlabelled detections are not counted against it."</p>
            match lines {
                Err(error) => error_panel(error: &error),
                Ok(_) if empty => empty_state(message: "No judgeable transmissions in this window."),
                Ok(lines) => data_table(
                    headers: &["Route", "Detector call", "Genuine", "False detection", "Unlabelled", "Precision"],
                    for l in lines {
                        <tr class=(if l.route.is_none() { "bg-zinc-50 font-medium dark:bg-zinc-900" } else { ROW })>
                            <td class=(TD)>
                                match l.route {
                                    Some(route) => route_badge(kind: route),
                                    None => <span class="text-xs uppercase tracking-wide text-zinc-500">"total"</span>,
                                }
                            </td>
                            <td class=(TD)>(l.match_kind)</td>
                            <td class=(TD_NUM)>(l.genuine)</td>
                            <td class=(TD_NUM)>(l.false_detection)</td>
                            <td class=(TD_NUM)>(l.unlabeled)</td>
                            <td class=(TD_NUM)>(l.precision)</td>
                        </tr>
                    }
                ),
            }
        </section>
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(route_kind: RouteKind, match_kind: QualityMatch, g: u64, f: u64, u: u64) -> QualityRow {
        QualityRow {
            route_kind,
            match_kind,
            genuine: g,
            false_detection: f,
            unlabeled: u,
        }
    }

    #[test]
    fn precision_needs_labels() {
        assert_eq!(precision(0, 0), None);
        assert_eq!(precision(3, 1), Some(0.75));
    }

    #[test]
    fn lines_end_with_totals() {
        let rows = vec![
            row(
                RouteKind::Channel,
                QualityMatch::Content(MatchClass::Decoded),
                3,
                1,
                10,
            ),
            row(
                RouteKind::Direct,
                QualityMatch::Content(MatchClass::Exact),
                0,
                0,
                4,
            ),
        ];
        let lines = quality_lines(&rows);
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0].precision, "75%");
        assert_eq!(lines[0].match_kind, "decoded");
        assert_eq!(lines[1].precision, "—");
        assert_eq!(lines[2].route, None);
        assert_eq!((lines[2].genuine, lines[2].unlabeled), (3, 14));
        assert!(quality_lines(&[]).is_empty());
    }

    #[test]
    fn precision_reads_confirmed_rows_only() {
        let rows = vec![
            row(
                RouteKind::Channel,
                QualityMatch::Content(MatchClass::Exact),
                1,
                1,
                0,
            ),
            row(RouteKind::Channel, QualityMatch::Suspected, 0, 5, 2),
            row(RouteKind::Channel, QualityMatch::Discarded, 4, 0, 1),
        ];
        let lines = quality_lines(&rows);
        assert_eq!(lines[1].match_kind, "suspected (access only)");
        assert_eq!(
            (lines[1].precision.as_str(), lines[2].precision.as_str()),
            ("—", "—")
        );
        let total = &lines[3];
        assert_eq!(
            (total.genuine, total.false_detection, total.unlabeled),
            (5, 6, 3)
        );
        assert_eq!(total.precision, "50%", "over the confirmed row only");
    }
}
