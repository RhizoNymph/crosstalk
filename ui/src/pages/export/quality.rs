//! Detection quality for the window: labelled and unlabelled detections
//! per route kind and match kind, with precision where verdicts exist.

use topcoat::Result;
use topcoat::view::{View, component, view};

use crate::components::form::{SECTION, SECTION_TITLE};
use crate::components::table::{ROW, TD, TD_NUM};
use crate::components::{data_table, empty_state, error_panel, route_badge};
use crate::contract::errors::QueryError;
use crate::contract::research::{MatchKindName, QualityRow};
use crosstalk_spec::aggregates::edge::RouteKind;

pub fn match_kind_name(kind: MatchKindName) -> &'static str {
    match kind {
        MatchKindName::Exact => "exact",
        MatchKindName::Normalized => "normalized",
        MatchKindName::Decoded => "decoded",
        MatchKindName::Semantic => "semantic",
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
    pub precision: String,
}

fn line(route: Option<RouteKind>, kind: String, g: u64, f: u64, u: u64) -> QualityLine {
    QualityLine {
        route,
        match_kind: kind,
        genuine: g,
        false_detection: f,
        unlabeled: u,
        precision: precision(g, f).map_or_else(|| "—".to_owned(), |p| format!("{:.0}%", p * 100.0)),
    }
}

/// One line per row, then the totals.
pub fn quality_lines(rows: &[QualityRow]) -> Vec<QualityLine> {
    let mut lines: Vec<QualityLine> = rows
        .iter()
        .map(|r| {
            line(
                Some(r.route),
                match_kind_name(r.match_kind).to_owned(),
                r.genuine,
                r.false_detection,
                r.unlabeled,
            )
        })
        .collect();
    if !rows.is_empty() {
        let sum = |f: fn(&QualityRow) -> u64| rows.iter().map(f).sum::<u64>();
        lines.push(line(
            None,
            "all".to_owned(),
            sum(|r| r.genuine),
            sum(|r| r.false_detection),
            sum(|r| r.unlabeled),
        ));
    }
    lines
}

#[component]
pub async fn quality_section(
    lines: std::result::Result<Vec<QualityLine>, QueryError>,
) -> Result<impl View> {
    let empty = lines.as_ref().is_ok_and(Vec::is_empty);
    Ok(view! {
        <section class=(format!("{SECTION} max-w-4xl"))>
            <h2 class=(SECTION_TITLE)>"Detection quality in this window"</h2>
            <p class="mb-2 text-xs text-zinc-500">"Confirmed transmissions by route and match kind. Precision is genuine over labelled; unlabelled detections are not counted against it."</p>
            match lines {
                Err(error) => error_panel(error: &error),
                Ok(_) if empty => empty_state(message: "No confirmed transmissions in this window."),
                Ok(lines) => data_table(
                    headers: &["Route", "Match kind", "Genuine", "False detection", "Unlabelled", "Precision"],
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

    #[test]
    fn precision_needs_labels() {
        assert_eq!(precision(0, 0), None);
        assert_eq!(precision(3, 1), Some(0.75));
    }

    #[test]
    fn lines_end_with_totals() {
        let rows = vec![
            QualityRow {
                route: RouteKind::Channel,
                match_kind: MatchKindName::Decoded,
                genuine: 3,
                false_detection: 1,
                unlabeled: 10,
            },
            QualityRow {
                route: RouteKind::Direct,
                match_kind: MatchKindName::Exact,
                genuine: 0,
                false_detection: 0,
                unlabeled: 4,
            },
        ];
        let lines = quality_lines(&rows);
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0].precision, "75%");
        assert_eq!(lines[1].precision, "—");
        assert_eq!(lines[2].route, None);
        assert_eq!((lines[2].genuine, lines[2].unlabeled), (3, 14));
        assert!(quality_lines(&[]).is_empty());
    }
}
