//! The human-readable report: a fixed-width table.

use std::fmt::Write;

use serde::Serialize;

use super::{Report, ReportRow};
use crate::report::gates::GateStatus;

/// A value's snake_case serde name (`user_turn`), for table cells.
fn name<T: Serialize>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::String(text)) => text,
        Ok(other) => other.to_string(),
        Err(_) => "?".to_owned(),
    }
}

fn rate(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_owned(), |v| format!("{:.3}", v))
}

const HEADER: [&str; 14] = [
    "route",
    "carrier",
    "class",
    "tier",
    "expected",
    "found",
    "missed",
    "recall",
    "predicted",
    "correct",
    "false",
    "unjudged",
    "dismissed",
    "precision",
];

fn cells(row: &ReportRow) -> [String; 14] {
    let c = &row.counts;
    [
        name(&row.key.route),
        name(&row.key.carrier),
        name(&row.key.class),
        row.key.tier.as_ref().map_or_else(|| "-".to_owned(), name),
        c.expected.to_string(),
        c.found.to_string(),
        c.missed.to_string(),
        rate(row.recall),
        c.predicted.to_string(),
        c.correct.to_string(),
        c.false_positive.to_string(),
        c.unjudged.to_string(),
        c.dismissed.to_string(),
        rate(row.precision),
    ]
}

/// The report as text: totals, the row table, violations, gates, failures.
pub fn render(report: &Report) -> String {
    let mut out = String::new();
    let t = &report.totals;
    let _ = writeln!(
        out,
        "dataset {} · detector {} · {} worlds, {} agents, {} exchanges, {} labels, {} negative controls, {} predictions",
        report.dataset,
        report.detector,
        t.worlds,
        t.agents,
        t.exchanges,
        t.expectations,
        t.negative_controls,
        t.predictions
    );
    if report.unscored.worlds > 0 {
        let _ = writeln!(
            out,
            "{}: {} exchanges from {} worlds ingested; no detector consumers yet, so those worlds are not scored",
            report.detector, report.unscored.ingested, report.unscored.worlds
        );
    }
    let o = &report.overall;
    let _ = writeln!(
        out,
        "overall: recall {} ({} / {}), precision {} ({} correct, {} false, {} unjudged)\n",
        rate(o.recall),
        o.counts.found,
        o.counts.expected,
        rate(o.precision),
        o.counts.correct,
        o.counts.false_positive,
        o.counts.unjudged
    );
    let access = &report.access_only;
    if access.labels > 0 {
        let _ = writeln!(
            out,
            "access-only recall (suspected or discarded only, not in overall): {} ({} / {})\n",
            rate(access.recall),
            access.labels,
            access.expected
        );
    }
    let reach = &report.out_of_reach;
    if reach.counts.expected > 0 || reach.counts.predicted > 0 {
        let _ = writeln!(
            out,
            "out of reach (missed by design, not in overall): found {} / {}, {} predictions\n",
            reach.counts.found, reach.counts.expected, reach.counts.predicted
        );
    }
    let forwarding = &report.forwarding;
    if forwarding.counts.expected > 0 || forwarding.counts.predicted > 0 {
        let _ = writeln!(
            out,
            "forwarding (sender relayed its own tool output, not in overall): recall {} ({} / {}), {} predictions\n",
            rate(forwarding.recall),
            forwarding.counts.found,
            forwarding.counts.expected,
            forwarding.counts.predicted
        );
    }
    let rows: Vec<[String; 14]> = report.rows.iter().map(cells).collect();
    let mut widths: Vec<usize> = HEADER.iter().map(|h| h.len()).collect();
    for row in &rows {
        for (width, cell) in widths.iter_mut().zip(row.iter()) {
            *width = (*width).max(cell.len());
        }
    }
    let line = |cells: &[String]| {
        cells
            .iter()
            .zip(&widths)
            .enumerate()
            .map(|(at, (cell, width))| {
                if at < 4 {
                    format!("{cell:<width$}")
                } else {
                    format!("{cell:>width$}")
                }
            })
            .collect::<Vec<_>>()
            .join("  ")
    };
    let header: Vec<String> = HEADER.iter().map(|h| (*h).to_owned()).collect();
    let _ = writeln!(out, "{}", line(&header));
    for row in &rows {
        let _ = writeln!(out, "{}", line(row));
    }
    if !report.violations.is_empty() {
        let _ = writeln!(out, "\nnegative-control violations:");
        for row in &report.violations {
            let _ = writeln!(out, "  {:<20} {}", name(&row.reason), row.count);
        }
    }
    if let Some(background) = &report.background {
        let _ = writeln!(
            out,
            "\nfalse positives: {:.1} per 1k exchanges ({} over {})",
            background.per_1k_exchanges, background.false_positives, background.exchanges
        );
        if !background.sources.is_empty() {
            let _ = writeln!(out, "top boilerplate sources:");
            for source in &background.sources {
                let _ = writeln!(
                    out,
                    "  {:>6}  {:<14} {}",
                    source.count,
                    name(&source.reason),
                    source.text
                );
            }
        }
    }
    if !report.gates.is_empty() {
        let _ = writeln!(out, "\ngates:");
        for gate in &report.gates {
            let status = match &gate.status {
                GateStatus::Pass { value } => format!("pass  {value:.3}"),
                GateStatus::Fail { value, bound } => format!("FAIL  {value:.3} (bound {bound:.3})"),
                GateStatus::Skipped => "skip  (no data)".to_owned(),
                GateStatus::OtherDataset => "skip  (other dataset)".to_owned(),
            };
            let _ = writeln!(out, "  {status:<30} {}", gate.name);
        }
    }
    if !report.failures.is_empty() {
        let _ = writeln!(out, "\n{} worlds failed:", report.failures.len());
        for failure in &report.failures {
            let _ = writeln!(out, "  {failure}");
        }
    }
    out
}
