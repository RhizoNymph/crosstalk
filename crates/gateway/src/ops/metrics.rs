//! `GET /metrics`: the capture counters in the Prometheus text exposition
//! format (version 0.0.4). Written by hand: a few counters do not need a
//! metrics library. Full observability comes later.
//!
//! In Postgres mode (a report with a `spool` section) the publish spool's
//! series follow: `crosstalk_spool_state{state}` (one series is 1),
//! `crosstalk_spool_bytes`, `crosstalk_spool_records`,
//! `crosstalk_spool_oldest_age_seconds`, `crosstalk_spool_appended_total`,
//! `crosstalk_spool_drained_total`, `crosstalk_spool_rejected_total{reason}`
//! (`spool_full` or `spool_io`, the values crosstalk-infra's dashboard and
//! alerts select on)
//! and `crosstalk_spool_truncated_bytes_total`.

use std::fmt::Write as _;

use super::{HealthReport, Phase};
use crate::normalize_failure::{FailureCounts, protocol_code};

/// The exposition format's content type.
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Render `report` as Prometheus text. The `normalize_failed` outcome is
/// one series per reason and protocol, from `failures`, every pair present
/// (zeros included) and no unlabelled total beside them, so `sum by
/// (outcome)` is the refusal total.
pub fn render(report: &HealthReport, failures: &FailureCounts) -> String {
    let mut out = String::new();
    let draining = u64::from(report.status == Phase::Draining);
    gauge(
        &mut out,
        "crosstalk_draining",
        "1 once the gateway has begun shutting down.",
        draining,
    );
    let capture = &report.capture;
    let spool_full = report
        .spool
        .as_ref()
        .map_or(0, |spool| spool.capture_spool_full);
    counter(
        &mut out,
        "crosstalk_capture_exchanges_total",
        "Generation exchanges the proxy handed to the capture stage.",
        &[(&[], capture.captured)],
    );
    counter(
        &mut out,
        "crosstalk_capture_uncaptured_total",
        "Requests the proxy forwarded without capturing, by reason.",
        &[
            (&[("reason", "unclassified")], capture.unclassified),
            (&[("reason", "decode_error")], capture.decode_error),
            (&[("reason", "channel_full")], capture.channel_full),
            (&[("reason", "channel_closed")], capture.channel_closed),
            (
                &[("reason", "response_too_large")],
                capture.response_too_large,
            ),
            (&[("reason", "ids_exhausted")], capture.ids_exhausted),
            (&[("reason", "spool_full")], spool_full),
        ],
    );
    let pipeline = &report.pipeline;
    let refusals: Vec<[(&str, &str); 3]> = failures
        .iter()
        .map(|(failure, _)| {
            [
                ("outcome", "normalize_failed"),
                ("reason", failure.reason.code()),
                ("protocol", protocol_code(failure.protocol)),
            ]
        })
        .collect();
    let published: [(&str, &str); 1] = [("outcome", "published")];
    let mut samples: Vec<(&[(&str, &str)], u64)> = vec![(&published, pipeline.published)];
    samples.extend(
        refusals
            .iter()
            .zip(failures.iter())
            .map(|(labels, (_, count))| (labels.as_slice(), count)),
    );
    samples.push((&[("outcome", "store_failed")], pipeline.store_failed));
    samples.push((&[("outcome", "publish_failed")], pipeline.publish_failed));
    samples.push((&[("outcome", "spool_full")], spool_full));
    counter(
        &mut out,
        "crosstalk_pipeline_exchanges_total",
        "Exchanges the capture stage finished, by outcome; refusals also by reason and protocol.",
        &samples,
    );
    counter(
        &mut out,
        "crosstalk_pipeline_blob_put_retries_total",
        "Blob put attempts that failed and were retried.",
        &[(&[], pipeline.store_retries)],
    );
    let log = &report.log;
    counter(
        &mut out,
        "crosstalk_exchange_log_deliveries_total",
        "ExchangeCaptured deliveries the exchange log handled, by outcome.",
        &[
            (&[("outcome", "written")], log.written),
            (&[("outcome", "duplicate")], log.duplicates),
            (&[("outcome", "write_failed")], log.write_failed),
        ],
    );
    if let Some(spool) = &report.spool {
        spool_series(&mut out, spool);
    }
    out
}

/// The publish spool's series.
fn spool_series(out: &mut String, spool: &super::SpoolReport) {
    let _ = writeln!(
        out,
        "# HELP crosstalk_spool_state Where publishes go: 1 for the spool's current state."
    );
    let _ = writeln!(out, "# TYPE crosstalk_spool_state gauge");
    for state in ["direct", "spooling", "draining", "corrupt"] {
        let value = u64::from(spool.state == state);
        let _ = writeln!(out, "crosstalk_spool_state{{state=\"{state}\"}} {value}");
    }
    gauge(
        out,
        "crosstalk_spool_bytes",
        "Bytes the spool's segment files hold.",
        spool.bytes,
    );
    gauge(
        out,
        "crosstalk_spool_records",
        "Records not yet sent to the bus.",
        spool.records,
    );
    gauge(
        out,
        "crosstalk_spool_oldest_age_seconds",
        "Age of the oldest unsent record, by the clock.",
        spool.oldest_age_seconds,
    );
    counter(
        out,
        "crosstalk_spool_appended_total",
        "Records appended since the spool opened.",
        &[(&[], spool.appended)],
    );
    counter(
        out,
        "crosstalk_spool_drained_total",
        "Records sent to the bus since the spool opened.",
        &[(&[], spool.drained)],
    );
    counter(
        out,
        "crosstalk_spool_rejected_total",
        "Appends refused, by reason.",
        &[
            (&[("reason", "spool_full")], spool.rejected_full),
            (&[("reason", "spool_io")], spool.rejected_io),
        ],
    );
    counter(
        out,
        "crosstalk_spool_truncated_bytes_total",
        "Bytes of torn appends truncated when the spool opened.",
        &[(&[], spool.truncated_bytes)],
    );
}

fn gauge(out: &mut String, name: &str, help: &str, value: u64) {
    // Writing to a String cannot fail.
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} gauge");
    let _ = writeln!(out, "{name} {value}");
}

/// A counter's samples, each with its labels (none for an unlabelled
/// sample). Label values are fixed codes, so none needs escaping.
fn counter(out: &mut String, name: &str, help: &str, samples: &[(&[(&str, &str)], u64)]) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} counter");
    for (labels, value) in samples {
        if labels.is_empty() {
            let _ = writeln!(out, "{name} {value}");
            continue;
        }
        let _ = write!(out, "{name}{{");
        for (at, (key, label)) in labels.iter().enumerate() {
            let comma = if at == 0 { "" } else { "," };
            let _ = write!(out, "{comma}{key}=\"{label}\"");
        }
        let _ = writeln!(out, "}} {value}");
    }
}
