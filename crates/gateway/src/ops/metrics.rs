//! `GET /metrics`: the capture counters in the Prometheus text exposition
//! format (version 0.0.4). Written by hand: a few counters do not need a
//! metrics library. Full observability comes later.

use std::fmt::Write as _;

use super::{HealthReport, Phase};

/// The exposition format's content type.
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Render `report` as Prometheus text.
pub fn render(report: &HealthReport) -> String {
    let mut out = String::new();
    let draining = u64::from(report.status == Phase::Draining);
    gauge(
        &mut out,
        "crosstalk_draining",
        "1 once the gateway has begun shutting down.",
        draining,
    );
    let capture = &report.capture;
    counter(
        &mut out,
        "crosstalk_capture_exchanges_total",
        "Generation exchanges the proxy handed to the capture stage.",
        &[(None, capture.captured)],
    );
    counter(
        &mut out,
        "crosstalk_capture_uncaptured_total",
        "Requests the proxy forwarded without capturing, by reason.",
        &[
            (Some(("reason", "unclassified")), capture.unclassified),
            (Some(("reason", "decode_error")), capture.decode_error),
            (Some(("reason", "channel_full")), capture.channel_full),
            (Some(("reason", "channel_closed")), capture.channel_closed),
            (
                Some(("reason", "response_too_large")),
                capture.response_too_large,
            ),
        ],
    );
    let pipeline = &report.pipeline;
    counter(
        &mut out,
        "crosstalk_pipeline_exchanges_total",
        "Exchanges the capture stage finished, by outcome.",
        &[
            (Some(("outcome", "published")), pipeline.published),
            (
                Some(("outcome", "normalize_failed")),
                pipeline.normalize_failed,
            ),
            (Some(("outcome", "store_failed")), pipeline.store_failed),
            (Some(("outcome", "publish_failed")), pipeline.publish_failed),
        ],
    );
    counter(
        &mut out,
        "crosstalk_pipeline_blob_put_retries_total",
        "Blob put attempts that failed and were retried.",
        &[(None, pipeline.store_retries)],
    );
    let log = &report.log;
    counter(
        &mut out,
        "crosstalk_exchange_log_deliveries_total",
        "ExchangeCaptured deliveries the exchange log handled, by outcome.",
        &[
            (Some(("outcome", "written")), log.written),
            (Some(("outcome", "duplicate")), log.duplicates),
            (Some(("outcome", "write_failed")), log.write_failed),
        ],
    );
    out
}

fn gauge(out: &mut String, name: &str, help: &str, value: u64) {
    // Writing to a String cannot fail.
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} gauge");
    let _ = writeln!(out, "{name} {value}");
}

fn counter(out: &mut String, name: &str, help: &str, samples: &[(Option<(&str, &str)>, u64)]) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} counter");
    for (label, value) in samples {
        match label {
            Some((key, label)) => {
                let _ = writeln!(out, "{name}{{{key}=\"{label}\"}} {value}");
            }
            None => {
                let _ = writeln!(out, "{name} {value}");
            }
        }
    }
}
