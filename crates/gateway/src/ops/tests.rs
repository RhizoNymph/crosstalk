//! The ops listener's documents: the health JSON, readiness, metrics text.

use super::*;
use crate::normalize_failure::{FailureCounts, FailureReason, NormalizeFailure};
use crosstalk_spec::observed::exchange::WireProtocol;

/// The refusals behind `report()`'s `normalize_failed: 7`.
fn failures() -> FailureCounts {
    FailureCounts::default()
        .with(
            NormalizeFailure {
                reason: FailureReason::RequestBody,
                protocol: WireProtocol::AnthropicMessages,
            },
            5,
        )
        .with(
            NormalizeFailure {
                reason: FailureReason::UnsupportedProtocol,
                protocol: WireProtocol::OpenAiChat,
            },
            2,
        )
}

fn report() -> HealthReport {
    HealthReport {
        status: Phase::Ok,
        capture: CaptureReport {
            captured: 3,
            unclassified: 1,
            decode_error: 2,
            channel_full: 4,
            channel_closed: 5,
            response_too_large: 6,
            ids_exhausted: 13,
        },
        pipeline: PipelineCounts {
            published: 3,
            normalize_failed: 7,
            store_failed: 8,
            store_retries: 9,
            publish_failed: 10,
        },
        log: LogCounts {
            written: 3,
            duplicates: 11,
            write_failed: 12,
        },
        live: Some(LiveReport {
            stages: [
                ("l3-reconstruct".to_owned(), 4),
                ("l7-topology".to_owned(), 2),
            ]
            .into_iter()
            .collect(),
            watermark_micros: 1_790_845_200_000_000,
        }),
        bus: None,
        recovery: None,
        spool: None,
    }
}

/// The health report's JSON shape, pinned.
#[test]
fn health_report_json_is_pinned() {
    let text = serde_json::to_string(&report()).expect("encodes");
    assert_eq!(
        text,
        concat!(
            r#"{"status":"ok","#,
            r#""capture":{"captured":3,"unclassified":1,"decode_error":2,"channel_full":4,"channel_closed":5,"response_too_large":6,"ids_exhausted":13},"#,
            r#""pipeline":{"published":3,"normalize_failed":7,"store_failed":8,"store_retries":9,"publish_failed":10},"#,
            r#""log":{"written":3,"duplicates":11,"write_failed":12},"#,
            r#""live":{"stages":{"l3-reconstruct":4,"l7-topology":2},"watermark_micros":1790845200000000}}"#
        )
    );
    let decoded: HealthReport = serde_json::from_str(&text).expect("decodes");
    assert_eq!(decoded, report());
    let draining = HealthReport {
        status: Phase::Draining,
        ..report()
    };
    let text = serde_json::to_string(&draining).expect("encodes");
    assert!(text.starts_with(r#"{"status":"draining","#));
}

#[test]
fn health_report_decoding_is_strict() {
    let mut value = serde_json::to_value(report()).expect("encodes");
    for pointer in ["", "/capture", "/pipeline", "/log", "/live"] {
        let mut changed = value.clone();
        changed
            .pointer_mut(pointer)
            .and_then(serde_json::Value::as_object_mut)
            .expect("an object")
            .insert("surprise".to_owned(), serde_json::json!(0));
        assert!(
            serde_json::from_value::<HealthReport>(changed).is_err(),
            "{pointer}"
        );
    }
    value["status"] = serde_json::json!("sleeping");
    assert!(serde_json::from_value::<HealthReport>(value).is_err());
}

#[test]
fn metrics_are_prometheus_text_with_every_counter() {
    let text = metrics::render(&report(), &failures());
    for line in [
        "# TYPE crosstalk_capture_exchanges_total counter",
        "crosstalk_draining 0",
        "crosstalk_capture_exchanges_total 3",
        "crosstalk_capture_uncaptured_total{reason=\"unclassified\"} 1",
        "crosstalk_capture_uncaptured_total{reason=\"decode_error\"} 2",
        "crosstalk_capture_uncaptured_total{reason=\"channel_full\"} 4",
        "crosstalk_capture_uncaptured_total{reason=\"channel_closed\"} 5",
        "crosstalk_capture_uncaptured_total{reason=\"response_too_large\"} 6",
        "crosstalk_capture_uncaptured_total{reason=\"ids_exhausted\"} 13",
        "crosstalk_pipeline_exchanges_total{outcome=\"published\"} 3",
        "crosstalk_pipeline_exchanges_total{outcome=\"normalize_failed\",reason=\"request_body\",protocol=\"anthropic_messages\"} 5",
        "crosstalk_pipeline_exchanges_total{outcome=\"normalize_failed\",reason=\"unsupported_protocol\",protocol=\"open_ai_chat\"} 2",
        "crosstalk_pipeline_exchanges_total{outcome=\"normalize_failed\",reason=\"request_body\",protocol=\"gemini_generate\"} 0",
        "crosstalk_pipeline_exchanges_total{outcome=\"store_failed\"} 8",
        "crosstalk_pipeline_exchanges_total{outcome=\"publish_failed\"} 10",
        "crosstalk_pipeline_blob_put_retries_total 9",
        "crosstalk_exchange_log_deliveries_total{outcome=\"written\"} 3",
        "crosstalk_exchange_log_deliveries_total{outcome=\"duplicate\"} 11",
        "crosstalk_exchange_log_deliveries_total{outcome=\"write_failed\"} 12",
    ] {
        assert!(
            text.lines().any(|candidate| candidate == line),
            "missing {line:?} in\n{text}"
        );
    }
    for line in text.lines() {
        assert!(
            line.starts_with("# HELP ")
                || line.starts_with("# TYPE ")
                || line.starts_with("crosstalk_"),
            "not exposition format: {line:?}"
        );
    }
    let draining = HealthReport {
        status: Phase::Draining,
        ..report()
    };
    assert!(
        metrics::render(&draining, &failures())
            .lines()
            .any(|line| line == "crosstalk_draining 1")
    );
}

/// The `normalize_failed` outcome is only its reason and protocol series,
/// one per pair: summed by outcome they are the health report's total, and
/// no unlabelled series counts the refusals twice.
#[test]
fn normalize_failed_series_sum_to_the_total() {
    let text = metrics::render(&report(), &failures());
    let refusals: Vec<&str> = text
        .lines()
        .filter(|line| {
            line.starts_with("crosstalk_pipeline_exchanges_total{outcome=\"normalize_failed\"")
        })
        .collect();
    assert_eq!(refusals.len(), 10, "2 reasons by 5 protocols: {refusals:?}");
    let sum: u64 = refusals
        .iter()
        .map(|line| {
            line.rsplit(' ')
                .next()
                .and_then(|value| value.parse::<u64>().ok())
                .expect("a count")
        })
        .sum();
    assert_eq!(sum, report().pipeline.normalize_failed);
    assert!(
        refusals
            .iter()
            .all(|line| line.contains(",reason=\"") && line.contains(",protocol=\"")),
        "{refusals:?}"
    );
    for outcome in ["published", "store_failed", "publish_failed"] {
        let line = format!("crosstalk_pipeline_exchanges_total{{outcome=\"{outcome}\"}} ");
        assert_eq!(
            text.lines()
                .filter(|candidate| candidate.starts_with(&line))
                .count(),
            1,
            "{outcome} keeps its one series"
        );
    }
}

#[test]
fn readiness_needs_every_task_running_and_no_drain() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    runtime.block_on(async {
        let mut tasks = Tasks::new();
        let held = tasks.spawn("capture", std::future::pending::<()>());
        let (phase, phase_watch) = watch::channel(Phase::Ok);
        let ops = Ops {
            role: Role::All,
            phase: phase_watch,
            capture: None,
            pipeline: Arc::new(PipelineStats::new()),
            log: Arc::new(LogStats::new()),
            live: None,
            tasks,
            store: StoreProbe::not_configured(),
            postgres: None,
        };
        let ready = ops.readiness().await;
        assert!(ready.ready, "{ready:?}");
        assert_eq!(ready.database, "not_configured");
        assert_eq!(ready.role, "all");

        let _ = phase.send(Phase::Draining);
        assert!(!ops.readiness().await.ready);
        let _ = phase.send(Phase::Ok);

        held.abort();
        let _ = held.await;
        let not_ready = ops.readiness().await;
        assert!(!not_ready.ready);
        assert_eq!(
            not_ready.tasks,
            vec![TaskState {
                name: "capture".to_owned(),
                running: false
            }]
        );
    });
}
