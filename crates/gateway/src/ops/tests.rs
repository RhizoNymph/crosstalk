//! The ops listener's documents: the health JSON, readiness, metrics text.

use super::*;

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
            r#""capture":{"captured":3,"unclassified":1,"decode_error":2,"channel_full":4,"channel_closed":5,"response_too_large":6},"#,
            r#""pipeline":{"published":3,"normalize_failed":7,"store_failed":8,"store_retries":9,"publish_failed":10},"#,
            r#""log":{"written":3,"duplicates":11,"write_failed":12}}"#
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
    for pointer in ["", "/capture", "/pipeline", "/log"] {
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
    let text = metrics::render(&report());
    for line in [
        "# TYPE crosstalk_capture_exchanges_total counter",
        "crosstalk_draining 0",
        "crosstalk_capture_exchanges_total 3",
        "crosstalk_capture_uncaptured_total{reason=\"unclassified\"} 1",
        "crosstalk_capture_uncaptured_total{reason=\"decode_error\"} 2",
        "crosstalk_capture_uncaptured_total{reason=\"channel_full\"} 4",
        "crosstalk_capture_uncaptured_total{reason=\"channel_closed\"} 5",
        "crosstalk_capture_uncaptured_total{reason=\"response_too_large\"} 6",
        "crosstalk_pipeline_exchanges_total{outcome=\"published\"} 3",
        "crosstalk_pipeline_exchanges_total{outcome=\"normalize_failed\"} 7",
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
        metrics::render(&draining)
            .lines()
            .any(|line| line == "crosstalk_draining 1")
    );
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
            tasks,
            store: StoreProbe::not_configured(),
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
