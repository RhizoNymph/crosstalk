//! A planned handover on the ops listener: `/readyz` is not ready and
//! names the step, `/healthz` carries it in `recovery.handover`; no
//! database needed.

use std::sync::Arc;

use crosstalk_spec::support::SystemClock;
use crosstalk_store::sqlx::postgres::PgPoolOptions;
use crosstalk_transport::{PgBus, PgBusConfig};
use tokio::sync::watch;

use super::*;
use crate::live::recovery::{
    HandoverStep, LockState, MigrationState, PipelinePhase, PipelineStatus, StatusReporter,
};

/// The ops listener of a process whose pipeline ran, now draining (the
/// shutdown began), its status written by `status`.
fn draining_ops(status: &StatusReporter) -> (Ops, watch::Sender<Phase>) {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://nobody@127.0.0.1:1/none")
        .expect("a lazy pool");
    let bus = PgBus::publisher(pool, Arc::new(SystemClock), PgBusConfig::default()).expect("a bus");
    let (phase_tx, phase) = watch::channel(Phase::Ok);
    let ops = Ops {
        role: Role::Pipeline,
        phase,
        capture: None,
        pipeline: Arc::new(PipelineStats::new()),
        log: Arc::new(LogStats::new()),
        live: None,
        tasks: Tasks::new(),
        store: StoreProbe::not_configured(),
        postgres: Some(PgOps {
            status: status.reader(),
            bus,
            spool: None,
            live: Arc::new(LateValue::default()),
            clock: Arc::new(SystemClock),
        }),
    };
    (ops, phase_tx)
}

/// Each step of the handover: `/readyz` 503-worthy (not ready, status
/// `draining`), `pipeline: handing_over`, `handover: <step>`, the lock held
/// until released; `/healthz` `recovery.handover` the same step. At the
/// end: `handed_over`, `done`, `released`.
#[tokio::test]
async fn readyz_and_healthz_name_the_handover_step() {
    let mut running = PipelineStatus::waiting();
    running.migrations = MigrationState::AtHead;
    running.lock = LockState::Held;
    running.phase = PipelinePhase::Running;
    let status = StatusReporter::new(running);
    let (ops, phase) = draining_ops(&status);
    let ready = ops.readiness().await;
    assert!(ready.ready, "running: ready {ready:?}");
    assert_eq!(ready.handover, None);
    assert!(
        !serde_json::to_string(&ready)
            .expect("encodes")
            .contains("handover"),
        "absent before a handover"
    );

    phase.send_replace(Phase::Draining);
    for step in HandoverStep::ALL {
        status.update(|status| {
            status.phase = PipelinePhase::HandingOver(step);
            status.report.handover = Some(step);
            if step == HandoverStep::Done {
                status.lock = LockState::Released;
            }
        });
        let ready = ops.readiness().await;
        assert!(!ready.ready, "{step:?}: {ready:?}");
        assert_eq!(ready.status, Phase::Draining);
        assert_eq!(ready.handover.as_deref(), Some(step.label()));
        let (pipeline, lock) = match step {
            HandoverStep::Done => ("handed_over", "released"),
            _ => ("handing_over", "held"),
        };
        assert_eq!(ready.pipeline.as_deref(), Some(pipeline), "{step:?}");
        assert_eq!(ready.pipeline_lock.as_deref(), Some(lock), "{step:?}");
        assert_eq!(ready.recovery.as_deref(), Some("done"));
        let json = serde_json::to_value(&ready).expect("encodes");
        assert_eq!(json["handover"], step.label());
        let decoded: Readiness = serde_json::from_value(json).expect("decodes");
        assert_eq!(decoded, ready);

        let health = ops.health();
        assert_eq!(health.status, Phase::Draining);
        let recovery = health.recovery.expect("postgres mode");
        assert_eq!(recovery.handover, Some(step));
        let json = serde_json::to_value(&health).expect("encodes");
        assert_eq!(json["recovery"]["handover"], step.label());
    }
}
