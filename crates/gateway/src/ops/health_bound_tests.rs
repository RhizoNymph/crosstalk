//! `/healthz` never hangs on its bus read, and reports the pipeline's
//! statement and lock timeouts; no database needed.

use std::sync::Arc;
use std::time::Duration;

use crosstalk_spec::support::SystemClock;
use crosstalk_store::sqlx::postgres::PgPoolOptions;
use crosstalk_transport::{PgBus, PgBusConfig};
use tokio::net::TcpListener;
use tokio::sync::watch;

use super::tests::report;
use super::*;
use crate::live::recovery::{PipelineStatus, StatusReporter};

/// Timer and scheduling slack on a bound.
const SLACK: Duration = Duration::from_secs(2);

/// A "database" that accepts connections and never answers: every
/// statement on it stalls, as on a server that stopped answering.
async fn silent_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("binds");
    let address = listener.local_addr().expect("an address");
    let task = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((socket, _)) = listener.accept().await {
            held.push(socket);
        }
    });
    (address, task)
}

fn ops_over(bus: PgBus) -> Ops {
    let (_, phase) = watch::channel(Phase::Ok);
    Ops {
        role: Role::Pipeline,
        phase,
        capture: None,
        pipeline: Arc::new(PipelineStats::new()),
        log: Arc::new(LogStats::new()),
        live: None,
        tasks: Tasks::new(),
        store: StoreProbe::not_configured(),
        postgres: Some(PgOps {
            status: StatusReporter::new(PipelineStatus::waiting()).reader(),
            bus,
            spool: None,
            live: Arc::new(LateValue::default()),
            clock: Arc::new(SystemClock),
        }),
    }
}

/// The bus read waits on a server that never answers (the pool's own
/// acquire timeout is far longer); `/healthz` answers after
/// [`HEALTH_BUS_BOUND`] with the bus section unavailable, the rest of the
/// report intact.
#[tokio::test(flavor = "multi_thread")]
async fn healthz_answers_within_its_bound_while_the_bus_read_stalls() {
    let (address, server) = silent_server().await;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(600))
        .connect_lazy(&format!("postgres://nobody@{address}/none?sslmode=disable"))
        .expect("a lazy pool");
    let bus = PgBus::publisher(pool, Arc::new(SystemClock), PgBusConfig::default()).expect("a bus");
    let ops = ops_over(bus);
    let began = tokio::time::Instant::now();
    let report = tokio::time::timeout(HEALTH_BUS_BOUND * 5, ops.health_full())
        .await
        .expect("/healthz never hangs on its bus read");
    let took = began.elapsed();
    assert!(took >= HEALTH_BUS_BOUND, "waited its bound: {took:?}");
    assert!(
        took < HEALTH_BUS_BOUND + SLACK,
        "answered at its bound: {took:?}"
    );
    assert_eq!(
        report.bus,
        Some(BusSection::Unavailable {
            unavailable: BusUnavailable::TimedOut {
                bound_ms: u64::try_from(HEALTH_BUS_BOUND.as_millis()).expect("fits"),
            }
        })
    );
    assert!(report.recovery.is_some(), "the rest of the report is there");
    server.abort();
}

/// The bound is short: a request path, read by probes and dashboards.
#[test]
fn the_health_bound_is_a_few_seconds() {
    assert!(HEALTH_BUS_BOUND <= Duration::from_secs(5));
    assert!(HEALTH_BUS_BOUND >= Duration::from_millis(500));
}

/// The bus section's JSON: the groups as before when read, and why not
/// when unavailable; both decode strictly.
#[test]
fn the_bus_section_reads_groups_or_why_not() {
    let read = HealthReport {
        bus: Some(BusSection::Read(BusReport {
            groups: [(
                "live-l3-reconstruct".to_owned(),
                GroupReport {
                    pending: 2,
                    oldest_pending_micros: Some(5),
                    dead_letters: 1,
                },
            )]
            .into_iter()
            .collect(),
        })),
        ..report()
    };
    let json = serde_json::to_value(&read).expect("encodes");
    assert_eq!(
        json["bus"],
        serde_json::json!({"groups": {"live-l3-reconstruct": {
            "pending": 2, "oldest_pending_micros": 5, "dead_letters": 1}}})
    );
    assert_eq!(
        serde_json::from_value::<HealthReport>(json).expect("decodes"),
        read
    );

    for (unavailable, expected) in [
        (
            BusUnavailable::TimedOut { bound_ms: 2_000 },
            serde_json::json!({"unavailable": {"timed_out": {"bound_ms": 2_000}}}),
        ),
        (
            BusUnavailable::Failed {
                error: "Disconnected".to_owned(),
            },
            serde_json::json!({"unavailable": {"failed": {"error": "Disconnected"}}}),
        ),
    ] {
        let report = HealthReport {
            bus: Some(BusSection::Unavailable { unavailable }),
            ..report()
        };
        let json = serde_json::to_value(&report).expect("encodes");
        assert_eq!(json["bus"], expected);
        assert_eq!(
            serde_json::from_value::<HealthReport>(json).expect("decodes"),
            report
        );
    }

    let mut surprise = serde_json::to_value(&read).expect("encodes");
    surprise["bus"]["surprise"] = serde_json::json!(0);
    assert!(serde_json::from_value::<HealthReport>(surprise).is_err());
}

/// `/healthz` carries the statements the server cancelled at a pipeline
/// bound, by kind, and `/metrics` the counter
/// `crosstalk_pg_statement_timeouts_total{kind}`.
#[test]
fn statement_timeouts_are_in_health_and_metrics() {
    let with = HealthReport {
        statement_timeouts: Some(StatementTimeoutsReport {
            statement: 3,
            lock: 7,
        }),
        ..report()
    };
    let json = serde_json::to_value(&with).expect("encodes");
    assert_eq!(
        json["statement_timeouts"],
        serde_json::json!({"statement": 3, "lock": 7})
    );
    assert_eq!(
        serde_json::from_value::<HealthReport>(json).expect("decodes"),
        with
    );
    let text = metrics::render(&with, &crate::normalize_failure::FailureCounts::default());
    for line in [
        "# TYPE crosstalk_pg_statement_timeouts_total counter",
        "crosstalk_pg_statement_timeouts_total{kind=\"statement\"} 3",
        "crosstalk_pg_statement_timeouts_total{kind=\"lock\"} 7",
    ] {
        assert!(
            text.lines().any(|candidate| candidate == line),
            "missing {line:?} in\n{text}"
        );
    }
    let without = metrics::render(
        &report(),
        &crate::normalize_failure::FailureCounts::default(),
    );
    assert!(
        !without.contains("crosstalk_pg_statement_timeouts_total"),
        "memory mode has no such series"
    );
}

/// In Postgres mode the report reads the process's counts.
#[tokio::test]
async fn health_reads_the_process_statement_timeouts_in_postgres_mode() {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://nobody@127.0.0.1:1/none")
        .expect("a lazy pool");
    let bus = PgBus::publisher(pool, Arc::new(SystemClock), PgBusConfig::default()).expect("a bus");
    let counted = crosstalk_store::statement_timeouts();
    let health = ops_over(bus).health();
    let reported = health
        .statement_timeouts
        .expect("Postgres mode reports them");
    assert!(reported.statement >= counted.statement);
    assert!(reported.lock >= counted.lock);
}
