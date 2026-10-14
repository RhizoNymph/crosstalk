//! What the lock-loss tests share: one test at a time, the uninterrupted
//! run's answers computed once, a connect prober and a `/readyz` sampler
//! running beside the traffic, waits on the pipeline status, and the
//! server's view of the gateway's sessions.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use crosstalk_e2e::answers::Answers;
use crosstalk_e2e::scenario::Scenario;
use crosstalk_gateway::gateway::Running;
use crosstalk_gateway::live::recovery::PipelineStatus;
use crosstalk_gateway::store::lock::PIPELINE_LOCK;
use crosstalk_store::TestDb;
use crosstalk_store::sqlx;
use crosstalk_testkit::client::HarnessClient;
use crosstalk_testkit::corpus::{CorpusRequest, Headers};
use hyper::Method;
use hyper::http::uri::PathAndQuery;
use serde_json::Value;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::common::gateway::{sessions, state, uninterrupted};

/// The longest the live process took to stop after a loss before resume
/// (5 s); the abandon now takes about 1 s, so waiting past the loss's
/// report plus this covers both.
pub const LOST_SHUTDOWN_MAX: Duration = Duration::from_secs(5);

/// How long the pipeline may take to run again once the database answers
/// and the lock is free: a probe's retry, the lock, the recovery.
pub const RESUME: Duration = Duration::from_secs(90);

/// How long the drain after a heal may take. Holds the abandoned
/// incarnation left must be ready at once (`recover_held`); left to the
/// stale-hold sweep they would wait about 65 s.
pub const DRAIN_AFTER_HEAL: Duration = Duration::from_secs(45);

/// Scheduling slack on a bound.
pub const SLACK: Duration = Duration::from_secs(2);

static SERIAL: Mutex<()> = Mutex::new(());
static EXPECTED: Mutex<Option<[Answers; 2]>> = Mutex::new(None);

/// One lock-loss test at a time (a failed one poisons nothing).
pub fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The uninterrupted gateway run's answers, computed by the first test
/// that needs them; `None` (skipped) without a test database.
pub async fn expected(test: &str, scenario: &Scenario) -> Option<[Answers; 2]> {
    if let Some(answers) = EXPECTED
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone()
    {
        return Some(answers);
    }
    let answers = uninterrupted(&format!("{test}_uninterrupted"), scenario).await?;
    *EXPECTED.lock().unwrap_or_else(PoisonError::into_inner) = Some(answers.clone());
    Some(answers)
}

/// What the connect prober saw.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Connects {
    pub attempts: u64,
    pub refused: u64,
    /// Any other connect failure (a timeout, a reset).
    pub failed: u64,
}

/// Connects to the proxy every few milliseconds and counts refusals: a
/// listener that closed (a process exiting, a restart) refuses.
pub struct Prober {
    stop: watch::Sender<bool>,
    task: JoinHandle<Connects>,
}

impl Prober {
    pub fn start(addr: SocketAddr) -> Self {
        let (stop, mut stopped) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut seen = Connects::default();
            loop {
                if *stopped.borrow_and_update() {
                    return seen;
                }
                seen.attempts += 1;
                match tokio::time::timeout(
                    Duration::from_secs(2),
                    tokio::net::TcpStream::connect(addr),
                )
                .await
                {
                    Ok(Ok(stream)) => drop(stream),
                    Ok(Err(error)) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                        seen.refused += 1;
                    }
                    Ok(Err(_)) | Err(_) => seen.failed += 1,
                }
                tokio::select! {
                    _ = stopped.changed() => {}
                    () = tokio::time::sleep(Duration::from_millis(25)) => {}
                }
            }
        });
        Self { stop, task }
    }

    pub async fn finish(self) -> Connects {
        let _ = self.stop.send(true);
        self.task.await.expect("the prober's task")
    }
}

/// What the `/readyz` sampler saw.
#[derive(Debug, Clone, Default)]
pub struct Samples {
    pub taken: u64,
    /// Answers other than 200, with their bodies.
    pub not_ok: Vec<String>,
    /// Requests the ops listener did not answer.
    pub unanswered: u64,
    /// Each `pipeline_lock` / `pipeline` pair seen, in order, repeats
    /// collapsed.
    pub path: Vec<String>,
}

/// Reads `/readyz` every 100 ms.
pub struct Sampler {
    stop: watch::Sender<bool>,
    task: JoinHandle<Samples>,
}

impl Sampler {
    pub fn start(ops: SocketAddr) -> Self {
        let (stop, mut stopped) = watch::channel(false);
        let task = tokio::spawn(async move {
            let client = HarnessClient::new(ops);
            let request = CorpusRequest {
                method: Method::GET,
                target: PathAndQuery::from_static("/readyz"),
                headers: Headers::new(),
                body: Bytes::new(),
            };
            let mut samples = Samples::default();
            loop {
                if *stopped.borrow_and_update() {
                    return samples;
                }
                samples.taken += 1;
                match client.send(&request).await {
                    Ok(response) => {
                        let body: Value =
                            serde_json::from_slice(&response.body).unwrap_or(Value::Null);
                        if response.status != hyper::StatusCode::OK {
                            samples.not_ok.push(format!("{}: {body}", response.status));
                        }
                        let step = format!(
                            "{} / {}",
                            body["pipeline_lock"].as_str().unwrap_or("?"),
                            body["pipeline"].as_str().unwrap_or("?")
                        );
                        if samples.path.last() != Some(&step) {
                            samples.path.push(step);
                        }
                    }
                    Err(_) => samples.unanswered += 1,
                }
                tokio::select! {
                    _ = stopped.changed() => {}
                    () = tokio::time::sleep(Duration::from_millis(100)) => {}
                }
            }
        });
        Self { stop, task }
    }

    pub async fn finish(self) -> Samples {
        let _ = self.stop.send(true);
        self.task.await.expect("the sampler's task")
    }
}

/// Wait up to `bound` until the pipeline status satisfies `ready`, or
/// fail with the gateway's state.
pub async fn wait_status(
    running: &Running,
    what: &str,
    bound: Duration,
    ready: impl FnMut(&PipelineStatus) -> bool,
) -> PipelineStatus {
    let mut status = running.postgres().expect("postgres mode").status.reader();
    match tokio::time::timeout(bound, status.wait_until(ready)).await {
        Ok(Some(status)) => status,
        Ok(None) => panic!("{what}: the status reporter is gone"),
        Err(_) => panic!(
            "{what}: not within {bound:?}; status {:?}\n{}",
            status.snapshot(),
            state(running).await
        ),
    }
}

/// The server's clock now (`clock_timestamp()`), in microseconds since the
/// epoch, read over the test's own pool.
pub async fn server_now(db: &TestDb) -> i64 {
    let (now,): (i64,) =
        sqlx::query_as("SELECT (extract(epoch FROM clock_timestamp()) * 1000000)::bigint")
            .fetch_one(db.pool())
            .await
            .expect("reads the server clock");
    now
}

/// The database's sessions by `application_name`, the test's own left
/// out.
pub async fn sessions_by_name(db: &TestDb) -> BTreeMap<String, i64> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT coalesce(application_name, ''), count(*) FROM pg_stat_activity \
         WHERE datname = current_database() AND pid <> pg_backend_pid() \
           AND backend_type = 'client backend' \
         GROUP BY 1 ORDER BY 1",
    )
    .fetch_all(db.pool())
    .await
    .expect("reads pg_stat_activity");
    rows.into_iter().collect()
}

/// The pipeline incarnations' `application_name`s this process opened
/// sessions under (`crosstalk-pipeline-<pid>-<n>-e<epoch>`), by
/// incarnation.
pub fn incarnations(names: &BTreeMap<String, i64>) -> BTreeMap<u64, i64> {
    let prefix = format!("crosstalk-pipeline-{}-", std::process::id());
    names
        .iter()
        .filter_map(|(name, count)| {
            let (n, _epoch) = name.strip_prefix(&prefix)?.split_once("-e")?;
            Some((n.parse::<u64>().ok()?, *count))
        })
        .collect()
}

/// The epoch a pipeline incarnation's `application_name`
/// (`crosstalk-pipeline-<pid>-<n>-e<epoch>`) names, any process's.
pub fn epoch_of(name: &str) -> Option<u64> {
    let rest = name.strip_prefix("crosstalk-pipeline-")?;
    let (_, epoch) = rest.rsplit_once("-e")?;
    epoch.parse().ok()
}

/// End the session holding the pipeline lock, as a crash of the server's
/// backend or an administrator's `pg_terminate_backend` does. The database
/// stays reachable. Returns the ended backend's pid.
pub async fn terminate_lock_session(db: &TestDb) -> i32 {
    let (pid,): (i32,) = sqlx::query_as(
        "SELECT l.pid FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid \
         WHERE l.locktype = 'advisory' AND l.granted \
           AND l.classid::bigint = $1 AND l.objid::bigint = $2 \
           AND a.datname = current_database()",
    )
    .bind(PIPELINE_LOCK >> 32)
    .bind(PIPELINE_LOCK & 0xFFFF_FFFF)
    .fetch_one(db.pool())
    .await
    .unwrap_or_else(|error| panic!("no session holds the pipeline lock: {error}"));
    let (ended,): (bool,) = sqlx::query_as("SELECT pg_terminate_backend($1)")
        .bind(pid)
        .fetch_one(db.pool())
        .await
        .expect("terminates the lock's session");
    assert!(ended, "the lock's session {pid} was not terminated");
    pid
}

/// Wait up to `bound` until `done` holds of the database's sessions, or
/// fail naming them.
pub async fn wait_sessions(
    db: &TestDb,
    what: &str,
    bound: Duration,
    mut done: impl FnMut(&BTreeMap<String, i64>) -> bool,
) -> BTreeMap<String, i64> {
    let deadline = Instant::now() + bound;
    loop {
        let names = sessions_by_name(db).await;
        if done(&names) {
            return names;
        }
        if Instant::now() > deadline {
            panic!(
                "{what}: not within {bound:?}; sessions {names:?}\n{}",
                sessions(db).await
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The runtime's alive tasks.
pub fn alive_tasks() -> usize {
    tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks()
}
