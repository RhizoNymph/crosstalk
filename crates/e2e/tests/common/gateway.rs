//! The gateway harness the spool tests share: the wiki relay sent as HTTP
//! through `serve --role all` in Postgres mode (`gateway::start_on` with
//! `store` and `spool` sections) to a `FakeUpstream`, the database reached
//! through a cuttable `DbLink`; outages, restarts, the answers read back.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use bytes::Bytes;
use crosstalk_api::PgIds;
use crosstalk_e2e::answers::Answers;
use crosstalk_e2e::options;
use crosstalk_e2e::scenario::{DEFAULT_START, Scenario, WireExchange};
use crosstalk_gateway::config::GatewayConfig;
use crosstalk_gateway::gateway::{self, FatalCause, Running, StartError};
use crosstalk_gateway::live::LiveClock;
use crosstalk_gateway::live::Slot;
use crosstalk_gateway::live::pg::diagnose::{DiagnoseFrom, diagnose};
use crosstalk_gateway::role::Role;
use crosstalk_gateway::store::lock::PIPELINE_LOCK;
use crosstalk_memory::support::ManualClock;
use crosstalk_spec::events::Subject;
use crosstalk_store::TestDb;
use crosstalk_store::sqlx;
use crosstalk_testkit::client::{BodyEnd, CollectedResponse, HarnessClient};
use crosstalk_testkit::corpus::{
    CorpusRequest, CorpusResponse, EventStream, Headers, ResponseBody,
};
use crosstalk_testkit::db_link::DbLink;
use crosstalk_testkit::upstream::{FakeUpstream, Reply, Script};
use crosstalk_transport::SpoolError;
use hyper::header::{HeaderName, HeaderValue};
use hyper::http::uri::PathAndQuery;
use hyper::{Method, StatusCode};
use serde_json::Value;
use tokio::time::Instant;

use super::{
    PENDING_DELIVERIES, POLL, UNADMITTED_ENTRIES, Volume, WAIT, migrated, passes, read, settle,
    start_pg, stop_pg,
};

pub const PREFIX: &str = "/anthropic";
pub const SECRET_ENV: &str = "CROSSTALK_SPOOL_TEST_SECRET_V1";
/// Nothing listens on port 1: every connection is refused at once.
pub const UNREACHABLE: &str = "postgres://crosstalk@127.0.0.1:1/crosstalk";

pub fn scenario() -> Scenario {
    Scenario::wiki_relay(DEFAULT_START)
}

/// The Postgres-mode gateway config: the scenario's correlator timing, one
/// shard, the periodic tick a day apart (never, in a test), a spool probing
/// every 50 ms with `spool` merged over it.
pub fn config(upstream: &str, data: &Path, spool: Value) -> GatewayConfig {
    let ms = |window: Duration| u64::try_from(window.as_millis()).expect("fits");
    let mut spool_section = serde_json::json!({"probe_ms": 50});
    if let (Some(base), Value::Object(extra)) = (spool_section.as_object_mut(), spool) {
        base.extend(extra);
    }
    let text = serde_json::json!({
        "ingress": {
            "listen": "127.0.0.1:0",
            "routes": [{
                "name": "anthropic",
                "prefix": PREFIX,
                "upstream": {
                    "id": "anthropic",
                    "kind": {"type": "vendor_api", "data": {"type": "anthropic"}},
                    "base_url": upstream,
                },
            }],
            "secrets": {"current": {"version": 1, "env": SECRET_ENV}},
            "capture": {"channel_capacity": 64},
        },
        "ops": {"listen": "127.0.0.1:0"},
        "store": {
            "pool": {"max_connections": 12, "acquire_timeout_ms": 2_000},
            "bus": {"poll_micros": 20_000, "publish_timeout_micros": 2_000_000},
        },
        "spool": spool_section,
        "blobs": {"root": data.join("blobs")},
        "flow": {
            "correlation_window_ms": ms(options::CORRELATION_WINDOW),
            "evidence_window_ms": ms(options::EVIDENCE_WINDOW),
            "suspected_ttl_ms": ms(options::SUSPECTED_TTL),
            "shards": 1,
            "tick_ms": 86_400_000,
        },
        "shutdown": {"drain_timeout_ms": 5_000, "flush_timeout_ms": 60_000},
    })
    .to_string();
    GatewayConfig::from_json(&text).expect("the test config parses")
}

/// The variables the gateway reads: the deployment secret and
/// `DATABASE_URL` (never printed).
pub fn lookup(database: String) -> impl Fn(&str) -> Option<String> {
    move |name| match name {
        SECRET_ENV => Some(super::SECRET_HEX.to_owned()),
        "DATABASE_URL" => Some(database.clone()),
        _ => None,
    }
}

/// The test server's URL as `TestDb` finds it: the environment, else the
/// nearest `.env.test` above the working directory.
pub fn admin_url() -> String {
    const VAR: &str = "TEST_DATABASE_URL";
    if let Ok(url) = std::env::var(VAR) {
        return url;
    }
    let cwd = std::env::current_dir().expect("a working directory");
    cwd.ancestors()
        .find_map(|dir| {
            let text = std::fs::read_to_string(dir.join(".env.test")).ok()?;
            text.lines().rev().map(str::trim).find_map(|line| {
                let line = line.strip_prefix("export ").map_or(line, str::trim_start);
                let (key, value) = line.split_once('=')?;
                (key.trim() == VAR).then(|| {
                    let value = value.trim();
                    ['"', '\'']
                        .iter()
                        .find_map(|q| value.strip_prefix(*q)?.strip_suffix(*q))
                        .unwrap_or(value)
                        .to_owned()
                })
            })
        })
        .expect("TestDb found the test server, so its URL is set")
}

/// `db`'s URL with the server's host and port replaced by `link`'s: the
/// gateway reaches the database only through the link. Never printed.
pub fn linked_url(db: &TestDb, link: &DbLink) -> String {
    let admin = admin_url();
    let (scheme, rest) = admin.split_once("://").expect("a scheme");
    let authority_end = rest.find(['/', '?']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(authority_end);
    let userinfo = authority.rfind('@').map_or("", |at| &authority[..=at]);
    let query = tail.find('?').map_or("", |at| &tail[at..]);
    let database = db
        .url()
        .connect_options()
        .get_database()
        .expect("a test database name");
    format!(
        "{scheme}://{userinfo}{}:{}/{database}{query}",
        link.host(),
        link.port()
    )
}

pub async fn link_to(db: &TestDb) -> DbLink {
    let options = db.url().connect_options();
    DbLink::start(options.get_host(), options.get_port())
        .await
        .expect("the db link")
}

pub fn headers(pairs: &[(String, String)]) -> Headers {
    let mut headers = Headers::new();
    for (name, value) in pairs {
        headers.push(
            HeaderName::from_bytes(name.as_bytes()).expect("a header name"),
            HeaderValue::from_str(value).expect("a header value"),
        );
    }
    headers
}

/// The request the harness sends (the route prefix is the client's).
pub fn request(exchange: &WireExchange) -> CorpusRequest {
    let path = exchange
        .request
        .path
        .strip_prefix(PREFIX)
        .expect("under the route");
    let target = match &exchange.request.query {
        Some(query) => format!("{path}?{query}"),
        None => path.to_owned(),
    };
    CorpusRequest {
        method: Method::from_bytes(exchange.request.method.as_bytes()).expect("a method"),
        target: PathAndQuery::try_from(target.as_str()).expect("a target"),
        headers: headers(&exchange.request.headers),
        body: Bytes::from(exchange.request.body.clone()),
    }
}

/// The response the upstream sends back.
pub fn response(exchange: &WireExchange) -> CorpusResponse {
    CorpusResponse {
        status: StatusCode::from_u16(exchange.response.status).expect("a status"),
        headers: headers(&exchange.response.headers),
        body: ResponseBody::EventStream(
            EventStream::parse(Bytes::from(exchange.response.body.clone())).expect("events"),
        ),
    }
}

/// A gateway under test, and what it was started with.
pub struct Gateway {
    pub running: Running,
    pub clock: ManualClock,
}

pub async fn start(config: &GatewayConfig, database: String, clock: &ManualClock) -> Running {
    let deadline = Instant::now() + WAIT;
    loop {
        let started = tokio::time::timeout(
            WAIT,
            gateway::start_on(
                config,
                Role::All,
                lookup(database.clone()),
                LiveClock::Manual(clock.clone()),
            ),
        )
        .await
        .unwrap_or_else(|_| panic!("the gateway did not start within {WAIT:?}"));
        match started {
            Ok(running) => return running,
            // A killed gateway's spool lock goes with its last handle.
            Err(StartError::Spool(SpoolError::Locked { .. })) if Instant::now() < deadline => {
                tokio::time::sleep(POLL).await;
            }
            Err(error) => panic!("the gateway starts: {error}"),
        }
    }
}

/// What the gateway's ops and Postgres side show now, for a failed wait.
pub async fn state(running: &Running) -> String {
    let readiness = running.readiness().await;
    let health = running.health();
    let diagnosis = match running.postgres() {
        Some(pg) => {
            let groups: Vec<_> = [
                Slot::L3Reconstruct,
                Slot::L4Provenance,
                Slot::L5Flow,
                Slot::L6Classify,
                Slot::L7Topology,
            ]
            .iter()
            .map(|slot| slot.group())
            .collect();
            let status = pg.status.reader();
            let diagnosis = diagnose(DiagnoseFrom {
                pool: &pg.pool,
                bus: &pg.bus,
                spool: pg.spool.as_ref(),
                groups: &groups,
                shards: std::num::NonZeroU16::MIN,
                status: Some(&status),
                watermark: None,
            });
            match tokio::time::timeout(Duration::from_secs(30), diagnosis).await {
                Ok(diagnosis) => diagnosis.to_string(),
                Err(_) => "(the diagnosis did not answer within 30 s)".to_owned(),
            }
        }
        None => "(not in postgres mode)".to_owned(),
    };
    format!("readiness: {readiness:?}\nhealth: {health:?}\n{diagnosis}")
}

/// `work` within [`WAIT`], or a panic with the gateway's state.
pub async fn within<T>(
    running: &Running,
    what: &str,
    work: impl std::future::Future<Output = T>,
) -> T {
    match tokio::time::timeout(WAIT, work).await {
        Ok(done) => done,
        Err(_) => panic!(
            "{what} did not finish within {WAIT:?}\n{}",
            state(running).await
        ),
    }
}

pub async fn ops_get(running: &Running, target: &str) -> CollectedResponse {
    let request = CorpusRequest {
        method: Method::GET,
        target: PathAndQuery::try_from(target).expect("a target"),
        headers: Headers::new(),
        body: Bytes::new(),
    };
    HarnessClient::new(running.ops_addr())
        .send(&request)
        .await
        .expect("the ops listener answers")
}

pub async fn json(running: &Running, target: &str) -> (StatusCode, Value) {
    let response = ops_get(running, target).await;
    let value = serde_json::from_slice(&response.body).expect("JSON");
    (response.status, value)
}

pub async fn metrics(running: &Running) -> String {
    String::from_utf8(ops_get(running, "/metrics").await.body.to_vec()).expect("text")
}

pub fn has_line(text: &str, line: &str) {
    assert!(
        text.lines().any(|candidate| candidate == line),
        "missing {line}\n{text}"
    );
}

/// How long a restarted gateway may see `pipeline_lock: held elsewhere`:
/// the server's reaping of a partitioned session (about 25 s with deploy's
/// `tcp_keepalives_*`) and a margin.
pub const LOCK_RELEASE: Duration = Duration::from_secs(60);

/// One row of [`sessions`].
type Session = (
    i32,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i32>,
    Option<String>,
    bool,
);

/// Every other session on the test database as the server sees it, and
/// which one holds the pipeline lock: read over the test's own pool,
/// which never goes through the `DbLink`.
pub async fn sessions(db: &TestDb) -> String {
    let rows: Result<Vec<Session>, _> = sqlx::query_as(
        "SELECT a.pid, a.application_name || ' ' || a.state, a.wait_event_type, a.wait_event, \
                a.backend_start::text, a.state_change::text, a.client_addr::text, a.client_port, \
                left(a.query, 120), \
                EXISTS (SELECT 1 FROM pg_locks l WHERE l.pid = a.pid AND l.locktype = 'advisory' \
                        AND l.granted AND l.classid::bigint = $1 AND l.objid::bigint = $2) \
         FROM pg_stat_activity a \
         WHERE a.datname = current_database() AND a.pid <> pg_backend_pid() \
         ORDER BY a.backend_start",
    )
    .bind(PIPELINE_LOCK >> 32)
    .bind(PIPELINE_LOCK & 0xFFFF_FFFF)
    .fetch_all(db.pool())
    .await;
    let server: Result<(i64, String, String, String, String), _> = sqlx::query_as(
        "SELECT (SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend'), \
                current_setting('max_connections'), current_setting('tcp_keepalives_idle'), \
                current_setting('tcp_keepalives_interval'), current_setting('tcp_keepalives_count')",
    )
    .fetch_one(db.pool())
    .await;
    let header = match server {
        Ok((clients, max, idle, interval, count)) => format!(
            "  server: {clients} client backends of max_connections {max}; \
             tcp_keepalives idle {idle} interval {interval} count {count} (0: the OS default)\n"
        ),
        Err(error) => format!("  (the server's settings did not read: {error})\n"),
    };
    let listed = match rows {
        Ok(rows) => rows
            .iter()
            .map(
                |(pid, state, wait_type, wait, started, changed, addr, port, query, lock)| {
                    format!(
                        "  pid {pid}{} state {state:?} wait {wait_type:?}/{wait:?} since {changed:?} \
                         (backend started {started:?}, client {addr:?}:{port:?}) query {query:?}",
                        if *lock { " HOLDS THE PIPELINE LOCK" } else { "" }
                    )
                },
            )
            .collect::<Vec<_>>()
            .join("\n"),
        Err(error) => format!("  (pg_stat_activity did not read: {error})"),
    };
    format!("{header}{listed}")
}

impl Gateway {
    /// Send `exchanges` through the proxy, the clock at each one's end
    /// first: each request reaches the upstream unchanged and its response
    /// reaches the client unchanged. Then wait until capture handled
    /// `captured` exchanges in all (published, spooled or refused).
    pub async fn send(&self, upstream: &FakeUpstream, exchanges: &[WireExchange], captured: u64) {
        let running = &self.running;
        let client =
            HarnessClient::new(running.proxy_addr().expect("role all proxies")).prefix(PREFIX);
        within(running, "forwarding through the proxy", async {
            for exchange in exchanges {
                self.clock.set(exchange.ended_at);
                let sent = request(exchange);
                let expected = response(exchange);
                upstream
                    .reply_next(Reply::from_response(&expected))
                    .await
                    .expect("scripted");
                let answered = client.send(&sent).await.expect("the gateway answers");
                assert_eq!(answered.end, BodyEnd::Complete, "{}", exchange.label);
                assert_eq!(
                    answered.differences_from(&expected),
                    Vec::new(),
                    "{}: the client's response changed",
                    exchange.label
                );
                let received = upstream.received().await.expect("the upstream log");
                let last = received.last().expect("the upstream got the request");
                assert_eq!(
                    last.differences_from(&sent),
                    Vec::new(),
                    "{}: the upstream's request changed",
                    exchange.label
                );
            }
            loop {
                let health = running.health();
                let refused = health
                    .spool
                    .as_ref()
                    .map_or(0, |spool| spool.capture_spool_full);
                let pipeline = health.pipeline;
                if pipeline.published
                    + pipeline.normalize_failed
                    + pipeline.store_failed
                    + pipeline.publish_failed
                    + refused
                    >= captured
                {
                    return;
                }
                tokio::time::sleep(POLL).await;
            }
        })
        .await;
    }

    /// Poll `/readyz` until `ready` holds. A pipeline that stopped (its
    /// lock lost) fails at once: it would never recover.
    pub async fn readyz_until(
        &self,
        what: &str,
        ready: impl Fn(StatusCode, &Value) -> bool,
    ) -> Value {
        let running = &self.running;
        within(running, what, async {
            loop {
                let (status, body) = json(running, "/readyz").await;
                if ready(status, &body) {
                    return body;
                }
                let pipeline = body["pipeline"].as_str().unwrap_or_default();
                if pipeline.starts_with("stopped") || body["pipeline_lock"] == "lost" {
                    panic!(
                        "{what}: the pipeline stopped and will not recover: {body}\n{}",
                        state(running).await
                    );
                }
                tokio::time::sleep(POLL).await;
            }
        })
        .await
    }

    /// Wait until the pipeline runs on a reachable database with a direct
    /// spool, and nothing is pending: `/readyz` `ok`, the spool empty, no
    /// log entry a group has still to admit, no delivery held by any group.
    /// (Without the log entries it returned while L3 had yet to take the
    /// third exchange; the next outage then cut L3 mid-delivery, and the
    /// restart's redelivery let the spooled fourth exchange overtake it.)
    ///
    /// A restarted gateway may find the pipeline lock still held by the
    /// stopped process's session: after a network partition (a `DbLink`
    /// cut) the server keeps that backend, and its advisory lock, until it
    /// notices the dead peer, about 25 s with deploy's server keepalives
    /// (`tcp_keepalives_idle=10`, `interval=5`, `count=3`), hours with the
    /// OS defaults. `pipeline_lock: held elsewhere` may last
    /// [`LOCK_RELEASE`]; past it, or past [`WAIT`] overall, the test fails
    /// naming every session on the database and which one holds the lock
    /// ([`sessions`], read over the test's own pool, not the gateway's).
    pub async fn settled(&self, db: &TestDb, what: &str) {
        let running = &self.running;
        let started = Instant::now();
        let mut held_since: Option<Instant> = None;
        loop {
            let (status, body) = json(running, "/readyz").await;
            if status == StatusCode::OK && body["status"] == "ok" && body["capture"] == "durable" {
                break;
            }
            let pipeline = body["pipeline"].as_str().unwrap_or_default();
            if pipeline.starts_with("stopped") || body["pipeline_lock"] == "lost" {
                panic!(
                    "{what}: the pipeline stopped and will not recover: {body}\n{}\nsessions:\n{}",
                    state(running).await,
                    sessions(db).await
                );
            }
            match body["pipeline_lock"] == "held elsewhere" {
                true => {
                    let since = *held_since.get_or_insert_with(Instant::now);
                    if since.elapsed() > LOCK_RELEASE {
                        panic!(
                            "{what}: the pipeline lock stayed held by another session for over \
                             {LOCK_RELEASE:?}: a partitioned backend the server has not reaped \
                             (are the server's tcp_keepalives_* set?)\nsessions:\n{}\n{}",
                            sessions(db).await,
                            state(running).await
                        );
                    }
                }
                false => held_since = None,
            }
            if started.elapsed() > WAIT {
                panic!(
                    "{what} did not finish within {WAIT:?}\n{}\nsessions:\n{}",
                    state(running).await,
                    sessions(db).await
                );
            }
            tokio::time::sleep(POLL).await;
        }
        let running = &self.running;
        within(running, what, async {
            let mut quiet = 0;
            while quiet < 2 {
                // Spool, log, deliveries: an envelope moving from one to
                // the next between two reads is seen by the later one.
                let spooled = running.health().spool.map_or(0, |spool| spool.records);
                let (unadmitted,): (i64,) = sqlx::query_as(UNADMITTED_ENTRIES)
                    .fetch_one(db.pool())
                    .await
                    .expect("reads");
                let (pending,): (i64,) = sqlx::query_as(PENDING_DELIVERIES)
                    .fetch_one(db.pool())
                    .await
                    .expect("reads");
                quiet = match spooled == 0 && unadmitted == 0 && pending == 0 {
                    true => quiet + 1,
                    false => 0,
                };
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await;
    }

    pub async fn shutdown(self) {
        let before = state(&self.running).await;
        if tokio::time::timeout(WAIT, self.running.shutdown())
            .await
            .is_err()
        {
            panic!("the gateway's shutdown did not finish within {WAIT:?}; before it:\n{before}");
        }
    }
}

/// The answers of what the gateways stored in `db` with their bodies in
/// `blobs`: a `Live` process started over them after the last gateway
/// stopped, settled through both passes.
pub async fn read_back(db: &TestDb, blobs: PathBuf, scenario: &Scenario) -> [Answers; 2] {
    let dir = tempfile::tempdir().expect("a temp dir");
    let volume = Volume::at(
        dir.path().to_path_buf(),
        blobs,
        ManualClock::at(scenario.ends_at()),
    );
    let live = start_pg(db, &volume, PgIds::Seeded(super::SEED)).await;
    let mut out = Vec::new();
    for until in passes(scenario) {
        settle(&live, until).await;
        out.push(read(&live, scenario).await);
    }
    stop_pg(live).await;
    out.try_into().expect("two passes")
}

/// The uninterrupted gateway run over its own database: every exchange
/// through the proxy, the link never cut.
pub async fn uninterrupted(test: &str, scenario: &Scenario) -> Option<[Answers; 2]> {
    let db = migrated(test).await?;
    let link = link_to(&db).await;
    let upstream = FakeUpstream::start(Script::new()).await.expect("upstream");
    let dir = tempfile::tempdir().expect("a temp dir");
    let data = dir.path().join("data");
    let clock = ManualClock::at(scenario.start);
    let gateway = Gateway {
        running: start(
            &config(&upstream.base_url(), &data, serde_json::json!({})),
            linked_url(&db, &link),
            &clock,
        )
        .await,
        clock,
    };
    gateway
        .settled(&db, "the uninterrupted gateway's start")
        .await;
    let all = u64::try_from(scenario.exchanges.len()).expect("fits");
    gateway.send(&upstream, &scenario.exchanges, all).await;
    gateway
        .settled(&db, "the uninterrupted gateway's pipeline")
        .await;
    gateway.shutdown().await;
    let answers = read_back(&db, data.join("blobs"), scenario).await;
    db.close().await.expect("drops");
    Some(answers)
}

/// A database outage while `gateway` runs: the link cut, `exchanges`
/// forwarded unchanged and spooled, the pipeline's lock lost
/// (`Running::fatal`: `serve` exits non-zero there for its supervisor to
/// restart it), the gateway stopped gracefully, a fresh one started on the
/// same data directory with the link still cut (the spool recovered from
/// disk, `/readyz` 200 degraded), then the link restored and the spool
/// drained. Returns the fresh gateway.
pub async fn outage(
    gateway: Gateway,
    db: &TestDb,
    link: &DbLink,
    upstream: &FakeUpstream,
    restart: (&GatewayConfig, &str),
    exchanges: &[WireExchange],
    captured_before: u64,
) -> Gateway {
    let spooled = u64::try_from(exchanges.len()).expect("fits");
    link.cut();
    gateway
        .send(upstream, exchanges, captured_before + spooled)
        .await;
    let spool = gateway.running.health().spool.expect("a spool");
    assert_eq!(spool.records, spooled, "{spool:?}");
    assert_eq!(spool.state, "spooling");
    let (_, ready) = json(&gateway.running, "/readyz").await;
    assert_eq!(ready["capture"], "spooling", "{ready}");
    let text = metrics(&gateway.running).await;
    has_line(&text, "crosstalk_spool_state{state=\"spooling\"} 1");
    has_line(&text, &format!("crosstalk_spool_records {spooled}"));

    // The lock's session failed with the connection: the process must
    // exit to be restarted.
    let fatal = within(
        &gateway.running,
        "the pipeline lock's loss",
        gateway.running.fatal(),
    )
    .await;
    assert_eq!(fatal.cause, FatalCause::PipelineLockLost, "{fatal:?}");
    assert!(
        fatal.detail.contains("pipeline lock lost"),
        "{}",
        fatal.detail
    );
    let clock = gateway.clock.clone();
    let bound = gateway.running.shutdown_bound();
    if tokio::time::timeout(bound, gateway.running.shutdown())
        .await
        .is_err()
    {
        panic!("the gateway's shutdown after the lock's loss took over {bound:?}");
    }

    // The supervisor's restart, the database still down.
    let (config, url) = restart;
    let fresh = Gateway {
        running: start(config, url.to_owned(), &clock).await,
        clock,
    };
    let recovered = fresh.running.health().spool.expect("a spool");
    assert_eq!(
        recovered.records, spooled,
        "recovered from disk: {recovered:?}"
    );
    let ready = fresh
        .readyz_until("the restarted gateway spooling", |status, body| {
            status == StatusCode::OK
                && body["capture"] == "spooling"
                && body["pipeline"] == "waiting_for_database"
        })
        .await;
    assert_eq!(ready["status"], "degraded", "{ready}");

    link.restore();
    fresh.settled(db, "the drain after the outage").await;
    let spool = fresh.running.health().spool.expect("a spool");
    assert_eq!(spool.state, "direct");
    // Each record drained once: the captures recovered from disk, and
    // whatever the restarted process spooled itself before its gate opened
    // (recovery relays a store outbox row the stopped process left behind
    // through the spool). The bus log holds each capture once (`finish`).
    assert_eq!(
        spool.drained,
        spooled + spool.appended,
        "every record drained exactly once: {spool:?}"
    );
    fresh
}

/// A started gateway on `db` through `link`, its first exchange through.
pub struct Run {
    pub db: TestDb,
    pub link: DbLink,
    pub upstream: FakeUpstream,
    pub _dir: tempfile::TempDir,
    pub data: PathBuf,
    pub config: GatewayConfig,
    pub url: String,
}

pub async fn run(db: TestDb) -> Run {
    let link = link_to(&db).await;
    let upstream = FakeUpstream::start(Script::new()).await.expect("upstream");
    let dir = tempfile::tempdir().expect("a temp dir");
    let data = dir.path().join("data");
    let config = config(&upstream.base_url(), &data, serde_json::json!({}));
    let url = linked_url(&db, &link);
    Run {
        db,
        link,
        upstream,
        _dir: dir,
        data,
        config,
        url,
    }
}

impl Run {
    pub async fn start(&self, clock: &ManualClock) -> Gateway {
        let gateway = Gateway {
            running: start(&self.config, self.url.clone(), clock).await,
            clock: clock.clone(),
        };
        gateway.settled(&self.db, "the gateway's start").await;
        gateway
    }

    pub async fn outage(
        &self,
        gateway: Gateway,
        exchanges: &[WireExchange],
        captured_before: u64,
    ) -> Gateway {
        outage(
            gateway,
            &self.db,
            &self.link,
            &self.upstream,
            (&self.config, &self.url),
            exchanges,
            captured_before,
        )
        .await
    }

    /// Stop `gateway`, check every capture is in the bus log once, and
    /// compare the answers with the uninterrupted run's.
    pub async fn finish(
        self,
        gateway: Gateway,
        scenario: &Scenario,
        expected: &[Answers; 2],
        what: &str,
    ) {
        gateway.shutdown().await;
        let all = i64::try_from(scenario.exchanges.len()).expect("fits");
        assert_eq!(
            captured_in_log(&self.db).await,
            (all, all),
            "{what}: every capture in the bus log once"
        );
        let observed = read_back(&self.db, self.data.join("blobs"), scenario).await;
        for (pass, (observed, expected)) in observed.iter().zip(expected).enumerate() {
            super::assert_same(
                observed,
                expected,
                &format!("pass {pass}: {what} differs from the uninterrupted run"),
            );
        }
        self.db.close().await.expect("drops");
    }
}

/// Count the `ExchangeCaptured` envelopes in the bus log.
pub async fn captured_in_log(db: &TestDb) -> (i64, i64) {
    let subject = match serde_json::to_value(Subject::ExchangeCaptured).expect("a subject") {
        Value::String(name) => name,
        other => panic!("a subject's wire name is a string: {other}"),
    };
    sqlx::query_as("SELECT count(*), count(DISTINCT id) FROM transport.events WHERE subject = $1")
        .bind(subject)
        .fetch_one(db.pool())
        .await
        .expect("reads the log")
}
