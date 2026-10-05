//! Short swarm runs over real sockets, checked row by row against the
//! requests the model actually received.
//!
//! The model here is the fake model behind a recorder: it logs every
//! `POST /v1/messages` (session header, body bytes, whether it failed it)
//! and fails the first attempt of about half the distinct bodies with a
//! 529, so the agents retry and turn ordinals must count failed requests.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::header::{CONTENT_TYPE, HeaderValue};
use hyper::{Request, Response, StatusCode};
use serde_json::{Value, json};
use sha2::Digest;
use tokio::sync::{mpsc, oneshot};

use crate::anthropic::sse::{Split, encode};
use crate::anthropic::{ResponseBlock, error_document};
use crate::http::{BaseUrl, DemoBody, json_response, serve};
use crate::knobs::{Fraction, PositiveSpan, Span};
use crate::swarm::config::{SwarmConfig, TaskMix};
use crate::swarm::stats::Report;
use crate::upstream::generate::{GenConfig, generate, parse_request};

use super::servers::{listener, wiki};

/// One request the model received, in arrival order.
#[derive(Debug, Clone)]
struct Recorded {
    session: String,
    body: Bytes,
    failed: bool,
}

enum Command {
    Request {
        session: String,
        body: Bytes,
        fail: oneshot::Sender<bool>,
    },
    Log(oneshot::Sender<Vec<Recorded>>),
}

fn model() -> GenConfig {
    GenConfig {
        seed: 9,
        words: Span::ordered(20, 40),
        first_byte_ms: Span::ordered(0, 0),
        stream_ms: Span::ordered(0, 0),
    }
}

/// The task that owns the log: decides failures and keeps every request.
async fn record(mut inbox: mpsc::Receiver<Command>) {
    let mut log = Vec::new();
    let mut seen = HashSet::new();
    while let Some(command) = inbox.recv().await {
        match command {
            Command::Request {
                session,
                body,
                fail,
            } => {
                let digest = blake3::hash(&body);
                // About half the bodies fail once, the first time they come.
                let failed = digest.as_bytes()[0] < 128 && seen.insert(digest);
                log.push(Recorded {
                    session,
                    body,
                    failed,
                });
                let _ = fail.send(failed);
            }
            Command::Log(answer) => {
                let _ = answer.send(log.clone());
            }
        }
    }
}

async fn answer(request: Request<Incoming>, recorder: mpsc::Sender<Command>) -> Response<DemoBody> {
    let session = request
        .headers()
        .get("x-claude-code-session-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let body = request
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let (fail, failed) = oneshot::channel();
    recorder
        .send(Command::Request {
            session,
            body: body.clone(),
            fail,
        })
        .await
        .expect("recorder");
    if failed.await.expect("decided") {
        return json_response(
            StatusCode::from_u16(529).expect("status"),
            &error_document("overloaded_error", "try again"),
        );
    }
    let parsed = parse_request(&body).expect("a request");
    let reply = generate(&model(), &parsed, &body);
    if !reply.stream {
        return json_response(StatusCode::OK, &reply.message.to_document());
    }
    let mut bytes = BytesMut::new();
    for frame in encode(&reply.message, Split::default()) {
        bytes.extend_from_slice(&frame.to_bytes());
    }
    let mut response = Response::new(DemoBody::whole(bytes.freeze()));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream; charset=utf-8"),
    );
    response
}

struct Recorder {
    addr: SocketAddr,
    commands: mpsc::Sender<Command>,
    _stop: oneshot::Sender<()>,
}

impl Recorder {
    async fn start() -> Self {
        let (listener, addr) = listener().await;
        let (commands, inbox) = mpsc::channel(64);
        tokio::spawn(record(inbox));
        let (stop, stopped) = oneshot::channel::<()>();
        let handler_commands = commands.clone();
        tokio::spawn(serve(
            listener,
            move |request| answer(request, handler_commands.clone()),
            async {
                let _ = stopped.await;
            },
        ));
        Self {
            addr,
            commands,
            _stop: stop,
        }
    }

    async fn log(&self) -> Vec<Recorded> {
        let (answer, answered) = oneshot::channel();
        self.commands
            .send(Command::Log(answer))
            .await
            .expect("recorder");
        answered.await.expect("log")
    }
}

/// A run's report, its rows, and every request by session in send order.
struct Run {
    config: SwarmConfig,
    report: Report,
    rows: Vec<Value>,
    sessions: BTreeMap<String, Vec<Recorded>>,
    total_recorded: usize,
}

async fn swarm(claude_code_shape: bool, seed: u64) -> Run {
    let mix = TaskMix::new(
        Fraction::new(0.3).expect("fraction"),
        Fraction::new(0.65).expect("fraction"),
    )
    .expect("mix");
    swarm_with(claude_code_shape, seed, mix).await
}

/// One swarm run against a fresh wiki with the given write/read mix.
async fn swarm_with(claude_code_shape: bool, seed: u64, mix: TaskMix) -> Run {
    let recorder = Recorder::start().await;
    let pages = wiki().await;
    let truth = std::env::temp_dir().join(format!(
        "crosstalk-demo-truth-v2-{}-{claude_code_shape}-{seed}.jsonl",
        std::process::id()
    ));
    let base = |addr: SocketAddr| format!("http://{addr}").parse::<BaseUrl>().expect("url");
    let mut config = SwarmConfig::new(base(recorder.addr), base(pages.addr));
    config.agents = NonZeroU32::MIN.saturating_add(4);
    config.agents_per_key = NonZeroU32::MIN.saturating_add(1);
    config.think_ms = Span::ordered(5, 20);
    config.turns = PositiveSpan::ordered(4, 6);
    config.mix = mix;
    // One page: every read reads it, so self-reads and rereads happen.
    config.pages = NonZeroU32::MIN;
    config.duration = Duration::from_millis(4000);
    config.ramp = Duration::from_millis(50);
    config.stream_fraction = Fraction::new(0.5).expect("fraction");
    config.grace = Duration::from_secs(5);
    config.seed = seed;
    config.claude_code_shape = claude_code_shape;
    config.ground_truth = Some(truth.clone());
    let report = crate::swarm::run(config.clone(), std::future::pending())
        .await
        .expect("report");
    let text = tokio::fs::read_to_string(&truth)
        .await
        .expect("ground truth");
    let _ = tokio::fs::remove_file(&truth).await;
    let rows = text
        .lines()
        .map(|line| serde_json::from_str(line).expect("json"))
        .collect();
    let log = recorder.log().await;
    let total_recorded = log.len();
    let mut sessions: BTreeMap<String, Vec<Recorded>> = BTreeMap::new();
    for request in log {
        sessions
            .entry(request.session.clone())
            .or_default()
            .push(request);
    }
    Run {
        config,
        report,
        rows,
        sessions,
        total_recorded,
    }
}

fn body_of(request: &Recorded) -> Value {
    serde_json::from_slice(&request.body).expect("json body")
}

fn turn(row: &Value, field: &str) -> usize {
    usize::try_from(row[field].as_u64().expect("turn")).expect("index")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Whether `body` carries a tool_result for `id` anywhere.
fn carries_result(body: &Value, id: &str) -> bool {
    body["messages"].as_array().is_some_and(|messages| {
        messages.iter().any(|m| {
            m["content"].as_array().is_some_and(|blocks| {
                blocks
                    .iter()
                    .any(|b| b["type"] == "tool_result" && b["tool_use_id"] == id)
            })
        })
    })
}

/// Checks every row of `run` against the requests the model received, and
/// returns the kinds seen and whether a turn ordinal skipped a failure.
fn check(run: &Run) -> (BTreeSet<String>, bool) {
    let rows = &run.rows;
    let header = &rows[0];
    assert_eq!(header["kind"], "header");
    assert_eq!(header["version"], 2);
    let run_id = header["run"].as_str().expect("run");
    assert_eq!(run_id.len(), 26);
    assert_eq!(run_id, run.report.run);
    assert_eq!(header["world"], format!("swarm-{run_id}"));
    assert_eq!(header["seed"], run.config.seed);
    assert_eq!(header["agents"], 5);
    assert_eq!(header["keys"], 3);
    assert_eq!(header["agents_per_key"], 2);
    assert_eq!(header["claude_code_shape"], run.config.claude_code_shape);
    assert_eq!(header["gateway_url"], run.config.gateway.url());
    assert_eq!(header["wiki_url"], run.config.wiki.url());
    let started = header["started_at_unix_ms"].as_u64().expect("start");
    let world = header["world"].clone();
    let wiki_url = run.config.wiki.url();

    // The clusters come right after the header, once each.
    let clusters: Vec<&Value> = rows
        .iter()
        .filter(|r| r["kind"] == "agent_cluster")
        .collect();
    assert_eq!(clusters.len(), 3);
    for (i, cluster) in clusters.iter().enumerate() {
        assert_eq!(rows[1 + i], **cluster);
        assert_eq!(cluster["key_group"], i);
        assert_eq!(cluster["world"], world);
    }
    let named: Vec<&str> = clusters
        .iter()
        .flat_map(|c| c["agents"].as_array().expect("agents"))
        .map(|a| a.as_str().expect("name"))
        .collect();
    assert_eq!(
        named,
        [
            "agent-000",
            "agent-001",
            "agent-002",
            "agent-003",
            "agent-004"
        ]
    );

    let mut kinds = BTreeSet::new();
    let mut skipped_failure = false;
    let mut by_kind: BTreeMap<String, u64> = BTreeMap::new();
    for row in &rows[4..] {
        let kind = row["kind"].as_str().expect("kind").to_owned();
        *by_kind.entry(kind.clone()).or_default() += 1;
        kinds.insert(kind.clone());
        assert_eq!(row["world"], world);
        let page = row["page"].as_str().expect("page");
        let url = format!("{wiki_url}/pages/{page}");
        assert_eq!(
            row["read_tool"],
            json!({"name": "http_request", "input": {"method": "GET", "url": url}})
        );
        assert_eq!(
            row["at_unix_ms"],
            started + row["at_ms"].as_u64().expect("at")
        );
        let reader = &run.sessions[row["reader_session"].as_str().expect("session")];
        let reader_turn = turn(row, "reader_turn");
        let id = row["reader_tool_use_id"].as_str().expect("id");
        let sent = body_of(&reader[reader_turn]);
        // The first request carrying the result: none before it does.
        assert!(carries_result(&sent, id), "{row}");
        assert!(
            reader[..reader_turn]
                .iter()
                .all(|r| !carries_result(&body_of(r), id)),
            "{row}"
        );
        skipped_failure |= reader[..reader_turn].iter().any(|r| r.failed);
        // The GET call is in the conversation as the model made it.
        let calls: Vec<&Value> = sent["messages"]
            .as_array()
            .expect("messages")
            .iter()
            .filter_map(|m| m["content"].as_array())
            .flatten()
            .filter(|b| b["type"] == "tool_use" && b["id"] == id)
            .collect();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["input"], row["read_tool"]["input"]);
        if kind == "miss" {
            continue;
        }

        assert_eq!(row["route"], json!({"kind": "channel", "url": url}));
        assert_eq!(row["carrier"], "tool_result");
        assert_eq!(row["at_unix_ms"], row["read_at_unix_ms"]);
        assert!(row["written_at_unix_ms"].as_u64() <= row["read_at_unix_ms"].as_u64());
        // content.at points at the exact tool_result of the request sent.
        let at = &row["content"]["at"];
        assert_eq!(at["tool_use_id"], id);
        let message = usize::try_from(at["message"].as_u64().expect("message")).expect("index");
        let block = usize::try_from(at["block"].as_u64().expect("block")).expect("index");
        let result = &sent["messages"][message]["content"][block];
        assert_eq!(result["type"], "tool_result", "{row}");
        assert_eq!(result["tool_use_id"], id);
        assert!(result.get("is_error").is_none());
        let text = result["content"].as_str().expect("content");
        // The hashes are of those bytes, and those bytes are in the body.
        assert_eq!(
            row["content"]["blake3"],
            blake3::hash(text.as_bytes()).to_hex().as_str()
        );
        assert_eq!(
            row["content"]["sha256"],
            hex(&sha2::Sha256::digest(text.as_bytes()))
        );
        let escaped = serde_json::to_string(text).expect("encode");
        let raw = String::from_utf8(reader[reader_turn].body.to_vec()).expect("utf-8");
        assert!(raw.contains(&escaped));
        let excerpt = row["content"]["excerpt"].as_str().expect("excerpt");
        assert!(!excerpt.is_empty() && excerpt.chars().count() <= 80);
        assert!(text.contains(excerpt));
        if run.config.claude_code_shape {
            // The system turns before the result are counted in `message`.
            let system_turns = sent["messages"]
                .as_array()
                .expect("messages")
                .iter()
                .take(message)
                .filter(|m| m["role"] == "system")
                .count();
            assert!(system_turns >= 1);
            assert_eq!(sent["messages"][0]["role"], "system");
        }

        // The writer's turn is the request whose answer held the PUT, and
        // that PUT's body is the text read.
        let writer = &run.sessions[row["writer_session"].as_str().expect("session")];
        let writer_turn = turn(row, "writer_turn");
        let request = &writer[writer_turn];
        assert!(!request.failed);
        skipped_failure |= writer[..writer_turn].iter().any(|r| r.failed);
        let answer = generate(
            &model(),
            &parse_request(&request.body).expect("request"),
            &request.body,
        )
        .message;
        let put = answer
            .content
            .iter()
            .find_map(|block| match block {
                ResponseBlock::ToolUse { id, input, .. }
                    if *id == row["writer_tool_use_id"].as_str().expect("id") =>
                {
                    Some(input.clone())
                }
                _ => None,
            })
            .expect("the PUT in the writer's answer");
        assert_eq!(put["method"], "PUT");
        assert_eq!(put["url"], url);
        assert_eq!(put["body"].as_str(), Some(text));

        let (writer_name, reader_name) = (&row["writer"], &row["reader"]);
        match kind.as_str() {
            "self_read" => assert_eq!(writer_name, reader_name),
            "transmission" | "reread" => assert_ne!(writer_name, reader_name),
            other => panic!("unexpected kind {other}"),
        }
        let group = |name: &Value| {
            name.as_str()
                .and_then(|n| n.strip_prefix("agent-"))
                .and_then(|n| n.parse::<u64>().ok())
                .map(|i| i / 2)
                .expect("an agent")
        };
        assert_eq!(row["writer_key_group"], group(writer_name));
        assert_eq!(row["reader_key_group"], group(reader_name));
    }
    // The report's counts are the file's rows.
    let count = |kind: &str| by_kind.get(kind).copied().unwrap_or(0);
    assert_eq!(count("transmission"), run.report.expected_transmissions);
    assert_eq!(count("self_read"), run.report.self_reads);
    assert_eq!(count("reread"), run.report.rereads);
    assert_eq!(count("miss"), run.report.wiki_misses);
    assert_eq!(run.report.unattributed_reads, 0);
    // Every request the model received was counted.
    assert_eq!(run.total_recorded as u64, run.report.requests);
    assert!(run.report.failures.contains_key("http 529"));
    (kinds, skipped_failure)
}

#[tokio::test]
async fn ground_truth_rows_point_into_the_requests_sent() {
    let run = swarm(false, 42).await;
    let (kinds, skipped) = check(&run);
    assert!(kinds.contains("transmission"), "{kinds:?}");
    assert!(skipped, "some turn ordinal counts a failed request");
    // Turn ordinals are positions among every request of the session,
    // failed ones included, so they exceed the successes before them.
    assert!(run.sessions.values().any(|s| s.iter().any(|r| r.failed)));
}

#[tokio::test]
async fn ground_truth_holds_in_the_claude_code_shape() {
    let run = swarm(true, 42).await;
    let (kinds, skipped) = check(&run);
    assert!(kinds.contains("transmission"), "{kinds:?}");
    assert!(skipped);
    // The system turns are counted in `content.at.message`.
    let shifted = run.rows.iter().any(|row| {
        row["content"]["at"]["message"]
            .as_u64()
            .is_some_and(|m| m >= 3)
    });
    assert!(shifted);
}

#[tokio::test]
async fn every_row_kind_appears() {
    let mut all = BTreeSet::new();
    for seed in [44, 7] {
        let (kinds, _) = check(&swarm(seed % 2 == 0, seed).await);
        all.extend(kinds);
    }
    // A miss needs a read to beat every write to the page, which the mixed
    // runs only do by timing. A run that only reads, against its own fresh
    // wiki, misses on every read whatever the scheduling.
    let reads_only = swarm_with(
        false,
        9,
        TaskMix::new(
            Fraction::new(0.0).expect("fraction"),
            Fraction::new(1.0).expect("fraction"),
        )
        .expect("mix"),
    )
    .await;
    let (kinds, _) = check(&reads_only);
    assert_eq!(kinds, BTreeSet::from(["miss".to_owned()]));
    assert!(reads_only.report.wiki_misses > 0);
    all.extend(kinds);
    let expected: BTreeSet<String> = ["miss", "reread", "self_read", "transmission"]
        .into_iter()
        .map(str::to_owned)
        .collect();
    assert_eq!(all, expected);
}
