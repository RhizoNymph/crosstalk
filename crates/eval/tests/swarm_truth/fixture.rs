//! A small synthetic swarm run, written as the files a real one leaves:
//! the truth file, the gateway's exchange log and blob directory, and a
//! saved export with its evidence. Built with testkit, never copied.
//!
//! Three agents, one session each, on the wiki at `http://wiki:8090`:
//!
//! | session | turn | request ends with | response |
//! | --- | --- | --- | --- |
//! | a001 | 0 | the task | PUT p1 (`P1`) |
//! | a001 | 1 | PUT p1's result | PUT p2 (`P2`) |
//! | a001 | 2 | PUT p2's result | GET p1 |
//! | a001 | 3 | GET p1 → `P1` (a self-read) | text |
//! | a002 | 0 | the task | GET p1 |
//! | a002 | 1 | GET p1 → `P1` (a transmission from a001) | GET p1 |
//! | a002 | 2 | GET p1 → `P1` (a reread) | GET p9 |
//! | a002 | 3 | GET p9 → not found (a miss) | text |
//! | a003 | 0 | the task | GET p2 |
//! | a003 | 1 | GET p2 → `P2` (a transmission from a001) | GET p1 |
//! | a003 | 2 | GET p1 → `P1` (a transmission from a001) | text |
//!
//! The run's exchanges start one second apart from the header's start
//! (`T0`), inside the run window its rows imply (they end at `T0` + 2 s,
//! plus the default minute of slack). [`write_with_prior_run`] also logs,
//! an hour before, an earlier run that reused a002's session id.

use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crosstalk_eval::datasets::swarm_truth::detected::Blake3RowHasher;
use crosstalk_eval::datasets::swarm_truth::fetch::FETCHED_STATES;
use crosstalk_eval::datasets::swarm_truth::schema::HexDigest;
use crosstalk_spec::aggregates::filter::TopologyFilter;
use crosstalk_spec::aggregates::topic::{EmbeddingModel, TopicModelVersion};
use crosstalk_spec::aliases::NoAliases;
use crosstalk_spec::derived::flow::resource::Resource;
use crosstalk_spec::derived::flow::transmission::Transmission;
use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::{AgentId, ExchangeId, MessageHash};
use crosstalk_spec::interfaces::l2_transport::BlobStore;
use crosstalk_spec::interfaces::l8_surface::evidence::{
    AccessDetail, InvalidEvidence, MatchQuotes, TransmissionEvidence,
};
use crosstalk_spec::interfaces::l8_surface::excerpt::Excerpted;
use crosstalk_spec::interfaces::l8_surface::export::rows::TransmissionRow;
use crosstalk_spec::interfaces::l8_surface::export::{
    ExportBasis, ExportDataset, ExportFormat, ExportHeader, ExportHeaderParts, ExportLine,
    ExportRequest, ExportRow, ExportScope, ExportSealer, ExportStates, GatewayVersion,
    TransmissionScope, settled_window,
};
use crosstalk_spec::interfaces::l8_surface::summary::TopicUnder;
use crosstalk_spec::observed::message::{MessageBody, PartRef, encoding};
use crosstalk_spec::support::{TimeWindow, Timestamp, Watermark};
use crosstalk_testkit::build::message::{
    assistant, assistant_text, system_text, tool_call, tool_result, user_text,
};
use crosstalk_testkit::build::{
    NormalizedExchangeBuilder, ResourceBuilder, TransmissionBuilder, TransmissionParts,
};
use crosstalk_testkit::ids::Ids;
use crosstalk_testkit::time::{T0, after};
use crosstalk_transport::blob::FsBlobStore;
use serde_json::json;

pub const WORLD: &str = "swarm-fixture";
pub const P1: &str = "# Plan\nMeet at the \"north\" gate at noon; bring the ledger.";
pub const P2: &str = "page two holds a plain sentence with no escapes at all";
pub const NOT_FOUND: &str = "404 page not found";

pub fn url(page: &str) -> String {
    format!("http://wiki:8090/pages/{page}")
}

pub fn blake3_hex(text: &str) -> String {
    HexDigest::blake3_of(text.as_bytes()).to_string()
}

/// One captured exchange: its id and the hash of the tool message it
/// ended its request with (if any).
#[derive(Debug, Clone, Copy)]
pub struct Turn {
    pub id: ExchangeId,
    pub response: MessageHash,
    pub last_tool: Option<MessageHash>,
}

/// An agent's session: its exchanges in order, each request the whole
/// history so far.
pub struct Agent {
    pub session: String,
    pub turns: Vec<Turn>,
    history: Vec<MessageBody>,
    credential: crosstalk_spec::observed::client::CredentialRef,
}

impl Agent {
    fn new(ids: &mut Ids, name: &str) -> Self {
        let client = crosstalk_testkit::build::exchange::claude_code_client(ids);
        let credential = client.credential.expect("the testkit client has a key");
        Self {
            session: format!("session-{name}"),
            turns: Vec::new(),
            history: vec![
                system_text("You are a swarm agent."),
                user_text("Keep the wiki."),
            ],
            credential,
        }
    }
}

/// Everything the fixture wrote, and the ids tests check against.
pub struct Written {
    pub truth: PathBuf,
    pub exchanges: PathBuf,
    pub blobs: PathBuf,
    pub export: PathBuf,
    pub evidence: PathBuf,
    pub a001: Vec<Turn>,
    pub a002: Vec<Turn>,
    pub a003: Vec<Turn>,
    /// The gateway's confirmed transmissions, as the export holds them.
    pub transmissions: Vec<Transmission>,
    /// The earlier run's exchanges in a002's session, an hour before the
    /// run (empty unless written by [`write_with_prior_run`]).
    pub prior_a002: Vec<Turn>,
}

struct Log {
    ids: Ids,
    envelopes: Vec<Envelope>,
    bodies: Vec<MessageBody>,
    /// When the exchanges' run started.
    base: Timestamp,
    /// Seconds since `base` of the last exchange.
    clock: u64,
}

impl Log {
    /// One exchange of `agent`: the history as request, then `response`;
    /// afterwards the history holds the response and `result`, if any.
    fn turn(&mut self, agent: &mut Agent, response: MessageBody, result: Option<MessageBody>) {
        self.clock += 1;
        let at = after(self.base, Duration::from_secs(self.clock));
        let session = agent.session.clone();
        let credential = agent.credential;
        let normalized = NormalizedExchangeBuilder::new(&mut self.ids)
            .exchange(|exchange| {
                exchange
                    .session(&session)
                    .credential(Some(credential))
                    .started_at(at)
            })
            .request(agent.history.clone())
            .response(response.clone())
            .build();
        let last_tool = agent
            .history
            .last()
            .filter(|body| matches!(body, MessageBody::Tool(_)))
            .map(encoding::hash);
        agent.turns.push(Turn {
            id: normalized.exchange.meta.id,
            response: encoding::hash(&response),
            last_tool,
        });
        self.bodies.extend(agent.history.iter().cloned());
        self.bodies.push(response.clone());
        agent.history.push(response);
        if let Some(result) = result {
            agent.history.push(result);
        }
        let event = BusEvent::Ingest(IngestEvent::ExchangeCaptured(Box::new(normalized.exchange)));
        let envelope = crosstalk_testkit::build::EnvelopeBuilder::new(&mut self.ids, event)
            .at(at)
            .build();
        self.envelopes.push(envelope);
    }
}

fn put(id: &str, page: &str, body: &str) -> MessageBody {
    assistant(vec![tool_call(
        id,
        "http_request",
        &json!({"method": "PUT", "url": url(page), "body": body}),
    )])
}

fn get(id: &str, page: &str) -> MessageBody {
    assistant(vec![tool_call(
        id,
        "http_request",
        &json!({"method": "GET", "url": url(page)}),
    )])
}

/// The truth file's header line.
pub fn header() -> serde_json::Value {
    json!({"kind": "header", "version": 2, "world": WORLD, "run": "01J00000000000000000000000",
        "seed": 42, "agents": 3, "keys": 3, "agents_per_key": 1, "claude_code_shape": true,
        "started_at_unix_ms": 1_790_812_800_000_u64, "gateway_url": "http://crosstalk:8080/anthropic",
        "wiki_url": "http://wiki:8090"})
}

/// A delivery row of `kind`.
pub fn delivery(
    kind: &str,
    writer: (&str, &str, u32, &str),
    reader: (&str, &str, u32, &str),
    page: &str,
    text: &str,
) -> serde_json::Value {
    let (writer, writer_session, writer_turn, writer_call) = writer;
    let (reader, reader_session, reader_turn, reader_call) = reader;
    json!({"kind": kind, "world": WORLD, "writer": writer, "reader": reader, "page": page,
        "version": 1, "writer_key_group": 0, "reader_key_group": 1,
        "writer_session": writer_session, "writer_turn": writer_turn, "writer_tool_use_id": writer_call,
        "reader_session": reader_session, "reader_turn": reader_turn, "reader_tool_use_id": reader_call,
        "route": {"kind": "channel", "url": url(page)}, "carrier": "tool_result",
        "read_tool": {"name": "http_request", "input": {"method": "GET", "url": url(page)}},
        "content": {"blake3": blake3_hex(text), "sha256": "00".repeat(32),
            "excerpt": text.chars().take(80).collect::<String>(),
            "at": {"message": 3, "block": 0, "tool_use_id": reader_call}},
        "at_ms": 1000, "at_unix_ms": 1_790_812_801_000_u64,
        "written_at_unix_ms": 1_790_812_800_500_u64, "read_at_unix_ms": 1_790_812_801_000_u64})
}

/// The truth rows the fixture's run implies, plus two the exchange log
/// contradicts: a003's read of p1 named at turn 1 (it is at turn 2), and
/// a003's read of p2 claimed with another body's hash.
pub fn truth_rows() -> Vec<serde_json::Value> {
    let a001 = |turn, call| ("a001", "session-a001", turn, call);
    let a002 = |turn, call| ("a002", "session-a002", turn, call);
    let a003 = |turn, call| ("a003", "session-a003", turn, call);
    vec![
        header(),
        // Line 2: the transmission the gateway finds.
        delivery(
            "transmission",
            a001(0, "toolu_w1"),
            a002(1, "toolu_r1"),
            "p1",
            P1,
        ),
        // Line 3: the transmission the gateway misses.
        delivery(
            "transmission",
            a001(1, "toolu_w2"),
            a003(1, "toolu_r3"),
            "p2",
            P2,
        ),
        // Line 4: a001 reads its own page.
        delivery(
            "self_read",
            a001(0, "toolu_w1"),
            a001(3, "toolu_self"),
            "p1",
            P1,
        ),
        // Line 5: a002 reads p1 again.
        delivery("reread", a001(0, "toolu_w1"), a002(2, "toolu_r2"), "p1", P1),
        // Line 6: a002 reads a page nobody wrote.
        json!({"kind": "miss", "world": WORLD, "reader": "a002", "reader_key_group": 1, "page": "p9",
            "reader_session": "session-a002", "reader_turn": 3, "reader_tool_use_id": "toolu_m",
            "read_tool": {"name": "http_request", "input": {"method": "GET", "url": url("p9")}},
            "at_ms": 2000, "at_unix_ms": 1_790_812_802_000_u64}),
        // Line 7: the turn is off by one.
        delivery(
            "transmission",
            a001(0, "toolu_w1"),
            a003(1, "toolu_r4"),
            "p1",
            P1,
        ),
        // Line 8: the hash is another body's.
        delivery(
            "transmission",
            a001(1, "toolu_w2"),
            a003(1, "toolu_r3"),
            "p2",
            "a different body",
        ),
        json!({"kind": "agent_cluster", "world": WORLD, "key_group": 0, "agents": ["a001"]}),
    ]
}

/// Writes `rows` as JSONL to `path`.
pub fn write_jsonl(path: &Path, rows: &[serde_json::Value]) {
    let mut text = String::new();
    for row in rows {
        text.push_str(&row.to_string());
        text.push('\n');
    }
    std::fs::write(path, text).expect("write a fixture file");
}

/// A fresh, empty directory for one test.
pub fn dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("swarm_truth")
        .join(name);
    if dir.exists() {
        std::fs::remove_dir_all(&dir).expect("clear the fixture directory");
    }
    std::fs::create_dir_all(&dir).expect("create the fixture directory");
    dir
}

/// Gateway-side ids of the three agents; a001 is split in two to model a
/// detector that does not see one agent behind both of its accesses.
struct GatewayAgents {
    a001: AgentId,
    a001_split: AgentId,
    a002: AgentId,
}

fn channel_transmission(
    ids: &mut Ids,
    (from, to): (AgentId, AgentId),
    resource: &Resource,
    write: (ExchangeId, MessageHash),
    read: (ExchangeId, MessageHash),
    text: &str,
    at_secs: u64,
) -> TransmissionParts {
    let length = u32::try_from(text.len()).expect("a short page");
    TransmissionBuilder::new(ids)
        .between(from, to)
        .opened_at(after(T0, Duration::from_secs(at_secs)))
        .matched(NonZeroU32::new(length).expect("a non-empty page"))
        .accesses(|cross| {
            cross
                .resource(resource.id)
                .write_access(|access| {
                    access.in_exchange(write.0).part(PartRef {
                        message: write.1,
                        index: 0,
                    })
                })
                .read_access(|access| {
                    access.in_exchange(read.0).part(PartRef {
                        message: read.1,
                        index: 0,
                    })
                })
        })
        .build_parts()
        .expect("a valid transmission")
}

/// Writes the whole fixture under `dir`: the truth file holds `truth`.
pub fn write(dir: &Path, truth: &[serde_json::Value]) -> Written {
    write_runs(dir, truth, false)
}

/// [`write`], with an earlier run an hour before in the same exchange log:
/// a002's session id reused, its first two turns (`GET p1`, then the read
/// of `P1` with the same tool use id), and a confirmed detection a001 →
/// a002 read in that earlier run's second turn.
pub fn write_with_prior_run(dir: &Path, truth: &[serde_json::Value]) -> Written {
    write_runs(dir, truth, true)
}

fn write_runs(dir: &Path, truth: &[serde_json::Value], prior: bool) -> Written {
    let mut log = Log {
        ids: Ids::seeded(7),
        envelopes: Vec::new(),
        bodies: Vec::new(),
        base: Timestamp::from_micros(T0.as_micros() - 3_600_000_000),
        clock: 0,
    };
    let mut prior_a002 = prior.then(|| Agent::new(&mut log.ids, "a002"));
    if let Some(agent) = &mut prior_a002 {
        log.turn(
            agent,
            get("toolu_r1", "p1"),
            Some(tool_result("toolu_r1", P1)),
        );
        log.turn(
            agent,
            get("toolu_r2", "p1"),
            Some(tool_result("toolu_r2", P1)),
        );
    }
    let prior_a002 = prior_a002.map_or_else(Vec::new, |agent| agent.turns);
    log.base = T0;
    log.clock = 0;
    let mut a001 = Agent::new(&mut log.ids, "a001");
    let mut a002 = Agent::new(&mut log.ids, "a002");
    let mut a003 = Agent::new(&mut log.ids, "a003");
    log.turn(
        &mut a001,
        put("toolu_w1", "p1", P1),
        Some(tool_result("toolu_w1", "ok")),
    );
    log.turn(
        &mut a001,
        put("toolu_w2", "p2", P2),
        Some(tool_result("toolu_w2", "ok")),
    );
    log.turn(
        &mut a001,
        get("toolu_self", "p1"),
        Some(tool_result("toolu_self", P1)),
    );
    log.turn(&mut a001, assistant_text("done"), None);
    log.turn(
        &mut a002,
        get("toolu_r1", "p1"),
        Some(tool_result("toolu_r1", P1)),
    );
    log.turn(
        &mut a002,
        get("toolu_r2", "p1"),
        Some(tool_result("toolu_r2", P1)),
    );
    log.turn(
        &mut a002,
        get("toolu_m", "p9"),
        Some(tool_result("toolu_m", NOT_FOUND)),
    );
    log.turn(&mut a002, assistant_text("done"), None);
    log.turn(
        &mut a003,
        get("toolu_r3", "p2"),
        Some(tool_result("toolu_r3", P2)),
    );
    log.turn(
        &mut a003,
        get("toolu_r4", "p1"),
        Some(tool_result("toolu_r4", P1)),
    );
    log.turn(&mut a003, assistant_text("done"), None);

    // The exchange log and the blobs.
    let exchanges_dir = dir.join("data").join("exchanges");
    std::fs::create_dir_all(&exchanges_dir).expect("create the exchanges directory");
    let exchanges = exchanges_dir.join("exchange-log.jsonl");
    let mut text = String::new();
    for envelope in &log.envelopes {
        text.push_str(&serde_json::to_string(envelope).expect("encode an envelope"));
        text.push('\n');
    }
    std::fs::write(&exchanges, text).expect("write the exchange log");
    let blobs = dir.join("data").join("blobs");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime");
    runtime.block_on(async {
        let store = FsBlobStore::open(blobs.clone())
            .await
            .expect("open the blob store");
        for body in &log.bodies {
            let hash = store
                .put(&encoding::encode(body))
                .await
                .expect("put a body");
            assert_eq!(hash, encoding::hash(body));
        }
    });

    // The gateway's detections.
    let mut ids = Ids::seeded(11);
    let gateway = GatewayAgents {
        a001: ids.agent(),
        a001_split: ids.agent(),
        a002: ids.agent(),
    };
    let p1 = ResourceBuilder::new(&mut ids)
        .url("http", "wiki:8090", "/pages/p1", None)
        .build();
    let found = channel_transmission(
        &mut ids,
        (gateway.a001, gateway.a002),
        &p1,
        (a001.turns[0].id, a001.turns[0].response),
        (
            a002.turns[1].id,
            a002.turns[1].last_tool.expect("a tool result"),
        ),
        P1,
        100,
    );
    let self_read = channel_transmission(
        &mut ids,
        (gateway.a001, gateway.a001_split),
        &p1,
        (a001.turns[0].id, a001.turns[0].response),
        (
            a001.turns[3].id,
            a001.turns[3].last_tool.expect("a tool result"),
        ),
        P1,
        200,
    );
    let reread = channel_transmission(
        &mut ids,
        (gateway.a001, gateway.a002),
        &p1,
        (a001.turns[0].id, a001.turns[0].response),
        (
            a002.turns[2].id,
            a002.turns[2].last_tool.expect("a tool result"),
        ),
        P1,
        300,
    );
    let mut parts = vec![found, self_read, reread];
    if let Some(read) = prior_a002.get(1) {
        parts.push(channel_transmission(
            &mut ids,
            (gateway.a001, gateway.a002),
            &p1,
            (a001.turns[0].id, a001.turns[0].response),
            (read.id, read.last_tool.expect("a tool result")),
            P1,
            50,
        ));
    }
    let evidence: Vec<TransmissionEvidence> =
        parts.iter().map(|parts| evidence_of(parts, &p1)).collect();
    let transmissions: Vec<Transmission> =
        parts.into_iter().map(|parts| parts.transmission).collect();

    let export = dir.join("export.jsonl");
    write_export(
        &mut ids,
        &export,
        &transmissions,
        &ExportStates::confirmed(),
    );
    let evidence_path = dir.join("evidence.jsonl");
    let mut text = String::new();
    for item in &evidence {
        text.push_str(&serde_json::to_string(item).expect("encode evidence"));
        text.push('\n');
    }
    std::fs::write(&evidence_path, text).expect("write the evidence");

    let truth_path = dir.join("truth.jsonl");
    write_jsonl(&truth_path, truth);
    Written {
        truth: truth_path,
        exchanges,
        blobs,
        export,
        evidence: evidence_path,
        a001: a001.turns,
        a002: a002.turns,
        a003: a003.turns,
        transmissions,
        prior_a002,
    }
}

/// The evidence of a built transmission: its matches (bodies not
/// quoted) and its write and read with the page's resource.
fn evidence_of(parts: &TransmissionParts, resource: &Resource) -> TransmissionEvidence {
    let accesses = [parts.write.clone(), parts.read.clone()];
    TransmissionEvidence::assemble(
        parts.transmission.clone(),
        |content| {
            Ok::<_, InvalidEvidence>(MatchQuotes {
                origin: Excerpted::BodyDropped {
                    message: content.read_at().part.message,
                },
                read: Excerpted::BodyDropped {
                    message: content.read_at().part.message,
                },
            })
        },
        |asked| {
            let access = accesses
                .iter()
                .find(|access| access.id == asked)
                .cloned()
                .ok_or(InvalidEvidence::WrongAccess { asked, got: asked })?;
            AccessDetail::new(access, resource.clone(), NoAliases)
        },
    )
    .expect("evidence")
}

/// Writes a sealed transmissions export of `transmissions` in `states`.
fn write_export(ids: &mut Ids, path: &Path, transmissions: &[Transmission], states: &ExportStates) {
    let window = TimeWindow::new(T0, after(T0, Duration::from_secs(86_400))).expect("a window");
    let filter = TopologyFilter::default();
    let scope = TransmissionScope {
        states: states.clone(),
        ..TransmissionScope::confirmed(ExportScope {
            window,
            filter: filter.clone(),
        })
    };
    let request = ExportRequest::new(
        ExportDataset::Transmissions(scope),
        ExportFormat::Jsonl,
        false,
    )
    .expect("a request");
    let watermark = Watermark(after(T0, Duration::from_secs(3_600)));
    let version = TopicModelVersion(1);
    let mut rows: Vec<TransmissionRow> = transmissions
        .iter()
        .map(|transmission| {
            TransmissionRow::of_in_scope(
                transmission,
                NoAliases,
                |_| None,
                |_| TopicUnder::Unassigned,
                None,
                states,
            )
            .expect("a row")
        })
        .collect();
    rows.sort_by_key(|row| (row.at(), row.summary().id));
    let header = ExportHeader::new(ExportHeaderParts {
        id: ids.id(),
        request,
        by: ids.operator(),
        started_at: after(T0, Duration::from_secs(3_700)),
        watermark,
        basis: ExportBasis::Scoped {
            topic_version: version,
            filter: filter.pinned(version),
            settled: settled_window(window, watermark),
        },
        embedding_model: EmbeddingModel {
            name: "test-embedding".to_owned(),
            dimension: std::num::NonZeroU16::new(8).expect("non-zero"),
        },
        gateway: GatewayVersion::new("0.1.0-test").expect("a version"),
        rows: rows.len() as u64,
    })
    .expect("a header");
    let mut sealer = ExportSealer::new(&header, Blake3RowHasher::default());
    let mut lines = vec![ExportLine::Header(Box::new(header.clone()))];
    for row in rows {
        let row = ExportRow::Transmission(Box::new(row));
        sealer.push(&row).expect("a row the sealer takes");
        lines.push(ExportLine::Row(row));
    }
    lines.push(ExportLine::Trailer(sealer.finish()));
    let mut text = String::new();
    for line in &lines {
        text.push_str(&serde_json::to_string(line).expect("encode an export line"));
        text.push('\n');
    }
    std::fs::write(path, text).expect("write the export");
}

/// An `unattributed_read` row, in the demo's pinned key order: a read of
/// `page` whose write was never logged.
pub fn unattributed(reader: (&str, &str, u32, &str), page: &str, text: &str) -> serde_json::Value {
    let (reader, reader_session, reader_turn, reader_call) = reader;
    json!({"kind": "unattributed_read", "world": WORLD, "reader": reader, "reader_key_group": 1,
        "page": page, "version": 5, "reader_session": reader_session, "reader_turn": reader_turn,
        "reader_tool_use_id": reader_call,
        "read_tool": {"name": "http_request", "input": {"method": "GET", "url": url(page)}},
        "content": {"blake3": blake3_hex(text), "sha256": "00".repeat(32),
            "excerpt": text.chars().take(80).collect::<String>(),
            "at": {"message": 3, "block": 0, "tool_use_id": reader_call}},
        "at_ms": 1000, "at_unix_ms": 1_790_812_801_000_u64})
}

/// Appends to the fixture's evidence file one access-only transmission
/// a001 → a003 over p2 (the delivery the gateway's content matching
/// missed): `discarded` or else suspected. Such a transmission has no row
/// in the transmissions export, which holds confirmed ones only, so it
/// reaches the benchmark through its evidence alone.
pub fn append_access_only(written: &Written, discarded: bool) {
    let (parts, p2) = access_only(written, discarded);
    append_evidence(written, &evidence_of(&parts, &p2));
}

/// The access-only transmission a001 → a003 over p2, and p2's resource.
fn access_only(written: &Written, discarded: bool) -> (TransmissionParts, Resource) {
    let mut ids = Ids::seeded(23);
    let (writer, reader) = (ids.agent(), ids.agent());
    let p2 = ResourceBuilder::new(&mut ids)
        .url("http", "wiki:8090", "/pages/p2", None)
        .build();
    let builder = TransmissionBuilder::new(&mut ids)
        .between(writer, reader)
        .opened_at(after(T0, Duration::from_secs(400)))
        .accesses(|cross| {
            cross
                .resource(p2.id)
                .write_access(|access| {
                    access.in_exchange(written.a001[1].id).part(PartRef {
                        message: written.a001[1].response,
                        index: 0,
                    })
                })
                .read_access(|access| {
                    access.in_exchange(written.a003[1].id).part(PartRef {
                        message: written.a003[1].last_tool.expect("a tool result"),
                        index: 0,
                    })
                })
        });
    let builder = if discarded {
        builder.discarded()
    } else {
        builder.suspected()
    };
    (
        builder.build_parts().expect("an access-only transmission"),
        p2,
    )
}

fn append_evidence(written: &Written, evidence: &TransmissionEvidence) {
    let mut text = std::fs::read_to_string(&written.evidence).expect("read the evidence");
    text.push_str(&serde_json::to_string(evidence).expect("encode evidence"));
    text.push('\n');
    std::fs::write(&written.evidence, text).expect("write the evidence");
}

/// Rewrites the fixture's export as `ct-eval swarm-fetch` asks for it (the
/// confirmed and the discarded transmissions), with the discarded a001 →
/// a003 transmission over p2 as a row of its own, and appends its evidence.
/// Returns the discarded transmission.
pub fn export_discarded(written: &Written) -> Transmission {
    let (parts, p2) = access_only(written, true);
    append_evidence(written, &evidence_of(&parts, &p2));
    let mut transmissions = written.transmissions.clone();
    transmissions.push(parts.transmission.clone());
    let states = ExportStates::new(FETCHED_STATES.to_vec()).expect("the fetched states");
    write_export(
        &mut Ids::seeded(29),
        &written.export,
        &transmissions,
        &states,
    );
    parts.transmission
}
