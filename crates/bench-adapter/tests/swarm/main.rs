//! A saved demo-swarm run's files as the adapter reads them, over a
//! synthetic run (`fixture`): the truth file (its header gives the run
//! window, its rows the agents' names), the gateway's verified export, the
//! run window, a replay through the live composition, and `swarm-fetch`
//! over a test API. Labelling and scoring the run is the bench's.

mod fixture;
mod replay;
mod window;

use std::io::Cursor;

use crosstalk_bench_adapter::swarm::detected::read_export;
use crosstalk_bench_adapter::swarm::schema::HexDigest;
use crosstalk_bench_adapter::swarm::truth_file::{self, Row, TruthFileError};
use serde_json::json;

use fixture::{P1, Written};

// ---- the truth file ----

#[test]
fn a_v2_file_reads_every_kind_in_order() {
    let mut text = String::new();
    for row in fixture::truth_rows() {
        text.push_str(&row.to_string());
        text.push('\n');
    }
    let truth = truth_file::read(Cursor::new(text)).expect("a valid truth file");
    assert_eq!(truth.header.world, fixture::WORLD);
    assert_eq!(truth.header.version, 2);
    let kinds: Vec<&str> = truth
        .rows
        .iter()
        .map(|numbered| match &numbered.row {
            Row::Delivery { kind, .. } => match kind {
                truth_file::DeliveryKind::Transmission => "transmission",
                truth_file::DeliveryKind::SelfRead => "self_read",
                truth_file::DeliveryKind::Reread => "reread",
            },
            Row::Miss(_) => "miss",
            Row::Unattributed(_) => "unattributed_read",
            Row::Cluster(_) => "agent_cluster",
            Row::Session(_) => "session",
        })
        .collect();
    assert_eq!(
        kinds,
        [
            "transmission",
            "transmission",
            "self_read",
            "reread",
            "miss",
            "transmission",
            "transmission",
            "agent_cluster"
        ]
    );
    assert_eq!(truth.rows[0].line, 2);
}

fn read_rows(rows: &[serde_json::Value]) -> Result<truth_file::TruthFile, TruthFileError> {
    let mut text = String::new();
    for row in rows {
        text.push_str(&row.to_string());
        text.push('\n');
    }
    truth_file::read(Cursor::new(text))
}

#[test]
fn another_version_is_refused() {
    let mut header = fixture::header();
    header["version"] = json!(1);
    assert!(matches!(
        read_rows(&[header]),
        Err(TruthFileError::UnsupportedVersion { found: 1 })
    ));
}

#[test]
fn an_unknown_kind_or_field_is_refused() {
    let mut row = fixture::truth_rows()[1].clone();
    row["kind"] = json!("delegation");
    assert!(matches!(
        read_rows(&[fixture::header(), row]),
        Err(TruthFileError::Decode { line: 2, .. })
    ));
    let mut row = fixture::truth_rows()[1].clone();
    row["extra"] = json!(true);
    assert!(matches!(
        read_rows(&[fixture::header(), row]),
        Err(TruthFileError::Decode { line: 2, .. })
    ));
}

#[test]
fn a_bad_digest_is_refused() {
    let mut row = fixture::truth_rows()[1].clone();
    row["content"]["blake3"] = json!("xyz");
    assert!(matches!(
        read_rows(&[fixture::header(), row]),
        Err(TruthFileError::Decode { line: 2, .. })
    ));
    let digest = HexDigest::blake3_of(b"abc");
    assert_eq!(HexDigest::parse(&digest.to_string()), Ok(digest));
}

#[test]
fn the_header_comes_first_once_and_rows_share_its_world() {
    let rows = fixture::truth_rows();
    assert!(matches!(
        read_rows(&[rows[1].clone()]),
        Err(TruthFileError::HeaderNotFirst { line: 1 })
    ));
    assert!(matches!(
        read_rows(&[fixture::header(), fixture::header()]),
        Err(TruthFileError::SecondHeader { line: 2 })
    ));
    assert!(matches!(read_rows(&[]), Err(TruthFileError::NoHeader)));
    let mut row = rows[1].clone();
    row["world"] = json!("swarm-other");
    assert!(matches!(
        read_rows(&[fixture::header(), row]),
        Err(TruthFileError::OtherWorld { line: 2, .. })
    ));
}

// ---- the export ----

#[test]
fn the_export_verifies_and_lists_its_transmissions() {
    let dir = fixture::dir("export");
    let written = fixture::write(&dir, &fixture::truth_rows());
    let bytes = std::fs::read(&written.export).expect("the export");
    let exported = read_export(&bytes).expect("a complete export");
    assert_eq!(exported.transmissions.len(), 3);
    // A cut-off export (no trailer) is refused.
    let text = String::from_utf8(bytes).expect("utf-8");
    let without_trailer: String = text
        .lines()
        .filter(|line| !line.starts_with(r#"{"type":"trailer""#))
        .map(|line| format!("{line}\n"))
        .collect();
    assert!(read_export(without_trailer.as_bytes()).is_err());
}

/// Lines from a real local swarm run (synthetic swarm output), verbatim.
const SAMPLE: &str = r#"{"kind":"header","version":2,"world":"swarm-01M45MWNEKJ1H4M3F2A2QECGDQ","run":"01M45MWNEKJ1H4M3F2A2QECGDQ","seed":7,"agents":4,"keys":2,"agents_per_key":3,"claude_code_shape":false,"started_at_unix_ms":1791191045587,"gateway_url":"http://127.0.0.1:18070","wiki_url":"http://127.0.0.1:18091"}
{"kind":"agent_cluster","world":"swarm-01M45MWNEKJ1H4M3F2A2QECGDQ","key_group":0,"agents":["agent-000","agent-001","agent-002"]}
{"kind":"transmission","world":"swarm-01M45MWNEKJ1H4M3F2A2QECGDQ","writer":"agent-000","reader":"agent-002","page":"rate-limiting-0","version":2,"writer_key_group":0,"reader_key_group":0,"writer_session":"9c0870f8-0dc3-49b1-8a44-2f50eccb87fd","writer_turn":0,"writer_tool_use_id":"toolu_01udn93Yxd0Sio7QLxy2g6vM","reader_session":"7367f467-b421-4961-848f-15dd3e5a1061","reader_turn":1,"reader_tool_use_id":"toolu_01VF5dXLfUFzgmMPG8AaCWKl","route":{"kind":"channel","url":"http://127.0.0.1:18091/pages/rate-limiting-0"},"carrier":"tool_result","read_tool":{"name":"http_request","input":{"method":"GET","url":"http://127.0.0.1:18091/pages/rate-limiting-0"}},"content":{"blake3":"3a8e56e9893d2d4abfcc57cd8bfb0394702a760ec24c998dfe3b33a3a1081873","sha256":"8f1953d2eb28b723a1e75bbc807e3cb6747de2f93d93ce49897ab6c5e4196803","excerpt":"revisit per-tenant quota after the next release. We measured leaky bucket on the","at":{"message":2,"block":0,"tool_use_id":"toolu_01VF5dXLfUFzgmMPG8AaCWKl"}},"at_ms":308,"at_unix_ms":1791191045895,"written_at_unix_ms":1791191045859,"read_at_unix_ms":1791191045895}
"#;

#[test]
fn a_real_swarm_run_parses_exactly() {
    use crosstalk_bench_adapter::swarm::schema::{TruthCarrier, TruthRoute};
    let truth = truth_file::read(Cursor::new(SAMPLE)).expect("the sample parses");
    let header = &truth.header;
    assert_eq!(header.version, 2);
    assert_eq!(header.world, "swarm-01M45MWNEKJ1H4M3F2A2QECGDQ");
    assert_eq!(header.run, "01M45MWNEKJ1H4M3F2A2QECGDQ");
    assert_eq!(
        (
            header.seed,
            header.agents,
            header.keys,
            header.agents_per_key
        ),
        (7, 4, 2, 3)
    );
    assert!(!header.claude_code_shape);
    assert_eq!(header.started_at_unix_ms, 1_791_191_045_587);
    assert_eq!(header.gateway_url, "http://127.0.0.1:18070");
    assert_eq!(header.wiki_url, "http://127.0.0.1:18091");
    assert_eq!(truth.rows.len(), 2);
    let Row::Cluster(cluster) = &truth.rows[0].row else {
        panic!("line 2 is a key group");
    };
    assert_eq!(cluster.key_group, 0);
    assert_eq!(cluster.agents, ["agent-000", "agent-001", "agent-002"]);
    let Row::Delivery { kind, row } = &truth.rows[1].row else {
        panic!("line 3 is a delivery");
    };
    assert_eq!(*kind, truth_file::DeliveryKind::Transmission);
    assert_eq!(truth.rows[1].line, 3);
    assert_eq!(
        (row.writer.as_str(), row.reader.as_str()),
        ("agent-000", "agent-002")
    );
    assert_eq!((row.page.as_str(), row.version), ("rate-limiting-0", 2));
    assert_eq!((row.writer_key_group, row.reader_key_group), (0, 0));
    assert_eq!(row.writer_session, "9c0870f8-0dc3-49b1-8a44-2f50eccb87fd");
    assert_eq!(
        (row.writer_turn, row.writer_tool_use_id.as_str()),
        (0, "toolu_01udn93Yxd0Sio7QLxy2g6vM")
    );
    assert_eq!(row.reader_session, "7367f467-b421-4961-848f-15dd3e5a1061");
    assert_eq!(
        (row.reader_turn, row.reader_tool_use_id.as_str()),
        (1, "toolu_01VF5dXLfUFzgmMPG8AaCWKl")
    );
    assert_eq!(
        row.route,
        TruthRoute::Channel {
            url: "http://127.0.0.1:18091/pages/rate-limiting-0".to_owned()
        }
    );
    assert_eq!(row.carrier, TruthCarrier::ToolResult);
    assert_eq!(row.read_tool.name, "http_request");
    assert_eq!(
        row.read_tool.input,
        json!({"method": "GET", "url": "http://127.0.0.1:18091/pages/rate-limiting-0"})
    );
    assert_eq!(
        row.content.blake3.to_string(),
        "3a8e56e9893d2d4abfcc57cd8bfb0394702a760ec24c998dfe3b33a3a1081873"
    );
    assert_eq!(
        row.content.sha256.to_string(),
        "8f1953d2eb28b723a1e75bbc807e3cb6747de2f93d93ce49897ab6c5e4196803"
    );
    assert_eq!(
        row.content.excerpt,
        "revisit per-tenant quota after the next release. We measured leaky bucket on the"
    );
    assert_eq!(
        (
            row.content.at.message,
            row.content.at.block,
            row.content.at.tool_use_id.as_str()
        ),
        (2, 0, "toolu_01VF5dXLfUFzgmMPG8AaCWKl")
    );
    assert_eq!(row.at_ms, 308);
    assert_eq!(row.at_unix_ms, header.started_at_unix_ms + row.at_ms);
    assert_eq!(row.written_at_unix_ms, 1_791_191_045_859);
    assert_eq!(row.read_at_unix_ms, 1_791_191_045_895);
    // Re-encoding gives back the same values, key for key.
    for (line, original) in SAMPLE.lines().enumerate() {
        let parsed: crosstalk_bench_adapter::swarm::schema::TruthLine =
            serde_json::from_str(original).expect("a line");
        let again: serde_json::Value = serde_json::to_value(&parsed).expect("encode");
        let original: serde_json::Value = serde_json::from_str(original).expect("json");
        assert_eq!(again, original, "line {}", line + 1);
    }
}

/// The key sets crates/demo pins for `self_read`, `reread` and `miss`
/// (`tests/truth.rs`, `rows_have_exactly_the_v2_keys`) decode.
#[test]
fn every_v2_kind_decodes_with_the_pinned_keys() {
    let rows = fixture::truth_rows();
    let self_read = rows[3].clone();
    let reread = rows[4].clone();
    let miss = rows[5].clone();
    let delivered = [
        "kind",
        "world",
        "writer",
        "reader",
        "page",
        "version",
        "writer_key_group",
        "reader_key_group",
        "writer_session",
        "writer_turn",
        "writer_tool_use_id",
        "reader_session",
        "reader_turn",
        "reader_tool_use_id",
        "route",
        "carrier",
        "read_tool",
        "content",
        "at_ms",
        "at_unix_ms",
        "written_at_unix_ms",
        "read_at_unix_ms",
    ];
    let miss_keys = [
        "kind",
        "world",
        "reader",
        "reader_key_group",
        "page",
        "reader_session",
        "reader_turn",
        "reader_tool_use_id",
        "read_tool",
        "at_ms",
        "at_unix_ms",
    ];
    let keys = |value: &serde_json::Value| {
        let mut keys: Vec<String> = value
            .as_object()
            .expect("an object")
            .keys()
            .cloned()
            .collect();
        keys.sort();
        keys
    };
    let sorted = |names: &[&str]| {
        let mut names: Vec<String> = names.iter().map(|name| (*name).to_owned()).collect();
        names.sort();
        names
    };
    assert_eq!(keys(&self_read), sorted(&delivered));
    assert_eq!(keys(&reread), sorted(&delivered));
    assert_eq!(keys(&miss), sorted(&miss_keys));
    let truth = read_rows(&[fixture::header(), self_read, reread, miss]).expect("decodes");
    assert_eq!(truth.rows.len(), 3);
}

#[test]
fn an_unattributed_read_decodes_with_the_pinned_keys() {
    let row = fixture::unattributed(("a002", "session-a002", 1, "toolu_r1"), "p1", P1);
    let mut keys: Vec<&str> = row
        .as_object()
        .expect("an object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    let mut pinned = [
        "kind",
        "world",
        "reader",
        "reader_key_group",
        "page",
        "version",
        "reader_session",
        "reader_turn",
        "reader_tool_use_id",
        "read_tool",
        "content",
        "at_ms",
        "at_unix_ms",
    ];
    pinned.sort_unstable();
    assert_eq!(keys, pinned);
    let truth = read_rows(&[fixture::header(), row]).expect("decodes");
    let Row::Unattributed(read) = &truth.rows[0].row else {
        panic!("an unattributed read");
    };
    assert_eq!((read.reader.as_str(), read.version), ("a002", 5));
    let mut extra = fixture::unattributed(("a002", "session-a002", 1, "toolu_r1"), "p1", P1);
    extra["writer"] = json!("a001");
    assert!(matches!(
        read_rows(&[fixture::header(), extra]),
        Err(TruthFileError::Decode { line: 2, .. })
    ));
}

// ---- swarm-fetch ----

/// A request the test API saw: method, path, body.
type Seen = (String, String, String);

/// A one-shot HTTP/1.1 API over the fixture's files: `POST /exports`
/// answers the export, `GET /transmissions/{id}/evidence` the evidence line
/// of that id (or `null`). Returns the base URL and, when it stops, the
/// requests it saw (method, path, body).
fn serve(written: &Written) -> (String, std::thread::JoinHandle<Vec<Seen>>) {
    use std::io::{BufRead, BufReader, Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a port");
    let base = format!("http://{}", listener.local_addr().expect("an address"));
    let export = std::fs::read(&written.export).expect("the export");
    let evidence: Vec<(String, String)> = std::fs::read_to_string(&written.evidence)
        .expect("the evidence")
        .lines()
        .map(|line| {
            let value: serde_json::Value = serde_json::from_str(line).expect("evidence json");
            let id = value["transmission"]["id"]
                .as_str()
                .expect("a transmission id")
                .to_owned();
            (id, line.to_owned())
        })
        .collect();
    let rows = read_export(&export)
        .expect("the export")
        .transmissions
        .len();
    let handle = std::thread::spawn(move || {
        let mut seen = Vec::new();
        for stream in listener.incoming().take(rows + 1) {
            let mut stream = stream.expect("a connection");
            let mut reader = BufReader::new(stream.try_clone().expect("a clone"));
            let mut line = String::new();
            reader.read_line(&mut line).expect("a request line");
            let mut parts = line.split_whitespace();
            let method = parts.next().unwrap_or_default().to_owned();
            let path = parts.next().unwrap_or_default().to_owned();
            let mut length = 0usize;
            loop {
                let mut header = String::new();
                reader.read_line(&mut header).expect("a header");
                if header.trim().is_empty() {
                    break;
                }
                if let Some(value) = header.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = value.trim().parse().expect("a length");
                }
            }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).expect("the body");
            let answer: Vec<u8> = if method == "POST" {
                export.clone()
            } else {
                let id = path
                    .trim_start_matches("/transmissions/")
                    .split('/')
                    .next()
                    .unwrap_or_default();
                evidence
                    .iter()
                    .find(|(known, _)| known == id)
                    .map_or_else(|| b"null".to_vec(), |(_, line)| line.clone().into_bytes())
            };
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                answer.len()
            )
            .expect("a status line");
            stream.write_all(&answer).expect("a body");
            seen.push((method, path, String::from_utf8_lossy(&body).into_owned()));
        }
        seen
    });
    (base, handle)
}

#[test]
fn swarm_fetch_asks_for_discarded_transmissions_and_their_evidence() {
    use crosstalk_bench_adapter::swarm::fetch::{FetchConfig, fetch};
    use crosstalk_spec::support::{TimeWindow, Timestamp};
    let dir = fixture::dir("fetch");
    let written = fixture::write(&dir, &fixture::truth_rows());
    let discarded = fixture::export_discarded(&written);
    let (api, server) = serve(&written);
    let out = dir.join("fetched");
    std::fs::create_dir_all(&out).expect("the output directory");
    let window =
        TimeWindow::new(Timestamp::from_micros(0), Timestamp::from_micros(1)).expect("a window");
    let fetched = fetch(
        &FetchConfig {
            api,
            token: None,
            window,
        },
        &out,
    )
    .expect("the fetch");
    let seen = server.join().expect("the server");
    assert_eq!(fetched.transmissions, 4);
    assert_eq!(fetched.without_evidence, 0);
    let (method, path, body) = &seen[0];
    assert_eq!((method.as_str(), path.as_str()), ("POST", "/exports"));
    let request: serde_json::Value = serde_json::from_str(body).expect("a JSON request");
    assert_eq!(
        request["dataset"]["data"]["states"],
        json!(["confirmed", "classified", "aggregated", "discarded"])
    );
    let asked = format!("/transmissions/{}/evidence", discarded.id.ulid_text());
    assert!(
        seen.iter().any(|(_, path, _)| path.starts_with(&asked)),
        "{seen:?}"
    );
    let saved = std::fs::read_to_string(&fetched.evidence).expect("the evidence");
    assert_eq!(saved.lines().count(), 4);
    assert!(saved.contains("\"discarded\""), "{saved}");
}
