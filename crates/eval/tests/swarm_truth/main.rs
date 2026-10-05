//! The demo swarm benchmark over a synthetic run (`fixture`): truth
//! parsing, the join of truth rows to captured exchanges, and the scoring
//! of a saved export.

mod fixture;
mod scenario;
mod sessions;
mod window;

use std::io::Cursor;

use crosstalk_eval::datasets::swarm_truth::detected::read_export;
use crosstalk_eval::datasets::swarm_truth::schema::HexDigest;
use crosstalk_eval::datasets::swarm_truth::truth_file::{self, Row, TruthFileError};
use crosstalk_eval::datasets::swarm_truth::{
    Effect, Inputs, JoinFailure, RowKind, Side, SwarmOutcome, default_blobs, default_evidence, run,
};
use crosstalk_eval::keys::{AgentKey, WorldKey};
use crosstalk_eval::predict::EvidenceClass;
use crosstalk_eval::report::Gates;
use crosstalk_eval::truth::{CarrierKind, MatchNeed, NegativeReason, RouteExpectation, Tier};
use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::aggregates::quality::MatchClass;
use crosstalk_spec::derived::flow::resource::{Host, Locator};
use serde_json::json;

use fixture::{P1, P2, Written};

fn key(name: &str) -> AgentKey {
    AgentKey::new(WorldKey::new(fixture::WORLD), name)
}

fn inputs(written: &Written) -> Inputs {
    Inputs {
        truth: written.truth.clone(),
        exchanges: written.exchanges.clone(),
        blobs: written.blobs.clone(),
        export: written.export.clone(),
        evidence: written.evidence.clone(),
    }
}

fn scored(name: &str) -> (Written, SwarmOutcome) {
    let dir = fixture::dir(name);
    let written = fixture::write(&dir, &fixture::truth_rows());
    let outcome = run(&inputs(&written), 50, &Gates::default()).expect("the run scores");
    (written, outcome)
}

fn p1_locator() -> Locator {
    Locator::Url {
        scheme: "http".to_owned(),
        host: Host("wiki:8090".to_owned()),
        path: "/pages/p1".to_owned(),
        query: None,
    }
}

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

// ---- the join ----

#[test]
fn a_transmission_joins_to_the_readers_exchange_and_tool_result() {
    let (written, outcome) = scored("join");
    let truth = outcome_truth(&written);
    let label = truth
        .iter()
        .find_map(|expectation| match expectation {
            crosstalk_eval::truth::Expectation::Transmission(expected)
                if expected.label().source.path == "line/2" =>
            {
                Some(expected.label().clone())
            }
            _ => None,
        })
        .expect("line 2 is labelled");
    assert_eq!(label.from, key("a001"));
    assert_eq!(label.to, key("a002"));
    assert_eq!(label.reader_exchange, written.a002[1].id);
    assert_eq!(label.sender_exchange, Some(written.a001[0].id));
    assert_eq!(
        label.route,
        RouteExpectation::Channel {
            resource: p1_locator()
        }
    );
    assert_eq!(label.carrier, CarrierKind::ToolResult);
    assert_eq!(label.tier, Tier::Construction);
    assert_eq!(label.content.text, P1);
    assert_eq!(
        label.content.at.part.message,
        written.a002[1].last_tool.expect("a tool result")
    );
    assert_eq!(label.content.at.part.index, 0);
    assert_eq!(label.content.at.range.start(), 0);
    assert_eq!(label.content.at.range.end() as usize, P1.len());
    // P1 holds a quote and a newline, which the writer's PUT escapes: one
    // JSON string level (spec #58), not normalization.
    assert_eq!(label.needs, MatchNeed::json_string());
    assert_eq!(outcome.resolved.transmissions, 3);
    assert_eq!(outcome.resolved.without_sender, 0);
}

/// The world's labels, resolved again from the fixture's files.
fn outcome_truth(written: &Written) -> Vec<crosstalk_eval::truth::Expectation> {
    use crosstalk_eval::datasets::swarm_truth::bodies::{BlobBodies, Cached};
    use crosstalk_eval::datasets::swarm_truth::exchange_log::{self, Sessions};
    let text = std::fs::read_to_string(&written.truth).expect("the truth file");
    let truth = truth_file::read(Cursor::new(text)).expect("a valid truth file");
    let log = exchange_log::read(&written.exchanges).expect("the exchange log");
    let sessions = Sessions::index(log.exchanges);
    let mut bodies = Cached::new(BlobBodies::open(&written.blobs).expect("the blobs"));
    let resolved = crosstalk_eval::datasets::swarm_truth::resolve(
        &truth,
        "truth.jsonl",
        &sessions,
        &mut bodies,
    )
    .expect("resolves");
    resolved.world.truth().to_vec()
}

#[test]
fn self_reads_rereads_and_misses_become_controls() {
    let (written, outcome) = scored("controls");
    let truth = outcome_truth(&written);
    let controls: Vec<_> = truth
        .iter()
        .filter_map(|expectation| match expectation {
            crosstalk_eval::truth::Expectation::NoTransmission(control) => {
                Some(control.label().clone())
            }
            _ => None,
        })
        .collect();
    let self_read = controls
        .iter()
        .find(|control| control.reason == NegativeReason::SelfRead)
        .expect("a self-read control");
    assert_eq!(self_read.from, key("a001"));
    assert_eq!(self_read.to, key("a001"));
    assert_eq!(self_read.reader_exchange, Some(written.a001[3].id));
    let reread = controls
        .iter()
        .find(|control| control.reason == NegativeReason::Reread)
        .expect("a reread control");
    assert_eq!((&reread.from, &reread.to), (&key("a001"), &key("a002")));
    assert_eq!(reread.reader_exchange, Some(written.a002[2].id));
    let misses: Vec<_> = controls
        .iter()
        .filter(|control| control.reason == NegativeReason::Miss)
        .collect();
    let senders: Vec<&AgentKey> = misses.iter().map(|control| &control.from).collect();
    assert_eq!(senders, [&key("a001"), &key("a003")]);
    assert!(misses.iter().all(|control| control.to == key("a002")
        && control.reader_exchange == Some(written.a002[3].id)
        && control.at.is_some()));
    assert_eq!(outcome.resolved.self_reads, 1);
    assert_eq!(outcome.resolved.rereads, 1);
    assert_eq!(outcome.resolved.misses, 1);
    assert_eq!(outcome.resolved.miss_controls, 2);
}

#[test]
fn a_turn_mismatch_is_reported_and_joined_by_the_tool_result() {
    let (written, outcome) = scored("turn-mismatch");
    let mismatches: Vec<_> = outcome.diagnostics.named("turn_mismatch").collect();
    assert_eq!(mismatches.len(), 1);
    let diagnostic = mismatches[0];
    assert_eq!(diagnostic.line, Some(7));
    assert_eq!(diagnostic.row, Some(RowKind::Transmission));
    assert_eq!(diagnostic.side, Side::Reader);
    assert_eq!(diagnostic.effect, Effect::Kept);
    assert_eq!(
        diagnostic.failure,
        JoinFailure::TurnMismatch {
            session: "session-a003".to_owned(),
            turn: 1,
            found_turn: 2,
            exchange: written.a003[2].id,
        }
    );
}

#[test]
fn a_hash_mismatch_is_reported_and_drops_the_row() {
    let (written, outcome) = scored("hash-mismatch");
    let mismatches: Vec<_> = outcome.diagnostics.named("hash_mismatch").collect();
    assert_eq!(mismatches.len(), 1);
    let diagnostic = mismatches[0];
    assert_eq!(diagnostic.line, Some(8));
    assert_eq!(diagnostic.side, Side::Reader);
    assert_eq!(diagnostic.effect, Effect::Dropped);
    assert_eq!(
        diagnostic.failure,
        JoinFailure::HashMismatch {
            session: "session-a003".to_owned(),
            turn: 1,
            tool_use_id: "toolu_r3".to_owned(),
            exchange: written.a003[1].id,
        }
    );
    assert_eq!(outcome.resolved.dropped, 1);
    let rendered = outcome.diagnostics.render();
    assert!(rendered.contains("| transmission | reader | hash_mismatch | dropped | 1 |"));
    assert!(rendered.contains("| transmission | reader | turn_mismatch | kept | 1 |"));
}

#[test]
fn key_groups_are_reported_not_labelled_as_clusters() {
    let (_, outcome) = scored("key-groups");
    assert_eq!(outcome.key_groups, 1);
    let noted: Vec<_> = outcome
        .diagnostics
        .named("key_group_not_a_cluster")
        .collect();
    assert_eq!(noted.len(), 1);
    assert_eq!(noted[0].effect, Effect::Noted);
}

#[test]
fn an_unknown_session_and_a_missing_turn_are_reported() {
    let dir = fixture::dir("missing");
    let mut rows = vec![fixture::header()];
    rows.push(fixture::delivery(
        "transmission",
        ("a001", "session-a001", 0, "toolu_w1"),
        ("a009", "session-a009", 1, "toolu_r1"),
        "p1",
        P1,
    ));
    rows.push(fixture::delivery(
        "transmission",
        ("a001", "session-a001", 0, "toolu_w1"),
        ("a002", "session-a002", 9, "toolu_nowhere"),
        "p1",
        P1,
    ));
    rows.push(fixture::delivery(
        "transmission",
        ("a001", "session-a001", 0, "toolu_nowhere"),
        ("a003", "session-a003", 1, "toolu_r3"),
        "p2",
        P2,
    ));
    let written = fixture::write(&dir, &rows);
    let outcome = run(&inputs(&written), 50, &Gates::default()).expect("the run scores");
    let failures: Vec<(Option<usize>, Side, &str, Effect)> = outcome
        .diagnostics
        .entries
        .iter()
        .map(|entry| (entry.line, entry.side, entry.failure.name(), entry.effect))
        .collect();
    assert_eq!(
        failures,
        [
            (Some(2), Side::Reader, "unknown_session", Effect::Dropped),
            (Some(3), Side::Reader, "turn_out_of_range", Effect::Dropped),
            (
                Some(4),
                Side::Writer,
                "tool_use_missing",
                Effect::KeptWithoutSender
            ),
        ]
    );
    assert_eq!(outcome.resolved.transmissions, 1);
    assert_eq!(outcome.resolved.without_sender, 1);
}

// ---- scoring the export ----

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

#[test]
fn a_found_transmission_is_a_true_positive() {
    let (_, outcome) = scored("tp");
    let row = outcome
        .report
        .rows
        .iter()
        .find(|row| {
            row.key.route == RouteKind::Channel
                && row.key.carrier == CarrierKind::ToolResult
                && row.key.class == EvidenceClass::from(MatchClass::Decoded)
                && row.key.tier == Some(Tier::Construction)
        })
        .expect("the decoded channel row");
    // Lines 2 and 7 carry P1; the gateway found line 2's.
    assert_eq!(row.counts.expected, 2);
    assert_eq!(row.counts.found, 1);
    assert_eq!(outcome.detected.exported, 3);
    assert_eq!(outcome.detected.evidence, 3);
    assert_eq!(outcome.detected.predictions, 3);
}

#[test]
fn a_missed_transmission_is_a_false_negative() {
    let (written, outcome) = scored("fn");
    let row = outcome
        .report
        .rows
        .iter()
        .find(|row| {
            row.key.class == EvidenceClass::from(MatchClass::Exact)
                && row.key.tier == Some(Tier::Construction)
        })
        .expect("the exact row");
    assert_eq!(row.counts.expected, 1);
    assert_eq!(row.counts.missed, 1);
    let missed: Vec<_> = outcome
        .report
        .misses
        .iter()
        .map(|miss| miss.expectation.label().reader_exchange)
        .collect();
    assert!(missed.contains(&written.a003[1].id));
}

#[test]
fn a_self_read_reported_as_a_transmission_is_a_violation() {
    let (_, outcome) = scored("violation");
    let count = |reason: NegativeReason| {
        outcome
            .report
            .violations
            .iter()
            .filter(|row| row.reason == reason)
            .map(|row| row.count)
            .sum::<u64>()
    };
    assert_eq!(count(NegativeReason::SelfRead), 1);
    assert_eq!(count(NegativeReason::Reread), 1);
    assert_eq!(count(NegativeReason::Miss), 0);
    let false_positives: u64 = outcome
        .report
        .rows
        .iter()
        .map(|row| row.counts.false_positive)
        .sum();
    let correct: u64 = outcome
        .report
        .rows
        .iter()
        .map(|row| row.counts.correct)
        .sum();
    assert_eq!((correct, false_positives), (1, 2));
}

#[test]
fn an_export_row_without_evidence_is_reported() {
    let dir = fixture::dir("no-evidence");
    let written = fixture::write(&dir, &fixture::truth_rows());
    let text = std::fs::read_to_string(&written.evidence).expect("the evidence");
    let first_only: String = text
        .lines()
        .take(1)
        .map(|line| format!("{line}\n"))
        .collect();
    std::fs::write(&written.evidence, first_only).expect("rewrite the evidence");
    let outcome = run(&inputs(&written), 50, &Gates::default()).expect("the run scores");
    let missing: Vec<_> = outcome.diagnostics.named("missing_evidence").collect();
    assert_eq!(missing.len(), 2);
    assert!(
        missing
            .iter()
            .all(|entry| entry.effect == Effect::PredictionsDropped)
    );
}

#[test]
fn a_rerun_is_byte_identical() {
    let (_, first) = scored("rerun-a");
    let (_, second) = scored("rerun-b");
    let encode = |outcome: &SwarmOutcome| {
        serde_json::to_string(&(&outcome.report, &outcome.diagnostics)).expect("encode")
    };
    assert_eq!(encode(&first), encode(&second));
}

#[test]
fn default_paths_follow_the_gateways_layout() {
    let log = std::path::Path::new("/data/exchanges/exchange-log.jsonl");
    assert_eq!(default_blobs(log), std::path::Path::new("/data/blobs"));
    let export = std::path::Path::new("/runs/1/export.jsonl");
    assert_eq!(
        default_evidence(export),
        std::path::Path::new("/runs/1/evidence.jsonl")
    );
}

#[test]
fn only_a_self_read_control_names_one_agent_twice() {
    use crosstalk_eval::keys::SourceRef;
    use crosstalk_eval::truth::{InvalidLabel, NegativeControl, NegativeLabel};
    let label = |reason| NegativeLabel {
        from: key("a001"),
        to: key("a001"),
        reader_exchange: None,
        at: None,
        origin: None,
        text: Some("x".to_owned()),
        reason,
        tier: Tier::Construction,
        source: SourceRef::new("truth.jsonl", "line/1"),
    };
    // Bounded by nothing: refused for being unbounded, not for the pair.
    assert_eq!(
        NegativeControl::new(label(NegativeReason::SelfRead)),
        Err(InvalidLabel::Unbounded)
    );
    assert_eq!(
        NegativeControl::new(label(NegativeReason::Reread)),
        Err(InvalidLabel::SelfTransmission(key("a001")))
    );
    assert_eq!(
        NegativeControl::new(label(NegativeReason::Miss)),
        Err(InvalidLabel::SelfTransmission(key("a001")))
    );
}

/// Lines from a real local swarm run (synthetic swarm output), verbatim.
const SAMPLE: &str = r#"{"kind":"header","version":2,"world":"swarm-01M45MWNEKJ1H4M3F2A2QECGDQ","run":"01M45MWNEKJ1H4M3F2A2QECGDQ","seed":7,"agents":4,"keys":2,"agents_per_key":3,"claude_code_shape":false,"started_at_unix_ms":1791191045587,"gateway_url":"http://127.0.0.1:18070","wiki_url":"http://127.0.0.1:18091"}
{"kind":"agent_cluster","world":"swarm-01M45MWNEKJ1H4M3F2A2QECGDQ","key_group":0,"agents":["agent-000","agent-001","agent-002"]}
{"kind":"transmission","world":"swarm-01M45MWNEKJ1H4M3F2A2QECGDQ","writer":"agent-000","reader":"agent-002","page":"rate-limiting-0","version":2,"writer_key_group":0,"reader_key_group":0,"writer_session":"9c0870f8-0dc3-49b1-8a44-2f50eccb87fd","writer_turn":0,"writer_tool_use_id":"toolu_01udn93Yxd0Sio7QLxy2g6vM","reader_session":"7367f467-b421-4961-848f-15dd3e5a1061","reader_turn":1,"reader_tool_use_id":"toolu_01VF5dXLfUFzgmMPG8AaCWKl","route":{"kind":"channel","url":"http://127.0.0.1:18091/pages/rate-limiting-0"},"carrier":"tool_result","read_tool":{"name":"http_request","input":{"method":"GET","url":"http://127.0.0.1:18091/pages/rate-limiting-0"}},"content":{"blake3":"3a8e56e9893d2d4abfcc57cd8bfb0394702a760ec24c998dfe3b33a3a1081873","sha256":"8f1953d2eb28b723a1e75bbc807e3cb6747de2f93d93ce49897ab6c5e4196803","excerpt":"revisit per-tenant quota after the next release. We measured leaky bucket on the","at":{"message":2,"block":0,"tool_use_id":"toolu_01VF5dXLfUFzgmMPG8AaCWKl"}},"at_ms":308,"at_unix_ms":1791191045895,"written_at_unix_ms":1791191045859,"read_at_unix_ms":1791191045895}
"#;

#[test]
fn a_real_swarm_run_parses_exactly() {
    use crosstalk_eval::datasets::swarm_truth::schema::{TruthCarrier, TruthRoute};
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
        let parsed: crosstalk_eval::datasets::swarm_truth::schema::TruthLine =
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

// ---- unattributed reads ----

/// The fixture's self-read and reread rows (they name both agents'
/// sessions, which tie the gateway's agent ids to truth agents) and, about
/// a002's first read of p1, at most that it was unattributed.
fn unattributed_truth(with_row: bool) -> Vec<serde_json::Value> {
    let all = fixture::truth_rows();
    let mut rows = vec![fixture::header(), all[3].clone(), all[4].clone()];
    if with_row {
        rows.push(fixture::unattributed(
            ("a002", "session-a002", 1, "toolu_r1"),
            "p1",
            P1,
        ));
    }
    rows
}

fn judged(outcome: &SwarmOutcome) -> (u64, u64, u64) {
    let sum = |pick: fn(&crosstalk_eval::score::Counts) -> u64| {
        outcome
            .report
            .rows
            .iter()
            .map(|row| pick(&row.counts))
            .sum::<u64>()
    };
    (
        sum(|counts| counts.correct),
        sum(|counts| counts.false_positive),
        sum(|counts| counts.unjudged),
    )
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

#[test]
fn a_detection_on_an_unattributed_read_is_unjudged() {
    let dir = fixture::dir("unattributed-row");
    let written = fixture::write(&dir, &unattributed_truth(true));
    let outcome = run(&inputs(&written), 50, &Gates::default()).expect("the run scores");
    assert_eq!(outcome.resolved.unattributed, 1);
    assert!(outcome.diagnostics.is_empty());
    // The found detection (a001 → a002 at the read) is unjudged; the
    // self-read and reread detections violate their controls.
    assert_eq!(judged(&outcome), (0, 2, 1));
    assert!(
        outcome
            .report
            .false_positives
            .iter()
            .all(|fp| fp.prediction.reader_exchange != written.a002[1].id)
    );
}

#[test]
fn the_same_detection_without_the_row_is_a_false_positive() {
    let dir = fixture::dir("unattributed-none");
    let written = fixture::write(&dir, &unattributed_truth(false));
    let outcome = run(&inputs(&written), 50, &Gates::default()).expect("the run scores");
    assert_eq!(outcome.resolved.unattributed, 0);
    assert_eq!(judged(&outcome), (0, 3, 0));
    assert!(
        outcome
            .report
            .false_positives
            .iter()
            .any(|fp| fp.prediction.reader_exchange == written.a002[1].id && fp.violated.is_none())
    );
}

#[test]
fn an_unattributed_read_with_the_wrong_hash_is_dropped() {
    let dir = fixture::dir("unattributed-hash");
    let mut rows = unattributed_truth(false);
    rows.push(fixture::unattributed(
        ("a002", "session-a002", 1, "toolu_r1"),
        "p1",
        "another body",
    ));
    let written = fixture::write(&dir, &rows);
    let outcome = run(&inputs(&written), 50, &Gates::default()).expect("the run scores");
    let mismatches: Vec<_> = outcome.diagnostics.named("hash_mismatch").collect();
    assert_eq!(mismatches.len(), 1);
    assert_eq!(mismatches[0].row, Some(RowKind::UnattributedRead));
    assert_eq!(mismatches[0].effect, Effect::Dropped);
    assert_eq!(outcome.resolved.dropped, 1);
    assert_eq!(judged(&outcome), (0, 3, 0));
}

// ---- access-only detections ----

#[test]
fn a_suspected_transmission_predicts_from_its_evidence_accesses() {
    for discarded in [false, true] {
        let dir = fixture::dir(if discarded { "discarded" } else { "suspected" });
        let written = fixture::write(&dir, &fixture::truth_rows());
        let before = run(&inputs(&written), 50, &Gates::default()).expect("the run scores");
        fixture::append_access_only(&written, discarded);
        let outcome = run(&inputs(&written), 50, &Gates::default()).expect("the run scores");
        let class = if discarded {
            EvidenceClass::Discarded
        } else {
            EvidenceClass::Suspected
        };
        // The evidence's accesses make one prediction: a001 → a003 at
        // a003's read of p2, located at the whole tool result.
        let made: Vec<_> = outcome
            .predictions
            .iter()
            .filter(|p| p.class == class)
            .collect();
        assert_eq!(made.len(), 1, "{:?}", outcome.diagnostics);
        let prediction = made[0];
        assert_eq!(
            prediction.from,
            AgentKey::new(WorldKey::new(fixture::WORLD), "a001")
        );
        assert_eq!(
            prediction.to,
            AgentKey::new(WorldKey::new(fixture::WORLD), "a003")
        );
        assert_eq!(prediction.reader_exchange, written.a003[1].id);
        assert_eq!(prediction.read_at.range.start(), 0);
        assert_eq!(prediction.read_at.range.end() as usize, P2.len());
        assert_eq!(
            prediction.origin_at.map(|at| at.part.message),
            Some(written.a001[1].response)
        );
        let row = outcome
            .report
            .rows
            .iter()
            .find(|row| row.key.class == class)
            .expect("the access-only row");
        assert_eq!(row.counts.predicted, 1);
        assert_eq!(row.counts.correct, 1, "it lines up with the P2 label");
        // Access-only recall counts the P2 label; overall does not find it.
        assert_eq!(outcome.report.access_only.labels, 1);
        assert_eq!(
            outcome.report.access_only.expected,
            outcome.report.overall.counts.expected
        );
        assert!(outcome.report.access_only.recall.is_some_and(|r| r > 0.0));
        assert_eq!(
            outcome.report.overall.counts.found,
            before.report.overall.counts.found
        );
        assert_eq!(outcome.report.overall.recall, before.report.overall.recall);
        assert_eq!(before.report.access_only.labels, 0);
        assert_eq!(outcome.detected.exported, 3, "the export is unchanged");
        assert_eq!(outcome.detected.evidence, 4);
        let text = crosstalk_eval::report::table::render(&outcome.report);
        assert!(text.contains("access-only recall"), "{text}");
    }
}

// ---- the all-states export swarm-fetch asks for ----

#[test]
fn an_exported_discarded_row_is_scored_from_its_evidence() {
    let dir = fixture::dir("all-states");
    let written = fixture::write(&dir, &fixture::truth_rows());
    let discarded = fixture::export_discarded(&written);
    let bytes = std::fs::read(&written.export).expect("the export");
    let exported = read_export(&bytes).expect("a complete export");
    assert_eq!(exported.transmissions.len(), 4);
    assert!(exported.transmissions.contains(&discarded.id));
    let outcome = run(&inputs(&written), 50, &Gates::default()).expect("the run scores");
    assert!(
        !outcome
            .diagnostics
            .entries
            .iter()
            .any(|d| matches!(d.failure, JoinFailure::MissingEvidence { .. })),
        "{:?}",
        outcome.diagnostics
    );
    let made: Vec<_> = outcome
        .predictions
        .iter()
        .filter(|p| p.class == EvidenceClass::Discarded)
        .collect();
    assert_eq!(made.len(), 1, "one prediction, not one per source");
    assert_eq!(made[0].transmission, discarded.id);
    assert_eq!(made[0].to, key("a003"));
    assert_eq!(outcome.detected.exported, 4);
    assert_eq!(outcome.report.access_only.labels, 1);
}

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
    use crosstalk_eval::datasets::swarm_truth::fetch::{FetchConfig, fetch};
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
