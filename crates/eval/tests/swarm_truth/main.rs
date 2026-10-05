//! The demo swarm benchmark over a synthetic run (`fixture`): truth
//! parsing, the join of truth rows to captured exchanges, and the scoring
//! of a saved export.

mod fixture;

use std::io::Cursor;

use crosstalk_eval::datasets::swarm_truth::detected::read_export;
use crosstalk_eval::datasets::swarm_truth::schema::HexDigest;
use crosstalk_eval::datasets::swarm_truth::truth_file::{self, Row, TruthFileError};
use crosstalk_eval::datasets::swarm_truth::{
    Effect, Inputs, JoinFailure, RowKind, Side, SwarmOutcome, default_blobs, default_evidence, run,
};
use crosstalk_eval::keys::{AgentKey, WorldKey};
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
            Row::Cluster(_) => "agent_cluster",
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
    // P1 holds a quote and a newline, which the writer's PUT escapes.
    assert_eq!(label.needs, MatchNeed::Normalized);
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
                && row.key.class == MatchClass::Normalized
                && row.key.tier == Some(Tier::Construction)
        })
        .expect("the normalized channel row");
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
        .find(|row| row.key.class == MatchClass::Exact && row.key.tier == Some(Tier::Construction))
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
