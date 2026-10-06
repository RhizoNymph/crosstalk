//! The detector contract's edges: part text checked live on every
//! fixture message, a world that cannot be processed written `failed`
//! with a coded reason while the run goes on, and files that are not one
//! export refused as a run failure.

use std::collections::BTreeMap;
use std::io::BufWriter;
use std::path::Path;

use a2a_bench_format as bench;
use bench::exchange::{
    AgentDecl, Client, Driven, Exchange, Fidelity, Request, Response, WorldDecl,
};
use bench::files::{ExchangeRow, Exchanges, MessageRow, Messages, WorldOnly};
use bench::ids::{DatasetId, Digest, SourceRef, WorldKey, exchange_id};
use bench::jsonl::{BasicHeader, FileWriter};
use bench::manifest::{Converter, FileDigests, Manifest, Source, Split, WorldEntry};
use bench::message::{AssistantPart, Body, MediaKind, Message, SystemPart, UserPart};
use bench::predictions::WorldStatus;
use bench::time::Timestamp;
use crosstalk_eval::bench_detect::convert::{self, check_part_text};
use crosstalk_eval::bench_detect::input::{InputDir, WorldRead};
use crosstalk_eval::bench_detect::{FailureCode, WorldFailure};
use crosstalk_eval::golden::run::write_manifest;
use crosstalk_eval::golden::writer::{EXCHANGES_FILE, MESSAGES_FILE};
use crosstalk_spec::observed::message::{self as spec, MessageBody, Text};

use super::common::{bench_detect, bench_detect_output, dir, export, fixtures, path, predictions};

fn message(body: Body) -> Message {
    Message::new(body).unwrap_or_else(|e| panic!("{e}"))
}

/// A world of one agent and one exchange whose user message is `user`,
/// under `credential`.
fn world(
    dataset: &DatasetId,
    key: &str,
    user: Vec<UserPart>,
    credential: &str,
) -> (WorldKey, Vec<Message>, WorldDecl, Vec<Exchange>) {
    let key = WorldKey::new(key).unwrap_or_else(|e| panic!("{e}"));
    let system = message(Body::System(vec![SystemPart::Text {
        text: "You are an agent.".to_owned(),
    }]));
    let user = message(Body::User(user));
    let reply = message(Body::Assistant(vec![AssistantPart::Text {
        text: "Noted.".to_owned(),
    }]));
    let source = SourceRef::new(format!("{key}.json"), "/0");
    let at = Timestamp::from_micros(1_767_225_600_000_000);
    let exchange = Exchange {
        id: exchange_id(dataset, &source, at),
        at_us: at,
        client: Client {
            credential: credential.to_owned(),
            session: None,
            turn: None,
            vendor: Some("openai".to_owned()),
            model: Some("gpt-4o".to_owned()),
        },
        request: Request {
            messages: vec![system.id(), user.id()],
            tools: None,
        },
        response: Response {
            messages: vec![reply.id()],
            stop: Some("end_turn".to_owned()),
            error: None,
        },
        fidelity: Fidelity::Synthetic,
        source,
    };
    let decl = WorldDecl {
        key: key.clone(),
        agents: vec![AgentDecl {
            key: bench::ids::AgentKey::new("alice").unwrap_or_else(|e| panic!("{e}")),
            driven: Driven::Model,
            model: Some("gpt-4o".to_owned()),
        }],
    };
    (key, vec![system, user, reply], decl, vec![exchange])
}

type World = (WorldKey, Vec<Message>, WorldDecl, Vec<Exchange>);

/// Writes an input view holding `worlds`; the manifest lists them in
/// `manifest_order` (indexes into `worlds`).
fn write_input(dir: &Path, dataset: &DatasetId, worlds: &[World], manifest_order: &[usize]) {
    let create = |name: &str| {
        BufWriter::new(std::fs::File::create(dir.join(name)).unwrap_or_else(|e| panic!("{e}")))
    };
    let mut messages = FileWriter::<Messages, _>::new(
        create(MESSAGES_FILE),
        &BasicHeader::new::<Messages>(dataset.clone()),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let mut exchanges = FileWriter::<Exchanges, _>::new(
        create(EXCHANGES_FILE),
        &BasicHeader::new::<Exchanges>(dataset.clone()),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    for (key, rows, decl, exchange_rows) in worlds {
        messages
            .world(&WorldOnly { key: key.clone() })
            .unwrap_or_else(|e| panic!("{e}"));
        for row in rows {
            messages
                .row(&MessageRow::Message(row.clone()))
                .unwrap_or_else(|e| panic!("{e}"));
        }
        exchanges.world(decl).unwrap_or_else(|e| panic!("{e}"));
        for row in exchange_rows {
            exchanges
                .row(&ExchangeRow::Exchange(row.clone()))
                .unwrap_or_else(|e| panic!("{e}"));
        }
    }
    let (_, messages) = messages.finish().unwrap_or_else(|e| panic!("{e}"));
    let (_, exchanges) = exchanges.finish().unwrap_or_else(|e| panic!("{e}"));
    let manifest = Manifest {
        format: bench::version::FORMAT,
        dataset: dataset.clone(),
        dataset_version: 1,
        split: Split::Dev,
        source: Source {
            path: "synthetic".to_owned(),
            revision: "test".to_owned(),
            digest: Digest::from_bytes([3; 32]),
        },
        converter: Converter {
            version: "test".to_owned(),
            git: "test".to_owned(),
        },
        selection: BTreeMap::new(),
        pace: BTreeMap::new(),
        worlds: manifest_order
            .iter()
            .map(|at| WorldEntry {
                key: worlds[*at].0.clone(),
                exchanges: worlds[*at].3.len() as u64,
                labels: None,
                notes: BTreeMap::new(),
            })
            .collect(),
        files: FileDigests {
            messages: messages.digest,
            exchanges: exchanges.digest,
            labels: None,
        },
    };
    write_manifest(dir, &manifest).unwrap_or_else(|e| panic!("{e}"));
}

const KEY: &str = "k:0101010101010101010101010101010101010101010101010101010101010101";

fn text(text: &str) -> UserPart {
    UserPart::Text {
        text: text.to_owned(),
    }
}

#[test]
fn a_world_that_cannot_be_converted_fails_with_its_code_and_the_run_goes_on() {
    let base = dir("failed-world");
    let dataset = DatasetId::new("synthetic").unwrap_or_else(|e| panic!("{e}"));
    let worlds = vec![
        world(&dataset, "fine", vec![text("Hello there.")], KEY),
        world(
            &dataset,
            "other-media",
            vec![
                text("See this."),
                UserPart::Media {
                    kind: MediaKind::Other,
                },
            ],
            KEY,
        ),
        world(&dataset, "bare-credential", vec![text("Hi.")], "0101"),
        world(&dataset, "after", vec![text("Still here.")], KEY),
    ];
    let input = base.join("input");
    std::fs::create_dir_all(&input).unwrap_or_else(|e| panic!("{e}"));
    write_input(&input, &dataset, &worlds, &[0, 1, 2, 3]);
    let out = base.join("predictions.jsonl");
    bench_detect(&["--input", path(&input), "--output", path(&out)]);
    let (_, sections) = predictions(&out);
    let statuses: Vec<(String, WorldStatus)> = sections
        .into_iter()
        .map(|section| (section.world.key.to_string(), section.world.status))
        .collect();
    assert_eq!(statuses.len(), 4);
    assert_eq!(statuses[0], ("fine".to_owned(), WorldStatus::Scored));
    assert_eq!(statuses[3], ("after".to_owned(), WorldStatus::Scored));
    for (world, status) in &statuses[1..3] {
        let WorldStatus::Failed { reason } = status else {
            panic!("{world}: {status:?}");
        };
        assert!(reason.starts_with("conversion: "), "{world}: {reason}");
    }
}

#[test]
fn files_that_are_not_one_export_fail_the_run() {
    let base = dir("world-order");
    let dataset = DatasetId::new("synthetic").unwrap_or_else(|e| panic!("{e}"));
    let worlds = vec![
        world(&dataset, "first", vec![text("One.")], KEY),
        world(&dataset, "second", vec![text("Two.")], KEY),
    ];
    let input = base.join("input");
    std::fs::create_dir_all(&input).unwrap_or_else(|e| panic!("{e}"));
    write_input(&input, &dataset, &worlds, &[1, 0]);
    let out = base.join("predictions.jsonl");
    let output = bench_detect_output(&["--input", path(&input), "--output", path(&out)]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("the manifest says second"), "{stderr}");
    assert!(!out.exists(), "no predictions file on a run failure");
}

#[test]
fn every_fixture_message_converts_with_its_part_text() {
    let root = |relative: &str| fixtures().join(relative).display().to_string();
    let selections: [(&str, Vec<String>); 4] = [
        (
            "salt",
            vec![
                "--dataset".into(),
                "salt".into(),
                "--root".into(),
                root("salt"),
            ],
        ),
        (
            "wiki",
            vec![
                "--dataset".into(),
                "wiki".into(),
                "--root".into(),
                root("wiki/collusion-wiki"),
            ],
        ),
        (
            "agentdojo",
            vec![
                "--dataset".into(),
                "agentdojo".into(),
                "--root".into(),
                root("agentdojo"),
            ],
        ),
        (
            "tau2",
            vec![
                "--dataset".into(),
                "tau2".into(),
                "--root".into(),
                root("tau2"),
            ],
        ),
    ];
    for (name, source) in selections {
        let out = dir(&format!("part-text-{name}"));
        let source: Vec<&str> = source.iter().map(String::as_str).collect();
        export(&source, &out);
        let mut input = InputDir::open(&out).unwrap_or_else(|e| panic!("{name}: {e}"));
        let dataset = input.manifest().dataset.clone();
        let mut messages = 0;
        while let Some(read) = input.next_world().unwrap_or_else(|e| panic!("{name}: {e}")) {
            let WorldRead::Ready(inputs) = read else {
                panic!("{name}: a fixture world does not check");
            };
            for bench_message in inputs.messages() {
                convert::message(bench_message).unwrap_or_else(|e| panic!("{name}: {e}"));
                messages += 1;
            }
            convert::world(&dataset, &inputs).unwrap_or_else(|e| panic!("{name}: {e}"));
        }
        assert!(messages > 0, "{name}: no message");
    }
}

#[test]
fn a_part_whose_text_differs_is_a_part_text_mismatch() {
    let bench_message = message(Body::User(vec![text("bench text")]));
    let spec_message = spec::Message::new(MessageBody::User(vec![spec::UserPart::Text(Text(
        "spec text".to_owned(),
    ))]));
    let error = check_part_text(&bench_message, &spec_message).expect_err("the texts differ");
    assert_eq!(error.code, FailureCode::PartTextMismatch);
    assert!(error.reason().starts_with("part_text_mismatch: "));
    assert!(
        !error.reason().contains("bench text"),
        "a reason never quotes text"
    );
    let longer = spec::Message::new(MessageBody::User(vec![
        spec::UserPart::Text(Text("bench text".to_owned())),
        spec::UserPart::Text(Text("more".to_owned())),
    ]));
    assert_eq!(
        check_part_text(&bench_message, &longer).map_err(|e: WorldFailure| e.code),
        Err(FailureCode::PartTextMismatch)
    );
}

#[test]
fn failure_and_stop_texts_read_back_as_the_golden_export_writes_them() {
    use crosstalk_spec::observed::exchange::{ExchangeFailure, StopReason};
    for (text, failure) in [
        ("upstream 529", ExchangeFailure::Upstream { status: 529 }),
        ("upstream_unreachable", ExchangeFailure::UpstreamUnreachable),
        ("stream_truncated", ExchangeFailure::StreamTruncated),
        (
            "malformed_stream at 17",
            ExchangeFailure::MalformedStream { offset: 17 },
        ),
        ("upstream_error_event", ExchangeFailure::UpstreamErrorEvent),
        ("unparseable_response", ExchangeFailure::UnparseableResponse),
        ("client_disconnected", ExchangeFailure::ClientDisconnected),
        ("timeout", ExchangeFailure::Timeout),
    ] {
        assert_eq!(convert::failure(text), Ok(failure), "{text}");
    }
    assert!(convert::failure("on fire").is_err());
    assert_eq!(convert::stop(Some("tool_use")), StopReason::ToolUse);
    assert_eq!(convert::stop(None), StopReason::Other);
}
