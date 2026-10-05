//! Open-SWE as a background corpus, on synthetic Parquet shards shaped like
//! the dataset (`tests/fixtures/open_swe`, rebuilt by its `make.sql`), and
//! the OpenAI-chat conversion and Parquet reading it rests on.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crosstalk_eval::corpus::{Coverage, TraceSource, World};
use crosstalk_eval::datasets::chat::{
    ChatFunction, ChatMessage, ChatToolCall, bodies, synthetic_call_id,
};
use crosstalk_eval::datasets::open_swe::{
    AGENTS_PER_WORLD, COLUMNS, Mixing, OpenSweRow, OpenSweSource, Shard, files,
};
use crosstalk_eval::datasets::parquet_rows::{ParquetError, ParquetRows, row_groups};
use crosstalk_eval::datasets::salt::Selection;
use crosstalk_eval::keys::DatasetId;
use crosstalk_eval::pipeline::{ReferenceDetector, Unscored, run};
use crosstalk_eval::report::Report;
use crosstalk_eval::report::table::render;
use crosstalk_eval::score::sources::{SOURCE_CHARS, TOP_SOURCES};
use crosstalk_eval::truth::{Expectation, NegativeReason, Tier};
use crosstalk_spec::observed::message::{AssistantPart, MessageBody, Reasoning};
use crosstalk_spec::support::Timestamp;

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/open_swe")
}

const OPENHANDS: &str = "data/openhands/fixture_model/fixture-set/train-00000-of-00001.parquet";
const SWEAGENT: &str = "data/sweagent/fixture_model/fixture-set/train-00000-of-00001.parquet";
const MINI: &str = "data/minisweagent/fixture_model/fixture-set/train-00000-of-00001.parquet";

fn worlds(mixing: Mixing) -> Vec<World> {
    let mut source = OpenSweSource::open(&root(), &Selection::default(), mixing)
        .unwrap_or_else(|e| panic!("{e}"));
    source
        .worlds()
        .map(|world| world.unwrap_or_else(|e| panic!("{e}")))
        .collect()
}

fn mixing(agents_per_world: usize) -> Mixing {
    Mixing {
        agents_per_world,
        per_shard: None,
    }
}

fn rows(relative: &str) -> Vec<(usize, OpenSweRow)> {
    ParquetRows::<OpenSweRow>::open(&root().join(relative), COLUMNS)
        .unwrap_or_else(|e| panic!("{e}"))
        .map(|row| row.unwrap_or_else(|e| panic!("{e}")))
        .collect()
}

fn assistant(text: &str, calls: &[(&str, &str)]) -> ChatMessage {
    ChatMessage {
        role: "assistant".into(),
        content: Some(text.into()),
        reasoning_content: None,
        tool_calls: (!calls.is_empty()).then(|| {
            calls
                .iter()
                .map(|(id, name)| ChatToolCall {
                    id: (!id.is_empty()).then(|| (*id).to_owned()),
                    function: ChatFunction {
                        name: (*name).into(),
                        arguments: Some("{\"command\": \"ls\"}".into()),
                    },
                })
                .collect()
        }),
        tool_call_id: None,
    }
}

fn tool(text: &str, id: Option<&str>) -> ChatMessage {
    ChatMessage {
        role: "tool".into(),
        content: Some(text.into()),
        tool_call_id: id.map(str::to_owned),
        ..ChatMessage::default()
    }
}

fn result_ids(bodies: &[MessageBody]) -> Vec<String> {
    bodies
        .iter()
        .filter_map(|body| match body {
            MessageBody::Tool(results) => results.iter().next().map(|r| r.call_id.0.clone()),
            _ => None,
        })
        .collect()
}

#[test]
fn shards_are_found_filtered_and_limited() {
    let all = files::discover(&root(), &Selection::default()).unwrap_or_else(|e| panic!("{e}"));
    let relative: Vec<&str> = all.iter().map(|s| s.relative.as_str()).collect();
    assert_eq!(relative, vec![MINI, OPENHANDS, SWEAGENT]);
    assert_eq!(
        all[1],
        Shard {
            relative: OPENHANDS.into(),
            harness: "openhands".into(),
            model: "fixture_model".into(),
            dataset: "fixture-set".into(),
        }
    );
    let harness = files::discover(
        &root(),
        &Selection {
            limit: None,
            include: vec!["sweagent".into()],
        },
    )
    .unwrap_or_default();
    // `sweagent` is a substring of `minisweagent` too.
    assert_eq!(harness.len(), 2);
    let limited = files::discover(
        &root(),
        &Selection {
            limit: Some(1),
            include: vec![],
        },
    )
    .unwrap_or_default();
    assert_eq!(limited.len(), 1);
    assert!(files::discover(Path::new("/nonexistent"), &Selection::default()).is_err());
    assert_eq!(Shard::parse("data/a/b/rows.jsonl"), None);
}

#[test]
fn parquet_rows_decode_the_projected_columns_with_row_numbers() {
    let read = rows(OPENHANDS);
    assert_eq!(read.len(), 2);
    assert_eq!(read[0].0, 0);
    assert_eq!(read[1].0, 1);
    assert_eq!(read[0].1.repo, "acme/widgets");
    assert_eq!(read[0].1.messages.len(), 11);
    let call = &read[0].1.messages[2].calls()[0];
    assert_eq!(call.function.name, "execute_bash");
    assert_eq!(call.id.as_deref(), Some("chatcmpl-tool-0a0000000000001"));
    assert_eq!(read[0].1.messages[0].tool_calls, None);
    assert_eq!(
        row_groups(&root().join(OPENHANDS)).unwrap_or_default(),
        vec![2]
    );
    let missing = ParquetRows::<OpenSweRow>::open(&root().join(OPENHANDS), &["no_such_column"]);
    assert!(matches!(missing, Err(ParquetError::MissingColumn { .. })));
    let absent = ParquetRows::<OpenSweRow>::open(&root().join("data/none.parquet"), COLUMNS);
    assert!(matches!(absent, Err(ParquetError::Open { .. })));
}

#[test]
fn tool_messages_without_ids_answer_calls_in_order() {
    let messages = vec![
        assistant("two at once", &[("call-a", "bash"), ("call-b", "bash")]),
        tool("first", None),
        tool("second", None),
        assistant("", &[("", "bash")]),
        tool("third", None),
        tool("nobody asked", None),
    ];
    let converted = bodies(&messages).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        result_ids(&converted),
        vec![
            "call-a".to_owned(),
            "call-b".to_owned(),
            synthetic_call_id(3, 0),
            "unpaired-5".to_owned(),
        ]
    );
}

#[test]
fn explicit_tool_call_ids_win_over_position() {
    let messages = vec![
        assistant("", &[("x", "bash"), ("y", "bash")]),
        tool("answers y", Some("y")),
        tool("answers the oldest open call", None),
    ];
    let converted = bodies(&messages).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(result_ids(&converted), vec!["y".to_owned(), "x".to_owned()]);
}

#[test]
fn assistant_parts_are_reasoning_text_then_calls() {
    let mut message = assistant("visible", &[("c1", "bash")]);
    message.reasoning_content = Some("thinking".into());
    let converted = bodies(&[message]).unwrap_or_else(|e| panic!("{e}"));
    let MessageBody::Assistant(parts) = &converted[0] else {
        panic!("not an assistant message");
    };
    assert!(matches!(
        &parts[0],
        AssistantPart::Reasoning(Reasoning::Visible { text, .. }) if text.0 == "thinking"
    ));
    assert!(matches!(&parts[1], AssistantPart::Text(text) if text.0 == "visible"));
    assert!(matches!(&parts[2], AssistantPart::ToolCall(call) if call.id.0 == "c1"));
    let empty = bodies(&[assistant("", &[])]).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(empty[0], MessageBody::Assistant(vec![]));
    let unknown = ChatMessage {
        role: "developer".into(),
        ..ChatMessage::default()
    };
    assert!(bodies(&[unknown]).is_err());
}

#[test]
fn trajectories_mix_round_robin_into_worlds() {
    let mixed = worlds(mixing(4));
    assert_eq!(mixed.len(), 2);
    let names: Vec<Vec<String>> = mixed
        .iter()
        .map(|w| w.agents().iter().map(|a| a.key.name.clone()).collect())
        .collect();
    assert_eq!(
        names[0],
        vec![
            "minisweagent/fixture_model/fixture-set/0",
            "minisweagent/fixture_model/fixture-set/1",
            "openhands/fixture_model/fixture-set/0",
            "sweagent/fixture_model/fixture-set/0",
        ]
    );
    assert_eq!(
        names[1],
        vec![
            "openhands/fixture_model/fixture-set/1",
            "sweagent/fixture_model/fixture-set/1",
        ]
    );
    assert_eq!(mixed[0].key().as_str(), "mix-00000");
    assert_eq!(AGENTS_PER_WORLD, 16);
    let capped = worlds(Mixing {
        agents_per_world: 16,
        per_shard: Some(1),
    });
    assert_eq!(capped.len(), 1);
    assert_eq!(capped[0].agents().len(), 3);
}

#[test]
fn every_assistant_message_is_one_call_on_a_shared_clock() {
    let world = &worlds(mixing(16))[0];
    assert_eq!(world.agents().len(), 6);
    let by_agent = |name: &str| -> Vec<_> {
        world
            .exchanges()
            .iter()
            .filter(|e| e.agent().name == name)
            .collect()
    };
    let openhands = by_agent("openhands/fixture_model/fixture-set/0");
    // Eleven messages, five of them the assistant's.
    assert_eq!(openhands.len(), 5);
    for (call, exchange) in openhands.iter().enumerate() {
        let request = exchange.request().count();
        assert!(exchange.response().is_some());
        assert_eq!(exchange.source().file, OPENHANDS);
        assert!(exchange.source().path.starts_with("/rows/0/messages/"));
        // Calls of a trajectory are 1000 s apart, slots 1 ms apart.
        let slot = 1; // shards are taken in turn: minisweagent, openhands, …
        let expected = crosstalk_eval::corpus::clock::compose(call as u64, slot, 0)
            .unwrap_or(Timestamp::from_micros(0));
        assert_eq!(exchange.at(), expected, "call {call}");
        assert!(request >= 2);
    }
    // Parallel calls are answered in order.
    let mini = by_agent("minisweagent/fixture_model/fixture-set/0");
    let second = mini[1];
    let ids: Vec<String> = second
        .request()
        .filter_map(|m| match &m.body {
            MessageBody::Tool(results) => results.iter().next().map(|r| r.call_id.0.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        ids,
        vec![
            "chatcmpl-tool-0e0000000000001".to_owned(),
            "chatcmpl-tool-0e0000000000002".to_owned()
        ]
    );
}

#[test]
fn background_worlds_label_every_pair_negative() {
    let world = &worlds(mixing(16))[0];
    assert_eq!(
        world.coverage(),
        Coverage::Complete {
            tier: Tier::Construction
        }
    );
    let mut reasons: BTreeMap<NegativeReason, usize> = BTreeMap::new();
    for expectation in world.truth() {
        match expectation {
            Expectation::NoTransmission(control) => {
                let label = control.label();
                assert!(label.reader_exchange.is_some());
                assert_eq!(label.tier, Tier::Structural);
                *reasons.entry(label.reason).or_default() += 1;
            }
            other => panic!("a background world has only negatives, got {other:?}"),
        }
    }
    let exchanges = world.exchanges().len();
    // One control per reader exchange per other trajectory.
    assert_eq!(reasons.values().sum::<usize>(), exchanges * 5);
    // acme/gadgets is worked on by an OpenHands and a SWE-agent trajectory.
    let shared = world
        .exchanges()
        .iter()
        .filter(|e| {
            e.agent().name == "openhands/fixture_model/fixture-set/1"
                || e.agent().name == "sweagent/fixture_model/fixture-set/0"
        })
        .count();
    assert_eq!(reasons.get(&NegativeReason::SharedSource), Some(&shared));
}

#[test]
fn the_reference_matcher_only_makes_false_positives_on_background() {
    let mut source = OpenSweSource::open(&root(), &Selection::default(), mixing(16))
        .unwrap_or_else(|e| panic!("{e}"));
    let summary = run(
        &mut source,
        &mut ReferenceDetector::default(),
        50,
        |_, _| {},
    );
    assert!(summary.failures.is_empty(), "{:?}", summary.failures);
    let score = summary.score;
    assert_eq!(score.totals.expectations, 0);
    let total = score.total(&Default::default());
    assert_eq!(total.correct, 0);
    assert_eq!(total.false_positive, total.predicted);
    // The planted phrase: minisweagent row 0 says it, OpenHands row 1 reads it
    // in a tool result. Different repositories: boilerplate.
    assert!(
        score
            .false_positives
            .iter()
            .any(
                |fp| fp.prediction.from.name == "minisweagent/fixture_model/fixture-set/0"
                    && fp.prediction.to.name == "openhands/fixture_model/fixture-set/1"
                    && fp.violated == Some(NegativeReason::Boilerplate)
            ),
        "{:#?}",
        score.false_positives
    );
    assert!(score.violation_count(None, Some(NegativeReason::Boilerplate)) >= 1);
}

#[test]
fn background_runs_report_the_false_positive_rate_and_its_sources() {
    let mut source = OpenSweSource::open(&root(), &Selection::default(), mixing(16))
        .unwrap_or_else(|e| panic!("{e}"));
    // No examples kept: the source tally is complete regardless.
    let summary = run(&mut source, &mut ReferenceDetector::default(), 0, |_, _| {});
    let score = summary.score;
    assert!(score.false_positives.is_empty());
    let violations = score.violation_count(None, None);
    assert!(violations >= 1);
    assert!(score.sources.len() <= TOP_SOURCES);
    let tallied: u64 = score.sources.iter().map(|s| s.count).sum();
    if score.sources.len() < TOP_SOURCES {
        assert_eq!(tallied, violations);
    }
    assert!(
        score
            .sources
            .windows(2)
            .all(|pair| pair[0].count >= pair[1].count)
    );
    assert!(score.sources.iter().all(|s| {
        s.count > 0
            && s.text.chars().count() <= SOURCE_CHARS
            && !s.text.contains('\n')
            && s.text == s.text.trim()
    }));
    let exchanges = score.totals.exchanges;
    let false_positives = score.total(&Default::default()).false_positive;
    let report = Report::new(
        DatasetId::new("open_swe"),
        "reference",
        score,
        Vec::new(),
        Vec::new(),
        Unscored::default(),
    );
    let Some(background) = &report.background else {
        panic!("a world with negative controls has a background summary");
    };
    assert_eq!(background.false_positives, false_positives);
    assert_eq!(background.exchanges, exchanges);
    let rate = false_positives as f64 * 1000.0 / exchanges as f64;
    assert!((background.per_1k_exchanges - rate).abs() < 1e-9);
    let text = render(&report);
    assert!(text.contains("per 1k exchanges"), "{text}");
    assert!(text.contains("top boilerplate sources"), "{text}");
}

#[test]
fn a_rerun_is_identical() {
    let first: Vec<_> = worlds(mixing(3))
        .iter()
        .map(|w| (w.key().clone(), w.exchanges().len(), w.truth().to_vec()))
        .collect();
    let second: Vec<_> = worlds(mixing(3))
        .iter()
        .map(|w| (w.key().clone(), w.exchanges().len(), w.truth().to_vec()))
        .collect();
    assert_eq!(first, second);
}
