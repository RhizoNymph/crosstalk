//! LMCache agentic traces as a background corpus, on synthetic Parquet files
//! shaped like the dataset (`tests/fixtures/lmcache`, rebuilt by its
//! `make.sql`). The first file has two row groups; a session crosses the
//! boundary between them.

use std::path::{Path, PathBuf};

use crosstalk_eval::corpus::clock::EPOCH_MICROS;
use crosstalk_eval::corpus::{Coverage, Fidelity, TraceSource, World};
use crosstalk_eval::datasets::lmcache::{
    LmcacheSource, Segment, Session, Sessions, calls, discover, group, segments,
};
use crosstalk_eval::datasets::open_swe::Mixing;
use crosstalk_eval::datasets::salt::Selection;
use crosstalk_eval::pipeline::{ReferenceDetector, run};
use crosstalk_eval::truth::{Expectation, NegativeReason, Tier};
use crosstalk_spec::observed::exchange::{ExchangeOutcome, StopReason};
use crosstalk_spec::observed::message::{AssistantPart, MessageBody};

const FIRST: &str = "data/train-00000-of-00002.parquet";
const SECOND: &str = "data/train-00001-of-00002.parquet";
const A: &str = "swebench__acme__acme-1__claude";
const B: &str = "swebench__acme__acme-2__minimax";
const C: &str = "gaia__task-9__claude";
const D: &str = "swebench__beta__beta-7__deepseek";
const E: &str = "wildclaw__job-3__claude";

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/lmcache")
}

fn sessions(per_file: Option<usize>) -> Vec<Session> {
    let files = discover(&root(), &Selection::default()).unwrap_or_else(|e| panic!("{e}"));
    let segments = segments(&root(), &files).unwrap_or_else(|e| panic!("{e}"));
    Sessions::new(&root(), segments, per_file)
        .map(|session| session.unwrap_or_else(|e| panic!("{e}")))
        .collect()
}

fn session(id: &str) -> Session {
    sessions(None)
        .into_iter()
        .find(|session| session.id == id)
        .unwrap_or_else(|| panic!("no session {id}"))
}

fn worlds(agents_per_world: usize) -> Vec<World> {
    let mut source = LmcacheSource::open(
        &root(),
        &Selection::default(),
        Mixing {
            agents_per_world,
            per_shard: None,
        },
    )
    .unwrap_or_else(|e| panic!("{e}"));
    source
        .worlds()
        .map(|world| world.unwrap_or_else(|e| panic!("{e}")))
        .collect()
}

#[test]
fn files_and_row_groups_are_interleaved() {
    let files = discover(&root(), &Selection::default()).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(files, vec![FIRST.to_owned(), SECOND.to_owned()]);
    let found = segments(&root(), &files).unwrap_or_else(|e| panic!("{e}"));
    let segment = |file: &str, group| Segment {
        file: file.into(),
        group,
    };
    assert_eq!(
        found,
        vec![segment(FIRST, 0), segment(SECOND, 0), segment(FIRST, 1)]
    );
    let limited = discover(
        &root(),
        &Selection {
            limit: Some(1),
            include: vec![],
        },
    )
    .unwrap_or_default();
    assert_eq!(limited, vec![FIRST.to_owned()]);
    assert!(discover(Path::new("/nonexistent"), &Selection::default()).is_err());
}

#[test]
fn sessions_come_from_each_segment_in_turn() {
    let all = sessions(None);
    let ids: Vec<&str> = all.iter().map(|s| s.id.as_str()).collect();
    // The second row group opens with the tail of B, which is skipped there.
    assert_eq!(ids, vec![A, D, C, B, E]);
    let rows = |s: &Session| s.rows.iter().map(|(row, _)| *row).collect::<Vec<_>>();
    assert_eq!(rows(&all[0]), vec![0, 1, 2]);
    assert_eq!(rows(&all[2]), vec![7, 8]);
    assert_eq!(rows(&all[3]), vec![3, 4, 5]);
    assert_eq!(all[2].file, FIRST);
    let capped: Vec<String> = sessions(Some(1)).into_iter().map(|s| s.id).collect();
    assert_eq!(capped, vec![A.to_owned(), D.to_owned()]);
}

#[test]
fn a_call_receives_what_the_next_request_appends() {
    let found = calls(&session(A)).unwrap_or_else(|e| panic!("{e}"));
    // Three requests: the last has no recorded response.
    assert_eq!(found.len(), 2);
    let MessageBody::Assistant(parts) = &found[0].response.message().body else {
        panic!("the response is the appended assistant message");
    };
    assert!(matches!(&parts[0], AssistantPart::Text(t) if t.0 == "Looking at the frobnicator."));
    assert!(matches!(&parts[1], AssistantPart::ToolCall(c) if c.id.0 == "toolu_fixture_a1"));
    assert_eq!(found[0].stop, StopReason::ToolUse);
    assert_eq!(found[1].stop, StopReason::EndTurn);
    assert_eq!(found[0].request.len(), 2);
    assert_eq!(found[1].request.len(), 4);
    // The response is echoed unchanged in the next request.
    assert_eq!(found[1].request[2], found[0].response);
    assert!(found.iter().all(|c| c.fidelity == Fidelity::Reconstructed));
    assert_eq!(found[0].source.path, "/rows/0");
    assert_eq!(found[1].source.path, "/rows/1");
}

#[test]
fn the_clock_sums_pre_gaps() {
    let found = calls(&session(A)).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(found[0].at.as_micros(), EPOCH_MICROS);
    assert_eq!(found[1].at.as_micros(), EPOCH_MICROS + 500_000);
    let d = calls(&session(D)).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(d[1].at.as_micros(), EPOCH_MICROS + 2_000_000);
}

#[test]
fn a_rewritten_history_makes_the_call_synthetic() {
    let found = calls(&session(B)).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(found.len(), 2);
    assert_eq!(found[0].fidelity, Fidelity::Reconstructed);
    assert_eq!(found[1].fidelity, Fidelity::Synthetic);
}

#[test]
fn tool_messages_keep_their_recorded_call_ids() {
    let found = calls(&session(D)).unwrap_or_else(|e| panic!("{e}"));
    let ids: Vec<String> = found[1]
        .request
        .iter()
        .filter_map(|m| match &m.message().body {
            MessageBody::Tool(results) => results.iter().next().map(|r| r.call_id.0.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(ids, vec!["call_fixture_d1".to_owned()]);
    assert!(calls(&session(E)).unwrap_or_default().is_empty());
}

#[test]
fn sessions_are_grouped_by_repository_or_task() {
    assert_eq!(group(A), "swebench/acme");
    assert_eq!(group(B), "swebench/acme");
    assert_eq!(group(C), "gaia/task-9");
    assert_eq!(group("loose"), "loose");
}

#[test]
fn sessions_mix_into_background_worlds() {
    let mixed = worlds(16);
    assert_eq!(mixed.len(), 1);
    let world = &mixed[0];
    assert_eq!(world.agents().len(), 5);
    assert_eq!(
        world.coverage(),
        Coverage::Complete {
            tier: Tier::Construction
        }
    );
    // A 2, B 2, C 1, D 2, E 0 calls.
    assert_eq!(world.exchanges().len(), 7);
    for exchange in world.exchanges() {
        assert!(matches!(
            exchange.exchange().outcome,
            ExchangeOutcome::Completed { .. }
        ));
    }
    let shared = world
        .truth()
        .iter()
        .filter(|e| {
            matches!(e, Expectation::NoTransmission(c)
                if c.label().reason == NegativeReason::SharedSource)
        })
        .count();
    // A and B both work on swebench/acme: each reads the other twice.
    assert_eq!(shared, 4);
    assert_eq!(world.truth().len(), 7 * 4);
    assert_eq!(worlds(2).len(), 3);
}

#[test]
fn the_reference_matcher_runs_over_lmcache() {
    let mut source = LmcacheSource::open(&root(), &Selection::default(), Mixing::default())
        .unwrap_or_else(|e| panic!("{e}"));
    let summary = run(
        &mut source,
        &mut ReferenceDetector::default(),
        10,
        |_, _| {},
    );
    assert!(summary.failures.is_empty(), "{:?}", summary.failures);
    let total = summary.score.total(&Default::default());
    assert_eq!(total.correct, 0);
    assert_eq!(total.false_positive, total.predicted);
}
