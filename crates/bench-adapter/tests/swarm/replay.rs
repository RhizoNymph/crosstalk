//! `ct-bench-detect replay`'s replay over the synthetic run: the fixture's
//! exchange log and blobs through the live composition, its export and
//! evidence read back as the gateway would serve them.
//!
//! The fixture holds the minimal reread shape of the node0 bench's
//! violations: a002 reads a001's p1 (turn 1), then reads the same version
//! again (turn 2, a `reread` row). L5 opens a co-access on the reread and
//! discards it (INV-1122): no content match lands on the reread.

use std::io::Cursor;
use std::time::Duration;

use crosstalk_bench_adapter::swarm::bodies::{BlobBodies, Cached};
use crosstalk_bench_adapter::swarm::exchange_log;
use crosstalk_bench_adapter::swarm::replay::{
    ReplayError, ReplaySettings, Replayed, demo_flow, replay,
};
use crosstalk_bench_adapter::swarm::truth_file;
use crosstalk_spec::derived::flow::access::AccessOp;
use crosstalk_spec::derived::flow::transmission::TransmissionState;
use crosstalk_spec::support::Timestamp;
use crosstalk_testkit::time::{T0, after};

use super::fixture::{self, Written};

fn replayed(name: &str, since: Option<Timestamp>) -> (Written, Result<Replayed, ReplayError>) {
    let dir = fixture::dir(name);
    let written = fixture::write(&dir, &fixture::truth_rows());
    let text = std::fs::read_to_string(&written.truth).expect("the truth file");
    let truth = truth_file::read(Cursor::new(text)).expect("a valid truth file");
    let settings = ReplaySettings {
        flow: demo_flow(10_000, 60_000),
        seed: 0,
        since: since.unwrap_or_else(|| {
            Timestamp::from_micros(truth.header.started_at_unix_ms.saturating_mul(1000))
        }),
        until: None,
    };
    let log = exchange_log::read(&written.exchanges).expect("the exchange log");
    let mut bodies = Cached::new(BlobBodies::open(&written.blobs).expect("the blobs"));
    let outcome = replay(&log, &mut bodies, &settings);
    (written, outcome)
}

#[test]
fn a_replay_runs_the_log_through_the_live_composition() {
    let (written, outcome) = replayed("replay", None);
    let replayed = outcome.expect("the replay");
    assert_eq!((replayed.ingested, replayed.skipped), (11, 0));
    assert_eq!(
        replayed.exported.transmissions.len(),
        replayed.evidence.len(),
        "every exported row has its evidence"
    );
    let reread = written.a002[2].id;
    // No content match at the reread exchange.
    for item in &replayed.evidence {
        if let Some(confirmed) = item.transmission().state.confirmed() {
            assert!(
                confirmed
                    .content()
                    .iter()
                    .all(|content| content.reader_exchange() != reread)
            );
        }
    }
    // The reread: one discarded co-access read at a002's second read of p1.
    let discarded: Vec<_> = replayed
        .evidence
        .iter()
        .filter(|item| {
            matches!(
                item.transmission().state,
                TransmissionState::Discarded { .. }
            )
        })
        .collect();
    assert_eq!(discarded.len(), 1, "{discarded:?}");
    assert!(discarded[0].accesses().iter().any(|detail| {
        matches!(detail.access().op, AccessOp::Read { .. }) && detail.access().exchange == reread
    }));
    let confirmed = replayed
        .evidence
        .iter()
        .filter(|item| item.transmission().state.confirmed().is_some())
        .count();
    assert_eq!(confirmed, 3, "the three deliveries");
}

#[test]
fn a_replay_is_deterministic() {
    let (_, first) = replayed("replay-first", None);
    let (_, second) = replayed("replay-second", None);
    let (first, second) = (
        first.expect("the first replay"),
        second.expect("the second replay"),
    );
    assert_eq!(first.export_bytes, second.export_bytes);
    assert_eq!(first.evidence, second.evidence);
    assert_eq!(first.queried, second.queried);
}

#[test]
fn entries_before_since_are_skipped() {
    // The fixture's exchanges are 10 s apart from T0 + 10 s.
    let (_, outcome) = replayed("replay-since", Some(after(T0, Duration::from_secs(35))));
    let replayed = outcome.expect("the replay");
    assert_eq!((replayed.ingested, replayed.skipped), (8, 3));
}

#[test]
fn a_replay_with_nothing_to_replay_is_refused() {
    let (_, outcome) = replayed("replay-empty", Some(after(T0, Duration::from_secs(3600))));
    assert!(
        matches!(outcome, Err(ReplayError::Empty { .. })),
        "{outcome:?}"
    );
}
