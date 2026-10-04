//! Checks on the scenario itself: it replays from its seed, and it
//! exercises every fault.

use super::SEEDS;
use super::scenario::{self, End, run};

/// The scenario is replayable: one seed, one run.
#[tokio::test(start_paused = true)]
async fn same_seed_replays_the_same_run() {
    for seed in [3, 17] {
        let first = run(seed).await;
        let second = run(seed).await;
        assert_eq!(first.records, second.records, "seed {seed}");
        assert_eq!(first.letters, second.letters, "seed {seed}");
        assert!(!first.records.is_empty());
    }
}

/// The scenario is not vacuous: across the seeds it acks, refuses late
/// acks, nacks, stalls, crashes and dead-letters.
#[tokio::test(start_paused = true)]
async fn scenario_exercises_every_fault() {
    let mut ends = [0usize; 5];
    let mut letters = 0;
    for seed in scenario::seeds(SEEDS) {
        let outcome = run(seed).await;
        letters += outcome.letters.len();
        for record in &outcome.records {
            let kind = match record.end {
                End::Acked(_) => 0,
                End::AckRefused(_) => 1,
                End::Nacked(_) => 2,
                End::Ignored => 3,
                End::Crashed(_) => 4,
            };
            ends[kind] += 1;
        }
    }
    assert!(ends.iter().all(|count| *count > 0), "{ends:?}");
    assert!(letters > 0);
}
