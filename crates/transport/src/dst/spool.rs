//! A seeded simulation of the publish spool over [`LogBus`] (an inner bus
//! idempotent on ids, like `PgBus`, that goes down and comes back), under
//! tokio's paused clock.
//!
//! A run publishes envelopes with random times while the inner bus goes
//! down and up, and crashes the process at random: plainly, mid-append (the
//! last record torn at a random byte, its publish never `Ok`), after a
//! drained batch committed and before the cursor moved, and between finding
//! the spool empty and switching to `Direct`. A crash drops the spool
//! without any shutdown and reopens the directory, as a restarted process
//! does. At the end the inner bus stays up until the spool is empty.
//!
//! The run records every `Ok` publish, in order, and after every step
//! whether [`SpoolingBus::oldest_at`] was ahead of an `Ok` envelope not yet
//! in the inner bus. Its blocking file work runs on real threads, so a seed
//! fixes the operations but not every interleaving.

use std::collections::HashMap;
use std::time::Duration;

use crosstalk_spec::events::Envelope;
use crosstalk_spec::ids::EventId;
use crosstalk_spec::interfaces::l2_transport::EventBus;
use crosstalk_spec::support::Timestamp;
use std::fs;
use std::path::{Path, PathBuf};

use crate::rng::SplitMix64;
use crate::spool::tests::{LogBus, small_config};
use crate::spool::{CrashPoint, SpoolConfig, SpoolState, SpoolingBus};
use crate::testing::changed;

/// What one run did and saw.
#[derive(Debug)]
pub(super) struct SpoolOutcome {
    /// Every envelope whose publish returned `Ok`, in publish order.
    pub(super) ok: Vec<Envelope>,
    /// The inner bus's log at the end.
    pub(super) log: Vec<Envelope>,
    /// Envelopes the inner bus was offered again and did not add.
    pub(super) resent: u64,
    /// Steps at which `oldest_at` was later than an `Ok` envelope still
    /// in the spool.
    pub(super) oldest_violations: Vec<String>,
    pub(super) crashes: u32,
    pub(super) torn: u32,
}

impl SpoolOutcome {
    /// Every `Ok` envelope is in the log once, equal to what was
    /// published, and in publish order.
    pub(super) fn check_ok_in_log_in_order(&self) -> Result<(), String> {
        let position: HashMap<EventId, usize> = self
            .log
            .iter()
            .enumerate()
            .map(|(i, e)| (e.id, i))
            .collect();
        if position.len() != self.log.len() {
            return Err("an id is in the inner log twice".to_owned());
        }
        let mut last = None;
        for envelope in &self.ok {
            let Some(&at) = position.get(&envelope.id) else {
                return Err(format!(
                    "Ok {} never reached the inner bus",
                    envelope.id.ulid_text()
                ));
            };
            if &self.log[at] != envelope {
                return Err(format!(
                    "{} reached the inner bus changed",
                    envelope.id.ulid_text()
                ));
            }
            if last.is_some_and(|previous| at <= previous) {
                return Err(format!(
                    "{} reached the inner bus before an earlier Ok publish",
                    envelope.id.ulid_text()
                ));
            }
            last = Some(at);
        }
        Ok(())
    }
}

/// The seeds a check runs: `CROSSTALK_DST_SEED` alone when set, else
/// `0..count`.
fn seeds(count: u64) -> Vec<u64> {
    match std::env::var("CROSSTALK_DST_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(seed) => vec![seed],
        None => (0..count).collect(),
    }
}

pub(super) async fn for_spool_seeds(
    count: u64,
    crashes: bool,
    check: impl Fn(&SpoolOutcome) -> Result<(), String>,
) -> Vec<SpoolOutcome> {
    let mut outcomes = Vec::new();
    for seed in seeds(count) {
        let outcome = run(seed, crashes).await;
        if let Err(failure) = check(&outcome) {
            panic!("{outcome:?}: {failure}\nrerun with CROSSTALK_DST_SEED={seed}");
        }
        outcomes.push(outcome);
    }
    outcomes
}

fn config(dir: &Path) -> SpoolConfig {
    // Small segments, so runs roll and delete them.
    small_config(dir, 1 << 22, 1 << 12)
}

/// The newest segment file and its length.
fn newest_segment(dir: &Path) -> Option<(PathBuf, u64)> {
    let mut files: Vec<PathBuf> = fs::read_dir(dir)
        .ok()?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("segment-"))
        })
        .collect();
    files.sort();
    let path = files.pop()?;
    let len = fs::metadata(&path).ok()?.len();
    Some((path, len))
}

/// Cut the file at `path` to `len` bytes.
fn truncate(path: &Path, len: u64) {
    let file = fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("opens the segment");
    file.set_len(len).expect("truncates");
}

/// Run one seeded scenario. Without `crashes`, only outages happen.
pub(super) async fn run(seed: u64, crashes: bool) -> SpoolOutcome {
    let mut rng = SplitMix64::new(seed ^ 0x5350_4F4F_4C00_0000);
    let dir = tempfile::tempdir().expect("tempdir");
    let inner = LogBus::new();
    let mut spool = SpoolingBus::open(inner.clone(), config(dir.path()))
        .await
        .expect("opens");
    let mut ok: Vec<Envelope> = Vec::new();
    let mut next = 1u128;
    let mut violations = Vec::new();
    let (mut crash_count, mut torn) = (0, 0);
    let mut envelope = |rng: &mut SplitMix64| {
        let mut e = changed(next);
        e.at = Timestamp::from_micros(1_000 + rng.below(1_000_000) as u64);
        next += 1;
        e
    };
    for step in 0..80 {
        let roll = rng.below(100);
        match roll {
            0..50 => {
                let e = envelope(&mut rng);
                if spool.publish(e.clone()).await.is_ok() {
                    ok.push(e);
                }
            }
            50..62 => inner.set_up(!inner.is_up()),
            62..72 => {
                tokio::time::sleep(Duration::from_millis(rng.below(40) as u64)).await;
            }
            72..80 if crashes => {
                spool.crash().await;
                crash_count += 1;
                spool = SpoolingBus::open(inner.clone(), config(dir.path()))
                    .await
                    .expect("reopens");
            }
            80..86 if crashes => {
                // A publish whose append is torn: the process dies during
                // it, so the drainer is already gone and the publish never
                // returned Ok.
                spool.close().await;
                let e = envelope(&mut rng);
                let before = spool.stats().records;
                let published = spool.publish(e.clone()).await.is_ok();
                let spooled = published && spool.stats().records == before + 1;
                let segment = if spooled {
                    newest_segment(dir.path())
                } else {
                    None
                };
                spool.crash().await;
                crash_count += 1;
                if let Some((path, len)) = segment {
                    let record = 28 + serde_json::to_vec(&e).map_or(0, |b| b.len() as u64);
                    let start = len.saturating_sub(record);
                    let cut = start + rng.below(record as usize) as u64;
                    truncate(&path, cut);
                    torn += 1;
                }
                spool = SpoolingBus::open(inner.clone(), config(dir.path()))
                    .await
                    .expect("reopens");
            }
            86..93 if crashes => {
                spool.arm_crash(CrashPoint::AfterBatchCommit);
                tokio::time::sleep(Duration::from_millis(rng.below(30) as u64)).await;
                spool.crash().await;
                crash_count += 1;
                spool = SpoolingBus::open(inner.clone(), config(dir.path()))
                    .await
                    .expect("reopens");
            }
            93..100 if crashes => {
                spool.arm_crash(CrashPoint::BeforeDirectSwitch);
                tokio::time::sleep(Duration::from_millis(rng.below(30) as u64)).await;
                spool.crash().await;
                crash_count += 1;
                spool = SpoolingBus::open(inner.clone(), config(dir.path()))
                    .await
                    .expect("reopens");
            }
            _ => tokio::task::yield_now().await,
        }
        // `oldest_at` is no later than any Ok envelope not yet sent.
        let oldest = spool.oldest_at();
        for e in &ok {
            if !inner.holds(e.id) && oldest.is_none_or(|t| t > e.at) {
                violations.push(format!(
                    "step {step}: {} at {:?} unsent, oldest_at {oldest:?}",
                    e.id.ulid_text(),
                    e.at
                ));
            }
        }
    }
    inner.set_up(true);
    for _ in 0..4_000 {
        if spool.state() == SpoolState::Direct && spool.stats().records == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        spool.state(),
        SpoolState::Direct,
        "seed {seed}: the spool drains once the inner bus stays up"
    );
    spool.close().await;
    SpoolOutcome {
        ok,
        log: inner.log(),
        resent: inner.resent(),
        oldest_violations: violations,
        crashes: crash_count,
        torn,
    }
}
