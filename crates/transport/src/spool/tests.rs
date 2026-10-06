//! Unit tests of the spool: the record format on disk, torn tails,
//! corruption, the cursor, segments, the bound and the lock; and the
//! decorator's states over [`LogBus`], a test inner bus that the
//! simulation tests share.
//!
//! The functions named by `transport.spool.*` invariants' `unit` evidence
//! live here, at `crosstalk_transport::spool::tests::<name>`.

use std::collections::HashSet;
use std::fs;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use crosstalk_spec::events::{Envelope, Subject};
use crosstalk_spec::ids::EventId;
use crosstalk_spec::interfaces::l2_transport::{BusError, ConsumerGroup, EventBus, RetryPolicy};
use crosstalk_spec::support::Timestamp;

use super::cursor::{self, CursorFile, Step};
use super::log::{AppendError, SpoolLog, segment_name};
use super::record::{RECORD_HEADER_LEN, SEGMENT_HEADER};
use super::{DrainTarget, SpoolConfig, SpoolError, SpoolState, SpoolingBus, discard_corrupt};
use crate::testing::{changed, config as bus_config, non_zero};
use crate::{MpscBus, MpscSubscription};

/// A test inner bus: an append-only log idempotent on envelope ids, as
/// `PgBus`'s is, that can be taken down and brought back. Every publish
/// attempt is counted, so a test can see a resend that added nothing.
#[derive(Clone)]
pub(crate) struct LogBus {
    state: Arc<Mutex<LogState>>,
    mpsc: MpscBus,
}

#[derive(Default)]
struct LogState {
    up: bool,
    log: Vec<Envelope>,
    ids: HashSet<EventId>,
    /// Envelopes offered that the log already held.
    resent: u64,
}

impl LogBus {
    /// An up bus. Needs a runtime (its subscriptions are an `MpscBus`'s).
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(LogState {
                up: true,
                ..LogState::default()
            })),
            mpsc: MpscBus::start(bus_config()).expect("bus starts"),
        }
    }

    fn with<T>(&self, f: impl FnOnce(&mut LogState) -> T) -> T {
        f(&mut self.state.lock().unwrap_or_else(PoisonError::into_inner))
    }

    pub(crate) fn set_up(&self, up: bool) {
        self.with(|s| s.up = up);
    }

    pub(crate) fn is_up(&self) -> bool {
        self.with(|s| s.up)
    }

    pub(crate) fn log(&self) -> Vec<Envelope> {
        self.with(|s| s.log.clone())
    }

    pub(crate) fn ids(&self) -> Vec<EventId> {
        self.with(|s| s.log.iter().map(|e| e.id).collect())
    }

    pub(crate) fn holds(&self, id: EventId) -> bool {
        self.with(|s| s.ids.contains(&id))
    }

    pub(crate) fn resent(&self) -> u64 {
        self.with(|s| s.resent)
    }

    fn append(&self, envelopes: Vec<Envelope>) -> Result<(), BusError> {
        self.with(|s| {
            if !s.up {
                return Err(BusError::Disconnected);
            }
            for envelope in envelopes {
                if s.ids.insert(envelope.id) {
                    s.log.push(envelope);
                } else {
                    s.resent += 1;
                }
            }
            Ok(())
        })
    }
}

impl EventBus for LogBus {
    type Subscription = MpscSubscription;

    async fn publish(&self, envelope: Envelope) -> Result<(), BusError> {
        self.append(vec![envelope])
    }

    async fn subscribe(
        &self,
        subjects: &[Subject],
        group: ConsumerGroup,
        retry: RetryPolicy,
    ) -> Result<MpscSubscription, BusError> {
        self.mpsc.subscribe(subjects, group, retry).await
    }
}

impl DrainTarget for LogBus {
    async fn probe(&self) -> Result<(), BusError> {
        if self.is_up() {
            Ok(())
        } else {
            Err(BusError::Disconnected)
        }
    }

    async fn publish_batch(&self, envelopes: Vec<Envelope>) -> Result<(), BusError> {
        self.append(envelopes)
    }
}

/// A spool config in `dir` with small bounds: segments of `segment` bytes,
/// at most `max` bytes, batches of 4, a 10 ms probe.
pub(crate) fn small_config(dir: &Path, max: u64, segment: u64) -> SpoolConfig {
    SpoolConfig::with_limits(
        dir,
        NonZeroU64::new(max).expect("non-zero"),
        NonZeroU64::new(segment).expect("non-zero"),
        NonZeroUsize::new(4).expect("non-zero"),
        non_zero(Duration::from_millis(10)),
    )
    .expect("valid spool config")
}

fn payload(n: u128) -> Vec<u8> {
    serde_json::to_vec(&changed(n)).expect("encodes")
}

fn record_size(n: u128) -> u64 {
    (RECORD_HEADER_LEN + payload(n).len()) as u64
}

fn append(log: &mut SpoolLog, n: u128) {
    log.append(&payload(n), changed(n).at).expect("appends");
}

fn segment_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = fs::read_dir(dir)
        .expect("lists")
        .map(|e| e.expect("entry").path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("segment-"))
        })
        .collect();
    files.sort();
    files
}

fn numbers_of(log: &SpoolLog) -> Vec<u64> {
    log.batch(usize::MAX).iter().map(|m| m.number).collect()
}

/// `transport.spool.torn-tail-only`: every proper prefix of the last
/// record of the last segment (an append cut short at any byte), and that
/// record with a bad checksum, is truncated at open; the records before it
/// survive, nothing is corrupt, and appending carries on.
#[test]
fn every_prefix_of_a_torn_tail_is_truncated() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = small_config(dir.path(), 1 << 20, 1 << 16);
    {
        let mut log = SpoolLog::open(&config).expect("opens");
        for n in 1..=3 {
            append(&mut log, n);
        }
    }
    let [segment] = segment_files(dir.path()).try_into().expect("one segment");
    let full = fs::read(&segment).expect("reads");
    let tail_start = full.len() as u64 - record_size(3);
    for cut in tail_start..full.len() as u64 {
        fs::write(&segment, &full[..cut as usize]).expect("writes");
        let mut log = SpoolLog::open(&config).expect("opens");
        assert_eq!(numbers_of(&log), vec![1, 2], "cut at {cut}");
        assert!(log.barrier().is_none(), "cut at {cut}");
        assert_eq!(log.counters().truncated_bytes, cut - tail_start);
        assert_eq!(fs::metadata(&segment).expect("stat").len(), tail_start);
        append(&mut log, 4);
        assert_eq!(
            numbers_of(&log),
            vec![1, 2, 3],
            "the torn number was never Ok"
        );
        drop(log);
        // The append opened a new segment; start the next cut without it.
        for other in segment_files(dir.path()) {
            if other != segment {
                fs::remove_file(other).expect("removes");
            }
        }
    }
    // The whole record, mis-checksummed.
    let mut flipped = full.clone();
    let last = flipped.len() - 1;
    flipped[last] ^= 0x01;
    fs::write(&segment, &flipped).expect("writes");
    let log = SpoolLog::open(&config).expect("opens");
    assert_eq!(numbers_of(&log), vec![1, 2]);
    assert!(log.barrier().is_none());
    drop(log);
    // A segment whose header was cut short is removed.
    fs::write(&segment, &full).expect("writes");
    let next = dir.path().join(segment_name(4));
    for cut in 0..SEGMENT_HEADER.len() {
        fs::write(&next, &SEGMENT_HEADER[..cut]).expect("writes");
        let log = SpoolLog::open(&config).expect("opens");
        assert_eq!(numbers_of(&log), vec![1, 2, 3]);
        assert!(log.barrier().is_none());
        assert!(!next.exists(), "header cut at {cut}");
    }
}

/// `transport.spool.torn-tail-only`: a bad record anywhere but the end of
/// the last segment stops draining before it, as `Corrupt`; nothing past
/// it is drained or skipped, and its bytes stay until an operator discards
/// them.
#[test]
fn a_bad_record_mid_segment_is_corrupt() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = small_config(dir.path(), 1 << 20, 1 << 16);
    {
        let mut log = SpoolLog::open(&config).expect("opens");
        for n in 1..=3 {
            append(&mut log, n);
        }
    }
    let [segment] = segment_files(dir.path()).try_into().expect("one segment");
    let full = fs::read(&segment).expect("reads");
    let second = SEGMENT_HEADER.len() as u64 + record_size(1);
    // Flip a payload byte of record 2, which a record follows.
    let mut bad = full.clone();
    bad[(second + RECORD_HEADER_LEN as u64 + 2) as usize] ^= 0x20;
    fs::write(&segment, &bad).expect("writes");
    let mut log = SpoolLog::open(&config).expect("opens");
    let barrier = log.barrier().expect("corrupt");
    assert_eq!(
        (barrier.segment, barrier.offset, barrier.before),
        (1, second, 1)
    );
    assert_eq!(
        numbers_of(&log),
        vec![1],
        "only what precedes the corruption drains"
    );
    assert_eq!(fs::read(&segment).expect("reads"), bad, "nothing truncated");
    // Drained up to the barrier, the corrupt segment stays.
    log.advance(1).expect("advances");
    assert!(log.batch(10).is_empty());
    assert!(segment.exists());
    drop(log);

    // A bad last record of a segment that is not the last one is corrupt
    // too: only the last segment's tail can be torn.
    let dir = tempfile::tempdir().expect("tempdir");
    let two_per_segment = SEGMENT_HEADER.len() as u64 + record_size(1) + record_size(2);
    let config = small_config(dir.path(), 1 << 20, two_per_segment);
    {
        let mut log = SpoolLog::open(&config).expect("opens");
        for n in 1..=4 {
            append(&mut log, n);
        }
    }
    let files = segment_files(dir.path());
    assert_eq!(files.len(), 2);
    let first = fs::read(&files[0]).expect("reads");
    fs::write(&files[0], &first[..first.len() - 1]).expect("writes");
    let log = SpoolLog::open(&config).expect("opens");
    let barrier = log.barrier().expect("corrupt");
    assert_eq!(barrier.segment, 1);
    assert_eq!(numbers_of(&log), vec![1]);
    drop(log);
}

/// A discarded corruption lets the next open drain the records after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_discarded_corruption_lets_the_rest_drain() {
    let dir = tempfile::tempdir().expect("tempdir");
    let two_per_segment = SEGMENT_HEADER.len() as u64 + record_size(1) + record_size(2);
    let config = small_config(dir.path(), 1 << 20, two_per_segment);
    {
        let mut log = SpoolLog::open(&config).expect("opens");
        for n in 1..=4 {
            append(&mut log, n);
        }
    }
    let files = segment_files(dir.path());
    let first = fs::read(&files[0]).expect("reads");
    let mut bad = first.clone();
    let last = bad.len() - 1;
    bad[last] ^= 0x01;
    fs::write(&files[0], &bad).expect("writes");

    let inner = LogBus::new();
    let spool = SpoolingBus::open(inner.clone(), config.clone())
        .await
        .expect("opens");
    assert!(matches!(spool.state(), SpoolState::Corrupt { .. }));
    wait_for(|| inner.ids().len() == 1).await;
    assert_eq!(inner.ids(), vec![changed(1).id]);
    // New publishes are durable behind the corruption, not sent.
    spool.publish(changed(5)).await.expect("spooled");
    assert!(!inner.holds(changed(5).id));
    spool.close().await;
    drop(spool);

    let discarded = discard_corrupt(&config)
        .await
        .expect("discards")
        .expect("there was a corruption");
    assert_eq!(discarded.segment, segment_name(1));
    let spool = SpoolingBus::open(inner.clone(), config)
        .await
        .expect("opens");
    wait_for(|| spool.state() == SpoolState::Direct).await;
    let expected: Vec<EventId> = [1, 3, 4, 5].into_iter().map(|n| changed(n).id).collect();
    assert_eq!(inner.ids(), expected, "record 2 was the discarded one");
}

/// The cursor is replaced atomically: a crash after any step of write,
/// fsync, rename leaves the old cursor or the new one, and a leftover
/// `cursor.tmp` is removed at open.
#[test]
fn the_cursor_is_replaced_atomically() {
    let old = CursorFile {
        last: 4,
        segment: 1,
        offset: 400,
    };
    let new = CursorFile {
        last: 9,
        segment: 5,
        offset: 900,
    };
    for (stop, expected) in [
        (Step::Written, old),
        (Step::Synced, old),
        (Step::Renamed, new),
        (Step::Done, new),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        cursor::write(dir.path(), &old).expect("writes");
        cursor::write_until(dir.path(), &new, stop).expect("writes");
        assert_eq!(
            cursor::read(dir.path()).expect("reads"),
            expected,
            "{stop:?}"
        );
        let config = small_config(dir.path(), 1 << 20, 1 << 16);
        drop(SpoolLog::open(&config).expect("opens"));
        assert!(!dir.path().join(cursor::CURSOR_TMP).exists(), "{stop:?}");
        assert_eq!(cursor::read(dir.path()).expect("reads"), expected);
    }
    // A cursor that does not decode is reported, not guessed.
    let dir = tempfile::tempdir().expect("tempdir");
    fs::write(dir.path().join(cursor::CURSOR), b"{\"last\":").expect("writes");
    let config = small_config(dir.path(), 1 << 20, 1 << 16);
    assert!(matches!(
        SpoolLog::open(&config),
        Err(SpoolError::Cursor { .. })
    ));
}

/// Segments roll at `segment_bytes`, are removed once the cursor is past
/// their last record, and a reopen resumes after the cursor.
#[test]
fn segments_roll_and_are_deleted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let two_per_segment = SEGMENT_HEADER.len() as u64 + record_size(1) + record_size(2);
    let config = small_config(dir.path(), 1 << 20, two_per_segment);
    let mut log = SpoolLog::open(&config).expect("opens");
    for n in 1..=5 {
        append(&mut log, n);
    }
    let names: Vec<String> = segment_files(dir.path())
        .iter()
        .map(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_owned()
        })
        .collect();
    assert_eq!(
        names,
        vec![segment_name(1), segment_name(3), segment_name(5)]
    );
    let on_disk: u64 = segment_files(dir.path())
        .iter()
        .map(|p| fs::metadata(p).expect("stat").len())
        .sum();
    assert_eq!(log.bytes(), on_disk);

    log.advance(3).expect("advances");
    assert_eq!(segment_files(dir.path()).len(), 2, "segment 1 is drained");
    drop(log);
    let mut log = SpoolLog::open(&config).expect("reopens");
    assert_eq!(numbers_of(&log), vec![4, 5], "resumes after the cursor");
    log.advance(2).expect("advances");
    assert_eq!(numbers_of(&log), Vec::<u64>::new());
    log.clear().expect("clears");
    assert!(segment_files(dir.path()).is_empty());
    assert_eq!(log.bytes(), 0);
    append(&mut log, 6);
    assert_eq!(numbers_of(&log), vec![6], "numbers are never reused");
}

/// `transport.spool.bounded`: an append that would take the segment files
/// past `max_bytes` is refused with the bytes the spool holds, and writes
/// nothing; the spool never exceeds the bound. Through the decorator, the
/// refusal is `SpoolFull` and does not wait.
#[tokio::test(start_paused = true)]
async fn append_past_the_bound_is_spool_full() {
    let dir = tempfile::tempdir().expect("tempdir");
    let size = record_size(1);
    let max = SEGMENT_HEADER.len() as u64 + 3 * size;
    let config = small_config(dir.path(), max, max);
    {
        let mut log = SpoolLog::open(&config).expect("opens");
        for n in 1..=3 {
            append(&mut log, n);
        }
        assert_eq!(log.bytes(), max);
        let before = fs::read(&segment_files(dir.path())[0]).expect("reads");
        match log.append(&payload(4), changed(4).at) {
            Err(AppendError::Full { bytes }) => assert_eq!(bytes, max),
            other => panic!("expected Full, got {other:?}"),
        }
        assert_eq!(
            fs::read(&segment_files(dir.path())[0]).expect("reads"),
            before
        );
        assert_eq!(log.counters().rejected_full, 1);
    }

    // Through the decorator, with the inner bus down.
    let dir = tempfile::tempdir().expect("tempdir");
    let config = small_config(dir.path(), max, max);
    let inner = LogBus::new();
    inner.set_up(false);
    let spool = SpoolingBus::open(inner.clone(), config)
        .await
        .expect("opens");
    for n in 1..=3 {
        spool.publish(changed(n)).await.expect("spooled");
    }
    let started = tokio::time::Instant::now();
    assert_eq!(
        spool.publish(changed(4)).await,
        Err(BusError::SpoolFull { bytes: max })
    );
    assert_eq!(started.elapsed(), Duration::ZERO, "the refusal never waits");
    let stats = spool.stats();
    assert!(stats.bytes <= stats.max_bytes);
    assert_eq!((stats.records, stats.rejected_full), (3, 1));
    // Once drained, there is room again.
    inner.set_up(true);
    wait_for(|| spool.state() == SpoolState::Direct).await;
    spool.publish(changed(4)).await.expect("direct");
    assert_eq!(inner.ids().len(), 4);
}

/// A second open of a spool directory is `Locked` until the first closes.
#[test]
fn a_second_open_is_locked() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = small_config(dir.path(), 1 << 20, 1 << 16);
    let first = SpoolLog::open(&config).expect("opens");
    assert!(matches!(
        SpoolLog::open(&config),
        Err(SpoolError::Locked { .. })
    ));
    drop(first);
    SpoolLog::open(&config).expect("free again");
}

/// Wait (in paused time, a few virtual seconds at most) for `done`.
pub(crate) async fn wait_for(mut done: impl FnMut() -> bool) {
    for _ in 0..2_000 {
        if done() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("condition not reached");
}

/// The decorator's states: direct while the inner bus answers; spooling
/// once it does not; draining in publish order when it answers again,
/// with later publishes queued behind the backlog; direct again at the
/// tail. `oldest_at` tracks the earliest spooled time.
#[tokio::test(start_paused = true)]
async fn publishes_go_direct_then_spool_then_drain_in_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = small_config(dir.path(), 1 << 20, 1 << 16);
    let inner = LogBus::new();
    let spool = SpoolingBus::open(inner.clone(), config)
        .await
        .expect("opens");
    assert_eq!(spool.state(), SpoolState::Direct);
    spool.publish(changed(1)).await.expect("direct");
    assert!(inner.holds(changed(1).id));
    assert_eq!(spool.oldest_at(), None);

    inner.set_up(false);
    // Later ids with earlier times: oldest_at is the minimum, not the first.
    let mut late = changed(2);
    late.at = Timestamp::from_micros(500);
    spool.publish(late.clone()).await.expect("spooled");
    assert_eq!(spool.state(), SpoolState::Spooling);
    let mut earliest = changed(3);
    earliest.at = Timestamp::from_micros(100);
    spool.publish(earliest.clone()).await.expect("spooled");
    assert_eq!(spool.oldest_at(), Some(Timestamp::from_micros(100)));
    assert_eq!(spool.stats().records, 2);

    inner.set_up(true);
    for n in 4..=12 {
        spool.publish(changed(n)).await.expect("published");
        tokio::task::yield_now().await;
    }
    wait_for(|| spool.state() == SpoolState::Direct).await;
    spool.publish(changed(13)).await.expect("direct");
    let expected: Vec<EventId> = std::iter::once(changed(1).id)
        .chain([late.id, earliest.id])
        .chain((4..=13).map(|n| changed(n).id))
        .collect();
    assert_eq!(inner.ids(), expected, "publish order, nothing overtakes");
    assert_eq!(spool.oldest_at(), None);
    let stats = spool.stats();
    assert_eq!((stats.records, stats.bytes), (0, 0));
    assert_eq!(stats.appended, stats.drained);
    assert!(segment_files(dir.path()).is_empty());
}

/// Only `Disconnected` is spooled; other errors of the inner bus pass
/// through untouched.
#[tokio::test(start_paused = true)]
async fn only_disconnected_is_spooled() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = small_config(dir.path(), 1 << 20, 1 << 16);
    let spool = SpoolingBus::open(Refusing, config).await.expect("opens");
    assert!(matches!(
        spool.publish(changed(1)).await,
        Err(BusError::PublishRejected { .. })
    ));
    assert_eq!(spool.state(), SpoolState::Direct);
    assert_eq!(spool.stats().records, 0);
}

/// An inner bus that refuses every publish without being down.
struct Refusing;

impl EventBus for Refusing {
    type Subscription = MpscSubscription;

    async fn publish(&self, _: Envelope) -> Result<(), BusError> {
        Err(BusError::PublishRejected {
            reason: "refused".to_owned(),
        })
    }

    async fn subscribe(
        &self,
        _: &[Subject],
        _: ConsumerGroup,
        _: RetryPolicy,
    ) -> Result<MpscSubscription, BusError> {
        Err(BusError::Disconnected)
    }
}

impl DrainTarget for Refusing {
    async fn probe(&self) -> Result<(), BusError> {
        Ok(())
    }

    async fn publish_batch(&self, _: Vec<Envelope>) -> Result<(), BusError> {
        Err(BusError::PublishRejected {
            reason: "refused".to_owned(),
        })
    }
}

/// A non-empty spool reopens `Spooling` and drains when the inner bus
/// answers; an `Ok` publish survives the crash.
#[tokio::test(start_paused = true)]
async fn a_reopened_spool_drains_what_it_held() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = small_config(dir.path(), 1 << 20, 1 << 16);
    let inner = LogBus::new();
    inner.set_up(false);
    let spool = SpoolingBus::open(inner.clone(), config.clone())
        .await
        .expect("opens");
    for n in 1..=3 {
        spool.publish(changed(n)).await.expect("spooled");
    }
    spool.crash().await;
    let spool = SpoolingBus::open(inner.clone(), config)
        .await
        .expect("reopens");
    assert_eq!(spool.state(), SpoolState::Spooling);
    assert_eq!(spool.stats().records, 3);
    inner.set_up(true);
    wait_for(|| spool.state() == SpoolState::Direct).await;
    let expected: Vec<EventId> = (1..=3).map(|n| changed(n).id).collect();
    assert_eq!(inner.ids(), expected);
}
