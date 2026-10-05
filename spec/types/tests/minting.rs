//! The ULID generator (`crate::ids::mint`): monotonic per generator,
//! whatever its clock reads or the time it is given, distinct across
//! generators, and a function of its clock and seed.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use proptest::prelude::*;

use crate::ids::mint::{MAX_ULID_MILLIS, ulid_millis};
use crate::ids::{AgentId, EntityId, RandomSource, SeededRandom, UlidExhausted, UlidGenerator};
use crate::observed::exchange::ConnectionId;
use crate::support::{Clock, Timestamp};

/// A clock that reads a fixed script of times, one per reading, repeating
/// the last one when the script runs out.
struct ScriptClock {
    readings: Vec<Timestamp>,
    next: AtomicUsize,
}

impl ScriptClock {
    fn new(micros: &[u64]) -> Arc<Self> {
        Arc::new(Self {
            readings: micros.iter().copied().map(Timestamp::from_micros).collect(),
            next: AtomicUsize::new(0),
        })
    }
}

impl Clock for ScriptClock {
    fn now(&self) -> Timestamp {
        let at = self.next.fetch_add(1, Ordering::Relaxed);
        let last = self.readings.len().saturating_sub(1);
        self.readings
            .get(at.min(last))
            .copied()
            .unwrap_or(Timestamp::from_micros(0))
    }
}

/// Always draws the same word.
struct Constant(u64);

impl RandomSource for Constant {
    fn next_u64(&mut self) -> u64 {
        self.0
    }
}

const MS: u64 = 1_000;
const T: u64 = 1_790_000_000_000 * MS;

fn generator<R: RandomSource>(micros: &[u64], random: R) -> UlidGenerator<R> {
    UlidGenerator::new(ScriptClock::new(micros), random)
}

fn next<R: RandomSource>(generator: &mut UlidGenerator<R>) -> u128 {
    generator
        .next_ulid()
        .unwrap_or_else(|error| panic!("{error}"))
}

/// A new millisecond draws a fresh random part under its time; the same
/// millisecond, or an earlier one, increments the last id.
#[test]
fn same_or_earlier_millisecond_increments_the_last_id() {
    let mut ids = generator(
        &[T, T + 999, T - 5 * MS, T + MS, T + MS],
        SeededRandom::new(7),
    );
    let first = next(&mut ids);
    assert_eq!(ulid_millis(first), T / MS);
    assert_eq!(next(&mut ids), first + 1, "same millisecond");
    assert_eq!(next(&mut ids), first + 2, "a clock stepped back");
    let later = next(&mut ids);
    assert_eq!(ulid_millis(later), T / MS + 1, "a new millisecond");
    assert_ne!(later & ((1 << 80) - 1), (first + 2) & ((1 << 80) - 1));
    assert_eq!(next(&mut ids), later + 1);
}

/// A full random part carries into the next millisecond, and the largest
/// ULID has no successor.
#[test]
fn a_full_random_part_carries_and_the_last_ulid_is_the_end() {
    let mut ids = generator(&[T], Constant(u64::MAX));
    let full = next(&mut ids);
    assert_eq!(full & ((1 << 80) - 1), (1 << 80) - 1);
    let carried = next(&mut ids);
    assert_eq!(carried, full + 1);
    assert_eq!(ulid_millis(carried), T / MS + 1);

    let mut end = generator(&[u64::MAX], Constant(u64::MAX));
    let last = next(&mut end);
    assert_eq!(last, u128::MAX);
    assert_eq!(ulid_millis(last), MAX_ULID_MILLIS);
    assert_eq!(end.next_ulid(), Err(UlidExhausted));
}

/// `next_at` stamps the time it is given, not the clock's reading, and
/// keeps the same monotonic rule: the same or an earlier millisecond than
/// the last id (minted either way) increments it.
#[test]
fn next_at_stamps_the_given_time_and_stays_monotonic() {
    let mut ids = generator(&[T + 50 * MS], SeededRandom::new(5));
    let at = |micros: u64| Timestamp::from_micros(micros);
    let first = ids.next_at(at(T)).unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(
        ulid_millis(first),
        T / MS,
        "the given time, not the clock's"
    );
    let same = ids
        .next_at(at(T + 999))
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(same, first + 1, "same millisecond");
    let earlier = ids
        .next_at(at(T - 7 * MS))
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(earlier, first + 2, "an earlier time");
    let later = ids
        .next_at(at(T + 2 * MS))
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(ulid_millis(later), T / MS + 2, "a later millisecond");
    assert_ne!(later & ((1 << 80) - 1), (first + 2) & ((1 << 80) - 1));
    // The clock's reading (T + 50 ms) and a given time share one last id.
    let clocked = next(&mut ids);
    assert_eq!(ulid_millis(clocked), T / MS + 50);
    let behind = ids
        .next_at(at(T + 3 * MS))
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(behind, clocked + 1, "a given time behind the clock's id");
    let typed: AgentId = ids
        .mint_at(at(T + 60 * MS))
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(ulid_millis(typed.as_ulid()), T / MS + 60);

    let mut end = generator(&[T], Constant(u64::MAX));
    assert_eq!(end.next_at(at(u64::MAX)), Ok(u128::MAX));
    assert_eq!(end.next_at(at(T)), Err(UlidExhausted));
}

/// The same clock and seed mint the same ids; another seed, others.
#[test]
fn minting_is_a_function_of_clock_and_seed() {
    let script = [T, T, T + 3 * MS, T + 2 * MS, T + 9 * MS];
    let run = |seed| {
        let mut ids = generator(&script, SeededRandom::new(seed));
        (0..script.len())
            .map(|_| next(&mut ids))
            .collect::<Vec<_>>()
    };
    assert_eq!(run(11), run(11));
    assert_ne!(run(11), run(12));
}

/// Ids come typed: any entity id, and a connection id.
#[test]
fn generators_mint_typed_ids() {
    let mut ids = generator(&[T], SeededRandom::new(3));
    let agent: AgentId = ids.mint().unwrap_or_else(|error| panic!("{error}"));
    let connection: ConnectionId = ids.mint().unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(connection.as_ulid(), agent.as_ulid() + 1);
    assert_eq!(ulid_millis(agent.as_ulid()), T / MS);
}

/// Two entropy-seeded sources start apart.
#[test]
fn entropy_seeded_sources_differ() {
    let mut one = SeededRandom::from_entropy();
    let mut two = SeededRandom::from_entropy();
    assert_ne!(one.next_u64(), two.next_u64());
}

/// A clock script: steps forward, repeats and steps back, within a few
/// milliseconds of each other so readings collide often.
fn arb_script() -> impl Strategy<Value = Vec<u64>> {
    proptest::collection::vec(-3i64..=4, 1..64).prop_map(|steps| {
        let mut at = T;
        steps
            .into_iter()
            .map(|step| {
                at = at.saturating_add_signed(step * 400);
                at
            })
            .collect()
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// `canonical.ids.ulid-monotonic`.
    #[test]
    fn ids_increase_whatever_the_clock_reads(script in arb_script(), seed in any::<u64>()) {
        let mut ids = generator(&script, SeededRandom::new(seed));
        let mut last = None;
        for _ in 0..script.len() + 8 {
            let id = ids.next_ulid().map_err(|error| TestCaseError::fail(error.to_string()))?;
            if let Some(last) = last {
                prop_assert!(id > last, "{id:#x} after {last:#x}");
            }
            last = Some(id);
        }
    }

    /// `canonical.ids.ulid-monotonic`, for ids stamped with given times
    /// that repeat and step back, interleaved with clock-stamped ones.
    #[test]
    fn ids_increase_whatever_time_they_are_given(
        script in arb_script(),
        given in arb_script(),
        seed in any::<u64>(),
        stamped in proptest::collection::vec(any::<bool>(), 1..96),
    ) {
        let mut ids = generator(&script, SeededRandom::new(seed));
        let mut times = given.iter().copied().cycle();
        let mut last = None;
        for use_given in stamped {
            let id = match (use_given, times.next()) {
                (true, Some(micros)) => ids.next_at(Timestamp::from_micros(micros)),
                _ => ids.next_ulid(),
            }
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
            if let Some(last) = last {
                prop_assert!(id > last, "{id:#x} after {last:#x}");
            }
            last = Some(id);
        }
    }

    /// `canonical.ids.ulid-unique`: generators with their own seeds and
    /// clocks (skewed, repeating, stepping back), minting interleaved,
    /// never mint one id twice.
    #[test]
    fn minted_ids_are_distinct(
        generators in proptest::collection::vec((arb_script(), any::<u64>()), 1..5),
        order in proptest::collection::vec(any::<prop::sample::Index>(), 1..200),
    ) {
        let mut seeds = BTreeSet::new();
        let mut all: Vec<_> = generators
            .iter()
            .filter(|(_, seed)| seeds.insert(*seed))
            .map(|(script, seed)| generator(script, SeededRandom::new(*seed)))
            .collect();
        let mut seen = BTreeSet::new();
        for pick in order {
            let ids = &mut all[pick.index(seeds.len())];
            let id = ids.next_ulid().map_err(|error| TestCaseError::fail(error.to_string()))?;
            prop_assert!(seen.insert(id), "{id:#x} minted twice");
        }
    }
}
