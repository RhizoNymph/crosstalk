use std::num::NonZeroU64;
use std::time::Duration;

use crate::rng::{DurationRange, InvalidDurationRange, Probability, Seed, SimRng};

fn draws(seed: u64, n: usize) -> Vec<u64> {
    let mut rng = SimRng::new(Seed::new(seed));
    (0..n).map(|_| rng.next_u64()).collect()
}

#[test]
fn same_seed_same_sequence() {
    assert_eq!(draws(42, 64), draws(42, 64));
}

#[test]
fn different_seeds_differ() {
    assert_ne!(draws(1, 8), draws(2, 8));
}

#[test]
fn splitmix_matches_reference_values() {
    // SplitMix64 from seed 0, as published with the algorithm.
    assert_eq!(
        draws(0, 3),
        vec![
            0xE220_A839_7B1D_CDAF,
            0x6E78_9E6A_A1B9_65F4,
            0x06C4_5D18_8009_454F
        ]
    );
}

#[test]
fn below_stays_in_bound_and_reaches_every_value() {
    let mut rng = SimRng::new(Seed::new(7));
    let bound = NonZeroU64::new(7).expect("non-zero");
    let mut seen = [false; 7];
    for _ in 0..1000 {
        let value = rng.below(bound);
        assert!(value < 7);
        seen[usize::try_from(value).expect("small")] = true;
    }
    assert!(seen.iter().all(|hit| *hit));
}

#[test]
fn index_of_empty_is_none() {
    let mut rng = SimRng::new(Seed::new(3));
    assert_eq!(rng.index(0), None);
    assert_eq!(rng.pick::<u8>(&[]), None);
    assert_eq!(rng.index(1), Some(0));
}

#[test]
fn never_and_always_draw_nothing() {
    let mut rng = SimRng::new(Seed::new(9));
    let before = rng.clone();
    assert!(!rng.chance(Probability::NEVER));
    assert!(rng.chance(Probability::ALWAYS));
    assert_eq!(rng, before);
}

#[test]
fn chance_tracks_its_probability() {
    let mut rng = SimRng::new(Seed::new(11));
    let p = Probability::new(0.25).expect("valid");
    let hits = (0..10_000).filter(|_| rng.chance(p)).count();
    assert!((2_200..=2_800).contains(&hits), "{hits} hits");
}

#[test]
fn duration_in_stays_in_range() {
    let mut rng = SimRng::new(Seed::new(5));
    let range =
        DurationRange::new(Duration::from_millis(3), Duration::from_millis(9)).expect("ordered");
    for _ in 0..500 {
        let d = rng.duration_in(range);
        assert!(d >= range.min() && d <= range.max(), "{d:?}");
    }
    let exact = DurationRange::exactly(Duration::from_secs(2));
    assert_eq!(rng.duration_in(exact), Duration::from_secs(2));
}

#[test]
fn shuffle_permutes_and_follows_the_seed() {
    let shuffled = |seed| {
        let mut items: Vec<u32> = (0..20).collect();
        SimRng::new(Seed::new(seed)).shuffle(&mut items);
        items
    };
    let mut sorted = shuffled(1);
    sorted.sort_unstable();
    assert_eq!(sorted, (0..20).collect::<Vec<_>>());
    assert_eq!(shuffled(1), shuffled(1));
    assert_ne!(shuffled(1), shuffled(2));
}

#[test]
fn forks_are_seeded_and_independent() {
    let mut a = SimRng::new(Seed::new(13));
    let mut b = SimRng::new(Seed::new(13));
    let (mut fa, mut fb) = (a.fork(), b.fork());
    assert_eq!(fa.next_u64(), fb.next_u64());
    let mut second = a.fork();
    assert_ne!(fa.next_u64(), second.next_u64());
    assert_ne!(a.next_u64(), fa.next_u64());
}

#[test]
fn probability_rejects_out_of_range_and_nan() {
    assert!(Probability::new(-0.1).is_err());
    assert!(Probability::new(1.5).is_err());
    assert!(Probability::new(f64::NAN).is_err());
    assert!(Probability::percent(101).is_err());
    assert_eq!(Probability::percent(100), Ok(Probability::ALWAYS));
    assert_eq!(Probability::percent(0), Ok(Probability::NEVER));
}

#[test]
fn duration_range_rejects_inverted() {
    let (min, max) = (Duration::from_secs(2), Duration::from_secs(1));
    assert_eq!(
        DurationRange::new(min, max),
        Err(InvalidDurationRange { min, max })
    );
}

#[test]
fn seed_parses_and_displays_as_decimal() {
    assert_eq!(" 1234 ".parse::<Seed>(), Ok(Seed::new(1234)));
    assert!("x".parse::<Seed>().is_err());
    assert_eq!(Seed::new(99).to_string(), "99");
}
