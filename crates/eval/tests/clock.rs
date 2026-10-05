//! The virtual clock's pace: calls of a dataset without times are a
//! deterministic 1 to 5 s apart, in component order.

use std::time::Duration;

use crosstalk_eval::corpus::clock::{
    ClockError, EPOCH_MICROS, MINOR_LIMIT, Pace, SUB_LIMIT, compose,
};

fn at(pace: &Pace, major: u64, minor: u64, sub: u64) -> u64 {
    pace.at(major, minor, sub)
        .unwrap_or_else(|e| panic!("{e}"))
        .as_micros()
}

#[test]
fn the_default_pace_steps_one_to_five_seconds() {
    let pace = Pace::DEFAULT;
    assert_eq!(pace.min(), Duration::from_secs(1));
    assert_eq!(pace.max(), Duration::from_secs(5));
    assert_eq!(at(&pace, 0, 0, 0), EPOCH_MICROS);
    let mut gaps = std::collections::BTreeSet::new();
    for major in 0..10_000 {
        let gap = at(&pace, major + 1, 0, 0) - at(&pace, major, 0, 0);
        assert!(
            (1_000_000..=5_000_000).contains(&gap),
            "step {major}: {gap} µs"
        );
        gaps.insert(gap);
    }
    // Jittered, not one fixed step.
    assert!(gaps.len() > 100);
}

#[test]
fn components_keep_their_order_under_any_pace() {
    let slow = Pace::new(Duration::from_secs(1), Duration::from_secs(1), 3)
        .unwrap_or_else(|e| panic!("{e}"));
    for pace in [Pace::DEFAULT, slow] {
        for major in 0..1_000 {
            assert!(at(&pace, major, MINOR_LIMIT - 1, SUB_LIMIT - 1) < at(&pace, major + 1, 0, 0));
            assert!(at(&pace, major, 3, SUB_LIMIT - 1) < at(&pace, major, 4, 0));
        }
    }
}

#[test]
fn a_pace_is_deterministic_and_seeded() {
    let one = |seed| {
        Pace::new(Duration::from_secs(1), Duration::from_secs(5), seed)
            .unwrap_or_else(|e| panic!("{e}"))
    };
    assert_eq!(one(0), Pace::DEFAULT);
    let times = |pace: Pace| (0..50).map(|m| at(&pace, m, 0, 0)).collect::<Vec<_>>();
    assert_eq!(times(one(9)), times(one(9)));
    assert_ne!(times(one(9)), times(one(10)));
    // `compose` is the default pace.
    for major in 0..50 {
        assert_eq!(
            compose(major, 2, 3).map(|t| t.as_micros()),
            Ok(at(&Pace::DEFAULT, major, 2, 3))
        );
    }
}

#[test]
fn a_pace_steps_within_its_bounds() {
    let pace = Pace::new(
        Duration::from_millis(2_000),
        Duration::from_millis(2_400),
        1,
    )
    .unwrap_or_else(|e| panic!("{e}"));
    for major in 0..1_000 {
        let gap = at(&pace, major + 1, 0, 0) - at(&pace, major, 0, 0);
        assert!((2_000_000..=2_400_000).contains(&gap), "{gap}");
    }
}

#[test]
fn a_pace_too_fine_for_its_components_is_refused() {
    assert_eq!(
        Pace::new(Duration::from_millis(999), Duration::from_secs(5), 0),
        Err(ClockError::Pace)
    );
    assert_eq!(
        Pace::new(Duration::from_secs(5), Duration::from_secs(1), 0),
        Err(ClockError::Pace)
    );
    assert!(Pace::DEFAULT.at(u64::MAX, 0, 0).is_err());
    assert!(Pace::DEFAULT.at(0, MINOR_LIMIT, 0).is_err());
    assert!(Pace::DEFAULT.at(0, 0, SUB_LIMIT).is_err());
}
