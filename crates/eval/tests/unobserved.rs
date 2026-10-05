//! An unobserved arrival (content read from a medium its sender never
//! wrote, INV-963) is out of reach exactly as an undecodable one is.

use crosstalk_eval::truth::kinds::SENDER_MEDIUM_UNOBSERVED;
use crosstalk_eval::truth::{MatchNeed, Tier};
use crosstalk_spec::aggregates::quality::MatchClass;

#[test]
fn an_unobserved_need_is_out_of_reach_and_keeps_its_arrival_class() {
    let need = MatchNeed::sender_medium_unobserved(MatchClass::Normalized);
    assert!(need.out_of_reach());
    assert_eq!(need.tier(Tier::Construction), Tier::OutOfReach);
    assert_eq!(need.class(), MatchClass::Normalized);
    assert_eq!(
        need,
        MatchNeed::Unobserved {
            reason: SENDER_MEDIUM_UNOBSERVED.into(),
            arrival: MatchClass::Normalized,
        }
    );
    for reachable in [
        MatchNeed::Exact,
        MatchNeed::Normalized,
        MatchNeed::json_string(),
    ] {
        assert!(!reachable.out_of_reach());
        assert_eq!(reachable.tier(Tier::Construction), Tier::Construction);
    }
    assert!(MatchNeed::two_string_levels().out_of_reach());
}

#[test]
fn an_unobserved_need_round_trips_through_json() {
    let need = MatchNeed::sender_medium_unobserved(MatchClass::Exact);
    let json = serde_json::to_string(&need).unwrap_or_else(|e| panic!("{e}"));
    assert!(json.contains("\"unobserved\""), "{json}");
    let back: MatchNeed = serde_json::from_str(&json).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(back, need);
}
