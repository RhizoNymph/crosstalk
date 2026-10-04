//! The L8 conformance suite, run against the fixture.

crosstalk_conformance::suite!(
    crate::conformance::FixtureHarness::new().expect("the fixture's clock constants")
);
