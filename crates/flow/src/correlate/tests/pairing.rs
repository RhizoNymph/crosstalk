//! The pairing rules on their own.

use crosstalk_spec::derived::flow::evidence::InvalidCoAccess;

use super::fixtures::{Scene, secs, timing};
use crate::correlate::pairing::{self, NoPair, WriteOutcome};

/// `Delivered` and `Unknown` writes pair, `Rejected` ones never
/// (`flow.correlator.unknown-write-pairs`, `flow.coaccess.write-not-rejected`).
#[test]
fn only_rejected_writes_never_pair() {
    assert!(WriteOutcome::Delivered.pairs());
    assert!(WriteOutcome::Unknown.pairs());
    assert!(!WriteOutcome::Rejected.pairs());
}

/// A write held without a result settles `settle_after` after it.
#[test]
fn a_write_settles_after_the_settle_window() {
    let settles = pairing::write_settles_at(timing(), secs(10));
    assert_eq!(
        settles.as_micros() - secs(10).as_micros(),
        u64::try_from(timing().settle_after().as_micros()).unwrap_or(u64::MAX)
    );
}

/// A co-access is `CoAccess::new`'s: same resource, two agents, a write
/// then a later read within the window.
#[test]
fn co_access_follows_the_spec_constructor() {
    let mut scene = Scene::new(30);
    let (a, b) = (scene.agent(), scene.agent());
    let (r, s) = (scene.resource(), scene.resource());
    let write = scene.write(a, r, secs(0), vec![]);
    assert!(pairing::co_access(&write, &scene.read(b, r, secs(1)), timing()).is_ok());
    assert_eq!(
        pairing::co_access(&write, &scene.read(a, r, secs(1)), timing()),
        Err(NoPair::Invalid(InvalidCoAccess::SameAgent))
    );
    assert_eq!(
        pairing::co_access(&write, &scene.read(b, s, secs(1)), timing()),
        Err(NoPair::Invalid(InvalidCoAccess::DifferentResources))
    );
    assert_eq!(
        pairing::co_access(&write, &scene.read(b, r, secs(0)), timing()),
        Err(NoPair::Invalid(InvalidCoAccess::ReadNotAfterWrite))
    );
    assert_eq!(
        pairing::co_access(&write, &scene.read(b, r, secs(601)), timing()),
        Err(NoPair::Invalid(InvalidCoAccess::OutsideWindow))
    );
}

/// A match is carried by the read whose result part it sits in, and
/// explained by a write of its origin agent holding its span.
#[test]
fn carriage_and_explanation() {
    let mut scene = Scene::new(31);
    let (a, b, c) = (scene.agent(), scene.agent(), scene.agent());
    let r = scene.resource();
    let span = scene.span();
    let write = scene.write(a, r, secs(0), vec![span]);
    let read = scene.read(b, r, secs(1));
    let other_read = scene.read(b, r, secs(2));
    let content = scene.carried(&read, a, span);
    assert!(pairing::carried_by(&content, &read));
    assert!(!pairing::carried_by(&content, &other_read));
    assert!(!pairing::carried_by(&content, &write));
    assert!(pairing::links(&content, &write));
    let foreign = scene.carried(&read, c, span);
    assert!(!pairing::links(&foreign, &write), "another agent's match");
    let other_span = scene.span();
    let elsewhere = scene.carried(&read, a, other_span);
    assert!(
        !pairing::links(&elsewhere, &write),
        "a span the write did not hold"
    );
}
