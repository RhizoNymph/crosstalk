//! Reference tests for the fingerprint index, one or more per invariant
//! that names `FingerprintIndex`. Each test's doc names the invariant.

use std::collections::BTreeSet;
use std::num::NonZeroU16;
use std::time::Duration;

use crosstalk_spec::derived::provenance::fingerprint::{
    Fingerprint, FingerprintHit, PositionedFingerprint,
};
use crosstalk_spec::derived::provenance::span::OriginatedSpan;
use crosstalk_spec::interfaces::l4_provenance::{FingerprintIndex, IndexError};
use crosstalk_spec::support::Timestamp;
use proptest::prelude::*;

use super::model::{self, originated, span_id};
use super::{IndexConfig, MemoryFingerprintIndex};
use crate::pipeline::ManualClock;
use crate::pipeline::harness::HarnessConfig;

fn at(micros: u64) -> Timestamp {
    Timestamp::from_micros(micros)
}

fn fp(n: u64) -> Fingerprint {
    Fingerprint(n)
}

fn positioned(n: u64, offset: u32) -> PositionedFingerprint {
    PositionedFingerprint {
        fingerprint: fp(n),
        offset,
    }
}

fn span(n: u8) -> OriginatedSpan {
    originated(n).unwrap_or_else(|| panic!("span fixture {n}"))
}

/// A single-node index with cutoff 1 and retention 100µs at time 0.
fn index() -> (MemoryFingerprintIndex, ManualClock) {
    let clock = ManualClock::default();
    let config = IndexConfig::single_node(1, Duration::from_micros(100));
    (MemoryFingerprintIndex::new(config, clock.clone()), clock)
}

/// `provenance.index.cutoff-not-inserted`.
#[tokio::test]
async fn insert_skips_fingerprints_above_cutoff() {
    let (mut index, _clock) = index();
    assert_eq!(index.observe(&[fp(1)], at(0)).await, Ok(()));
    assert_eq!(index.observe(&[fp(1), fp(1)], at(0)).await, Ok(()));
    assert_eq!(index.frequency(fp(1)).await, Ok(2));
    assert_eq!(
        index
            .insert(&span(0), &[positioned(1, 0), positioned(2, 4)])
            .await,
        Ok(())
    );
    let postings = index.snapshot().postings;
    assert!(!postings.contains_key(&fp(1)));
    assert!(postings.contains_key(&fp(2)));
}

/// `provenance.index.cutoff-not-returned`: a posting stored while below the
/// cutoff is not returned once the fingerprint crosses it.
#[tokio::test]
async fn lookup_ignores_fingerprints_that_crossed_cutoff() {
    let (mut index, _clock) = index();
    assert_eq!(index.insert(&span(0), &[positioned(1, 3)]).await, Ok(()));
    let query = [positioned(1, 7)];
    assert_eq!(
        index.lookup(&query).await,
        Ok(vec![FingerprintHit {
            fingerprint: fp(1),
            span: span_id(0),
            span_offset: 3,
            query_offset: 7
        }])
    );
    assert_eq!(index.observe(&[fp(1)], at(0)).await, Ok(()));
    assert_eq!(index.observe(&[fp(1)], at(0)).await, Ok(()));
    assert_eq!(index.lookup(&query).await, Ok(Vec::new()));
}

/// `provenance.index.wrong-shard-rejected`, for `insert`.
#[tokio::test]
async fn insert_on_wrong_shard_errors() {
    let two = NonZeroU16::MIN.saturating_add(1);
    let Ok(config) = IndexConfig::sharded(5, Duration::from_micros(100), two, BTreeSet::from([0]))
    else {
        panic!("config");
    };
    let mut index = MemoryFingerprintIndex::new(config, ManualClock::default());
    assert_eq!(
        index
            .insert(&span(0), &[positioned(2, 0), positioned(3, 0)])
            .await,
        Err(IndexError::WrongShard { fingerprint: fp(3) })
    );
    assert!(index.snapshot().postings.is_empty());
}

/// `provenance.index.wrong-shard-rejected`, for `lookup`.
#[tokio::test]
async fn lookup_on_wrong_shard_errors() {
    let two = NonZeroU16::MIN.saturating_add(1);
    let Ok(config) = IndexConfig::sharded(5, Duration::from_micros(100), two, BTreeSet::from([1]))
    else {
        panic!("config");
    };
    let index = MemoryFingerprintIndex::new(config, ManualClock::default());
    assert_eq!(
        index.lookup(&[positioned(1, 0), positioned(4, 0)]).await,
        Err(IndexError::WrongShard { fingerprint: fp(4) })
    );
}

/// `provenance.index.originated-indexed`, at the index: every below-cutoff
/// fingerprint inserted for a span finds it.
#[tokio::test]
async fn originated_span_fingerprints_are_indexed() {
    let (mut index, _clock) = index();
    let fingerprints = [positioned(1, 0), positioned(2, 5), positioned(3, 9)];
    assert_eq!(index.insert(&span(4), &fingerprints).await, Ok(()));
    let Ok(hits) = index.lookup(&fingerprints).await else {
        panic!("lookup");
    };
    let found: Vec<(Fingerprint, u32)> = hits
        .iter()
        .filter(|hit| hit.span == span_id(4))
        .map(|hit| (hit.fingerprint, hit.span_offset))
        .collect();
    assert_eq!(found, vec![(fp(1), 0), (fp(2), 5), (fp(3), 9)]);
}

/// `provenance.match.none-after-expiry`, at the index: an evicted span is
/// never returned.
#[tokio::test]
async fn lookup_after_evict_has_no_hits() {
    let (mut index, _clock) = index();
    assert_eq!(index.insert(&span(0), &[positioned(1, 0)]).await, Ok(()));
    assert_eq!(index.insert(&span(1), &[positioned(1, 2)]).await, Ok(()));
    assert_eq!(index.evict(&[span_id(0)]).await, Ok(()));
    let Ok(hits) = index.lookup(&[positioned(1, 0)]).await else {
        panic!("lookup");
    };
    assert!(hits.iter().all(|hit| hit.span != span_id(0)));
    assert_eq!(hits.len(), 1);
}

/// `provenance.index.retention-bound`, at the index: observations age out
/// of the frequency and out of the stored data.
#[tokio::test]
async fn observations_age_out() {
    let (mut index, clock) = index();
    assert_eq!(index.observe(&[fp(1)], at(10)).await, Ok(()));
    assert_eq!(index.frequency(fp(1)).await, Ok(1));
    clock.set(at(110));
    assert_eq!(index.frequency(fp(1)).await, Ok(1));
    clock.set(at(111));
    assert_eq!(index.frequency(fp(1)).await, Ok(0));
    assert_eq!(index.observe(&[fp(2)], at(111)).await, Ok(()));
    let observations = index.snapshot().observations;
    assert_eq!(observations.len(), 1);
    assert!(observations.iter().all(|(_, fps)| !fps.contains(&fp(1))));
}

proptest! {
    /// `provenance.index.frequency-counts-observed-texts`: frequency is the
    /// number of observed texts containing the fingerprint within the
    /// retention period, against a direct count.
    #[test]
    fn frequency_matches_observation_model(
        texts in proptest::collection::vec((proptest::collection::vec(0u64..6, 0..5), 0u64..300), 0..12),
        now in 0u64..400,
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let (index, clock) = index();
        let mut index = index;
        let counted = runtime.block_on(async {
            for (fingerprints, t) in &texts {
                let fingerprints: Vec<Fingerprint> = fingerprints.iter().map(|n| fp(*n)).collect();
                let _ = index.observe(&fingerprints, at(*t)).await;
            }
            clock.set(at(now));
            let mut counted = Vec::new();
            for n in 0..6 {
                counted.push(index.frequency(fp(n)).await);
            }
            counted
        });
        for (n, frequency) in (0u64..6).zip(counted) {
            let expected = texts
                .iter()
                .filter(|(fingerprints, t)| fingerprints.contains(&n) && t + 100 >= now)
                .count();
            prop_assert_eq!(frequency, Ok(u64::try_from(expected).unwrap_or(u64::MAX)));
        }
    }
}

/// The reference agrees with itself under the harness, which proves the
/// harness runs; see `harness_rejects_an_index_that_ignores_the_cutoff`
/// for the converse.
#[test]
fn reference_agrees_with_itself_under_the_harness() {
    model::check_fingerprint_index(HarnessConfig::default(), MemoryFingerprintIndex::new);
}

/// The harness catches an index that keeps returning boilerplate.
#[test]
#[should_panic(expected = "disagrees with the reference")]
fn harness_rejects_an_index_that_ignores_the_cutoff() {
    model::check_fingerprint_index(HarnessConfig::default(), |config, clock| NoCutoff {
        inner: MemoryFingerprintIndex::new(
            IndexConfig::single_node(u64::MAX, config.retention()),
            clock,
        ),
    });
}

/// An index with no cutoff at all.
struct NoCutoff {
    inner: MemoryFingerprintIndex,
}

impl FingerprintIndex for NoCutoff {
    async fn insert(
        &mut self,
        span: &OriginatedSpan,
        fingerprints: &[PositionedFingerprint],
    ) -> Result<(), IndexError> {
        self.inner.insert(span, fingerprints).await
    }

    async fn lookup(
        &self,
        fingerprints: &[PositionedFingerprint],
    ) -> Result<Vec<FingerprintHit>, IndexError> {
        self.inner.lookup(fingerprints).await
    }

    async fn frequency(&self, fingerprint: Fingerprint) -> Result<u64, IndexError> {
        self.inner.frequency(fingerprint).await
    }

    async fn observe(
        &mut self,
        fingerprints: &[Fingerprint],
        at: Timestamp,
    ) -> Result<(), IndexError> {
        self.inner.observe(fingerprints, at).await
    }

    async fn evict(&mut self, spans: &[crosstalk_spec::ids::SpanId]) -> Result<(), IndexError> {
        self.inner.evict(spans).await
    }
}

/// The index is `Send + Sync`, so its futures are `Send`.
#[test]
fn the_index_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync + 'static>() {}
    assert_send_sync::<MemoryFingerprintIndex>();
}
