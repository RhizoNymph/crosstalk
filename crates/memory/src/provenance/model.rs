//! Model-based property harness for `FingerprintIndex` implementations.
//!
//! [`check_fingerprint_index`] generates random sequences of
//! [`IndexOp`]s (inserts of originated spans, lookups, frequency reads,
//! observations, evictions and clock moves) over a small pool of
//! fingerprints, so fingerprints repeat, cross the cutoff and age out. It
//! runs each sequence on the index under test and on
//! [`MemoryFingerprintIndex`], both built from the same [`IndexConfig`] and
//! sharing one [`ManualClock`], and requires equal results from every
//! operation. Lookup hits are compared as multisets, since the trait does
//! not order them. Each run checks two configurations: a single node, and
//! a node owning one of two shards (which exercises `WrongShard`).

use std::collections::BTreeSet;
use std::num::NonZeroU16;
use std::time::Duration;

use crosstalk_spec::derived::provenance::fingerprint::{
    Fingerprint, FingerprintHit, PositionedFingerprint,
};
use crosstalk_spec::derived::provenance::span::{OriginatedSpan, Span, SpanLocation, SpanState};
use crosstalk_spec::ids::{AgentId, ExchangeId, MessageHash, SpanId};
use crosstalk_spec::interfaces::l4_provenance::{FingerprintIndex, IndexError};
use crosstalk_spec::observed::message::PartRef;
use crosstalk_spec::support::{Blake3, ByteRange, Timestamp};
use proptest::prelude::*;

use super::index::{IndexConfig, MemoryFingerprintIndex};
use crate::pipeline::ManualClock;
use crate::pipeline::harness::{HarnessConfig, Mismatch, run, same};

/// The cutoff both configurations use: a fingerprint observed in more than
/// two live texts is boilerplate.
pub const CUTOFF: u64 = 2;

/// How long observations count, in microseconds.
pub const RETENTION_MICROS: u64 = 100;

/// The configurations the harness checks.
pub fn configs() -> Vec<IndexConfig> {
    let retention = Duration::from_micros(RETENTION_MICROS);
    let mut configs = vec![IndexConfig::single_node(CUTOFF, retention)];
    let two = NonZeroU16::MIN.saturating_add(1);
    if let Ok(sharded) = IndexConfig::sharded(CUTOFF, retention, two, BTreeSet::from([0])) {
        configs.push(sharded);
    }
    configs
}

/// The `n`th span id of the pool.
pub fn span_id(n: u8) -> SpanId {
    SpanId::from_ulid(0x5DA0_0000 | u128::from(n))
}

/// An originated span with id `span_id(n)`.
pub fn originated(n: u8) -> Option<OriginatedSpan> {
    let range = ByteRange::new(0, 64).ok()?;
    OriginatedSpan::new(Span {
        id: span_id(n),
        location: SpanLocation {
            part: PartRef {
                message: MessageHash::from_digest(Blake3::from_bytes([n; 32])),
                index: 0,
            },
            range,
        },
        agent: AgentId::from_ulid(0xA6E0 | u128::from(n % 3)),
        exchange: ExchangeId::from_ulid(0xE0 | u128::from(n)),
        state: SpanState::Originated,
    })
}

/// One step. Fingerprints are `Fingerprint(n)` for small `n`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexOp {
    Insert {
        span: u8,
        fingerprints: Vec<(u8, u8)>,
    },
    Lookup {
        fingerprints: Vec<(u8, u8)>,
    },
    Frequency {
        fingerprint: u8,
    },
    Observe {
        fingerprints: Vec<u8>,
        at: u64,
    },
    Evict {
        spans: Vec<u8>,
    },
    /// Set the shared clock.
    Clock {
        now: u64,
    },
}

fn positioned(pairs: &[(u8, u8)]) -> Vec<PositionedFingerprint> {
    pairs
        .iter()
        .map(|(fingerprint, offset)| PositionedFingerprint {
            fingerprint: Fingerprint(u64::from(*fingerprint)),
            offset: u32::from(*offset),
        })
        .collect()
}

fn pairs() -> impl Strategy<Value = Vec<(u8, u8)>> {
    proptest::collection::vec((0u8..8, 0u8..4), 0..5)
}

/// One generated operation.
pub fn index_op() -> impl Strategy<Value = IndexOp> {
    prop_oneof![
        4 => (0u8..6, pairs()).prop_map(|(span, fingerprints)| IndexOp::Insert { span, fingerprints }),
        3 => pairs().prop_map(|fingerprints| IndexOp::Lookup { fingerprints }),
        2 => (0u8..8).prop_map(|fingerprint| IndexOp::Frequency { fingerprint }),
        4 => (proptest::collection::vec(0u8..8, 0..5), 0u64..400)
            .prop_map(|(fingerprints, at)| IndexOp::Observe { fingerprints, at }),
        1 => proptest::collection::vec(0u8..6, 0..3).prop_map(|spans| IndexOp::Evict { spans }),
        2 => (0u64..400).prop_map(|now| IndexOp::Clock { now }),
    ]
}

/// Generated sequences of up to `max` steps.
pub fn index_ops(max: usize) -> impl Strategy<Value = Vec<IndexOp>> {
    proptest::collection::vec(index_op(), 1..=max.max(1))
}

/// Run the harness on every configuration of [`configs`]: the index `make`
/// builds from a configuration and a clock must agree with
/// [`MemoryFingerprintIndex`]. Panics on the first disagreement.
pub fn check_fingerprint_index<S, F>(config: HarnessConfig, make: F)
where
    S: FingerprintIndex,
    F: Fn(IndexConfig, ManualClock) -> S,
{
    for index_config in configs() {
        run(
            "fingerprint index",
            config,
            index_ops(config.max_ops),
            |ops| {
                let clock = ManualClock::default();
                let sut = make(index_config.clone(), clock.clone());
                let model = MemoryFingerprintIndex::new(index_config.clone(), clock.clone());
                run_case(sut, model, clock, ops)
            },
        );
    }
}

fn sorted(
    hits: Result<Vec<FingerprintHit>, IndexError>,
) -> Result<Vec<FingerprintHit>, IndexError> {
    hits.map(|mut hits| {
        hits.sort_by_key(|hit| (hit.query_offset, hit.fingerprint, hit.span, hit.span_offset));
        hits
    })
}

async fn run_case<S: FingerprintIndex>(
    mut sut: S,
    mut model: MemoryFingerprintIndex,
    clock: ManualClock,
    ops: Vec<IndexOp>,
) -> Result<(), Mismatch> {
    for (step, op) in ops.iter().enumerate() {
        match op {
            IndexOp::Insert { span, fingerprints } => {
                let Some(span) = originated(*span) else {
                    return Err(format!("step {step}: span fixture"));
                };
                let fingerprints = positioned(fingerprints);
                let s = sut.insert(&span, &fingerprints).await;
                let m = model.insert(&span, &fingerprints).await;
                same(step, "insert", &s, &m)?;
            }
            IndexOp::Lookup { fingerprints } => {
                let fingerprints = positioned(fingerprints);
                let s = sorted(sut.lookup(&fingerprints).await);
                let m = sorted(model.lookup(&fingerprints).await);
                same(step, "lookup", &s, &m)?;
            }
            IndexOp::Frequency { fingerprint } => {
                let fingerprint = Fingerprint(u64::from(*fingerprint));
                let s = sut.frequency(fingerprint).await;
                let m = model.frequency(fingerprint).await;
                same(step, "frequency", &s, &m)?;
            }
            IndexOp::Observe { fingerprints, at } => {
                let fingerprints: Vec<Fingerprint> = fingerprints
                    .iter()
                    .map(|n| Fingerprint(u64::from(*n)))
                    .collect();
                let at = Timestamp::from_micros(*at);
                let s = sut.observe(&fingerprints, at).await;
                let m = model.observe(&fingerprints, at).await;
                same(step, "observe", &s, &m)?;
            }
            IndexOp::Evict { spans } => {
                let spans: Vec<SpanId> = spans.iter().map(|n| span_id(*n)).collect();
                let s = sut.evict(&spans).await;
                let m = model.evict(&spans).await;
                same(step, "evict", &s, &m)?;
            }
            IndexOp::Clock { now } => clock.set(Timestamp::from_micros(*now)),
        }
        observe(step, &sut, &model).await?;
    }
    Ok(())
}

/// Every fingerprint of the pool: its frequency, and a lookup of it at
/// offset 0 (when this node owns it).
async fn observe<S: FingerprintIndex>(
    step: usize,
    sut: &S,
    model: &MemoryFingerprintIndex,
) -> Result<(), Mismatch> {
    for n in 0..8u8 {
        let fingerprint = Fingerprint(u64::from(n));
        same(
            step,
            "frequency after the step",
            &sut.frequency(fingerprint).await,
            &model.frequency(fingerprint).await,
        )?;
        if model.config().owns(fingerprint) {
            let query = [PositionedFingerprint {
                fingerprint,
                offset: 0,
            }];
            same(
                step,
                "lookup after the step",
                &sorted(sut.lookup(&query).await),
                &sorted(model.lookup(&query).await),
            )?;
        }
    }
    Ok(())
}
