//! Model-based property harness for `TransmissionVerdicts`.
//!
//! [`check_transmission_verdicts`] generates random sequences of
//! [`VerdictOp`]s: transmissions put (and moved through every state, by
//! every route kind and match class), verdicts set, repeated and
//! withdrawn, logs read and quality tallied over random windows. It runs
//! each on the store under test and on [`MemoryVerdicts`] and, after every
//! step, requires equal results, equal published events (the store under
//! test must announce at least the reference's `Changed` notifications),
//! equal logs of every transmission id and an equal all-time quality tally.
//! It also checks that `set` never changes a stored transmission
//! (`flow.verdict.state-untouched`), through
//! `TransmissionStore::transmission`, and that every stored transmission
//! reads back as last saved.

use std::num::NonZeroU32;
use std::time::Duration;

use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::access::{Access, AccessOp, Extraction, WriteOutcome};
use crosstalk_spec::derived::flow::evidence::CoAccess;
use crosstalk_spec::derived::flow::transmission::{
    Classification, Confirmed, DelegationDirection, DirectCarrier, Route, Transmission,
    TransmissionState,
};
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::derived::provenance::matching::{Carrier, Codec, ContentMatch, MatchKind};
use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::ids::{
    AccessId, AgentId, ChannelId, ExchangeId, MessageHash, OperatorId, ResourceId, SpanId,
    TransmissionId,
};
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::interfaces::l5_flow::verdicts::TransmissionVerdicts;
use crosstalk_spec::observed::message::{PartRef, ToolCallId};
use crosstalk_spec::support::{Blake3, ByteRange, NonEmpty, Similarity, TimeWindow, Timestamp};
use proptest::prelude::*;

use super::MemoryVerdicts;
use crate::flow::registry::model::compare_events;
use crate::model::{Divergence, HarnessConfig, ModelMismatch, run, same};
use crate::support::{Outbox, drain};

/// Every spec trait a transmission store implements.
pub trait VerdictStore: TransmissionVerdicts + TransmissionStore {}

impl<T: TransmissionVerdicts + TransmissionStore> VerdictStore for T {}

/// How many transmission ids the harness draws from.
pub const TRANSMISSIONS: u8 = 5;

pub fn transmission_id(n: u8) -> TransmissionId {
    TransmissionId::from_ulid(0x7A00_0000 | u128::from(n % TRANSMISSIONS))
}

/// An id no transmission has.
pub fn unknown_transmission() -> TransmissionId {
    TransmissionId::from_ulid(0x7A00_FFFF)
}

fn reader() -> AgentId {
    AgentId::from_ulid(0x0A6E_0002)
}

fn sender() -> AgentId {
    AgentId::from_ulid(0x0A6E_0001)
}

fn access(id: u128, agent: AgentId, write: bool, at: u64) -> Access {
    let part = PartRef {
        message: MessageHash::from_digest(Blake3::from_bytes([3; 32])),
        index: 0,
    };
    Access {
        id: AccessId::from_ulid(id),
        agent,
        exchange: ExchangeId::from_ulid(id),
        resource: ResourceId::from_ulid(0x4E50),
        at: Timestamp::from_micros(at),
        via: Extraction::Structured,
        op: if write {
            AccessOp::Write {
                call: part,
                spans: Vec::new(),
                outcome: WriteOutcome::Delivered,
            }
        } else {
            AccessOp::Read { result: part }
        },
    }
}

/// A co-access: the sender wrote, the reader read 5µs later.
pub fn co_access() -> Option<CoAccess> {
    let write = access(0xACC1, sender(), true, 10);
    let read = access(0xACC2, reader(), false, 15);
    CoAccess::new(&write, &read, Duration::from_secs(60)).ok()
}

/// A content match of class `class` (0 exact, 1 normalized, 2 decoded,
/// 3 semantic).
pub fn content(class: u8) -> Option<ContentMatch> {
    let kind = match class % 4 {
        0 => MatchKind::Exact,
        1 => MatchKind::Normalized,
        2 => MatchKind::Decoded(NonEmpty::new(Codec::Base64)),
        _ => MatchKind::Semantic(Similarity::new(0.9).ok()?),
    };
    ContentMatch::new(
        SpanId::from_ulid(0x5DA0),
        sender(),
        reader(),
        ExchangeId::from_ulid(0xE1),
        SpanLocation {
            part: PartRef {
                message: MessageHash::from_digest(Blake3::from_bytes([4; 32])),
                index: 1,
            },
            range: ByteRange::new(0, 32).ok()?,
        },
        Carrier::ToolResult(ToolCallId("call-1".to_owned())),
        kind,
        NonZeroU32::new(16)?,
    )
    .ok()
}

fn confirmed(classes: &[u8]) -> Option<Confirmed> {
    let matches: Vec<ContentMatch> = classes.iter().filter_map(|c| content(*c)).collect();
    Confirmed::new(
        NonEmpty::from_vec(matches)?,
        co_access().into_iter().collect(),
        Timestamp::from_micros(20),
    )
    .ok()
}

/// The transmission state `n`: every variant, confirmed ones with matches
/// of the classes in `classes`.
pub fn state(n: u8, classes: &[u8]) -> Option<TransmissionState> {
    let co_access = co_access()?;
    let classification = || Classification {
        version: TopicModelVersion(1),
        topic: None,
        watched: false,
    };
    Some(match n % 7 {
        0 => TransmissionState::Detected,
        1 => TransmissionState::AwaitingContent {
            co_access,
            window_closes_at: Timestamp::from_micros(30),
        },
        2 => TransmissionState::Suspected {
            co_access: NonEmpty::new(co_access),
            since: Timestamp::from_micros(30),
        },
        3 => TransmissionState::Confirmed(confirmed(classes)?),
        4 => TransmissionState::Classified {
            confirmed: confirmed(classes)?,
            classification: classification(),
        },
        5 => TransmissionState::Aggregated {
            confirmed: confirmed(classes)?,
            classification: classification(),
        },
        _ => TransmissionState::Discarded {
            at: Timestamp::from_micros(40),
            co_access: NonEmpty::new(co_access),
        },
    })
}

/// The route `n`: one of each kind.
pub fn route(n: u8) -> Route {
    match n % 4 {
        0 => Route::Channel(ChannelId::from_ulid(0x0C4A)),
        1 => Route::Delegation(DelegationDirection::ParentToChild),
        2 => Route::Direct(DirectCarrier::UserTurn),
        _ => Route::Unobserved,
    }
}

/// One step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerdictOp {
    Put {
        transmission: u8,
        state: u8,
        classes: Vec<u8>,
        route: u8,
        opened_at: u64,
    },
    Set {
        transmission: u8,
        verdict: Option<bool>,
        operator: u8,
        at: u64,
        note: bool,
    },
    Log {
        transmission: u8,
    },
    Quality {
        start: u64,
        len: u64,
    },
}

/// One generated operation; `transmission` 5 is an unknown id.
pub fn verdict_op() -> impl Strategy<Value = VerdictOp> {
    prop_oneof![
        3 => (0u8..TRANSMISSIONS, 0u8..7, proptest::collection::vec(0u8..4, 1..3), 0u8..4, 0u64..60)
            .prop_map(|(transmission, state, classes, route, opened_at)| VerdictOp::Put {
                transmission, state, classes, route, opened_at,
            }),
        5 => (0u8..=TRANSMISSIONS, proptest::option::of(any::<bool>()), 0u8..2, 0u64..100, any::<bool>())
            .prop_map(|(transmission, verdict, operator, at, note)| VerdictOp::Set {
                transmission, verdict, operator, at, note,
            }),
        1 => (0u8..=TRANSMISSIONS).prop_map(|transmission| VerdictOp::Log { transmission }),
        1 => (0u64..60, 1u64..60).prop_map(|(start, len)| VerdictOp::Quality { start, len }),
    ]
}

/// Generated sequences of up to `max` steps.
pub fn verdict_ops(max: usize) -> impl Strategy<Value = Vec<VerdictOp>> {
    proptest::collection::vec(verdict_op(), 1..=max.max(1))
}

fn id(n: u8) -> TransmissionId {
    if n < TRANSMISSIONS {
        transmission_id(n)
    } else {
        unknown_transmission()
    }
}

/// Run the harness: the store `make` builds from its outbox must agree with
/// [`MemoryVerdicts`]. A failure is a [`ModelMismatch`] with the shrunk
/// sequence.
pub fn check_transmission_verdicts<S, F>(
    config: HarnessConfig,
    make: F,
) -> Result<(), ModelMismatch>
where
    S: VerdictStore,
    F: Fn(Outbox) -> S,
{
    run(config, verdict_ops(config.max_ops), |runtime, ops| {
        let (sut_outbox, sut_events) = Outbox::channel();
        let sut = make(sut_outbox);
        runtime.block_on(run_case(sut, sut_events, ops))
    })
}

async fn run_case<S: VerdictStore>(
    mut sut: S,
    mut sut_events: tokio::sync::mpsc::UnboundedReceiver<BusEvent>,
    ops: &[VerdictOp],
) -> Result<(), Divergence> {
    let (model_outbox, mut model_events) = Outbox::channel();
    let mut model = MemoryVerdicts::new(model_outbox);
    let all_time = TimeWindow::new(Timestamp::from_micros(0), Timestamp::from_micros(1_000))
        .map_err(|_| Divergence::new(0, "window"))?;
    for (step, op) in ops.iter().enumerate() {
        match op {
            VerdictOp::Put {
                transmission,
                state: n,
                classes,
                route: r,
                opened_at,
            } => {
                let state =
                    state(*n, classes).ok_or_else(|| Divergence::new(step, "state fixture"))?;
                let stored = Transmission {
                    id: transmission_id(*transmission),
                    to: reader(),
                    route: route(*r),
                    opened_at: Timestamp::from_micros(*opened_at),
                    state,
                };
                let s = sut.save(stored.clone()).await;
                let m = model.save(stored).await;
                same(step, "save", &s, &m)?;
            }
            VerdictOp::Set {
                transmission,
                verdict,
                operator,
                at,
                note,
            } => {
                let verdict = verdict.map(|genuine| {
                    if genuine {
                        Verdict::Genuine
                    } else {
                        Verdict::FalseDetection
                    }
                });
                let by = OperatorId::from_ulid(0x0B0B_0000 | u128::from(*operator));
                let at = Timestamp::from_micros(*at);
                let note = note.then(|| "checked".to_owned());
                let before = sut.transmission(id(*transmission)).await;
                let s = sut
                    .set(id(*transmission), verdict, by, at, note.clone())
                    .await;
                let m = model.set(id(*transmission), verdict, by, at, note).await;
                same(step, "set", &s, &m)?;
                same(
                    step,
                    "transmission after set",
                    &sut.transmission(id(*transmission)).await,
                    &before,
                )?;
            }
            VerdictOp::Log { transmission } => {
                same(
                    step,
                    "log",
                    &sut.log(id(*transmission)).await,
                    &model.log(id(*transmission)).await,
                )?;
            }
            VerdictOp::Quality { start, len } => {
                let window = TimeWindow::new(
                    Timestamp::from_micros(*start),
                    Timestamp::from_micros(start + len),
                )
                .map_err(|_| Divergence::new(step, "window"))?;
                same(
                    step,
                    "quality",
                    &sut.quality(window).await,
                    &model.quality(window).await,
                )?;
            }
        }
        compare_events(step, drain(&mut sut_events), drain(&mut model_events))?;
        for n in 0..=TRANSMISSIONS {
            same(
                step,
                "every log",
                &sut.log(id(n)).await,
                &model.log(id(n)).await,
            )?;
            same(
                step,
                "every transmission",
                &sut.transmission(id(n)).await,
                &model.transmission(id(n)).await,
            )?;
        }
        same(
            step,
            "all-time quality",
            &sut.quality(all_time).await,
            &model.quality(all_time).await,
        )?;
    }
    Ok(())
}
