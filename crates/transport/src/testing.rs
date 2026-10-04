//! Fixtures shared by the unit, property and simulation tests: envelopes,
//! policies, configs, and proptest strategies.

use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::ids::{AgentId, AlertId, ChannelId, EventId, OperatorId};
use crosstalk_spec::interfaces::l2_transport::{
    BusError, ConsumerGroup, Delivery, RetryPolicy, Subscription,
};
use crosstalk_spec::observed::agent::AgentLabel;
use crosstalk_spec::paging::{DeadLetterList, PageRequest, PageSize};
use crosstalk_spec::support::{Timestamp, Watermark};
use proptest::prelude::*;

use crate::{BusConfig, DeliveryOrder, NonZeroDuration};

/// 2026-10-04T00:00:00Z.
pub(crate) const EPOCH_2026: u64 = 1_791_072_000_000_000;

/// The last timestamp with RFC 3339 text (`crosstalk_spec::wire::time::MAX`).
pub(crate) const MAX_TIMESTAMP_MICROS: u64 = 253_402_300_799_999_999;

pub(crate) fn group(name: &str) -> ConsumerGroup {
    ConsumerGroup(name.to_owned())
}

pub(crate) fn retry(max_attempts: u32, initial: Duration, max: Duration) -> RetryPolicy {
    let attempts = NonZeroU32::new(max_attempts).unwrap_or(NonZeroU32::MIN);
    RetryPolicy::new(attempts, initial, max).expect("test retry policy is valid")
}

/// 3 attempts, 10 ms to 80 ms.
pub(crate) fn default_retry() -> RetryPolicy {
    retry(3, Duration::from_millis(10), Duration::from_millis(80))
}

pub(crate) fn non_zero(duration: Duration) -> NonZeroDuration {
    NonZeroDuration::new(duration).expect("test duration is non-zero")
}

/// A small bus: capacity 64, a 1 s ack timeout, a 50 ms dead-letter retry.
pub(crate) fn config() -> BusConfig {
    BusConfig {
        group_capacity: NonZeroUsize::new(64).expect("non-zero"),
        command_buffer: NonZeroUsize::new(16).expect("non-zero"),
        ack_timeout: non_zero(Duration::from_secs(1)),
        dead_letter_retry: non_zero(Duration::from_millis(50)),
        order: DeliveryOrder::Fifo,
        retry: default_retry(),
    }
}

pub(crate) fn event_id(n: u128) -> EventId {
    EventId::from_ulid(n)
}

/// An agent change notification (subject `changed`) with id `n`.
pub(crate) fn changed(n: u128) -> Envelope {
    Envelope {
        id: event_id(n),
        at: Timestamp::from_micros(EPOCH_2026 + n as u64),
        event: BusEvent::Changed(Changed::Agent(AgentId::from_ulid(n))),
    }
}

/// A watermark advance (subject `watermark_advanced`) with id `n`.
pub(crate) fn watermark(n: u128) -> Envelope {
    Envelope {
        id: event_id(n),
        at: Timestamp::from_micros(EPOCH_2026 + n as u64),
        event: BusEvent::Insight(InsightEvent::WatermarkAdvanced(Watermark(
            Timestamp::from_micros(EPOCH_2026 + n as u64),
        ))),
    }
}

/// An agent rename (subject `agent_renamed`) whose label is `label`.
pub(crate) fn renamed(n: u128, label: AgentLabel) -> Envelope {
    Envelope {
        id: event_id(n),
        at: Timestamp::from_micros(EPOCH_2026 + n as u64),
        event: BusEvent::Ingest(IngestEvent::AgentRenamed {
            agent: AgentId::from_ulid(n),
            label: Some(label),
            by: OperatorId::from_ulid(n.wrapping_add(1)),
        }),
    }
}

pub(crate) fn page(size: u16) -> PageRequest<DeadLetterList> {
    PageRequest {
        size: PageSize::new(size).expect("valid page size"),
        after: None,
    }
}

/// The next delivery, failing the test on an error or a closed bus.
pub(crate) async fn next_ok<S: Subscription>(sub: &mut S) -> Delivery {
    match sub.next().await {
        Some(Ok(delivery)) => delivery,
        Some(Err(error)) => panic!("next failed: {error:?}"),
        None => panic!("bus closed"),
    }
}

/// The next delivery within `within`, or `None` if nothing arrives.
pub(crate) async fn next_within<S: Subscription>(
    sub: &mut S,
    within: Duration,
) -> Option<Result<Delivery, BusError>> {
    tokio::time::timeout(within, sub.next())
        .await
        .ok()
        .flatten()
}

/// Let the bus task and every other ready task run to quiescence without
/// moving the paused clock.
pub(crate) async fn settle() {
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
}

/// The subjects the strategies generate envelopes for.
pub(crate) const GENERATED_SUBJECTS: [Subject; 6] = [
    Subject::Changed,
    Subject::WatermarkAdvanced,
    Subject::TopicVersionReady,
    Subject::TopicVersionActivated,
    Subject::TopicVersionDropped,
    Subject::AgentRenamed,
];

fn arb_timestamp() -> impl Strategy<Value = Timestamp> {
    (0..=MAX_TIMESTAMP_MICROS).prop_map(Timestamp::from_micros)
}

fn arb_label() -> impl Strategy<Value = AgentLabel> {
    "[a-zA-Z0-9 ._:/-]{1,64}".prop_filter_map("a valid label", |text| AgentLabel::new(&text).ok())
}

fn arb_changed() -> impl Strategy<Value = Changed> {
    prop_oneof![
        any::<u128>().prop_map(|n| Changed::Alert(AlertId::from_ulid(n))),
        any::<u128>().prop_map(|n| Changed::Channel(ChannelId::from_ulid(n))),
        any::<u128>().prop_map(|n| Changed::Agent(AgentId::from_ulid(n))),
        arb_timestamp().prop_map(|at| Changed::Watermark(Watermark(at))),
        any::<u32>().prop_map(|v| Changed::TopicVersion(TopicModelVersion(v))),
    ]
}

/// Events of the subjects in [`GENERATED_SUBJECTS`], with generated
/// payloads, including free text (agent labels).
pub(crate) fn arb_event() -> impl Strategy<Value = BusEvent> {
    prop_oneof![
        arb_changed().prop_map(BusEvent::Changed),
        arb_timestamp()
            .prop_map(|at| BusEvent::Insight(InsightEvent::WatermarkAdvanced(Watermark(at)))),
        (any::<u32>(), any::<u64>()).prop_map(|(v, n)| BusEvent::Insight(
            InsightEvent::TopicVersionReady {
                version: TopicModelVersion(v),
                transmissions: n,
            }
        )),
        (any::<u32>(), any::<u32>()).prop_map(|(v, p)| BusEvent::Insight(
            InsightEvent::TopicVersionActivated {
                version: TopicModelVersion(v),
                previous: TopicModelVersion(p),
            }
        )),
        any::<u32>().prop_map(|v| BusEvent::Insight(InsightEvent::TopicVersionDropped {
            version: TopicModelVersion(v),
        })),
        (
            any::<u128>(),
            proptest::option::of(arb_label()),
            any::<u128>()
        )
            .prop_map(
                |(agent, label, by)| BusEvent::Ingest(IngestEvent::AgentRenamed {
                    agent: AgentId::from_ulid(agent),
                    label,
                    by: OperatorId::from_ulid(by),
                })
            ),
    ]
}

/// Envelopes with distinct ids.
pub(crate) fn arb_envelopes(max: usize) -> impl Strategy<Value = Vec<Envelope>> {
    proptest::collection::vec((arb_timestamp(), arb_event()), 1..=max).prop_map(|items| {
        items
            .into_iter()
            .enumerate()
            .map(|(index, (at, event))| Envelope {
                id: event_id(index as u128 + 1),
                at,
                event,
            })
            .collect()
    })
}

/// A paused, single-threaded runtime for a property case.
pub(crate) fn paused_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("test runtime builds")
}
