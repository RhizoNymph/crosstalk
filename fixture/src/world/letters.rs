//! The dead letters in the world's past: four deliveries that ran out of
//! retries, one per consumer group (`analyze`, `topology`, `alerts`,
//! `flow`), each with its attempts and last error.

use std::num::{NonZeroU32, NonZeroU64};

use crosstalk_spec::aggregates::edge::{EdgeKey, TopicSlot};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::EventId;
use crosstalk_spec::interfaces::l2_transport::{ConsumerGroup, DeadLetter};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use crate::clock::{HOUR, minus, plus};
use crate::store::State;

use super::channels::{ChannelKey, PASTEBIN_DECIDED_AT};
use super::{GenError, World};

/// Generates the dead letters into the store.
pub fn populate(world: &World, state: &mut State) -> Result<(), GenError> {
    let mut letters = Vec::new();
    let envelope = |state: &mut State, at: Timestamp, event: BusEvent| Envelope {
        id: EventId::from_ulid(state.mint.ulid(at)),
        at,
        event,
    };
    if let Some(record) = world.transmissions.iter().rev().find(|t| t.is_confirmed())
        && let (Some(from), Some(bytes)) = (record.from, NonZeroU64::new(record.matched_bytes))
    {
        let at = record.transmission.opened_at;
        letters.push(DeadLetter {
            group: ConsumerGroup("analyze".to_owned()),
            envelope: envelope(
                state,
                at,
                BusEvent::Detect(DetectEvent::TransmissionConfirmed {
                    transmission: record.transmission.id,
                    from,
                    to: record.transmission.to,
                    route: record.transmission.route.clone(),
                    at,
                    matched_bytes: bytes,
                }),
            ),
            attempts: NonZeroU32::new(5).unwrap_or(NonZeroU32::MIN),
            last_error: "embedder: request timed out after 30s".to_owned(),
        });
        let bucket_start = minus(at, at.as_micros() % HOUR);
        let bucket = TimeWindow::new(bucket_start, plus(bucket_start, HOUR))
            .map_err(|e| GenError::invalid("TimeWindow", e))?;
        if let Ok(key) = EdgeKey::new(
            from,
            record.transmission.to,
            record.transmission.route.clone(),
            TopicSlot {
                version: TopicModelVersion(2),
                topic: record.topic(TopicModelVersion(2)),
            },
            bucket,
        ) {
            letters.push(DeadLetter {
                group: ConsumerGroup("topology".to_owned()),
                envelope: envelope(state, at, BusEvent::Insight(InsightEvent::EdgeUpdated(key))),
                attempts: NonZeroU32::new(3).unwrap_or(NonZeroU32::MIN),
                last_error: "edge store: deadlock detected, transaction rolled back".to_owned(),
            });
        }
    }
    let pastebin = world
        .scenario
        .channel(ChannelKey::Pastebin)
        .ok_or_else(|| GenError::Missing("pastebin".to_owned()))?;
    if let Some(policy) = state
        .channels
        .get(&pastebin)
        .map(|r| r.channel().policy.clone())
    {
        let at = PASTEBIN_DECIDED_AT;
        letters.push(DeadLetter {
            group: ConsumerGroup("alerts".to_owned()),
            envelope: envelope(
                state,
                at,
                BusEvent::Insight(InsightEvent::PolicyChanged {
                    channel: pastebin,
                    policy,
                }),
            ),
            attempts: NonZeroU32::new(5).unwrap_or(NonZeroU32::MIN),
            last_error: "sink soc-webhook rejected the delivery: HTTP 503".to_owned(),
        });
    }
    let recorded = world.accesses.iter().rev().nth(3).and_then(|access| {
        let channel = *world.resource_channel.get(&access.resource)?;
        Some((access, channel))
    });
    if let Some((access, channel)) = recorded {
        letters.push(DeadLetter {
            group: ConsumerGroup("flow".to_owned()),
            envelope: envelope(
                state,
                access.at,
                BusEvent::Detect(DetectEvent::AccessRecorded {
                    access: access.clone(),
                    channel: Some(channel),
                }),
            ),
            attempts: NonZeroU32::new(4).unwrap_or(NonZeroU32::MIN),
            last_error: "resource extractor: unparseable bash command".to_owned(),
        });
    }
    letters.sort_by_key(|l| (l.envelope.at, l.envelope.id));
    state.dead_letters = letters;
    Ok(())
}
