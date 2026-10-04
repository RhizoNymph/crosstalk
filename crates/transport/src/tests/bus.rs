//! `MpscBus` behaviour on hand-picked inputs: groups, decoding, shutdown.

use std::collections::HashSet;
use std::time::Duration;

use crosstalk_spec::events::Subject;
use crosstalk_spec::interfaces::l2_transport::{BusError, DeadLetterStore, EventBus, Subscription};

use crosstalk_spec::ids::EventId;
use crosstalk_spec::interfaces::l2_transport::ConsumerGroup;

use crate::testing::{
    changed, config, default_retry, group, next_ok, next_within, page, watermark,
};
use crate::{Dedup, HandledIds, MemoryHandledIds, MpscBus, StartError};

const SECOND: Duration = Duration::from_secs(1);

#[test]
fn start_outside_a_runtime_is_an_error() {
    assert_eq!(MpscBus::start(config()).err(), Some(StartError::NoRuntime));
}

#[tokio::test(start_paused = true)]
async fn each_group_gets_every_envelope_and_members_share_them() {
    let bus = MpscBus::start(config()).expect("bus starts");
    let (flow, analysis) = (group("flow"), group("analysis"));
    let mut flow_a = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    let mut flow_b = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    let mut analysis_sub = bus
        .subscribe(&[Subject::Changed], analysis, default_retry())
        .await
        .expect("subscribe");
    for n in 1..=10 {
        bus.publish(changed(n)).await.expect("publish");
    }

    // The two flow subscriptions alternate taking deliveries: together they
    // see each envelope once.
    let mut flow_seen = Vec::new();
    for round in 0..10 {
        let sub = if round % 2 == 0 {
            &mut flow_a
        } else {
            &mut flow_b
        };
        let delivery = next_ok(sub).await;
        flow_seen.push(delivery.envelope.id.as_ulid());
        sub.ack(delivery.id).await.expect("ack");
    }
    let mut analysis_seen = Vec::new();
    for _ in 0..10 {
        let delivery = next_ok(&mut analysis_sub).await;
        analysis_seen.push(delivery.envelope.id.as_ulid());
        analysis_sub.ack(delivery.id).await.expect("ack");
    }
    let every: HashSet<u128> = (1..=10).collect();
    assert_eq!(flow_seen.iter().copied().collect::<HashSet<_>>(), every);
    assert_eq!(flow_seen.len(), 10);
    assert_eq!(analysis_seen.iter().copied().collect::<HashSet<_>>(), every);
    assert_eq!(analysis_seen.len(), 10);
    assert!(next_within(&mut flow_a, 10 * SECOND).await.is_none());
    assert!(next_within(&mut flow_b, 10 * SECOND).await.is_none());
}

#[tokio::test(start_paused = true)]
async fn a_group_holds_envelopes_while_no_consumer_is_connected() {
    let bus = MpscBus::start(config()).expect("bus starts");
    let flow = group("flow");
    let first = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    drop(first);
    bus.publish(changed(1)).await.expect("publish");
    let mut again = bus
        .subscribe(&[Subject::Changed], flow, default_retry())
        .await
        .expect("subscribe");
    assert_eq!(next_ok(&mut again).await.envelope, changed(1));
}

#[tokio::test(start_paused = true)]
async fn a_publish_no_group_subscribed_to_is_owed_to_none() {
    let bus = MpscBus::start(config()).expect("bus starts");
    bus.publish(changed(1)).await.expect("publish");
    let mut late = bus
        .subscribe(&[Subject::Changed], group("late"), default_retry())
        .await
        .expect("subscribe");
    assert!(
        next_within(&mut late, 10 * SECOND).await.is_none(),
        "no backfill"
    );
}

/// `transport.codec.undecodable-not-redelivered`, on the in-process bus:
/// bytes that do not decode reach the group once as `Decode`, are neither
/// redelivered nor dead-lettered, and other groups get their own report.
#[tokio::test(start_paused = true)]
async fn an_undecodable_message_is_reported_once_per_group() {
    let bus = MpscBus::start(config()).expect("bus starts");
    let (flow, analysis) = (group("flow"), group("analysis"));
    let mut flow_sub = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    let mut analysis_sub = bus
        .subscribe(&[Subject::Changed], analysis.clone(), default_retry())
        .await
        .expect("subscribe");
    // A newer node's event: a field this node does not know.
    let mut json = serde_json::to_value(changed(1)).expect("encodes");
    json["event"]["data"]["extra"] = serde_json::Value::Bool(true);
    let bytes = serde_json::to_vec(&json).expect("encodes");
    bus.publish_encoded(Subject::Changed, bytes)
        .await
        .expect("publish");
    bus.publish(changed(2)).await.expect("publish");

    for sub in [&mut flow_sub, &mut analysis_sub] {
        assert!(matches!(
            sub.next().await,
            Some(Err(BusError::Decode { .. }))
        ));
        let next = next_ok(sub).await;
        assert_eq!(next.envelope, changed(2));
        sub.ack(next.id).await.expect("ack");
        assert!(
            next_within(sub, 60 * SECOND).await.is_none(),
            "not redelivered"
        );
    }
    for g in [&flow, &analysis] {
        let depth = bus.depth(g).await.expect("depth").expect("group");
        assert_eq!(depth.tracked(), 0);
    }
    let letters = bus
        .dead_letters()
        .list(None, &page(10))
        .await
        .expect("lists");
    assert!(
        letters.items().is_empty(),
        "an undecodable message has no envelope to dead-letter"
    );
}

/// A foreign publisher's envelope routed under a subject other than its
/// event's is refused at decode, never handed to a subscription that did
/// not ask for that subject.
#[tokio::test(start_paused = true)]
async fn an_envelope_under_the_wrong_subject_does_not_decode() {
    let bus = MpscBus::start(config()).expect("bus starts");
    let mut sub = bus
        .subscribe(&[Subject::Changed], group("flow"), default_retry())
        .await
        .expect("subscribe");
    let bytes = serde_json::to_vec(&watermark(1)).expect("encodes");
    bus.publish_encoded(Subject::Changed, bytes)
        .await
        .expect("publish");
    assert!(matches!(
        sub.next().await,
        Some(Err(BusError::Decode { .. }))
    ));
}

#[tokio::test(start_paused = true)]
async fn shutdown_ends_subscriptions() {
    let bus = MpscBus::start(config()).expect("bus starts");
    let mut sub = bus
        .subscribe(&[Subject::Changed], group("flow"), default_retry())
        .await
        .expect("subscribe");
    bus.publish(changed(1)).await.expect("publish");
    let delivery = next_ok(&mut sub).await;
    bus.shutdown().await;
    assert!(sub.next().await.is_none());
    assert_eq!(sub.ack(delivery.id).await, Err(BusError::Disconnected));
    assert_eq!(bus.publish(changed(2)).await, Err(BusError::Disconnected));
}

#[tokio::test(start_paused = true)]
async fn dropping_every_bus_handle_ends_subscriptions() {
    let bus = MpscBus::start(config()).expect("bus starts");
    let mut sub = bus
        .subscribe(&[Subject::Changed], group("flow"), default_retry())
        .await
        .expect("subscribe");
    drop(bus);
    assert!(sub.next().await.is_none());
}

/// A cancelled `next` loses nothing: the delivery it was granted comes
/// out of the following call.
#[tokio::test(start_paused = true)]
async fn next_is_cancel_safe() {
    let bus = MpscBus::start(config()).expect("bus starts");
    let flow = group("flow");
    let mut sub = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    // Cancelled while waiting: nothing is published yet.
    assert!(next_within(&mut sub, SECOND).await.is_none());
    bus.publish(changed(1)).await.expect("publish");
    let delivery = next_ok(&mut sub).await;
    assert_eq!(delivery.attempt.get(), 1);
    assert_eq!(delivery.envelope, changed(1));
}

/// A `next` dropped after the bus granted it a delivery loses nothing: the
/// following call returns that delivery, on its first attempt, without
/// waiting for an ack timeout.
#[tokio::test(start_paused = true)]
async fn a_dropped_next_loses_no_delivery() {
    let bus = MpscBus::start(config()).expect("bus starts");
    let flow = group("flow");
    let mut sub = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    bus.publish(changed(1)).await.expect("publish");
    // Polled once (the request reaches the bus), then dropped.
    let dropped = tokio::time::timeout(Duration::ZERO, sub.next()).await;
    assert!(dropped.is_err(), "the first poll does not complete");
    crate::testing::settle().await;
    let depth = bus.depth(&flow).await.expect("depth").expect("group");
    assert_eq!(depth.held, 1, "granted to the dropped call");

    let started = tokio::time::Instant::now();
    let delivery = next_ok(&mut sub).await;
    assert_eq!((delivery.attempt.get(), delivery.envelope), (1, changed(1)));
    assert_eq!(started.elapsed(), Duration::ZERO);
}

/// A handled-id record that takes 10 ms to answer `contains`.
#[derive(Clone)]
struct SlowRecord(MemoryHandledIds);

impl HandledIds for SlowRecord {
    async fn contains(&self, group: &ConsumerGroup, id: EventId) -> Result<bool, BusError> {
        tokio::time::sleep(Duration::from_millis(10)).await;
        self.0.contains(group, id).await
    }

    async fn record(&self, group: &ConsumerGroup, id: EventId) -> Result<(), BusError> {
        self.0.record(group, id).await
    }
}

/// `Dedup::next` dropped while it checks the record resumes with the same
/// delivery.
#[tokio::test(start_paused = true)]
async fn a_dropped_dedup_next_loses_no_delivery() {
    let bus = MpscBus::start(config()).expect("bus starts");
    let flow = group("flow");
    let sub = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    let mut consumer = Dedup::new(sub, flow, SlowRecord(bus.handled_ids()));
    bus.publish(changed(1)).await.expect("publish");
    let dropped = tokio::time::timeout(Duration::from_millis(5), consumer.next()).await;
    assert!(dropped.is_err(), "dropped while checking the record");

    let started = tokio::time::Instant::now();
    let delivery = next_ok(&mut consumer).await;
    assert_eq!((delivery.attempt.get(), delivery.envelope), (1, changed(1)));
    assert!(started.elapsed() < SECOND, "no ack timeout was needed");
}
