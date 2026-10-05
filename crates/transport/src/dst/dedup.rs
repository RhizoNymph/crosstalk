//! The dedup wrapper's scenarios; the evidence functions in the parent
//! module run them.

use std::time::Duration;

use crosstalk_spec::events::Subject;
use crosstalk_spec::interfaces::l2_transport::{
    BusError, DeadLetterStore, DeliveryId, EventBus, Subscription,
};

use super::faults::{depth, letters_of};
use super::{LONG, SECOND};
use crate::testing::{changed, config, default_retry, group, next_ok, next_within, page};
use crate::{Dedup, MpscBus};

pub(super) async fn dedup_hands_each_id_once_per_group() {
    let bus = MpscBus::start(config()).expect("bus starts");
    let flow = group("flow");
    let mut consumers = Vec::new();
    for _ in 0..2 {
        let sub = bus
            .subscribe(&[Subject::Changed], flow.clone(), default_retry())
            .await
            .expect("subscribe");
        consumers.push(Dedup::new(sub, flow.clone(), bus.handled_ids()));
    }

    // Republished three times; handled once.
    for _ in 0..3 {
        bus.publish(changed(1)).await.expect("publish");
    }
    let handled = next_ok(&mut consumers[0]).await;
    consumers[0].ack(handled.id).await.expect("ack");
    for consumer in &mut consumers {
        assert!(next_within(consumer, LONG).await.is_none());
    }

    // A lost ack: handled, but acked after the timeout. The record is
    // written, so the redelivery is withheld, from either consumer.
    bus.publish(changed(2)).await.expect("publish");
    let slow = next_ok(&mut consumers[1]).await;
    tokio::time::sleep(2 * SECOND).await;
    assert_eq!(
        consumers[1].ack(slow.id).await,
        Err(BusError::UnknownDelivery(slow.id))
    );
    for consumer in &mut consumers {
        assert!(next_within(consumer, LONG).await.is_none());
    }
    assert_eq!(depth(&bus, &flow).await.tracked(), 0);
    let letters = letters_of(
        bus.dead_letters()
            .list(None, &page(10))
            .await
            .expect("lists"),
    );
    assert!(letters.is_empty());

    // A consumer restart keeps the record: a fresh consumer of the group
    // withholds both handled ids.
    drop(consumers);
    let sub = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    let mut restarted = Dedup::new(sub, flow.clone(), bus.handled_ids());
    bus.publish(changed(1)).await.expect("publish");
    bus.publish(changed(2)).await.expect("publish");
    assert!(next_within(&mut restarted, LONG).await.is_none());
    assert_eq!(depth(&bus, &flow).await.tracked(), 0);
}

pub(super) async fn dedup_acks_suppressed_duplicates() {
    let bus = MpscBus::start(config()).expect("bus starts");
    let flow = group("flow");
    let sub = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    let mut consumer = Dedup::new(sub, flow.clone(), bus.handled_ids());
    bus.publish(changed(1)).await.expect("publish");
    bus.publish(changed(1)).await.expect("publish");
    let first = next_ok(&mut consumer).await;
    consumer.ack(first.id).await.expect("ack");
    assert_eq!(depth(&bus, &flow).await.tracked(), 1, "the duplicate waits");

    // Asking for the next delivery withholds and acks the duplicate.
    assert!(
        next_within(&mut consumer, Duration::from_millis(1))
            .await
            .is_none()
    );
    assert_eq!(
        depth(&bus, &flow).await.tracked(),
        0,
        "the duplicate was acked"
    );
    assert!(next_within(&mut consumer, LONG).await.is_none());
    let letters = letters_of(
        bus.dead_letters()
            .list(None, &page(10))
            .await
            .expect("lists"),
    );
    assert!(letters.is_empty());
}

pub(super) async fn dedup_never_suppresses_unhandled_id() {
    let bus = MpscBus::start(config()).expect("bus starts");
    let (flow, analysis) = (group("flow"), group("analysis"));
    let flow_sub = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    let analysis_sub = bus
        .subscribe(&[Subject::Changed], analysis.clone(), default_retry())
        .await
        .expect("subscribe");
    let handled = bus.handled_ids();
    let mut flow_consumer = Dedup::new(flow_sub, flow.clone(), handled.clone());
    let mut analysis_consumer = Dedup::new(analysis_sub, analysis.clone(), handled);

    // Published twice; the first handling fails.
    bus.publish(changed(1)).await.expect("publish");
    bus.publish(changed(1)).await.expect("publish");
    let mut seen: Vec<DeliveryId> = Vec::new();
    let first = next_ok(&mut flow_consumer).await;
    flow_consumer
        .nack(first.id, Duration::ZERO, "failed".into())
        .await
        .expect("nack");
    seen.push(first.id);
    // Both the duplicate and the redelivery reach consumer logic until one
    // is handled.
    let second = next_ok(&mut flow_consumer).await;
    seen.push(second.id);
    flow_consumer
        .nack(second.id, Duration::ZERO, "failed".into())
        .await
        .expect("nack");
    let third = next_ok(&mut flow_consumer).await;
    seen.push(third.id);
    flow_consumer.ack(third.id).await.expect("ack");
    assert_eq!(seen.len(), 3);
    assert!(next_within(&mut flow_consumer, LONG).await.is_none());

    // The flow group's handling withholds nothing from analysis.
    let mut analysis_seen = 0;
    while let Some(Ok(delivery)) = next_within(&mut analysis_consumer, SECOND).await {
        analysis_seen += 1;
        analysis_consumer.ack(delivery.id).await.expect("ack");
    }
    assert_eq!(
        analysis_seen, 1,
        "the second copy is a duplicate within analysis"
    );
}
