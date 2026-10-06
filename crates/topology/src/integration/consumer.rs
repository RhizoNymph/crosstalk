//! The topology consumer over the Postgres store across a restart.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use crosstalk_memory::model::build::ts;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::EventId;
use crosstalk_spec::interfaces::l2_transport::BusError;

use crate::consumer::{EDGE_UPDATED, Outcome, handle};
use crate::outbox::Announce;
use crate::tests::buckets;
use crate::tests::support::{contribution, database, small_pool, world};

/// Records every envelope; fails every publish while `down`.
#[derive(Default)]
struct Recorder {
    down: AtomicBool,
    sent: Mutex<Vec<Envelope>>,
}

impl Announce for Recorder {
    async fn announce(&self, envelope: Envelope) -> Result<(), BusError> {
        if self.down.load(Ordering::SeqCst) {
            return Err(BusError::Disconnected);
        }
        self.sent.lock().expect("not poisoned").push(envelope);
        Ok(())
    }
}

/// The delivery of transmission `n`'s classification, under `id`.
fn delivery(id: u128, n: u64) -> Envelope {
    let one = contribution(n, 1, 2, Route::Unobserved, 5, 4);
    Envelope {
        id: EventId::from_ulid(id),
        at: ts(7),
        event: BusEvent::Insight(InsightEvent::TransmissionClassified {
            cause: one.cause,
            transmission: one.transmission,
            from: one.from,
            to: one.to,
            route: one.route,
            at: one.at,
            matched_bytes: one.matched_bytes,
            classification: one.classification,
        }),
    }
}

/// transport.consumer.derived-envelope-ids, topology.consumer.ack-after-publish:
/// a process applies a delivery, cannot publish `EdgeUpdated` and dies
/// before acking; a new process over the same database gets the delivery
/// again, counts it once and publishes the `EdgeUpdated` under the id
/// derived from the delivery. A further redelivery republishes the same
/// envelope.
#[tokio::test(flavor = "multi_thread")]
async fn pg_consumer_restart_republishes_the_same_edge_updated() {
    let Some(db) = database("pg_consumer_restart_republishes_the_same_edge_updated").await else {
        return;
    };
    let input = delivery(0x0000_0000_0001_0000_0000_0000_0000_0001, 1);

    let first_pool = small_pool(db.url().connect_options().clone(), 2).await;
    let mut first = world(first_pool.clone());
    let down = Recorder::default();
    down.down.store(true, Ordering::SeqCst);
    assert!(matches!(
        handle(&mut first.store, &down, &input).await,
        Outcome::Nack(_)
    ));
    drop(first);
    first_pool.close().await;
    let stored = buckets(db.pool()).await;
    assert_eq!(stored, vec![(0, 0, 1, 4)], "applied before the crash");

    let mut second = world(db.pool().clone());
    let up = Recorder::default();
    assert_eq!(handle(&mut second.store, &up, &input).await, Outcome::Ack);
    assert_eq!(handle(&mut second.store, &up, &input).await, Outcome::Ack);
    assert_eq!(buckets(db.pool()).await, stored, "counted once");

    let sent = up.sent.into_inner().expect("not poisoned");
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0], sent[1]);
    assert_eq!(sent[0].id, EventId::derive(input.id, EDGE_UPDATED, 0));
    assert_eq!(sent[0].at, input.at);
    assert!(matches!(
        sent[0].event,
        BusEvent::Insight(InsightEvent::EdgeUpdated(_))
    ));
    db.close().await.expect("drop the database");
}
