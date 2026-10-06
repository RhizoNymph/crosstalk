//! Integration tests of the outbox relay against Postgres
//! (`analysis.outbox.stable-envelope-id`): each staged event reaches the
//! sink under one envelope id, stamped once before its first publish,
//! across relays that stop after the stamp, after a publish and before the
//! delete, and concurrent relays; and nothing is relayed before the
//! transaction that staged it commits. Skipped when `TEST_DATABASE_URL` is
//! not configured.

use std::collections::{BTreeMap, BTreeSet};

use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::EventId;
use sqlx::PgPool;

use crate::pg::outbox::{self, Pending};
use crate::pg::testing::{RecordingSink, database};

/// `n` distinct events.
fn events(from: u32, n: u32) -> Vec<BusEvent> {
    (from..from + n)
        .map(|version| BusEvent::Changed(Changed::TopicVersion(TopicModelVersion(version))))
        .collect()
}

/// Stage `events` in one committed transaction.
async fn stage(pool: &PgPool, events: Vec<BusEvent>) -> Pending {
    let mut tx = pool
        .begin()
        .await
        .unwrap_or_else(|error| panic!("begin: {error}"));
    let pending = outbox::append(&mut tx, events)
        .await
        .unwrap_or_else(|error| panic!("append: {error}"));
    tx.commit()
        .await
        .unwrap_or_else(|error| panic!("commit: {error}"));
    pending
}

async fn outbox_rows(pool: &PgPool) -> Vec<(i64, Option<String>, Option<i64>)> {
    sqlx::query_as("SELECT seq, envelope_id, at FROM analysis.outbox ORDER BY seq")
        .fetch_all(pool)
        .await
        .unwrap_or_else(|error| panic!("reading the outbox: {error}"))
}

/// What a bus deduplicating on envelope id would hold: each id once, with
/// the first envelope published under it. Panics when one id was published
/// with two different envelopes.
fn log_of(published: &[Envelope]) -> BTreeMap<EventId, Envelope> {
    let mut log = BTreeMap::new();
    for envelope in published {
        let first = log.entry(envelope.id).or_insert_with(|| envelope.clone());
        assert_eq!(first, envelope, "one id published with two envelopes");
    }
    log
}

/// Every event in `log` appears under exactly one id, and the log holds
/// exactly `expected`.
fn assert_once_each(log: &BTreeMap<EventId, Envelope>, expected: &[BusEvent]) {
    let mut by_event: BTreeMap<String, Vec<EventId>> = BTreeMap::new();
    for envelope in log.values() {
        by_event
            .entry(format!("{:?}", envelope.event))
            .or_default()
            .push(envelope.id);
    }
    for (event, ids) in &by_event {
        assert_eq!(ids.len(), 1, "{event} published under {ids:?}");
    }
    let held: BTreeSet<String> = by_event.into_keys().collect();
    let wanted: BTreeSet<String> = expected.iter().map(|event| format!("{event:?}")).collect();
    assert_eq!(held, wanted);
}

#[tokio::test(flavor = "multi_thread")]
async fn outbox_relay_publishes_each_event_once_under_its_stamped_id() {
    let Some(db) = database("outbox_relay_publishes_each_event_once").await else {
        return;
    };
    let pool = db.pool();
    let staged = events(0, 3);
    let pending = stage(pool, staged.clone()).await;
    let sink = RecordingSink::new(11);

    // Stops after the stamp: the sink refuses, every row keeps its stamp.
    sink.refuse(true);
    outbox::deliver(pool, &sink, pending).await;
    assert_eq!(sink.published(), Vec::new());
    assert_eq!(sink.stamps(), 3);
    let stamped = outbox_rows(pool).await;
    assert_eq!(stamped.len(), 3);
    assert!(
        stamped
            .iter()
            .all(|(_, id, at)| id.is_some() && at.is_some()),
        "{stamped:?}"
    );

    // Stops after publishing and before deleting: the relay's stamp step
    // and every publish, then nothing.
    sink.refuse(false);
    let rows = outbox::stamp(pool, &sink, None)
        .await
        .unwrap_or_else(|error| panic!("stamp: {error}"));
    for (_, envelope) in rows {
        let published = crate::pg::EventSink::publish(&sink, envelope).await;
        assert_eq!(published, Ok(()));
    }
    assert_eq!(outbox_rows(pool).await, stamped);

    // The next relay republishes the same envelopes and clears the rows.
    let flushed = outbox::flush(pool, &sink).await;
    assert_eq!(flushed.ok(), Some(3));
    assert_eq!(outbox_rows(pool).await, Vec::new());
    assert_eq!(sink.stamps(), 3, "a stamped row was stamped again");
    let published = sink.published();
    assert_eq!(published.len(), 6);
    let log = log_of(&published);
    assert_once_each(&log, &staged);
    let stamped_ids: BTreeSet<String> =
        stamped.iter().filter_map(|(_, id, _)| id.clone()).collect();
    let logged_ids: BTreeSet<String> = log.keys().map(|id| id.ulid_text()).collect();
    assert_eq!(logged_ids, stamped_ids);
}

#[tokio::test(flavor = "multi_thread")]
async fn outbox_relay_publishes_nothing_before_the_staging_commit() {
    let Some(db) = database("outbox_relay_waits_for_the_commit").await else {
        return;
    };
    let pool = db.pool();
    let sink = RecordingSink::new(12);
    let mut open = pool
        .begin()
        .await
        .unwrap_or_else(|error| panic!("begin: {error}"));
    let _uncommitted = outbox::append(&mut open, events(0, 2))
        .await
        .unwrap_or_else(|error| panic!("append: {error}"));
    assert_eq!(outbox::flush(pool, &sink).await.ok(), Some(0));
    assert_eq!(sink.stamps(), 0);
    open.rollback()
        .await
        .unwrap_or_else(|error| panic!("rollback: {error}"));
    assert_eq!(outbox::flush(pool, &sink).await.ok(), Some(0));
    assert_eq!(sink.published(), Vec::new());

    let committed = events(10, 2);
    let pending = stage(pool, committed.clone()).await;
    outbox::deliver(pool, &sink, pending).await;
    assert_once_each(&log_of(&sink.published()), &committed);
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_relays_publish_each_event_under_one_id() {
    let Some(db) = database("concurrent_relays_publish_once").await else {
        return;
    };
    let pool = db.pool().clone();
    let staged = events(0, 600);
    for chunk in staged.chunks(100) {
        let _pending = stage(&pool, chunk.to_vec()).await;
    }
    let sink = RecordingSink::new(13);
    let (a, b) = tokio::join!(outbox::flush(&pool, &sink), outbox::flush(&pool, &sink));
    assert!(a.is_ok() && b.is_ok(), "{a:?} {b:?}");
    // Whatever a relay skipped while the other held it is left for a later
    // one.
    let rest = outbox::flush(&pool, &sink).await;
    assert!(rest.is_ok(), "{rest:?}");
    assert_eq!(outbox_rows(&pool).await, Vec::new());
    assert_eq!(sink.stamps(), 600);
    assert_once_each(&log_of(&sink.published()), &staged);
}
