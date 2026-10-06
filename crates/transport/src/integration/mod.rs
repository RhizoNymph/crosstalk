//! Integration tests against a real Postgres (`TEST_DATABASE_URL`; each
//! test passes with a skip line without it): what only `PgBus` promises
//! (durability, idempotent publish, restart recovery, retention, stats,
//! admission bounds), and the publish spool over `PgBus` through a
//! cuttable [`DbLink`].
//!
//! A "process" here is a pool and a bus over it. A crash drops both
//! without any shutdown step (a subscription holding deliveries is
//! abandoned rather than dropped, so nothing is handed back), and a restart
//! builds a fresh pool and bus over the same database.
//!
//! The functions named by transport invariants' `integration` evidence
//! live here, at `crosstalk_transport::integration::<name>`.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use crosstalk_spec::events::{Envelope, Subject};
use crosstalk_spec::interfaces::l2_transport::{BusError, DeadLetterStore, EventBus, Subscription};
use crosstalk_spec::paging::{DeadLetterList, PageRequest, PageSize};
use crosstalk_spec::support::{SystemClock, Timestamp};
use crosstalk_store::TestDb;
use crosstalk_testkit::db_link::DbLink;
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

use crate::spool::tests::small_config;
use crate::testing::{EPOCH_2026, changed, default_retry, group, non_zero, retry, watermark};
use crate::{DrainTarget, PgBus, PgBusConfig, SpoolState, SpoolingBus};

const SECOND: Duration = Duration::from_secs(1);

/// A migrated test database, or `None` (a skip).
async fn database(test: &str) -> Option<TestDb> {
    let db = TestDb::new_or_skip(test).await.expect("test database")?;
    crate::pg::migrate(db.pool())
        .await
        .expect("transport migrates");
    Some(db)
}

/// A process's own small pool on the test database.
async fn pool(options: PgConnectOptions) -> PgPool {
    PgPoolOptions::new()
        .max_connections(3)
        .acquire_timeout(5 * SECOND)
        .connect_with(options)
        .await
        .expect("a pool on the test database")
}

fn config() -> PgBusConfig {
    PgBusConfig {
        poll: non_zero(Duration::from_millis(50)),
        ..PgBusConfig::default()
    }
}

/// A process: a pool and a started, recovered bus.
async fn process(db: &TestDb, config: PgBusConfig) -> PgBus {
    let pool = pool(db.url().connect_options().clone()).await;
    let bus = PgBus::new(pool, Arc::new(SystemClock), config).expect("bus starts");
    bus.recover_held().await.expect("recovers");
    bus
}

async fn next_soon<S: Subscription>(
    sub: &mut S,
) -> crosstalk_spec::interfaces::l2_transport::Delivery {
    match tokio::time::timeout(20 * SECOND, sub.next()).await {
        Ok(Some(Ok(delivery))) => delivery,
        other => panic!("expected a delivery, got {other:?}"),
    }
}

async fn nothing_within<S: Subscription>(sub: &mut S, within: Duration) -> bool {
    tokio::time::timeout(within, sub.next()).await.is_err()
}

/// The log's ids, in seq order.
async fn log_ids(pool: &PgPool) -> Vec<String> {
    sqlx::query_scalar("SELECT id FROM transport.events ORDER BY seq")
        .fetch_all(pool)
        .await
        .expect("reads the log")
}

fn ids(envelopes: &[Envelope]) -> Vec<String> {
    envelopes.iter().map(|e| e.id.ulid_text()).collect()
}

fn page(size: u16) -> PageRequest<DeadLetterList> {
    PageRequest {
        size: PageSize::new(size).expect("valid size"),
        after: None,
    }
}

/// `transport.durability.pg-publish-persisted`: an envelope whose publish
/// returned `Ok` reaches a group subscribed before it, after the
/// publishing process dies without any shutdown.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_publish_survives_restart() {
    let Some(db) = database("pg_publish_survives_restart").await else {
        return;
    };
    let flow = group("flow");
    {
        let bus = process(&db, config()).await;
        let sub = bus
            .subscribe(&[Subject::Changed], flow.clone(), default_retry())
            .await
            .expect("subscribe");
        drop(sub);
        for n in 1..=3 {
            bus.publish(changed(n)).await.expect("publish");
        }
        // Killed: bus and pool dropped, nothing flushed or shut down.
    }
    let bus = process(&db, config()).await;
    let mut sub = bus
        .subscribe(&[Subject::Changed], flow, default_retry())
        .await
        .expect("subscribe");
    let mut got = Vec::new();
    for _ in 0..3 {
        let delivery = next_soon(&mut sub).await;
        assert_eq!(delivery.attempt.get(), 1);
        got.push(delivery.envelope.clone());
        sub.ack(delivery.id).await.expect("ack");
    }
    assert_eq!(got, vec![changed(1), changed(2), changed(3)]);
    assert!(nothing_within(&mut sub, 500 * Duration::from_millis(1)).await);
    drop(sub);
    bus.shutdown();
    db.close().await.expect("drop the test database");
}

/// `transport.publish.idempotent-on-id`: publishing an id the log holds,
/// alone or in a batch, returns `Ok`, adds no log entry and delivers
/// nothing new.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_publish_is_idempotent_on_id() {
    let Some(db) = database("pg_publish_is_idempotent_on_id").await else {
        return;
    };
    let bus = process(&db, config()).await;
    let mut sub = bus
        .subscribe(&[Subject::Changed], group("flow"), default_retry())
        .await
        .expect("subscribe");
    bus.publish(changed(1)).await.expect("publish");
    bus.publish(changed(1)).await.expect("a republish is Ok");
    bus.publish_batch(vec![changed(1), changed(2), changed(2)])
        .await
        .expect("a batch with known ids is Ok");
    // A republish with the same id and other content changes nothing.
    let mut altered = changed(2);
    altered.at = Timestamp::from_micros(EPOCH_2026 + 999);
    bus.publish(altered).await.expect("Ok");
    assert_eq!(log_ids(db.pool()).await, ids(&[changed(1), changed(2)]));
    let mut got = Vec::new();
    for _ in 0..2 {
        let delivery = next_soon(&mut sub).await;
        got.push(delivery.envelope.clone());
        sub.ack(delivery.id).await.expect("ack");
    }
    assert_eq!(got, vec![changed(1), changed(2)]);
    assert!(nothing_within(&mut sub, Duration::from_millis(500)).await);
    drop(sub);
    bus.shutdown();
    db.close().await.expect("drop the test database");
}

/// `transport.restart.held-redelivered`: a delivery held by a process
/// that died is redelivered by the next one with the attempt counted; on
/// its group's last attempt it is dead-lettered instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_held_delivery_redelivered_after_restart() {
    let Some(db) = database("pg_held_delivery_redelivered_after_restart").await else {
        return;
    };
    let (flow, once) = (group("flow"), group("once"));
    let one_attempt = retry(1, Duration::from_millis(10), Duration::from_millis(80));
    {
        let bus = process(&db, config()).await;
        let mut flow_sub = bus
            .subscribe(&[Subject::Changed], flow.clone(), default_retry())
            .await
            .expect("subscribe");
        let mut once_sub = bus
            .subscribe(&[Subject::Changed], once.clone(), one_attempt)
            .await
            .expect("subscribe");
        bus.publish(changed(1)).await.expect("publish");
        assert_eq!(next_soon(&mut flow_sub).await.attempt.get(), 1);
        assert_eq!(next_soon(&mut once_sub).await.attempt.get(), 1);
        // Killed while holding both.
        flow_sub.abandon();
        once_sub.abandon();
    }
    let held: i64 =
        sqlx::query_scalar("SELECT count(*) FROM transport.deliveries WHERE state = 'held'")
            .fetch_one(db.pool())
            .await
            .expect("counts");
    assert_eq!(held, 2, "the dead process left both held");

    let pool = pool(db.url().connect_options().clone()).await;
    let bus = PgBus::new(pool, Arc::new(SystemClock), config()).expect("bus starts");
    let recovered = bus.recover_held().await.expect("recovers");
    assert_eq!((recovered.redelivered, recovered.dead_lettered), (1, 1));
    let mut flow_sub = bus
        .subscribe(&[Subject::Changed], flow, default_retry())
        .await
        .expect("subscribe");
    let again = next_soon(&mut flow_sub).await;
    assert_eq!((again.envelope, again.attempt.get()), (changed(1), 2));
    flow_sub.ack(again.id).await.expect("ack");
    let letters = bus
        .dead_letters()
        .list(Some(&once), &page(10))
        .await
        .expect("lists");
    let (items, _) = letters.into_parts();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].envelope, changed(1));
    assert_eq!(items[0].attempts.get(), 1);
    assert_eq!(
        items[0].last_error,
        "process restarted while holding the delivery"
    );
    drop(flow_sub);
    bus.shutdown();
    db.close().await.expect("drop the test database");
}

/// Dead letters survive a restart, and a replay in the next process
/// delivers the letter at attempt 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_dead_letters_persist_and_replay() {
    let Some(db) = database("pg_dead_letters_persist_and_replay").await else {
        return;
    };
    let flow = group("flow");
    {
        let bus = process(&db, config()).await;
        let mut sub = bus
            .subscribe(&[Subject::Changed], flow.clone(), default_retry())
            .await
            .expect("subscribe");
        bus.publish(changed(1)).await.expect("publish");
        for _ in 0..3 {
            let delivery = next_soon(&mut sub).await;
            sub.nack(delivery.id, Duration::ZERO, "constraint violated".into())
                .await
                .expect("nack");
        }
    }
    let bus = process(&db, config()).await;
    let letters = bus.dead_letters();
    let listed = letters.list(None, &page(10)).await.expect("lists");
    assert_eq!(listed.items().len(), 1);
    assert_eq!(listed.items()[0].last_error, "constraint violated");
    assert_eq!(listed.items()[0].attempts.get(), 3);
    let mut sub = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    letters.replay(&flow, changed(1).id).await.expect("replays");
    let replayed = next_soon(&mut sub).await;
    assert_eq!((replayed.envelope, replayed.attempt.get()), (changed(1), 1));
    sub.ack(replayed.id).await.expect("ack");
    assert!(
        letters
            .list(None, &page(10))
            .await
            .expect("lists")
            .items()
            .is_empty()
    );
    drop(sub);
    bus.shutdown();
    db.close().await.expect("drop the test database");
}

/// `PgBus::prune` deletes only entries every group is done with and older
/// than the retention: never an unacked one, nor one a dead letter names.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_prune_never_drops_an_unacked_event() {
    let Some(db) = database("pg_prune_never_drops_an_unacked_event").await else {
        return;
    };
    let bus = process(&db, config()).await;
    let (fast, slow) = (group("fast"), group("slow"));
    let mut fast_sub = bus
        .subscribe(
            &[Subject::Changed, Subject::WatermarkAdvanced],
            fast,
            default_retry(),
        )
        .await
        .expect("subscribe");
    let mut slow_sub = bus
        .subscribe(
            &[Subject::Changed],
            slow,
            retry(1, Duration::from_millis(10), Duration::from_millis(80)),
        )
        .await
        .expect("subscribe");
    for n in 1..=5 {
        bus.publish(changed(n)).await.expect("publish");
    }
    bus.publish(watermark(6)).await.expect("publish");
    for _ in 0..6 {
        let delivery = next_soon(&mut fast_sub).await;
        fast_sub.ack(delivery.id).await.expect("ack");
    }
    // slow: acks 1, dead-letters 2, acks 3, leaves 4 and 5 pending.
    for n in 1..=3 {
        let delivery = next_soon(&mut slow_sub).await;
        assert_eq!(delivery.envelope, changed(n));
        if n == 2 {
            slow_sub
                .nack(delivery.id, Duration::ZERO, "fails".into())
                .await
                .expect("nack");
        } else {
            slow_sub.ack(delivery.id).await.expect("ack");
        }
    }
    let ten_days = Duration::from_secs(10 * 24 * 3600);
    let now =
        Timestamp::from_micros(EPOCH_2026 + u64::try_from(ten_days.as_micros()).expect("fits"));
    // Retention longer than the envelopes' age: nothing goes.
    assert_eq!(bus.prune(now, 2 * ten_days).await.expect("prunes"), 0);
    let pruned = bus
        .prune(now, Duration::from_secs(3600))
        .await
        .expect("prunes");
    // 1 and 3 are done everywhere; 2 has a dead letter; 4, 5 are pending
    // in slow; 6 (a watermark slow never takes) is after slow's horizon.
    assert_eq!(pruned, 2);
    assert_eq!(
        log_ids(db.pool()).await,
        ids(&[changed(2), changed(4), changed(5), watermark(6)])
    );
    // What stays still delivers.
    let fourth = next_soon(&mut slow_sub).await;
    assert_eq!(fourth.envelope, changed(4));
    slow_sub.ack(fourth.id).await.expect("ack");
    let fifth = next_soon(&mut slow_sub).await;
    slow_sub.ack(fifth.id).await.expect("ack");
    // Now slow is past 5 and 6 too: only the dead letter's entry stays.
    assert_eq!(
        bus.prune(now, Duration::from_secs(3600))
            .await
            .expect("prunes"),
        3
    );
    assert_eq!(log_ids(db.pool()).await, ids(&[changed(2)]));
    bus.dead_letters()
        .replay(&group("slow"), changed(2).id)
        .await
        .expect("a pruned log still replays its dead letters");
    assert_eq!(next_soon(&mut slow_sub).await.envelope, changed(2));
    drop((fast_sub, slow_sub));
    bus.shutdown();
    db.close().await.expect("drop the test database");
}

/// `group_stats` agrees with the deliveries and dead letters tables.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_group_stats_agree_with_deliveries() {
    let Some(db) = database("pg_group_stats_agree_with_deliveries").await else {
        return;
    };
    let bus = process(&db, config()).await;
    let (a, b) = (group("a"), group("b"));
    let mut a_sub = bus
        .subscribe(
            &[Subject::Changed],
            a.clone(),
            retry(1, Duration::from_millis(10), Duration::from_millis(80)),
        )
        .await
        .expect("subscribe");
    let mut b_sub = bus
        .subscribe(&[Subject::Changed], b.clone(), default_retry())
        .await
        .expect("subscribe");
    for n in 1..=4 {
        bus.publish(changed(n)).await.expect("publish");
    }
    // a: dead-letters 1, acks 2; 3 and 4 pending. b: holds 1; all pending.
    let first = next_soon(&mut a_sub).await;
    a_sub
        .nack(first.id, Duration::ZERO, "fails".into())
        .await
        .expect("nack");
    let second = next_soon(&mut a_sub).await;
    a_sub.ack(second.id).await.expect("ack");
    let _held = next_soon(&mut b_sub).await;
    let stats = bus.group_stats().await.expect("stats");
    assert_eq!(stats.len(), 2);
    let at = |n: u64| Some(Timestamp::from_micros(EPOCH_2026 + n));
    assert_eq!(stats[0].group, a);
    assert_eq!((stats[0].pending, stats[0].oldest_pending), (2, at(3)));
    assert_eq!(
        (stats[0].dead_letters, stats[0].oldest_dead_letter),
        (1, at(1))
    );
    assert_eq!(stats[1].group, b);
    // b admitted on its first next; the rest is admitted as it asks.
    let (pending, oldest): (i64, Option<i64>) =
        sqlx::query_as("SELECT count(*), min(at) FROM transport.deliveries WHERE group_name = 'b'")
            .fetch_one(db.pool())
            .await
            .expect("counts");
    assert_eq!(
        stats[1].pending,
        u64::try_from(pending).expect("non-negative")
    );
    assert_eq!(
        stats[1].oldest_pending,
        oldest.map(|m| Timestamp::from_micros(u64::try_from(m).expect("non-negative")))
    );
    assert_eq!(
        (stats[1].dead_letters, stats[1].oldest_dead_letter),
        (0, None)
    );
    drop((a_sub, b_sub));
    bus.shutdown();
    db.close().await.expect("drop the test database");
}

/// A group tracks at most `group_capacity` envelopes: it admits more only
/// as acks make room, and `publish` never waits for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_group_capacity_bounds_admission() {
    let Some(db) = database("pg_group_capacity_bounds_admission").await else {
        return;
    };
    let config = PgBusConfig {
        group_capacity: NonZeroUsize::new(2).expect("non-zero"),
        ..config()
    };
    let bus = process(&db, config).await;
    let mut sub = bus
        .subscribe(&[Subject::Changed], group("flow"), default_retry())
        .await
        .expect("subscribe");
    for n in 1..=5 {
        bus.publish(changed(n)).await.expect("publish never waits");
    }
    let first = next_soon(&mut sub).await;
    let second = next_soon(&mut sub).await;
    assert!(
        nothing_within(&mut sub, Duration::from_millis(400)).await,
        "the group is full"
    );
    let tracked: i64 = sqlx::query_scalar("SELECT count(*) FROM transport.deliveries")
        .fetch_one(db.pool())
        .await
        .expect("counts");
    assert_eq!(tracked, 2);
    sub.ack(first.id).await.expect("ack");
    sub.ack(second.id).await.expect("ack");
    let mut rest = Vec::new();
    for _ in 0..3 {
        let delivery = next_soon(&mut sub).await;
        rest.push(delivery.envelope.clone());
        sub.ack(delivery.id).await.expect("ack");
    }
    assert_eq!(rest, vec![changed(3), changed(4), changed(5)]);
    drop(sub);
    bus.shutdown();
    db.close().await.expect("drop the test database");
}

/// A process whose pool goes through `link`, with short timeouts so an
/// outage is seen at once.
fn linked_bus(db: &TestDb, link: &DbLink) -> PgBus {
    let options = db
        .url()
        .connect_options()
        .clone()
        .host(&link.host())
        .port(link.port());
    let pool = PgPoolOptions::new()
        .max_connections(3)
        .acquire_timeout(2 * SECOND)
        .connect_lazy_with(options);
    let config = PgBusConfig {
        publish_timeout: non_zero(2 * SECOND),
        ..config()
    };
    PgBus::new(pool, Arc::new(SystemClock), config).expect("bus starts")
}

async fn link_to(db: &TestDb) -> DbLink {
    let options = db.url().connect_options();
    DbLink::start(options.get_host(), options.get_port())
        .await
        .expect("db link")
}

async fn wait_direct<B: DrainTarget + Send + Sync + 'static>(spool: &SpoolingBus<B>) {
    for _ in 0..600 {
        if spool.state() == SpoolState::Direct && spool.stats().records == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the spool did not drain: {:?}", spool.stats());
}

/// `transport.spool.drained-once-under-its-id`: publishes made while the
/// database is unreachable are spooled, and once it answers they are in
/// `transport.events` exactly once each, under their ids, in publish
/// order, ahead of what was published after.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spool_drain_after_outage_publishes_each_id_once() {
    let Some(db) = database("spool_drain_after_outage_publishes_each_id_once").await else {
        return;
    };
    let link = link_to(&db).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let spool = SpoolingBus::open(
        linked_bus(&db, &link),
        small_config(dir.path(), 1 << 22, 1 << 12),
    )
    .await
    .expect("opens");
    let mut published = Vec::new();
    spool.publish(changed(1)).await.expect("direct");
    published.push(changed(1));

    link.cut();
    for n in 2..=21 {
        spool.publish(changed(n)).await.expect("spooled");
        published.push(changed(n));
    }
    assert_ne!(spool.state(), SpoolState::Direct);
    assert_eq!(spool.stats().records, 20);
    assert_eq!(log_ids(db.pool()).await, ids(&published[..1]));

    link.restore();
    for n in 22..=30 {
        spool.publish(changed(n)).await.expect("published");
        published.push(changed(n));
    }
    wait_direct(&spool).await;
    spool.publish(changed(31)).await.expect("direct");
    published.push(changed(31));
    assert_eq!(log_ids(db.pool()).await, ids(&published));
    spool.close().await;
    drop(spool);
    db.close().await.expect("drop the test database");
}

/// `transport.spool.ok-means-durable`: publishes spooled while the
/// database is down survive a crash of the process that spooled them; the
/// restarted process, the database still down, spools more, and once the
/// database answers every `Ok` id is in the log once, in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spool_publish_survives_restart_while_database_down() {
    let Some(db) = database("spool_publish_survives_restart_while_database_down").await else {
        return;
    };
    let link = link_to(&db).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let spool_config = small_config(dir.path(), 1 << 22, 1 << 12);
    let mut published = Vec::new();
    link.cut();
    {
        let spool = SpoolingBus::open(linked_bus(&db, &link), spool_config.clone())
            .await
            .expect("opens");
        for n in 1..=10 {
            spool.publish(changed(n)).await.expect("spooled");
            published.push(changed(n));
        }
        // Killed while spooling.
        spool.crash().await;
    }
    let spool = SpoolingBus::open(linked_bus(&db, &link), spool_config)
        .await
        .expect("reopens with the link still cut");
    assert_eq!(spool.state(), SpoolState::Spooling);
    assert_eq!(spool.stats().records, 10);
    for n in 11..=15 {
        spool.publish(changed(n)).await.expect("spooled");
        published.push(changed(n));
    }
    assert!(log_ids(db.pool()).await.is_empty());
    link.restore();
    wait_direct(&spool).await;
    assert_eq!(log_ids(db.pool()).await, ids(&published));
    spool.close().await;
    drop(spool);
    db.close().await.expect("drop the test database");
}

/// While the database is down a plain `PgBus` publish reports
/// `Disconnected` within its publish timeout, which is what the spool
/// spools on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_publish_while_unreachable_is_disconnected() {
    let Some(db) = database("pg_publish_while_unreachable_is_disconnected").await else {
        return;
    };
    let link = link_to(&db).await;
    let bus = linked_bus(&db, &link);
    bus.publish(changed(1)).await.expect("up");
    link.cut();
    let started = tokio::time::Instant::now();
    assert_eq!(bus.publish(changed(2)).await, Err(BusError::Disconnected));
    assert!(started.elapsed() < 5 * SECOND);
    assert_eq!(bus.probe().await, Err(BusError::Disconnected));
    link.restore();
    let mut answered = false;
    for _ in 0..100 {
        if bus.probe().await.is_ok() {
            answered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(answered, "the bus answers again once the link is restored");
    bus.publish(changed(2)).await.expect("up again");
    assert_eq!(log_ids(db.pool()).await, ids(&[changed(1), changed(2)]));
    bus.shutdown();
    db.close().await.expect("drop the test database");
}
