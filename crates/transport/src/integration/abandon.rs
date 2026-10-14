//! [`PgBus::abandon`]: what a pipeline that lost its lock does to its bus.
//! Nothing it owned may act after the abandon returns, and nothing it held
//! may be handed back early: the next holder's `recover_held` readies it.

use std::time::Duration;

use crosstalk_spec::events::Subject;
use crosstalk_spec::interfaces::l2_transport::{EventBus, Subscription};
use crosstalk_spec::support::SystemClock;
use tokio::time::Instant;

use super::{SECOND, config, database, next_soon, pool};
use crate::PgBus;
use crate::testing::{changed, default_retry, group};

/// The deliveries' rows: (seq, state, attempt), by seq.
async fn rows(db: &crosstalk_store::TestDb) -> Vec<(i64, String, i32)> {
    sqlx::query_as("SELECT seq, state, attempt FROM transport.deliveries ORDER BY seq")
        .fetch_all(db.pool())
        .await
        .expect("reads deliveries")
}

/// A bus abandoned with a hold out and a take in flight: the abandon
/// returns with its listener, reaper and every take joined (none left in
/// flight), waiting `next` calls end, and the database is left as a
/// killed process leaves it: the hold stays held, dropping the
/// subscription afterwards nacks nothing, and no row changes after the
/// abandon. The next bus's `recover_held` readies the holds at once and the
/// first is taken first, at its next attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_an_abandoned_bus_joins_its_takes_and_leaves_its_holds_for_recovery() {
    let Some(db) =
        database("pg_an_abandoned_bus_joins_its_takes_and_leaves_its_holds_for_recovery").await
    else {
        return;
    };
    let flow = group("flow");
    let pool = pool(db.url().connect_options().clone()).await;
    let bus =
        PgBus::new(pool.clone(), std::sync::Arc::new(SystemClock), config()).expect("bus starts");
    bus.recover_held().await.expect("recovers");
    let mut sub = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    bus.publish(changed(1)).await.expect("publish");
    let first = next_soon(&mut sub).await;
    assert_eq!(first.envelope, changed(1));
    bus.publish(changed(2)).await.expect("publish");
    // A take in flight, its caller gone: it belongs to the bus.
    {
        let mut next = std::pin::pin!(sub.next());
        let ready =
            std::future::poll_fn(|cx| std::task::Poll::Ready(next.as_mut().poll(cx).is_ready()))
                .await;
        assert!(!ready, "a take is a round trip at least");
    }

    // The fence first, as the gateway does: the pool hands out nothing.
    let _marked = pool.close();
    let abandoned = bus.abandon(Instant::now() + 5 * SECOND).await;
    assert!(abandoned.tasks_joined, "{abandoned:?}");
    assert!(abandoned.takes_joined, "{abandoned:?}");
    assert!(bus.takes().is_empty(), "no take left in flight");
    assert!(
        tokio::time::timeout(5 * SECOND, sub.next())
            .await
            .expect("an abandoned bus's next returns at once")
            .is_none(),
        "a waiting next ends"
    );
    let after = rows(&db).await;
    assert!(
        after.iter().any(|(_, state, _)| state == "held"),
        "the hold stays held: {after:?}"
    );
    drop(sub);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(rows(&db).await, after, "nothing changes after the abandon");
    let held = after.iter().filter(|(_, state, _)| state == "held").count();

    let pool = super::pool(db.url().connect_options().clone()).await;
    let next = PgBus::new(pool, std::sync::Arc::new(SystemClock), config()).expect("bus starts");
    let recovered = next.recover_held().await.expect("recovers");
    assert_eq!(
        recovered.redelivered,
        u64::try_from(held).expect("fits"),
        "every abandoned hold readied"
    );
    let mut sub = next
        .subscribe(&[Subject::Changed], flow, default_retry())
        .await
        .expect("subscribe");
    let again = next_soon(&mut sub).await;
    assert_eq!((again.envelope, again.attempt.get()), (changed(1), 2));
    sub.ack(again.id).await.expect("acks");
    drop(sub);
    next.shutdown();
    db.close().await.expect("drop the test database");
}
