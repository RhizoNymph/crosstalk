//! A planned handover's release (`PgBus::release_held`): what the lock's
//! holder still holds when it stops on purpose goes back ready at once,
//! its attempt given back, never dead-lettered, and is taken before
//! anything published after it; the next holder's `recover_held` finds
//! nothing left (`transport.pipeline-lock.planned-handover-clean`).

use std::sync::Arc;
use std::time::Duration;

use crosstalk_spec::events::Subject;
use crosstalk_spec::interfaces::l2_transport::{EventBus, Subscription};
use crosstalk_spec::support::SystemClock;

use super::{SECOND, config, database, next_soon, pool, process};
use crate::testing::{changed, group, retry};
use crate::{HANDOVER_REASON, PgBus};

/// A delivery's group, attempt, state, due time and last error.
type DeliveryRow = (String, i32, String, Option<i64>, Option<String>);

/// Two groups hold a delivery each, one of them on its last attempt (a
/// one-attempt group). The release readies both at once (no backoff) with
/// their attempts given back and the handover's reason; nothing is
/// dead-lettered. The next process's `recover_held` finds nothing held,
/// and its consumer takes the released delivery first, on its first
/// attempt again, before an entry published after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_released_holds_are_ready_at_once_on_the_same_attempt() {
    let Some(db) = database("pg_released_holds_are_ready_at_once_on_the_same_attempt").await else {
        return;
    };
    let (flow, once) = (group("flow"), group("once"));
    // A long backoff: a release that delayed would be seen.
    let slow = retry(5, 10 * SECOND, 30 * SECOND);
    let one_attempt = retry(1, 10 * SECOND, 30 * SECOND);
    {
        let bus = process(&db, config()).await;
        let mut flow_sub = bus
            .subscribe(&[Subject::Changed], flow.clone(), slow)
            .await
            .expect("subscribe");
        let mut once_sub = bus
            .subscribe(&[Subject::Changed], once.clone(), one_attempt)
            .await
            .expect("subscribe");
        bus.publish(changed(1)).await.expect("publish");
        assert_eq!(next_soon(&mut flow_sub).await.attempt.get(), 1);
        assert_eq!(next_soon(&mut once_sub).await.attempt.get(), 1);
        bus.publish(changed(2)).await.expect("publish");
        // The handover: the subscriptions stop without settling, the bus's
        // tasks and takes are joined, then the holds are released.
        bus.shutdown();
        drop(flow_sub);
        drop(once_sub);
        let abandoned = bus.abandon(tokio::time::Instant::now() + 5 * SECOND).await;
        assert!(abandoned.takes_joined, "{abandoned:?}");
        let released = bus.release_held().await.expect("releases");
        assert_eq!(released, 2, "both holds released");
    }

    let rows: Vec<DeliveryRow> = sqlx::query_as(
        "SELECT group_name, attempt, state, available_at, last_error FROM transport.deliveries \
         WHERE seq = (SELECT min(seq) FROM transport.events) ORDER BY group_name",
    )
    .fetch_all(db.pool())
    .await
    .expect("reads deliveries");
    let expected = |name: &str| {
        (
            name.to_owned(),
            0,
            "ready".to_owned(),
            None,
            Some(HANDOVER_REASON.to_owned()),
        )
    };
    assert_eq!(
        rows,
        vec![expected("flow"), expected("once")],
        "ready at once, the attempt given back"
    );
    let (letters,): (i64,) = sqlx::query_as("SELECT count(*) FROM transport.dead_letters")
        .fetch_one(db.pool())
        .await
        .expect("counts");
    assert_eq!(letters, 0, "a release dead-letters nothing");

    let pool = pool(db.url().connect_options().clone()).await;
    let bus = PgBus::new(pool, Arc::new(SystemClock), config()).expect("bus starts");
    let recovered = bus.recover_held().await.expect("recovers");
    assert_eq!(
        (recovered.redelivered, recovered.dead_lettered),
        (0, 0),
        "nothing left held for the next holder"
    );
    let mut flow_sub = bus
        .subscribe(&[Subject::Changed], flow, slow)
        .await
        .expect("subscribe");
    let mut once_sub = bus
        .subscribe(&[Subject::Changed], once, one_attempt)
        .await
        .expect("subscribe");
    bus.publish(changed(3)).await.expect("publish");
    let mut got = Vec::new();
    for _ in 0..3 {
        let delivery = next_soon(&mut flow_sub).await;
        got.push((delivery.envelope.clone(), delivery.attempt.get()));
        flow_sub.ack(delivery.id).await.expect("ack");
    }
    assert_eq!(
        got,
        vec![(changed(1), 1), (changed(2), 1), (changed(3), 1)],
        "the released delivery first, on its first attempt again"
    );
    let first = tokio::time::timeout(Duration::from_secs(20), once_sub.next())
        .await
        .expect("the one-attempt group's delivery is ready")
        .expect("open")
        .expect("a delivery");
    assert_eq!((first.envelope, first.attempt.get()), (changed(1), 1));
    once_sub.ack(first.id).await.expect("ack");
    drop(flow_sub);
    drop(once_sub);
    bus.shutdown();
    db.close().await.expect("drop the test database");
}

/// Nothing held: the release touches nothing and says so.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_a_release_with_nothing_held_releases_nothing() {
    let Some(db) = database("pg_a_release_with_nothing_held_releases_nothing").await else {
        return;
    };
    let bus = process(&db, config()).await;
    let mut sub = bus
        .subscribe(
            &[Subject::Changed],
            group("acked"),
            retry(3, SECOND, SECOND),
        )
        .await
        .expect("subscribe");
    bus.publish(changed(1)).await.expect("publish");
    let delivery = next_soon(&mut sub).await;
    sub.ack(delivery.id).await.expect("ack");
    bus.publish(changed(2)).await.expect("publish");
    assert_eq!(bus.release_held().await.expect("releases"), 0);
    drop(sub);
    bus.shutdown();
    db.close().await.expect("drop the test database");
}
