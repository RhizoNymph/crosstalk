//! A delivery released because its consumer's store stalled (a statement
//! Postgres cancelled at its `statement_timeout` or `lock_timeout`) is
//! redelivered without spending an attempt and is never dead-lettered for
//! it; a nack still spends one and dead-letters on the last
//! (`transport.pipeline.statements-bounded`).

use std::time::Duration;

use crosstalk_spec::events::Subject;
use crosstalk_spec::interfaces::l2_transport::{EventBus, Subscription};

use super::{config, database, next_soon, process};
use crate::testing::{changed, group, retry};

const STALL: Duration = Duration::from_millis(20);

/// The stored attempt and state of the group's one delivery, or `None`.
async fn row(db: &crosstalk_store::TestDb, group: &str) -> Option<(i32, String)> {
    sqlx::query_as("SELECT attempt, state FROM transport.deliveries WHERE group_name = $1")
        .bind(group)
        .fetch_optional(db.pool())
        .await
        .expect("reads the delivery")
}

async fn dead_letters(db: &crosstalk_store::TestDb, group: &str) -> i64 {
    let (n,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM transport.dead_letters WHERE group_name = $1")
            .bind(group)
            .fetch_one(db.pool())
            .await
            .expect("counts");
    n
}

/// Released ten times on a two-attempt group, the delivery comes back
/// every time on its first attempt and is never dead-lettered; its first
/// failure of the message itself (a nack) spends that attempt, its second
/// dead-letters it.
#[tokio::test(flavor = "multi_thread")]
async fn pg_a_released_delivery_keeps_its_attempt_and_is_never_dead_lettered() {
    let Some(db) =
        database("pg_a_released_delivery_keeps_its_attempt_and_is_never_dead_lettered").await
    else {
        return;
    };
    let bus = process(&db, config()).await;
    let two = retry(2, Duration::from_millis(10), Duration::from_millis(40));
    let mut sub = bus
        .subscribe(&[Subject::Changed], group("stalled"), two)
        .await
        .expect("subscribes");
    bus.publish(changed(1)).await.expect("publishes");

    for round in 0..10 {
        let delivery = next_soon(&mut sub).await;
        assert_eq!(
            delivery.attempt.get(),
            1,
            "round {round}: a release spends no attempt"
        );
        sub.release(
            delivery.id,
            STALL,
            "lock timeout (SQLSTATE 55P03)".to_owned(),
        )
        .await
        .expect("released");
        let (attempt, state) = row(&db, "stalled").await.expect("still delivered");
        assert_eq!(
            (attempt, state.as_str()),
            (0, "delayed"),
            "round {round}: delayed, its attempt given back"
        );
        assert_eq!(dead_letters(&db, "stalled").await, 0, "round {round}");
    }

    let first = next_soon(&mut sub).await;
    assert_eq!(first.attempt.get(), 1);
    sub.nack(first.id, STALL, "the message itself failed".to_owned())
        .await
        .expect("nacked");
    let second = next_soon(&mut sub).await;
    assert_eq!(second.attempt.get(), 2, "a nack spends its attempt");
    sub.nack(second.id, STALL, "the message itself failed".to_owned())
        .await
        .expect("nacked");
    assert_eq!(row(&db, "stalled").await, None);
    assert_eq!(
        dead_letters(&db, "stalled").await,
        1,
        "the last attempt's nack dead-letters it"
    );

    drop(sub);
    bus.shutdown();
    db.close().await.expect("drops");
}

/// A hold released and taken again on the same attempt is a new hold: the
/// old hold's deadlines (its ack timeout, its provisional deadline) never
/// take the new one back.
#[tokio::test(flavor = "multi_thread")]
async fn pg_an_old_holds_deadline_never_reaps_the_hold_after_a_release() {
    let Some(db) = database("pg_an_old_holds_deadline_never_reaps_the_hold_after_a_release").await
    else {
        return;
    };
    let ack_timeout = Duration::from_secs(1);
    let bus = process(
        &db,
        crate::PgBusConfig {
            ack_timeout: crate::testing::non_zero(ack_timeout),
            ..config()
        },
    )
    .await;
    let policy = retry(5, Duration::from_millis(10), Duration::from_millis(40));
    let mut sub = bus
        .subscribe(&[Subject::Changed], group("reheld"), policy)
        .await
        .expect("subscribes");
    bus.publish(changed(2)).await.expect("publishes");
    let first = next_soon(&mut sub).await;
    // Released just before its ack deadline; taken again on attempt 1.
    tokio::time::sleep(ack_timeout / 2).await;
    sub.release(
        first.id,
        STALL,
        "statement timeout (SQLSTATE 57014)".to_owned(),
    )
    .await
    .expect("released");
    let again = next_soon(&mut sub).await;
    assert_eq!(again.attempt.get(), 1);
    // Past the first hold's deadline, inside the second's: still held.
    tokio::time::sleep(ack_timeout * 3 / 4).await;
    let (attempt, state) = row(&db, "reheld").await.expect("delivered");
    assert_eq!(
        (attempt, state.as_str()),
        (1, "held"),
        "the first hold's deadline took the second back"
    );
    sub.ack(again.id).await.expect("acks the second hold");
    assert_eq!(row(&db, "reheld").await, None);

    drop(sub);
    bus.shutdown();
    db.close().await.expect("drops");
}
