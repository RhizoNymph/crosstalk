//! The pipeline lock across a silent network partition: the holder must
//! report the lock possibly lost within a bound strictly shorter than the
//! server's reaping of the partitioned session, since after that reap
//! another process may take the lock.

use std::time::Duration;

use crosstalk_testkit::db_link::DbLink;
use tokio::time::Instant;

use super::database;
use crate::store::lock::{PING, PipelineLock};

/// The detection bound under test: a ping's interval plus a ping's deadline.
const BOUND: Duration = PING.saturating_add(Duration::from_secs(5));

/// Timer and scheduling slack on top of the bound.
const SLACK: Duration = Duration::from_secs(1);

/// A lock session whose link is silently partitioned (nothing answered,
/// nothing reset) is reported lost within the bound, and the timed-out
/// connection is dropped: once the link heals the server ends the session
/// and another session takes the lock.
#[tokio::test(flavor = "multi_thread")]
async fn pg_a_silently_partitioned_lock_session_is_lost_within_the_bound() {
    let Some(db) =
        database("pg_a_silently_partitioned_lock_session_is_lost_within_the_bound").await
    else {
        return;
    };
    let options = db.url().connect_options();
    let link = DbLink::start(options.get_host(), options.get_port())
        .await
        .expect("the db link");
    let url = db.url().with_address(&link.host(), link.port());
    let mut lock = PipelineLock::try_take(&url)
        .await
        .expect("connects through the link")
        .expect("takes the lock");

    link.partition();
    let partitioned = Instant::now();
    let why = tokio::time::timeout(BOUND + SLACK, lock.lost())
        .await
        .unwrap_or_else(|_| {
            panic!(
                "a silently partitioned lock session was not reported lost within {:?}",
                BOUND + SLACK
            )
        });
    let detected = partitioned.elapsed();
    tracing::info!(?detected, %why, "lock loss detected");
    assert!(
        detected <= BOUND + SLACK,
        "detected after {detected:?}: {why}"
    );

    // The holder dropped its connection; healed, the close crosses, the
    // server ends the session and the lock is free again.
    link.restore();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let taken = PipelineLock::try_take(db.url())
            .await
            .expect("connects directly");
        if taken.is_some() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the partitioned session still holds the lock after the link healed"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    drop(lock);
    drop(link);
    db.close().await.expect("drops");
}
