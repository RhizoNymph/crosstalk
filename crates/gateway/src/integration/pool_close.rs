//! Why a shutdown across a partition hung (the node0 bench's 53 s
//! `pool.close()`): sqlx 0.9's `Pool::close` waits for every checked-out
//! connection to come back, and a connection whose query was aborted
//! comes back through a ping with no deadline, which across a silent
//! partition waits for TCP. Every close of a pool the gateway owns is
//! therefore bounded, and a lost pipeline's pool is closed before its
//! tasks are aborted (a closed pool closes a returning connection instead
//! of pinging it).

use std::time::Duration;

use crosstalk_store::sqlx;
use crosstalk_store::{StoreConfig, TestDb};
use crosstalk_testkit::db_link::DbLink;
use tokio::time::Instant;

use super::database;
use crate::store::lazy_pool;

/// How long the close is given while partitioned: far past a loopback
/// round trip.
const CLOSE_WAIT: Duration = Duration::from_secs(3);

/// A pool on `db`'s server through `link`.
fn pool_through(db: &TestDb, link: &DbLink) -> sqlx::PgPool {
    let url = db.url().with_address(&link.host(), link.port());
    let settings = crosstalk_store::PoolSettings::new(
        std::num::NonZeroU32::MIN.saturating_add(3),
        0,
        Duration::from_secs(5),
    )
    .expect("pool settings");
    lazy_pool(&StoreConfig::new(url, settings))
}

/// Wait until the server shows a `pg_sleep` running on the test database.
async fn sleeping(db: &TestDb) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (running,): (i64,) = sqlx::query_as(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() \
             AND state = 'active' AND query LIKE 'SELECT pg_sleep%'",
        )
        .fetch_one(db.pool())
        .await
        .expect("reads pg_stat_activity");
        if running > 0 {
            return;
        }
        assert!(Instant::now() < deadline, "the pg_sleep never started");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The cause, shown: a query in flight through a link that is then
/// silently partitioned, its task aborted (as a stopping pipeline's
/// tasks are), and `pool.close()` does not finish while the link stays
/// partitioned. Closed before the abort instead, the pool closes the
/// returning connection without a ping, and `close()` finishes.
#[tokio::test(flavor = "multi_thread")]
async fn pg_a_partitioned_pool_close_waits_for_an_aborted_query() {
    let Some(db) = database("pg_a_partitioned_pool_close_waits_for_an_aborted_query").await else {
        return;
    };

    // Aborted first, closed after: the close waits on the dead link.
    let link = DbLink::start(
        db.url().connect_options().get_host(),
        db.url().connect_options().get_port(),
    )
    .await
    .expect("the db link");
    let pool = pool_through(&db, &link);
    let query_pool = pool.clone();
    let query = tokio::spawn(async move {
        sqlx::query("SELECT pg_sleep(60)")
            .execute(&query_pool)
            .await
    });
    sleeping(&db).await;
    link.partition();
    query.abort();
    let _ = query.await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let closed = tokio::time::timeout(CLOSE_WAIT, pool.close()).await;
    assert!(
        closed.is_err(),
        "pool.close() finished across a silent partition with an aborted query's connection out"
    );
    eprintln!("aborted then closed: pool.close() still waiting after {CLOSE_WAIT:?}");
    drop(link);

    // Closed first (the fence), aborted after: nothing waits on the link.
    let link = DbLink::start(
        db.url().connect_options().get_host(),
        db.url().connect_options().get_port(),
    )
    .await
    .expect("the db link");
    let pool = pool_through(&db, &link);
    let query_pool = pool.clone();
    let query = tokio::spawn(async move {
        sqlx::query("SELECT pg_sleep(60)")
            .execute(&query_pool)
            .await
    });
    sleeping(&db).await;
    link.partition();
    drop(pool.close());
    assert!(pool.is_closed());
    query.abort();
    let _ = query.await;
    let started = Instant::now();
    let closed = tokio::time::timeout(CLOSE_WAIT, pool.close()).await;
    assert!(
        closed.is_ok(),
        "a pool closed before the abort still waited {CLOSE_WAIT:?}"
    );
    eprintln!(
        "closed then aborted: pool.close() took {:?}",
        started.elapsed()
    );
    assert!(
        matches!(
            sqlx::query("SELECT 1").execute(&pool).await,
            Err(sqlx::Error::PoolClosed)
        ),
        "a closed pool hands out nothing"
    );
    drop(link);
    // The server runs each pg_sleep to its end whatever became of the
    // client: end those sessions before the database is dropped.
    sqlx::query(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE datname = current_database() AND pid <> pg_backend_pid()",
    )
    .execute(db.pool())
    .await
    .expect("ends the sleeping sessions");
    db.close().await.expect("drops");
}
