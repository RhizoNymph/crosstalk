//! `retry_serializable`: conflicts retried, budget enforced, aborts and
//! other failures returned as they are.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use crosstalk_store::{
    DbFailure, Layer, Migrations, SerializableError, SerializableRetry, StoreError, TestDb,
    TxError, retry_serializable,
};
use tokio::sync::Barrier;

use crate::{Failure, TestResult, db, fixture};

fn policy(attempts: u32) -> Result<SerializableRetry, Failure> {
    let attempts = NonZeroU32::new(attempts)
        .ok_or_else(|| Failure::Unexpected("attempts must be non-zero".into()))?;
    SerializableRetry::new(
        attempts,
        Duration::from_millis(1),
        Duration::from_millis(20),
    )
    .map_err(|e| Failure::Unexpected(e.to_string()))
}

async fn migrated(test: &str) -> Result<Option<TestDb>, Failure> {
    let Some(db) = db(test).await? else {
        return Ok(None);
    };
    db.migrate(Layer::Ingress, Migrations::Directory(&fixture("alpha")))
        .await?;
    Ok(Some(db))
}

async fn count_labelled(pool: &sqlx::PgPool, label: &str) -> Result<i64, Failure> {
    Ok(
        sqlx::query_scalar("SELECT count(*) FROM ingress.items WHERE label = $1")
            .bind(label)
            .fetch_one(pool)
            .await?,
    )
}

/// Classic write skew: two transactions each read the same predicate, meet
/// at a barrier, then insert. Serializable isolation aborts one; the retry
/// re-runs it and both inserts land.
#[tokio::test(flavor = "multi_thread")]
async fn write_skew_is_retried_to_success() -> TestResult {
    let Some(db) = migrated("write_skew_is_retried_to_success").await? else {
        return Ok(());
    };
    let barrier = Arc::new(Barrier::new(2));
    let calls = Arc::new(AtomicU32::new(0));
    let policy = policy(10)?;

    let mut tasks = tokio::task::JoinSet::new();
    for id in [1_i64, 2] {
        let pool = db.pool().clone();
        let barrier = Arc::clone(&barrier);
        let calls = Arc::clone(&calls);
        tasks.spawn(async move {
            let mut first = true;
            retry_serializable(&pool, &policy, move |conn| {
                let barrier = Arc::clone(&barrier);
                calls.fetch_add(1, Ordering::SeqCst);
                let meet = std::mem::replace(&mut first, false);
                Box::pin(async move {
                    let seen: i64 = sqlx::query_scalar(
                        "SELECT count(*) FROM ingress.items WHERE label = 'skew'",
                    )
                    .fetch_one(&mut *conn)
                    .await?;
                    if meet {
                        barrier.wait().await;
                    }
                    sqlx::query("INSERT INTO ingress.items (id, label) VALUES ($1, 'skew')")
                        .bind(id)
                        .execute(&mut *conn)
                        .await?;
                    Ok::<i64, TxError<()>>(seen)
                })
            })
            .await
        });
    }
    let mut seen = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        match joined? {
            Ok(n) => seen.push(n),
            Err(err) => return Err(Failure::Unexpected(format!("{err:?}"))),
        }
    }
    seen.sort_unstable();
    // The retried transaction saw the other's committed row.
    assert_eq!(seen, [0, 1]);
    assert!(
        calls.load(Ordering::SeqCst) >= 3,
        "one transaction must have been retried"
    );
    assert_eq!(count_labelled(db.pool(), "skew").await?, 2);
    db.close().await?;
    Ok(())
}

/// A body that always conflicts runs exactly `max_attempts` times, then
/// fails with `RetriesExhausted`.
#[tokio::test(flavor = "multi_thread")]
async fn exhausted_retries_are_typed() -> TestResult {
    let Some(db) = migrated("exhausted_retries_are_typed").await? else {
        return Ok(());
    };
    let calls = Arc::new(AtomicU32::new(0));
    let counter = Arc::clone(&calls);
    let got = retry_serializable(db.pool(), &policy(3)?, move |conn| {
        counter.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            sqlx::query(
                "DO $$ BEGIN RAISE EXCEPTION 'forced conflict' \
                 USING ERRCODE = 'serialization_failure'; END $$",
            )
            .execute(&mut *conn)
            .await?;
            Ok::<(), TxError<()>>(())
        })
    })
    .await;
    match got {
        Err(SerializableError::Store(StoreError::RetriesExhausted {
            attempts,
            failure: DbFailure::SerializationFailure,
            ..
        })) => assert_eq!(attempts.get(), 3),
        other => return Err(Failure::Unexpected(format!("{other:?}"))),
    }
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    db.close().await?;
    Ok(())
}

/// The body's own error rolls back and is returned without retrying.
#[tokio::test(flavor = "multi_thread")]
async fn an_aborted_body_rolls_back_once() -> TestResult {
    let Some(db) = migrated("an_aborted_body_rolls_back_once").await? else {
        return Ok(());
    };
    let calls = Arc::new(AtomicU32::new(0));
    let counter = Arc::clone(&calls);
    let got = retry_serializable(db.pool(), &policy(5)?, move |conn| {
        counter.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            sqlx::query("INSERT INTO ingress.items (id, label) VALUES (1, 'aborted')")
                .execute(&mut *conn)
                .await?;
            Err::<(), _>(TxError::Abort("refused by the body"))
        })
    })
    .await;
    assert!(matches!(
        got,
        Err(SerializableError::Aborted("refused by the body"))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(count_labelled(db.pool(), "aborted").await?, 0);
    db.close().await?;
    Ok(())
}

/// A non-retryable failure (a unique violation) is returned after one
/// attempt, classified.
#[tokio::test(flavor = "multi_thread")]
async fn non_retryable_failures_are_not_retried() -> TestResult {
    let Some(db) = migrated("non_retryable_failures_are_not_retried").await? else {
        return Ok(());
    };
    sqlx::query("INSERT INTO ingress.items (id, label) VALUES (1, 'taken')")
        .execute(db.pool())
        .await?;
    let calls = Arc::new(AtomicU32::new(0));
    let counter = Arc::clone(&calls);
    let got = retry_serializable(db.pool(), &policy(5)?, move |conn| {
        counter.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            sqlx::query("INSERT INTO ingress.items (id, label) VALUES (1, 'again')")
                .execute(&mut *conn)
                .await?;
            Ok::<(), TxError<()>>(())
        })
    })
    .await;
    match got {
        Err(SerializableError::Store(StoreError::Query {
            failure: DbFailure::UniqueViolation { constraint },
            ..
        })) => assert_eq!(constraint.as_deref(), Some("items_pkey")),
        other => return Err(Failure::Unexpected(format!("{other:?}"))),
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    db.close().await?;
    Ok(())
}

/// The transaction really is serializable.
#[tokio::test(flavor = "multi_thread")]
async fn the_transaction_is_serializable() -> TestResult {
    let Some(db) = migrated("the_transaction_is_serializable").await? else {
        return Ok(());
    };
    let level = retry_serializable(db.pool(), &policy(1)?, |conn| {
        Box::pin(async move {
            let level: String = sqlx::query_scalar("SHOW transaction_isolation")
                .fetch_one(&mut *conn)
                .await?;
            Ok::<String, TxError<()>>(level)
        })
    })
    .await
    .map_err(|e| Failure::Unexpected(format!("{e:?}")))?;
    assert_eq!(level, "serializable");
    db.close().await?;
    Ok(())
}
