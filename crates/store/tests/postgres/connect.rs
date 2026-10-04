//! `Store::connect`: a working pool, and a typed error when the server is
//! unreachable.

use std::num::NonZeroU32;
use std::time::Duration;

use crosstalk_store::{
    DATABASE_URL_VAR, DatabaseUrl, DbFailure, PoolSettings, Store, StoreConfig, StoreError,
};

use crate::{Failure, TestResult, db};

/// Runs without a server: nothing listens on port 1.
#[tokio::test(flavor = "multi_thread")]
async fn unreachable_server_is_a_typed_connect_error() -> TestResult {
    let url = DatabaseUrl::parse(DATABASE_URL_VAR, "postgres://nobody:pw@127.0.0.1:1/none")
        .map_err(|e| Failure::Unexpected(e.to_string()))?;
    let settings = PoolSettings::new(NonZeroU32::MIN, 0, Duration::from_secs(2))
        .map_err(|e| Failure::Unexpected(e.to_string()))?;
    let got = Store::connect(&StoreConfig::new(url, settings)).await;
    match got {
        Err(StoreError::Connect {
            target, failure, ..
        }) => {
            assert_eq!(target, "127.0.0.1:1/none");
            assert!(failure.is_unavailable(), "{failure:?}");
            assert!(matches!(
                failure,
                DbFailure::ConnectionLost | DbFailure::PoolTimedOut
            ));
        }
        other => return Err(Failure::Unexpected(format!("{other:?}"))),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn connect_opens_a_working_pool() -> TestResult {
    let Some(db) = db("connect_opens_a_working_pool").await? else {
        return Ok(());
    };
    let store =
        Store::connect(&StoreConfig::new(db.url().clone(), PoolSettings::default())).await?;
    let one: i32 = sqlx::query_scalar("SELECT 1")
        .fetch_one(store.pool())
        .await?;
    assert_eq!(one, 1);
    store.close().await;
    db.close().await?;
    Ok(())
}
