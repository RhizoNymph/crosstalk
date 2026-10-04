//! `TestDb`: isolation, parallelism and cleanup.

use crosstalk_store::{Layer, Migrations, TEST_DB_PREFIX, TestDb};

use crate::{Failure, TestResult, database_exists, db, fixture, table_exists};

#[tokio::test(flavor = "multi_thread")]
async fn test_databases_are_isolated() -> TestResult {
    let Some(a) = db("test_databases_are_isolated").await? else {
        return Ok(());
    };
    let b = TestDb::new().await?;
    let (Some(name_a), Some(name_b)) = (a.name(), b.name()) else {
        return Err(Failure::Unexpected("a live TestDb has a name".into()));
    };
    assert_ne!(name_a, name_b);
    assert!(name_a.as_str().starts_with(TEST_DB_PREFIX));

    sqlx::query("CREATE TABLE public.only_in_a (id int)")
        .execute(a.pool())
        .await?;
    assert!(table_exists(a.pool(), "public", "only_in_a").await?);
    assert!(!table_exists(b.pool(), "public", "only_in_a").await?);

    let current: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(a.pool())
        .await?;
    assert_eq!(current, name_a.as_str());

    a.close().await?;
    b.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn close_drops_the_database() -> TestResult {
    let Some(observer) = db("close_drops_the_database").await? else {
        return Ok(());
    };
    let doomed = TestDb::new().await?;
    let name = doomed
        .name()
        .map(|n| n.as_str().to_owned())
        .ok_or_else(|| Failure::Unexpected("a live TestDb has a name".into()))?;
    assert!(database_exists(observer.pool(), &name).await?);
    doomed.close().await?;
    assert!(!database_exists(observer.pool(), &name).await?);
    observer.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_drops_the_database() -> TestResult {
    let Some(observer) = db("drop_drops_the_database").await? else {
        return Ok(());
    };
    let doomed = TestDb::new().await?;
    let name = doomed
        .name()
        .map(|n| n.as_str().to_owned())
        .ok_or_else(|| Failure::Unexpected("a live TestDb has a name".into()))?;
    // Hold a checked-out connection across the drop: the drop must still
    // succeed, forcing the session off after the close grace period.
    let held = doomed.pool().acquire().await?;
    drop(doomed);
    drop(held);
    assert!(!database_exists(observer.pool(), &name).await?);
    observer.close().await?;
    Ok(())
}

/// Many test databases at once, each migrating the same layer: no
/// collisions, because each test has its own database.
#[tokio::test(flavor = "multi_thread")]
async fn parallel_test_databases_migrate_the_same_layer() -> TestResult {
    let Some(first) = db("parallel_test_databases_migrate_the_same_layer").await? else {
        return Ok(());
    };
    first.close().await?;
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        tasks.spawn(async {
            let db = TestDb::new().await?;
            db.migrate(Layer::Ingress, Migrations::Directory(&fixture("alpha")))
                .await?;
            let exists = table_exists(db.pool(), "ingress", "items").await?;
            db.close().await?;
            Ok::<bool, Failure>(exists)
        });
    }
    while let Some(joined) = tasks.join_next().await {
        assert!(
            joined??,
            "every parallel database got its own ingress.items"
        );
    }
    Ok(())
}
