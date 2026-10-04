//! Per-layer migrations: own schema, own migrations table, no collisions.

use std::borrow::Cow;

use crosstalk_store::{DbFailure, Layer, Migrations, StoreError};
use sqlx::SqlSafeStr;
use sqlx::migrate::{MigrateError, Migration, MigrationType, Migrator};

use crate::{Failure, TestResult, db, fixture, table_exists};

/// The alpha fixture embedded at compile time, the way a layer crate embeds
/// its own `migrations/` directory.
static ALPHA: Migrator = sqlx::migrate!("tests/fixtures/alpha");

async fn applied_versions(pool: &sqlx::PgPool, layer: Layer) -> Result<Vec<i64>, Failure> {
    let sql = format!(
        "SELECT version FROM {} WHERE success ORDER BY version",
        layer.migrations_table()
    );
    Ok(sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
        .fetch_all(pool)
        .await?)
}

async fn columns(pool: &sqlx::PgPool, schema: &str, table: &str) -> Result<Vec<String>, Failure> {
    Ok(sqlx::query_scalar(
        "SELECT column_name::text FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position",
    )
    .bind(schema)
    .bind(table)
    .fetch_all(pool)
    .await?)
}

/// Two layers whose migrations share version 1 and a table name both apply,
/// each in its own schema with its own migrations table.
#[tokio::test(flavor = "multi_thread")]
async fn layers_with_the_same_versions_do_not_collide() -> TestResult {
    let Some(db) = db("layers_with_the_same_versions_do_not_collide").await? else {
        return Ok(());
    };
    db.migrate(Layer::Ingress, Migrations::Embedded(&ALPHA))
        .await?;
    db.migrate(Layer::Canonical, Migrations::Directory(&fixture("beta")))
        .await?;

    assert_eq!(
        columns(db.pool(), "ingress", "items").await?,
        ["id", "label"]
    );
    assert_eq!(
        columns(db.pool(), "canonical", "items").await?,
        ["id", "weight"]
    );
    assert!(table_exists(db.pool(), "canonical", "notes").await?);
    assert!(!table_exists(db.pool(), "ingress", "notes").await?);

    assert_eq!(applied_versions(db.pool(), Layer::Ingress).await?, [1]);
    assert_eq!(applied_versions(db.pool(), Layer::Canonical).await?, [1, 2]);

    // Nothing leaks into public: no shared migrations table, no tables.
    assert!(!table_exists(db.pool(), "public", "_sqlx_migrations").await?);
    assert!(!table_exists(db.pool(), "public", "items").await?);
    assert!(!table_exists(db.pool(), "public", "notes").await?);

    // The pool's own search_path is untouched by the migration runner.
    let path: String = sqlx::query_scalar("SHOW search_path")
        .fetch_one(db.pool())
        .await?;
    assert!(
        !path.contains("ingress") && !path.contains("canonical"),
        "{path}"
    );

    db.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn rerunning_migrations_is_a_no_op() -> TestResult {
    let Some(db) = db("rerunning_migrations_is_a_no_op").await? else {
        return Ok(());
    };
    for _ in 0..3 {
        db.migrate(Layer::Canonical, Migrations::Directory(&fixture("beta")))
            .await?;
    }
    assert_eq!(applied_versions(db.pool(), Layer::Canonical).await?, [1, 2]);
    db.close().await?;
    Ok(())
}

/// Migrating several layers at once serializes on the advisory lock and
/// every layer ends up migrated.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_layer_migrations_all_apply() -> TestResult {
    let Some(db) = db("concurrent_layer_migrations_all_apply").await? else {
        return Ok(());
    };
    let mut runs = tokio::task::JoinSet::new();
    for layer in Layer::ALL {
        let store = db.store();
        let beta = fixture("beta");
        runs.spawn(async move { store.migrate(layer, Migrations::Directory(&beta)).await });
    }
    while let Some(joined) = runs.join_next().await {
        joined??;
    }
    for layer in Layer::ALL {
        assert!(
            table_exists(db.pool(), layer.schema(), "notes").await?,
            "{layer}"
        );
        assert_eq!(applied_versions(db.pool(), layer).await?, [1, 2], "{layer}");
    }
    db.close().await?;
    Ok(())
}

/// Editing an applied migration is refused with a typed error instead of
/// silently diverging.
#[tokio::test(flavor = "multi_thread")]
async fn an_edited_applied_migration_is_refused() -> TestResult {
    let Some(db) = db("an_edited_applied_migration_is_refused").await? else {
        return Ok(());
    };
    db.migrate(Layer::Ingress, Migrations::Embedded(&ALPHA))
        .await?;
    let edited = Migrator::with_migrations(vec![Migration::new(
        1,
        Cow::Borrowed("items"),
        MigrationType::Simple,
        "CREATE TABLE items (id bigint PRIMARY KEY)".into_sql_str(),
        false,
    )]);
    let got = db
        .migrate(Layer::Ingress, Migrations::Embedded(&edited))
        .await;
    match got {
        Err(StoreError::Migrate {
            layer: Layer::Ingress,
            source: MigrateError::VersionMismatch(1),
        }) => {}
        other => return Err(Failure::Unexpected(format!("{other:?}"))),
    }
    // The same edit under another layer is a different history: it applies.
    db.migrate(Layer::Flow, Migrations::Embedded(&edited))
        .await?;
    db.close().await?;
    Ok(())
}

/// A migration whose SQL fails reports the layer and leaves nothing behind.
#[tokio::test(flavor = "multi_thread")]
async fn a_failing_migration_is_typed() -> TestResult {
    let Some(db) = db("a_failing_migration_is_typed").await? else {
        return Ok(());
    };
    let broken = Migrator::with_migrations(vec![Migration::new(
        1,
        Cow::Borrowed("broken"),
        MigrationType::Simple,
        "CREATE TABLE ok_table (id int); SELECT * FROM no_such_table".into_sql_str(),
        false,
    )]);
    let got = db
        .migrate(Layer::Topology, Migrations::Embedded(&broken))
        .await;
    let Err(
        err @ StoreError::Migrate {
            layer: Layer::Topology,
            ..
        },
    ) = got
    else {
        return Err(Failure::Unexpected(format!("{got:?}")));
    };
    assert!(err.to_string().contains("topology"), "{err}");
    assert!(!table_exists(db.pool(), "topology", "ok_table").await?);
    assert_eq!(
        applied_versions(db.pool(), Layer::Topology).await?,
        Vec::<i64>::new()
    );
    db.close().await?;
    Ok(())
}

/// A layer's migrations can reference its own earlier tables unqualified,
/// and writes classify into typed failures layer crates can map.
#[tokio::test(flavor = "multi_thread")]
async fn layer_tables_produce_typed_violations() -> TestResult {
    let Some(db) = db("layer_tables_produce_typed_violations").await? else {
        return Ok(());
    };
    db.migrate(Layer::Canonical, Migrations::Directory(&fixture("beta")))
        .await?;
    let insert = "INSERT INTO canonical.items (id, weight) VALUES ($1, $2)";
    sqlx::query(insert)
        .bind(1_i64)
        .bind(5)
        .execute(db.pool())
        .await?;

    let dup = sqlx::query(insert)
        .bind(1_i64)
        .bind(5)
        .execute(db.pool())
        .await;
    let dup = dup.err().map(StoreError::from);
    assert_eq!(
        dup.as_ref().and_then(StoreError::failure),
        Some(&DbFailure::UniqueViolation {
            constraint: Some("items_pkey".to_owned())
        })
    );

    let check = sqlx::query(insert)
        .bind(2_i64)
        .bind(0)
        .execute(db.pool())
        .await;
    assert!(matches!(
        check.err().map(|e| crosstalk_store::classify(&e)),
        Some(DbFailure::CheckViolation { .. })
    ));

    let orphan = sqlx::query("INSERT INTO canonical.notes (item_id, body) VALUES (99, 'x')")
        .execute(db.pool())
        .await;
    assert!(matches!(
        orphan.err().map(|e| crosstalk_store::classify(&e)),
        Some(DbFailure::ForeignKeyViolation { .. })
    ));

    let missing = sqlx::query("SELECT * FROM canonical.nope")
        .execute(db.pool())
        .await;
    assert_eq!(
        missing.err().map(|e| crosstalk_store::classify(&e)),
        Some(DbFailure::Server {
            sqlstate: Some("42P01".to_owned())
        })
    );
    db.close().await?;
    Ok(())
}
