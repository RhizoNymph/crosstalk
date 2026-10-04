//! Required extensions: created once, idempotent, usable from layer schemas.

use crosstalk_store::{Extension, Layer, Migrations};

use crate::{TestResult, db, fixture};

#[tokio::test(flavor = "multi_thread")]
async fn ensure_extensions_creates_vector_and_pg_trgm() -> TestResult {
    let Some(db) = db("ensure_extensions_creates_vector_and_pg_trgm").await? else {
        return Ok(());
    };
    // template0 databases start without either extension.
    let before: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_extension WHERE extname IN ('vector', 'pg_trgm')",
    )
    .fetch_one(db.pool())
    .await?;
    assert_eq!(before, 0);

    let installed = db.ensure_extensions().await?;
    let names: Vec<Extension> = installed.iter().map(|i| i.extension).collect();
    assert_eq!(names, Extension::REQUIRED);
    assert!(installed.iter().all(|i| !i.version.is_empty()));

    // Idempotent: a second run reports the same versions.
    let again = db.ensure_extensions().await?;
    assert_eq!(again, installed);

    let schemas: Vec<String> = sqlx::query_scalar(
        "SELECT n.nspname::text FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace \
         WHERE e.extname IN ('vector', 'pg_trgm') ORDER BY e.extname",
    )
    .fetch_all(db.pool())
    .await?;
    assert_eq!(schemas, ["public", "public"]);
    db.close().await?;
    Ok(())
}

/// Parallel `ensure_extensions` calls in one database all succeed (the
/// catalog race on `CREATE EXTENSION IF NOT EXISTS` is absorbed).
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_ensure_extensions_all_succeed() -> TestResult {
    let Some(db) = db("concurrent_ensure_extensions_all_succeed").await? else {
        return Ok(());
    };
    let mut runs = tokio::task::JoinSet::new();
    for _ in 0..4 {
        let store = db.store();
        runs.spawn(async move { store.ensure_extensions().await });
    }
    while let Some(joined) = runs.join_next().await {
        assert_eq!(joined??.len(), Extension::REQUIRED.len());
    }
    db.close().await?;
    Ok(())
}

/// A layer migration uses `vector(…)` and `gin_trgm_ops` unqualified: the
/// runner's search_path reaches the extensions in `public`.
#[tokio::test(flavor = "multi_thread")]
async fn layer_migrations_see_the_extensions() -> TestResult {
    let Some(db) = db("layer_migrations_see_the_extensions").await? else {
        return Ok(());
    };
    db.ensure_extensions().await?;
    db.migrate(Layer::Analysis, Migrations::Directory(&fixture("search")))
        .await?;
    sqlx::query(
        "INSERT INTO analysis.embeddings (id, label, embedding) VALUES \
         (1, 'shared wiki page', '[1,0,0]'), (2, 'unrelated', '[0,1,0]')",
    )
    .execute(db.pool())
    .await?;
    let nearest: i64 = sqlx::query_scalar(
        "SELECT id FROM analysis.embeddings ORDER BY embedding <-> '[0.9,0.1,0]' LIMIT 1",
    )
    .fetch_one(db.pool())
    .await?;
    assert_eq!(nearest, 1);
    let fuzzy: i64 = sqlx::query_scalar(
        "SELECT id FROM analysis.embeddings WHERE label % 'shared wiki pages' LIMIT 1",
    )
    .fetch_one(db.pool())
    .await?;
    assert_eq!(fuzzy, 1);
    db.close().await?;
    Ok(())
}
