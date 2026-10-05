//! The per-layer migration runner.
//!
//! Each layer crate owns `crates/<layer>/migrations/` and embeds it with
//! `sqlx::migrate!("./migrations")` (the macro needs a path relative to the
//! calling crate's manifest, so the embedding happens in the layer crate).
//! This module only runs them: inside the layer's own schema, recording
//! applied versions in the layer's own `"<layer>"."_sqlx_migrations"`, so
//! version numbers never collide across layers.

use std::path::Path;

use sqlx::migrate::{Migration, Migrator};
use sqlx::{AssertSqlSafe, PgPool};
use tracing::info;

use crate::error::StoreError;
use crate::layer::Layer;

/// Where a layer's migrations come from.
#[derive(Debug, Clone, Copy)]
pub enum Migrations<'a> {
    /// Embedded at compile time with `sqlx::migrate!("./migrations")` in the
    /// layer crate (the normal case).
    Embedded(&'a Migrator),
    /// Read from a directory at run time.
    Directory(&'a Path),
}

/// Runs `layer`'s pending migrations in its schema.
///
/// - The schema is created if missing, and so is the layer's migrations
///   table, `"<layer>"."_sqlx_migrations"`.
/// - Every migration runs with `search_path` set to the layer's schema and
///   then `public`, so unqualified DDL lands in the layer's schema while the
///   shared extensions stay visible.
/// - Applied migrations are checked against their recorded checksums: an
///   edited, already-applied migration fails with
///   [`sqlx::migrate::MigrateError::VersionMismatch`].
/// - The runner holds sqlx's per-database advisory lock, so concurrent
///   runners (several layers at start-up, or several gateway nodes)
///   serialize instead of racing.
///
/// The connection used is taken from `pool` and closed afterwards rather
/// than returned, so its `search_path` never leaks into other queries.
pub async fn migrate(
    pool: &PgPool,
    layer: Layer,
    migrations: Migrations<'_>,
) -> Result<(), StoreError> {
    let migrate_err = |source| StoreError::Migrate { layer, source };
    let resolved: Vec<Migration> = match migrations {
        Migrations::Embedded(migrator) => migrator.iter().cloned().collect(),
        Migrations::Directory(dir) => Migrator::new(dir.to_path_buf())
            .await
            .map_err(migrate_err)?
            .iter()
            .cloned()
            .collect(),
    };
    let count = resolved.len();
    let migrator = layer_migrator(layer, resolved);

    let mut conn = pool.acquire().await?;
    conn.close_on_drop();
    let set_path = format!("SET search_path TO {}", layer.search_path());
    // The layer's schema name is a fixed lowercase identifier (see `Layer`).
    sqlx::raw_sql(AssertSqlSafe(set_path))
        .execute(&mut *conn)
        .await?;
    // `run_direct` rather than `run`: `run`'s `Acquire<'a>` bound makes the
    // future fail the `Send + 'static` check `tokio::spawn` needs (rustc
    // #100013); sqlx exposes `run_direct` for exactly this.
    migrator
        .run_direct(None, &mut *conn, false)
        .await
        .map_err(migrate_err)?;
    info!(
        layer = layer.schema(),
        migrations = count,
        table = %layer.migrations_table(),
        "layer migrations up to date"
    );
    Ok(())
}

/// A migrator for `layer`: its schema created on demand and its own
/// migrations table inside that schema.
fn layer_migrator(layer: Layer, migrations: Vec<Migration>) -> Migrator {
    let mut migrator = Migrator::with_migrations(migrations);
    migrator.dangerous_set_table_name(layer.migrations_table());
    migrator.create_schema(layer.quoted_schema());
    migrator
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layer_migrator_uses_the_layer_schema_and_table() {
        let m = layer_migrator(Layer::Flow, Vec::new());
        assert_eq!(m.table_name, "\"flow\".\"_sqlx_migrations\"");
        assert_eq!(m.create_schemas.len(), 1);
        assert_eq!(m.create_schemas[0], "\"flow\"");
        assert!(m.locking, "the runner must hold the advisory lock");
    }

    fn assert_spawnable<F: std::future::Future + Send + 'static>(_: F) {}

    /// Layer crates run migrations from spawned start-up tasks, so the
    /// future must be `Send + 'static` once it owns its inputs. Never polled.
    #[tokio::test]
    async fn migrate_future_is_spawnable() -> Result<(), sqlx::Error> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://nobody@127.0.0.1:1/none")?;
        let dir = std::path::PathBuf::from("migrations");
        assert_spawnable(
            async move { migrate(&pool, Layer::Flow, Migrations::Directory(&dir)).await },
        );
        Ok(())
    }

    #[test]
    fn layer_migrators_never_share_a_table() {
        let a = layer_migrator(Layer::Ingress, Vec::new());
        let b = layer_migrator(Layer::Canonical, Vec::new());
        assert_ne!(a.table_name, b.table_name);
    }
}
