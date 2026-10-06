//! Every layer's migrations, as the gateway runs and checks them.
//!
//! [`LAYERS`] lists each layer crate's embedded migrations in the order
//! `crosstalk migrate` runs them: `transport` first (the bus the others
//! publish on), then the stack in order. `ingress` has none.
//!
//! [`check_heads`] reads each layer's `"<layer>"._sqlx_migrations` and
//! compares its highest applied version with the binary's embedded head. A
//! layer behind (a missing table counts as nothing applied) is reported by
//! name, as `/readyz` shows it: `flow 1 < 2`.

use crosstalk_flow::consumer::{FlowConfig, InvalidFlowConfig, Settings as FlowSettings};
use crosstalk_flow::store::PgFlowDurability;
use crosstalk_store::sqlx::PgPool;
use crosstalk_store::sqlx::migrate::Migrator;
use crosstalk_store::sqlx::{self};
use crosstalk_store::{Layer, Migrations, SerializableRetry, Store, StoreError, migrate};
use crosstalk_spec::support::Timestamp;

use super::MigrateError;

/// One layer's embedded migrations.
#[derive(Debug, Clone, Copy)]
pub struct LayerMigrations {
    pub layer: Layer,
    pub migrator: &'static Migrator,
}

impl LayerMigrations {
    /// The highest embedded version (0 with none).
    pub fn head(&self) -> i64 {
        self.migrator
            .iter()
            .map(|migration| migration.version)
            .max()
            .unwrap_or(0)
    }
}

/// Every layer with migrations, in the order they run.
pub static LAYERS: [LayerMigrations; 8] = [
    LayerMigrations {
        layer: Layer::Transport,
        migrator: &crosstalk_transport::pg::MIGRATIONS,
    },
    LayerMigrations {
        layer: Layer::Canonical,
        migrator: &crosstalk_canonical::exchanges::MIGRATIONS,
    },
    LayerMigrations {
        layer: Layer::Reconstruct,
        migrator: &crosstalk_reconstruct::agents::MIGRATIONS,
    },
    LayerMigrations {
        layer: Layer::Provenance,
        migrator: &crosstalk_provenance::store::MIGRATIONS,
    },
    LayerMigrations {
        layer: Layer::Flow,
        migrator: &crosstalk_flow::store::MIGRATIONS,
    },
    LayerMigrations {
        layer: Layer::Analysis,
        migrator: &crosstalk_analysis::pg::MIGRATIONS,
    },
    LayerMigrations {
        layer: Layer::Topology,
        migrator: &crosstalk_topology::store::MIGRATIONS,
    },
    LayerMigrations {
        layer: Layer::Surface,
        migrator: &crosstalk_surface::pg::MIGRATIONS,
    },
];

/// Ensure the extensions, then run every layer's migrations in
/// [`LAYERS`] order. Idempotent: applied migrations are checked against
/// their checksums and skipped.
pub async fn migrate_all(store: &Store) -> Result<(), StoreError> {
    for installed in store.ensure_extensions().await? {
        tracing::info!(extension = %installed.extension, version = %installed.version, "extension ready");
    }
    for layer in &LAYERS {
        migrate(
            store.pool(),
            layer.layer,
            Migrations::Embedded(layer.migrator),
        )
        .await?;
        tracing::info!(layer = %layer.layer, head = layer.head(), "layer migrations at head");
    }
    Ok(())
}

/// One layer whose applied migrations are behind the binary's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Behind {
    pub layer: Layer,
    pub applied: i64,
    pub head: i64,
}

impl std::fmt::Display for Behind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} < {}", self.layer, self.applied, self.head)
    }
}

/// `behind` as `/readyz` shows it: `flow 1 < 2, surface 0 < 1`.
pub fn describe(behind: &[Behind]) -> String {
    behind
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// The highest version applied successfully in `layer`'s migrations
/// table; 0 when the table does not exist.
async fn applied(pool: &PgPool, layer: Layer) -> Result<i64, StoreError> {
    let (exists,): (bool,) = sqlx::query_as("SELECT to_regclass($1) IS NOT NULL")
        .bind(layer.migrations_table())
        .fetch_one(pool)
        .await?;
    if !exists {
        return Ok(0);
    }
    // The table name is the layer's fixed identifier (see `Layer`).
    let statement = format!(
        "SELECT coalesce(max(version), 0) FROM {} WHERE success",
        layer.migrations_table()
    );
    let (version,): (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(statement))
        .fetch_one(pool)
        .await?;
    Ok(version)
}

/// Every layer whose applied migrations are behind its embedded head, in
/// [`LAYERS`] order; empty when the database is at head.
pub async fn check_heads(pool: &PgPool) -> Result<Vec<Behind>, StoreError> {
    let mut behind = Vec::new();
    for layer in &LAYERS {
        let applied = applied(pool, layer.layer).await?;
        let head = layer.head();
        if applied < head {
            behind.push(Behind {
                layer: layer.layer,
                applied,
                head,
            });
        }
    }
    Ok(behind)
}

/// `crosstalk migrate --reset-correlator`: replace L5's checkpoint with
/// empty shards at the latest stored tick (`PgFlowDurability::reset_correlator`).
pub async fn reset_correlator(
    pool: &PgPool,
    flow: FlowConfig,
    now: Timestamp,
) -> Result<(), MigrateError> {
    let settings =
        FlowSettings::try_from(flow).map_err(|error: InvalidFlowConfig| MigrateError::Flow(error))?;
    PgFlowDurability::new(pool.clone(), SerializableRetry::default())
        .reset_correlator(&settings, now)
        .await?;
    tracing::warn!(
        shards = settings.shards.get(),
        "correlator reset: the pairings pending at the last checkpoint are lost"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_runs_first_and_every_layer_has_a_head() {
        assert_eq!(LAYERS[0].layer, Layer::Transport);
        for layer in &LAYERS {
            assert!(layer.head() > 0, "{} has migrations", layer.layer);
        }
        let mut seen: Vec<Layer> = LAYERS.iter().map(|layer| layer.layer).collect();
        seen.dedup();
        assert_eq!(seen.len(), LAYERS.len());
    }

    #[test]
    fn behind_reads_as_readyz_shows_it() {
        let text = describe(&[
            Behind {
                layer: Layer::Flow,
                applied: 1,
                head: 2,
            },
            Behind {
                layer: Layer::Surface,
                applied: 0,
                head: 1,
            },
        ]);
        assert_eq!(text, "flow 1 < 2, surface 0 < 1");
    }
}
