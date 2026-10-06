//! [`PgSinkRegistry`]: the configured alert sinks and how each one's last
//! delivery went, in `surface.sinks` (one `SinkInfo` wire JSON per sink).
//!
//! ```text
//! configure(sinks) ─ txn: DELETE sinks config no longer defines; UPSERT each defined one
//!                         (its last delivery kept while its kind is unchanged)
//! record_delivery  ─ txn: read the sink (UnknownSink if config does not define it), replace its last delivery
//! sinks            ─ every row, by id
//! ```
//!
//! The sinks come from config at start ([`PgSinkRegistry::configure`]);
//! deliveries are recorded by the component that delivers alerts. A
//! restart keeps each sink's last delivery.

use std::collections::BTreeMap;
use std::sync::Arc;

use crosstalk_spec::ids::SinkId;
use crosstalk_spec::interfaces::l8_surface::sinks::{SinkRegistry, SinkRegistryError};
use crosstalk_spec::interfaces::l8_surface::{SinkError, SinkInfo, SinkKind};
use crosstalk_spec::support::Timestamp;
use crosstalk_store::{SerializableRetry, TxError, retry_serializable};
use sqlx::{PgConnection, PgPool, Row};

use super::codec::{CodecError, from_json, to_json};
use super::{StorageFailure, settle};

/// One sink config defines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkConfig {
    pub id: SinkId,
    pub kind: SinkKind,
    pub name: String,
}

/// The configured sinks on Postgres. Cloning shares the pool.
#[derive(Debug, Clone)]
pub struct PgSinkRegistry {
    pool: PgPool,
    retry: SerializableRetry,
}

fn store_error(failure: &StorageFailure) -> SinkRegistryError {
    SinkRegistryError::Store {
        reason: failure.reason(),
    }
}

fn codec(error: CodecError) -> TxError<SinkRegistryError> {
    TxError::Abort(store_error(&StorageFailure::Codec(error)))
}

async fn stored(conn: &mut PgConnection) -> Result<Vec<SinkInfo>, TxError<SinkRegistryError>> {
    let rows = sqlx::query("SELECT info FROM surface.sinks ORDER BY id")
        .fetch_all(&mut *conn)
        .await?;
    let mut infos = Vec::with_capacity(rows.len());
    for row in &rows {
        let text: String = row.try_get("info")?;
        infos.push(from_json("sink", &text).map_err(codec)?);
    }
    Ok(infos)
}

async fn save(conn: &mut PgConnection, info: &SinkInfo) -> Result<(), TxError<SinkRegistryError>> {
    let text = to_json("sink", info).map_err(codec)?;
    sqlx::query(
        "INSERT INTO surface.sinks (id, info) VALUES ($1, $2) \
         ON CONFLICT (id) DO UPDATE SET info = EXCLUDED.info",
    )
    .bind(info.id.ulid_text())
    .bind(text)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

impl PgSinkRegistry {
    /// The registry in `pool`'s database (migrated with
    /// [`super::run_migrations`]), as last configured. For a process that
    /// only reads or records deliveries; the one that loads config calls
    /// [`PgSinkRegistry::configure`].
    pub fn open(pool: PgPool, retry: SerializableRetry) -> Self {
        Self { pool, retry }
    }

    /// The registry configured with `sinks`, in one transaction: sinks
    /// config no longer defines are removed, the others stored. A sink
    /// listed twice keeps its last definition. A sink whose kind is
    /// unchanged keeps its last delivery; a new one, or one whose kind
    /// changed, has none.
    pub async fn configure(
        pool: PgPool,
        retry: SerializableRetry,
        sinks: impl IntoIterator<Item = SinkConfig>,
    ) -> Result<Self, SinkRegistryError> {
        let defined: Arc<BTreeMap<SinkId, SinkConfig>> =
            Arc::new(sinks.into_iter().map(|sink| (sink.id, sink)).collect());
        let registry = Self::open(pool, retry);
        retry_serializable(&registry.pool, &registry.retry, |conn| {
            let defined = Arc::clone(&defined);
            Box::pin(async move {
                let before: BTreeMap<SinkId, SinkInfo> = stored(conn)
                    .await?
                    .into_iter()
                    .map(|info| (info.id, info))
                    .collect();
                let removed: Vec<String> = before
                    .keys()
                    .filter(|id| !defined.contains_key(id))
                    .map(|id| id.ulid_text())
                    .collect();
                sqlx::query("DELETE FROM surface.sinks WHERE id = ANY($1::text[])")
                    .bind(removed)
                    .execute(&mut *conn)
                    .await?;
                for sink in defined.values() {
                    let last_delivery = before
                        .get(&sink.id)
                        .filter(|info| info.kind == sink.kind)
                        .and_then(|info| info.last_delivery.clone());
                    let info = SinkInfo {
                        id: sink.id,
                        kind: sink.kind,
                        name: sink.name.clone(),
                        last_delivery,
                    };
                    save(conn, &info).await?;
                }
                Ok(())
            })
        })
        .await
        .map_err(|error| settle(error, |failure| store_error(&failure)))?;
        tracing::info!(sinks = defined.len(), "alert sinks configured");
        Ok(registry)
    }
}

impl SinkRegistry for PgSinkRegistry {
    async fn record_delivery(
        &mut self,
        sink: SinkId,
        outcome: Result<Timestamp, SinkError>,
    ) -> Result<(), SinkRegistryError> {
        let outcome = Arc::new(outcome);
        retry_serializable(&self.pool, &self.retry, |conn| {
            let outcome = Arc::clone(&outcome);
            Box::pin(async move {
                let row = sqlx::query("SELECT info FROM surface.sinks WHERE id = $1")
                    .bind(sink.ulid_text())
                    .fetch_optional(&mut *conn)
                    .await?;
                let Some(row) = row else {
                    return Err(TxError::Abort(SinkRegistryError::UnknownSink(sink)));
                };
                let text: String = row.try_get("info")?;
                let mut info: SinkInfo = from_json("sink", &text).map_err(codec)?;
                info.last_delivery = Some((*outcome).clone());
                save(conn, &info).await
            })
        })
        .await
        .map_err(|error| settle(error, |failure| store_error(&failure)))
    }

    async fn sinks(&self) -> Result<Vec<SinkInfo>, SinkRegistryError> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|error| store_error(&StorageFailure::Query(error)))?;
        stored(&mut conn).await.map_err(|error| match error {
            TxError::Abort(refused) => refused,
            TxError::Db(error) => store_error(&StorageFailure::Query(error)),
        })
    }
}
