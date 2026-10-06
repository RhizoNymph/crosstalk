//! [`PgExchanges`]: L1's exchange store in the `canonical` schema. `put`
//! inserts the record once (`ON CONFLICT DO NOTHING`); the list reads the
//! window's exchanges after the cursor, newest first, from the
//! `(started_at, id)` index.

use std::collections::BTreeMap;

use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::ids::ExchangeId;
use crosstalk_spec::interfaces::l1_canonical::exchanges::{
    ExchangeQuery, ExchangeReads, ExchangeStore, ExchangeStoreError, StoredExchange,
};
use crosstalk_spec::paging::{ExchangeList, Page, PageRequest};
use crosstalk_spec::support::Timestamp;
use crosstalk_store::{Layer, Migrations, StoreError};
use sqlx::PgPool;

use super::{Cursors, DEFAULT_CURSOR_KEY};

/// L1's migrations, embedded.
pub static MIGRATIONS: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Run L1's migrations in the `canonical` schema.
pub async fn migrate(pool: &PgPool) -> Result<(), StoreError> {
    crosstalk_store::migrate(pool, Layer::Canonical, Migrations::Embedded(&MIGRATIONS)).await
}

fn failed(what: &str, reason: impl std::fmt::Display) -> ExchangeStoreError {
    ExchangeStoreError::Store {
        reason: format!("{what}: {reason}"),
    }
}

fn micros(at: Timestamp) -> Result<i64, ExchangeStoreError> {
    i64::try_from(at.as_micros())
        .map_err(|_| failed("time beyond the stored range", at.as_micros()))
}

fn record_of(text: &str) -> Result<StoredExchange, ExchangeStoreError> {
    serde_json::from_str(text).map_err(|error| failed("stored exchange unreadable", error))
}

/// The exchange store on Postgres. Clones share the pool.
#[derive(Debug, Clone)]
pub struct PgExchanges {
    pool: PgPool,
    cursors: Cursors,
}

impl PgExchanges {
    /// The store on `pool`, whose L1 migrations have run, its cursors
    /// tagged with [`DEFAULT_CURSOR_KEY`].
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            cursors: Cursors::new(DEFAULT_CURSOR_KEY),
        }
    }

    /// The same store, its cursors tagged with `key`: every node serving
    /// one list shares it.
    pub fn with_cursor_key(mut self, key: [u8; 32]) -> Self {
        self.cursors = Cursors::new(key);
        self
    }
}

impl ExchangeStore for PgExchanges {
    async fn put(&mut self, exchange: StoredExchange) -> Result<(), ExchangeStoreError> {
        let record = serde_json::to_string(&exchange)
            .map_err(|error| failed("exchange unencodable", error))?;
        sqlx::query(
            "INSERT INTO canonical.exchanges (id, started_at, record) VALUES ($1, $2, $3) \
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(exchange.id().ulid_text())
        .bind(micros(exchange.exchange.meta.started_at)?)
        .bind(record)
        .execute(&self.pool)
        .await
        .map_err(|error| failed("exchange not stored", error))?;
        Ok(())
    }
}

impl ExchangeReads for PgExchanges {
    async fn exchanges(
        &self,
        ids: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, StoredExchange>, ExchangeStoreError> {
        let wanted: Vec<String> = ids.ids().iter().map(|id| id.ulid_text()).collect();
        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT record FROM canonical.exchanges WHERE id = ANY($1)")
                .bind(&wanted)
                .fetch_all(&self.pool)
                .await
                .map_err(|error| failed("exchanges unreadable", error))?;
        let mut read = BTreeMap::new();
        for (record,) in rows {
            let exchange = record_of(&record)?;
            read.insert(exchange.id(), exchange);
        }
        Ok(read)
    }

    async fn list(
        &self,
        query: &ExchangeQuery,
        page: &PageRequest<ExchangeList>,
    ) -> Result<Page<StoredExchange, ExchangeList>, ExchangeStoreError> {
        let after = page
            .after
            .as_ref()
            .map(|cursor| self.cursors.resume(query, cursor))
            .transpose()?;
        let (start, end) = match query.window {
            Some(window) => (Some(micros(window.start())?), Some(micros(window.end())?)),
            None => (None, None),
        };
        let (after_at, after_id) = match after {
            Some((at, id)) => (Some(micros(at)?), Some(id.ulid_text())),
            None => (None, None),
        };
        let limit = i64::from(page.size.get().get()) + 1;
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT record FROM canonical.exchanges \
             WHERE ($1::bigint IS NULL OR started_at >= $1) \
               AND ($2::bigint IS NULL OR started_at < $2) \
               AND ($3::bigint IS NULL OR (started_at, id) < ($3, $4::text)) \
             ORDER BY started_at DESC, id DESC LIMIT $5",
        )
        .bind(start)
        .bind(end)
        .bind(after_at)
        .bind(after_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|error| failed("exchanges unreadable", error))?;
        let rows = rows
            .iter()
            .map(|(record,)| record_of(record))
            .collect::<Result<Vec<_>, _>>()?;
        self.cursors.page(query, page.size, rows)
    }
}
