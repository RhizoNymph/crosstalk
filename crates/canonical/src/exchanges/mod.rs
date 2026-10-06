//! L1's exchange store (`interfaces::l1_canonical::exchanges`): every
//! exchange capture announced, without its bodies, by id and by time.
//!
//! - [`MemoryExchanges`]: in memory, for a single-process gateway, the
//!   simulation and the reference the Postgres store is held to.
//! - [`PgExchanges`]: the `canonical` schema (`migrations/0001_exchanges.sql`).
//!
//! Both keep the first record of an id (`put` is idempotent) and list
//! newest first by (`started_at`, id). The list's cursors are
//! `<micros>-<last id>_<tag>`, the tag a keyed BLAKE3 over the store's key,
//! the query's window and the position, so a cursor this store did not
//! issue, or issued for another window, is `InvalidCursor`
//! (`canonical.exchange.store-read`).

mod pg;

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::ids::ExchangeId;
use crosstalk_spec::interfaces::l1_canonical::exchanges::{
    ExchangeQuery, ExchangeReads, ExchangeStore, ExchangeStoreError, StoredExchange,
};
use crosstalk_spec::paging::{Cursor, ExchangeList, Page, PageRequest, PageSize};
use crosstalk_spec::support::{Blake3, NonEmpty, Timestamp};

pub use pg::{MIGRATIONS, PgExchanges, migrate};

/// The key cursors are tagged with when a store is given none: a cursor
/// binds a query and a position in a list, not a secret.
pub const DEFAULT_CURSOR_KEY: [u8; 32] = [0; 32];

/// A stored exchange's place in the list.
type Key = (Timestamp, ExchangeId);

fn key_of(exchange: &StoredExchange) -> Key {
    (exchange.exchange.meta.started_at, exchange.id())
}

/// The query as the text a cursor binds.
fn binding(query: &ExchangeQuery) -> String {
    match query.window {
        None => "window=*".to_owned(),
        Some(window) => format!(
            "window={}..{}",
            window.start().as_micros(),
            window.end().as_micros()
        ),
    }
}

/// The list's cursors under one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Cursors {
    key: [u8; 32],
}

impl Cursors {
    pub(crate) fn new(key: [u8; 32]) -> Self {
        Self { key }
    }

    fn tag(&self, binding: &str, key: Key) -> String {
        let mut bytes = self.key.to_vec();
        bytes.extend_from_slice(binding.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(key.0.as_micros().to_string().as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(key.1.ulid_text().as_bytes());
        Blake3::of(&bytes).to_hex()[..32].to_owned()
    }

    fn issue(&self, binding: &str, key: Key) -> Result<Cursor<ExchangeList>, ExchangeStoreError> {
        let token = format!(
            "{}-{}_{}",
            key.0.as_micros(),
            key.1.ulid_text(),
            self.tag(binding, key)
        );
        Cursor::from_token(token).map_err(|error| ExchangeStoreError::Store {
            reason: format!("cursor not issued: {error:?}"),
        })
    }

    /// The position a cursor this store issued for `query` names.
    pub(crate) fn resume(
        &self,
        query: &ExchangeQuery,
        cursor: &Cursor<ExchangeList>,
    ) -> Result<Key, ExchangeStoreError> {
        let invalid = || ExchangeStoreError::InvalidCursor;
        let (position, given) = cursor.token().split_once('_').ok_or_else(invalid)?;
        let (micros, id) = position.split_once('-').ok_or_else(invalid)?;
        let micros: u64 = micros.parse().map_err(|_| invalid())?;
        let id = ExchangeId::from_ulid_text(id).map_err(|_| invalid())?;
        let key = (Timestamp::from_micros(micros), id);
        if self.tag(&binding(query), key) == given {
            Ok(key)
        } else {
            Err(invalid())
        }
    }

    /// One page of `rows`: every admitted exchange after the cursor,
    /// newest first, at least `size + 1` of them when there are more.
    pub(crate) fn page(
        &self,
        query: &ExchangeQuery,
        size: PageSize,
        mut rows: Vec<StoredExchange>,
    ) -> Result<Page<StoredExchange, ExchangeList>, ExchangeStoreError> {
        let limit = usize::from(size.get().get());
        let overflow = |_| ExchangeStoreError::Store {
            reason: "page larger than its size".to_owned(),
        };
        if rows.len() <= limit {
            return Page::last(size, rows).map_err(overflow);
        }
        rows.truncate(limit);
        let last = rows
            .last()
            .map(key_of)
            .ok_or_else(|| ExchangeStoreError::Store {
                reason: "empty page with more to follow".to_owned(),
            })?;
        let items = NonEmpty::from_vec(rows).ok_or_else(|| ExchangeStoreError::Store {
            reason: "empty page with more to follow".to_owned(),
        })?;
        Page::more(size, items, self.issue(&binding(query), last)?).map_err(overflow)
    }
}

/// The exchange store in memory. Clones are handles on one store; its
/// state sits behind a std `Mutex` held only for synchronous sections.
#[derive(Debug, Clone)]
pub struct MemoryExchanges {
    exchanges: Arc<Mutex<BTreeMap<ExchangeId, StoredExchange>>>,
    cursors: Cursors,
}

impl Default for MemoryExchanges {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryExchanges {
    /// An empty store, its cursors tagged with [`DEFAULT_CURSOR_KEY`].
    pub fn new() -> Self {
        Self {
            exchanges: Arc::default(),
            cursors: Cursors::new(DEFAULT_CURSOR_KEY),
        }
    }

    /// The same store, its cursors tagged with `key`.
    pub fn with_cursor_key(mut self, key: [u8; 32]) -> Self {
        self.cursors = Cursors::new(key);
        self
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<ExchangeId, StoredExchange>> {
        // Every write inserts one whole record, so a poisoned lock still
        // guards a consistent map.
        self.exchanges
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl ExchangeStore for MemoryExchanges {
    async fn put(&mut self, exchange: StoredExchange) -> Result<(), ExchangeStoreError> {
        self.lock().entry(exchange.id()).or_insert(exchange);
        Ok(())
    }
}

impl ExchangeReads for MemoryExchanges {
    async fn exchanges(
        &self,
        ids: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, StoredExchange>, ExchangeStoreError> {
        let stored = self.lock();
        Ok(ids
            .ids()
            .iter()
            .filter_map(|id| stored.get(id).map(|exchange| (*id, exchange.clone())))
            .collect())
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
        let mut rows: Vec<StoredExchange> = self
            .lock()
            .values()
            .filter(|exchange| query.admits(exchange))
            .filter(|exchange| after.is_none_or(|after| key_of(exchange) < after))
            .cloned()
            .collect();
        rows.sort_by_key(|exchange| std::cmp::Reverse(key_of(exchange)));
        rows.truncate(usize::from(page.size.get().get()) + 1);
        self.cursors.page(query, page.size, rows)
    }
}
