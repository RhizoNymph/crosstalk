//! L1's exchange store: every exchange capture announced, kept as
//! [`StoredExchange`] (the [`Exchange`] record and its normalizer
//! warnings, without the bodies, which live in the blob store under the
//! hashes the record names), and read back by id or by time.
//!
//! Capture writes an exchange here ([`ExchangeStore::put`]) before it
//! publishes `ExchangeCaptured`, so a reader that sees the event, or any
//! later record naming the exchange (a threading outcome, a span, a match),
//! finds it stored (`canonical.exchange.store-read`). It replaces the
//! gateway's append-only exchange log stopgap.
//!
//! The record is immutable: `put` of an id already stored keeps the first
//! record. Where L3 threaded an exchange is L3's
//! (`ConversationReads::locate`), not a field of this record.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::batch::IdBatch;
use crate::ids::ExchangeId;
use crate::interfaces::l1_canonical::NormalizeWarning;
use crate::observed::exchange::Exchange;
use crate::paging::{ExchangeList, Page, PageRequest};
use crate::support::TimeWindow;

#[cfg(doc)]
use crate::interfaces::l3_reconstruction::conversations::ConversationReads;

/// One stored exchange: `NormalizedExchange` without the bodies.
/// `exchange.meta.client` is the full `ClientContext` (ingress, upstream,
/// claims, harness ids). On the wire `{"exchange": .., "warnings": [..]}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct StoredExchange {
    pub exchange: Exchange,
    pub warnings: Vec<NormalizeWarning>,
}

impl StoredExchange {
    pub fn id(&self) -> ExchangeId {
        self.exchange.meta.id
    }
}

/// Which stored exchanges [`ExchangeReads::list`] returns. A store input,
/// never on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ExchangeQuery {
    /// `ExchangeMeta::started_at` lies in it; `None` admits every time.
    pub window: Option<TimeWindow>,
}

impl ExchangeQuery {
    /// Whether `exchange` is one the query admits.
    pub fn admits(&self, exchange: &StoredExchange) -> bool {
        self.window
            .is_none_or(|window| window.contains(exchange.exchange.meta.started_at))
    }
}

/// The exchange store's write, called by capture.
pub trait ExchangeStore {
    /// Store `exchange`. Idempotent by id: an id already stored keeps its
    /// first record and nothing changes, so a redelivered capture stores
    /// nothing new. Publishes nothing: capture publishes
    /// `ExchangeCaptured` once this returns.
    fn put(
        &mut self,
        exchange: StoredExchange,
    ) -> impl Future<Output = Result<(), ExchangeStoreError>> + Send;
}

/// Reads of stored exchanges, each as `put` stored it.
pub trait ExchangeReads {
    /// The stored exchanges of `ids`, read in one snapshot; an id never
    /// stored is absent, so the map's keys are a subset of `ids`. The batch
    /// is at most `IdBatch::MAX` ids by construction.
    fn exchanges(
        &self,
        ids: &IdBatch<ExchangeId>,
    ) -> impl Future<Output = Result<BTreeMap<ExchangeId, StoredExchange>, ExchangeStoreError>> + Send;

    /// The exchanges `query` admits, newest first by (`started_at`,
    /// `ExchangeId`) descending; a keyset traversal returns each admitted
    /// exchange stored throughout it exactly once. The cursor binds the
    /// query: one presented with another query, or one this store did not
    /// issue, is `InvalidCursor`.
    fn list(
        &self,
        query: &ExchangeQuery,
        page: &PageRequest<ExchangeList>,
    ) -> impl Future<Output = Result<Page<StoredExchange, ExchangeList>, ExchangeStoreError>> + Send;
}

/// Why an exchange store call failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExchangeStoreError {
    /// A store failure; a retry may succeed.
    Store { reason: String },
    /// A `list` cursor this store did not issue for this query.
    InvalidCursor,
}
