//! [`PgEvidence`]: the evidence page's records by id, read from the stores
//! that recorded them. It replaces `MemoryEvidence` (spans copied in from
//! the bus) in Postgres mode, so nothing the evidence page reads lives
//! only in memory.
//!
//! - **Spans** are L4's: `SpanIndex::spans` on `PgProvenanceStore`, the
//!   record of an originated or forwarded span (its location, author and
//!   exchange), rebuilt as `Originated`. The surface reads only a span's
//!   location (the excerpt), so a forwarded span's `Relayed` state, which
//!   the index does not keep, changes nothing it shows.
//! - **Accesses and resources** are L5's, as `PgChannelRegistry` recorded
//!   them: the `access` and `resource` columns of `flow.accesses` and
//!   `flow.resources` hold the spec values' wire JSON, read here by
//!   primary key in one statement each. This is a stopgap read of L5's
//!   schema: the spec's `AccessStore` (which "a store implementing
//!   `ChannelTraffic` implements") is not implemented by the Postgres
//!   registry yet, and the registry has no public read of a resource by
//!   id. Once `crosstalk-flow` has both, these two reads go through it.

use crosstalk_provenance::store::PgProvenanceStore;
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::resource::Resource;
use crosstalk_spec::derived::provenance::span::{Span, SpanState};
use crosstalk_spec::ids::{AccessId, ResourceId, SpanId};
use crosstalk_spec::interfaces::l4_provenance::SpanIndex;
use crosstalk_store::sqlx::{self, PgPool};
use crosstalk_surface::{EvidenceRecords, RecordReadError};
use serde::de::DeserializeOwned;

/// The evidence records over L4's span index and L5's tables. Clones
/// share the pool.
#[derive(Debug, Clone)]
pub struct PgEvidence {
    spans: PgProvenanceStore,
    pool: PgPool,
}

impl PgEvidence {
    /// Spans from `spans`; accesses and resources from `pool`'s `flow`
    /// schema.
    pub fn new(spans: PgProvenanceStore, pool: PgPool) -> Self {
        Self { spans, pool }
    }
}

fn store_error(what: &str, error: impl std::fmt::Display) -> RecordReadError {
    RecordReadError::Store {
        reason: format!("{what}: {error}"),
    }
}

/// The wire JSON in `column`, decoded; a value that does not decode is a
/// store fault (named, never echoed).
fn decode<T: DeserializeOwned>(column: &str, text: &str) -> Result<T, RecordReadError> {
    serde_json::from_str(text).map_err(|error| store_error(column, error))
}

impl EvidenceRecords for PgEvidence {
    async fn span(&self, id: SpanId) -> Result<Option<Span>, RecordReadError> {
        let batch = IdBatch::new([id])
            .map_err(|error| store_error("one id is a batch", format!("{error:?}")))?;
        let spans = self
            .spans
            .spans(&batch)
            .await
            .map_err(|error| store_error("reading the span", format!("{error:?}")))?;
        Ok(spans.get(&id).map(|indexed| Span {
            id,
            location: indexed.location,
            agent: indexed.author,
            exchange: indexed.exchange,
            state: SpanState::Originated,
        }))
    }

    async fn access(&self, id: AccessId) -> Result<Option<Access>, RecordReadError> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT access FROM flow.accesses WHERE id = $1")
                .bind(id.ulid_text())
                .fetch_optional(&self.pool)
                .await
                .map_err(|error| store_error("reading the access", error))?;
        row.map(|(text,)| decode("flow.accesses.access", &text))
            .transpose()
    }

    async fn resource(&self, id: ResourceId) -> Result<Option<Resource>, RecordReadError> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT resource FROM flow.resources WHERE id = $1")
                .bind(id.ulid_text())
                .fetch_optional(&self.pool)
                .await
                .map_err(|error| store_error("reading the resource", error))?;
        row.map(|(text,)| decode("flow.resources.resource", &text))
            .transpose()
    }
}
