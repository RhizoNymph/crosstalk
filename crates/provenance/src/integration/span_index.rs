//! `SpanIndex::record` on `PgProvenanceStore` (INV-965): the scenarios of
//! `tests::span_index`.

use crate::store::PgProvenanceStore;
use crate::tests::span_index::{
    committed_then_recorded_keeps_the_commit, recorded_without_a_scan_reads_back,
};

use super::database;

/// INV-965 on Postgres, for a span no scan committed: the world seed's
/// origin spans, which the evidence page reads back.
#[tokio::test(flavor = "multi_thread")]
async fn pg_recorded_span_reads_back_without_a_scan() {
    let Some(db) = database("pg_recorded_span_reads_back_without_a_scan").await else {
        return;
    };
    recorded_without_a_scan_reads_back(&mut PgProvenanceStore::new(db.pool().clone())).await;
}

/// INV-965 on Postgres, for a span a scan committed first.
#[tokio::test(flavor = "multi_thread")]
async fn pg_committed_span_keeps_its_record() {
    let Some(db) = database("pg_committed_span_keeps_its_record").await else {
        return;
    };
    committed_then_recorded_keeps_the_commit(&mut PgProvenanceStore::new(db.pool().clone())).await;
}
