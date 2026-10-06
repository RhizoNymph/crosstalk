//! `ProvenanceReads` and `SpanIndex` on `PgProvenanceStore`: the
//! scenarios of `tests::reads`.

use crate::store::PgProvenanceStore;
use crate::tests::reads::{
    matches_read_in_every_match, output_spans_keep_every_origin, readers_newest_first,
    status_follows_the_commit,
};

use super::database;

/// INV-1023 on Postgres.
#[tokio::test(flavor = "multi_thread")]
async fn pg_scan_status_follows_the_commit() {
    let Some(db) = database("pg_scan_status_follows_the_commit").await else {
        return;
    };
    status_follows_the_commit(&mut PgProvenanceStore::new(db.pool().clone())).await;
}

/// INV-1014 on Postgres: relayed spans are kept with their source.
#[tokio::test(flavor = "multi_thread")]
async fn pg_output_spans_keep_every_origin() {
    let Some(db) = database("pg_output_spans_keep_every_origin").await else {
        return;
    };
    output_spans_keep_every_origin(&mut PgProvenanceStore::new(db.pool().clone())).await;
}

/// INV-1012 on Postgres.
#[tokio::test(flavor = "multi_thread")]
async fn pg_matches_read_in_every_match() {
    let Some(db) = database("pg_matches_read_in_every_match").await else {
        return;
    };
    matches_read_in_every_match(&mut PgProvenanceStore::new(db.pool().clone())).await;
}

/// INV-1015 on Postgres.
#[tokio::test(flavor = "multi_thread")]
async fn pg_readers_agree_with_memory() {
    let Some(db) = database("pg_readers_agree_with_memory").await else {
        return;
    };
    readers_newest_first(&mut PgProvenanceStore::new(db.pool().clone())).await;
}
