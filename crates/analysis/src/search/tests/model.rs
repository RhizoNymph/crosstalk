//! The memory crate's search harness against [`PgSearchIndex`]: random
//! indexing, removals, verdicts, merges, supersessions, re-fits,
//! assignments, searches and samples. Semantic traversals and samples
//! match the reference exactly; text and hybrid traversals resolve the same
//! version and are in rank order, within their page size and window.

use crosstalk_memory::model::HarnessConfig;
use crosstalk_memory::model::analysis::{ReferenceSearch, SearchWorld, check_search_index};
use crosstalk_spec::aggregates::topic::EmbeddingModel;
use crosstalk_store::DatabaseUrl;

use super::Subject;
use crate::pg::testing::{case_pool, database, off_runtime};

async fn make(url: DatabaseUrl, model: EmbeddingModel, world: SearchWorld) -> Subject {
    let pool = case_pool(&url).await;
    // The catalog configured as the reference's (keep two activated
    // versions, lineage floor 0.5), version 0 only.
    let catalog = ReferenceSearch::new(model.clone(), world.clone())
        .unwrap_or_else(|error| panic!("a catalog: {error:?}"))
        .catalog;
    Subject::new(pool, model, world.directory, world.watermark, catalog).await
}

#[tokio::test(flavor = "multi_thread")]
async fn search_index_matches_the_reference() {
    let Some(db) = database("search_index_matches_the_reference").await else {
        return;
    };
    let url = db.url().clone();
    let harness = HarnessConfig {
        cases: 24,
        max_ops: 30,
    };
    let result = off_runtime(move || {
        check_search_index(harness, move |model, world| make(url.clone(), model, world))
    });
    if let Err(mismatch) = result {
        panic!("{mismatch}");
    }
}
