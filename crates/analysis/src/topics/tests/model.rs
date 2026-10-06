//! The memory crate's catalog harness against [`PgTopicCatalog`]: random
//! fit lifecycles, assignments (redelivered ones included), pins and
//! retention give the same results and leave the same history, sizes,
//! lineage and topics as the reference catalog, and the sizes agree with an
//! independent count of the assignments.

use std::sync::Arc;

use crosstalk_memory::analysis::aliases::StaticDirectory;
use crosstalk_memory::analysis::catalog::CatalogConfig as ReferenceConfig;
use crosstalk_memory::model::HarnessConfig;
use crosstalk_memory::model::analysis::check_topic_catalog;
use crosstalk_memory::model::build::ts;
use crosstalk_store::DatabaseUrl;

use super::super::{CatalogConfig, CatalogParts, PgTopicCatalog};
use crate::pg::testing::{CURSOR_KEY, DiscardSink, case_pool, database, off_runtime, retry};

type Subject = PgTopicCatalog<StaticDirectory, DiscardSink>;

/// A fresh catalog over emptied tables for one harness case.
async fn make(url: DatabaseUrl, reference: ReferenceConfig) -> Subject {
    let pool = case_pool(&url).await;
    PgTopicCatalog::open(
        pool,
        CatalogConfig {
            retention: reference.retention,
            lineage_floor: reference.lineage_floor,
        },
        ts(0),
        CatalogParts {
            agents: StaticDirectory::new(),
            sink: Arc::new(DiscardSink),
            cursor_key: CURSOR_KEY,
            retry: retry(),
        },
    )
    .await
    .unwrap_or_else(|error| panic!("opening a harness catalog: {error}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn catalog_matches_the_reference() {
    let Some(db) = database("catalog_matches_the_reference").await else {
        return;
    };
    let url = db.url().clone();
    let result = off_runtime(move || {
        check_topic_catalog(
            HarnessConfig {
                cases: 24,
                max_ops: 40,
            },
            move |config| make(url.clone(), config),
        )
    });
    if let Err(mismatch) = result {
        panic!("{mismatch}");
    }
}
