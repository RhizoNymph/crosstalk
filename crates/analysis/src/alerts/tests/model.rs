//! The memory crate's alert harnesses against [`PgAlertStore`]: random
//! rule writes, re-fits, model changes, drafts, sanctions, verdicts,
//! acknowledgements and resolutions give the same results and leave the
//! same rules and alerts as the reference store.

use std::sync::Arc;

use crosstalk_memory::analysis::alerts::AlertStoreConfig as ReferenceConfig;
use crosstalk_memory::model::HarnessConfig;
use crosstalk_memory::model::analysis::{AlertWorld, check_alert_rule_store, check_alert_triage};
use crosstalk_store::DatabaseUrl;

use super::super::{AlertStoreConfig, AlertStoreParts, NoFacts, PgAlertStore};
use crate::pg::testing::{CURSOR_KEY, DiscardSink, case_pool, database, ids, off_runtime, retry};

type Subject = PgAlertStore<
    crosstalk_memory::analysis::fakes::FakeEmbedder,
    crosstalk_memory::analysis::aliases::StaticDirectory,
    NoFacts,
    DiscardSink,
>;

fn config_of(reference: ReferenceConfig) -> AlertStoreConfig {
    AlertStoreConfig {
        rules: reference.rules,
        sinks: reference.sinks,
        builtins: reference.builtins,
    }
}

/// A fresh store over emptied tables for one harness case.
async fn make(url: DatabaseUrl, world: AlertWorld) -> Subject {
    let pool = case_pool(&url).await;
    PgAlertStore::open(
        pool,
        config_of(world.config),
        AlertStoreParts {
            embedder: world.embedder,
            directory: world.directory,
            facts: NoFacts,
            sink: Arc::new(DiscardSink),
            ids: ids(3),
            cursor_key: CURSOR_KEY,
            retry: retry(),
        },
    )
    .await
    .unwrap_or_else(|error| panic!("opening a harness store: {error}"))
}

fn harness() -> HarnessConfig {
    HarnessConfig {
        cases: 16,
        max_ops: 30,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn rule_store_matches_the_reference() {
    let Some(db) = database("rule_store_matches_the_reference").await else {
        return;
    };
    let url = db.url().clone();
    let result = off_runtime(move || {
        check_alert_rule_store(harness(), move |world| make(url.clone(), world))
    });
    if let Err(mismatch) = result {
        panic!("{mismatch}");
    }
}

/// Also the property evidence of the triage and lifecycle invariants: the
/// reference is their model (`analysis.triage.dedup-iff-active`,
/// `analysis.alert.state-transitions`, `rule-disabled-suppresses`,
/// `sanction-suppresses`, `false-detection-suppresses`), and the harness
/// checks at most one active alert per key after every operation.
#[tokio::test(flavor = "multi_thread")]
async fn triage_matches_the_reference() {
    let Some(db) = database("triage_matches_the_reference").await else {
        return;
    };
    let url = db.url().clone();
    let result =
        off_runtime(move || check_alert_triage(harness(), move |world| make(url.clone(), world)));
    if let Err(mismatch) = result {
        panic!("{mismatch}");
    }
}
