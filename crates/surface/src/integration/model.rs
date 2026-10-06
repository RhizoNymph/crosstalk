//! The memory crate's L8 harnesses against the Postgres stores: random
//! appends, intents, completions, recoveries, traversals, config loads,
//! caller lookups and deliveries give the same results and leave the same
//! state as the reference stores.

use crosstalk_memory::model::HarnessConfig;
use crosstalk_memory::model::surface::{
    check_audit_intents, check_audit_log, check_operator_store, check_sink_registry,
};
use crosstalk_memory::surface::sinks::SinkConfig as ReferenceSink;
use crosstalk_spec::ids::{ConfigHash, SeededRandom};
use crosstalk_spec::interfaces::l8_surface::Caller;
use crosstalk_spec::interfaces::l8_surface::audit::ConfigChange;
use crosstalk_spec::interfaces::l8_surface::audit::{
    AuditEntry, AuditError, AuditFilter, AuditLog,
};
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, CallerError, Operator, OperatorLoadError, OperatorStore, OperatorStoreError,
    RequestIdentity,
};
use crosstalk_spec::paging::{AuditList, Page, PageRequest};
use crosstalk_spec::support::Timestamp;
use crosstalk_store::DatabaseUrl;

use crate::pg::testing::{case_pool, database, off_runtime, retry, secret};
use crate::pg::{PgAuditLog, PgOperatorStore, PgSinkRegistry, SinkConfig};

fn harness() -> HarnessConfig {
    HarnessConfig {
        cases: 16,
        max_ops: 30,
    }
}

async fn audit_log(url: DatabaseUrl) -> PgAuditLog {
    PgAuditLog::new(case_pool(&url).await, retry(), &secret())
}

/// The operator store and the log its loads record into, over one pool.
#[derive(Debug, Clone)]
struct PgOperators {
    store: PgOperatorStore,
    log: PgAuditLog,
}

impl OperatorStore for PgOperators {
    fn load(
        &mut self,
        config: &AccessConfig,
        hash: ConfigHash,
        at: Timestamp,
    ) -> impl Future<Output = Result<Vec<ConfigChange>, OperatorLoadError>> + Send {
        self.store.load(config, hash, at)
    }

    fn operators(&self) -> impl Future<Output = Result<Vec<Operator>, OperatorStoreError>> + Send {
        self.store.operators()
    }

    fn caller(
        &self,
        identity: RequestIdentity,
    ) -> impl Future<Output = Result<Caller, CallerError>> + Send {
        self.store.caller(identity)
    }
}

impl AuditLog for PgOperators {
    fn append(&mut self, entry: AuditEntry) -> impl Future<Output = Result<(), AuditError>> + Send {
        self.log.append(entry)
    }

    fn query(
        &self,
        filter: &AuditFilter,
        page: &PageRequest<AuditList>,
    ) -> impl Future<Output = Result<Page<AuditEntry, AuditList>, AuditError>> + Send {
        self.log.query(filter, page)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_audit_log_matches_the_reference() {
    // surface.audit.append-only, on Postgres
    let Some(db) = database("pg_audit_log_matches_the_reference").await else {
        return;
    };
    let url = db.url().clone();
    let result = off_runtime(move || check_audit_log(harness(), move || audit_log(url.clone())));
    if let Err(mismatch) = result {
        panic!("{mismatch}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_audit_intents_match_the_reference() {
    // surface.audit.no-silent-effect, on Postgres: the harness's oracle
    // checks every leftover intent is recovered as Interrupted.
    let Some(db) = database("pg_audit_intents_match_the_reference").await else {
        return;
    };
    let url = db.url().clone();
    let result =
        off_runtime(move || check_audit_intents(harness(), move || audit_log(url.clone())));
    if let Err(mismatch) = result {
        panic!("{mismatch}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_operator_store_matches_the_reference() {
    // surface.audit.config-changes-recorded and the operator invariants
    // the reference folds, on Postgres
    let Some(db) = database("pg_operator_store_matches_the_reference").await else {
        return;
    };
    let url = db.url().clone();
    let result = off_runtime(move || {
        check_operator_store(harness(), move || {
            let url = url.clone();
            async move {
                let pool = case_pool(&url).await;
                PgOperators {
                    store: PgOperatorStore::new(pool.clone(), retry(), SeededRandom::new(5)),
                    log: PgAuditLog::new(pool, retry(), &secret()),
                }
            }
        })
    });
    if let Err(mismatch) = result {
        panic!("{mismatch}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_sink_registry_matches_the_reference() {
    let Some(db) = database("pg_sink_registry_matches_the_reference").await else {
        return;
    };
    let url = db.url().clone();
    let result = off_runtime(move || {
        check_sink_registry(harness(), move |sinks: Vec<ReferenceSink>| {
            let url = url.clone();
            async move {
                let pool = case_pool(&url).await;
                let sinks = sinks.into_iter().map(|sink| SinkConfig {
                    id: sink.id,
                    kind: sink.kind,
                    name: sink.name,
                });
                match PgSinkRegistry::configure(pool, retry(), sinks).await {
                    Ok(registry) => registry,
                    Err(error) => panic!("configuring a harness registry: {error:?}"),
                }
            }
        })
    });
    if let Err(mismatch) = result {
        panic!("{mismatch}");
    }
}
