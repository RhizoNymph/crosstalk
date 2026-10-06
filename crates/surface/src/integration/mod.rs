//! L8's Postgres stores against a real database (`TEST_DATABASE_URL`;
//! skipped when it is not configured): the memory harnesses ([`model`]),
//! and what only a database shows: state that survives a restart (a new
//! pool and new store values over the same database, the old ones dropped
//! without shutdown), the append-only guard, and a config load's entries
//! committed with its directory.

mod model;

use crosstalk_memory::model::build::{audit_id, operator, raw, sink, ts};
use crosstalk_spec::ids::mint::UlidGenerator;
use crosstalk_spec::ids::{
    AlertId, AuditId, ConfigHash, DeploymentSecret, KeyedHasher, SecretVersion, SeededRandom,
};
use crosstalk_spec::interfaces::l8_surface::audit::{
    AuditBody, AuditEntry, AuditError, AuditFilter, AuditIntent, AuditIntents, AuditLog,
    AuditOutcome, AuditSubject, ConfigChange, ConfigOutcome, ConfigRecord, INTERRUPTED_REASON,
};
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, CallerError, OperatorConfig, OperatorLoadError, OperatorName, OperatorStore,
    RequestIdentity,
};
use crosstalk_spec::interfaces::l8_surface::sinks::SinkRegistry;
use crosstalk_spec::interfaces::l8_surface::{
    ActionError, ActionOutcome, CallerSnapshot, OperatorAction, Permission, PermissionSet,
    SinkError, SinkKind,
};
use crosstalk_spec::paging::{AuditList, PageRequest, PageSize};
use crosstalk_spec::support::{Blake3, Clock, Timestamp};
use crosstalk_store::DatabaseUrl;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use crate::pg::testing::{database, retry, secret};
use crate::pg::{PgAuditLog, PgOperatorStore, PgSinkRegistry, SinkConfig};

/// A pool of a process of its own over the test database.
async fn process_pool(url: &DatabaseUrl) -> PgPool {
    match PgPoolOptions::new()
        .max_connections(2)
        .connect_with(url.connect_options().clone())
        .await
    {
        Ok(pool) => pool,
        Err(error) => panic!("connecting a process: {error}"),
    }
}

fn acknowledge_intent(n: u64, at: u64, alert: u64) -> AuditIntent {
    let Ok(caller) = CallerSnapshot::new(operator(1), PermissionSet::ALL) else {
        panic!("caller");
    };
    match AuditIntent::new(
        audit_id(n),
        ts(at),
        caller,
        OperatorAction::Acknowledge {
            alert: AlertId::from_ulid(raw(alert)),
        },
    ) {
        Ok(intent) => intent,
        Err(error) => panic!("intent: {error:?}"),
    }
}

fn config_entry(id: AuditId, at: u64, change: ConfigChange) -> AuditEntry {
    AuditEntry {
        id,
        at: ts(at),
        body: AuditBody::Config(ConfigRecord {
            config: ConfigHash::from_digest(Blake3::from_bytes([3; 32])),
            change,
            outcome: ConfigOutcome::Applied,
        }),
    }
}

fn first<L>(size: u16) -> PageRequest<L> {
    match PageSize::new(size) {
        Ok(size) => PageRequest { size, after: None },
        Err(error) => panic!("page size: {error:?}"),
    }
}

async fn everything(log: &PgAuditLog) -> Vec<AuditEntry> {
    let mut request = first::<AuditList>(100);
    let mut entries = Vec::new();
    loop {
        let page = match log.query(&AuditFilter::default(), &request).await {
            Ok(page) => page,
            Err(error) => panic!("query: {error:?}"),
        };
        let (items, next) = page.into_parts();
        entries.extend(items);
        match next {
            Some(next) => request.after = Some(next),
            None => return entries,
        }
    }
}

async fn ok<T, E: std::fmt::Debug>(what: &str, result: impl Future<Output = Result<T, E>>) -> T {
    match result.await {
        Ok(value) => value,
        Err(error) => panic!("{what}: {error:?}"),
    }
}

/// INV-1218 (`surface.audit.no-silent-effect`): intents a process left
/// when it stopped are, after a restart over the same database, appended
/// as `Interrupted` entries oldest first, each once; an intent completed
/// before the stop is its call's entry and is not recovered.
#[tokio::test(flavor = "multi_thread")]
async fn pg_leftover_intent_recovered_as_interrupted() {
    let Some(db) = database("pg_leftover_intent_recovered_as_interrupted").await else {
        return;
    };
    let later = acknowledge_intent(1, 30, 1);
    let earlier = acknowledge_intent(2, 20, 2);
    let finished = acknowledge_intent(3, 10, 3);
    let finished_entry = match finished.entry(AuditOutcome::Succeeded(ActionOutcome::Applied)) {
        Ok(entry) => entry,
        Err(error) => panic!("entry: {error:?}"),
    };
    {
        // The process that stops: its pool and store are dropped, never
        // shut down.
        let mut log = PgAuditLog::new(process_pool(db.url()).await, retry(), &secret());
        ok("intend", log.intend(&later)).await;
        ok("intend", log.intend(&earlier)).await;
        ok("intend again", log.intend(&earlier)).await;
        ok("intend", log.intend(&finished)).await;
        ok("complete", log.complete(finished_entry.clone())).await;
        assert_eq!(everything(&log).await, vec![finished_entry.clone()]);
    }
    let mut log = PgAuditLog::new(process_pool(db.url()).await, retry(), &secret());
    assert_eq!(
        log.recover_interrupted().await,
        Ok(vec![earlier.id(), later.id()])
    );
    assert_eq!(
        everything(&log).await,
        vec![
            later.interrupted(),
            earlier.interrupted(),
            finished_entry.clone()
        ]
    );
    let AuditBody::Operator(record) = &later.interrupted().body else {
        panic!("not an operator entry");
    };
    assert_eq!(
        record.outcome().result(),
        Err(ActionError::Store {
            reason: INTERRUPTED_REASON.to_owned()
        })
    );
    assert_eq!(log.recover_interrupted().await, Ok(Vec::new()));
    // A recovered id is the entry's: neither an intent nor another entry
    // may take it.
    assert_eq!(
        log.intend(&later).await,
        Err(AuditError::IdReused(later.id()))
    );
    // The subject filter reads the recovered entries too.
    let about = AuditFilter {
        subject: Some(AuditSubject::Alert(AlertId::from_ulid(raw(2)))),
        ..AuditFilter::default()
    };
    let page = ok("filtered", log.query(&about, &first(10))).await;
    assert_eq!(page.items(), [earlier.interrupted()]);
}

/// INV-457 (`surface.audit.append-only`): the audit tables refuse every
/// UPDATE and DELETE, whatever the role, and the entry stays as appended.
#[tokio::test(flavor = "multi_thread")]
async fn audit_table_rejects_update_and_delete() {
    let Some(db) = database("audit_table_rejects_update_and_delete").await else {
        return;
    };
    let mut log = PgAuditLog::new(db.pool().clone(), retry(), &secret());
    let intent = acknowledge_intent(1, 10, 1);
    let entry = match intent.entry(AuditOutcome::Succeeded(ActionOutcome::Applied)) {
        Ok(entry) => entry,
        Err(error) => panic!("entry: {error:?}"),
    };
    ok("append", log.append(entry.clone())).await;
    for statement in [
        "UPDATE surface.audit SET at = 0",
        "UPDATE surface.audit SET entry = '{}'",
        "DELETE FROM surface.audit",
        "UPDATE surface.audit_subjects SET subject = 'x'",
        "DELETE FROM surface.audit_subjects",
    ] {
        let refused = sqlx::query(statement).execute(db.pool()).await;
        let Err(error) = refused else {
            panic!("{statement} was not refused");
        };
        assert!(
            error.to_string().contains("append-only"),
            "{statement}: {error}"
        );
    }
    assert_eq!(everything(&log).await, vec![entry.clone()]);
    let about = AuditFilter {
        subject: Some(AuditSubject::Alert(AlertId::from_ulid(raw(1)))),
        ..AuditFilter::default()
    };
    let page = ok("filtered", log.query(&about, &first(10))).await;
    assert_eq!(page.items(), [entry]);
}

fn name(text: &str) -> OperatorName {
    match OperatorName::new(text) {
        Ok(name) => name,
        Err(error) => panic!("name: {error:?}"),
    }
}

fn two_operators() -> AccessConfig {
    AccessConfig::Authenticated(vec![
        OperatorConfig {
            id: operator(1),
            name: name("ana"),
            permissions: PermissionSet::of([Permission::View]),
        },
        OperatorConfig {
            id: operator(2),
            name: name("bo"),
            permissions: PermissionSet::ALL,
        },
    ])
}

/// A clock for predicting the ids a seeded operator store mints.
#[derive(Debug)]
struct Never;

impl Clock for Never {
    fn now(&self) -> Timestamp {
        Timestamp::from_micros(0)
    }
}

/// INV-543 (`surface.audit.config-changes-recorded`): a load's directory
/// and its config entries commit together. A load whose entries the log
/// refuses stores no directory and records nothing; a load that commits
/// shows both, to every later process.
#[tokio::test(flavor = "multi_thread")]
async fn config_entry_committed_with_change() {
    let Some(db) = database("config_entry_committed_with_change").await else {
        return;
    };
    let at = ts(1_000_000);
    let hash = ConfigHash::from_digest(Blake3::from_bytes([9; 32]));
    // The first id a store seeded with 11 mints at `at`, taken beforehand
    // by another entry: that load's first append is refused.
    let squatted: AuditId =
        match UlidGenerator::new(std::sync::Arc::new(Never), SeededRandom::new(11)).mint_at(at) {
            Ok(id) => id,
            Err(error) => panic!("mint: {error}"),
        };
    let mut log = PgAuditLog::new(db.pool().clone(), retry(), &secret());
    let squatter = config_entry(squatted, 5, ConfigChange::RemoveSink { sink: sink(9) });
    ok("squat", log.append(squatter.clone())).await;
    let mut refused = PgOperatorStore::new(db.pool().clone(), retry(), SeededRandom::new(11));
    assert_eq!(
        refused.load(&two_operators(), hash, at).await,
        Err(OperatorLoadError::Audit(AuditError::IdReused(squatted)))
    );
    assert_eq!(refused.operators().await, Ok(Vec::new()));
    assert_eq!(
        refused.caller(RequestIdentity::Verified(operator(2))).await,
        Err(CallerError::NotLoaded)
    );
    assert_eq!(everything(&log).await, vec![squatter.clone()]);

    let mut store = PgOperatorStore::new(db.pool().clone(), retry(), SeededRandom::from_entropy());
    let changes = ok("load", store.load(&two_operators(), hash, at)).await;
    assert_eq!(changes.len(), 3);
    // Another process sees the directory and every entry.
    let pool = process_pool(db.url()).await;
    let later = PgOperatorStore::new(pool.clone(), retry(), SeededRandom::from_entropy());
    let operators = ok("operators", later.operators()).await;
    assert_eq!(operators.len(), 2);
    let caller = ok(
        "caller",
        later.caller(RequestIdentity::Verified(operator(1))),
    )
    .await;
    assert_eq!(caller.permissions(), PermissionSet::of([Permission::View]));
    let recorded: Vec<ConfigChange> = everything(&PgAuditLog::new(pool, retry(), &secret()))
        .await
        .into_iter()
        .filter(|entry| entry.id != squatted)
        .map(|entry| match entry.body {
            AuditBody::Config(record) => {
                assert_eq!(record.config, hash);
                assert_eq!(entry.at, at);
                record.change
            }
            other => panic!("not a config entry: {other:?}"),
        })
        .collect();
    let mut expected = changes.clone();
    let mut recorded = recorded;
    let key = |change: &ConfigChange| format!("{change:?}");
    expected.sort_by_key(key);
    recorded.sort_by_key(key);
    assert_eq!(recorded, expected);
    // Loading the same config again changes and records nothing.
    assert_eq!(
        store.load(&two_operators(), hash, ts(2_000_000)).await,
        Ok(Vec::new())
    );
}

/// INV-1219 (`surface.cursor.survives-restart`), for the audit log: a
/// cursor issued before a restart resolves to the same page after it with
/// the same secret, and is refused under another secret or filter.
#[tokio::test(flavor = "multi_thread")]
async fn pg_audit_cursor_survives_restart() {
    let Some(db) = database("pg_audit_cursor_survives_restart").await else {
        return;
    };
    let mut log = PgAuditLog::new(process_pool(db.url()).await, retry(), &secret());
    for n in 0..5 {
        let intent = acknowledge_intent(n, 10 * n, n % 2);
        let entry = match intent.entry(AuditOutcome::Succeeded(ActionOutcome::Applied)) {
            Ok(entry) => entry,
            Err(error) => panic!("entry: {error:?}"),
        };
        ok("append", log.append(entry)).await;
    }
    let filter = AuditFilter::default();
    let page = ok("first", log.query(&filter, &first(2))).await;
    let resumed = PageRequest {
        size: first::<AuditList>(2).size,
        after: page.next().cloned(),
    };
    let expected = ok("second", log.query(&filter, &resumed)).await;
    drop(log);

    let after = PgAuditLog::new(process_pool(db.url()).await, retry(), &secret());
    assert_eq!(after.query(&filter, &resumed).await, Ok(expected));
    let rotated = PgAuditLog::new(
        db.pool().clone(),
        retry(),
        &KeyedHasher::new(DeploymentSecret::new(SecretVersion(1), [43; 32])),
    );
    assert_eq!(
        rotated.query(&filter, &resumed).await,
        Err(AuditError::InvalidCursor)
    );
    let other = AuditFilter {
        subject: Some(AuditSubject::Alert(AlertId::from_ulid(raw(1)))),
        ..AuditFilter::default()
    };
    assert_eq!(
        after.query(&other, &resumed).await,
        Err(AuditError::InvalidCursor)
    );
}

/// Sinks and their last deliveries survive a restart; reconfiguring
/// removes the sinks config dropped and keeps a sink's last delivery
/// while its kind is unchanged.
#[tokio::test(flavor = "multi_thread")]
async fn pg_sinks_survive_restart_and_reconfiguration() {
    let Some(db) = database("pg_sinks_survive_restart_and_reconfiguration").await else {
        return;
    };
    let defined = |kinds: &[(u64, SinkKind)]| -> Vec<SinkConfig> {
        kinds
            .iter()
            .map(|(n, kind)| SinkConfig {
                id: sink(*n),
                kind: *kind,
                name: format!("sink {n}"),
            })
            .collect()
    };
    let mut registry = ok(
        "configure",
        PgSinkRegistry::configure(
            process_pool(db.url()).await,
            retry(),
            defined(&[
                (1, SinkKind::Log),
                (2, SinkKind::Webhook),
                (3, SinkKind::Slack),
            ]),
        ),
    )
    .await;
    for n in [1, 2, 3] {
        ok("record", registry.record_delivery(sink(n), Ok(ts(n)))).await;
    }
    let failure = SinkError::Rejected { status: 500 };
    ok(
        "record",
        registry.record_delivery(sink(2), Err(failure.clone())),
    )
    .await;
    let before = ok("sinks", registry.sinks()).await;
    drop(registry);

    let reopened = PgSinkRegistry::open(process_pool(db.url()).await, retry());
    assert_eq!(reopened.sinks().await, Ok(before));

    let mut registry = ok(
        "reconfigure",
        PgSinkRegistry::configure(
            db.pool().clone(),
            retry(),
            defined(&[(1, SinkKind::Log), (2, SinkKind::Slack), (4, SinkKind::Log)]),
        ),
    )
    .await;
    let sinks = ok("sinks", registry.sinks()).await;
    let summary: Vec<_> = sinks
        .iter()
        .map(|info| (info.id, info.kind, info.last_delivery.clone()))
        .collect();
    assert_eq!(
        summary,
        vec![
            (sink(1), SinkKind::Log, Some(Ok(ts(1)))),
            (sink(2), SinkKind::Slack, None),
            (sink(4), SinkKind::Log, None),
        ]
    );
    assert_eq!(
        registry.record_delivery(sink(3), Ok(ts(9))).await,
        Err(crosstalk_spec::interfaces::l8_surface::sinks::SinkRegistryError::UnknownSink(sink(3)))
    );
}
