//! The L8 reference stores: the audit log, the operator store and the sink
//! registry.

use crosstalk_spec::aggregates::alert::{Alert, AlertState, AlertSubject};
use crosstalk_spec::ids::{AlertId, ConfigHash};
use crosstalk_spec::interfaces::l8_surface::audit::{
    AuditAuthor, AuditBody, AuditEntry, AuditError, AuditFilter, AuditLog, AuditOutcome,
    AuditSubject, ConfigChange, ConfigOutcome, ConfigRecord, OperatorRecord,
};
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, AccessMode, CallerError, InvalidAccessConfig, OperatorConfig, OperatorLoadError,
    OperatorName, OperatorStore, RequestIdentity, TrustedOperator, Unauthenticated,
};
use crosstalk_spec::interfaces::l8_surface::sinks::{SinkRegistry, SinkRegistryError};
use crosstalk_spec::interfaces::l8_surface::{
    ActionOutcome, AlertSink, CallerSnapshot, OperatorAction, Permission, PermissionSet, SinkError,
    SinkKind,
};
use crosstalk_spec::paging::{AuditList, PageRequest, PageSize};
use crosstalk_spec::support::Blake3;

use super::audit::InMemoryAuditLog;
use super::operators::InMemoryOperatorStore;
use super::sinks::{FakeSink, InMemorySinkRegistry, SinkConfig};
use crate::model::build::{audit_id, channel, operator, raw, rule_id, sink, ts, window};
use crate::support::IdSequence;

fn hash(n: u8) -> ConfigHash {
    ConfigHash::from_digest(Blake3::from_bytes([n; 32]))
}

fn config_entry(n: u64, at: u64, change: ConfigChange) -> AuditEntry {
    AuditEntry {
        id: audit_id(n),
        at: ts(at),
        body: AuditBody::Config(ConfigRecord {
            config: hash(1),
            change,
            outcome: ConfigOutcome::Applied,
        }),
    }
}

fn operator_entry(n: u64, at: u64, by: u64, alert: AlertId) -> AuditEntry {
    let caller = CallerSnapshot::new(operator(by), PermissionSet::ALL).unwrap();
    let record = OperatorRecord::new(
        caller,
        OperatorAction::Acknowledge { alert },
        AuditOutcome::Succeeded(ActionOutcome::Applied),
    )
    .unwrap();
    AuditEntry {
        id: audit_id(n),
        at: ts(at),
        body: AuditBody::Operator(record),
    }
}

fn first(size: u16) -> PageRequest<AuditList> {
    PageRequest {
        size: PageSize::new(size).unwrap(),
        after: None,
    }
}

async fn traverse(log: &InMemoryAuditLog, filter: &AuditFilter, size: u16) -> Vec<AuditEntry> {
    let mut request = first(size);
    let mut seen = Vec::new();
    loop {
        let page = log.query(filter, &request).await.unwrap();
        let (items, next) = page.into_parts();
        seen.extend(items);
        match next {
            Some(cursor) => request.after = Some(cursor),
            None => return seen,
        }
    }
}

#[tokio::test]
async fn audit_append_is_idempotent_and_refuses_reused_ids() {
    // surface.audit.append-only
    let mut log = InMemoryAuditLog::new();
    let entry = config_entry(1, 10, ConfigChange::SetAccessMode(AccessMode::Trusted));
    log.append(entry.clone()).await.unwrap();
    log.append(entry.clone()).await.unwrap();
    assert_eq!(log.entries(), vec![entry.clone()]);
    let other = config_entry(
        1,
        11,
        ConfigChange::SetAccessMode(AccessMode::Authenticated),
    );
    assert_eq!(
        log.append(other).await,
        Err(AuditError::IdReused(audit_id(1)))
    );
    assert_eq!(log.entries(), vec![entry]);
}

#[tokio::test]
async fn audit_query_is_newest_first_and_stable_under_appends() {
    // surface.audit.append-only: an entry returned once is returned
    // unchanged by every later query it matches.
    let mut log = InMemoryAuditLog::new();
    let alert = AlertId::from_ulid(raw(1));
    for (n, at) in [(1, 10), (2, 30), (3, 20), (4, 30)] {
        log.append(operator_entry(n, at, 1, alert)).await.unwrap();
    }
    let all = traverse(&log, &AuditFilter::default(), 1).await;
    let order: Vec<_> = all.iter().map(|entry| entry.id).collect();
    assert_eq!(
        order,
        vec![audit_id(4), audit_id(2), audit_id(3), audit_id(1)]
    );
    // Appends during a traversal never shift a page.
    let page = log.query(&AuditFilter::default(), &first(2)).await.unwrap();
    log.append(operator_entry(5, 40, 1, alert)).await.unwrap();
    log.append(operator_entry(6, 15, 1, alert)).await.unwrap();
    let next = PageRequest {
        size: PageSize::new(10).unwrap(),
        after: page.next().cloned(),
    };
    let rest = log.query(&AuditFilter::default(), &next).await.unwrap();
    let rest: Vec<_> = rest.items().iter().map(|entry| entry.id).collect();
    assert_eq!(rest, vec![audit_id(3), audit_id(6), audit_id(1)]);
    let again = traverse(&log, &AuditFilter::default(), 3).await;
    for entry in &all {
        assert!(again.contains(entry));
    }
}

#[tokio::test]
async fn audit_query_filters_and_binds_cursors_to_the_filter() {
    let mut log = InMemoryAuditLog::new();
    let alert = AlertId::from_ulid(raw(1));
    log.append(operator_entry(1, 10, 1, alert)).await.unwrap();
    log.append(operator_entry(2, 20, 2, alert)).await.unwrap();
    log.append(config_entry(
        3,
        30,
        ConfigChange::RemoveOperator {
            operator: operator(2),
        },
    ))
    .await
    .unwrap();
    let by_config = AuditFilter {
        by: vec![AuditAuthor::Config],
        ..AuditFilter::default()
    };
    assert_eq!(
        traverse(&log, &by_config, 5)
            .await
            .iter()
            .map(|entry| entry.id)
            .collect::<Vec<_>>(),
        vec![audit_id(3)]
    );
    let about = AuditFilter {
        subject: Some(AuditSubject::Operator(operator(2))),
        window: window(0, 100),
        ..AuditFilter::default()
    };
    assert_eq!(
        traverse(&log, &about, 5)
            .await
            .iter()
            .map(|entry| entry.id)
            .collect::<Vec<_>>(),
        vec![audit_id(3)]
    );
    let page = log.query(&AuditFilter::default(), &first(1)).await.unwrap();
    let next = PageRequest {
        size: PageSize::new(1).unwrap(),
        after: page.next().cloned(),
    };
    assert_eq!(
        log.query(&by_config, &next).await,
        Err(AuditError::InvalidCursor)
    );
}

fn name(text: &str) -> OperatorName {
    OperatorName::new(text).unwrap()
}

fn authenticated(operators: &[(u64, &str, &[Permission])]) -> AccessConfig {
    AccessConfig::Authenticated(
        operators
            .iter()
            .map(|(id, text, permissions)| OperatorConfig {
                id: operator(*id),
                name: name(text),
                permissions: PermissionSet::of(permissions.iter().copied()),
            })
            .collect(),
    )
}

fn store() -> (InMemoryOperatorStore, InMemoryAuditLog) {
    let log = InMemoryAuditLog::new();
    (
        InMemoryOperatorStore::new(log.clone(), IdSequence::default()),
        log,
    )
}

#[tokio::test]
async fn config_reload_records_each_change_once() {
    // surface.audit.config-changes-recorded, for the operator directory
    let (mut store, log) = store();
    let first = authenticated(&[
        (1, "ana", &[Permission::View]),
        (2, "bo", &[Permission::Audit]),
    ]);
    let changes = store.load(&first, hash(1), ts(10)).await.unwrap();
    assert_eq!(changes.len(), 3);
    let recorded: Vec<_> = log
        .entries()
        .into_iter()
        .map(|entry| match entry.body {
            AuditBody::Config(record) => {
                assert_eq!(record.config, hash(1));
                assert_eq!(record.outcome, ConfigOutcome::Applied);
                assert_eq!(entry.at, ts(10));
                record.change
            }
            other => panic!("expected a config entry, got {other:?}"),
        })
        .collect();
    assert_eq!(recorded, changes);
    // Reloading the same config records nothing.
    assert_eq!(store.load(&first, hash(2), ts(20)).await, Ok(Vec::new()));
    assert_eq!(log.entries().len(), 3);
    // Dropping operator 2 records one removal.
    let second = authenticated(&[(1, "ana", &[Permission::View])]);
    assert_eq!(
        store.load(&second, hash(3), ts(30)).await,
        Ok(vec![ConfigChange::RemoveOperator {
            operator: operator(2)
        }])
    );
    assert_eq!(log.entries().len(), 4);
}

#[tokio::test]
async fn invalid_config_changes_nothing_and_records_nothing() {
    // surface.operators.config-rejects-unusable, through the store
    let (mut store, log) = store();
    store
        .load(
            &authenticated(&[(1, "ana", &[Permission::View])]),
            hash(1),
            ts(10),
        )
        .await
        .unwrap();
    let before = store.directory();
    assert_eq!(
        store
            .load(&AccessConfig::Authenticated(Vec::new()), hash(2), ts(20))
            .await,
        Err(OperatorLoadError::Invalid(InvalidAccessConfig::NoOperators))
    );
    assert_eq!(store.directory(), before);
    assert_eq!(log.entries().len(), 2);
}

#[tokio::test]
async fn operators_query_returns_directory_with_former_operators() {
    // surface.query.operators-match-directory and operators.former-kept
    let (mut store, _) = store();
    store
        .load(
            &authenticated(&[
                (2, "bo", &[Permission::View]),
                (1, "ana", &[Permission::Audit]),
            ]),
            hash(1),
            ts(10),
        )
        .await
        .unwrap();
    store
        .load(
            &authenticated(&[(1, "ana", &[Permission::Audit])]),
            hash(2),
            ts(20),
        )
        .await
        .unwrap();
    let operators = store.operators().await.unwrap();
    assert_eq!(
        operators.iter().map(|one| one.id).collect::<Vec<_>>(),
        vec![operator(1), operator(2)]
    );
    assert!(operators[1].permissions.is_empty());
    assert_eq!(operators[1].name, name("bo"));
    assert_eq!(
        store.caller(RequestIdentity::Verified(operator(2))).await,
        Err(CallerError::Unauthenticated(
            Unauthenticated::FormerOperator(operator(2))
        ))
    );
    let caller = store
        .caller(RequestIdentity::Verified(operator(1)))
        .await
        .unwrap();
    assert_eq!(caller.operator(), operator(1));
    assert!(caller.has(Permission::Audit));
    assert!(!caller.has(Permission::View));
}

#[tokio::test]
async fn trusted_mode_gives_every_request_the_trusted_caller() {
    let (mut store, _) = store();
    assert_eq!(
        store.caller(RequestIdentity::Anonymous).await,
        Err(CallerError::NotLoaded)
    );
    store
        .load(
            &AccessConfig::Trusted(TrustedOperator {
                id: operator(1),
                name: name("solo"),
            }),
            hash(1),
            ts(10),
        )
        .await
        .unwrap();
    let caller = store.caller(RequestIdentity::Anonymous).await.unwrap();
    assert_eq!(caller.operator(), operator(1));
    assert_eq!(caller.permissions(), PermissionSet::ALL);
}

#[tokio::test]
async fn sink_registry_reports_last_delivery() {
    let mut registry = InMemorySinkRegistry::new([
        SinkConfig {
            id: sink(2),
            kind: SinkKind::Slack,
            name: "ops".to_owned(),
        },
        SinkConfig {
            id: sink(1),
            kind: SinkKind::Log,
            name: "log".to_owned(),
        },
    ]);
    assert_eq!(
        registry.ids().into_iter().collect::<Vec<_>>(),
        vec![sink(1), sink(2)]
    );
    assert!(
        registry
            .sinks()
            .await
            .unwrap()
            .iter()
            .all(|info| info.last_delivery.is_none())
    );
    registry.record_delivery(sink(2), Ok(ts(5))).await.unwrap();
    let failure = SinkError::Rejected { status: 500 };
    registry
        .record_delivery(sink(2), Err(failure.clone()))
        .await
        .unwrap();
    assert_eq!(
        registry.sinks().await.unwrap()[1].last_delivery,
        Some(Err(failure))
    );
    assert_eq!(
        registry.record_delivery(sink(9), Ok(ts(5))).await,
        Err(SinkRegistryError::UnknownSink(sink(9)))
    );
    assert!(registry.contains(sink(1)));
}

#[tokio::test]
async fn fake_sink_records_and_fails_on_demand() {
    let sink_double = FakeSink::new(sink(1));
    let alert = Alert {
        id: AlertId::from_ulid(raw(1)),
        rule: rule_id(1),
        subject: AlertSubject::Channel(channel(1)),
        raised_at: ts(1),
        occurrences: 1,
        state: AlertState::Open,
    };
    sink_double.deliver(&alert).await.unwrap();
    sink_double.fail_with(Some(SinkError::Unreachable {
        reason: "down".to_owned(),
    }));
    assert!(sink_double.deliver(&alert).await.is_err());
    sink_double.fail_with(None);
    sink_double.deliver(&alert).await.unwrap();
    assert_eq!(sink_double.delivered(), vec![alert.clone(), alert]);
    assert_eq!(sink_double.id(), sink(1));
}
