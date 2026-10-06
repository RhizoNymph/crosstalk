//! Harnesses for the L8 stores.
//!
//! | Harness | Trait | Subject |
//! | --- | --- | --- |
//! | [`check_audit_log`] | `AuditLog` | the trait itself |
//! | [`check_operator_store`] | `OperatorStore` | [`OperatorStoreSubject`]: the store and the `AuditLog` its loads record into |
//! | [`check_audit_intents`] | `AuditIntents` | the trait itself (and its `AuditLog`) |
//! | [`check_sink_registry`] | `SinkRegistry` | a registry configured with the sinks the harness draws |
//!
//! `check_audit_log` also checks `surface.audit.append-only`: an entry a
//! query returned is returned unchanged by every later query it matches.

use std::collections::BTreeMap;

use proptest::prelude::*;

use crosstalk_spec::ids::{AlertId, AuditId, ConfigHash};
use crosstalk_spec::interfaces::l8_surface::audit::{
    AuditAuthor, AuditBody, AuditEntry, AuditError, AuditFilter, AuditIntent, AuditIntents,
    AuditLog, AuditOutcome, AuditSubject, ConfigChange, ConfigOutcome, ConfigRecord,
    OperatorRecord, Rejection,
};
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, CallerError, Operator, OperatorConfig, OperatorLoadError, OperatorName,
    OperatorStore, OperatorStoreError, RequestIdentity, TrustedOperator,
};
use crosstalk_spec::interfaces::l8_surface::sinks::SinkRegistry;
use crosstalk_spec::interfaces::l8_surface::{
    ActionOutcome, Caller, CallerSnapshot, OperatorAction, Permission, PermissionSet, SinkError,
    SinkKind,
};
use crosstalk_spec::paging::{AuditList, PageRequest, PageSize};
use crosstalk_spec::support::{Blake3, Timestamp};

use crate::model::build::{audit_id, operator, raw, sink, ts, window};
use crate::model::{Divergence, HarnessConfig, ModelMismatch, holds, run, same};
use crate::support::IdSequence;
use crate::surface::audit::InMemoryAuditLog;
use crate::surface::operators::InMemoryOperatorStore;
use crate::surface::sinks::{InMemorySinkRegistry, SinkConfig};

#[derive(Debug, Clone)]
pub enum AuditOp {
    Append {
        id: u64,
        at: u64,
        operator_entry: bool,
        by: u64,
        about: u64,
    },
    Query {
        by: Vec<u8>,
        about: Option<u64>,
        window: Option<(u64, u64)>,
        size: u16,
    },
}

fn audit_op() -> impl Strategy<Value = AuditOp> {
    prop_oneof![
        3 => (0u64..10, 0u64..100, any::<bool>(), 0u64..3, 0u64..3)
            .prop_map(|(id, at, operator_entry, by, about)| AuditOp::Append { id, at, operator_entry, by, about }),
        2 => (prop::collection::vec(0u8..4, 0..2), prop::option::of(0u64..3), prop::option::of((0u64..100, 1u64..100)), 1u16..4)
            .prop_map(|(by, about, window, size)| AuditOp::Query { by, about, window, size }),
    ]
}

/// Entry `id` at `at`: an operator acknowledging alert `about`, or config
/// removing operator `about`.
fn entry(id: u64, at: u64, operator_entry: bool, by: u64, about: u64) -> Option<AuditEntry> {
    let body = if operator_entry {
        let caller = CallerSnapshot::new(operator(by), PermissionSet::ALL).ok()?;
        let record = OperatorRecord::new(
            caller,
            OperatorAction::Acknowledge {
                alert: AlertId::from_ulid(raw(about)),
            },
            AuditOutcome::Succeeded(ActionOutcome::Applied),
        )
        .ok()?;
        AuditBody::Operator(record)
    } else {
        AuditBody::Config(ConfigRecord {
            config: ConfigHash::from_digest(Blake3::from_bytes([7; 32])),
            change: ConfigChange::RemoveOperator {
                operator: operator(about),
            },
            outcome: ConfigOutcome::Applied,
        })
    };
    Some(AuditEntry {
        id: audit_id(id),
        at: ts(at),
        body,
    })
}

fn author(n: u8) -> AuditAuthor {
    match n {
        0 => AuditAuthor::Config,
        n => AuditAuthor::Operator(operator(u64::from(n - 1))),
    }
}

type Traversal = (Vec<(Vec<AuditEntry>, bool)>, Option<AuditError>);

async fn traverse<S: AuditLog>(log: &S, filter: &AuditFilter, size: PageSize) -> Traversal {
    let mut request: PageRequest<AuditList> = PageRequest { size, after: None };
    let mut pages = Vec::new();
    loop {
        match log.query(filter, &request).await {
            Err(error) => return (pages, Some(error)),
            Ok(page) => {
                let (items, next) = page.into_parts();
                pages.push((items, next.is_some()));
                match next {
                    Some(cursor) => request.after = Some(cursor),
                    None => return (pages, None),
                }
            }
        }
    }
}

/// Random appends (ids reused on purpose) and filtered traversals against
/// the reference. `make` builds a fresh, empty log.
pub fn check_audit_log<S, F, Fut>(harness: HarnessConfig, make: F) -> Result<(), ModelMismatch>
where
    S: AuditLog,
    F: Fn() -> Fut,
    Fut: Future<Output = S>,
{
    let strategy = prop::collection::vec(audit_op(), 1..harness.max_ops);
    run(harness, strategy, |runtime, ops| {
        runtime.block_on(async {
            let mut subject = make().await;
            let mut reference = InMemoryAuditLog::new();
            let mut seen: BTreeMap<AuditId, AuditEntry> = BTreeMap::new();
            for (step, op) in ops.iter().enumerate() {
                match op {
                    AuditOp::Append {
                        id,
                        at,
                        operator_entry,
                        by,
                        about,
                    } => {
                        let Some(one) = entry(*id, *at, *operator_entry, *by, *about) else {
                            continue;
                        };
                        let theirs = subject.append(one.clone()).await;
                        same(
                            step,
                            &format!("{op:?}"),
                            &theirs,
                            &reference.append(one).await,
                        )?;
                    }
                    AuditOp::Query {
                        by,
                        about,
                        window: w,
                        size,
                    } => {
                        let Ok(size) = PageSize::new(*size) else {
                            continue;
                        };
                        let filter = AuditFilter {
                            by: by.iter().copied().map(author).collect(),
                            subject: about.map(|n| AuditSubject::Alert(AlertId::from_ulid(raw(n)))),
                            window: w.and_then(|(start, length)| window(start, start + length)),
                        };
                        let theirs = traverse(&subject, &filter, size).await;
                        same(
                            step,
                            &format!("{op:?}"),
                            &theirs,
                            &traverse(&reference, &filter, size).await,
                        )?;
                        for entry in theirs.0.iter().flat_map(|(items, _)| items) {
                            let first = seen.entry(entry.id).or_insert_with(|| entry.clone());
                            holds(step, first == entry, || {
                                format!("entry {:?} changed: {first:?} then {entry:?}", entry.id)
                            })?;
                        }
                    }
                }
            }
            Ok::<(), Divergence>(())
        })
    })
}

/// The operator directory's store and the audit log its config loads are
/// recorded in, both through their spec traits.
pub trait OperatorStoreSubject: OperatorStore + AuditLog {}

impl<T: OperatorStore + AuditLog> OperatorStoreSubject for T {}

/// The reference operator store and the log it records into.
#[derive(Debug, Clone)]
pub struct ReferenceOperators {
    pub store: InMemoryOperatorStore,
    pub log: InMemoryAuditLog,
}

impl ReferenceOperators {
    pub fn new() -> Self {
        let log = InMemoryAuditLog::new();
        Self {
            store: InMemoryOperatorStore::new(log.clone(), IdSequence::default()),
            log,
        }
    }
}

impl Default for ReferenceOperators {
    fn default() -> Self {
        Self::new()
    }
}

impl OperatorStore for ReferenceOperators {
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

impl AuditLog for ReferenceOperators {
    fn append(&mut self, entry: AuditEntry) -> impl Future<Output = Result<(), AuditError>> + Send {
        self.log.append(entry)
    }

    fn query(
        &self,
        filter: &AuditFilter,
        page: &PageRequest<AuditList>,
    ) -> impl Future<
        Output = Result<crosstalk_spec::paging::Page<AuditEntry, AuditList>, AuditError>,
    > + Send {
        self.log.query(filter, page)
    }
}

/// Every audit entry a store holds, by a full traversal of the unfiltered
/// log.
async fn audit_entries<S: AuditLog>(store: &S) -> Result<Vec<AuditEntry>, AuditError> {
    let size = PageSize::new(10).map_err(|_| AuditError::InvalidCursor)?;
    let (pages, error) = traverse(store, &AuditFilter::default(), size).await;
    match error {
        Some(error) => Err(error),
        None => Ok(pages.into_iter().flat_map(|(items, _)| items).collect()),
    }
}

#[derive(Debug, Clone)]
pub enum OperatorOp {
    Trusted { id: u64, name: u8 },
    Authenticated { operators: Vec<(u64, u8, u8)> },
    Caller { verified: Option<u64> },
}

fn operator_op() -> impl Strategy<Value = OperatorOp> {
    prop_oneof![
        1 => (0u64..4, 0u8..3).prop_map(|(id, name)| OperatorOp::Trusted { id, name }),
        3 => prop::collection::vec((0u64..4, 0u8..3, 0u8..64), 0..4).prop_map(|operators| OperatorOp::Authenticated { operators }),
        3 => prop::option::of(0u64..5).prop_map(|verified| OperatorOp::Caller { verified }),
    ]
}

const NAMES: [&str; 3] = ["ana", "bo", "cy"];

fn name(n: u8) -> Option<OperatorName> {
    OperatorName::new(NAMES[usize::from(n) % NAMES.len()]).ok()
}

fn config_of(op: &OperatorOp) -> Option<AccessConfig> {
    match op {
        OperatorOp::Trusted { id, name: n } => Some(AccessConfig::Trusted(TrustedOperator {
            id: operator(*id),
            name: name(*n)?,
        })),
        OperatorOp::Authenticated { operators } => Some(AccessConfig::Authenticated(
            operators
                .iter()
                .map(|(id, n, bits)| {
                    Some(OperatorConfig {
                        id: operator(*id),
                        name: name(*n)?,
                        permissions: PermissionSet::of(
                            Permission::ALL
                                .iter()
                                .enumerate()
                                .filter(|(index, _)| bits >> index & 1 == 1)
                                .map(|(_, permission)| *permission),
                        ),
                    })
                })
                .collect::<Option<Vec<_>>>()?,
        )),
        OperatorOp::Caller { .. } => None,
    }
}

/// An entry without its store-assigned id.
fn entry_view(entry: &AuditEntry) -> String {
    format!("{:?}", (entry.at, &entry.body))
}

/// Random config loads (valid and not) and caller lookups against the
/// reference. `make` builds a fresh subject with no directory and an empty
/// audit log.
pub fn check_operator_store<S, F, Fut>(harness: HarnessConfig, make: F) -> Result<(), ModelMismatch>
where
    S: OperatorStoreSubject,
    F: Fn() -> Fut,
    Fut: Future<Output = S>,
{
    let strategy = prop::collection::vec(operator_op(), 1..harness.max_ops);
    run(harness, strategy, |runtime, ops| {
        runtime.block_on(async {
            let mut subject = make().await;
            let mut reference = ReferenceOperators::new();
            for (step, op) in ops.iter().enumerate() {
                let at = ts(10 * u64::try_from(step).unwrap_or(0));
                let hash = ConfigHash::from_digest(Blake3::from_bytes(
                    [u8::try_from(step % 256).unwrap_or(0); 32],
                ));
                match op {
                    OperatorOp::Caller { verified } => {
                        let identity = verified.map_or(RequestIdentity::Anonymous, |id| {
                            RequestIdentity::Verified(operator(id))
                        });
                        same(
                            step,
                            &format!("{op:?}"),
                            &subject.caller(identity).await,
                            &reference.caller(identity).await,
                        )?;
                    }
                    load => {
                        let Some(config) = config_of(load) else {
                            continue;
                        };
                        let theirs = subject.load(&config, hash, at).await;
                        same(
                            step,
                            &format!("{op:?}"),
                            &theirs,
                            &reference.load(&config, hash, at).await,
                        )?;
                    }
                }
                same(
                    step,
                    "operators",
                    &subject.operators().await,
                    &reference.operators().await,
                )?;
                let read = |error: AuditError| Divergence::new(step, format!("{error:?}"));
                let mut theirs: Vec<String> = audit_entries(&subject)
                    .await
                    .map_err(read)?
                    .iter()
                    .map(entry_view)
                    .collect();
                let mut ours: Vec<String> = audit_entries(&reference)
                    .await
                    .map_err(read)?
                    .iter()
                    .map(entry_view)
                    .collect();
                theirs.sort();
                ours.sort();
                same(step, "config entries", &theirs, &ours)?;
            }
            Ok::<(), Divergence>(())
        })
    })
}

#[derive(Debug, Clone)]
pub enum IntentOp {
    /// `AuditIntents::intend` for operator `by` acknowledging alert `about`.
    Intend {
        id: u64,
        at: u64,
        by: u64,
        about: u64,
    },
    /// `AuditIntents::complete` with the entry of that call and `outcome`
    /// (0: applied, 1: unchanged, 2: not found): the entry of an intent
    /// when the fields agree with one, a mismatch or a plain entry when not.
    Complete {
        id: u64,
        at: u64,
        by: u64,
        about: u64,
        outcome: u8,
    },
    /// `AuditLog::append`, as in [`check_audit_log`].
    Append {
        id: u64,
        at: u64,
        operator_entry: bool,
        by: u64,
        about: u64,
    },
    Recover,
    Query {
        about: Option<u64>,
        size: u16,
    },
}

fn intent_op() -> impl Strategy<Value = IntentOp> {
    prop_oneof![
        4 => (0u64..6, 0u64..4, 0u64..2, 0u64..2)
            .prop_map(|(id, at, by, about)| IntentOp::Intend { id, at, by, about }),
        4 => (0u64..6, 0u64..4, 0u64..2, 0u64..2, 0u8..3)
            .prop_map(|(id, at, by, about, outcome)| IntentOp::Complete { id, at, by, about, outcome }),
        2 => (0u64..6, 0u64..4, any::<bool>(), 0u64..2, 0u64..2)
            .prop_map(|(id, at, operator_entry, by, about)| IntentOp::Append { id, at, operator_entry, by, about }),
        1 => Just(IntentOp::Recover),
        2 => (prop::option::of(0u64..2), 1u16..4)
            .prop_map(|(about, size)| IntentOp::Query { about, size }),
    ]
}

/// The intent of operator `by`'s acknowledgement of alert `about`, accepted
/// at `at`, under id `id`.
fn intent(id: u64, at: u64, by: u64, about: u64) -> Option<AuditIntent> {
    let caller = CallerSnapshot::new(operator(by), PermissionSet::ALL).ok()?;
    AuditIntent::new(
        audit_id(id),
        ts(at),
        caller,
        OperatorAction::Acknowledge {
            alert: AlertId::from_ulid(raw(about)),
        },
    )
    .ok()
}

fn intent_outcome(outcome: u8) -> AuditOutcome {
    match outcome {
        0 => AuditOutcome::Succeeded(ActionOutcome::Applied),
        1 => AuditOutcome::Succeeded(ActionOutcome::Unchanged),
        _ => AuditOutcome::Rejected(Rejection::NotFound),
    }
}

/// Random intents, completions (matching their intent or not), plain
/// appends over the same ids, recoveries and traversals against the
/// reference. `make` builds a fresh, empty log.
///
/// Also checks `surface.audit.no-silent-effect` against an oracle of its
/// own: after a recovery, every intent the subject accepted and no
/// completion removed has an `Interrupted` entry under its id.
pub fn check_audit_intents<S, F, Fut>(harness: HarnessConfig, make: F) -> Result<(), ModelMismatch>
where
    S: AuditIntents,
    F: Fn() -> Fut,
    Fut: Future<Output = S>,
{
    let strategy = prop::collection::vec(intent_op(), 1..harness.max_ops);
    run(harness, strategy, |runtime, ops| {
        runtime.block_on(async {
            let mut subject = make().await;
            let mut reference = InMemoryAuditLog::new();
            let mut pending: BTreeMap<AuditId, AuditIntent> = BTreeMap::new();
            for (step, op) in ops.iter().enumerate() {
                let label = format!("{op:?}");
                match op {
                    IntentOp::Intend { id, at, by, about } => {
                        let Some(intent) = intent(*id, *at, *by, *about) else {
                            continue;
                        };
                        let theirs = subject.intend(&intent).await;
                        same(step, &label, &theirs, &reference.intend(&intent).await)?;
                        if theirs.is_ok() {
                            pending.insert(intent.id(), intent);
                        }
                    }
                    IntentOp::Complete {
                        id,
                        at,
                        by,
                        about,
                        outcome,
                    } => {
                        let Some(intent) = intent(*id, *at, *by, *about) else {
                            continue;
                        };
                        let Ok(entry) = intent.entry(intent_outcome(*outcome)) else {
                            continue;
                        };
                        let theirs = subject.complete(entry.clone()).await;
                        same(step, &label, &theirs, &reference.complete(entry).await)?;
                        if theirs.is_ok() {
                            pending.remove(&intent.id());
                        }
                    }
                    IntentOp::Append {
                        id,
                        at,
                        operator_entry,
                        by,
                        about,
                    } => {
                        let Some(one) = entry(*id, *at, *operator_entry, *by, *about) else {
                            continue;
                        };
                        let theirs = subject.append(one.clone()).await;
                        same(step, &label, &theirs, &reference.append(one).await)?;
                    }
                    IntentOp::Recover => {
                        let theirs = subject.recover_interrupted().await;
                        same(
                            step,
                            &label,
                            &theirs,
                            &reference.recover_interrupted().await,
                        )?;
                        let read = |error: AuditError| Divergence::new(step, format!("{error:?}"));
                        let logged = audit_entries(&subject).await.map_err(read)?;
                        for (id, intent) in std::mem::take(&mut pending) {
                            let recorded =
                                logged.iter().any(|entry| entry == &intent.interrupted());
                            holds(step, recorded, || {
                                format!("intent {id:?} left no Interrupted entry")
                            })?;
                        }
                    }
                    IntentOp::Query { about, size } => {
                        let Ok(size) = PageSize::new(*size) else {
                            continue;
                        };
                        let filter = AuditFilter {
                            subject: about.map(|n| AuditSubject::Alert(AlertId::from_ulid(raw(n)))),
                            ..AuditFilter::default()
                        };
                        same(
                            step,
                            &label,
                            &traverse(&subject, &filter, size).await,
                            &traverse(&reference, &filter, size).await,
                        )?;
                    }
                }
            }
            Ok::<(), Divergence>(())
        })
    })
}

#[derive(Debug, Clone)]
pub enum SinkOp {
    /// A delivery to sink `sink` at `at`: succeeded, or failed with
    /// `status` (`Unreachable` when 0).
    Record {
        sink: u64,
        at: u64,
        failed: Option<u16>,
    },
    Sinks,
}

fn sink_op() -> impl Strategy<Value = SinkOp> {
    prop_oneof![
        3 => (0u64..5, 0u64..100, prop::option::of(0u16..3))
            .prop_map(|(sink, at, failed)| SinkOp::Record { sink, at, failed }),
        1 => Just(SinkOp::Sinks),
    ]
}

const SINK_KINDS: [SinkKind; 3] = [SinkKind::Webhook, SinkKind::Slack, SinkKind::Log];

fn sink_configs() -> impl Strategy<Value = Vec<SinkConfig>> {
    prop::collection::vec((0u64..4, 0usize..3, 0usize..3), 0..5).prop_map(|sinks| {
        sinks
            .into_iter()
            .map(|(id, kind, n)| SinkConfig {
                id: sink(id),
                kind: SINK_KINDS[kind],
                name: NAMES[n].to_owned(),
            })
            .collect()
    })
}

/// Random configured sinks (one listed twice keeps its last definition),
/// then random deliveries, known and unknown, against the reference.
/// `make` builds a fresh registry configured with the sinks given.
pub fn check_sink_registry<S, F, Fut>(harness: HarnessConfig, make: F) -> Result<(), ModelMismatch>
where
    S: SinkRegistry,
    F: Fn(Vec<SinkConfig>) -> Fut,
    Fut: Future<Output = S>,
{
    let strategy = (
        sink_configs(),
        prop::collection::vec(sink_op(), 1..harness.max_ops),
    );
    run(harness, strategy, |runtime, (configured, ops)| {
        runtime.block_on(async {
            let mut subject = make(configured.clone()).await;
            let mut reference = InMemorySinkRegistry::new(configured.clone());
            same(
                0,
                "configured",
                &subject.sinks().await,
                &reference.sinks().await,
            )?;
            for (step, op) in ops.iter().enumerate() {
                let label = format!("{op:?}");
                match op {
                    SinkOp::Record {
                        sink: n,
                        at,
                        failed,
                    } => {
                        let outcome = match failed {
                            None => Ok(ts(*at)),
                            Some(0) => Err(SinkError::Unreachable {
                                reason: format!("down at {at}"),
                            }),
                            Some(status) => Err(SinkError::Rejected {
                                status: 400 + status,
                            }),
                        };
                        same(
                            step,
                            &label,
                            &subject.record_delivery(sink(*n), outcome.clone()).await,
                            &reference.record_delivery(sink(*n), outcome).await,
                        )?;
                    }
                    SinkOp::Sinks => {}
                }
                same(
                    step,
                    "sinks",
                    &subject.sinks().await,
                    &reference.sinks().await,
                )?;
            }
            Ok::<(), Divergence>(())
        })
    })
}
