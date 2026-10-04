//! Harnesses for the L8 stores.
//!
//! | Harness | Trait | Subject |
//! | --- | --- | --- |
//! | [`check_audit_log`] | `AuditLog` | the trait itself |
//! | [`check_operator_store`] | the stored `OperatorDirectory` | [`OperatorStoreSubject`] |
//!
//! `check_audit_log` also checks `surface.audit.append-only`: an entry a
//! query returned is returned unchanged by every later query it matches.

use std::collections::BTreeMap;

use proptest::prelude::*;

use crosstalk_spec::ids::{AlertId, AuditId, ConfigHash};
use crosstalk_spec::interfaces::l8_surface::audit::{
    AuditAuthor, AuditBody, AuditEntry, AuditError, AuditFilter, AuditLog, AuditOutcome,
    AuditSubject, ConfigChange, ConfigOutcome, ConfigRecord, OperatorRecord,
};
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, Operator, OperatorConfig, OperatorName, RequestIdentity, TrustedOperator,
};
use crosstalk_spec::interfaces::l8_surface::{
    ActionOutcome, Caller, CallerSnapshot, OperatorAction, Permission, PermissionSet,
};
use crosstalk_spec::paging::{AuditList, PageRequest, PageSize};
use crosstalk_spec::support::{Blake3, Timestamp};

use crate::analysis::support::IdSequence;
use crate::model::build::{audit_id, operator, raw, ts, window};
use crate::model::{Divergence, HarnessConfig, ModelMismatch, holds, run, same};
use crate::surface::audit::InMemoryAuditLog;
use crate::surface::operators::{CallerError, InMemoryOperatorStore, LoadError};

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

/// The operator directory's store: config loads recorded in the audit log,
/// and the reads the surface makes.
pub trait OperatorStoreSubject {
    fn load(
        &self,
        config: &AccessConfig,
        hash: ConfigHash,
        at: Timestamp,
    ) -> impl Future<Output = Result<Vec<ConfigChange>, LoadError>> + Send;

    fn operators(&self) -> impl Future<Output = Vec<Operator>> + Send;

    fn caller(
        &self,
        identity: RequestIdentity,
    ) -> impl Future<Output = Result<Caller, CallerError>> + Send;

    /// The config entries recorded so far, in any order.
    fn audit_entries(&self) -> impl Future<Output = Vec<AuditEntry>> + Send;
}

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

impl OperatorStoreSubject for ReferenceOperators {
    async fn load(
        &self,
        config: &AccessConfig,
        hash: ConfigHash,
        at: Timestamp,
    ) -> Result<Vec<ConfigChange>, LoadError> {
        self.store.load(config, hash, at)
    }

    async fn operators(&self) -> Vec<Operator> {
        self.store.operators()
    }

    async fn caller(&self, identity: RequestIdentity) -> Result<Caller, CallerError> {
        self.store.caller(identity)
    }

    async fn audit_entries(&self) -> Vec<AuditEntry> {
        self.log.entries()
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
            let subject = make().await;
            let reference = ReferenceOperators::new();
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
                let mut theirs: Vec<String> = subject
                    .audit_entries()
                    .await
                    .iter()
                    .map(entry_view)
                    .collect();
                let mut ours: Vec<String> = reference
                    .audit_entries()
                    .await
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
