//! [`PgOperatorStore`]: the stored operator directory, each config load
//! recorded in the audit log in the same transaction.
//!
//! ```text
//! load(config, hash, at): txn ─ read directory ─ OperatorDirectory::load(previous, config)
//!                               ├─ Invalid ─▶ rollback, nothing stored or recorded
//!                               └─ (directory, changes) ─▶ one Applied config entry per change
//!                                  (ids minted at `at`) appended + directory upserted, together
//! operators / caller: read the directory, rebuild it, answer as the spec value does
//! ```
//!
//! **The stored form.** `OperatorDirectory` is a spec value built only by
//! `OperatorDirectory::load`, with no wire form. The store keeps its mode
//! and every operator (`{"mode", "operators"}`) and rebuilds it with two
//! loads: one defining every operator ever known (former ones with every
//! permission), then the current config (the mode, and the operators that
//! hold permissions), which turns the others former exactly as the
//! original loads did. The rebuilt directory is checked against what was
//! stored; a mismatch is a codec failure, never a silently different
//! directory.
//!
//! **Audit ids.** Minted by a ULID generator stamped with the load's `at`
//! and drawing from the random source the composer passes:
//! `SeededRandom::from_entropy` in a running gateway, so a restart never
//! mints an id already in the log (`surface.ids.unique-across-restart`).

use std::sync::{Arc, Mutex, PoisonError};

use crosstalk_spec::ids::mint::{SeededRandom, UlidGenerator};
use crosstalk_spec::ids::{AuditId, ConfigHash};
use crosstalk_spec::interfaces::l8_surface::audit::{
    AuditBody, AuditEntry, AuditError, ConfigChange, ConfigOutcome, ConfigRecord,
};
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, AccessMode, CallerError, Operator, OperatorConfig, OperatorDirectory,
    OperatorLoadError, OperatorStore, OperatorStoreError, RequestIdentity, TrustedOperator,
};
use crosstalk_spec::interfaces::l8_surface::{Caller, PermissionSet};
use crosstalk_spec::support::{Clock, Timestamp};
use crosstalk_store::{SerializableRetry, TxError, retry_serializable};
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, PgPool, Row};

use super::audit::append_in;
use super::codec::{CodecError, from_json, to_json};
use super::{StorageFailure, settle};

/// The directory as stored: its mode and every operator, by id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct StoredDirectory {
    mode: AccessMode,
    operators: Vec<Operator>,
}

impl StoredDirectory {
    fn of(directory: &OperatorDirectory) -> Self {
        Self {
            mode: directory.mode(),
            operators: directory.operators().cloned().collect(),
        }
    }

    /// The directory this was stored from (module docs).
    fn rebuild(&self) -> Result<OperatorDirectory, CodecError> {
        let invalid = |reason: String| CodecError::Value {
            what: "operator directory",
            reason,
        };
        let current: Vec<&Operator> = self
            .operators
            .iter()
            .filter(|operator| !operator.permissions.is_empty())
            .collect();
        let config = match self.mode {
            AccessMode::Trusted => match current.as_slice() {
                [trusted] if trusted.permissions == PermissionSet::ALL => {
                    AccessConfig::Trusted(TrustedOperator {
                        id: trusted.id,
                        name: trusted.name.clone(),
                    })
                }
                _ => {
                    return Err(invalid(format!(
                        "trusted mode with {} operators holding permissions",
                        current.len()
                    )));
                }
            },
            AccessMode::Authenticated => AccessConfig::Authenticated(
                current
                    .iter()
                    .map(|operator| OperatorConfig {
                        id: operator.id,
                        name: operator.name.clone(),
                        permissions: operator.permissions,
                    })
                    .collect(),
            ),
        };
        let everyone = AccessConfig::Authenticated(
            self.operators
                .iter()
                .map(|operator| OperatorConfig {
                    id: operator.id,
                    name: operator.name.clone(),
                    permissions: if operator.permissions.is_empty() {
                        PermissionSet::ALL
                    } else {
                        operator.permissions
                    },
                })
                .collect(),
        );
        let (known, _) = OperatorDirectory::load(None, &everyone)
            .map_err(|error| invalid(format!("{error:?}")))?;
        let (directory, _) = OperatorDirectory::load(Some(&known), &config)
            .map_err(|error| invalid(format!("{error:?}")))?;
        if Self::of(&directory) != *self {
            return Err(invalid(
                "the rebuilt directory differs from the stored one".to_owned(),
            ));
        }
        Ok(directory)
    }
}

/// A clock the generator never reads: every id is minted at a load's `at`.
#[derive(Debug)]
struct LoadTime;

impl Clock for LoadTime {
    fn now(&self) -> Timestamp {
        Timestamp::from_micros(0)
    }
}

/// The operator directory on Postgres, recording into the `surface` audit
/// log. Cloning shares the pool and the id generator.
#[derive(Clone)]
pub struct PgOperatorStore {
    pool: PgPool,
    retry: SerializableRetry,
    ids: Arc<Mutex<UlidGenerator<SeededRandom>>>,
}

impl std::fmt::Debug for PgOperatorStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgOperatorStore").finish_non_exhaustive()
    }
}

impl PgOperatorStore {
    /// The store in `pool`'s database (migrated with
    /// [`super::run_migrations`]). `random` is where the audit ids of its
    /// config entries draw from: `SeededRandom::from_entropy` in a running
    /// gateway, a fixed seed in tests.
    pub fn new(pool: PgPool, retry: SerializableRetry, random: SeededRandom) -> Self {
        Self {
            pool,
            retry,
            ids: Arc::new(Mutex::new(UlidGenerator::new(Arc::new(LoadTime), random))),
        }
    }

    /// The stored directory, if a config was ever loaded.
    pub async fn directory(&self) -> Result<Option<OperatorDirectory>, OperatorStoreError> {
        let mut conn = self.pool.acquire().await.map_err(read_failure)?;
        stored_directory(&mut conn)
            .await
            .map_err(|failure| OperatorStoreError::Store {
                reason: failure.reason(),
            })
    }

    /// `changes.len()` fresh audit ids stamped `at`.
    fn mint(&self, at: Timestamp, count: usize) -> Result<Vec<AuditId>, OperatorLoadError> {
        // A poisoned lock still holds a valid generator: minting cannot
        // panic between reading and writing its state.
        let mut ids = self.ids.lock().unwrap_or_else(PoisonError::into_inner);
        (0..count)
            .map(|_| {
                ids.mint_at(at).map_err(|error| OperatorLoadError::Store {
                    reason: format!("no audit id left to mint: {error}"),
                })
            })
            .collect()
    }
}

fn read_failure(error: sqlx::Error) -> OperatorStoreError {
    OperatorStoreError::Store {
        reason: StorageFailure::Query(error).reason(),
    }
}

async fn stored_directory(
    conn: &mut PgConnection,
) -> Result<Option<OperatorDirectory>, StorageFailure> {
    let row = sqlx::query("SELECT directory FROM surface.operator_directory WHERE singleton")
        .fetch_optional(&mut *conn)
        .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let text: String = row.try_get("directory")?;
    let stored: StoredDirectory = from_json("operator directory", &text)?;
    Ok(Some(stored.rebuild()?))
}

fn load_failure(failure: StorageFailure) -> OperatorLoadError {
    OperatorLoadError::Store {
        reason: failure.reason(),
    }
}

/// A transaction body's failure: a driver error goes back to the retry
/// helper; anything else aborts the load.
fn into_tx(failure: StorageFailure) -> TxError<OperatorLoadError> {
    match failure {
        StorageFailure::Query(error) => TxError::Db(error),
        other => TxError::Abort(load_failure(other)),
    }
}

fn audit_refused(error: TxError<AuditError>) -> TxError<OperatorLoadError> {
    match error {
        TxError::Db(error) => TxError::Db(error),
        TxError::Abort(refused) => TxError::Abort(OperatorLoadError::Audit(refused)),
    }
}

impl OperatorStore for PgOperatorStore {
    async fn load(
        &mut self,
        config: &AccessConfig,
        hash: ConfigHash,
        at: Timestamp,
    ) -> Result<Vec<ConfigChange>, OperatorLoadError> {
        let config = Arc::new(config.clone());
        let store = self.clone();
        let changes = retry_serializable(&self.pool, &self.retry, |conn| {
            let config = Arc::clone(&config);
            let store = store.clone();
            Box::pin(async move {
                let previous = stored_directory(conn).await.map_err(into_tx)?;
                let (directory, changes) = OperatorDirectory::load(previous.as_ref(), &config)
                    .map_err(|invalid| TxError::Abort(OperatorLoadError::Invalid(invalid)))?;
                let ids = store.mint(at, changes.len()).map_err(TxError::Abort)?;
                for (change, id) in changes.iter().zip(ids) {
                    let entry = AuditEntry {
                        id,
                        at,
                        body: AuditBody::Config(ConfigRecord {
                            config: hash,
                            change: change.clone(),
                            outcome: ConfigOutcome::Applied,
                        }),
                    };
                    append_in(conn, &entry).await.map_err(audit_refused)?;
                }
                let text = to_json("operator directory", &StoredDirectory::of(&directory))
                    .map_err(|error| into_tx(StorageFailure::Codec(error)))?;
                sqlx::query(
                    "INSERT INTO surface.operator_directory (singleton, directory) \
                     VALUES (true, $1) \
                     ON CONFLICT (singleton) DO UPDATE SET directory = EXCLUDED.directory",
                )
                .bind(text)
                .execute(&mut *conn)
                .await?;
                Ok(changes)
            })
        })
        .await
        .map_err(|error| settle(error, load_failure))?;
        tracing::info!(
            changes = changes.len(),
            mode = ?config.mode(),
            at = at.as_micros(),
            "operator config loaded"
        );
        Ok(changes)
    }

    async fn operators(&self) -> Result<Vec<Operator>, OperatorStoreError> {
        Ok(self
            .directory()
            .await?
            .map(|directory| directory.operators().cloned().collect())
            .unwrap_or_default())
    }

    async fn caller(&self, identity: RequestIdentity) -> Result<Caller, CallerError> {
        let directory = self
            .directory()
            .await
            .map_err(|OperatorStoreError::Store { reason }| CallerError::Store { reason })?
            .ok_or(CallerError::NotLoaded)?;
        directory
            .caller(identity)
            .map_err(CallerError::Unauthenticated)
    }
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::ids::OperatorId;
    use crosstalk_spec::interfaces::l8_surface::Permission;
    use crosstalk_spec::interfaces::l8_surface::operators::OperatorName;

    use super::*;

    fn name(text: &str) -> OperatorName {
        match OperatorName::new(text) {
            Ok(name) => name,
            Err(error) => panic!("name: {error:?}"),
        }
    }

    fn operator(n: u128, permissions: PermissionSet) -> OperatorConfig {
        OperatorConfig {
            id: OperatorId::from_ulid(n),
            name: name(&format!("op {n}")),
            permissions,
        }
    }

    fn load(previous: Option<&OperatorDirectory>, config: &AccessConfig) -> OperatorDirectory {
        match OperatorDirectory::load(previous, config) {
            Ok((directory, _)) => directory,
            Err(error) => panic!("load: {error:?}"),
        }
    }

    fn round_trip(directory: &OperatorDirectory) {
        let stored = StoredDirectory::of(directory);
        let text = to_json("directory", &stored).unwrap_or_default();
        let decoded: StoredDirectory = match from_json("directory", &text) {
            Ok(decoded) => decoded,
            Err(error) => panic!("decode: {error}"),
        };
        assert_eq!(decoded.rebuild().as_ref(), Ok(directory));
    }

    #[test]
    fn every_directory_rebuilds_from_its_stored_form() {
        let view = PermissionSet::of([Permission::View]);
        let first = load(
            None,
            &AccessConfig::Authenticated(vec![operator(1, view), operator(2, PermissionSet::ALL)]),
        );
        round_trip(&first);
        // Operator 1 becomes former; trusted mode keeps it.
        let trusted = load(
            Some(&first),
            &AccessConfig::Trusted(TrustedOperator {
                id: OperatorId::from_ulid(2),
                name: name("root"),
            }),
        );
        round_trip(&trusted);
        // Back to authenticated with a new operator: 2 former too.
        let again = load(
            Some(&trusted),
            &AccessConfig::Authenticated(vec![operator(3, view)]),
        );
        round_trip(&again);
        assert_eq!(again.operators().count(), 3);
    }

    #[test]
    fn a_stored_form_no_load_produces_is_refused() {
        let stored = StoredDirectory {
            mode: AccessMode::Trusted,
            operators: Vec::new(),
        };
        assert!(stored.rebuild().is_err());
        let two = StoredDirectory {
            mode: AccessMode::Trusted,
            operators: vec![
                Operator {
                    id: OperatorId::from_ulid(1),
                    name: name("a"),
                    permissions: PermissionSet::ALL,
                },
                Operator {
                    id: OperatorId::from_ulid(2),
                    name: name("b"),
                    permissions: PermissionSet::ALL,
                },
            ],
        };
        assert!(two.rebuild().is_err());
    }
}
