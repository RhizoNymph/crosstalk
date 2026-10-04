//! The fixture's audit log: spec `AuditEntry`s, append-only.
//!
//! [`AuditLog`] has no update or delete, as the spec's `AuditLog` has none
//! (`surface.audit.append-only`): an entry is only ever appended, and an
//! append is idempotent on the entry's id, a different entry under a used
//! id being `IdReused`. Entries are kept in append order; `audit` sorts
//! them newest first when it pages ([`super::queries::lists`]).

use crosstalk_spec::ids::{AuditId, ConfigHash};
use crosstalk_spec::interfaces::l8_surface::audit::{
    AuditBody, AuditEntry, AuditError, ConfigChange, ConfigOutcome, ConfigRecord, OperatorRecord,
};
use crosstalk_spec::interfaces::l8_surface::export::ExportRecord;
use crosstalk_spec::support::{Blake3, Timestamp};

use super::clock::Mint;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct AuditLog {
    entries: Vec<AuditEntry>,
}

impl AuditLog {
    /// Appends `entry`. The same entry again is a no-op; another entry
    /// under its id is `IdReused`.
    pub fn append(&mut self, entry: AuditEntry) -> Result<(), AuditError> {
        match self.entries.iter().find(|e| e.id == entry.id) {
            Some(existing) if *existing == entry => Ok(()),
            Some(_) => Err(AuditError::IdReused(entry.id)),
            None => {
                self.entries.push(entry);
                Ok(())
            }
        }
    }

    /// Every entry, in append order.
    pub fn entries(&self) -> &[AuditEntry] {
        &self.entries
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Appends `body` at `at` under a newly minted id, and returns the id.
    pub fn record(
        &mut self,
        mint: &mut Mint,
        at: Timestamp,
        body: AuditBody,
    ) -> Result<AuditId, AuditError> {
        let id = AuditId::from_ulid(mint.ulid(at));
        self.append(AuditEntry { id, at, body })?;
        Ok(id)
    }

    /// One operator action call.
    pub fn operator(
        &mut self,
        mint: &mut Mint,
        at: Timestamp,
        record: OperatorRecord,
    ) -> Result<AuditId, AuditError> {
        self.record(mint, at, AuditBody::Operator(record))
    }

    /// One export event: its refusal, its start, its end or its
    /// abandonment.
    pub fn export(
        &mut self,
        mint: &mut Mint,
        at: Timestamp,
        record: ExportRecord,
    ) -> Result<AuditId, AuditError> {
        self.record(mint, at, AuditBody::Export(record))
    }

    /// Every change one config load made, each applied, in order.
    pub fn config_load(
        &mut self,
        mint: &mut Mint,
        at: Timestamp,
        config: ConfigHash,
        changes: impl IntoIterator<Item = ConfigChange>,
    ) -> Result<(), AuditError> {
        for change in changes {
            let record = ConfigRecord {
                config,
                change,
                outcome: ConfigOutcome::Applied,
            };
            self.record(mint, at, AuditBody::Config(record))?;
        }
        Ok(())
    }
}

/// The hash of the fixture's `n`th config document: a stand-in digest, the
/// same for every seed.
pub fn config_hash(n: u8) -> ConfigHash {
    ConfigHash::from_digest(Blake3::from_bytes([n; 32]))
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::interfaces::l8_surface::audit::ConfigChange;
    use crosstalk_spec::interfaces::l8_surface::operators::AccessMode;

    use super::*;

    fn entry(id: u128, mode: AccessMode) -> AuditEntry {
        AuditEntry {
            id: AuditId::from_ulid(id),
            at: Timestamp::from_micros(1),
            body: AuditBody::Config(ConfigRecord {
                config: config_hash(1),
                change: ConfigChange::SetAccessMode(mode),
                outcome: ConfigOutcome::Applied,
            }),
        }
    }

    #[test]
    fn appends_are_idempotent_and_ids_are_never_reused() {
        let mut log = AuditLog::default();
        log.append(entry(1, AccessMode::Trusted)).expect("append");
        log.append(entry(1, AccessMode::Trusted))
            .expect("same entry");
        assert_eq!(log.len(), 1);
        assert_eq!(
            log.append(entry(1, AccessMode::Authenticated)),
            Err(AuditError::IdReused(AuditId::from_ulid(1)))
        );
        assert_eq!(log.entries(), [entry(1, AccessMode::Trusted)]);
    }
}
