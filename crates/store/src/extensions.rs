//! The Postgres extensions the gateway requires.
//!
//! `vector` (pgvector) holds embeddings for L6 search and topics; `pg_trgm`
//! backs trigram text search. Both are created in `public`, which every
//! layer's `search_path` includes. TimescaleDB is deliberately not here:
//! decision D3 keeps time-series in plain partitioned tables for now.

use std::fmt;

use sqlx::PgPool;
use tracing::{debug, info};

use crate::error::{ExtensionProblem, StoreError, classify};

/// A required extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Extension {
    /// pgvector, extension name `vector`.
    Vector,
    /// Trigram matching, extension name `pg_trgm`.
    PgTrgm,
}

impl Extension {
    /// Every extension [`ensure_extensions`] creates.
    pub const REQUIRED: [Extension; 2] = [Extension::Vector, Extension::PgTrgm];

    /// The extension's name in `pg_extension`.
    pub const fn name(self) -> &'static str {
        match self {
            Extension::Vector => "vector",
            Extension::PgTrgm => "pg_trgm",
        }
    }

    /// The idempotent DDL that creates it in `public`.
    const fn create_sql(self) -> &'static str {
        match self {
            Extension::Vector => "CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public",
            Extension::PgTrgm => "CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public",
        }
    }
}

impl fmt::Display for Extension {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// An extension present in the database, and its version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstalledExtension {
    /// The extension.
    pub extension: Extension,
    /// Its `extversion`.
    pub version: String,
}

/// Creates every [`Extension::REQUIRED`] extension that is missing, and
/// returns each one's installed version.
///
/// Fails with [`StoreError::ExtensionMissing`] when the server does not
/// ship an extension ([`ExtensionProblem::NotAvailable`]) or refuses to
/// create it ([`ExtensionProblem::CreateFailed`], typically privileges).
pub async fn ensure_extensions(pool: &PgPool) -> Result<Vec<InstalledExtension>, StoreError> {
    let mut installed = Vec::with_capacity(Extension::REQUIRED.len());
    for extension in Extension::REQUIRED {
        installed.push(ensure_extension(pool, extension).await?);
    }
    Ok(installed)
}

/// Creates one extension if it is missing, and returns its version.
pub async fn ensure_extension(
    pool: &PgPool,
    extension: Extension,
) -> Result<InstalledExtension, StoreError> {
    let available: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = $1)")
            .bind(extension.name())
            .fetch_one(pool)
            .await?;
    if !available {
        return Err(StoreError::ExtensionMissing {
            extension,
            problem: ExtensionProblem::NotAvailable,
        });
    }
    let created = sqlx::raw_sql(extension.create_sql()).execute(pool).await;
    // Read the installed version whether or not the create succeeded: two
    // sessions racing `CREATE EXTENSION IF NOT EXISTS` in one database make
    // the loser fail on a catalog unique index after the winner commits, and
    // the extension then exists all the same.
    let version: Option<String> =
        sqlx::query_scalar("SELECT extversion FROM pg_extension WHERE extname = $1")
            .bind(extension.name())
            .fetch_optional(pool)
            .await?;
    let version = match (created, version) {
        (Ok(_), Some(version)) => version,
        (Err(err), Some(version)) => {
            debug!(extension = extension.name(), error = %err, "extension created concurrently");
            version
        }
        (Err(err), None) => {
            return Err(StoreError::ExtensionMissing {
                extension,
                problem: ExtensionProblem::CreateFailed(classify(&err)),
            });
        }
        (Ok(_), None) => {
            return Err(StoreError::ExtensionMissing {
                extension,
                problem: ExtensionProblem::NotAvailable,
            });
        }
    };
    info!(extension = extension.name(), version = %version, "extension ready");
    Ok(InstalledExtension { extension, version })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required_extensions_are_vector_and_pg_trgm() {
        let names: Vec<&str> = Extension::REQUIRED
            .into_iter()
            .map(Extension::name)
            .collect();
        assert_eq!(names, ["vector", "pg_trgm"]);
    }

    #[test]
    fn create_sql_is_idempotent_and_targets_public() {
        for ext in Extension::REQUIRED {
            let sql = ext.create_sql();
            assert!(sql.starts_with("CREATE EXTENSION IF NOT EXISTS "), "{sql}");
            assert!(sql.contains(ext.name()), "{sql}");
            assert!(sql.ends_with("WITH SCHEMA public"), "{sql}");
        }
    }

    #[test]
    fn timescaledb_is_not_required() {
        assert!(
            Extension::REQUIRED
                .iter()
                .all(|e| e.name() != "timescaledb")
        );
    }
}
