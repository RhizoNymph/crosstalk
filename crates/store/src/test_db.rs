//! The test database harness.
//!
//! [`TestDb::new`] creates a fresh, uniquely named database on the server
//! that `TEST_DATABASE_URL` points at, and drops it again when the `TestDb`
//! is closed or dropped. Each test gets a whole database, not a schema,
//! because the layer schemas have fixed names (`ingress`, `flow`, …): two
//! tests migrating the same layer would otherwise collide. Databases are
//! independent, so tests run in parallel.
//!
//! Database tests are gated: when `TEST_DATABASE_URL` is unset,
//! [`TestDb::new_or_skip`] prints why and returns `None`, and the test
//! returns early as a pass. `scripts/test-db.sh` starts a disposable server.
//!
//! The URL comes from the process environment or, when the environment does
//! not set it, from a [`TEST_ENV_FILE`] in the working directory or one of its
//! ancestors (the workspace root, for `cargo test`). The file is gitignored,
//! so a machine can point its tests at a server without exporting anything.

use std::ffi::OsString;
use std::io::Write;
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sqlx::{AssertSqlSafe, Connection, PgConnection, PgPool};
use tokio::runtime::{Handle, RuntimeFlavor};
use tracing::{debug, info, warn};

use crate::config::{ConfigError, DatabaseUrl, PoolSettings, TEST_DATABASE_URL_VAR};
use crate::error::{DbFailure, StoreError, classify};
use crate::extensions::{InstalledExtension, ensure_extensions};
use crate::layer::Layer;
use crate::migrate::{Migrations, migrate};
use crate::pool::{Store, open_pool};

/// The dotenv-style file that supplies `TEST_DATABASE_URL` when the process
/// environment does not: `NAME=value` lines, `#` comments and blank lines.
pub const TEST_ENV_FILE: &str = ".env.test";

/// The admin URL of the test server: the environment variable, else the
/// nearest [`TEST_ENV_FILE`] above the working directory that sets it.
fn admin_url() -> Result<DatabaseUrl, ConfigError> {
    DatabaseUrl::from_lookup(TEST_DATABASE_URL_VAR, |name| {
        std::env::var_os(name).or_else(|| env_file_value(name))
    })
}

fn env_file_value(name: &str) -> Option<OsString> {
    let cwd = match std::env::current_dir() {
        Ok(dir) => dir,
        Err(err) => {
            warn!(error = %err, "no working directory to search for {TEST_ENV_FILE}");
            return None;
        }
    };
    cwd.ancestors()
        .find_map(|dir| read_env_file(&dir.join(TEST_ENV_FILE), name))
        .map(OsString::from)
}

fn read_env_file(path: &Path, name: &str) -> Option<String> {
    match std::fs::read_to_string(path) {
        Ok(text) => {
            let value = env_file_lookup(&text, name);
            if value.is_some() {
                debug!(path = %path.display(), var = name, "read from env file");
            }
            value
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        Err(err) => {
            warn!(path = %path.display(), error = %err, "could not read env file");
            None
        }
    }
}

/// The value `text` gives `name`: the last `NAME=value` line for it, with
/// surrounding whitespace, an optional leading `export ` and one pair of
/// matching quotes removed. Blank lines and `#` comments are skipped.
fn env_file_lookup(text: &str, name: &str) -> Option<String> {
    text.lines()
        .rev()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .find_map(|line| {
            let line = line.strip_prefix("export ").map_or(line, str::trim_start);
            let (key, value) = line.split_once('=')?;
            (key.trim() == name).then(|| unquote(value.trim()).to_owned())
        })
}

fn unquote(value: &str) -> &str {
    ['"', '\'']
        .iter()
        .find_map(|q| value.strip_prefix(*q)?.strip_suffix(*q))
        .unwrap_or(value)
}

/// Every test database's name starts with this.
pub const TEST_DB_PREFIX: &str = "crosstalk_test_";

/// A test database name: [`TEST_DB_PREFIX`], the process id, the creation
/// time in nanoseconds and a per-process sequence number. Only digits and
/// underscores follow the prefix, so it is a safe SQL identifier, and it is
/// at most 63 bytes, Postgres's identifier limit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestDbName(String);

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

impl TestDbName {
    fn fresh() -> Self {
        let seq = SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        Self(format!(
            "{TEST_DB_PREFIX}{}_{nanos}_{seq}",
            std::process::id()
        ))
    }

    /// The name, unquoted.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn quoted(&self) -> String {
        format!("\"{}\"", self.0)
    }
}

/// Why the runtime cannot host a [`TestDb`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RuntimeProblem {
    /// Not inside a tokio runtime.
    #[error("TestDb must be created inside a tokio runtime")]
    NoRuntime,
    /// A current-thread runtime cannot block in `Drop` to drop the database.
    #[error(
        "TestDb needs a multi-threaded runtime to drop its database on drop; \
         use #[tokio::test(flavor = \"multi_thread\")]"
    )]
    CurrentThread,
}

/// How the test harness fails.
#[derive(Debug, thiserror::Error)]
pub enum TestDbError {
    /// `TEST_DATABASE_URL` is not set; database tests are skipped.
    #[error("{TEST_DATABASE_URL_VAR} is not set; run scripts/test-db.sh and export it")]
    NotConfigured,
    /// `TEST_DATABASE_URL` is set but unusable.
    #[error("test database configuration: {0}")]
    Config(ConfigError),
    /// The runtime cannot host a `TestDb`.
    #[error(transparent)]
    Runtime(#[from] RuntimeProblem),
    /// `CREATE DATABASE` failed.
    #[error("creating test database {name}: {failure}")]
    Create {
        /// The database name.
        name: String,
        /// The classified cause.
        failure: DbFailure,
        /// The driver error.
        #[source]
        source: sqlx::Error,
    },
    /// `DROP DATABASE` failed.
    #[error("dropping test database {name}: {failure}")]
    Drop {
        /// The database name.
        name: String,
        /// The classified cause.
        failure: DbFailure,
        /// The driver error.
        #[source]
        source: sqlx::Error,
    },
    /// Connecting, migrating or creating extensions failed.
    #[error(transparent)]
    Store(#[from] StoreError),
}

impl From<ConfigError> for TestDbError {
    fn from(err: ConfigError) -> Self {
        match err {
            ConfigError::MissingVar { .. } => TestDbError::NotConfigured,
            other => TestDbError::Config(other),
        }
    }
}

/// A fresh database for one test. Dropped (with `DROP DATABASE … WITH
/// (FORCE)`) by [`TestDb::close`] or, failing that, on drop.
#[derive(Debug)]
pub struct TestDb {
    pool: PgPool,
    url: DatabaseUrl,
    admin: DatabaseUrl,
    /// `Some` while the database exists and this value owns it.
    name: Option<TestDbName>,
}

impl TestDb {
    /// The pool size every test database gets by default: small, so many
    /// tests in parallel stay under the server's `max_connections`.
    pub fn default_pool_settings() -> PoolSettings {
        PoolSettings::new(
            NonZeroU32::MIN.saturating_add(3),
            0,
            Duration::from_secs(30),
        )
        // Infallible: 0 <= 4 and the timeout is non-zero; the default is
        // only the type-level fallback.
        .unwrap_or_default()
    }

    /// Creates a fresh database on the `TEST_DATABASE_URL` server, with
    /// [`TestDb::default_pool_settings`]. Needs a multi-threaded tokio
    /// runtime (`#[tokio::test(flavor = "multi_thread")]`).
    pub async fn new() -> Result<Self, TestDbError> {
        Self::with_pool_settings(Self::default_pool_settings()).await
    }

    /// As [`TestDb::new`], with explicit pool settings.
    pub async fn with_pool_settings(settings: PoolSettings) -> Result<Self, TestDbError> {
        let admin = admin_url()?;
        Self::create(admin, settings).await
    }

    /// As [`TestDb::new`], except that an unset `TEST_DATABASE_URL` prints
    /// `skipping <test>: …` to stderr and returns `Ok(None)`:
    ///
    /// ```no_run
    /// # use crosstalk_store::{TestDb, TestDbError};
    /// # async fn my_test() -> Result<(), TestDbError> {
    /// let Some(db) = TestDb::new_or_skip("my_test").await? else {
    ///     return Ok(());
    /// };
    /// // ... use db.pool() ...
    /// db.close().await
    /// # }
    /// ```
    pub async fn new_or_skip(test: &str) -> Result<Option<Self>, TestDbError> {
        match Self::new().await {
            Ok(db) => Ok(Some(db)),
            Err(TestDbError::NotConfigured) => {
                // Straight to the stderr handle, not `eprintln!`: libtest
                // captures the macros' output of passing tests, and the skip
                // reason should show in a plain `cargo test` run.
                let line = format!(
                    "skipping {test}: {TEST_DATABASE_URL_VAR} is not set \
                     (run scripts/test-db.sh and export the URL it prints, \
                     or put it in {TEST_ENV_FILE})\n"
                );
                if let Err(err) = std::io::stderr().write_all(line.as_bytes()) {
                    debug!(error = %err, "could not print the skip reason");
                }
                Ok(None)
            }
            Err(other) => Err(other),
        }
    }

    /// Creates a fresh database on the server `admin` points at.
    pub async fn create(admin: DatabaseUrl, settings: PoolSettings) -> Result<Self, TestDbError> {
        check_runtime()?;
        let name = TestDbName::fresh();
        let mut conn = connect_admin(&admin).await?;
        // The name is built from digits and underscores only (`TestDbName`).
        // template0 keeps the database free of anything installed in
        // template1, extensions included.
        let create = format!("CREATE DATABASE {} TEMPLATE template0", name.quoted());
        let created = sqlx::raw_sql(AssertSqlSafe(create))
            .execute(&mut conn)
            .await;
        close_admin(conn).await;
        if let Err(source) = created {
            return Err(TestDbError::Create {
                name: name.0,
                failure: classify(&source),
                source,
            });
        }
        let url = admin.with_database(name.as_str());
        let pool = match open_pool(&url, &settings).await {
            Ok(pool) => pool,
            Err(err) => {
                if let Err(drop_err) = drop_database(&admin, &name).await {
                    warn!(database = name.as_str(), error = %drop_err, "could not drop test database after a failed connect");
                }
                return Err(err.into());
            }
        };
        info!(database = name.as_str(), "test database created");
        Ok(Self {
            pool,
            url,
            admin,
            name: Some(name),
        })
    }

    /// The pool on the test database.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// A [`Store`] sharing the test database's pool.
    pub fn store(&self) -> Store {
        Store::from_pool(self.pool.clone())
    }

    /// The test database's URL.
    pub fn url(&self) -> &DatabaseUrl {
        &self.url
    }

    /// The test database's name, while it exists.
    pub fn name(&self) -> Option<&TestDbName> {
        self.name.as_ref()
    }

    /// Runs `layer`'s migrations into the test database; see [`migrate`].
    pub async fn migrate(
        &self,
        layer: Layer,
        migrations: Migrations<'_>,
    ) -> Result<(), StoreError> {
        migrate(&self.pool, layer, migrations).await
    }

    /// Creates the required extensions in the test database.
    pub async fn ensure_extensions(&self) -> Result<Vec<InstalledExtension>, StoreError> {
        ensure_extensions(&self.pool).await
    }

    /// Closes the pool and drops the database, reporting failure.
    pub async fn close(mut self) -> Result<(), TestDbError> {
        let Some(name) = self.name.take() else {
            return Ok(());
        };
        teardown(&self.pool, &self.admin, &name).await
    }
}

impl Drop for TestDb {
    /// Drops the database if [`TestDb::close`] did not. Blocks the current
    /// thread with `block_in_place`, which is why `TestDb` requires a
    /// multi-threaded runtime.
    fn drop(&mut self) {
        let Some(name) = self.name.take() else {
            return;
        };
        let label = name.as_str().to_owned();
        let pool = self.pool.clone();
        let admin = self.admin.clone();
        let done = block_on_in_drop(async move { teardown(&pool, &admin, &name).await });
        match done {
            Some(Ok(())) => {}
            Some(Err(err)) => {
                warn!(database = %label, error = %err, "test database left behind");
                eprintln!("crosstalk-store: {err}");
            }
            None => {
                warn!(database = %label, "TestDb dropped outside a multi-threaded runtime; test database left behind");
            }
        }
    }
}

/// Runs `fut` to completion from synchronous code inside a multi-threaded
/// tokio runtime, or returns `None` when there is no such runtime.
fn block_on_in_drop<F: std::future::Future>(fut: F) -> Option<F::Output> {
    let handle = Handle::try_current().ok()?;
    if handle.runtime_flavor() != RuntimeFlavor::MultiThread {
        return None;
    }
    Some(tokio::task::block_in_place(|| handle.block_on(fut)))
}

fn check_runtime() -> Result<(), RuntimeProblem> {
    let handle = Handle::try_current().map_err(|_| RuntimeProblem::NoRuntime)?;
    if handle.runtime_flavor() == RuntimeFlavor::MultiThread {
        Ok(())
    } else {
        Err(RuntimeProblem::CurrentThread)
    }
}

async fn connect_admin(admin: &DatabaseUrl) -> Result<PgConnection, StoreError> {
    PgConnection::connect_with(admin.connect_options())
        .await
        .map_err(|source| StoreError::Connect {
            target: admin.target(),
            failure: classify(&source),
            source,
        })
}

async fn close_admin(conn: PgConnection) {
    if let Err(err) = conn.close().await {
        warn!(error = %err, "closing the test admin connection failed");
    }
}

/// How long teardown waits for checked-out connections to come back before
/// forcing the drop anyway.
const CLOSE_GRACE: Duration = Duration::from_secs(1);

/// Closes the pool (giving checked-out connections [`CLOSE_GRACE`] to
/// return) and drops the database, forcing off any session still on it.
async fn teardown(
    pool: &PgPool,
    admin: &DatabaseUrl,
    name: &TestDbName,
) -> Result<(), TestDbError> {
    if tokio::time::timeout(CLOSE_GRACE, pool.close())
        .await
        .is_err()
    {
        debug!(
            database = name.as_str(),
            "connections still checked out after the grace period; forcing the drop"
        );
    }
    drop_database(admin, name).await
}

async fn drop_database(admin: &DatabaseUrl, name: &TestDbName) -> Result<(), TestDbError> {
    let mut conn = connect_admin(admin).await?;
    let drop = format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", name.quoted());
    // The name is built from digits and underscores only (`TestDbName`).
    let dropped = sqlx::raw_sql(AssertSqlSafe(drop)).execute(&mut conn).await;
    close_admin(conn).await;
    match dropped {
        Ok(_) => {
            info!(database = name.as_str(), "test database dropped");
            Ok(())
        }
        Err(source) => Err(TestDbError::Drop {
            name: name.0.clone(),
            failure: classify(&source),
            source,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn names_are_unique_safe_identifiers() {
        let names: BTreeSet<String> = (0..1000).map(|_| TestDbName::fresh().0).collect();
        assert_eq!(names.len(), 1000);
        for name in &names {
            let rest = name.strip_prefix(TEST_DB_PREFIX).unwrap_or("!");
            assert!(
                !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit() || b == b'_'),
                "{name}"
            );
            assert!(name.len() <= 63, "{name} exceeds the identifier limit");
        }
    }

    #[test]
    fn missing_variable_means_not_configured() {
        let err = TestDbError::from(ConfigError::MissingVar {
            var: TEST_DATABASE_URL_VAR,
        });
        assert!(matches!(err, TestDbError::NotConfigured));
        let err = TestDbError::from(ConfigError::NotUnicode {
            var: TEST_DATABASE_URL_VAR,
        });
        assert!(matches!(err, TestDbError::Config(_)));
    }

    #[test]
    fn env_file_lookup_reads_the_named_variable() {
        let text = "# test server\n\nOTHER=x\nTEST_DATABASE_URL=postgres://a@h:1/d\n";
        assert_eq!(
            env_file_lookup(text, TEST_DATABASE_URL_VAR).as_deref(),
            Some("postgres://a@h:1/d")
        );
        assert_eq!(env_file_lookup(text, "MISSING"), None);
    }

    #[test]
    fn env_file_lookup_strips_export_quotes_and_whitespace() {
        let text = "  export  TEST_DATABASE_URL = \"postgres://q@h:1/d\"  \n";
        assert_eq!(
            env_file_lookup(text, TEST_DATABASE_URL_VAR).as_deref(),
            Some("postgres://q@h:1/d")
        );
        assert_eq!(env_file_lookup("V='x'", "V").as_deref(), Some("x"));
        assert_eq!(env_file_lookup("V=\"x'", "V").as_deref(), Some("\"x'"));
    }

    #[test]
    fn env_file_lookup_takes_the_last_assignment_and_skips_comments() {
        let text = "V=first\n# V=commented\nV=second\nVX=other\n";
        assert_eq!(env_file_lookup(text, "V").as_deref(), Some("second"));
    }

    #[test]
    fn env_file_lookup_ignores_lines_without_equals() {
        assert_eq!(env_file_lookup("V\nexport V\n", "V"), None);
    }

    #[test]
    fn read_env_file_treats_a_missing_file_as_unset() {
        let path = std::env::temp_dir()
            .join("crosstalk-no-such-dir")
            .join(TEST_ENV_FILE);
        assert_eq!(read_env_file(&path, TEST_DATABASE_URL_VAR), None);
    }

    #[test]
    fn default_pool_settings_are_small() {
        assert_eq!(TestDb::default_pool_settings().max_connections().get(), 4);
    }

    #[test]
    fn outside_a_runtime_is_refused() {
        assert_eq!(check_runtime(), Err(RuntimeProblem::NoRuntime));
        assert!(block_on_in_drop(async { 1 }).is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn current_thread_runtime_is_refused() {
        assert_eq!(check_runtime(), Err(RuntimeProblem::CurrentThread));
        assert!(block_on_in_drop(async { 1 }).is_none());
    }

    /// The mechanism `Drop` relies on: blocking on async work from sync
    /// code inside a multi-threaded `#[tokio::test]`.
    #[tokio::test(flavor = "multi_thread")]
    async fn multi_thread_runtime_can_block_in_drop() {
        assert_eq!(check_runtime(), Ok(()));
        let got = block_on_in_drop(async {
            tokio::time::sleep(Duration::from_millis(1)).await;
            7
        });
        assert_eq!(got, Some(7));
    }
}
