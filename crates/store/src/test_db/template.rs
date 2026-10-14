//! Template databases: contents built once, cloned per test.
//!
//! ```text
//! TestDb::create_template   drop stale templates (drop_stale_templates); CREATE DATABASE
//!                           "crosstalk_test_tmpl_<pid>_<nanos>_<seq>" TEMPLATE template0;
//!                           COMMENT ON DATABASE … 'crosstalk test template host=<host>'
//!   … the caller migrates and fills it through the TestDb's pool and its own …
//! TestDb::freeze            close the pool; ALLOW_CONNECTIONS false; terminate every
//!                           client session still on it; wait until none is left
//!                           -> TemplateDb (never connected to again)
//! TemplateDb::instantiate   CREATE DATABASE "crosstalk_test_<pid>_<nanos>_<seq>"
//!                           TEMPLATE <template> ALLOW_CONNECTIONS true -> TestDb
//! TemplateDb::close / Drop  DROP DATABASE … WITH (FORCE)
//! ```
//!
//! `CREATE DATABASE … TEMPLATE` refuses while any session is connected to
//! the template, which is why freezing closes every pool on it first and
//! then forbids and ends every other connection. Clones are ordinary
//! [`TestDb`]s: dropped with their value.
//!
//! A template held for a whole test process (in a static, say) is never
//! dropped by its value. Its name starts with [`TEST_TEMPLATE_PREFIX`],
//! which is also under [`TEST_DB_PREFIX`](super::TEST_DB_PREFIX), so the
//! manual `crosstalk_test_%` cleanup finds it, and the next
//! [`TestDb::create_template`] drops it through [`drop_stale_templates`]:
//! when the process that made it (the pid in its name, on the host its
//! comment names) is gone, or when it is older than
//! [`STALE_TEMPLATE_AGE`].

use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sqlx::{AssertSqlSafe, PgConnection};
use tracing::{debug, info, warn};

use super::{
    CLOSE_GRACE, TestDb, TestDbError, TestDbName, block_on_in_drop, check_runtime, close_admin,
    connect_admin, drop_database,
};
use crate::config::{DatabaseUrl, PoolSettings};
use crate::error::classify;

/// Every template database's name starts with this (and so with
/// [`TEST_DB_PREFIX`](super::TEST_DB_PREFIX)).
pub const TEST_TEMPLATE_PREFIX: &str = "crosstalk_test_tmpl_";

/// A template older than this is dropped by the next
/// [`TestDb::create_template`] on the server, whoever made it.
pub const STALE_TEMPLATE_AGE: Duration = Duration::from_secs(24 * 60 * 60);

/// How long [`TestDb::freeze`] waits for the sessions on a template to end.
const FREEZE_WAIT: Duration = Duration::from_secs(30);

/// How often [`TemplateDb::instantiate`] tries while the server reports the
/// template in use (an autovacuum worker on it, say), one second apart.
const INSTANTIATE_ATTEMPTS: u32 = 30;

/// SQLSTATE `55006`: the object (here the template) is in use.
const OBJECT_IN_USE: &str = "55006";

/// What a template's comment starts with; the host follows.
const COMMENT_TAG: &str = "crosstalk test template host=";

/// A step of the template lifecycle, named in [`TestDbError::Template`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemplateStep {
    /// Recording the owning host in the template's comment.
    Comment,
    /// `ALTER DATABASE … ALLOW_CONNECTIONS false`.
    Lock,
    /// Ending the sessions still on it.
    Terminate,
    /// Creating a clone from it.
    Instantiate,
    /// Listing the server's templates.
    List,
}

impl std::fmt::Display for TemplateStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            TemplateStep::Comment => "commenting",
            TemplateStep::Lock => "refusing connections",
            TemplateStep::Terminate => "ending its sessions",
            TemplateStep::Instantiate => "cloning",
            TemplateStep::List => "listing templates",
        })
    }
}

fn template_error(name: &TestDbName, step: TemplateStep, source: sqlx::Error) -> TestDbError {
    TestDbError::Template {
        name: name.as_str().to_owned(),
        step,
        failure: classify(&source),
        source,
    }
}

impl TestDbName {
    fn fresh_template() -> Self {
        Self::fresh_with(TEST_TEMPLATE_PREFIX)
    }

    /// `name` as a template's name, when it is one: the prefix, then the
    /// pid, the creation time in nanoseconds and the sequence number.
    fn parse_template(name: &str) -> Option<(Self, TemplateBirth)> {
        let rest = name.strip_prefix(TEST_TEMPLATE_PREFIX)?;
        let mut parts = rest.split('_');
        let pid = parts.next()?.parse::<u32>().ok()?;
        let nanos = parts.next()?.parse::<u64>().ok()?;
        parts.next()?.parse::<u64>().ok()?;
        if parts.next().is_some() {
            return None;
        }
        Some((
            Self(name.to_owned()),
            TemplateBirth {
                pid,
                at: UNIX_EPOCH + Duration::from_nanos(nanos),
            },
        ))
    }
}

/// Who made a template and when, from its name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TemplateBirth {
    pid: u32,
    at: SystemTime,
}

/// This process: its host and pid.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Here {
    host: String,
    pid: u32,
}

impl Here {
    fn now() -> Self {
        Self {
            host: host_name(),
            pid: std::process::id(),
        }
    }
}

/// The host name, reduced to `[A-Za-z0-9._-]` so it is safe inside a SQL
/// string literal; `unknown` when none can be read.
fn host_name() -> String {
    let raw = std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .or_else(|| std::env::var("HOSTNAME").ok())
        .unwrap_or_default();
    let safe: String = raw
        .trim()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
        .collect();
    if safe.is_empty() {
        "unknown".to_owned()
    } else {
        safe
    }
}

/// The comment a template carries: the host that made it.
fn owner_comment(host: &str) -> String {
    format!("{COMMENT_TAG}{host}")
}

/// The host a template's comment names.
fn comment_host(comment: &str) -> Option<&str> {
    comment.strip_prefix(COMMENT_TAG)
}

/// Whether the process `pid` on this host is alive: `/proc/<pid>` exists.
/// Without `/proc`, every process counts as alive (age decides).
fn process_alive(pid: u32) -> bool {
    if !Path::new("/proc/self").exists() {
        return true;
    }
    Path::new("/proc").join(pid.to_string()).exists()
}

/// Whether a template born `birth`, commented `comment`, is stale for
/// `here` at `now`: never this process's own; older than
/// [`STALE_TEMPLATE_AGE`]; or made on this host by a process no longer
/// alive.
fn is_stale(
    birth: TemplateBirth,
    comment: Option<&str>,
    here: &Here,
    now: SystemTime,
    alive: impl Fn(u32) -> bool,
) -> bool {
    let same_host = comment.and_then(comment_host) == Some(here.host.as_str());
    if same_host && birth.pid == here.pid {
        return false;
    }
    let age = now.duration_since(birth.at).unwrap_or(Duration::ZERO);
    if age > STALE_TEMPLATE_AGE {
        return true;
    }
    same_host && !alive(birth.pid)
}

/// Drops every stale template on `admin`'s server (see the module docs)
/// and returns their names. A template being created by a live process
/// elsewhere is left alone until it is [`STALE_TEMPLATE_AGE`] old.
pub async fn drop_stale_templates(admin: &DatabaseUrl) -> Result<Vec<TestDbName>, TestDbError> {
    let mut conn = connect_admin(admin).await?;
    let listed: Result<Vec<(String, Option<String>)>, sqlx::Error> = sqlx::query_as(
        "SELECT datname::text, shobj_description(oid, 'pg_database') \
         FROM pg_database WHERE left(datname, length($1)) = $1",
    )
    .bind(TEST_TEMPLATE_PREFIX)
    .fetch_all(&mut conn)
    .await;
    close_admin(conn).await;
    let listed = listed.map_err(|source| TestDbError::Template {
        name: TEST_TEMPLATE_PREFIX.to_owned(),
        step: TemplateStep::List,
        failure: classify(&source),
        source,
    })?;
    let here = Here::now();
    let now = SystemTime::now();
    let mut dropped = Vec::new();
    for (name, comment) in listed {
        let Some((name, birth)) = TestDbName::parse_template(&name) else {
            debug!(database = %name, "not a template name this harness makes; left alone");
            continue;
        };
        if !is_stale(birth, comment.as_deref(), &here, now, process_alive) {
            continue;
        }
        drop_database(admin, &name).await?;
        info!(database = name.as_str(), "stale template database dropped");
        dropped.push(name);
    }
    Ok(dropped)
}

impl TestDb {
    /// Creates a fresh database to build a template in: as
    /// [`TestDb::create`], named under [`TEST_TEMPLATE_PREFIX`] and
    /// commented with this host, after dropping the server's stale
    /// templates (a failure to is logged, not fatal). Fill it, then
    /// [`TestDb::freeze`] it. Dropped before then, it is dropped like any
    /// test database.
    pub async fn create_template(
        admin: DatabaseUrl,
        settings: PoolSettings,
    ) -> Result<Self, TestDbError> {
        check_runtime()?;
        match drop_stale_templates(&admin).await {
            Ok(dropped) if !dropped.is_empty() => {
                info!(count = dropped.len(), "stale template databases dropped");
            }
            Ok(_) => {}
            Err(error) => warn!(error = %error, "stale template databases not dropped"),
        }
        let name = TestDbName::fresh_template();
        let mut conn = connect_admin(&admin).await?;
        let create = format!("CREATE DATABASE {} TEMPLATE template0", name.quoted());
        let created = sqlx::raw_sql(AssertSqlSafe(create))
            .execute(&mut conn)
            .await;
        if let Err(source) = created {
            close_admin(conn).await;
            return Err(TestDbError::Create {
                name: name.0,
                failure: classify(&source),
                source,
            });
        }
        // The host is reduced to `[A-Za-z0-9._-]` (`host_name`).
        let comment = format!(
            "COMMENT ON DATABASE {} IS '{}'",
            name.quoted(),
            owner_comment(&Here::now().host)
        );
        let commented = sqlx::raw_sql(AssertSqlSafe(comment))
            .execute(&mut conn)
            .await;
        close_admin(conn).await;
        if let Err(source) = commented {
            if let Err(error) = drop_database(&admin, &name).await {
                warn!(database = name.as_str(), error = %error, "could not drop a template that failed to take its comment");
            }
            return Err(template_error(&name, TemplateStep::Comment, source));
        }
        info!(database = name.as_str(), "template database created");
        Self::open_created(admin, settings, name).await
    }

    /// Turns this database into a template: closes its pool, refuses new
    /// connections and ends every client session still on it, waiting up to
    /// 30 s for them to go. Every other pool or connection on it (a bus's
    /// `LISTEN` connection included) should be closed first; one left open
    /// is terminated. Fails, dropping the database, when sessions stay.
    pub async fn freeze(mut self) -> Result<TemplateDb, TestDbError> {
        let name = self.name.take().ok_or(TestDbError::Closed)?;
        let admin = self.admin.clone();
        if tokio::time::timeout(CLOSE_GRACE, self.pool.close())
            .await
            .is_err()
        {
            debug!(
                database = name.as_str(),
                "connections still checked out after the grace period; terminating them"
            );
        }
        match lock_and_empty(&admin, &name).await {
            Ok(()) => {
                info!(database = name.as_str(), "template database frozen");
                Ok(TemplateDb {
                    admin,
                    name: Some(name),
                })
            }
            Err(error) => {
                if let Err(drop_error) = drop_database(&admin, &name).await {
                    warn!(database = name.as_str(), error = %drop_error, "could not drop a template that failed to freeze");
                }
                Err(error)
            }
        }
    }
}

/// Refuses new connections to `name`, then terminates its client sessions
/// until none is left or [`FREEZE_WAIT`] passes.
async fn lock_and_empty(admin: &DatabaseUrl, name: &TestDbName) -> Result<(), TestDbError> {
    let mut conn = connect_admin(admin).await?;
    let emptied = async {
        let lock = format!("ALTER DATABASE {} ALLOW_CONNECTIONS false", name.quoted());
        sqlx::raw_sql(AssertSqlSafe(lock))
            .execute(&mut conn)
            .await
            .map_err(|source| template_error(name, TemplateStep::Lock, source))?;
        let started = tokio::time::Instant::now();
        loop {
            let sessions = terminate_sessions(&mut conn, name)
                .await
                .map_err(|source| template_error(name, TemplateStep::Terminate, source))?;
            if sessions == 0 {
                return Ok(());
            }
            if started.elapsed() >= FREEZE_WAIT {
                return Err(TestDbError::TemplateBusy {
                    name: name.as_str().to_owned(),
                    sessions,
                    waited: FREEZE_WAIT,
                });
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    .await;
    close_admin(conn).await;
    emptied
}

/// Asks every client session on `name` to terminate and returns how many
/// there were (zero once it is empty).
async fn terminate_sessions(
    conn: &mut PgConnection,
    name: &TestDbName,
) -> Result<i64, sqlx::Error> {
    let (sessions,): (i64,) = sqlx::query_as(
        "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity \
         WHERE datname = $1 AND pid <> pg_backend_pid() AND backend_type = 'client backend'",
    )
    .bind(name.as_str())
    .fetch_one(&mut *conn)
    .await?;
    Ok(sessions)
}

/// A frozen template: no session is ever on it, so it can be cloned. Its
/// clones are fresh [`TestDb`]s. Dropped (with `DROP DATABASE … WITH
/// (FORCE)`) by [`TemplateDb::close`] or, failing that, on drop inside a
/// multi-threaded runtime; never dropped while held in a static (see the
/// module docs for how it is found later).
#[derive(Debug)]
pub struct TemplateDb {
    admin: DatabaseUrl,
    /// `Some` while the template exists and this value owns it.
    name: Option<TestDbName>,
}

impl TemplateDb {
    /// The template's name, while it exists.
    pub fn name(&self) -> Option<&TestDbName> {
        self.name.as_ref()
    }

    /// A fresh test database cloned from the template, with
    /// [`TestDb::default_pool_settings`]. Needs a multi-threaded runtime,
    /// as [`TestDb::new`] does. Retries for up to 30 s while the server
    /// reports the template in use.
    pub async fn instantiate(&self) -> Result<TestDb, TestDbError> {
        self.instantiate_with(TestDb::default_pool_settings()).await
    }

    /// As [`TemplateDb::instantiate`], with explicit pool settings.
    pub async fn instantiate_with(&self, settings: PoolSettings) -> Result<TestDb, TestDbError> {
        check_runtime()?;
        let template = self.name.as_ref().ok_or(TestDbError::Closed)?;
        let name = TestDbName::fresh();
        // Both names are built from digits, letters and underscores only
        // (`TestDbName`). The clone takes connections whatever the
        // template's setting.
        let create = format!(
            "CREATE DATABASE {} TEMPLATE {} ALLOW_CONNECTIONS true",
            name.quoted(),
            template.quoted()
        );
        let mut attempt = 1;
        loop {
            let mut conn = connect_admin(&self.admin).await?;
            let created = sqlx::raw_sql(AssertSqlSafe(create.clone()))
                .execute(&mut conn)
                .await;
            close_admin(conn).await;
            match created {
                Ok(_) => break,
                Err(source) if in_use(&source) && attempt < INSTANTIATE_ATTEMPTS => {
                    debug!(
                        template = template.as_str(),
                        attempt, "template in use; retrying the clone"
                    );
                    attempt += 1;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(source) => {
                    return Err(template_error(template, TemplateStep::Instantiate, source));
                }
            }
        }
        info!(
            database = name.as_str(),
            template = template.as_str(),
            "test database cloned from a template"
        );
        TestDb::open_created(self.admin.clone(), settings, name).await
    }

    /// Drops the template, reporting failure.
    pub async fn close(mut self) -> Result<(), TestDbError> {
        let Some(name) = self.name.take() else {
            return Ok(());
        };
        drop_database(&self.admin, &name).await
    }
}

impl Drop for TemplateDb {
    /// Drops the template if [`TemplateDb::close`] did not, when inside a
    /// multi-threaded runtime; otherwise it is left for
    /// [`drop_stale_templates`].
    fn drop(&mut self) {
        let Some(name) = self.name.take() else {
            return;
        };
        let label = name.as_str().to_owned();
        let admin = self.admin.clone();
        match block_on_in_drop(async move { drop_database(&admin, &name).await }) {
            Some(Ok(())) => {}
            Some(Err(err)) => {
                warn!(database = %label, error = %err, "template database left behind");
            }
            None => {
                warn!(database = %label, "TemplateDb dropped outside a multi-threaded runtime; template left behind");
            }
        }
    }
}

/// Whether `error` is the server refusing because the template is in use.
fn in_use(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|db| db.code())
        .is_some_and(|code| code == OBJECT_IN_USE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_db::TEST_DB_PREFIX;

    fn here() -> Here {
        Here {
            host: "node0".to_owned(),
            pid: 100,
        }
    }

    fn birth(pid: u32, age: Duration, now: SystemTime) -> TemplateBirth {
        TemplateBirth { pid, at: now - age }
    }

    #[test]
    fn template_names_are_safe_and_under_the_test_prefix() {
        let name = TestDbName::fresh_template();
        assert!(name.as_str().starts_with(TEST_TEMPLATE_PREFIX));
        assert!(name.as_str().starts_with(TEST_DB_PREFIX));
        let rest = name
            .as_str()
            .strip_prefix(TEST_TEMPLATE_PREFIX)
            .unwrap_or("!");
        assert!(
            rest.bytes().all(|b| b.is_ascii_digit() || b == b'_'),
            "{name:?}"
        );
        assert!(name.as_str().len() <= 63, "{name:?}");
    }

    #[test]
    fn a_fresh_template_name_parses_back_to_its_pid_and_birth() {
        let before = SystemTime::now();
        let name = TestDbName::fresh_template();
        let Some((parsed, birth)) = TestDbName::parse_template(name.as_str()) else {
            panic!("{name:?} does not parse");
        };
        assert_eq!(parsed, name);
        assert_eq!(birth.pid, std::process::id());
        assert!(birth.at >= before - Duration::from_secs(1));
    }

    #[test]
    fn only_template_names_parse() {
        for name in [
            "crosstalk_test_1_2_3",
            "crosstalk_test_tmpl_",
            "crosstalk_test_tmpl_1_2",
            "crosstalk_test_tmpl_1_2_3_4",
            "crosstalk_test_tmpl_a_2_3",
            "crosstalk_test_tmpl_1_2_3\"; DROP",
            "postgres",
        ] {
            assert!(TestDbName::parse_template(name).is_none(), "{name}");
        }
        assert!(TestDbName::parse_template("crosstalk_test_tmpl_1_2_3").is_some());
    }

    #[test]
    fn the_comment_names_the_host() {
        assert_eq!(comment_host(&owner_comment("node0")), Some("node0"));
        assert_eq!(comment_host("something else"), None);
    }

    #[test]
    fn host_names_are_literal_safe() {
        let host = host_name();
        assert!(!host.is_empty());
        assert!(
            host.chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')),
            "{host}"
        );
    }

    #[test]
    fn a_template_of_a_dead_process_on_this_host_is_stale() {
        let now = SystemTime::now();
        let comment = owner_comment("node0");
        let young = birth(7, Duration::from_secs(60), now);
        assert!(is_stale(young, Some(&comment), &here(), now, |_| false));
        assert!(!is_stale(young, Some(&comment), &here(), now, |_| true));
    }

    #[test]
    fn a_young_template_of_another_host_or_no_comment_is_left() {
        let now = SystemTime::now();
        let young = birth(7, Duration::from_secs(60), now);
        let elsewhere = owner_comment("other");
        assert!(!is_stale(young, Some(&elsewhere), &here(), now, |_| false));
        assert!(!is_stale(young, None, &here(), now, |_| false));
    }

    #[test]
    fn an_old_template_is_stale_wherever_it_was_made() {
        let now = SystemTime::now();
        let old = birth(7, STALE_TEMPLATE_AGE + Duration::from_secs(1), now);
        assert!(is_stale(
            old,
            Some(&owner_comment("other")),
            &here(),
            now,
            |_| true
        ));
        assert!(is_stale(old, None, &here(), now, |_| true));
    }

    #[test]
    fn this_process_never_drops_its_own_template() {
        let now = SystemTime::now();
        let comment = owner_comment("node0");
        let own = birth(100, STALE_TEMPLATE_AGE * 2, now);
        assert!(!is_stale(own, Some(&comment), &here(), now, |_| false));
    }

    #[test]
    fn this_process_is_alive() {
        assert!(process_alive(std::process::id()));
    }
}
