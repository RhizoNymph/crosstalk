//! Store configuration: the database URL from the environment (it carries
//! the password, so it is a secret and never lives in a config file) and the
//! pool sizing from structured config.

use std::ffi::OsString;
use std::fmt;
use std::num::NonZeroU32;
use std::str::FromStr;
use std::time::Duration;

use serde::Deserialize;
use sqlx::postgres::PgConnectOptions;

/// The environment variable the gateway reads its database URL from.
pub const DATABASE_URL_VAR: &str = "DATABASE_URL";

/// The environment variable the test harness reads its server URL from.
pub const TEST_DATABASE_URL_VAR: &str = "TEST_DATABASE_URL";

/// Why the store configuration was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    /// The environment variable is not set.
    #[error("{var} is not set")]
    MissingVar {
        /// The variable read.
        var: &'static str,
    },
    /// The environment variable is set but is not valid unicode.
    #[error("{var} is not valid unicode")]
    NotUnicode {
        /// The variable read.
        var: &'static str,
    },
    /// The URL is not a usable Postgres URL. The reason never repeats the
    /// URL, which may carry a password.
    #[error("{var} is not a usable postgres URL: {reason}")]
    InvalidUrl {
        /// The variable read.
        var: &'static str,
        /// What is wrong with it.
        reason: UrlProblem,
    },
    /// `min_connections` is above `max_connections`.
    #[error("pool min_connections ({min}) exceeds max_connections ({max})")]
    MinAboveMax {
        /// The configured minimum.
        min: u32,
        /// The configured maximum.
        max: NonZeroU32,
    },
    /// `max_connections` is zero.
    #[error("pool max_connections must be at least 1")]
    ZeroMaxConnections,
    /// The acquire timeout is zero, so every acquire would fail at once.
    #[error("pool acquire timeout must be non-zero")]
    ZeroAcquireTimeout,
}

/// What is wrong with a database URL.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UrlProblem {
    /// The scheme is not `postgres://` or `postgresql://`.
    #[error("the scheme must be postgres:// or postgresql://")]
    Scheme,
    /// The driver refused to parse it.
    #[error("{0}")]
    Unparsable(String),
}

/// A parsed Postgres URL. `Debug` shows the host, port, database and user,
/// never the password.
#[derive(Clone)]
pub struct DatabaseUrl {
    options: PgConnectOptions,
}

impl DatabaseUrl {
    /// Parses a Postgres URL. `var` names where it came from, for the error.
    pub fn parse(var: &'static str, raw: &str) -> Result<Self, ConfigError> {
        let invalid = |reason| ConfigError::InvalidUrl { var, reason };
        let has_scheme = raw.starts_with("postgres://") || raw.starts_with("postgresql://");
        if !has_scheme {
            return Err(invalid(UrlProblem::Scheme));
        }
        let options = PgConnectOptions::from_str(raw)
            .map_err(|e| invalid(UrlProblem::Unparsable(e.to_string())))?;
        Ok(Self { options })
    }

    /// Reads and parses the URL in `var`, looking variables up with
    /// `lookup` (the process environment in [`DatabaseUrl::from_env`]).
    pub fn from_lookup(
        var: &'static str,
        lookup: impl FnOnce(&str) -> Option<OsString>,
    ) -> Result<Self, ConfigError> {
        let raw = lookup(var).ok_or(ConfigError::MissingVar { var })?;
        let raw = raw
            .into_string()
            .map_err(|_| ConfigError::NotUnicode { var })?;
        Self::parse(var, &raw)
    }

    /// Reads and parses the URL in the process environment variable `var`.
    pub fn from_env(var: &'static str) -> Result<Self, ConfigError> {
        Self::from_lookup(var, |name| std::env::var_os(name))
    }

    /// The driver's connect options.
    pub fn connect_options(&self) -> &PgConnectOptions {
        &self.options
    }

    /// The same server, a different database.
    pub fn with_database(&self, database: &str) -> Self {
        Self {
            options: self.options.clone().database(database),
        }
    }

    /// `host:port/database` for logs and errors (no user, no password).
    pub fn target(&self) -> String {
        format!(
            "{}:{}/{}",
            self.options.get_host(),
            self.options.get_port(),
            self.options.get_database().unwrap_or("")
        )
    }
}

impl fmt::Debug for DatabaseUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatabaseUrl")
            .field("host", &self.options.get_host())
            .field("port", &self.options.get_port())
            .field("database", &self.options.get_database())
            .field("username", &self.options.get_username())
            .finish_non_exhaustive()
    }
}

/// Connection pool sizing. Built only through [`PoolSettings::new`] (or by
/// deserializing, which goes through it): `max_connections` is at least 1,
/// `min_connections` is at most `max_connections`, and the acquire timeout
/// is non-zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(try_from = "PoolSettingsRaw")]
pub struct PoolSettings {
    max_connections: NonZeroU32,
    min_connections: u32,
    acquire_timeout: Duration,
}

/// The JSON/YAML shape of [`PoolSettings`]:
/// `{"max_connections": 10, "min_connections": 0, "acquire_timeout_ms": 5000}`.
/// `max_connections` is required; the others default to 0 and 5000.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PoolSettingsRaw {
    max_connections: u32,
    #[serde(default)]
    min_connections: u32,
    #[serde(default = "default_acquire_timeout_ms")]
    acquire_timeout_ms: u64,
}

const DEFAULT_ACQUIRE_TIMEOUT_MS: u64 = 5_000;

fn default_acquire_timeout_ms() -> u64 {
    DEFAULT_ACQUIRE_TIMEOUT_MS
}

impl TryFrom<PoolSettingsRaw> for PoolSettings {
    type Error = ConfigError;

    fn try_from(raw: PoolSettingsRaw) -> Result<Self, Self::Error> {
        let max = NonZeroU32::new(raw.max_connections).ok_or(ConfigError::ZeroMaxConnections)?;
        Self::new(
            max,
            raw.min_connections,
            Duration::from_millis(raw.acquire_timeout_ms),
        )
    }
}

impl PoolSettings {
    /// Checked constructor.
    pub fn new(
        max_connections: NonZeroU32,
        min_connections: u32,
        acquire_timeout: Duration,
    ) -> Result<Self, ConfigError> {
        if min_connections > max_connections.get() {
            return Err(ConfigError::MinAboveMax {
                min: min_connections,
                max: max_connections,
            });
        }
        if acquire_timeout.is_zero() {
            return Err(ConfigError::ZeroAcquireTimeout);
        }
        Ok(Self {
            max_connections,
            min_connections,
            acquire_timeout,
        })
    }

    /// The most connections the pool opens.
    pub fn max_connections(&self) -> NonZeroU32 {
        self.max_connections
    }

    /// The connections the pool keeps open while idle.
    pub fn min_connections(&self) -> u32 {
        self.min_connections
    }

    /// How long an acquire waits for a free connection.
    pub fn acquire_timeout(&self) -> Duration {
        self.acquire_timeout
    }
}

impl Default for PoolSettings {
    /// Ten connections, none kept idle, a five-second acquire timeout.
    fn default() -> Self {
        Self {
            max_connections: NonZeroU32::MIN.saturating_add(9),
            min_connections: 0,
            acquire_timeout: Duration::from_millis(DEFAULT_ACQUIRE_TIMEOUT_MS),
        }
    }
}

/// Everything [`crate::Store::connect`] needs.
#[derive(Debug, Clone)]
pub struct StoreConfig {
    url: DatabaseUrl,
    pool: PoolSettings,
}

impl StoreConfig {
    /// A config from parts.
    pub fn new(url: DatabaseUrl, pool: PoolSettings) -> Self {
        Self { url, pool }
    }

    /// Reads [`DATABASE_URL_VAR`] from the process environment; the pool
    /// settings come from structured config.
    pub fn from_env(pool: PoolSettings) -> Result<Self, ConfigError> {
        Ok(Self::new(DatabaseUrl::from_env(DATABASE_URL_VAR)?, pool))
    }

    /// As [`StoreConfig::from_env`], with an explicit variable lookup.
    pub fn from_lookup(
        pool: PoolSettings,
        lookup: impl FnOnce(&str) -> Option<OsString>,
    ) -> Result<Self, ConfigError> {
        Ok(Self::new(
            DatabaseUrl::from_lookup(DATABASE_URL_VAR, lookup)?,
            pool,
        ))
    }

    /// The database URL.
    pub fn url(&self) -> &DatabaseUrl {
        &self.url
    }

    /// The pool sizing.
    pub fn pool(&self) -> &PoolSettings {
        &self.pool
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(value: &'static str) -> impl FnOnce(&str) -> Option<OsString> {
        move |var| {
            assert_eq!(var, DATABASE_URL_VAR);
            Some(OsString::from(value))
        }
    }

    fn nz(n: u32) -> NonZeroU32 {
        NonZeroU32::new(n).unwrap_or(NonZeroU32::MIN)
    }

    #[test]
    fn reads_the_database_url_from_the_environment() -> Result<(), ConfigError> {
        let config = StoreConfig::from_lookup(
            PoolSettings::default(),
            env("postgres://crosstalk:secret@db.local:6543/gateway"),
        )?;
        let options = config.url().connect_options();
        assert_eq!(options.get_host(), "db.local");
        assert_eq!(options.get_port(), 6543);
        assert_eq!(options.get_database(), Some("gateway"));
        assert_eq!(options.get_username(), "crosstalk");
        assert_eq!(config.url().target(), "db.local:6543/gateway");
        Ok(())
    }

    #[test]
    fn accepts_the_postgresql_scheme() {
        assert!(DatabaseUrl::parse(DATABASE_URL_VAR, "postgresql://u@h/d").is_ok());
    }

    #[test]
    fn missing_variable_is_typed() {
        let got = StoreConfig::from_lookup(PoolSettings::default(), |_| None);
        assert_eq!(
            got.err(),
            Some(ConfigError::MissingVar {
                var: DATABASE_URL_VAR
            })
        );
    }

    #[cfg(unix)]
    #[test]
    fn non_unicode_variable_is_typed() {
        use std::os::unix::ffi::OsStringExt;
        let got = StoreConfig::from_lookup(PoolSettings::default(), |_| {
            Some(OsString::from_vec(vec![0x70, 0xff, 0xfe]))
        });
        assert_eq!(
            got.err(),
            Some(ConfigError::NotUnicode {
                var: DATABASE_URL_VAR
            })
        );
    }

    #[test]
    fn wrong_scheme_is_refused() {
        for raw in ["mysql://u:p@h/d", "http://h/d", "h:5432/d", ""] {
            assert_eq!(
                DatabaseUrl::parse(DATABASE_URL_VAR, raw).err(),
                Some(ConfigError::InvalidUrl {
                    var: DATABASE_URL_VAR,
                    reason: UrlProblem::Scheme
                }),
                "{raw:?}"
            );
        }
    }

    #[test]
    fn unparsable_url_is_refused_without_echoing_the_password() {
        let raw = "postgres://u:hunter2@h:notaport/d";
        let err = DatabaseUrl::parse(DATABASE_URL_VAR, raw).err();
        let Some(ConfigError::InvalidUrl {
            reason: UrlProblem::Unparsable(reason),
            ..
        }) = &err
        else {
            panic!("expected an unparsable URL error, got {err:?}");
        };
        assert!(!reason.contains("hunter2"), "{reason}");
        assert!(
            !err.map(|e| e.to_string())
                .unwrap_or_default()
                .contains("hunter2")
        );
    }

    #[test]
    fn debug_never_shows_the_password() -> Result<(), ConfigError> {
        let url = DatabaseUrl::parse(DATABASE_URL_VAR, "postgres://u:hunter2@h:5432/d")?;
        let shown = format!("{url:?}");
        assert!(!shown.contains("hunter2"), "{shown}");
        assert!(shown.contains("\"h\""), "{shown}");
        Ok(())
    }

    #[test]
    fn with_database_keeps_the_server() -> Result<(), ConfigError> {
        let url = DatabaseUrl::parse(DATABASE_URL_VAR, "postgres://u:p@h:5433/postgres")?;
        let other = url.with_database("crosstalk_test_1");
        assert_eq!(other.target(), "h:5433/crosstalk_test_1");
        assert_eq!(other.connect_options().get_username(), "u");
        Ok(())
    }

    #[test]
    fn pool_settings_parse_from_json() -> Result<(), serde_json::Error> {
        let settings: PoolSettings = serde_json::from_str(
            r#"{"max_connections": 20, "min_connections": 2, "acquire_timeout_ms": 1500}"#,
        )?;
        assert_eq!(settings.max_connections(), nz(20));
        assert_eq!(settings.min_connections(), 2);
        assert_eq!(settings.acquire_timeout(), Duration::from_millis(1500));
        Ok(())
    }

    #[test]
    fn pool_settings_defaults_fill_optional_fields() -> Result<(), serde_json::Error> {
        let settings: PoolSettings = serde_json::from_str(r#"{"max_connections": 3}"#)?;
        assert_eq!(settings.max_connections(), nz(3));
        assert_eq!(settings.min_connections(), 0);
        assert_eq!(settings.acquire_timeout(), Duration::from_secs(5));
        Ok(())
    }

    #[test]
    fn pool_settings_refuse_bad_shapes() {
        let refused = [
            r#"{}"#,
            r#"{"max_connections": 0}"#,
            r#"{"max_connections": 2, "min_connections": 3}"#,
            r#"{"max_connections": 2, "acquire_timeout_ms": 0}"#,
            r#"{"max_connections": 2, "idle": true}"#,
            r#"{"max_connections": -1}"#,
            r#"{"max_connections": "10"}"#,
        ];
        for json in refused {
            assert!(
                serde_json::from_str::<PoolSettings>(json).is_err(),
                "{json} should be refused"
            );
        }
    }

    #[test]
    fn pool_settings_checked_constructor() {
        assert_eq!(
            PoolSettings::new(nz(2), 3, Duration::from_secs(1)),
            Err(ConfigError::MinAboveMax { min: 3, max: nz(2) })
        );
        assert_eq!(
            PoolSettings::new(nz(2), 0, Duration::ZERO),
            Err(ConfigError::ZeroAcquireTimeout)
        );
        assert!(PoolSettings::new(nz(2), 2, Duration::from_millis(1)).is_ok());
    }

    #[test]
    fn default_pool_settings_are_valid() {
        let d = PoolSettings::default();
        assert_eq!(
            PoolSettings::new(
                d.max_connections(),
                d.min_connections(),
                d.acquire_timeout()
            ),
            Ok(d)
        );
        assert_eq!(d.max_connections(), nz(10));
    }
}
