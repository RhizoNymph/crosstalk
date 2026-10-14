//! The longer statement bound retention deletes run under.
//!
//! The gateway runs every pipeline connection with a `statement_timeout`
//! (60 s by default) and records two session settings beside it:
//! [`STATEMENT_BOUND`], that bound, and [`RETENTION_BOUND`], the much longer
//! one retention deletes get (15 min by default). A retention delete (the
//! bus's log prune, the provenance and ledger expiries) is a single
//! unbatched `DELETE` whose size grows with how long retention did not run:
//! under the pipeline's bound it could be cancelled every round after a long
//! stop, silently stopping retention. So each runs in a transaction that
//! first calls [`raise_for_retention`] (`SET LOCAL statement_timeout` to the
//! retention bound) and, when the transaction does other work after it,
//! [`restore_after_retention`]. `lock_timeout` is left as it is.
//!
//! On a session without the settings (capture's, the `api` role's, a test
//! pool) both calls leave `statement_timeout` as it is. Both must run inside
//! a transaction: `set_config(.., true)` outside one has no effect.
//!
//! Batching the deletes is the real fix; this bound keeps them from failing
//! every round until then.

use sqlx::PgConnection;

/// The session setting holding the pipeline's own statement bound.
pub const STATEMENT_BOUND: &str = "crosstalk.statement_timeout";

/// The session setting holding the retention deletes' statement bound.
pub const RETENTION_BOUND: &str = "crosstalk.retention_timeout";

/// Raise this transaction's `statement_timeout` to the session's retention
/// bound, if it has one.
pub async fn raise_for_retention(connection: &mut PgConnection) -> Result<(), sqlx::Error> {
    set_local_from(connection, RETENTION_BOUND).await
}

/// Put this transaction's `statement_timeout` back to the session's
/// pipeline bound, if it has one, for the work after a retention delete.
pub async fn restore_after_retention(connection: &mut PgConnection) -> Result<(), sqlx::Error> {
    set_local_from(connection, STATEMENT_BOUND).await
}

async fn set_local_from(
    connection: &mut PgConnection,
    setting: &'static str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "SELECT set_config('statement_timeout', \
         coalesce(nullif(current_setting($1, true), ''), current_setting('statement_timeout')), \
         true)",
    )
    .bind(setting)
    .execute(connection)
    .await
    .map(|_| ())
}
