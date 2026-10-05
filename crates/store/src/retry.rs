//! Serializable transactions with bounded retries.
//!
//! [`retry_serializable`] runs a closure inside
//! `BEGIN ISOLATION LEVEL SERIALIZABLE`, commits, and re-runs the whole
//! transaction when Postgres reports a serialization failure or a deadlock
//! (at the statement or at commit), up to the policy's attempt budget, with
//! exponential backoff between attempts.

use std::future::Future;
use std::num::NonZeroU32;
use std::pin::Pin;
use std::time::Duration;

use sqlx::{PgConnection, PgPool};
use tracing::{debug, warn};

use crate::error::{StoreError, classify};

/// How often and how patiently to retry a serializable transaction.
/// Built only through [`SerializableRetry::new`]: the initial backoff is at
/// most the maximum backoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SerializableRetry {
    max_attempts: NonZeroU32,
    initial_backoff: Duration,
    max_backoff: Duration,
}

/// Why a [`SerializableRetry`] was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidSerializableRetry {
    /// The initial backoff exceeds the maximum.
    #[error("initial backoff {initial:?} exceeds max backoff {max:?}")]
    InitialAboveMax {
        /// The initial backoff.
        initial: Duration,
        /// The maximum backoff.
        max: Duration,
    },
}

impl SerializableRetry {
    /// Checked constructor. `max_attempts` counts the first attempt, so `1`
    /// means never retry.
    pub fn new(
        max_attempts: NonZeroU32,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) -> Result<Self, InvalidSerializableRetry> {
        if initial_backoff > max_backoff {
            return Err(InvalidSerializableRetry::InitialAboveMax {
                initial: initial_backoff,
                max: max_backoff,
            });
        }
        Ok(Self {
            max_attempts,
            initial_backoff,
            max_backoff,
        })
    }

    /// The attempt budget, the first attempt included.
    pub fn max_attempts(&self) -> NonZeroU32 {
        self.max_attempts
    }

    /// The pause after failed attempt `attempt` (1-based): the initial
    /// backoff doubled per earlier attempt, capped at the maximum.
    pub fn backoff_after(&self, attempt: NonZeroU32) -> Duration {
        let doublings = attempt.get().saturating_sub(1).min(31);
        self.initial_backoff
            .saturating_mul(1u32 << doublings)
            .min(self.max_backoff)
    }
}

impl Default for SerializableRetry {
    /// Five attempts, backing off 5 ms, 10 ms, 20 ms, 40 ms (capped at
    /// 200 ms).
    fn default() -> Self {
        Self {
            max_attempts: NonZeroU32::MIN.saturating_add(4),
            initial_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(200),
        }
    }
}

/// How a transaction body fails: a database error (retried when it is a
/// serialization failure or deadlock) or the caller's own refusal, which
/// rolls back and is returned as is. `?` on a [`sqlx::Error`] converts.
#[derive(Debug)]
pub enum TxError<E> {
    /// A statement failed.
    Db(sqlx::Error),
    /// The body chose to abort.
    Abort(E),
}

impl<E> From<sqlx::Error> for TxError<E> {
    fn from(err: sqlx::Error) -> Self {
        TxError::Db(err)
    }
}

/// The outcome of a failed [`retry_serializable`].
#[derive(Debug, thiserror::Error)]
pub enum SerializableError<E> {
    /// The body aborted with its own error; the transaction rolled back.
    #[error("transaction aborted")]
    Aborted(E),
    /// The database failed in a way retrying did not or could not fix:
    /// a non-retryable [`StoreError::Query`], or
    /// [`StoreError::RetriesExhausted`].
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// The future a transaction body returns. It borrows the transaction's
/// connection and must be `Send`, like every spec trait future.
pub type TxFuture<'c, T, E> = Pin<Box<dyn Future<Output = Result<T, TxError<E>>> + Send + 'c>>;

/// Runs `body` in a `SERIALIZABLE` transaction and commits it, re-running
/// the whole transaction on a serialization failure or deadlock until
/// `policy` runs out of attempts.
///
/// `body` may run several times, so it must not have side effects outside
/// the transaction. Write it as
/// `|conn| Box::pin(async move { ...; Ok(value) })`.
pub async fn retry_serializable<T, E, F>(
    pool: &PgPool,
    policy: &SerializableRetry,
    mut body: F,
) -> Result<T, SerializableError<E>>
where
    F: for<'c> FnMut(&'c mut PgConnection) -> TxFuture<'c, T, E> + Send,
    T: Send,
    E: Send,
{
    let mut attempt = NonZeroU32::MIN;
    loop {
        let err = match attempt_once(pool, &mut body).await {
            Ok(value) => return Ok(value),
            Err(TxError::Abort(e)) => return Err(SerializableError::Aborted(e)),
            Err(TxError::Db(err)) => err,
        };
        let failure = classify(&err);
        if !failure.is_retryable() {
            return Err(StoreError::Query {
                failure,
                source: err,
            }
            .into());
        }
        if attempt >= policy.max_attempts {
            warn!(attempts = attempt.get(), failure = %failure, "serializable transaction gave up");
            return Err(StoreError::RetriesExhausted {
                attempts: attempt,
                failure,
                source: err,
            }
            .into());
        }
        let pause = policy.backoff_after(attempt);
        debug!(
            attempt = attempt.get(),
            max_attempts = policy.max_attempts.get(),
            failure = %failure,
            backoff_ms = u64::try_from(pause.as_millis()).unwrap_or(u64::MAX),
            "serializable transaction conflicted, retrying"
        );
        tokio::time::sleep(pause).await;
        attempt = attempt.saturating_add(1);
    }
}

/// One attempt: begin, run the body, commit (or roll back on error).
async fn attempt_once<T, E, F>(pool: &PgPool, body: &mut F) -> Result<T, TxError<E>>
where
    F: for<'c> FnMut(&'c mut PgConnection) -> TxFuture<'c, T, E> + Send,
{
    let mut tx = pool
        .begin_with("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await?;
    match body(&mut tx).await {
        Ok(value) => {
            tx.commit().await?;
            Ok(value)
        }
        Err(err) => {
            if let Err(rollback) = tx.rollback().await {
                // The body's error is the one to report; a failed rollback
                // means the connection is gone, and the server discards the
                // transaction with it.
                debug!(error = %rollback, "rollback after a failed transaction body failed");
            }
            Err(err)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nz(n: u32) -> NonZeroU32 {
        NonZeroU32::new(n).unwrap_or(NonZeroU32::MIN)
    }

    #[test]
    fn backoff_doubles_and_caps() -> Result<(), InvalidSerializableRetry> {
        let p =
            SerializableRetry::new(nz(6), Duration::from_millis(10), Duration::from_millis(50))?;
        let got: Vec<u128> = (1..=6)
            .map(|a| p.backoff_after(nz(a)).as_millis())
            .collect();
        assert_eq!(got, [10, 20, 40, 50, 50, 50]);
        Ok(())
    }

    #[test]
    fn backoff_never_overflows() -> Result<(), InvalidSerializableRetry> {
        let p = SerializableRetry::new(nz(u32::MAX), Duration::from_secs(1), Duration::MAX)?;
        assert!(p.backoff_after(nz(u32::MAX)) >= Duration::from_secs(1));
        Ok(())
    }

    #[test]
    fn initial_above_max_is_refused() {
        assert_eq!(
            SerializableRetry::new(nz(3), Duration::from_secs(2), Duration::from_secs(1)),
            Err(InvalidSerializableRetry::InitialAboveMax {
                initial: Duration::from_secs(2),
                max: Duration::from_secs(1),
            })
        );
    }

    #[test]
    fn default_policy_is_valid() {
        let d = SerializableRetry::default();
        assert_eq!(d.max_attempts(), nz(5));
        assert_eq!(
            SerializableRetry::new(d.max_attempts, d.initial_backoff, d.max_backoff),
            Ok(d)
        );
    }

    #[test]
    fn sqlx_errors_convert_into_tx_errors() {
        let e: TxError<()> = sqlx::Error::RowNotFound.into();
        assert!(matches!(e, TxError::Db(sqlx::Error::RowNotFound)));
    }

    fn assert_send<T: Send>(_: &T) {}

    /// The retry future is `Send`, so layer crates can return it from spec
    /// trait methods. Built lazily: the pool never connects.
    #[tokio::test]
    async fn retry_future_is_send() -> Result<(), sqlx::Error> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://nobody@127.0.0.1:1/none")?;
        let policy = SerializableRetry::default();
        let fut = retry_serializable(&pool, &policy, |conn| {
            Box::pin(async move {
                let one: i32 = sqlx::query_scalar("SELECT 1").fetch_one(conn).await?;
                Ok::<_, TxError<()>>(one)
            })
        });
        assert_send(&fut);
        Ok(())
    }
}
