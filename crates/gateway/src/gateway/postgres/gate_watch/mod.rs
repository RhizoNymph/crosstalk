//! The capture gate's watch: open the gate as soon as every pipeline
//! group exists, whichever process subscribed them.
//!
//! ```text
//! at start, then every GATE_POLL (1 s), until the gate is open:
//!   gate open (this process's pipeline subscribed)   ─▶ done (WatchEnd::OpenedBySubscribe)
//!   this process holds the lock                      ─▶ no look: its own subscribe opens it
//!   otherwise, on the template pool, in one transaction bounded by the
//!   pipeline's statement and lock timeouts (and client-side by the statement's):
//!     SELECT the pipeline groups' names with no row in transport.groups
//!       none missing ─▶ the gate opens (GateOpened::GroupsFound), done (WatchEnd::Found)
//!       some missing ─▶ closed: captures stay in the spool
//!       failed / timed out ─▶ closed, warned (rate-limited), looked at again
//! ```
//!
//! Why "every row exists" is the condition (INV-1241
//! `ingress.capture.gate-opens-once-groups-exist`): a group's row is
//! inserted with its admission base, `admitted_through` = the log head, in
//! one statement (`PgBus::subscribe`), and rows are never deleted. From
//! then on every log entry past the base is admitted by the group's next
//! take, whichever process takes it. An envelope appended once every row
//! exists is past every base, so every group receives it: what the gate
//! guards (a group created after a publish starts past it) cannot happen
//! any more. Before, the gate opened only at this process's own subscribe,
//! so a gateway that restarted while another held the lock spooled every
//! capture until it took the lock itself, and the holder's pipeline saw
//! none of them (the node0 two-gateway fault run: 97 exchanges for five
//! minutes, past the watermark, late L7 dead letters).

#[cfg(test)]
mod tests;

use std::future::Future;
use std::time::Duration;

use crosstalk_store::sqlx::{self, PgPool};
use crosstalk_store::{DbFailure, classify};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use super::PipelineStart;
use super::pipeline::runs_exchange_log;
use crate::live::pg::groups::{ExchangeLogGroup, PipelineGroups};
use crate::live::recovery::{LockState, StatusReader};
use crate::spool::{Gate, GateOpened};
use crate::store::timeouts::PipelineTimeouts;

/// How often a closed gate's watch looks for the groups.
pub const GATE_POLL: Duration = Duration::from_secs(1);

/// A failed look is warned at its first failure in a row and every this
/// many after it (about every 30 s at [`GATE_POLL`]); the others go to
/// debug.
const WARN_EVERY: u64 = 30;

/// The pipeline groups' names with no row in `transport.groups`, in the
/// order given.
const MISSING: &str = "SELECT wanted.name FROM unnest($1::text[]) WITH ORDINALITY AS wanted(name, n) \
     WHERE NOT EXISTS (SELECT 1 FROM transport.groups g WHERE g.name = wanted.name) \
     ORDER BY wanted.n";

/// Groups a look did not find: never empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingGroups(Vec<String>);

impl MissingGroups {
    pub fn names(&self) -> &[String] {
        &self.0
    }
}

/// What a look at `transport.groups` found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupsFound {
    /// Every pipeline group's row exists: the gate may open.
    All,
    Missing(MissingGroups),
}

/// [`GroupsFound`] from the names a look did not find.
pub fn missing_from(missing: Vec<String>) -> GroupsFound {
    match missing.is_empty() {
        true => GroupsFound::All,
        false => GroupsFound::Missing(MissingGroups(missing)),
    }
}

/// Why a look did not answer.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GroupCheckError {
    /// No answer within the statement bound (a silent partition; the
    /// server-side bounds end a server-side stall first).
    #[error("the pipeline groups' check did not answer within {bound_ms} ms")]
    TimedOut { bound_ms: u64 },
    /// The database refused or the connection failed (a statement or lock
    /// timeout included, as `DbFailure::TimedOut`).
    #[error("the pipeline groups' check failed: {0}")]
    Failed(DbFailure),
}

/// One look for the pipeline's groups.
pub trait GroupCheck {
    fn found(
        &self,
        names: &[String],
    ) -> impl Future<Output = Result<GroupsFound, GroupCheckError>> + Send;
}

/// [`GroupCheck`] over a pool, bounded by the pipeline's timeouts.
#[derive(Debug, Clone)]
pub struct PgGroupCheck {
    pool: PgPool,
    timeouts: PipelineTimeouts,
}

impl PgGroupCheck {
    pub fn new(pool: PgPool, timeouts: PipelineTimeouts) -> Self {
        Self { pool, timeouts }
    }

    /// The missing names, in one transaction whose statements the server
    /// cancels past the pipeline's statement and lock bounds.
    async fn missing(&self, names: &[String]) -> Result<Vec<String>, sqlx::Error> {
        let [
            (statement, statement_value),
            (lock, lock_value),
            _statement_bound,
            _retention,
        ] = self.timeouts.session_settings();
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SELECT set_config($1, $2, true), set_config($3, $4, true)")
            .bind(statement)
            .bind(statement_value)
            .bind(lock)
            .bind(lock_value)
            .execute(&mut *transaction)
            .await?;
        let missing: Vec<String> = sqlx::query_scalar(MISSING)
            .bind(names)
            .fetch_all(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(missing)
    }
}

impl GroupCheck for PgGroupCheck {
    async fn found(&self, names: &[String]) -> Result<GroupsFound, GroupCheckError> {
        let bound = self.timeouts.statement();
        match tokio::time::timeout(bound, self.missing(names)).await {
            Ok(Ok(missing)) => Ok(missing_from(missing)),
            Ok(Err(error)) => Err(GroupCheckError::Failed(classify(&error))),
            Err(_) => Err(GroupCheckError::TimedOut {
                bound_ms: millis(bound),
            }),
        }
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// How a watch ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchEnd {
    /// It found every group and opened the gate.
    Found,
    /// The gate was open already: this process's pipeline subscribed.
    OpenedBySubscribe,
}

/// Watch until `gate` is open (see the module docs).
pub async fn watch(
    gate: Gate,
    groups: PipelineGroups,
    check: impl GroupCheck,
    status: StatusReader,
) -> WatchEnd {
    let names = groups.names();
    tracing::info!(
        groups = ?names,
        poll_ms = millis(GATE_POLL),
        "capture gate closed; looking for the pipeline groups"
    );
    let mut ticker = tokio::time::interval(GATE_POLL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut looks: u64 = 0;
    let mut failing: u64 = 0;
    let mut reported: Option<MissingGroups> = None;
    loop {
        ticker.tick().await;
        if gate.is_open() {
            tracing::debug!(looks, "capture gate opened by the pipeline's subscribe");
            return WatchEnd::OpenedBySubscribe;
        }
        if status.snapshot().lock == LockState::Held {
            continue;
        }
        looks += 1;
        match check.found(&names).await {
            Ok(GroupsFound::All) => {
                return match gate.open_by(GateOpened::GroupsFound) {
                    true => {
                        tracing::info!(looks, groups = names.len(), "every pipeline group exists");
                        WatchEnd::Found
                    }
                    false => WatchEnd::OpenedBySubscribe,
                };
            }
            Ok(GroupsFound::Missing(missing)) => {
                failing = 0;
                match reported.as_ref() == Some(&missing) {
                    true => {
                        tracing::debug!(looks, missing = ?missing.names(), "pipeline groups still missing")
                    }
                    false => tracing::info!(
                        looks,
                        missing = ?missing.names(),
                        "pipeline groups missing; captures stay in the spool"
                    ),
                }
                reported = Some(missing);
            }
            Err(error) => {
                failing += 1;
                match failing == 1 || failing.is_multiple_of(WARN_EVERY) {
                    true => tracing::warn!(
                        looks,
                        failing,
                        error = %error,
                        "looking for the pipeline groups failed; the gate stays closed"
                    ),
                    false => tracing::debug!(
                        looks,
                        failing,
                        error = %error,
                        "looking for the pipeline groups failed"
                    ),
                }
            }
        }
    }
}

/// A running watch; dropping it stops the watch.
#[derive(Debug)]
pub struct GateWatch(JoinHandle<WatchEnd>);

impl GateWatch {
    pub fn spawn(watch: impl Future<Output = WatchEnd> + Send + 'static) -> Self {
        Self(tokio::spawn(watch))
    }

    /// The watch for a pipeline task's process: its gate, its pipeline's
    /// groups (the exchange log's when its incarnations run one), the
    /// template pool under the store section's pipeline timeouts.
    pub fn for_pipeline(start: &PipelineStart) -> Self {
        let groups = PipelineGroups::new(ExchangeLogGroup::when(runs_exchange_log(start.role)));
        let timeouts = start
            .config
            .store
            .map(|store| store.pipeline_timeouts)
            .unwrap_or_default();
        Self::spawn(watch(
            start.gate.clone(),
            groups,
            PgGroupCheck::new(start.pool.clone(), timeouts),
            start.status.reader(),
        ))
    }
}

impl Drop for GateWatch {
    fn drop(&mut self) {
        self.0.abort();
    }
}
