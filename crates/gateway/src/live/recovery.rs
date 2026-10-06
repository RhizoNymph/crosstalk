//! Where a Postgres-mode process stands on its way to running, for
//! `/readyz` and `/healthz` (`docs/features/postgres_stores.md`, "Restart
//! semantics end to end").
//!
//! ```text
//! waiting_for_database ─▶ migrations checked (behind: stop) ─▶ pipeline lock (held elsewhere: wait)
//!   ─▶ recovering: recovering_bus ─▶ relaying_outboxes ─▶ restoring_flow ─▶ rebuilding_nodes
//!                  ─▶ subscribing ─▶ running
//! ```
//!
//! The process that walks it ([`super::pg`]) reports each step through a
//! [`StatusReporter`]; the ops listener reads [`PipelineStatus`] snapshots.
//! One writer, many readers: a `watch` channel.

use serde::{Deserialize, Serialize};
use tokio::sync::watch;

/// Whether every layer's applied migrations are its embedded head.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationState {
    /// Not read yet (the database has not answered).
    Unknown,
    AtHead,
    /// The layers that are behind, as `/readyz` shows them
    /// (`flow 1 < 2, surface 0 < 1`).
    Behind(String),
}

/// The one-pipeline-per-database advisory lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockState {
    /// Not taken (yet, or by a process that runs no pipeline).
    NotTaken,
    Held,
    /// Another process holds it: this one neither consumes nor relays.
    HeldElsewhere,
    /// The lock's connection dropped: the pipeline stopped.
    Lost,
}

/// A step of the recovery sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryStep {
    /// Deliveries a stopped process held go back to their groups.
    RecoveringBus,
    /// Every store outbox is relayed until empty.
    RelayingOutboxes,
    /// L5's checkpoint, held writes and re-fed accesses.
    RestoringFlow,
    /// The node facts, from the stores.
    RebuildingNodes,
    /// Every consumer group subscribes; the stages start.
    Subscribing,
}

impl RecoveryStep {
    pub fn label(self) -> &'static str {
        match self {
            Self::RecoveringBus => "recovering_bus",
            Self::RelayingOutboxes => "relaying_outboxes",
            Self::RestoringFlow => "restoring_flow",
            Self::RebuildingNodes => "rebuilding_nodes",
            Self::Subscribing => "subscribing",
        }
    }
}

/// Where the pipeline is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelinePhase {
    /// This process runs no Postgres-mode pipeline (memory mode, or the
    /// `api` role).
    NotRun,
    WaitingForDatabase,
    /// Migrations are behind: `serve` never migrates; nothing consumes.
    WaitingForMigrations,
    /// Another process holds the pipeline lock.
    WaitingForLock,
    Recovering(RecoveryStep),
    Running,
    /// The pipeline did not start or stopped, with why.
    Stopped(String),
}

/// What recovery did, for `/healthz`'s `recovery` section.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct RecoveryReport {
    /// Events a store outbox still held at start, now published.
    pub outbox_relayed: u64,
    /// The restored L5 checkpoint's tick, when there was one.
    pub flow_checkpoint_micros: Option<u64>,
    /// Accesses and tool calls recorded after the checkpoint, re-fed.
    pub accesses_refed: u64,
    /// Deliveries a stopped process held, returned to their groups.
    pub deliveries_redelivered: u64,
}

/// A snapshot of a Postgres-mode process's progress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineStatus {
    pub migrations: MigrationState,
    pub lock: LockState,
    pub phase: PipelinePhase,
    pub report: RecoveryReport,
    /// Every recovery step entered so far, in order (a retried start
    /// repeats them).
    pub steps: Vec<RecoveryStep>,
}

impl PipelineStatus {
    /// Before the database answers.
    pub fn waiting() -> Self {
        Self {
            migrations: MigrationState::Unknown,
            lock: LockState::NotTaken,
            phase: PipelinePhase::WaitingForDatabase,
            report: RecoveryReport::default(),
            steps: Vec::new(),
        }
    }

    /// `/readyz`'s `migrations`: `unknown`, `at_head` or `behind: ...`.
    pub fn migrations_text(&self) -> String {
        match &self.migrations {
            MigrationState::Unknown => "unknown".to_owned(),
            MigrationState::AtHead => "at_head".to_owned(),
            MigrationState::Behind(layers) => format!("behind: {layers}"),
        }
    }

    /// `/readyz`'s `pipeline_lock`.
    pub fn lock_text(&self) -> &'static str {
        match self.lock {
            LockState::NotTaken => "not_taken",
            LockState::Held => "held",
            LockState::HeldElsewhere => "held elsewhere",
            LockState::Lost => "lost",
        }
    }

    /// `/readyz`'s `pipeline`.
    pub fn pipeline_text(&self) -> String {
        match &self.phase {
            PipelinePhase::NotRun => "not_run".to_owned(),
            PipelinePhase::WaitingForDatabase => "waiting_for_database".to_owned(),
            PipelinePhase::WaitingForMigrations => "waiting_for_migrations".to_owned(),
            PipelinePhase::WaitingForLock => "waiting_for_lock".to_owned(),
            PipelinePhase::Recovering(_) => "recovering".to_owned(),
            PipelinePhase::Running => "running".to_owned(),
            PipelinePhase::Stopped(why) => format!("stopped: {why}"),
        }
    }

    /// `/readyz`'s `recovery`: `pending`, a step, `done`, or `failed: ...`.
    pub fn recovery_text(&self) -> String {
        match &self.phase {
            PipelinePhase::NotRun
            | PipelinePhase::WaitingForDatabase
            | PipelinePhase::WaitingForMigrations
            | PipelinePhase::WaitingForLock => "pending".to_owned(),
            PipelinePhase::Recovering(step) => step.label().to_owned(),
            PipelinePhase::Running => "done".to_owned(),
            PipelinePhase::Stopped(why) => format!("failed: {why}"),
        }
    }

    /// Whether this status alone keeps the process from being ready:
    /// migrations behind, the lock held elsewhere or lost, the pipeline
    /// stopped.
    pub fn blocks_readiness(&self) -> bool {
        matches!(self.migrations, MigrationState::Behind(_))
            || matches!(self.lock, LockState::HeldElsewhere | LockState::Lost)
            || matches!(self.phase, PipelinePhase::Stopped(_))
    }

    /// Whether the process is on its way but not there yet (degraded, not
    /// unready).
    pub fn degraded(&self) -> bool {
        !matches!(self.phase, PipelinePhase::Running | PipelinePhase::NotRun)
    }
}

/// The writer of a [`PipelineStatus`]. Clones share it.
#[derive(Debug, Clone)]
pub struct StatusReporter(watch::Sender<PipelineStatus>);

impl StatusReporter {
    /// A reporter starting at `status`.
    pub fn new(status: PipelineStatus) -> Self {
        Self(watch::Sender::new(status))
    }

    /// A reader of the current status.
    pub fn reader(&self) -> StatusReader {
        StatusReader(self.0.subscribe())
    }

    pub fn update(&self, change: impl FnOnce(&mut PipelineStatus)) {
        self.0.send_modify(change);
    }

    pub fn phase(&self, phase: PipelinePhase) {
        let step = match &phase {
            PipelinePhase::Recovering(step) => {
                tracing::info!(step = step.label(), "recovery step");
                Some(*step)
            }
            _ => None,
        };
        self.update(|status| {
            status.steps.extend(step);
            status.phase = phase;
        });
    }

    pub fn snapshot(&self) -> PipelineStatus {
        self.0.borrow().clone()
    }
}

/// A reader of a [`PipelineStatus`]; cloneable.
#[derive(Debug, Clone)]
pub struct StatusReader(watch::Receiver<PipelineStatus>);

impl StatusReader {
    pub fn snapshot(&self) -> PipelineStatus {
        self.0.borrow().clone()
    }

    /// Wait until the status satisfies `ready`; `None` when the reporter
    /// is gone first.
    pub async fn wait_until(
        &mut self,
        ready: impl FnMut(&PipelineStatus) -> bool,
    ) -> Option<PipelineStatus> {
        self.0.wait_for(ready).await.ok().map(|status| status.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn texts_follow_the_phase() {
        let mut status = PipelineStatus::waiting();
        assert_eq!(status.pipeline_text(), "waiting_for_database");
        assert_eq!(status.recovery_text(), "pending");
        assert_eq!(status.migrations_text(), "unknown");
        assert!(status.degraded() && !status.blocks_readiness());

        status.phase = PipelinePhase::Recovering(RecoveryStep::RestoringFlow);
        assert_eq!(status.recovery_text(), "restoring_flow");
        assert_eq!(status.pipeline_text(), "recovering");

        status.phase = PipelinePhase::Running;
        status.migrations = MigrationState::AtHead;
        status.lock = LockState::Held;
        assert_eq!(status.recovery_text(), "done");
        assert_eq!(status.lock_text(), "held");
        assert!(!status.degraded() && !status.blocks_readiness());

        status.migrations = MigrationState::Behind("flow 1 < 2".to_owned());
        assert_eq!(status.migrations_text(), "behind: flow 1 < 2");
        assert!(status.blocks_readiness());
    }

    #[test]
    fn a_lock_held_elsewhere_blocks_readiness() {
        let mut status = PipelineStatus::waiting();
        status.lock = LockState::HeldElsewhere;
        status.phase = PipelinePhase::WaitingForLock;
        assert_eq!(status.lock_text(), "held elsewhere");
        assert!(status.blocks_readiness());
    }
}
