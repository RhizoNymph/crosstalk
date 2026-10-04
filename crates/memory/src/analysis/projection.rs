//! [`InMemoryProjectionStore`]: the reference [`ProjectionStore`].
//!
//! Jobs move only through the spec's own transitions
//! ([`ProjectionInfo::start`], `requeue`, `complete`, `fail`, `expire`), so
//! every stored record keeps its invariants. A claim records a lease; a
//! fitting job whose lease lapsed returns to `Queued` on
//! [`ProjectionStore::requeue_lapsed`], and is claimed again before younger
//! jobs, so a fitter crash never fails a job and never leaves it fitting.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crosstalk_spec::aggregates::projection::frame::ProjectionFrame;
use crosstalk_spec::aggregates::projection::{
    FitFailure, Fitted, InvalidTransition, Projection, ProjectionInfo, ProjectionStatus,
    ProjectionStatusKind,
};
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::ids::ProjectionId;
use crosstalk_spec::interfaces::l6_analysis::{
    ProjectionJobError, ProjectionStore, ProjectionStoreError,
};
use crosstalk_spec::paging::{Page, PageRequest, ProjectionList};
use crosstalk_spec::support::Timestamp;

use super::support::{Outbox, Published, lock};
use crate::surface::paging::{CursorBook, page_after};

/// How long a claim holds a job, and how long a ready frame is kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionConfig {
    pub lease: Duration,
    /// `projection.frame_retention_days`.
    pub frame_retention: Duration,
}

impl ProjectionConfig {
    /// The default frame retention: 180 days.
    pub const DEFAULT_FRAME_RETENTION: Duration = Duration::from_secs(180 * 24 * 60 * 60);
}

/// `at + by`, saturating at the largest timestamp.
pub fn plus(at: Timestamp, by: Duration) -> Timestamp {
    let micros = u64::try_from(by.as_micros()).unwrap_or(u64::MAX);
    Timestamp::from_micros(at.as_micros().saturating_add(micros))
}

/// The reference projection store. Cloning shares the store.
#[derive(Clone)]
pub struct InMemoryProjectionStore {
    config: ProjectionConfig,
    state: Arc<Mutex<ProjectionState>>,
}

#[derive(Debug, Default)]
struct ProjectionState {
    jobs: BTreeMap<ProjectionId, Job>,
    frames: BTreeMap<ProjectionId, ProjectionFrame>,
    cursors: CursorBook<(), ProjectionId>,
    outbox: Outbox,
}

#[derive(Debug, Clone)]
struct Job {
    info: ProjectionInfo,
    /// While fitting: when the claim's lease lapses.
    lease_until: Option<Timestamp>,
}

impl InMemoryProjectionStore {
    pub fn new(config: ProjectionConfig) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(ProjectionState::default())),
        }
    }

    pub fn config(&self) -> ProjectionConfig {
        self.config
    }

    /// `Changed::Projection` for each job that became ready or failed, or
    /// whose frame expired, since the last drain.
    pub fn drain_published(&self) -> Vec<Published> {
        lock(&self.state).outbox.drain()
    }

    /// How many jobs are queued or fitting.
    pub fn pending(&self) -> usize {
        lock(&self.state).pending()
    }
}

impl ProjectionState {
    fn pending(&self) -> usize {
        self.jobs
            .values()
            .filter(|job| {
                matches!(
                    job.info.status().kind(),
                    ProjectionStatusKind::Queued | ProjectionStatusKind::Fitting
                )
            })
            .count()
    }

    /// Apply `transition` to job `id`, storing the result only on success.
    fn transition(
        &mut self,
        id: ProjectionId,
        transition: impl FnOnce(ProjectionInfo) -> Result<ProjectionInfo, InvalidTransition>,
    ) -> Result<ProjectionInfo, ProjectionJobError> {
        let job = self
            .jobs
            .get_mut(&id)
            .ok_or(ProjectionJobError::Unknown(id))?;
        let next = transition(job.info.clone()).map_err(ProjectionJobError::Transition)?;
        job.info = next.clone();
        job.lease_until = None;
        Ok(next)
    }
}

fn not_allowed(from: ProjectionStatusKind, to: ProjectionStatusKind) -> ProjectionJobError {
    ProjectionJobError::Transition(InvalidTransition::NotAllowed { from, to })
}

impl ProjectionStore for InMemoryProjectionStore {
    async fn enqueue(&mut self, job: ProjectionInfo) -> Result<(), ProjectionStoreError> {
        let mut state = lock(&self.state);
        if state.jobs.contains_key(&job.id()) {
            return Ok(());
        }
        if job.status().kind() != ProjectionStatusKind::Queued {
            return Err(ProjectionStoreError::Store {
                reason: format!(
                    "only a queued job is enqueued, not a {:?} one",
                    job.status().kind()
                ),
            });
        }
        if state.pending() >= Self::MAX_PENDING as usize {
            return Err(ProjectionStoreError::QueueFull);
        }
        state.jobs.insert(
            job.id(),
            Job {
                info: job,
                lease_until: None,
            },
        );
        Ok(())
    }

    async fn claim(&mut self, at: Timestamp) -> Result<Option<ProjectionInfo>, ProjectionJobError> {
        let mut state = lock(&self.state);
        let oldest = state
            .jobs
            .values()
            .filter(|job| job.info.status().kind() == ProjectionStatusKind::Queued)
            .min_by_key(|job| (job.info.requested_at(), job.info.id()))
            .map(|job| job.info.id());
        let Some(id) = oldest else {
            return Ok(None);
        };
        let started = state.transition(id, |info| info.start(at))?;
        if let Some(job) = state.jobs.get_mut(&id) {
            job.lease_until = Some(plus(at, self.config.lease));
        }
        Ok(Some(started))
    }

    async fn complete(
        &mut self,
        id: ProjectionId,
        frame: ProjectionFrame,
        at: Timestamp,
    ) -> Result<(), ProjectionJobError> {
        let mut state = lock(&self.state);
        let job = state.jobs.get(&id).ok_or(ProjectionJobError::Unknown(id))?;
        let ProjectionStatus::Fitting { started_at } = *job.info.status() else {
            return Err(not_allowed(
                job.info.status().kind(),
                ProjectionStatusKind::Ready,
            ));
        };
        let header = frame.header();
        let fit = Fitted {
            started_at,
            fitted_at: at,
            watermark: header.watermark,
            matching: header.matching,
            points: frame.count(),
        };
        let ready = job
            .info
            .clone()
            .complete(fit)
            .map_err(ProjectionJobError::Transition)?;
        // A frame that does not belong to the job (another id, version,
        // watermark, sample size or count) is refused like a transition
        // the job does not allow.
        let projection = Projection::new(ready, frame)
            .map_err(|_| not_allowed(ProjectionStatusKind::Fitting, ProjectionStatusKind::Ready))?;
        let ready = projection.info().clone();
        state.transition(id, |_| Ok(ready))?;
        state.frames.insert(id, projection.frame().clone());
        state.outbox.changed(Changed::Projection(id));
        Ok(())
    }

    async fn fail(
        &mut self,
        id: ProjectionId,
        failure: FitFailure,
        at: Timestamp,
    ) -> Result<(), ProjectionJobError> {
        let mut state = lock(&self.state);
        state.transition(id, |info| info.fail(at, failure))?;
        state.outbox.changed(Changed::Projection(id));
        Ok(())
    }

    async fn requeue_lapsed(&mut self, now: Timestamp) -> Result<u32, ProjectionJobError> {
        let mut state = lock(&self.state);
        let lapsed: Vec<ProjectionId> = state
            .jobs
            .values()
            .filter(|job| job.info.status().kind() == ProjectionStatusKind::Fitting)
            .filter(|job| job.lease_until.is_some_and(|until| until < now))
            .map(|job| job.info.id())
            .collect();
        for id in &lapsed {
            state.transition(*id, ProjectionInfo::requeue)?;
        }
        Ok(u32::try_from(lapsed.len()).unwrap_or(u32::MAX))
    }

    async fn expire(&mut self, now: Timestamp) -> Result<u32, ProjectionJobError> {
        let mut state = lock(&self.state);
        let retention = self.config.frame_retention;
        let due: Vec<ProjectionId> = state
            .jobs
            .values()
            .filter_map(|job| match job.info.status() {
                ProjectionStatus::Ready(fit) if plus(fit.fitted_at, retention) < now => {
                    Some(job.info.id())
                }
                _ => None,
            })
            .collect();
        for id in &due {
            state.transition(*id, |info| info.expire(now))?;
            state.frames.remove(id);
            state.outbox.changed(Changed::Projection(*id));
        }
        Ok(u32::try_from(due.len()).unwrap_or(u32::MAX))
    }

    async fn info(&self, id: ProjectionId) -> Result<Option<ProjectionInfo>, ProjectionStoreError> {
        Ok(lock(&self.state).jobs.get(&id).map(|job| job.info.clone()))
    }

    async fn list(
        &self,
        page: &PageRequest<ProjectionList>,
    ) -> Result<Page<ProjectionInfo, ProjectionList>, ProjectionStoreError> {
        let mut state = lock(&self.state);
        let after = match &page.after {
            None => None,
            Some(cursor) => Some(
                state
                    .cursors
                    .resolve(cursor, &())
                    .ok_or(ProjectionStoreError::InvalidCursor)?,
            ),
        };
        let remaining: Vec<ProjectionInfo> = state
            .jobs
            .values()
            .rev()
            .filter(|job| after.is_none_or(|after| job.info.id() < after))
            .map(|job| job.info.clone())
            .collect();
        page_after(
            &mut state.cursors,
            remaining,
            page.size,
            (),
            ProjectionInfo::id,
        )
        .map_err(|error| ProjectionStoreError::Store {
            reason: error.to_string(),
        })
    }

    async fn projection(&self, id: ProjectionId) -> Result<Projection, ProjectionStoreError> {
        let state = lock(&self.state);
        let job = state
            .jobs
            .get(&id)
            .ok_or(ProjectionStoreError::Unknown(id))?;
        match job.info.status() {
            ProjectionStatus::Queued | ProjectionStatus::Fitting { .. } => {
                Err(ProjectionStoreError::NotReady {
                    projection: id,
                    status: job.info.status().kind(),
                })
            }
            ProjectionStatus::Failed { failure, .. } => Err(ProjectionStoreError::Failed {
                projection: id,
                failure: failure.clone(),
            }),
            ProjectionStatus::Expired { .. } => Err(ProjectionStoreError::NotRetained(id)),
            ProjectionStatus::Ready(_) => {
                let frame = state
                    .frames
                    .get(&id)
                    .ok_or(ProjectionStoreError::NotRetained(id))?;
                Projection::new(job.info.clone(), frame.clone()).map_err(|error| {
                    ProjectionStoreError::Store {
                        reason: format!("stored frame does not match its job: {error:?}"),
                    }
                })
            }
        }
    }
}
