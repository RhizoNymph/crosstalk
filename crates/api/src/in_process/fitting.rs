//! Fitting queued projection jobs in this process (opt-in,
//! [`ProjectionFitting`]): a task that claims each queued job, reads its
//! sample, lays it out and stores the frame, as an external fitter would.
//!
//! ```text
//! every `poll`:
//!   ProjectionStore::requeue_lapsed(now)
//!   while ProjectionStore::claim(now) ─▶ job:
//!     ProjectionSource::sample(job.spec)   Failed(f) ─▶ fail(job, f)    Store ─▶ left to its lease
//!     LayoutFitter::fit(embeddings, params) Failed(f) ─▶ fail(job, f)   Backend ─▶ left to its lease
//!     ProjectedPoint::new per row, ProjectionFrame::from_points(header)
//!     ProjectionStore::complete(job, frame, now)
//! ```
//!
//! A job whose fit hit a transient failure stays `Fitting` until its lease
//! lapses and the next pass requeues it, as the spec's lifecycle has it. A
//! fit whose output breaks the fitter's contract (a coordinate count that
//! is not the sample's, a point or frame the spec's constructors refuse)
//! is logged and left to its lease too: it is a fault, not the job's
//! failure.

use std::sync::Arc;
use std::time::Duration;

use crosstalk_spec::aggregates::projection::frame::{FrameHeader, InvalidFrame, ProjectionFrame};
use crosstalk_spec::aggregates::projection::{
    FitFailure, PointParts, PointWithinOneAgent, ProjectedPoint, ProjectionInfo,
};
use crosstalk_spec::aggregates::topic::Embedding;
use crosstalk_spec::ids::ProjectionId;
use crosstalk_spec::interfaces::l6_analysis::{
    LayoutError, LayoutFitter, ProjectionJobError, ProjectionSource, ProjectionStore, SampleError,
    SampleRow,
};
use crosstalk_spec::support::{Clock, Finite};
use tokio::task::JoinHandle;

/// Whether queued projection jobs are fitted in this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProjectionFitting {
    /// Jobs wait for a fitter outside this process.
    #[default]
    External,
    /// A task claims every queued job each `poll` and lays it out with
    /// `crosstalk-memory`'s `FakeLayoutFitter`: deterministic bit for bit,
    /// two coordinates read off each embedding. For tests and demos, not
    /// UMAP.
    Deterministic { poll: Duration },
}

/// Why a pass over the queue stopped: a store write failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FitRunError {
    #[error("projection job store failed: {0:?}")]
    Jobs(ProjectionJobError),
}

impl From<ProjectionJobError> for FitRunError {
    fn from(error: ProjectionJobError) -> Self {
        Self::Jobs(error)
    }
}

/// Why a claimed job was left to its lease rather than completed or
/// failed.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
enum Unfinished {
    #[error("the sample could not be read: {reason}")]
    Sample { reason: String },
    #[error("the layout backend failed: {reason}")]
    Backend { reason: String },
    #[error("the fitter returned {got} coordinates for {sampled} points")]
    Coordinates { sampled: usize, got: usize },
    #[error("a sampled point is within one agent: {0:?}")]
    Point(PointWithinOneAgent),
    #[error("the frame was refused: {0:?}")]
    Frame(InvalidFrame),
}

/// What became of one claimed job.
enum Fit {
    Frame(Box<ProjectionFrame>),
    Failed(FitFailure),
    Unfinished(Unfinished),
}

/// The projection jobs, the sample source and the fitter one pass works
/// over.
pub(crate) struct Fitter<J, S, F> {
    pub jobs: J,
    pub source: S,
    pub fitter: F,
    pub clock: Arc<dyn Clock>,
}

impl<J, S, F> Fitter<J, S, F>
where
    J: ProjectionStore + Send + Sync + 'static,
    S: ProjectionSource + Send + Sync + 'static,
    F: LayoutFitter + Send + Sync + 'static,
{
    /// Run a pass every `poll` until aborted.
    pub(crate) fn spawn(mut self, poll: Duration) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                match self.pass().await {
                    Ok(0) => {}
                    Ok(settled) => tracing::debug!(settled, "projection jobs fitted"),
                    Err(error) => tracing::warn!(error = %error, "projection fitting pass failed"),
                }
                tokio::time::sleep(poll).await;
            }
        })
    }

    /// Requeue lapsed jobs, then fit every queued job, oldest first.
    /// Returns how many claimed jobs were completed or failed.
    pub(crate) async fn pass(&mut self) -> Result<u32, FitRunError> {
        self.jobs.requeue_lapsed(self.clock.now()).await?;
        let mut settled = 0u32;
        while let Some(job) = self.jobs.claim(self.clock.now()).await? {
            let id = job.id();
            match self.fit(&job).await {
                Fit::Frame(frame) => {
                    self.jobs.complete(id, *frame, self.clock.now()).await?;
                    settled = settled.saturating_add(1);
                }
                Fit::Failed(failure) => {
                    tracing::info!(projection = %id.ulid_text(), failure = ?failure, "projection fit failed");
                    self.jobs.fail(id, failure, self.clock.now()).await?;
                    settled = settled.saturating_add(1);
                }
                Fit::Unfinished(reason) => {
                    log_unfinished(id, &reason);
                }
            }
        }
        Ok(settled)
    }

    async fn fit(&self, job: &ProjectionInfo) -> Fit {
        let spec = job.spec();
        let sample = match self.source.sample(spec).await {
            Ok(sample) => sample,
            Err(SampleError::Failed(failure)) => return Fit::Failed(failure),
            Err(SampleError::Store { reason }) => {
                return Fit::Unfinished(Unfinished::Sample { reason });
            }
        };
        let embeddings: Vec<Embedding> = sample
            .rows
            .iter()
            .map(|row| row.embedding.clone())
            .collect();
        let coordinates = match self.fitter.fit(&embeddings, spec.params()).await {
            Ok(coordinates) => coordinates,
            Err(LayoutError::Failed(failure)) => return Fit::Failed(failure),
            Err(LayoutError::Backend { reason }) => {
                return Fit::Unfinished(Unfinished::Backend { reason });
            }
        };
        if coordinates.len() != sample.rows.len() {
            return Fit::Unfinished(Unfinished::Coordinates {
                sampled: sample.rows.len(),
                got: coordinates.len(),
            });
        }
        let mut points = Vec::with_capacity(sample.rows.len());
        for (row, [x, y]) in sample.rows.into_iter().zip(coordinates) {
            match point(row, x, y) {
                Ok(point) => points.push(point),
                Err(fit) => return fit,
            }
        }
        let header = FrameHeader {
            projection: job.id(),
            topic_version: spec.topic_version(),
            watermark: sample.watermark,
            limit: spec.params().limit(),
            matching: sample.matching,
        };
        match ProjectionFrame::from_points(header, &points) {
            Ok(frame) => Fit::Frame(Box::new(frame)),
            Err(error) => Fit::Unfinished(Unfinished::Frame(error)),
        }
    }
}

/// `row` laid out at (`x`, `y`): a non-finite coordinate fails the job.
fn point(row: SampleRow, x: f32, y: f32) -> Result<ProjectedPoint, Fit> {
    let (Ok(x), Ok(y)) = (Finite::new(x), Finite::new(y)) else {
        return Err(Fit::Failed(FitFailure::NonFiniteLayout));
    };
    ProjectedPoint::new(PointParts {
        transmission: row.transmission,
        from: row.from,
        to: row.to,
        route: row.route,
        topic: row.topic,
        confirmed_at: row.confirmed_at,
        x,
        y,
    })
    .map_err(|error| Fit::Unfinished(Unfinished::Point(error)))
}

fn log_unfinished(id: ProjectionId, reason: &Unfinished) {
    match reason {
        Unfinished::Sample { .. } | Unfinished::Backend { .. } => {
            tracing::warn!(projection = %id.ulid_text(), reason = %reason, "projection fit left to its lease");
        }
        Unfinished::Coordinates { .. } | Unfinished::Point(_) | Unfinished::Frame(_) => {
            tracing::error!(projection = %id.ulid_text(), reason = %reason, "projection fit refused; left to its lease");
        }
    }
}
