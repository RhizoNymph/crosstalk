//! `ProjectionStore` on [`PgProjectionStore`]: the job rows and how each
//! call reads and writes them.

use crosstalk_spec::aggregates::projection::frame::ProjectionFrame;
use crosstalk_spec::aggregates::projection::{
    FitFailure, Fitted, InvalidTransition, Projection, ProjectionInfo, ProjectionStatus,
    ProjectionStatusKind,
};
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::ids::ProjectionId;
use crosstalk_spec::interfaces::l6_analysis::{
    ProjectionJobError, ProjectionStore, ProjectionStoreError,
};
use crosstalk_spec::paging::{Page, PageRequest, ProjectionList};
use crosstalk_spec::support::Timestamp;
use crosstalk_store::{TxError, retry_serializable};
use sqlx::PgConnection;

use super::{PgProjectionStore, plus};
use crate::pg::codec::{from_json, id_of, id_text, micros, to_json};
use crate::pg::outbox;
use crate::pg::paging::{Binding, page_of};
use crate::pg::tx::{abort, fail, finish};
use crate::pg::{EventSink, StorageFailure};

/// The list name projection cursors are bound to.
const PROJECTIONS_LIST: &str = "projections";

type JobAbort = TxError<ProjectionJobError>;

fn state_text(kind: ProjectionStatusKind) -> &'static str {
    match kind {
        ProjectionStatusKind::Queued => "queued",
        ProjectionStatusKind::Fitting => "fitting",
        ProjectionStatusKind::Ready => "ready",
        ProjectionStatusKind::Failed => "failed",
        ProjectionStatusKind::Expired => "expired",
    }
}

fn not_allowed(from: ProjectionStatusKind, to: ProjectionStatusKind) -> ProjectionJobError {
    ProjectionJobError::Transition(InvalidTransition::NotAllowed { from, to })
}

fn changed(id: ProjectionId) -> BusEvent {
    BusEvent::Changed(Changed::Projection(id))
}

/// Job `id`'s record, if stored.
async fn load(
    conn: &mut PgConnection,
    id: ProjectionId,
) -> Result<Option<ProjectionInfo>, StorageFailure> {
    let stored: Option<String> =
        sqlx::query_scalar("SELECT info FROM analysis.projection_jobs WHERE id = $1")
            .bind(id_text(id))
            .fetch_optional(&mut *conn)
            .await?;
    stored
        .map(|json| from_json("projection job", &json).map_err(StorageFailure::from))
        .transpose()
}

/// Insert a new job.
async fn insert(conn: &mut PgConnection, info: &ProjectionInfo) -> Result<(), StorageFailure> {
    sqlx::query(
        "INSERT INTO analysis.projection_jobs (id, info, state, requested_at) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(id_text(info.id()))
    .bind(to_json("projection job", info)?)
    .bind(state_text(info.status().kind()))
    .bind(micros("requested at", info.requested_at())?)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Store a job's next record, with its lease when it is fitting.
async fn save(
    conn: &mut PgConnection,
    info: &ProjectionInfo,
    lease_until: Option<Timestamp>,
) -> Result<(), StorageFailure> {
    let fitted_at = match info.status() {
        ProjectionStatus::Ready(fit) => Some(micros("fitted at", fit.fitted_at)?),
        _ => None,
    };
    let lease_until = lease_until
        .map(|until| micros("lease until", until))
        .transpose()?;
    sqlx::query(
        "UPDATE analysis.projection_jobs SET info = $2, state = $3, lease_until = $4, fitted_at = $5 \
         WHERE id = $1",
    )
    .bind(id_text(info.id()))
    .bind(to_json("projection job", info)?)
    .bind(state_text(info.status().kind()))
    .bind(lease_until)
    .bind(fitted_at)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Every job of `state` selected by `filter` (with its bound `$1`), in id
/// order.
async fn select(
    conn: &mut PgConnection,
    filter: &'static str,
    bound: i64,
) -> Result<Vec<ProjectionInfo>, StorageFailure> {
    let rows: Vec<String> = sqlx::query_scalar(filter)
        .bind(bound)
        .fetch_all(&mut *conn)
        .await?;
    rows.iter()
        .map(|json| from_json("projection job", json).map_err(StorageFailure::from))
        .collect()
}

/// Apply `transition` to stored job `id` and store the result.
async fn transition(
    conn: &mut PgConnection,
    id: ProjectionId,
    transition: impl FnOnce(ProjectionInfo) -> Result<ProjectionInfo, InvalidTransition>,
) -> Result<ProjectionInfo, JobAbort> {
    let info = load(conn, id)
        .await
        .map_err(abort)?
        .ok_or(TxError::Abort(ProjectionJobError::Unknown(id)))?;
    let next =
        transition(info).map_err(|error| TxError::Abort(ProjectionJobError::Transition(error)))?;
    save(conn, &next, None).await.map_err(abort)?;
    Ok(next)
}

impl<S: EventSink> ProjectionStore for PgProjectionStore<S> {
    async fn enqueue(&mut self, job: ProjectionInfo) -> Result<(), ProjectionStoreError> {
        let job = &job;
        finish(
            retry_serializable(&self.pool, &self.retry, |conn| {
                let job = job.clone();
                Box::pin(async move {
                    if load(conn, job.id()).await.map_err(abort)?.is_some() {
                        return Ok(());
                    }
                    if job.status().kind() != ProjectionStatusKind::Queued {
                        return Err(TxError::Abort(ProjectionStoreError::Store {
                            reason: format!(
                                "only a queued job is enqueued, not a {:?} one",
                                job.status().kind()
                            ),
                        }));
                    }
                    let pending: i64 = sqlx::query_scalar(
                        "SELECT count(*) FROM analysis.projection_jobs \
                         WHERE state IN ('queued', 'fitting')",
                    )
                    .fetch_one(&mut *conn)
                    .await?;
                    if pending >= i64::from(Self::MAX_PENDING) {
                        return Err(TxError::Abort(ProjectionStoreError::QueueFull));
                    }
                    insert(conn, &job).await.map_err(abort)
                })
            })
            .await,
        )
    }

    async fn claim(&mut self, at: Timestamp) -> Result<Option<ProjectionInfo>, ProjectionJobError> {
        let lease_until = plus(at, self.config.lease);
        finish(
            retry_serializable(&self.pool, &self.retry, |conn| {
                Box::pin(async move {
                    let oldest: Option<String> = sqlx::query_scalar(
                        "SELECT info FROM analysis.projection_jobs WHERE state = 'queued' \
                         ORDER BY requested_at, id LIMIT 1 FOR UPDATE SKIP LOCKED",
                    )
                    .fetch_optional(&mut *conn)
                    .await?;
                    let Some(json) = oldest else {
                        return Ok(None);
                    };
                    let info: ProjectionInfo = from_json("projection job", &json).map_err(abort)?;
                    let started = info
                        .start(at)
                        .map_err(|error| TxError::Abort(ProjectionJobError::Transition(error)))?;
                    save(conn, &started, Some(lease_until))
                        .await
                        .map_err(abort)?;
                    Ok(Some(started))
                })
            })
            .await,
        )
    }

    async fn complete(
        &mut self,
        id: ProjectionId,
        frame: ProjectionFrame,
        at: Timestamp,
    ) -> Result<(), ProjectionJobError> {
        let frame = &frame;
        let pending = finish(
            retry_serializable(&self.pool, &self.retry, |conn| {
                let frame = frame.clone();
                Box::pin(async move {
                    let info = load(conn, id)
                        .await
                        .map_err(abort)?
                        .ok_or(TxError::Abort(ProjectionJobError::Unknown(id)))?;
                    let ProjectionStatus::Fitting { started_at } = *info.status() else {
                        return Err(TxError::Abort(not_allowed(
                            info.status().kind(),
                            ProjectionStatusKind::Ready,
                        )));
                    };
                    let header = frame.header();
                    let fit = Fitted {
                        started_at,
                        fitted_at: at,
                        watermark: header.watermark,
                        matching: header.matching,
                        points: frame.count(),
                    };
                    let ready = info
                        .complete(fit)
                        .map_err(|error| TxError::Abort(ProjectionJobError::Transition(error)))?;
                    // A frame that does not belong to the job (another id,
                    // version, watermark, sample size or count) is refused
                    // as a mismatch.
                    let projection = Projection::new(ready, frame).map_err(|mismatch| {
                        TxError::Abort(ProjectionJobError::FrameMismatch {
                            projection: id,
                            mismatch,
                        })
                    })?;
                    save(conn, projection.info(), None).await.map_err(abort)?;
                    sqlx::query(
                        "INSERT INTO analysis.projection_frames (job, frame) VALUES ($1, $2)",
                    )
                    .bind(id_text(id))
                    .bind(projection.frame().encode())
                    .execute(&mut *conn)
                    .await?;
                    outbox::append(conn, vec![changed(id)]).await.map_err(abort)
                })
            })
            .await,
        )?;
        self.deliver(pending).await;
        Ok(())
    }

    async fn fail(
        &mut self,
        id: ProjectionId,
        failure: FitFailure,
        at: Timestamp,
    ) -> Result<(), ProjectionJobError> {
        let failure = &failure;
        let pending = finish(
            retry_serializable(&self.pool, &self.retry, |conn| {
                let failure = failure.clone();
                Box::pin(async move {
                    transition(conn, id, |info| info.fail(at, failure)).await?;
                    outbox::append(conn, vec![changed(id)]).await.map_err(abort)
                })
            })
            .await,
        )?;
        self.deliver(pending).await;
        Ok(())
    }

    async fn requeue_lapsed(&mut self, now: Timestamp) -> Result<u32, ProjectionJobError> {
        let now = micros("now", now).map_err(fail::<ProjectionJobError>)?;
        finish(
            retry_serializable(&self.pool, &self.retry, |conn| {
                Box::pin(async move {
                    let lapsed = select(
                        conn,
                        "SELECT info FROM analysis.projection_jobs \
                         WHERE state = 'fitting' AND lease_until < $1 ORDER BY id",
                        now,
                    )
                    .await
                    .map_err(abort)?;
                    for job in &lapsed {
                        transition(conn, job.id(), ProjectionInfo::requeue).await?;
                    }
                    Ok(u32::try_from(lapsed.len()).unwrap_or(u32::MAX))
                })
            })
            .await,
        )
    }

    async fn expire(&mut self, now: Timestamp) -> Result<u32, ProjectionJobError> {
        let retention = u64::try_from(self.config.frame_retention.as_micros()).unwrap_or(u64::MAX);
        // A frame fitted at `f` is due when `f + retention < now`, that is
        // `f < now - retention`; nothing is due before `now` passes the
        // retention.
        let Some(horizon) = now.as_micros().checked_sub(retention) else {
            return Ok(0);
        };
        let horizon = micros("expiry horizon", Timestamp::from_micros(horizon))
            .map_err(fail::<ProjectionJobError>)?;
        let (count, pending) = finish(
            retry_serializable(&self.pool, &self.retry, |conn| {
                Box::pin(async move {
                    let due = select(
                        conn,
                        "SELECT info FROM analysis.projection_jobs \
                         WHERE state = 'ready' AND fitted_at < $1 ORDER BY id",
                        horizon,
                    )
                    .await
                    .map_err(abort)?;
                    let mut events = Vec::with_capacity(due.len());
                    for job in &due {
                        transition(conn, job.id(), |info| info.expire(now)).await?;
                        sqlx::query("DELETE FROM analysis.projection_frames WHERE job = $1")
                            .bind(id_text(job.id()))
                            .execute(&mut *conn)
                            .await?;
                        events.push(changed(job.id()));
                    }
                    let pending = outbox::append(conn, events).await.map_err(abort)?;
                    Ok((u32::try_from(due.len()).unwrap_or(u32::MAX), pending))
                })
            })
            .await,
        )?;
        self.deliver(pending).await;
        Ok(count)
    }

    async fn info(&self, id: ProjectionId) -> Result<Option<ProjectionInfo>, ProjectionStoreError> {
        let mut conn = self.pool.acquire().await.map_err(fail)?;
        load(&mut conn, id).await.map_err(fail)
    }

    async fn list(
        &self,
        page: &PageRequest<ProjectionList>,
    ) -> Result<Page<ProjectionInfo, ProjectionList>, ProjectionStoreError> {
        let binding = Binding {
            key: &self.cursor_key,
            list: PROJECTIONS_LIST,
            request: &[],
        };
        let after = match &page.after {
            None => None,
            Some(cursor) => {
                let position = binding
                    .resume(cursor)
                    .ok_or(ProjectionStoreError::InvalidCursor)?;
                let text =
                    String::from_utf8(position).map_err(|_| ProjectionStoreError::InvalidCursor)?;
                Some(
                    id_of::<ProjectionId>("projection cursor", &text)
                        .map_err(|_| ProjectionStoreError::InvalidCursor)?,
                )
            }
        };
        let limit = i64::from(page.size.get().get()) + 1;
        let mut conn = self.pool.acquire().await.map_err(fail)?;
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT info FROM analysis.projection_jobs WHERE ($1::text IS NULL OR id < $1) \
             ORDER BY id DESC LIMIT $2",
        )
        .bind(after.map(id_text))
        .bind(limit)
        .fetch_all(&mut *conn)
        .await
        .map_err(fail)?;
        let jobs = rows
            .iter()
            .map(|json| from_json::<ProjectionInfo>("projection job", json))
            .collect::<Result<Vec<_>, _>>()
            .map_err(fail)?;
        page_of(jobs, page.size, binding, |job: &ProjectionInfo| {
            job.id().ulid_text().into_bytes()
        })
    }

    async fn projection(&self, id: ProjectionId) -> Result<Projection, ProjectionStoreError> {
        let mut tx = self
            .pool
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .map_err(fail)?;
        let info = load(&mut tx, id)
            .await
            .map_err(fail)?
            .ok_or(ProjectionStoreError::Unknown(id))?;
        match info.status() {
            ProjectionStatus::Queued | ProjectionStatus::Fitting { .. } => {
                Err(ProjectionStoreError::NotReady {
                    projection: id,
                    status: info.status().kind(),
                })
            }
            ProjectionStatus::Failed { failure, .. } => Err(ProjectionStoreError::Failed {
                projection: id,
                failure: failure.clone(),
            }),
            ProjectionStatus::Expired { .. } => Err(ProjectionStoreError::NotRetained(id)),
            ProjectionStatus::Ready(_) => {
                let bytes: Option<Vec<u8>> = sqlx::query_scalar(
                    "SELECT frame FROM analysis.projection_frames WHERE job = $1",
                )
                .bind(id_text(id))
                .fetch_optional(&mut *tx)
                .await
                .map_err(fail)?;
                let bytes = bytes.ok_or(ProjectionStoreError::NotRetained(id))?;
                let frame = ProjectionFrame::decode(&bytes).map_err(|error| {
                    ProjectionStoreError::Store {
                        reason: format!("stored frame does not decode: {error:?}"),
                    }
                })?;
                Projection::new(info, frame).map_err(|error| ProjectionStoreError::Store {
                    reason: format!("stored frame does not match its job: {error:?}"),
                })
            }
        }
    }
}
