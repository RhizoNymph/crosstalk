//! `TopicCatalog` on [`PgTopicCatalog`]: reads in one snapshot each, pins
//! and retention in one serializable transaction each, and the search
//! index's view ([`TopicAssignments`]).

use std::collections::BTreeMap;

use crosstalk_spec::aggregates::retention::{Pin, PinChange, PinError, Retention, RetentionPolicy};
use crosstalk_spec::aggregates::topic::{Topic, TopicModelVersion};
use crosstalk_spec::aggregates::topic_history::{
    TopicLineage, TopicSizes, TopicVersionHistory, TopicVersionStatusKind,
};
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::ids::{TopicId, TransmissionId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l6_analysis::{CatalogError, TopicCatalog};
use crosstalk_spec::paging::{Page, PageRequest, TopicList};
use crosstalk_spec::support::{TimeWindow, Timestamp};
use crosstalk_store::{TxError, retry_serializable};
use sqlx::{PgConnection, Postgres, Transaction};

use super::rows::{self, Versions};
use super::{PgTopicCatalog, catalog_abort};
use crate::pg::codec::id_of;
use crate::pg::outbox::{self, Pending};
use crate::pg::paging::{Binding, page_of};
use crate::pg::tx::{fail, finish};
use crate::pg::{EventSink, StorageFailure};
use crate::search::TopicAssignments;

/// The list name topic cursors are bound to.
const TOPICS_LIST: &str = "topics";

/// Mark every version `policy` drops dropped at `at` in `history` and in
/// the store, freezing its all-time sizes and deleting its assignments.
/// Oldest first. A version superseded after `at` cannot be marked yet and
/// is left for a later enforcement. Returns the dropped versions and their
/// events (`TopicVersionDropped` and `Changed::TopicVersion` each).
pub(super) async fn enforce(
    conn: &mut PgConnection,
    history: &mut TopicVersionHistory,
    policy: RetentionPolicy,
    at: Timestamp,
    agents: &(impl AgentDirectory + ?Sized),
) -> Result<(Vec<TopicModelVersion>, Vec<BusEvent>), StorageFailure> {
    let mut dropped = Vec::new();
    let mut events = Vec::new();
    for version in policy.to_drop(history) {
        let topics: Vec<TopicId> = rows::topics(conn, version)
            .await?
            .iter()
            .map(|topic| topic.id)
            .collect();
        let assigned = rows::assignments(conn, version).await?;
        let Ok(frozen) = rows::count_sizes(version, None, &topics, &assigned, agents) else {
            continue;
        };
        if history.mark_dropped(version, at, policy).is_err() {
            continue;
        }
        let info = history.get(version).ok_or_else(|| {
            StorageFailure::Invariant(format!("dropped version {version:?} left the history"))
        })?;
        rows::mark_dropped(conn, info, &frozen).await?;
        events.push(BusEvent::Insight(InsightEvent::TopicVersionDropped {
            version,
        }));
        events.push(BusEvent::Changed(Changed::TopicVersion(version)));
        dropped.push(version);
    }
    Ok((dropped, events))
}

fn catalog_pin_error(error: PinError) -> CatalogError {
    match error {
        PinError::UnknownVersion(version) => CatalogError::UnknownVersion(version),
        PinError::Fitting(version) => CatalogError::StillFitting(version),
        PinError::Dropped { version, .. } => CatalogError::VersionNotRetained(version),
    }
}

impl<D, S> PgTopicCatalog<D, S>
where
    D: AgentDirectory + Send + Sync + 'static,
    S: EventSink,
{
    /// A read-only snapshot for one read.
    async fn snapshot(&self) -> Result<Transaction<'static, Postgres>, CatalogError> {
        self.pool
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .map_err(fail)
    }

    /// Unpin (when `unpin`) or pin `version`, then, when an unpin changed
    /// the pin, enforce retention at `at`; one transaction.
    async fn change_pin(
        &self,
        version: TopicModelVersion,
        change: PinRequest,
    ) -> Result<PinChange, CatalogError> {
        let policy = self.config.retention;
        let agents = std::sync::Arc::clone(&self.agents);
        let (result, pending) = finish(
            retry_serializable(&self.pool, &self.retry, |conn| {
                let agents = std::sync::Arc::clone(&agents);
                Box::pin(async move {
                    let Versions { history, .. } =
                        rows::versions(conn).await.map_err(catalog_abort)?;
                    let mut after = history.clone();
                    let result = match change {
                        PinRequest::Pin(pin) => after.pin(version, pin),
                        PinRequest::Unpin(_) => after.unpin(version),
                    }
                    .map_err(|error| TxError::Abort(catalog_pin_error(error)))?;
                    if result == PinChange::Unchanged {
                        return Ok((result, Pending::default()));
                    }
                    rows::save_changed(conn, &history, &after)
                        .await
                        .map_err(catalog_abort)?;
                    let mut events = vec![BusEvent::Changed(Changed::TopicVersion(version))];
                    if let PinRequest::Unpin(at) = change {
                        let (_, drops) = enforce(conn, &mut after, policy, at, agents.as_ref())
                            .await
                            .map_err(catalog_abort)?;
                        events.extend(drops);
                    }
                    let pending = outbox::append(conn, events).await.map_err(catalog_abort)?;
                    Ok((result, pending))
                })
            })
            .await,
        )?;
        self.deliver(pending).await;
        Ok(result)
    }
}

#[derive(Debug, Clone, Copy)]
enum PinRequest {
    Pin(Pin),
    /// Unpin, enforcing retention at this time when the pin changed.
    Unpin(Timestamp),
}

impl<D, S> TopicCatalog for PgTopicCatalog<D, S>
where
    D: AgentDirectory + Send + Sync + 'static,
    S: EventSink,
{
    async fn versions(&self) -> Result<TopicVersionHistory, CatalogError> {
        let mut conn = self.pool.acquire().await.map_err(fail)?;
        Ok(rows::versions(&mut conn).await.map_err(fail)?.history)
    }

    async fn sizes(
        &self,
        version: TopicModelVersion,
        window: Option<TimeWindow>,
    ) -> Result<TopicSizes, CatalogError> {
        let mut tx = self.snapshot().await?;
        let versions = rows::versions(&mut tx).await.map_err(fail)?;
        let info = versions
            .history
            .get(version)
            .ok_or(CatalogError::UnknownVersion(version))?;
        if info.status().kind() == TopicVersionStatusKind::Fitting {
            return Err(CatalogError::StillFitting(version));
        }
        match (info.retention(), window) {
            (Retention::Dropped { .. }, Some(_)) => Err(CatalogError::VersionNotRetained(version)),
            (Retention::Dropped { .. }, None) => rows::frozen_sizes(&mut tx, version)
                .await
                .map_err(fail)?
                .ok_or(CatalogError::VersionNotRetained(version)),
            (Retention::Retained { .. }, window) => {
                let topics: Vec<TopicId> = rows::topics(&mut tx, version)
                    .await
                    .map_err(fail)?
                    .iter()
                    .map(|topic| topic.id)
                    .collect();
                let assigned = rows::assignments(&mut tx, version).await.map_err(fail)?;
                rows::count_sizes(version, window, &topics, &assigned, self.agents.as_ref())
                    .map_err(|error| CatalogError::Store {
                        reason: format!("topic {:?} counted twice", error.0),
                    })
            }
        }
    }

    async fn lineage(&self, from: TopicModelVersion) -> Result<Option<TopicLineage>, CatalogError> {
        let mut tx = self.snapshot().await?;
        let versions = rows::versions(&mut tx).await.map_err(fail)?;
        if versions.history.get(from).is_none() {
            return Err(CatalogError::UnknownVersion(from));
        }
        rows::lineage(&mut tx, from).await.map_err(fail)
    }

    fn retention(&self) -> RetentionPolicy {
        self.config.retention
    }

    async fn pin(&self, version: TopicModelVersion, pin: Pin) -> Result<PinChange, CatalogError> {
        self.change_pin(version, PinRequest::Pin(pin)).await
    }

    async fn unpin(
        &self,
        version: TopicModelVersion,
        at: Timestamp,
    ) -> Result<PinChange, CatalogError> {
        self.change_pin(version, PinRequest::Unpin(at)).await
    }

    async fn enforce_retention(
        &self,
        at: Timestamp,
    ) -> Result<Vec<TopicModelVersion>, CatalogError> {
        let policy = self.config.retention;
        let agents = std::sync::Arc::clone(&self.agents);
        let (dropped, pending) = finish(
            retry_serializable(&self.pool, &self.retry, |conn| {
                let agents = std::sync::Arc::clone(&agents);
                Box::pin(async move {
                    let Versions { mut history, .. } =
                        rows::versions(conn).await.map_err(catalog_abort)?;
                    let (dropped, events) =
                        enforce(conn, &mut history, policy, at, agents.as_ref())
                            .await
                            .map_err(catalog_abort)?;
                    let pending = outbox::append(conn, events).await.map_err(catalog_abort)?;
                    Ok((dropped, pending))
                })
            })
            .await,
        )?;
        self.deliver(pending).await;
        Ok(dropped)
    }

    async fn topics(
        &self,
        version: TopicModelVersion,
        page: &PageRequest<TopicList>,
    ) -> Result<Page<Topic, TopicList>, CatalogError> {
        let mut tx = self.snapshot().await?;
        let versions = rows::versions(&mut tx).await.map_err(fail)?;
        let info = versions
            .history
            .get(version)
            .ok_or(CatalogError::UnknownVersion(version))?;
        if info.status().kind() == TopicVersionStatusKind::Fitting {
            return Err(CatalogError::StillFitting(version));
        }
        let request = version.0.to_be_bytes();
        let binding = Binding {
            key: &self.cursor_key,
            list: TOPICS_LIST,
            request: &request,
        };
        let after = match &page.after {
            None => None,
            Some(cursor) => {
                let position = binding.resume(cursor).ok_or(CatalogError::InvalidCursor)?;
                let text = String::from_utf8(position).map_err(|_| CatalogError::InvalidCursor)?;
                Some(
                    id_of::<TopicId>("topic cursor", &text)
                        .map_err(|_| CatalogError::InvalidCursor)?,
                )
            }
        };
        let limit = i64::from(page.size.get().get()) + 1;
        let topics = rows::topics_page(&mut tx, version, after, limit)
            .await
            .map_err(fail)?;
        page_of(topics, page.size, binding, |topic: &Topic| {
            topic.id.ulid_text().into_bytes()
        })
    }

    async fn assignments(
        &self,
        version: TopicModelVersion,
        ids: &IdBatch<TransmissionId>,
    ) -> Result<BTreeMap<TransmissionId, Option<TopicId>>, CatalogError> {
        let mut tx = self.snapshot().await?;
        let versions = rows::versions(&mut tx).await.map_err(fail)?;
        let readable = versions
            .history
            .get(version)
            .is_some_and(|info| info.status().kind() != TopicVersionStatusKind::Fitting);
        if !readable {
            return Ok(BTreeMap::new());
        }
        let stored = rows::assignments_of(&mut tx, version, ids.ids())
            .await
            .map_err(fail)?;
        Ok(stored
            .into_iter()
            .map(|(id, assigned)| (id, assigned.topic))
            .collect())
    }
}

impl<D, S> TopicAssignments for PgTopicCatalog<D, S>
where
    D: AgentDirectory + Send + Sync + 'static,
    S: EventSink,
{
    async fn history(&self) -> Result<TopicVersionHistory, CatalogError> {
        TopicCatalog::versions(self).await
    }

    async fn version_of(&self, topic: TopicId) -> Result<Option<TopicModelVersion>, CatalogError> {
        let mut conn = self.pool.acquire().await.map_err(fail)?;
        rows::topic_version(&mut conn, topic).await.map_err(fail)
    }

    async fn assigned(
        &self,
        version: TopicModelVersion,
        transmissions: &[TransmissionId],
    ) -> Result<BTreeMap<TransmissionId, Option<TopicId>>, CatalogError> {
        let mut conn = self.pool.acquire().await.map_err(fail)?;
        let stored = rows::assignments_of(&mut conn, version, transmissions)
            .await
            .map_err(fail)?;
        Ok(stored
            .into_iter()
            .map(|(id, assigned)| (id, assigned.topic))
            .collect())
    }
}
