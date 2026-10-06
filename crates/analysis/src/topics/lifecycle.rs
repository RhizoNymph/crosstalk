//! `TopicLifecycle` on [`PgTopicCatalog`]: the fit lifecycle and the
//! assignments, each call one serializable transaction.

use std::collections::BTreeMap;

use crosstalk_spec::aggregates::topic::{Topic, TopicModelVersion};
use crosstalk_spec::aggregates::topic_history::{
    CompletedFit, FitRecord, InvalidVersionInfo, TopicLineage, TopicVersionHistory,
    TopicVersionInfo, TopicVersionStatus,
};
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::ids::TransmissionId;
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l6_analysis::lifecycle::{
    CatalogActivation, StoredAssignment, TopicLifecycle, TopicLifecycleError,
};
use crosstalk_spec::support::{Change, Timestamp};
use crosstalk_store::{TxError, retry_serializable};

use super::catalog::enforce;
use super::lineage::lineage_between;
use super::rows::{self, Versions};
use super::{PgTopicCatalog, lifecycle_abort};
use crate::pg::EventSink;
use crate::pg::outbox::{self, Pending};
use crate::pg::tx::finish;

type Abort = TxError<TopicLifecycleError>;

fn refuse(error: TopicLifecycleError) -> Abort {
    TxError::Abort(error)
}

/// When `version`'s fit started, if it is fitting.
fn fitting_start(versions: &Versions, version: TopicModelVersion) -> Result<Timestamp, Abort> {
    match versions.history.get(version).map(TopicVersionInfo::status) {
        None => Err(refuse(TopicLifecycleError::UnknownVersion(version))),
        Some(TopicVersionStatus::Fitting { started_at }) => Ok(*started_at),
        Some(_) => Err(refuse(TopicLifecycleError::NotFitting(version))),
    }
}

/// The version right before `version` in the history.
fn predecessor(
    history: &TopicVersionHistory,
    version: TopicModelVersion,
) -> Result<TopicModelVersion, Abort> {
    history
        .versions()
        .iter()
        .map(TopicVersionInfo::version)
        .take_while(|older| *older < version)
        .last()
        .ok_or(refuse(TopicLifecycleError::UnknownVersion(version)))
}

/// `history` with `version`'s status replaced, its retention kept.
fn with_status(
    history: &TopicVersionHistory,
    version: TopicModelVersion,
    status: TopicVersionStatus,
) -> Result<TopicVersionHistory, Abort> {
    let mut versions = Vec::with_capacity(history.versions().len());
    for info in history.versions() {
        if info.version() == version {
            versions.push(
                TopicVersionInfo::with_retention(version, status, info.retention())
                    .map_err(|error| refuse(TopicLifecycleError::VersionInfo(error)))?,
            );
        } else {
            versions.push(*info);
        }
    }
    TopicVersionHistory::new(versions).map_err(|error| refuse(TopicLifecycleError::History(error)))
}

fn changed(version: TopicModelVersion) -> BusEvent {
    BusEvent::Changed(Changed::TopicVersion(version))
}

impl<D, S> TopicLifecycle for PgTopicCatalog<D, S>
where
    D: AgentDirectory + Send + Sync + 'static,
    S: EventSink,
{
    async fn begin_fit(&mut self, at: Timestamp) -> Result<TopicModelVersion, TopicLifecycleError> {
        finish(
            retry_serializable(&self.pool, &self.retry, |conn| {
                Box::pin(async move {
                    let versions = rows::versions(conn).await.map_err(lifecycle_abort)?;
                    if let Some(fitting) = versions.fitting() {
                        return Err(refuse(TopicLifecycleError::FitInProgress(fitting)));
                    }
                    let version = rows::peek_version_number(conn)
                        .await
                        .map_err(lifecycle_abort)?;
                    let info = TopicVersionInfo::new(
                        version,
                        TopicVersionStatus::Fitting { started_at: at },
                    )
                    .map_err(|error| refuse(TopicLifecycleError::VersionInfo(error)))?;
                    let mut listed = versions.history.versions().to_vec();
                    listed.push(info);
                    TopicVersionHistory::new(listed)
                        .map_err(|error| refuse(TopicLifecycleError::History(error)))?;
                    rows::take_version_number(conn)
                        .await
                        .map_err(lifecycle_abort)?;
                    rows::insert_version(conn, &info)
                        .await
                        .map_err(lifecycle_abort)?;
                    Ok(version)
                })
            })
            .await,
        )
    }

    async fn complete_fit(
        &mut self,
        version: TopicModelVersion,
        topics: Vec<Topic>,
        fitted_at: Timestamp,
    ) -> Result<TopicLineage, TopicLifecycleError> {
        let floor = self.config.lineage_floor;
        let topics = &topics;
        finish(
            retry_serializable(&self.pool, &self.retry, |conn| {
                let topics = topics.clone();
                Box::pin(async move {
                    let versions = rows::versions(conn).await.map_err(lifecycle_abort)?;
                    let started_at = fitting_start(&versions, version)?;
                    if versions.returned.contains_key(&version) {
                        return Err(refuse(TopicLifecycleError::AlreadyReturned(version)));
                    }
                    if fitted_at < started_at {
                        return Err(refuse(TopicLifecycleError::VersionInfo(
                            InvalidVersionInfo::TimestampsOutOfOrder,
                        )));
                    }
                    let ids: Vec<_> = topics.iter().map(|topic| topic.id).collect();
                    let existing = rows::existing_topics(conn, &ids)
                        .await
                        .map_err(lifecycle_abort)?;
                    let mut by_id = BTreeMap::new();
                    for topic in topics {
                        if topic.version != version || topic.fitted_at != fitted_at {
                            return Err(refuse(TopicLifecycleError::ForeignTopic {
                                topic: topic.id,
                                version,
                            }));
                        }
                        if existing.contains(&topic.id) || by_id.contains_key(&topic.id) {
                            return Err(refuse(TopicLifecycleError::TopicIdReused(topic.id)));
                        }
                        by_id.insert(topic.id, topic);
                    }
                    let predecessor = predecessor(&versions.history, version)?;
                    let from_topics = rows::topics(conn, predecessor)
                        .await
                        .map_err(lifecycle_abort)?;
                    let older: Vec<&Topic> = from_topics.iter().collect();
                    let newer: Vec<&Topic> = by_id.values().collect();
                    let lineage = lineage_between(predecessor, &older, version, &newer, floor)
                        .map_err(refuse)?;
                    for topic in by_id.values() {
                        rows::insert_topic(conn, topic)
                            .await
                            .map_err(lifecycle_abort)?;
                    }
                    rows::save_lineage(conn, &lineage)
                        .await
                        .map_err(lifecycle_abort)?;
                    rows::set_returned(conn, version, Some(fitted_at))
                        .await
                        .map_err(lifecycle_abort)?;
                    Ok(lineage)
                })
            })
            .await,
        )
    }

    async fn fail_fit(&mut self, version: TopicModelVersion) -> Result<(), TopicLifecycleError> {
        finish(
            retry_serializable(&self.pool, &self.retry, |conn| {
                Box::pin(async move {
                    let versions = rows::versions(conn).await.map_err(lifecycle_abort)?;
                    fitting_start(&versions, version)?;
                    let kept: Vec<TopicVersionInfo> = versions
                        .history
                        .versions()
                        .iter()
                        .filter(|info| info.version() != version)
                        .copied()
                        .collect();
                    TopicVersionHistory::new(kept)
                        .map_err(|error| refuse(TopicLifecycleError::History(error)))?;
                    rows::remove_version(conn, version)
                        .await
                        .map_err(lifecycle_abort)?;
                    Ok(())
                })
            })
            .await,
        )
    }

    async fn mark_ready(
        &mut self,
        version: TopicModelVersion,
        at: Timestamp,
    ) -> Result<(), TopicLifecycleError> {
        let pending = finish(
            retry_serializable(&self.pool, &self.retry, |conn| {
                Box::pin(async move {
                    let versions = rows::versions(conn).await.map_err(lifecycle_abort)?;
                    let started_at = fitting_start(&versions, version)?;
                    let fitted_at = *versions
                        .returned
                        .get(&version)
                        .ok_or(refuse(TopicLifecycleError::FitNotReturned(version)))?;
                    let topics = rows::topics(conn, version)
                        .await
                        .map_err(lifecycle_abort)?
                        .len();
                    let fit = CompletedFit {
                        started_at,
                        fitted_at,
                        ready_at: at,
                        topics: u32::try_from(topics).unwrap_or(u32::MAX),
                    };
                    let after = with_status(
                        &versions.history,
                        version,
                        TopicVersionStatus::Ready { fit },
                    )?;
                    rows::set_returned(conn, version, None)
                        .await
                        .map_err(lifecycle_abort)?;
                    rows::save_changed(conn, &versions.history, &after)
                        .await
                        .map_err(lifecycle_abort)?;
                    outbox::append(conn, vec![changed(version)])
                        .await
                        .map_err(lifecycle_abort)
                })
            })
            .await,
        )?;
        self.deliver(pending).await;
        Ok(())
    }

    async fn mark_active(
        &mut self,
        version: TopicModelVersion,
        at: Timestamp,
    ) -> Result<CatalogActivation, TopicLifecycleError> {
        let policy = self.config.retention;
        let agents = std::sync::Arc::clone(&self.agents);
        let (activation, pending) = finish(
            retry_serializable(&self.pool, &self.retry, |conn| {
                let agents = std::sync::Arc::clone(&agents);
                Box::pin(async move {
                    let versions = rows::versions(conn).await.map_err(lifecycle_abort)?;
                    if version <= versions.history.active().version() {
                        return Ok((CatalogActivation::Ignored, Pending::default()));
                    }
                    let info = *versions
                        .history
                        .get(version)
                        .ok_or(refuse(TopicLifecycleError::UnknownVersion(version)))?;
                    let TopicVersionStatus::Ready { fit } = *info.status() else {
                        return Err(refuse(TopicLifecycleError::NotReady(version)));
                    };
                    let mut superseded = Vec::new();
                    let mut listed = Vec::new();
                    for info in versions.history.versions() {
                        let status = match *info.status() {
                            _ if info.version() == version => TopicVersionStatus::Active {
                                fit: FitRecord::Fitted(fit),
                                activated_at: at,
                            },
                            TopicVersionStatus::Active { fit, activated_at }
                                if info.version() < version =>
                            {
                                superseded.push(info.version());
                                TopicVersionStatus::Superseded {
                                    fit,
                                    activated_at: Some(activated_at),
                                    by: version,
                                    superseded_at: at,
                                }
                            }
                            TopicVersionStatus::Ready { fit } if info.version() < version => {
                                superseded.push(info.version());
                                TopicVersionStatus::Superseded {
                                    fit: FitRecord::Fitted(fit),
                                    activated_at: None,
                                    by: version,
                                    superseded_at: at,
                                }
                            }
                            status => status,
                        };
                        listed.push(
                            TopicVersionInfo::with_retention(
                                info.version(),
                                status,
                                info.retention(),
                            )
                            .map_err(|error| refuse(TopicLifecycleError::VersionInfo(error)))?,
                        );
                    }
                    let mut after = TopicVersionHistory::new(listed)
                        .map_err(|error| refuse(TopicLifecycleError::History(error)))?;
                    rows::save_changed(conn, &versions.history, &after)
                        .await
                        .map_err(lifecycle_abort)?;
                    let mut events = vec![changed(version)];
                    events.extend(superseded.iter().copied().map(changed));
                    let (dropped, drop_events) =
                        enforce(conn, &mut after, policy, at, agents.as_ref())
                            .await
                            .map_err(lifecycle_abort)?;
                    events.extend(drop_events);
                    let pending = outbox::append(conn, events)
                        .await
                        .map_err(lifecycle_abort)?;
                    Ok((
                        CatalogActivation::Switched {
                            superseded,
                            dropped,
                        },
                        pending,
                    ))
                })
            })
            .await,
        )?;
        self.deliver(pending).await;
        Ok(activation)
    }

    async fn assign(
        &mut self,
        transmission: TransmissionId,
        version: TopicModelVersion,
        assignment: StoredAssignment,
    ) -> Result<Change, TopicLifecycleError> {
        finish(
            retry_serializable(&self.pool, &self.retry, |conn| {
                Box::pin(async move {
                    let versions = rows::versions(conn).await.map_err(lifecycle_abort)?;
                    let info = versions
                        .history
                        .get(version)
                        .ok_or(refuse(TopicLifecycleError::UnknownVersion(version)))?;
                    if !info.retention().is_retained() {
                        return Err(refuse(TopicLifecycleError::VersionNotRetained(version)));
                    }
                    if !versions.has_topics(version) {
                        return Err(refuse(TopicLifecycleError::FitNotReturned(version)));
                    }
                    if let Some(topic) = assignment.topic
                        && !rows::topic_in(conn, version, topic)
                            .await
                            .map_err(lifecycle_abort)?
                    {
                        return Err(refuse(TopicLifecycleError::ForeignTopic { topic, version }));
                    }
                    let stored = rows::assignments_of(conn, version, &[transmission])
                        .await
                        .map_err(lifecycle_abort)?;
                    match stored.get(&transmission) {
                        Some(existing) if *existing == assignment => Ok(Change::Unchanged),
                        Some(_) => Err(refuse(TopicLifecycleError::Conflicting {
                            transmission,
                            version,
                        })),
                        None => {
                            rows::insert_assignment(conn, version, transmission, &assignment)
                                .await
                                .map_err(lifecycle_abort)?;
                            Ok(Change::Applied)
                        }
                    }
                })
            })
            .await,
        )
    }
}
