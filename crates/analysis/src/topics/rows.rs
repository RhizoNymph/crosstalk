//! The catalog's rows: how a transaction loads and stores versions, topics,
//! lineage and assignments, and how sizes are counted from assignments.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;

use crosstalk_spec::aggregates::edge::EdgeStats;
use crosstalk_spec::aggregates::topic::{Topic, TopicModelVersion};
use crosstalk_spec::aggregates::topic_history::{
    DuplicateTopic, TopicLineage, TopicSize, TopicSizes, TopicVersionHistory, TopicVersionInfo,
    TopicVersionStatusKind,
};
use crosstalk_spec::ids::{AgentId, TopicId, TransmissionId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l6_analysis::lifecycle::StoredAssignment;
use crosstalk_spec::support::{TimeWindow, Timestamp};
use sqlx::PgConnection;

use crate::pg::StorageFailure;
use crate::pg::codec::{CodecError, from_json, id_of, id_text, micros, timestamp, to_json};

/// The version history and, for the fitting version, when its fit
/// returned.
#[derive(Debug, Clone)]
pub(super) struct Versions {
    pub history: TopicVersionHistory,
    pub returned: BTreeMap<TopicModelVersion, Timestamp>,
}

impl Versions {
    /// The fitting version, if one is.
    pub fn fitting(&self) -> Option<TopicModelVersion> {
        self.history
            .versions()
            .iter()
            .find(|info| info.status().kind() == TopicVersionStatusKind::Fitting)
            .map(TopicVersionInfo::version)
    }

    /// Whether `version`'s topics are stored: it is in the history and its
    /// fit has returned (version 0 and every version past fitting have).
    pub fn has_topics(&self, version: TopicModelVersion) -> bool {
        self.history.get(version).is_some_and(|info| {
            info.status().kind() != TopicVersionStatusKind::Fitting
                || self.returned.contains_key(&version)
        })
    }
}

fn version_number(version: TopicModelVersion) -> i64 {
    i64::from(version.0)
}

fn version_of(stored: i64) -> Result<TopicModelVersion, StorageFailure> {
    u32::try_from(stored)
        .map(TopicModelVersion)
        .map_err(|_| StorageFailure::Invariant(format!("stored topic version {stored}")))
}

fn state_text(kind: TopicVersionStatusKind) -> &'static str {
    match kind {
        TopicVersionStatusKind::Fitting => "fitting",
        TopicVersionStatusKind::Ready => "ready",
        TopicVersionStatusKind::Active => "active",
        TopicVersionStatusKind::Superseded => "superseded",
    }
}

/// Every stored version, oldest first, as the checked history.
pub(super) async fn versions(conn: &mut PgConnection) -> Result<Versions, StorageFailure> {
    let rows: Vec<(i64, String, Option<i64>)> = sqlx::query_as(
        "SELECT version, info, fit_returned_at FROM analysis.topic_versions ORDER BY version",
    )
    .fetch_all(&mut *conn)
    .await?;
    let mut infos = Vec::with_capacity(rows.len());
    let mut returned = BTreeMap::new();
    for (version, info, fit_returned_at) in rows {
        let info: TopicVersionInfo = from_json("topic version", &info)?;
        if info.version() != version_of(version)? {
            return Err(StorageFailure::Invariant(format!(
                "topic version row {version} holds version {:?}",
                info.version()
            )));
        }
        if let Some(at) = fit_returned_at {
            returned.insert(info.version(), timestamp("fit returned at", at)?);
        }
        infos.push(info);
    }
    let history = TopicVersionHistory::new(infos)
        .map_err(|error| StorageFailure::Invariant(format!("stored history: {error:?}")))?;
    Ok(Versions { history, returned })
}

/// Insert a new version's row (retained, its fit not returned).
pub(super) async fn insert_version(
    conn: &mut PgConnection,
    info: &TopicVersionInfo,
) -> Result<(), StorageFailure> {
    sqlx::query(
        "INSERT INTO analysis.topic_versions (version, info, state, retained) \
         VALUES ($1, $2, $3, true)",
    )
    .bind(version_number(info.version()))
    .bind(to_json("topic version", info)?)
    .bind(state_text(info.status().kind()))
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Store every version of `after` that differs from `before`. Neither adds
/// nor removes versions, and none it stores is newly dropped: drops go
/// through [`mark_dropped`], which freezes the sizes with them.
pub(super) async fn save_changed(
    conn: &mut PgConnection,
    before: &TopicVersionHistory,
    after: &TopicVersionHistory,
) -> Result<(), StorageFailure> {
    for info in after.versions() {
        if before.get(info.version()) == Some(info) {
            continue;
        }
        let updated = sqlx::query(
            "UPDATE analysis.topic_versions SET info = $2, state = $3 WHERE version = $1 AND retained",
        )
        .bind(version_number(info.version()))
        .bind(to_json("topic version", info)?)
        .bind(state_text(info.status().kind()))
        .execute(&mut *conn)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(StorageFailure::Invariant(format!(
                "no retained row for changed version {:?}",
                info.version()
            )));
        }
    }
    Ok(())
}

/// Record `info` (now dropped) with the sizes frozen at the drop, and
/// delete the version's assignments, after the mark.
pub(super) async fn mark_dropped(
    conn: &mut PgConnection,
    info: &TopicVersionInfo,
    frozen: &TopicSizes,
) -> Result<(), StorageFailure> {
    let version = version_number(info.version());
    sqlx::query(
        "UPDATE analysis.topic_versions SET info = $2, retained = false, frozen_sizes = $3 \
         WHERE version = $1",
    )
    .bind(version)
    .bind(to_json("topic version", info)?)
    .bind(to_json("frozen sizes", frozen)?)
    .execute(&mut *conn)
    .await?;
    sqlx::query("DELETE FROM analysis.topic_assignments WHERE version = $1")
        .bind(version)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// The sizes frozen when `version` was dropped.
pub(super) async fn frozen_sizes(
    conn: &mut PgConnection,
    version: TopicModelVersion,
) -> Result<Option<TopicSizes>, StorageFailure> {
    let stored: Option<Option<String>> =
        sqlx::query_scalar("SELECT frozen_sizes FROM analysis.topic_versions WHERE version = $1")
            .bind(version_number(version))
            .fetch_optional(&mut *conn)
            .await?;
    stored
        .flatten()
        .map(|json| from_json("frozen sizes", &json).map_err(StorageFailure::from))
        .transpose()
}

/// Set or clear when the fitting `version`'s fit returned.
pub(super) async fn set_returned(
    conn: &mut PgConnection,
    version: TopicModelVersion,
    at: Option<Timestamp>,
) -> Result<(), StorageFailure> {
    let at = at.map(|at| micros("fit returned at", at)).transpose()?;
    sqlx::query("UPDATE analysis.topic_versions SET fit_returned_at = $2 WHERE version = $1")
        .bind(version_number(version))
        .bind(at)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// The next version number to give, and record the one after it.
pub(super) async fn take_version_number(
    conn: &mut PgConnection,
) -> Result<TopicModelVersion, StorageFailure> {
    let next: i64 = sqlx::query_scalar(
        "UPDATE analysis.topic_catalog SET next_version = next_version + 1 \
         RETURNING next_version - 1",
    )
    .fetch_one(&mut *conn)
    .await?;
    version_of(next)
}

/// The number the next fit would get, without taking it.
pub(super) async fn peek_version_number(
    conn: &mut PgConnection,
) -> Result<TopicModelVersion, StorageFailure> {
    let next: i64 = sqlx::query_scalar("SELECT next_version FROM analysis.topic_catalog")
        .fetch_one(&mut *conn)
        .await?;
    version_of(next)
}

/// Remove a failed fit's version with its topics, the lineage into it and
/// its assignments.
pub(super) async fn remove_version(
    conn: &mut PgConnection,
    version: TopicModelVersion,
) -> Result<(), StorageFailure> {
    let version = version_number(version);
    for statement in [
        "DELETE FROM analysis.topic_assignments WHERE version = $1",
        "DELETE FROM analysis.topic_lineage WHERE to_version = $1 OR from_version = $1",
        "DELETE FROM analysis.topics WHERE version = $1",
        "DELETE FROM analysis.topic_versions WHERE version = $1",
    ] {
        sqlx::query(statement)
            .bind(version)
            .execute(&mut *conn)
            .await?;
    }
    Ok(())
}

/// `version`'s topics, ascending by id.
pub(super) async fn topics(
    conn: &mut PgConnection,
    version: TopicModelVersion,
) -> Result<Vec<Topic>, StorageFailure> {
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT topic_row FROM analysis.topics WHERE version = $1 ORDER BY topic",
    )
    .bind(version_number(version))
    .fetch_all(&mut *conn)
    .await?;
    rows.iter()
        .map(|json| from_json("topic", json).map_err(StorageFailure::from))
        .collect()
}

/// `version`'s topics after `after` (descending), newest id first, at most
/// `limit`.
pub(super) async fn topics_page(
    conn: &mut PgConnection,
    version: TopicModelVersion,
    after: Option<TopicId>,
    limit: i64,
) -> Result<Vec<Topic>, StorageFailure> {
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT topic_row FROM analysis.topics WHERE version = $1 \
         AND ($2::text IS NULL OR topic < $2) ORDER BY topic DESC LIMIT $3",
    )
    .bind(version_number(version))
    .bind(after.map(id_text))
    .bind(limit)
    .fetch_all(&mut *conn)
    .await?;
    rows.iter()
        .map(|json| from_json("topic", json).map_err(StorageFailure::from))
        .collect()
}

/// The ids among `ids` some stored topic already has.
pub(super) async fn existing_topics(
    conn: &mut PgConnection,
    ids: &[TopicId],
) -> Result<BTreeSet<TopicId>, StorageFailure> {
    let texts: Vec<String> = ids.iter().copied().map(id_text).collect();
    let rows: Vec<String> =
        sqlx::query_scalar("SELECT topic FROM analysis.topics WHERE topic = ANY($1)")
            .bind(&texts)
            .fetch_all(&mut *conn)
            .await?;
    rows.iter()
        .map(|text| id_of("topic", text).map_err(StorageFailure::from))
        .collect()
}

/// The version `topic` belongs to.
pub(super) async fn topic_version(
    conn: &mut PgConnection,
    topic: TopicId,
) -> Result<Option<TopicModelVersion>, StorageFailure> {
    let stored: Option<i64> =
        sqlx::query_scalar("SELECT version FROM analysis.topics WHERE topic = $1")
            .bind(id_text(topic))
            .fetch_optional(&mut *conn)
            .await?;
    stored.map(version_of).transpose()
}

/// Whether `topic` is one of `version`'s.
pub(super) async fn topic_in(
    conn: &mut PgConnection,
    version: TopicModelVersion,
    topic: TopicId,
) -> Result<bool, StorageFailure> {
    Ok(topic_version(conn, topic).await? == Some(version))
}

pub(super) async fn insert_topic(
    conn: &mut PgConnection,
    topic: &Topic,
) -> Result<(), StorageFailure> {
    sqlx::query("INSERT INTO analysis.topics (topic, version, topic_row) VALUES ($1, $2, $3)")
        .bind(id_text(topic.id))
        .bind(version_number(topic.version))
        .bind(to_json("topic", topic)?)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Store `lineage`, keyed by its older version (replacing a lineage from it
/// to a successor whose fit failed).
pub(super) async fn save_lineage(
    conn: &mut PgConnection,
    lineage: &TopicLineage,
) -> Result<(), StorageFailure> {
    sqlx::query(
        "INSERT INTO analysis.topic_lineage (from_version, to_version, lineage) VALUES ($1, $2, $3) \
         ON CONFLICT (from_version) DO UPDATE SET to_version = $2, lineage = $3",
    )
    .bind(version_number(lineage.from()))
    .bind(version_number(lineage.to()))
    .bind(to_json("lineage", lineage)?)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

pub(super) async fn lineage(
    conn: &mut PgConnection,
    from: TopicModelVersion,
) -> Result<Option<TopicLineage>, StorageFailure> {
    let stored: Option<String> =
        sqlx::query_scalar("SELECT lineage FROM analysis.topic_lineage WHERE from_version = $1")
            .bind(version_number(from))
            .fetch_optional(&mut *conn)
            .await?;
    stored
        .map(|json| from_json("lineage", &json).map_err(StorageFailure::from))
        .transpose()
}

type AssignmentRow = (String, Option<String>, i64, i64, String, String);

fn assignment_of(row: AssignmentRow) -> Result<(TransmissionId, StoredAssignment), StorageFailure> {
    let (transmission, topic, confirmed_at, matched_bytes, from, to) = row;
    let matched_bytes = u64::try_from(matched_bytes)
        .ok()
        .and_then(NonZeroU64::new)
        .ok_or_else(|| CodecError::Value {
            what: "matched bytes",
            reason: format!("{matched_bytes} is not positive"),
        })?;
    Ok((
        id_of("transmission", &transmission)?,
        StoredAssignment {
            topic: topic.map(|topic| id_of("topic", &topic)).transpose()?,
            confirmed_at: timestamp("confirmed at", confirmed_at)?,
            matched_bytes,
            from: id_of::<AgentId>("sender", &from)?,
            to: id_of::<AgentId>("reader", &to)?,
        },
    ))
}

/// Every assignment under `version`.
pub(super) async fn assignments(
    conn: &mut PgConnection,
    version: TopicModelVersion,
) -> Result<Vec<StoredAssignment>, StorageFailure> {
    let rows: Vec<AssignmentRow> = sqlx::query_as(
        "SELECT transmission, topic, confirmed_at, matched_bytes, from_agent, to_agent \
         FROM analysis.topic_assignments WHERE version = $1",
    )
    .bind(version_number(version))
    .fetch_all(&mut *conn)
    .await?;
    rows.into_iter()
        .map(|row| assignment_of(row).map(|(_, assigned)| assigned))
        .collect()
}

/// The assignments under `version` of the transmissions in `ids`.
pub(super) async fn assignments_of(
    conn: &mut PgConnection,
    version: TopicModelVersion,
    ids: &[TransmissionId],
) -> Result<BTreeMap<TransmissionId, StoredAssignment>, StorageFailure> {
    let texts: Vec<String> = ids.iter().copied().map(id_text).collect();
    let rows: Vec<AssignmentRow> = sqlx::query_as(
        "SELECT transmission, topic, confirmed_at, matched_bytes, from_agent, to_agent \
         FROM analysis.topic_assignments WHERE version = $1 AND transmission = ANY($2)",
    )
    .bind(version_number(version))
    .bind(&texts)
    .fetch_all(&mut *conn)
    .await?;
    rows.into_iter().map(assignment_of).collect()
}

pub(super) async fn insert_assignment(
    conn: &mut PgConnection,
    version: TopicModelVersion,
    transmission: TransmissionId,
    assigned: &StoredAssignment,
) -> Result<(), StorageFailure> {
    let matched_bytes =
        i64::try_from(assigned.matched_bytes.get()).map_err(|_| CodecError::Value {
            what: "matched bytes",
            reason: format!("{} does not fit a stored integer", assigned.matched_bytes),
        })?;
    sqlx::query(
        "INSERT INTO analysis.topic_assignments \
         (version, transmission, topic, confirmed_at, matched_bytes, from_agent, to_agent) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(version_number(version))
    .bind(id_text(transmission))
    .bind(assigned.topic.map(id_text))
    .bind(micros("confirmed at", assigned.confirmed_at)?)
    .bind(matched_bytes)
    .bind(id_text(assigned.from))
    .bind(id_text(assigned.to))
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// `version`'s sizes from `assigned`: every topic of `topics` once,
/// ascending, and the outliers. A transmission outside `window`, or whose
/// two agents have since merged into one
/// (`analysis.sizes.match-cross-agent-assignments`), counts nowhere.
pub(super) fn count_sizes(
    version: TopicModelVersion,
    window: Option<TimeWindow>,
    topics: &[TopicId],
    assigned: &[StoredAssignment],
    agents: &(impl AgentDirectory + ?Sized),
) -> Result<TopicSizes, DuplicateTopic> {
    let mut per_topic: BTreeMap<TopicId, Option<EdgeStats>> =
        topics.iter().map(|id| (*id, None)).collect();
    let mut outliers = None;
    for assigned in assigned {
        if window.is_some_and(|window| !window.contains(assigned.confirmed_at)) {
            continue;
        }
        if agents.canonical(assigned.from) == agents.canonical(assigned.to) {
            continue;
        }
        let slot = match assigned.topic {
            Some(topic) => per_topic.entry(topic).or_default(),
            None => &mut outliers,
        };
        *slot = Some(add_stats(*slot, assigned.matched_bytes));
    }
    let topics = per_topic
        .into_iter()
        .map(|(topic, stats)| TopicSize { topic, stats })
        .collect();
    TopicSizes::new(version, window, topics, outliers)
}

fn add_stats(stats: Option<EdgeStats>, matched_bytes: NonZeroU64) -> EdgeStats {
    match stats {
        None => EdgeStats {
            transmissions: NonZeroU64::MIN,
            matched_bytes,
        },
        Some(stats) => EdgeStats {
            transmissions: stats.transmissions.saturating_add(1),
            matched_bytes: stats.matched_bytes.saturating_add(matched_bytes.get()),
        },
    }
}
