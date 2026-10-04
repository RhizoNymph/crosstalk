//! The alert store's rows: how a transaction loads and saves rules, alerts,
//! the consumer's current version and model, and triage's verdict copy,
//! and how storage failures become each spec error's `Store` variant.

use std::collections::BTreeSet;
use std::num::NonZeroU32;

use crosstalk_spec::aggregates::alert::{
    Alert, AlertRevision, AlertRuleDef, AlertState, AlertStateKind, AlertSubject, BuiltinRule,
    RuleRevision, SuppressReason,
};
use crosstalk_spec::aggregates::topic::{EmbeddingModel, TopicModelVersion};
use crosstalk_spec::derived::flow::verdict::{CurrentVerdict, VerdictRevision};
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::ids::{AlertRuleId, TopicId, TransmissionId};
use crosstalk_spec::interfaces::l6_analysis::alerts::{AlertActionError, AlertReadError};
use crosstalk_spec::interfaces::l6_analysis::{RuleError, TriageError};
use crosstalk_store::{SerializableError, TxError};
use sqlx::PgConnection;

use crate::pg::StorageFailure;
use crate::pg::codec::{from_json, id_text, to_i32, to_json, to_u32};

/// A spec error with a `Store { reason }` variant, which every storage
/// failure becomes.
pub trait Failure: Sized {
    fn store(reason: String) -> Self;
}

macro_rules! failure {
    ($($error:ty),* $(,)?) => {
        $(
            impl Failure for $error {
                fn store(reason: String) -> Self {
                    Self::Store { reason }
                }
            }
        )*
    };
}

failure!(RuleError, TriageError, AlertActionError, AlertReadError);

/// A storage failure inside a transaction body: abort with the spec
/// error's `Store` variant.
pub(crate) fn abort<E: Failure>(failure: impl Into<StorageFailure>) -> TxError<E> {
    TxError::Abort(E::store(failure.into().reason()))
}

/// A storage failure outside a transaction.
pub(crate) fn fail<E: Failure>(failure: impl Into<StorageFailure>) -> E {
    E::store(failure.into().reason())
}

/// A finished transaction's result as the spec error.
pub(crate) fn finish<T, E: Failure>(result: Result<T, SerializableError<E>>) -> Result<T, E> {
    result.map_err(|error| match error {
        SerializableError::Aborted(error) => error,
        SerializableError::Store(store) => E::store(store.to_string()),
    })
}

/// What the `alerts` consumer last made current.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RuleState {
    pub version: TopicModelVersion,
    pub topics: BTreeSet<TopicId>,
}

pub(crate) async fn rule_state(conn: &mut PgConnection) -> Result<RuleState, StorageFailure> {
    let (version, topics): (i64, String) =
        sqlx::query_as("SELECT topic_version, topics FROM analysis.alert_rule_state")
            .fetch_one(&mut *conn)
            .await?;
    let version = u32::try_from(version)
        .map(TopicModelVersion)
        .map_err(|_| StorageFailure::Invariant(format!("stored topic version {version}")))?;
    let topics: Vec<TopicId> = from_json("current topics", &topics)?;
    Ok(RuleState {
        version,
        topics: topics.into_iter().collect(),
    })
}

pub(crate) async fn set_rule_version(
    conn: &mut PgConnection,
    version: TopicModelVersion,
    topics: &[TopicId],
) -> Result<(), StorageFailure> {
    sqlx::query("UPDATE analysis.alert_rule_state SET topic_version = $1, topics = $2")
        .bind(i64::from(version.0))
        .bind(to_json("current topics", &topics)?)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

pub(crate) async fn set_model(
    conn: &mut PgConnection,
    model: &EmbeddingModel,
) -> Result<(), StorageFailure> {
    sqlx::query("UPDATE analysis.alert_rule_state SET model = $1")
        .bind(to_json("embedding model", model)?)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

fn rule_revision(stored: i32) -> Result<RuleRevision, StorageFailure> {
    NonZeroU32::new(to_u32("rule revision", stored)?)
        .map(RuleRevision::new)
        .ok_or_else(|| StorageFailure::Invariant("a rule at revision 0".to_owned()))
}

fn alert_revision(stored: i32) -> Result<AlertRevision, StorageFailure> {
    NonZeroU32::new(to_u32("alert revision", stored)?)
        .map(AlertRevision::new)
        .ok_or_else(|| StorageFailure::Invariant("an alert at revision 0".to_owned()))
}

fn decode_rule(
    (definition, revision): (String, i32),
) -> Result<(AlertRuleDef, RuleRevision), StorageFailure> {
    Ok((
        from_json("alert rule", &definition)?,
        rule_revision(revision)?,
    ))
}

/// The rule stored under `id` and its revision.
pub(crate) async fn rule(
    conn: &mut PgConnection,
    id: AlertRuleId,
) -> Result<Option<(AlertRuleDef, RuleRevision)>, StorageFailure> {
    let row: Option<(String, i32)> =
        sqlx::query_as("SELECT definition, revision FROM analysis.alert_rules WHERE id = $1")
            .bind(id_text(id))
            .fetch_optional(&mut *conn)
            .await?;
    row.map(decode_rule).transpose()
}

/// Every rule and its revision, ascending id (built-in rules first).
pub(crate) async fn rules(
    conn: &mut PgConnection,
) -> Result<Vec<(AlertRuleDef, RuleRevision)>, StorageFailure> {
    let rows: Vec<(String, i32)> =
        sqlx::query_as("SELECT definition, revision FROM analysis.alert_rules ORDER BY id")
            .fetch_all(&mut *conn)
            .await?;
    rows.into_iter().map(decode_rule).collect()
}

/// Store `rule` at the revision after `previous` (`CREATED` for a new rule)
/// and return the events announcing it.
pub(crate) async fn save_rule(
    conn: &mut PgConnection,
    rule: &AlertRuleDef,
    previous: Option<RuleRevision>,
) -> Result<Vec<BusEvent>, StorageFailure> {
    let id = rule.id();
    let json = to_json("alert rule", rule)?;
    let revision = match previous {
        None => {
            let builtin = BuiltinRule::from_id(id)
                .and_then(|builtin| BuiltinRule::ALL.iter().position(|b| *b == builtin))
                .map(|index| i16::try_from(index).unwrap_or(i16::MAX));
            sqlx::query(
                "INSERT INTO analysis.alert_rules (id, builtin, definition, revision) VALUES ($1, $2, $3, 1)",
            )
            .bind(id_text(id))
            .bind(builtin)
            .bind(json)
            .execute(&mut *conn)
            .await?;
            RuleRevision::CREATED
        }
        Some(previous) => {
            let next = previous
                .next()
                .ok_or_else(|| StorageFailure::RevisionExhausted(format!("rule {id:?}")))?;
            let updated = sqlx::query(
                "UPDATE analysis.alert_rules SET definition = $2, revision = $3 WHERE id = $1 AND revision = $4",
            )
            .bind(id_text(id))
            .bind(json)
            .bind(to_i32("rule revision", next.get().get())?)
            .bind(to_i32("rule revision", previous.get().get())?)
            .execute(&mut *conn)
            .await?;
            if updated.rows_affected() != 1 {
                return Err(StorageFailure::Invariant(format!(
                    "rule {id:?} is not at revision {}",
                    previous.get()
                )));
            }
            next
        }
    };
    Ok(super::rule_events(rule, revision))
}

/// The wire text of a state kind, as the `state` column holds it.
pub(crate) fn kind_text(kind: AlertStateKind) -> &'static str {
    match kind {
        AlertStateKind::Open => "open",
        AlertStateKind::Acknowledged => "acknowledged",
        AlertStateKind::Resolved => "resolved",
        AlertStateKind::Suppressed => "suppressed",
    }
}

pub(crate) fn state_text(state: &AlertState) -> &'static str {
    kind_text(state.kind())
}

/// The kind and id columns of a subject.
pub(crate) fn subject_columns(subject: AlertSubject) -> (&'static str, String) {
    match subject {
        AlertSubject::Channel(channel) => ("channel", id_text(channel)),
        AlertSubject::Transmission(transmission) => ("transmission", id_text(transmission)),
        AlertSubject::Agent(agent) => ("agent", id_text(agent)),
    }
}

fn decode_alert(
    (alert, revision): (String, i32),
) -> Result<(Alert, AlertRevision), StorageFailure> {
    Ok((from_json("alert", &alert)?, alert_revision(revision)?))
}

/// The alert stored under `id` and its revision.
pub(crate) async fn alert(
    conn: &mut PgConnection,
    id: crosstalk_spec::ids::AlertId,
) -> Result<Option<(Alert, AlertRevision)>, StorageFailure> {
    let row: Option<(String, i32)> =
        sqlx::query_as("SELECT alert, revision FROM analysis.alerts WHERE id = $1")
            .bind(id_text(id))
            .fetch_optional(&mut *conn)
            .await?;
    row.map(decode_alert).transpose()
}

/// Which active alerts a statement selects.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Active<'a> {
    /// Raised by this rule with this stored subject.
    Key(AlertRuleId, &'a str),
    /// Raised by this rule.
    Rule(AlertRuleId),
    /// About a channel.
    Channels,
    /// About this transmission.
    Transmission(TransmissionId),
}

/// The active (open or acknowledged) alerts `which` selects, ascending id.
pub(crate) async fn active(
    conn: &mut PgConnection,
    which: Active<'_>,
) -> Result<Vec<(Alert, AlertRevision)>, StorageFailure> {
    const ACTIVE: &str =
        "SELECT alert, revision FROM analysis.alerts WHERE state IN ('open', 'acknowledged')";
    let rows: Vec<(String, i32)> = match which {
        Active::Key(rule, subject) => {
            sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "{ACTIVE} AND rule = $1 AND subject = $2 ORDER BY id"
            )))
            .bind(id_text(rule))
            .bind(subject)
            .fetch_all(&mut *conn)
            .await?
        }
        Active::Rule(rule) => {
            sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "{ACTIVE} AND rule = $1 ORDER BY id"
            )))
            .bind(id_text(rule))
            .fetch_all(&mut *conn)
            .await?
        }
        Active::Channels => {
            sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "{ACTIVE} AND subject_kind = 'channel' ORDER BY id"
            )))
            .fetch_all(&mut *conn)
            .await?
        }
        Active::Transmission(transmission) => {
            sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "{ACTIVE} AND subject_kind = 'transmission' AND subject_id = $1 ORDER BY id"
            )))
            .bind(id_text(transmission))
            .fetch_all(&mut *conn)
            .await?
        }
    };
    rows.into_iter().map(decode_alert).collect()
}

/// Store a newly opened alert at `AlertRevision::OPENED` and return the
/// events announcing it.
pub(crate) async fn open_alert(
    conn: &mut PgConnection,
    alert: &Alert,
) -> Result<Vec<BusEvent>, StorageFailure> {
    let (kind, subject_id) = subject_columns(alert.subject);
    sqlx::query(
        "INSERT INTO analysis.alerts (id, rule, subject, subject_kind, subject_id, state, alert, revision) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, 1)",
    )
    .bind(id_text(alert.id))
    .bind(id_text(alert.rule))
    .bind(to_json("alert subject", &alert.subject)?)
    .bind(kind)
    .bind(subject_id)
    .bind(state_text(&alert.state))
    .bind(to_json("alert", alert)?)
    .execute(&mut *conn)
    .await?;
    Ok(vec![
        BusEvent::Insight(InsightEvent::AlertOpened(alert.clone())),
        BusEvent::Changed(Changed::Alert(alert.id)),
    ])
}

/// Replace a stored alert with `alert` at the revision after `previous`
/// (compare-and-set on it) and return the events announcing it.
pub(crate) async fn save_alert(
    conn: &mut PgConnection,
    alert: &Alert,
    previous: AlertRevision,
) -> Result<Vec<BusEvent>, StorageFailure> {
    let next = previous
        .next()
        .ok_or_else(|| StorageFailure::RevisionExhausted(format!("alert {:?}", alert.id)))?;
    let updated = sqlx::query(
        "UPDATE analysis.alerts SET state = $2, alert = $3, revision = $4 WHERE id = $1 AND revision = $5",
    )
    .bind(id_text(alert.id))
    .bind(state_text(&alert.state))
    .bind(to_json("alert", alert)?)
    .bind(to_i32("alert revision", next.get().get())?)
    .bind(to_i32("alert revision", previous.get().get())?)
    .execute(&mut *conn)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(StorageFailure::Invariant(format!(
            "alert {:?} is not at revision {}",
            alert.id,
            previous.get()
        )));
    }
    Ok(vec![
        BusEvent::Insight(InsightEvent::AlertChanged {
            alert: alert.clone(),
            revision: next,
        }),
        BusEvent::Changed(Changed::Alert(alert.id)),
    ])
}

/// Suppress `alerts` (all active) at `at` with `reason`; all or none, as
/// the transaction is. Returns the events.
pub(crate) async fn suppress(
    conn: &mut PgConnection,
    alerts: Vec<(Alert, AlertRevision)>,
    reason: SuppressReason,
    at: crosstalk_spec::support::Timestamp,
) -> Result<Vec<BusEvent>, StorageFailure> {
    let mut events = Vec::new();
    for (mut alert, revision) in alerts {
        alert.state = AlertState::Suppressed { at, reason };
        events.extend(save_alert(conn, &alert, revision).await?);
    }
    Ok(events)
}

/// Triage's copy of `transmission`'s verdict.
pub(crate) async fn verdict(
    conn: &mut PgConnection,
    transmission: TransmissionId,
) -> Result<Option<CurrentVerdict>, StorageFailure> {
    let row: Option<(Option<String>, i32)> = sqlx::query_as(
        "SELECT verdict, revision FROM analysis.alert_verdicts WHERE transmission = $1",
    )
    .bind(id_text(transmission))
    .fetch_optional(&mut *conn)
    .await?;
    row.map(|(verdict, revision)| {
        let revision = NonZeroU32::new(to_u32("verdict revision", revision)?)
            .map(VerdictRevision::new)
            .ok_or_else(|| StorageFailure::Invariant("a verdict at revision 0".to_owned()))?;
        Ok(CurrentVerdict {
            verdict: verdict
                .map(|json| from_json("verdict", &json))
                .transpose()?,
            revision,
        })
    })
    .transpose()
}

pub(crate) async fn save_verdict(
    conn: &mut PgConnection,
    transmission: TransmissionId,
    copy: &CurrentVerdict,
) -> Result<(), StorageFailure> {
    let verdict = copy
        .verdict
        .map(|verdict| to_json("verdict", &verdict))
        .transpose()?;
    sqlx::query(
        "INSERT INTO analysis.alert_verdicts (transmission, verdict, revision) VALUES ($1, $2, $3) \
         ON CONFLICT (transmission) DO UPDATE SET verdict = EXCLUDED.verdict, revision = EXCLUDED.revision",
    )
    .bind(id_text(transmission))
    .bind(verdict)
    .bind(to_i32("verdict revision", copy.revision.get().get())?)
    .execute(&mut *conn)
    .await?;
    Ok(())
}
