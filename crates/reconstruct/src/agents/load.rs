//! Reading L3's agent rows back into spec values.

use std::collections::BTreeMap;

use crosstalk_spec::ids::AgentId;
use crosstalk_spec::observed::agent::{
    Agent, AgentLabel, AgentState, ClaimSet, IdentityEvidence, MergeRecord, MergeVeto, SeenClaim,
};
use crosstalk_spec::observed::client::HarnessClaim;
use crosstalk_spec::support::NonEmpty;
use sqlx::{PgConnection, PgPool};

use super::codec::{CodecError, from_json, id_of, timestamp};
use super::table::Table;
use crate::error::{StorageFailure, TxFailure};

/// Every merged agent and its target.
pub(super) async fn merged_pairs(pool: &PgPool) -> Result<Vec<(AgentId, AgentId)>, StorageFailure> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT id, merged_into FROM reconstruct.agents WHERE merged_into IS NOT NULL",
    )
    .fetch_all(pool)
    .await?;
    rows.iter()
        .map(|(id, into)| Ok((id_of("agents.id", id)?, id_of("agents.merged_into", into)?)))
        .collect::<Result<_, CodecError>>()
        .map_err(StorageFailure::from)
}

/// An agent label as stored.
pub(super) fn label(text: Option<String>) -> Result<Option<AgentLabel>, CodecError> {
    text.map(|text| {
        AgentLabel::new(&text).map_err(|error| CodecError::Json {
            column: "agents.label",
            reason: format!("{error:?}"),
        })
    })
    .transpose()
}

type AgentRow = (String, Option<String>, String, Option<String>);

/// Agents from their rows and their evidence rows (ordered by agent, then
/// position).
fn agents(
    rows: Vec<AgentRow>,
    evidence: Vec<(String, String)>,
) -> Result<BTreeMap<AgentId, Agent>, CodecError> {
    let mut held: BTreeMap<AgentId, Vec<IdentityEvidence>> = BTreeMap::new();
    for (agent, item) in evidence {
        held.entry(id_of("agent_evidence.agent", &agent)?)
            .or_default()
            .push(from_json("agent_evidence.item", &item)?);
    }
    let mut agents = BTreeMap::new();
    for (id, parent, state, label_text) in rows {
        let id: AgentId = id_of("agents.id", &id)?;
        let evidence =
            NonEmpty::from_vec(held.remove(&id).unwrap_or_default()).ok_or(CodecError::Json {
                column: "agent_evidence",
                reason: format!("agent {} holds no evidence", id.ulid_text()),
            })?;
        let parent = parent
            .map(|parent| id_of("agents.parent", &parent))
            .transpose()?;
        let state: AgentState = from_json("agents.state", &state)?;
        agents.insert(
            id,
            Agent {
                id,
                evidence,
                parent,
                state,
                label: label(label_text)?,
            },
        );
    }
    Ok(agents)
}

/// One agent, with its evidence, if stored.
pub(super) async fn agent(
    conn: &mut PgConnection,
    id: AgentId,
) -> Result<Option<Agent>, TxFailure> {
    let text = super::codec::id_text(id);
    let rows: Vec<AgentRow> =
        sqlx::query_as("SELECT id, parent, state, label FROM reconstruct.agents WHERE id = $1")
            .bind(&text)
            .fetch_all(&mut *conn)
            .await?;
    if rows.is_empty() {
        return Ok(None);
    }
    let evidence: Vec<(String, String)> = sqlx::query_as(
        "SELECT agent, item FROM reconstruct.agent_evidence WHERE agent = $1 ORDER BY position",
    )
    .bind(&text)
    .fetch_all(&mut *conn)
    .await?;
    Ok(agents(rows, evidence)?.remove(&id))
}

/// Claim sets from claim rows.
fn claim_sets(rows: Vec<(String, String, i64)>) -> Result<BTreeMap<AgentId, ClaimSet>, CodecError> {
    let mut entries: BTreeMap<AgentId, Vec<SeenClaim>> = BTreeMap::new();
    for (agent, claim, last_seen) in rows {
        let claim: HarnessClaim = from_json("claims.claim", &claim)?;
        entries
            .entry(id_of("claims.agent", &agent)?)
            .or_default()
            .push(SeenClaim {
                claim,
                last_seen: timestamp("claims.last_seen", last_seen)?,
            });
    }
    entries
        .into_iter()
        .map(|(agent, entries)| {
            ClaimSet::from_entries(entries)
                .map(|set| (agent, set))
                .map_err(|error| CodecError::Json {
                    column: "claims",
                    reason: format!("{error:?}"),
                })
        })
        .collect()
}

/// The JSON aggregates of the snapshot statement: one statement reads
/// every table in one snapshot and one round trip.
const SNAPSHOT: &str = "SELECT \
    (SELECT coalesce(json_agg(json_build_array(id, parent, state, label) ORDER BY id), '[]')::text \
     FROM reconstruct.agents), \
    (SELECT coalesce(json_agg(json_build_array(agent, item) ORDER BY agent, position), '[]')::text \
     FROM reconstruct.agent_evidence), \
    (SELECT coalesce(json_agg(record ORDER BY id), '[]')::text FROM reconstruct.merges), \
    (SELECT coalesce(json_agg(veto ORDER BY a, b), '[]')::text FROM reconstruct.vetoes), \
    (SELECT coalesce(json_agg(json_build_array(agent, claim, last_seen)), '[]')::text \
     FROM reconstruct.claims WHERE $1), \
    (SELECT coalesce(json_agg(json_build_array(agent, last_seen)), '[]')::text \
     FROM reconstruct.activity WHERE $1)";

fn rows<T: serde::de::DeserializeOwned>(column: &'static str, text: &str) -> Result<T, CodecError> {
    from_json(column, text)
}

/// Every table in one statement (one snapshot). Claims and activity only
/// when `with_activity`.
async fn snapshot(conn: &mut PgConnection, with_activity: bool) -> Result<Table, TxFailure> {
    let (agents_json, evidence_json, merges_json, vetoes_json, claims_json, activity_json): (
        String,
        String,
        String,
        String,
        String,
        String,
    ) = sqlx::query_as(SNAPSHOT)
        .bind(with_activity)
        .fetch_one(&mut *conn)
        .await?;
    let agent_rows: Vec<AgentRow> = rows("agents", &agents_json)?;
    let evidence_rows: Vec<(String, String)> = rows("agent_evidence", &evidence_json)?;
    let mut merges = BTreeMap::new();
    for record in rows::<Vec<String>>("merges", &merges_json)? {
        let record: MergeRecord = from_json("merges.record", &record)?;
        merges.insert(record.id(), record);
    }
    let mut vetoes = BTreeMap::new();
    for veto in rows::<Vec<String>>("vetoes", &vetoes_json)? {
        let veto: MergeVeto = from_json("vetoes.veto", &veto)?;
        vetoes.insert((veto.a(), veto.b()), veto);
    }
    let claims = claim_sets(rows("claims", &claims_json)?)?;
    let mut activity = BTreeMap::new();
    for (agent, at) in rows::<Vec<(String, i64)>>("activity", &activity_json)? {
        activity.insert(
            id_of("activity.agent", &agent)?,
            timestamp("activity.last_seen", at)?,
        );
    }
    Ok(Table {
        agents: agents(agent_rows, evidence_rows)?,
        merges,
        vetoes,
        claims,
        activity,
    })
}

/// What a write's decision reads: every agent, the merge log and the
/// vetoes.
pub(super) async fn merge_table(conn: &mut PgConnection) -> Result<Table, TxFailure> {
    snapshot(conn, false).await
}

/// Everything, for the reads.
pub(super) async fn full_table(conn: &mut PgConnection) -> Result<Table, TxFailure> {
    snapshot(conn, true).await
}

/// The claim sets and latest activity of the cluster `id` belongs to, in
/// one statement.
pub(super) async fn cluster_activity(
    conn: &mut PgConnection,
    id: AgentId,
) -> Result<
    (
        BTreeMap<AgentId, ClaimSet>,
        Option<crosstalk_spec::support::Timestamp>,
    ),
    TxFailure,
> {
    let (claims_json, last_seen): (String, Option<i64>) = sqlx::query_as(
        "WITH target AS ( \
             SELECT coalesce((SELECT merged_into FROM reconstruct.agents WHERE id = $1), $1) AS canonical \
         ), members AS ( \
             SELECT t.canonical AS id FROM target t \
             UNION SELECT a.id FROM reconstruct.agents a, target t WHERE a.merged_into = t.canonical \
         ) \
         SELECT \
             (SELECT coalesce(json_agg(json_build_array(c.agent, c.claim, c.last_seen)), '[]')::text \
              FROM reconstruct.claims c JOIN members m ON m.id = c.agent), \
             (SELECT max(v.last_seen) FROM reconstruct.activity v JOIN members m ON m.id = v.agent)",
    )
    .bind(super::codec::id_text(id))
    .fetch_one(&mut *conn)
    .await?;
    let claims = claim_sets(rows("claims", &claims_json)?)?;
    let last_seen = last_seen
        .map(|at| timestamp("activity.last_seen", at))
        .transpose()?;
    Ok((claims, last_seen))
}
