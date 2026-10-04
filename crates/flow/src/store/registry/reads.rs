//! The registry's reads: stored channels with their cross-agent traffic
//! (`ChannelReads`), a channel's transmissions, its resources' use and a
//! policy history. Each runs in one `REPEATABLE READ` snapshot; a list
//! issues its next cursor after the snapshot ends.
//!
//! **Traffic is read, not stored.** A channel's [`CrossTraffic`] is the
//! tally over the recorded transmissions whose stored route channel is it
//! or a channel it superseded, with agents resolved through the registry's
//! `AgentDirectory` at the read.
//!
//! Lists walk their sort order in chunks of [`CHUNK`] rows and keep the
//! rows the filter keeps until one more than the page holds is found, so a
//! page reads only as far as it needs.

use std::collections::{BTreeMap, HashMap};

use std::num::NonZeroU64;

use crosstalk_spec::aggregates::access::{AgentAccesses, ResourceUse};
use crosstalk_spec::derived::flow::channel::confirmation::{Confirmation, CrossTraffic};
use crosstalk_spec::derived::flow::channel::policy::PolicyHistory;
use crosstalk_spec::derived::flow::resource::Resource;
use crosstalk_spec::derived::flow::transmission::{Crossing, Transmission};
use crosstalk_spec::ids::{AgentId, ChannelId, ResourceId, TransmissionId};
use crosstalk_spec::interfaces::l5_flow::RegistryError;
use crosstalk_spec::interfaces::l5_flow::channels::ChannelWithTraffic;
use crosstalk_spec::interfaces::l8_surface::channel_traffic::ChannelTransmissionFilter;
use crosstalk_spec::interfaces::l8_surface::lists::ChannelFilter;
use crosstalk_spec::support::{TimeWindow, Timestamp};
use sqlx::PgConnection;

use super::rows::{self, StoredChannel};
use crate::store::codec::{count, from_json, id_text, micros, parse_id};
use crate::store::error::Fault;

/// How many rows a list reads per statement.
pub(crate) const CHUNK: i64 = 128;

/// Where `ChannelReads::channels` resumes: after this (`created_at`, id).
pub(crate) type ChannelKey = (Timestamp, ChannelId);

/// Where `ChannelReads::transmissions` resumes: after this (`opened_at`, id).
pub(crate) type TransmissionKey = (Timestamp, TransmissionId);

/// A read's result, or the spec refusal it ends in.
pub(crate) type Read<T> = Result<Result<T, RegistryError>, Fault>;

/// The columns of a channel row as reads return it, in one statement: the
/// channel, its own resources in joining order, and every recorded
/// transmission routed through its canonical channel or a channel that one
/// superseded. Spliced into the queries below by `channel_select!`.
macro_rules! channel_select {
    ($tail:literal) => {
        concat!(
            "SELECT c.id, c.origin, c.policy, c.created_at, \
             ARRAY(SELECT r.id FROM flow.resources r \
                   WHERE r.channel_id = c.id AND r.listed_seq IS NOT NULL \
                   ORDER BY r.listed_seq), \
             ARRAY(SELECT t.transmission FROM flow.channel_traffic t \
                   JOIN flow.channels rc ON rc.id = t.channel_id \
                   WHERE rc.id = COALESCE(c.superseded_by, c.id) \
                      OR rc.superseded_by = COALESCE(c.superseded_by, c.id)) \
             FROM flow.channels c ",
            $tail
        )
    };
}

/// One row of `channel_select!`.
type ChannelRow = (String, String, String, i64, Vec<String>, Vec<String>);

/// A channel row with its traffic tallied, agents resolved through `agent`.
fn read_row(
    (id, origin, policy, _, listed, routed): ChannelRow,
    agent: &(impl Fn(AgentId) -> AgentId + Copy),
) -> Result<ChannelWithTraffic, Fault> {
    let stored = StoredChannel {
        id: parse_id("channels.id", &id)?,
        origin: from_json("channels.origin", &origin)?,
        policy: from_json("channels.policy", &policy)?,
    };
    let resources = listed
        .iter()
        .map(|id| parse_id("resources.id", id))
        .collect::<Result<Vec<ResourceId>, _>>()?;
    let routed = routed
        .iter()
        .map(|text| from_json::<Transmission>("channel_traffic.transmission", text))
        .collect::<Result<Vec<_>, _>>()?;
    let traffic = CrossTraffic::tally(&routed, *agent);
    Ok(ChannelWithTraffic::new(
        stored.with_resources(resources),
        traffic,
    ))
}

/// `ChannelReads::channel`, in one statement.
pub(crate) async fn channel(
    conn: &mut PgConnection,
    id: ChannelId,
    agent: &(impl Fn(AgentId) -> AgentId + Copy),
) -> Result<Option<ChannelWithTraffic>, Fault> {
    let row: Option<ChannelRow> = sqlx::query_as(channel_select!("WHERE c.id = $1"))
        .bind(id_text(id))
        .fetch_optional(&mut *conn)
        .await?;
    row.map(|row| read_row(row, agent)).transpose()
}

/// `ChannelReads::channels`, before paging: up to `limit` channels `filter`
/// keeps, newest created first (ties by id), after `after`.
pub(crate) async fn channels(
    conn: &mut PgConnection,
    filter: &ChannelFilter,
    after: Option<ChannelKey>,
    limit: usize,
    agent: &(impl Fn(AgentId) -> AgentId + Copy),
) -> Result<Vec<ChannelWithTraffic>, Fault> {
    let mut kept = Vec::new();
    let mut position = match after {
        Some((at, id)) => Some((micros("channels.created_at", at)?, id_text(id))),
        None => None,
    };
    loop {
        let (at, id) = position.clone().unzip();
        let rows: Vec<ChannelRow> = sqlx::query_as(channel_select!(
            "WHERE $1::bigint IS NULL OR (c.created_at, c.id) < ($1, $2::text COLLATE \"C\") \
             ORDER BY c.created_at DESC, c.id DESC LIMIT $3"
        ))
        .bind(at)
        .bind(id)
        .bind(CHUNK)
        .fetch_all(&mut *conn)
        .await?;
        let exhausted = i64::try_from(rows.len()).unwrap_or(i64::MAX) < CHUNK;
        position = rows.last().map(|row| (row.3, row.0.clone()));
        for row in rows {
            let read = read_row(row, agent)?;
            if filter.keeps(&read) {
                kept.push(read);
                if kept.len() >= limit {
                    return Ok(kept);
                }
            }
        }
        if exhausted {
            return Ok(kept);
        }
    }
}

/// `ChannelReads::transmissions`, before paging: up to `limit` crossing
/// transmissions routed through `canonical`'s members that `filter` keeps,
/// newest opened first (ties by id), after `after`.
pub(crate) async fn transmissions(
    conn: &mut PgConnection,
    canonical: ChannelId,
    filter: &ChannelTransmissionFilter,
    after: Option<TransmissionKey>,
    limit: usize,
    agent: &(impl Fn(AgentId) -> AgentId + Copy),
) -> Result<Vec<Transmission>, Fault> {
    let members = rows::members(conn, canonical).await?;
    let confirmed = filter
        .confirmation
        .map(|wanted| wanted == Confirmation::Confirmed);
    let mut kept = Vec::new();
    let mut position = match after {
        Some((at, id)) => Some((micros("channel_traffic.opened_at", at)?, id_text(id))),
        None => None,
    };
    loop {
        let (at, id) = position.clone().unzip();
        let rows: Vec<(String, i64, String)> = sqlx::query_as(
            "SELECT transmission_id, opened_at, transmission FROM flow.channel_traffic \
             WHERE channel_id = ANY($1) AND ($2::boolean IS NULL OR confirmed = $2) \
               AND ($3::bigint IS NULL \
                    OR (opened_at, transmission_id) < ($3, $4::text COLLATE \"C\")) \
             ORDER BY opened_at DESC, transmission_id DESC LIMIT $5",
        )
        .bind(&members)
        .bind(confirmed)
        .bind(at)
        .bind(id)
        .bind(CHUNK)
        .fetch_all(&mut *conn)
        .await?;
        let exhausted = i64::try_from(rows.len()).unwrap_or(i64::MAX) < CHUNK;
        position = rows.last().map(|(id, at, _)| (*at, id.clone()));
        for (_, _, text) in rows {
            let transmission: Transmission = from_json("channel_traffic.transmission", &text)?;
            if transmission.crossing(*agent) == Crossing::Crosses {
                kept.push(transmission);
                if kept.len() >= limit {
                    return Ok(kept);
                }
            }
        }
        if exhausted {
            return Ok(kept);
        }
    }
}

/// `ChannelRegistry::resource_use`, before paging: up to `limit` resources
/// held by `canonical`'s members with an access in `window`, newest
/// (highest id) first, after `after`, with their canonical writers and
/// readers.
pub(crate) async fn resource_use(
    conn: &mut PgConnection,
    canonical: ChannelId,
    window: TimeWindow,
    after: Option<ResourceId>,
    limit: usize,
    agent: &(impl Fn(AgentId) -> AgentId + Copy),
) -> Result<Result<Vec<ResourceUse>, RegistryError>, Fault> {
    let start = micros("window.start", window.start())?;
    let end = micros("window.end", window.end())?;
    // One statement: the page of resources, then their access counts per
    // agent and kind in the window.
    let rows: Vec<(String, String, String, String, i64)> = sqlx::query_as(
        "WITH page AS ( \
             SELECT r.id, r.resource FROM flow.resources r \
             JOIN flow.channels c ON c.id = r.channel_id \
             WHERE (c.id = $1 OR c.superseded_by = $1) \
               AND ($2::text IS NULL OR r.id < $2::text COLLATE \"C\") \
               AND EXISTS (SELECT 1 FROM flow.accesses a \
                           WHERE a.resource_id = r.id AND a.at >= $3 AND a.at < $4) \
             ORDER BY r.id DESC LIMIT $5) \
         SELECT p.id, p.resource, a.agent, a.kind, count(*) FROM page p \
         JOIN flow.accesses a ON a.resource_id = p.id AND a.at >= $3 AND a.at < $4 \
         GROUP BY p.id, p.resource, a.agent, a.kind ORDER BY p.id DESC",
    )
    .bind(id_text(canonical))
    .bind(after.map(id_text))
    .bind(start)
    .bind(end)
    .bind(i64::try_from(limit).unwrap_or(i64::MAX))
    .fetch_all(&mut *conn)
    .await?;
    let mut resources: Vec<(String, String)> = Vec::new();
    let mut counts = Vec::with_capacity(rows.len());
    for (id, resource, agent, kind, n) in rows {
        if resources.last().is_none_or(|(last, _)| *last != id) {
            resources.push((id.clone(), resource));
        }
        counts.push((id, agent, kind, n));
    }
    type Counts = BTreeMap<AgentId, u64>;
    let mut tallies: HashMap<String, (Counts, Counts)> = HashMap::new();
    for (resource, raw_agent, kind, n) in counts {
        let canonical_agent = agent(parse_id("accesses.agent", &raw_agent)?);
        let (writers, readers) = tallies.entry(resource).or_default();
        let side = if kind == "write" { writers } else { readers };
        *side.entry(canonical_agent).or_default() += count("access count", n)?;
    }
    let list = |counts: Counts| -> Vec<AgentAccesses> {
        counts
            .into_iter()
            .filter_map(|(agent, n)| {
                NonZeroU64::new(n).map(|accesses| AgentAccesses { agent, accesses })
            })
            .collect()
    };
    let mut uses = Vec::with_capacity(resources.len());
    for (id, resource) in resources {
        let resource: Resource = from_json("resources.resource", &resource)?;
        let (writers, readers) = tallies.remove(&id).unwrap_or_default();
        match ResourceUse::new(resource, list(writers), list(readers)) {
            Ok(row) => uses.push(row),
            Err(error) => {
                return Ok(Err(RegistryError::Store {
                    reason: format!("resource use refused: {error:?}"),
                }));
            }
        }
    }
    Ok(Ok(uses))
}

/// `ChannelRegistry::policy_history`, in one statement.
pub(crate) async fn policy_history(conn: &mut PgConnection, id: ChannelId) -> Read<PolicyHistory> {
    let row: Option<(Vec<String>,)> = sqlx::query_as(
        "SELECT ARRAY(SELECT d.decision FROM flow.policy_decisions d \
                      WHERE d.channel_id = c.id ORDER BY d.at, d.seq) \
         FROM flow.channels c WHERE c.id = $1",
    )
    .bind(id_text(id))
    .fetch_optional(&mut *conn)
    .await?;
    let Some((decisions,)) = row else {
        return Ok(Err(RegistryError::UnknownChannel(id)));
    };
    Ok(Ok(rows::history_of(&decisions)?))
}
