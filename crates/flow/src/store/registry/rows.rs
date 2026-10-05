//! The registry's rows: reading and writing channels, resources and policy
//! histories inside a transaction. Every function takes the transaction's
//! connection; none commits.
//!
//! A channel row's `kind`, `created_at` and `superseded_by` are derived from
//! its origin by [`OriginColumns`], the only place that writes them, so
//! they never disagree with the stored origin.

use std::collections::HashMap;

use crosstalk_spec::derived::flow::channel::policy::{
    Policy, PolicyDecision, PolicyHistory, Recorded,
};
use crosstalk_spec::derived::flow::channel::{Channel, ChannelOrigin};
use crosstalk_spec::derived::flow::resource::{Locator, Resource, ResourcePattern};
use crosstalk_spec::ids::{ChannelId, ResourceId};
use crosstalk_spec::interfaces::l5_flow::ChannelLookup;
use sqlx::PgConnection;

use crate::store::codec::{from_json, id_text, json, micros, parse_id};
use crate::store::error::Fault;

/// A channel as its row stores it, without its resource list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredChannel {
    pub(crate) id: ChannelId,
    pub(crate) origin: ChannelOrigin,
    pub(crate) policy: Policy,
}

impl StoredChannel {
    /// The full channel, with its own resources in the order they joined.
    pub(crate) fn with_resources(self, resources: Vec<ResourceId>) -> Channel {
        Channel {
            id: self.id,
            origin: self.origin,
            resources,
            policy: self.policy,
        }
    }
}

/// A stored resource and the channel it is on, `None` for none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredResource {
    pub(crate) resource: Resource,
    pub(crate) channel: Option<ChannelId>,
}

/// The columns derived from a channel's origin.
struct OriginColumns {
    kind: &'static str,
    created_at: i64,
    superseded_by: Option<String>,
    origin: String,
}

impl OriginColumns {
    fn of(origin: &ChannelOrigin) -> Result<Self, Fault> {
        let kind = match origin {
            ChannelOrigin::Declared { .. } => "declared",
            ChannelOrigin::Discovered { .. } => "discovered",
            ChannelOrigin::Superseded { .. } => "superseded",
        };
        Ok(Self {
            kind,
            created_at: micros("channels.created_at", origin.created_at())?,
            superseded_by: origin.supersession().map(|s| id_text(s.by)),
            origin: json("channel origin", origin)?,
        })
    }
}

/// The JSON text that keys a locator: one stored resource per locator.
pub(crate) fn locator_key(locator: &Locator) -> Result<String, Fault> {
    Ok(json("locator", locator)?)
}

fn stored_channel(id: &str, origin: &str, policy: &str) -> Result<StoredChannel, Fault> {
    Ok(StoredChannel {
        id: parse_id("channels.id", id)?,
        origin: from_json("channels.origin", origin)?,
        policy: from_json("channels.policy", policy)?,
    })
}

/// The channel stored under `id`.
pub(crate) async fn channel(
    conn: &mut PgConnection,
    id: ChannelId,
) -> Result<Option<StoredChannel>, Fault> {
    let row: Option<(String, String, String)> =
        sqlx::query_as("SELECT id, origin, policy FROM flow.channels WHERE id = $1")
            .bind(id_text(id))
            .fetch_optional(&mut *conn)
            .await?;
    row.map(|(id, origin, policy)| stored_channel(&id, &origin, &policy))
        .transpose()
}

/// Every stored channel, in registry order (ascending id).
pub(crate) async fn all_channels(conn: &mut PgConnection) -> Result<Vec<StoredChannel>, Fault> {
    let rows: Vec<(String, String, String)> =
        sqlx::query_as("SELECT id, origin, policy FROM flow.channels ORDER BY id")
            .fetch_all(&mut *conn)
            .await?;
    rows.iter()
        .map(|(id, origin, policy)| stored_channel(id, origin, policy))
        .collect()
}

/// The declared channels and their patterns, in registry order.
pub(crate) async fn declared(
    conn: &mut PgConnection,
) -> Result<Vec<(ChannelId, ResourcePattern)>, Fault> {
    let rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT id, origin, policy FROM flow.channels WHERE kind = 'declared' ORDER BY id",
    )
    .fetch_all(&mut *conn)
    .await?;
    let mut declared = Vec::with_capacity(rows.len());
    for (id, origin, policy) in rows {
        let stored = stored_channel(&id, &origin, &policy)?;
        if let Some(pattern) = stored.origin.pattern() {
            declared.push((stored.id, pattern.clone()));
        }
    }
    Ok(declared)
}

/// `ChannelDirectory::canonical` read in the transaction; `None` for an
/// unknown id.
pub(crate) async fn canonical(
    conn: &mut PgConnection,
    id: ChannelId,
) -> Result<Option<ChannelId>, Fault> {
    let row: Option<(Option<String>,)> =
        sqlx::query_as("SELECT superseded_by FROM flow.channels WHERE id = $1")
            .bind(id_text(id))
            .fetch_optional(&mut *conn)
            .await?;
    match row {
        None => Ok(None),
        Some((None,)) => Ok(Some(id)),
        Some((Some(by),)) => Ok(Some(parse_id("channels.superseded_by", &by)?)),
    }
}

/// The ids of `canonical` and of every channel it superseded.
pub(crate) async fn members(
    conn: &mut PgConnection,
    canonical: ChannelId,
) -> Result<Vec<String>, Fault> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT id FROM flow.channels WHERE id = $1 OR superseded_by = $1 ORDER BY id",
    )
    .bind(id_text(canonical))
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows.into_iter().map(|(id,)| id).collect())
}

/// Store a new channel.
pub(crate) async fn insert_channel(
    conn: &mut PgConnection,
    id: ChannelId,
    origin: &ChannelOrigin,
    policy: &Policy,
) -> Result<(), Fault> {
    let columns = OriginColumns::of(origin)?;
    sqlx::query(
        "INSERT INTO flow.channels (id, kind, origin, created_at, superseded_by, policy) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(id_text(id))
    .bind(columns.kind)
    .bind(columns.origin)
    .bind(columns.created_at)
    .bind(columns.superseded_by)
    .bind(json("channel policy", policy)?)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Replace a stored channel's origin.
pub(crate) async fn set_origin(
    conn: &mut PgConnection,
    id: ChannelId,
    origin: &ChannelOrigin,
) -> Result<(), Fault> {
    let columns = OriginColumns::of(origin)?;
    sqlx::query(
        "UPDATE flow.channels SET kind = $2, origin = $3, created_at = $4, superseded_by = $5 \
         WHERE id = $1",
    )
    .bind(id_text(id))
    .bind(columns.kind)
    .bind(columns.origin)
    .bind(columns.created_at)
    .bind(columns.superseded_by)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Every resource stored on one of `ids` (seeds included), by channel.
pub(crate) async fn held_resources(
    conn: &mut PgConnection,
    ids: &[String],
) -> Result<HashMap<ChannelId, Vec<Resource>>, Fault> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT channel_id, resource FROM flow.resources WHERE channel_id = ANY($1) ORDER BY id",
    )
    .bind(ids)
    .fetch_all(&mut *conn)
    .await?;
    let mut held: HashMap<ChannelId, Vec<Resource>> = HashMap::new();
    for (channel, resource) in rows {
        held.entry(parse_id("resources.channel_id", &channel)?)
            .or_default()
            .push(from_json("resources.resource", &resource)?);
    }
    Ok(held)
}

/// The stored resources with the given ids.
pub(crate) async fn resources_by_id(
    conn: &mut PgConnection,
    ids: &[String],
) -> Result<HashMap<ResourceId, Resource>, Fault> {
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT id, resource FROM flow.resources WHERE id = ANY($1)")
            .bind(ids)
            .fetch_all(&mut *conn)
            .await?;
    let mut found = HashMap::with_capacity(rows.len());
    for (id, resource) in rows {
        found.insert(
            parse_id("resources.id", &id)?,
            from_json("resources.resource", &resource)?,
        );
    }
    Ok(found)
}

fn stored_resource(resource: &str, channel: Option<&str>) -> Result<StoredResource, Fault> {
    Ok(StoredResource {
        resource: from_json("resources.resource", resource)?,
        channel: channel
            .map(|channel| parse_id("resources.channel_id", channel))
            .transpose()?,
    })
}

/// The resource stored under `id`.
pub(crate) async fn resource(
    conn: &mut PgConnection,
    id: ResourceId,
) -> Result<Option<StoredResource>, Fault> {
    let row: Option<(String, Option<String>)> =
        sqlx::query_as("SELECT resource, channel_id FROM flow.resources WHERE id = $1")
            .bind(id_text(id))
            .fetch_optional(&mut *conn)
            .await?;
    row.map(|(resource, channel)| stored_resource(&resource, channel.as_deref()))
        .transpose()
}

/// The resource stored with `locator`.
pub(crate) async fn resource_with_locator(
    conn: &mut PgConnection,
    locator: &Locator,
) -> Result<Option<StoredResource>, Fault> {
    let row: Option<(String, Option<String>)> =
        sqlx::query_as("SELECT resource, channel_id FROM flow.resources WHERE locator_key = $1")
            .bind(locator_key(locator)?)
            .fetch_optional(&mut *conn)
            .await?;
    row.map(|(resource, channel)| stored_resource(&resource, channel.as_deref()))
        .transpose()
}

/// `ChannelRegistry::lookup` in one statement: `Known` on the canonical
/// channel of the stored resource with that locator when it is on one,
/// else `Declared` for the first declared channel (registry order) whose
/// pattern matches, else `NoChannel`.
pub(crate) async fn lookup(
    conn: &mut PgConnection,
    locator: &Locator,
) -> Result<ChannelLookup, Fault> {
    // The known row (if any) first, then every declared channel by id.
    let rows: Vec<(bool, String, Option<String>)> = sqlx::query_as(
        "SELECT known, id, origin FROM ( \
             SELECT true AS known, COALESCE(c.superseded_by, c.id) AS id, NULL::text AS origin \
             FROM flow.resources r JOIN flow.channels c ON c.id = r.channel_id \
             WHERE r.locator_key = $1 \
             UNION ALL \
             SELECT false, id, origin FROM flow.channels WHERE kind = 'declared' \
         ) candidates ORDER BY known DESC, id",
    )
    .bind(locator_key(locator)?)
    .fetch_all(&mut *conn)
    .await?;
    for (known, id, origin) in rows {
        if known {
            return Ok(ChannelLookup::Known(parse_id("channels.id", &id)?));
        }
        let origin: ChannelOrigin =
            from_json("channels.origin", origin.as_deref().unwrap_or_default())?;
        if origin
            .pattern()
            .is_some_and(|pattern| pattern.matches(locator))
        {
            return Ok(ChannelLookup::Declared(parse_id("channels.id", &id)?));
        }
    }
    Ok(ChannelLookup::NoChannel)
}

/// Store a resource seen for the first time on `channel` (`None` for
/// none); on a channel it is listed among that channel's own resources.
pub(crate) async fn insert_resource(
    conn: &mut PgConnection,
    resource: &Resource,
    channel: Option<ChannelId>,
) -> Result<(), Fault> {
    sqlx::query(
        "INSERT INTO flow.resources (id, locator_key, resource, channel_id, listed_seq) \
         VALUES ($1, $2, $3, $4, \
                 CASE WHEN $4::text IS NULL THEN NULL ELSE nextval('flow.resource_listing') END)",
    )
    .bind(id_text(resource.id))
    .bind(locator_key(&resource.locator)?)
    .bind(json("resource", resource)?)
    .bind(channel.map(id_text))
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Move a resource stored on no channel onto `channel`, listed among its
/// own resources (it joins a declared channel).
pub(crate) async fn list_on(
    conn: &mut PgConnection,
    resource: ResourceId,
    channel: ChannelId,
) -> Result<(), Fault> {
    sqlx::query(
        "UPDATE flow.resources SET channel_id = $2, listed_seq = nextval('flow.resource_listing') \
         WHERE id = $1",
    )
    .bind(id_text(resource))
    .bind(id_text(channel))
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Move a resource stored on no channel onto the channel it seeds (held
/// through the channel's origin, not listed).
pub(crate) async fn seed_on(
    conn: &mut PgConnection,
    resource: ResourceId,
    channel: ChannelId,
) -> Result<(), Fault> {
    sqlx::query("UPDATE flow.resources SET channel_id = $2, listed_seq = NULL WHERE id = $1")
        .bind(id_text(resource))
        .bind(id_text(channel))
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// The channel's policy history, oldest first.
pub(crate) async fn history(
    conn: &mut PgConnection,
    id: ChannelId,
) -> Result<PolicyHistory, Fault> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT decision FROM flow.policy_decisions WHERE channel_id = $1 ORDER BY at, seq",
    )
    .bind(id_text(id))
    .fetch_all(&mut *conn)
    .await?;
    let decisions: Vec<String> = rows.into_iter().map(|(decision,)| decision).collect();
    history_of(&decisions)
}

/// A policy history from its stored decisions, in (at, seq) order.
pub(crate) fn history_of(decisions: &[String]) -> Result<PolicyHistory, Fault> {
    let entries = decisions
        .iter()
        .map(|decision| from_json::<PolicyDecision>("policy_decisions.decision", decision))
        .collect::<Result<Vec<_>, _>>()?;
    PolicyHistory::from_entries(entries).map_err(|error| Fault::corrupt("policy history", error))
}

/// Record `decision` in the channel's history and set the channel's policy
/// to the history's current one: `PolicyHistory::record`'s result, with
/// nothing written for a duplicate.
pub(crate) async fn record_decision(
    conn: &mut PgConnection,
    id: ChannelId,
    decision: &PolicyDecision,
) -> Result<(Recorded, Policy), Fault> {
    let mut history = history(conn, id).await?;
    let recorded = history.record(decision.clone());
    let current = history.current();
    if recorded != Recorded::Duplicate {
        sqlx::query(
            "INSERT INTO flow.policy_decisions (channel_id, at, decision) VALUES ($1, $2, $3)",
        )
        .bind(id_text(id))
        .bind(micros("policy_decisions.at", decision.decision.at)?)
        .bind(json("policy decision", decision)?)
        .execute(&mut *conn)
        .await?;
        sqlx::query("UPDATE flow.channels SET policy = $2 WHERE id = $1")
            .bind(id_text(id))
            .bind(json("channel policy", &current)?)
            .execute(&mut *conn)
            .await?;
    }
    Ok((recorded, current))
}
