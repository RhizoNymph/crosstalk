//! The flow consumer's writes (`ChannelTraffic`), each the body of one
//! serializable transaction. Each checks before it writes, so a refusal
//! changes nothing.
//!
//! **Discovery races.** Two discoveries from one resource read the
//! resource on no channel and both try to move it; under `SERIALIZABLE`
//! the second to commit fails with a serialization failure (both wrote the
//! resource row), and its retry reads the resource on the first one's
//! channel and returns `Existing` with it. So a resource is on at most one
//! channel and one `ChannelDiscovered` is staged
//! (`flow.registry.at-most-one-channel-per-resource`).

use crosstalk_spec::derived::flow::access::{Access, AccessKind, AccessOp, WriteOutcome};
use crosstalk_spec::derived::flow::channel::detection::TrafficDetection;
use crosstalk_spec::derived::flow::channel::policy::Policy;
use crosstalk_spec::derived::flow::channel::{ChannelOrigin, Seed};
use crosstalk_spec::derived::flow::resource::Resource;
use crosstalk_spec::derived::flow::transmission::{Route, Transmission};
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::ids::{ChannelId, ResourceId, TransmissionId};
use crosstalk_spec::interfaces::l5_flow::channels::{DetectionUpdate, TrafficError};
use crosstalk_spec::interfaces::l5_flow::{ChannelLookup, Discovery};
use crosstalk_spec::support::{Change, Timestamp};
use crosstalk_store::TxError;
use sqlx::PgConnection;

use super::declarations::changed;
use super::detection::{advanced, next_origin};
use super::rows::{self, StoredResource};
use crate::store::codec::{from_json, id_text, json, micros};
use crate::store::error::Fault;

type Traffic<T> = Result<T, TxError<TrafficError>>;

fn refuse<T>(error: TrafficError) -> Traffic<T> {
    Err(TxError::Abort(error))
}

/// `ChannelTraffic::add_resource`.
pub(crate) async fn add_resource(
    conn: &mut PgConnection,
    resource: Resource,
) -> Traffic<(Option<ChannelId>, Vec<BusEvent>)> {
    let lookup = rows::lookup(conn, &resource.locator).await?;
    match rows::resource(conn, resource.id).await? {
        // Stored on no channel: it joins a declared channel whose pattern
        // now matches it, and is otherwise already where it belongs.
        Some(StoredResource { channel: None, .. }) => match lookup {
            ChannelLookup::Declared(channel) => {
                rows::list_on(conn, resource.id, channel).await?;
                Ok((Some(channel), vec![changed(channel)]))
            }
            ChannelLookup::NoChannel | ChannelLookup::Known(_) => {
                refuse(TrafficError::DuplicateResource(resource.id))
            }
        },
        Some(StoredResource {
            channel: Some(_), ..
        }) => refuse(TrafficError::DuplicateResource(resource.id)),
        None => {
            if let Some(existing) = rows::resource_with_locator(conn, &resource.locator).await? {
                return refuse(TrafficError::DuplicateLocator {
                    existing: existing.resource.id,
                    lookup,
                });
            }
            match lookup {
                ChannelLookup::Declared(channel) => {
                    rows::insert_resource(conn, &resource, Some(channel)).await?;
                    Ok((Some(channel), vec![changed(channel)]))
                }
                ChannelLookup::NoChannel => {
                    rows::insert_resource(conn, &resource, None).await?;
                    Ok((None, Vec::new()))
                }
                // A resource on a channel has this locator, and the locator
                // check above found it.
                ChannelLookup::Known(_) => refuse(TrafficError::Store {
                    reason: "a known locator without a stored resource".to_owned(),
                }),
            }
        }
    }
}

/// `ChannelTraffic::record_access`.
pub(crate) async fn record_access(conn: &mut PgConnection, access: Access) -> Traffic<()> {
    if rows::resource(conn, access.resource).await?.is_none() {
        return refuse(TrafficError::UnknownResource(access.resource));
    }
    let taken: Option<(i32,)> = sqlx::query_as("SELECT 1 FROM flow.accesses WHERE id = $1")
        .bind(id_text(access.id))
        .fetch_optional(&mut *conn)
        .await?;
    if taken.is_some() {
        return refuse(TrafficError::DuplicateAccess(access.id));
    }
    let kind = match access.op.kind() {
        AccessKind::Write => "write",
        AccessKind::Read => "read",
    };
    let write_outcome: Option<&str> = match &access.op {
        AccessOp::Write { outcome, .. } => Some(match outcome {
            WriteOutcome::Delivered => "delivered",
            WriteOutcome::Rejected => "rejected",
            WriteOutcome::Unknown => "unknown",
        }),
        AccessOp::Read { .. } => None,
    };
    sqlx::query(
        "INSERT INTO flow.accesses (id, resource_id, agent, at, kind, write_outcome, access) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(id_text(access.id))
    .bind(id_text(access.resource))
    .bind(id_text(access.agent))
    .bind(micros("accesses.at", access.at).map_err(Fault::from)?)
    .bind(kind)
    .bind(write_outcome)
    .bind(json("access", &access).map_err(Fault::from)?)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// `ChannelTraffic::discover`.
pub(crate) async fn discover(
    conn: &mut PgConnection,
    id: ChannelId,
    resource: ResourceId,
    transmission: TransmissionId,
    at: Timestamp,
) -> Traffic<(Discovery, Vec<BusEvent>)> {
    let Some(stored) = rows::resource(conn, resource).await? else {
        return refuse(TrafficError::UnknownResource(resource));
    };
    if let Some(channel) = stored.channel {
        let canonical = rows::canonical(conn, channel).await?.unwrap_or(channel);
        return Ok((Discovery::Existing(canonical), Vec::new()));
    }
    match rows::lookup(conn, &stored.resource.locator).await? {
        ChannelLookup::Declared(channel) => {
            rows::list_on(conn, resource, channel).await?;
            return Ok((Discovery::Existing(channel), vec![changed(channel)]));
        }
        // A resource on no channel is never `Known`: no other stored
        // resource has its locator.
        ChannelLookup::NoChannel | ChannelLookup::Known(_) => {}
    }
    if rows::channel(conn, id).await?.is_some() {
        return refuse(TrafficError::DuplicateChannel(id));
    }
    let seed = Seed {
        resource,
        first_transmission: transmission,
        opened_at: at,
    };
    let origin = ChannelOrigin::Discovered {
        seed,
        detection: TrafficDetection::Active {
            since: at,
            last_transmission: transmission,
        },
    };
    rows::insert_channel(conn, id, &origin, &Policy::Unreviewed(None)).await?;
    rows::seed_on(conn, resource, id).await?;
    let events = vec![
        BusEvent::Detect(DetectEvent::ChannelDiscovered { channel: id, seed }),
        changed(id),
    ];
    Ok((Discovery::Created(id), events))
}

/// `ChannelTraffic::record_transmission`.
pub(crate) async fn record_transmission(
    conn: &mut PgConnection,
    transmission: Transmission,
) -> Traffic<(Change, Vec<BusEvent>)> {
    let Route::Channel(routed) = transmission.route else {
        return refuse(TrafficError::NotChannelRouted(transmission.id));
    };
    let Some(canonical) = rows::canonical(conn, routed).await? else {
        return refuse(TrafficError::UnknownChannel(routed));
    };
    let Some(channel) = rows::channel(conn, canonical).await? else {
        return refuse(TrafficError::UnknownChannel(canonical));
    };
    let origin = advanced(&channel.origin, canonical, &transmission).map_err(TxError::Abort)?;
    let stored: Option<(String,)> =
        sqlx::query_as("SELECT transmission FROM flow.channel_traffic WHERE transmission_id = $1")
            .bind(id_text(transmission.id))
            .fetch_optional(&mut *conn)
            .await?;
    let recorded = match stored {
        None => true,
        Some((text,)) => {
            from_json::<Transmission>("channel_traffic.transmission", &text).map_err(Fault::from)?
                != transmission
        }
    };
    if origin.is_none() && !recorded {
        return Ok((Change::Unchanged, Vec::new()));
    }
    if let Some(origin) = &origin {
        rows::set_origin(conn, canonical, origin).await?;
    }
    if recorded {
        let fault = Fault::from;
        sqlx::query(
            "INSERT INTO flow.channel_traffic \
                 (transmission_id, channel_id, opened_at, confirmed, transmission) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (transmission_id) DO UPDATE SET channel_id = EXCLUDED.channel_id, \
                 opened_at = EXCLUDED.opened_at, confirmed = EXCLUDED.confirmed, \
                 transmission = EXCLUDED.transmission",
        )
        .bind(id_text(transmission.id))
        .bind(id_text(routed))
        .bind(micros("channel_traffic.opened_at", transmission.opened_at).map_err(fault)?)
        .bind(transmission.state.confirmed().is_some())
        .bind(json("transmission", &transmission).map_err(fault)?)
        .execute(&mut *conn)
        .await?;
    }
    Ok((Change::Applied, vec![changed(canonical)]))
}

/// `ChannelTraffic::set_detection`.
pub(crate) async fn set_detection(
    conn: &mut PgConnection,
    id: ChannelId,
    update: DetectionUpdate,
) -> Traffic<(Change, Vec<BusEvent>)> {
    let Some(channel) = rows::channel(conn, id).await? else {
        return refuse(TrafficError::UnknownChannel(id));
    };
    let origin = next_origin(&channel.origin, id, update).map_err(TxError::Abort)?;
    if origin == channel.origin {
        return Ok((Change::Unchanged, Vec::new()));
    }
    rows::set_origin(conn, id, &origin).await?;
    Ok((Change::Applied, vec![changed(id)]))
}
