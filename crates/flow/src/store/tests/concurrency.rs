//! Races on the registry, against Postgres: concurrent discoveries from one
//! resource and concurrent overlapping declarations.

use std::num::NonZeroU32;
use std::time::Duration;

use crosstalk_memory::flow::registry::model::{pattern, resource};
use crosstalk_spec::derived::flow::channel::policy::{Policy, PolicyAuthor};
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::ids::{ChannelId, TransmissionId};
use crosstalk_spec::interfaces::l5_flow::channels::ChannelTraffic;
use crosstalk_spec::interfaces::l5_flow::{ChannelRegistry, Discovery, RegistryError};
use crosstalk_store::SerializableRetry;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crosstalk_spec::support::Timestamp;

use super::support::{Failure, TestResult, agents, at, db, drain, ensure, registry, same};
use crate::store::{ChannelIdSource, EventSink, IdSourceError, PgChannelRegistry};

/// A fresh id on every call, as ULIDs are.
struct FreshIds(AtomicU64);

impl ChannelIdSource for FreshIds {
    fn pending(&self, _at: Timestamp) -> Result<ChannelId, IdSourceError> {
        Ok(ChannelId::from_ulid(
            (1u128 << 90) | u128::from(self.0.fetch_add(1, Ordering::SeqCst)),
        ))
    }

    fn consume(&self, _id: ChannelId) {}
}

const RACERS: u8 = 6;

fn patient() -> Result<SerializableRetry, Failure> {
    SerializableRetry::new(
        NonZeroU32::new(30).unwrap_or(NonZeroU32::MIN),
        Duration::from_millis(1),
        Duration::from_millis(50),
    )
    .map_err(|e| Failure::Unexpected(e.to_string()))
}

/// INV-852 `flow.registry.at-most-one-channel-per-resource`: racing
/// discoveries from one resource, each with its own minted id, commit one
/// channel: one returns Created, every other Existing with that channel,
/// one ChannelDiscovered is published and the resource is on that channel.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_concurrent_discover() -> TestResult {
    let Some(db) = db("pg_concurrent_discover").await? else {
        return Ok(());
    };
    let (registry, mut events) = registry(db.pool()).await?;
    let registry = registry.with_retry(patient()?);
    for round in 0u8..4 {
        let mut setup = registry.clone();
        setup
            .add_resource(resource(round))
            .await
            .map_err(|e| Failure::Unexpected(format!("{e:?}")))?;
        drain(&mut events);
        let mut racers = Vec::new();
        for n in 0..RACERS {
            let mut racer = registry.clone();
            let minted =
                ChannelId::from_ulid(0x0D00_0000 | (u128::from(round) << 8) | u128::from(n));
            let transmission = TransmissionId::from_ulid(0x7D00_0000 | u128::from(n));
            let id = resource(round).id;
            racers.push(tokio::spawn(async move {
                racer
                    .discover(minted, id, transmission, at(u64::from(n)))
                    .await
            }));
        }
        let mut created = Vec::new();
        let mut existing = Vec::new();
        for racer in racers {
            match racer.await? {
                Ok(Discovery::Created(id)) => created.push(id),
                Ok(Discovery::Existing(id)) => existing.push(id),
                Err(error) => return Err(Failure::Unexpected(format!("discover: {error:?}"))),
            }
        }
        same("one created", &created.len(), &1)?;
        let winner = created[0];
        ensure(existing.iter().all(|id| *id == winner), || {
            format!("existing {existing:?} vs {winner:?}")
        })?;
        let discovered = drain(&mut events)
            .into_iter()
            .filter(|event| {
                matches!(
                    event,
                    BusEvent::Detect(DetectEvent::ChannelDiscovered { .. })
                )
            })
            .count();
        same("one ChannelDiscovered", &discovered, &1)?;
        let on: Option<String> =
            sqlx::query_scalar("SELECT channel_id FROM flow.resources WHERE id = $1")
                .bind(crate::store::codec::id_text(resource(round).id))
                .fetch_one(db.pool())
                .await?;
        same(
            "resource on the winner",
            &on,
            &Some(crate::store::codec::id_text(winner)),
        )?;
    }
    let channels: i64 = sqlx::query_scalar("SELECT count(*) FROM flow.channels")
        .fetch_one(db.pool())
        .await?;
    same("one channel per resource", &channels, &4)?;
    db.close().await?;
    Ok(())
}

/// INV-259 `flow.registry.declared-patterns-disjoint` under races:
/// concurrent declarations of overlapping patterns accept exactly one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_concurrent_overlapping_declarations_accept_one() -> TestResult {
    let Some(db) = db("pg_concurrent_overlapping_declarations_accept_one").await? else {
        return Ok(());
    };
    let (sender, _events) = tokio::sync::mpsc::unbounded_channel();
    let registry = PgChannelRegistry::open(
        db.pool().clone(),
        agents().await?,
        Arc::new(FreshIds(AtomicU64::new(1))),
        EventSink::new(sender),
    )
    .await?
    .with_retry(patient()?);
    let mut racers = Vec::new();
    for n in 0..RACERS {
        let mut racer = registry.clone();
        // Patterns 0 (the host) and 1 (a prefix under it) overlap.
        let p = pattern(n % 2);
        racers.push(tokio::spawn(async move {
            racer
                .declare(
                    p,
                    Policy::Unreviewed(None),
                    PolicyAuthor::Config,
                    at(u64::from(n)),
                )
                .await
        }));
    }
    let mut accepted = Vec::new();
    for racer in racers {
        match racer.await? {
            Ok(id) => accepted.push(id),
            Err(RegistryError::OverlappingDeclaration { .. }) => {}
            Err(error) => return Err(Failure::Unexpected(format!("declare: {error:?}"))),
        }
    }
    same("one accepted", &accepted.len(), &1)?;
    let declared: i64 =
        sqlx::query_scalar("SELECT count(*) FROM flow.channels WHERE kind = 'declared'")
            .fetch_one(db.pool())
            .await?;
    same("one declared channel", &declared, &1)?;
    db.close().await?;
    Ok(())
}
