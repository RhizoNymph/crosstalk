//! The flow consumer's durability on Postgres (`PgFlowDurability`):
//!
//! - `flow.checkpoint.ticks-with-state` (INV-1216): `flow.shard_ticks`
//!   never names a tick later than the stored checkpoint of its shard, over
//!   a run of checkpoints and a `--reset-correlator`;
//! - a restore reads back the held writes, and exactly the accesses and
//!   tool calls recorded after the checkpoint, in recording order, each
//!   access with the channel it was resolved to;
//! - a consumer restored over Postgres takes up a held write and a tool
//!   call recorded before its restart.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use crosstalk_memory::flow::MemoryVerdicts;
use crosstalk_memory::support::{IdSequence, ManualClock, Outbox};
use crosstalk_spec::derived::flow::access::{Access, AccessOp, Extraction};
use crosstalk_spec::derived::flow::resource::Resource;
use crosstalk_spec::ids::{AgentId, ChannelId};
use crosstalk_spec::interfaces::l5_flow::channels::ChannelTraffic;
use crosstalk_spec::observed::message::{PartRef, ToolCallId, ToolName};
use crosstalk_spec::support::Timestamp;
use crosstalk_store::SerializableRetry;
use crosstalk_testkit::time::{T0, after};
use sqlx::PgPool;

use crate::consumer::tests::harness::{RecordingBus, settings, wiki_page, write};
use crate::consumer::{
    Extracted, FlowConsumer, FlowDeps, FlowDurability, Recorded, Resolved, Shards, ToolCalled,
};
use crate::correlate::tests::fixtures::Scene;
use crate::store::PgFlowDurability;
use crate::store::tests::support::{Failure, TestResult, db, ensure, registry, reset, same};

fn secs(n: u64) -> Timestamp {
    after(T0, Duration::from_secs(n))
}

/// Every `(shard, ticked_through)` of `flow.shard_ticks` and of
/// `flow.checkpoints`.
async fn ticks(pool: &PgPool) -> Result<(BTreeMap<i32, i64>, BTreeMap<i32, Option<i64>>), Failure> {
    let records: Vec<(i32, i64)> =
        sqlx::query_as("SELECT shard, ticked_through FROM flow.shard_ticks")
            .fetch_all(pool)
            .await?;
    let stored: Vec<(i32, Option<i64>)> =
        sqlx::query_as("SELECT shard, ticked_through FROM flow.checkpoints")
            .fetch_all(pool)
            .await?;
    Ok((records.into_iter().collect(), stored.into_iter().collect()))
}

async fn ticks_with_state(pool: &PgPool, step: &str) -> TestResult {
    let (records, stored) = ticks(pool).await?;
    for (shard, tick) in &records {
        let snapshot = stored.get(shard).copied().flatten();
        ensure(snapshot.is_some_and(|at| *tick <= at), || {
            format!("{step}: shard {shard} ticks through {tick}, its checkpoint {snapshot:?}")
        })?;
    }
    Ok(())
}

/// Checkpoints of shards ticking forward, then a reset, never leave a
/// tick record ahead of its shard's stored state; a checkpoint's tick
/// record moves only forward.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shard_ticks_never_ahead_of_checkpoint() -> TestResult {
    let Some(db) = db("shard_ticks_never_ahead_of_checkpoint").await? else {
        return Ok(());
    };
    let pool = db.pool().clone();
    reset(&pool).await?;
    let durability = PgFlowDurability::new(pool.clone(), SerializableRetry::default());
    let config = settings(3);
    let mut shards = Shards::new(config.timing, config.content_retention, config.shards);
    // Before any tick: snapshots without ticks, no tick record.
    let Ok(checkpoint) = shards.checkpoint() else {
        return Err(Failure::Unexpected("checkpoint".to_owned()));
    };
    durability
        .save(&checkpoint, secs(0))
        .await
        .map_err(|error| Failure::Unexpected(error.to_string()))?;
    ticks_with_state(&pool, "untouched").await?;
    for n in 1..=5 {
        let _ = shards.tick(secs(n * 10));
        let Ok(checkpoint) = shards.checkpoint() else {
            return Err(Failure::Unexpected("checkpoint".to_owned()));
        };
        durability
            .save(&checkpoint, secs(n * 10))
            .await
            .map_err(|error| Failure::Unexpected(error.to_string()))?;
        ticks_with_state(&pool, &format!("checkpoint {n}")).await?;
    }
    let (records, _) = ticks(&pool).await?;
    same(
        "tick records",
        &records.values().copied().collect::<Vec<_>>(),
        &vec![i64::try_from(secs(50).as_micros()).unwrap_or(i64::MAX); 3],
    )?;
    durability
        .reset_correlator(&config, secs(60))
        .await
        .map_err(|error| Failure::Unexpected(error.to_string()))?;
    ticks_with_state(&pool, "after the reset").await?;
    // The reset leaves empty shards at the latest tick, which restore.
    let recovered = durability
        .load()
        .await
        .map_err(|error| Failure::Unexpected(error.to_string()))?;
    let Some(stored) = recovered.checkpoint else {
        return Err(Failure::Unexpected(
            "no checkpoint after the reset".to_owned(),
        ));
    };
    let restored = Shards::restore(
        config.timing,
        config.content_retention,
        config.shards,
        &stored,
    )
    .map_err(|error| Failure::Unexpected(error.to_string()))?;
    same("restored tick", &restored.last_tick(), &Some(secs(50)))
}

fn access(scene: &mut Scene, agent: AgentId, resource: Resource, at: Timestamp) -> Access {
    Access {
        id: scene.ids.access(),
        agent,
        exchange: scene.exchange(),
        resource: resource.id,
        at,
        via: Extraction::Structured,
        op: AccessOp::Read {
            result: PartRef {
                message: scene.ids.message(),
                index: 0,
            },
        },
    }
}

/// What a restore reads: the held writes, and the accesses and tool calls
/// recorded after the checkpoint, in recording order, each access with its
/// resolution (unknown until the consumer stored it); a checkpoint covers
/// what was recorded before it and drops its tool calls.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restore_reads_what_the_checkpoint_has_not_seen() -> TestResult {
    let Some(db) = db("a_restore_reads_what_the_checkpoint_has_not_seen").await? else {
        return Ok(());
    };
    let pool = db.pool().clone();
    reset(&pool).await?;
    let durability = PgFlowDurability::new(pool.clone(), SerializableRetry::default());
    let (mut registry, _events) = registry(&pool).await?;
    let mut scene = Scene::new(90);
    let (a, b) = (scene.agent(), scene.agent());
    let page = Resource {
        id: scene.resource(),
        locator: wiki_page("Durable"),
        first_seen: secs(1),
    };
    registry
        .add_resource(page.clone())
        .await
        .map_err(|error| Failure::Unexpected(format!("{error:?}")))?;
    let unchecked = |error| Failure::Unexpected(format!("{error:?}"));
    let first = access(&mut scene, a, page.clone(), secs(2));
    registry
        .record_access(first.clone())
        .await
        .map_err(unchecked)?;
    durability
        .access_recorded(&first, &page.locator, None)
        .await
        .map_err(|error| Failure::Unexpected(error.to_string()))?;
    let call = ToolCalled {
        agent: b,
        call: ToolCallId("toolu_pg".to_owned()),
        name: ToolName("Bash".to_owned()),
        at: secs(3),
    };
    durability
        .tool_called(&call)
        .await
        .map_err(|error| Failure::Unexpected(error.to_string()))?;
    let second = access(&mut scene, b, page.clone(), secs(4));
    registry
        .record_access(second.clone())
        .await
        .map_err(unchecked)?;
    let channel = ChannelId::from_ulid(77);
    durability
        .access_recorded(&second, &page.locator, Some(channel))
        .await
        .map_err(|error| Failure::Unexpected(error.to_string()))?;
    // A third, recorded, its resolution never stored.
    let third = access(&mut scene, a, page.clone(), secs(5));
    registry
        .record_access(third.clone())
        .await
        .map_err(unchecked)?;
    let held = write(&mut scene, a, &page.locator, secs(6), Vec::new());
    durability
        .hold(&held, secs(66))
        .await
        .map_err(|error| Failure::Unexpected(error.to_string()))?;
    let recovered = durability
        .load()
        .await
        .map_err(|error| Failure::Unexpected(error.to_string()))?;
    same("no checkpoint", &recovered.checkpoint, &None)?;
    same("held", &recovered.held, &vec![(held.clone(), secs(66))])?;
    same(
        "inputs",
        &recovered.inputs,
        &vec![
            Recorded::Access {
                access: first.clone(),
                locator: page.locator.clone(),
                resolved: Resolved::NoChannel,
            },
            Recorded::ToolCall(call.clone()),
            Recorded::Access {
                access: second.clone(),
                locator: page.locator.clone(),
                resolved: Resolved::Channel(channel),
            },
            Recorded::Access {
                access: third.clone(),
                locator: page.locator.clone(),
                resolved: Resolved::Unknown,
            },
        ],
    )?;
    // A checkpoint covers all of it; the tool call row goes with it.
    let config = settings(2);
    let shards = Shards::new(config.timing, config.content_retention, config.shards);
    let Ok(checkpoint) = shards.checkpoint() else {
        return Err(Failure::Unexpected("checkpoint".to_owned()));
    };
    durability
        .save(&checkpoint, secs(7))
        .await
        .map_err(|error| Failure::Unexpected(error.to_string()))?;
    let (tool_rows,): (i64,) = sqlx::query_as("SELECT count(*) FROM flow.tool_calls")
        .fetch_one(&pool)
        .await?;
    same("tool calls kept", &tool_rows, &0)?;
    let fourth = access(&mut scene, b, page.clone(), secs(8));
    registry
        .record_access(fourth.clone())
        .await
        .map_err(unchecked)?;
    durability
        .release(held.id)
        .await
        .map_err(|error| Failure::Unexpected(error.to_string()))?;
    let recovered = durability
        .load()
        .await
        .map_err(|error| Failure::Unexpected(error.to_string()))?;
    same("held after release", &recovered.held, &Vec::new())?;
    same(
        "inputs after the checkpoint",
        &recovered.inputs,
        &vec![Recorded::Access {
            access: fourth,
            locator: page.locator.clone(),
            resolved: Resolved::Unknown,
        }],
    )?;
    ensure(
        recovered
            .checkpoint
            .is_some_and(|stored| stored.count == 2 && stored.shards.len() == 2),
        || "the checkpoint has two shards".to_owned(),
    )
}

/// A consumer over the Postgres stores holds a write and records a tool
/// call; a new process restored from the same database still holds the
/// write and settles it, recording its access once and dropping its row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restored_consumer_settles_a_write_held_before_its_restart() -> TestResult {
    let Some(db) = db("a_restored_consumer_settles_a_write_held_before_its_restart").await? else {
        return Ok(());
    };
    let pool = db.pool().clone();
    reset(&pool).await?;
    let mut scene = Scene::new(91);
    let (a, b) = (scene.agent(), scene.agent());
    let span = scene.span();
    let edit = write(&mut scene, a, &wiki_page("Restart"), secs(5), vec![span]);
    let id = edit.id;
    let start = || async {
        let (registry, _events) = registry(&pool).await?;
        let agents = crosstalk_memory::reconstruct::MemoryAgents::new(
            IdSequence::new(1 << 90),
            Outbox::none(),
        );
        let mut consumer = FlowConsumer::with_durability(
            settings(2),
            FlowDeps {
                registry,
                transmissions: MemoryVerdicts::new(Outbox::none()),
                agents,
                bus: RecordingBus::default(),
                clock: Arc::new(ManualClock::at(T0)),
            },
            PgFlowDurability::new(pool.clone(), SerializableRetry::default()),
        );
        consumer
            .restore()
            .await
            .map_err(|error| Failure::Unexpected(error.to_string()))?;
        Ok::<_, Failure>(consumer)
    };
    let mut first = start().await?;
    same(
        "batch",
        &first
            .handle_batch(vec![
                Extracted::Write {
                    write: edit,
                    outcome: None,
                },
                Extracted::ToolCall {
                    agent: b,
                    call: ToolCallId("toolu_restart".to_owned()),
                    name: ToolName("Bash".to_owned()),
                    at: secs(5),
                },
            ])
            .await,
        &Ok(()),
    )?;
    drop(first);
    let mut second = start().await?;
    ensure(second.held_writes().contains(id), || {
        "the held write was not restored".to_owned()
    })?;
    second.tick(secs(3_600)).await;
    same("backlog", &second.backlog(), &0)?;
    let (held,): (i64,) = sqlx::query_as("SELECT count(*) FROM flow.held_writes")
        .fetch_one(&pool)
        .await?;
    same("held rows", &held, &0)?;
    let (recorded, resolved): (i64, i64) = sqlx::query_as(
        "SELECT count(*), count(*) FILTER (WHERE resolved) FROM flow.accesses WHERE id = $1",
    )
    .bind(id.ulid_text())
    .fetch_one(&pool)
    .await?;
    same(
        "access recorded once, resolved",
        &(recorded, resolved),
        &(1, 1),
    )
}
