//! The flow consumer's durability on Postgres
//! ([`crate::consumer::FlowDurability`], migration `0003_restart`).
//!
//! - `flow.held_writes`: one row per held write, inserted when held and
//!   deleted once its released access is recorded.
//! - `flow.accesses.recorded_seq` and `flow.tool_calls`: numbered from one
//!   sequence (`flow.recording`) as they are recorded, so a restore
//!   re-feeds exactly what its checkpoint has not seen, in order. The
//!   registry numbers an access in the transaction that records it.
//! - `flow.checkpoints`: one row per shard, written with the shard's
//!   `flow.shard_ticks` row in one serializable transaction
//!   (`flow.checkpoint.ticks-with-state`, INV-1216). `recorded_through` is
//!   the highest recording number committed when the checkpoint is
//!   written: the consumer checkpoints only when idle, and it is the only
//!   writer (one pipeline process per database), so that is everything it
//!   took. Tool calls a checkpoint covers are deleted with it.
//!
//! [`PgFlowDurability::reset_correlator`] is what `crosstalk migrate
//! --reset-correlator` runs after an incompatible snapshot (decision Q2).

use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::resource::{Locator, Resource};
use crosstalk_spec::ids::{AccessId, AgentId, ChannelId};
use crosstalk_spec::observed::message::{ToolCallId, ToolName};
use crosstalk_spec::support::Timestamp;
use crosstalk_store::{SerializableRetry, TxError, retry_serializable};
use sqlx::{PgConnection, PgPool};

use super::codec::{CodecError, from_json, id_text, json, micros, parse_id, timestamp};
use super::error::{Fault, FlowStoreError, StoreFault, finished};
use crate::consumer::{
    Checkpoint, DurabilityError, FlowDurability, Observed, Recorded, Recovered, Resolved, Settings,
    ShardSnapshot, Shards, StoredCheckpoint, ToolCalled, WriteCall,
};

impl StoreFault for DurabilityError {
    fn store(error: FlowStoreError) -> Self {
        durability(error)
    }
}

/// A store failure as the port reports it: a value that does not decode
/// or breaks a rule is corrupt; anything else may pass.
fn durability(error: FlowStoreError) -> DurabilityError {
    match error {
        FlowStoreError::Codec(_) | FlowStoreError::Corrupt { .. } => DurabilityError::Corrupt {
            reason: error.to_string(),
        },
        FlowStoreError::Store(_)
        | FlowStoreError::Query { .. }
        | FlowStoreError::Ids(_)
        | FlowStoreError::Sink(_) => DurabilityError::Unavailable {
            reason: error.to_string(),
        },
    }
}

fn query_failed(error: sqlx::Error) -> DurabilityError {
    durability(error.into())
}

fn codec_failed(error: CodecError) -> DurabilityError {
    durability(error.into())
}

/// A count or index as an `INTEGER` column.
fn integer(what: &'static str, value: u32) -> Result<i32, CodecError> {
    i32::try_from(value).map_err(|_| CodecError::Count {
        what,
        value: i128::from(value),
    })
}

/// A recording number as a `BIGINT` column.
fn seq(what: &'static str, value: u64) -> Result<i64, CodecError> {
    i64::try_from(value).map_err(|_| CodecError::Count {
        what,
        value: i128::from(value),
    })
}

fn unsigned(what: &'static str, value: i64) -> Result<u64, CodecError> {
    u64::try_from(value).map_err(|_| CodecError::Count {
        what,
        value: i128::from(value),
    })
}

fn index(what: &'static str, value: i32) -> Result<u32, CodecError> {
    u32::try_from(value).map_err(|_| CodecError::Count {
        what,
        value: i128::from(value),
    })
}

/// The flow consumer's durability over the flow schema.
#[derive(Debug, Clone)]
pub struct PgFlowDurability {
    pool: PgPool,
    retry: SerializableRetry,
}

/// A `flow.checkpoints` row.
type CheckpointRow = (i32, i32, i32, Option<i64>, i64, Vec<u8>);

/// An access recorded after a checkpoint, with its resource and
/// resolution.
type AccessRow = (i64, String, String, bool, Option<String>);

/// A `flow.tool_calls` row.
type ToolCallRow = (i64, String, String, String, i64);

impl PgFlowDurability {
    pub fn new(pool: PgPool, retry: SerializableRetry) -> Self {
        Self { pool, retry }
    }

    /// Replace the stored checkpoint with empty shards under this binary's
    /// format, covering everything recorded so far, each shard at the
    /// latest tick stored for any shard: what `crosstalk migrate
    /// --reset-correlator` runs after an incompatible checkpoint. The
    /// correlator restarts empty and the pairings pending at the reset are
    /// lost, knowingly (decision Q2); the held writes stay.
    pub async fn reset_correlator(
        &self,
        settings: &Settings,
        now: Timestamp,
    ) -> Result<(), DurabilityError> {
        let latest: (Option<i64>,) = sqlx::query_as("SELECT max(ticked_through) FROM flow.shard_ticks")
            .fetch_one(&self.pool)
            .await
            .map_err(query_failed)?;
        let mut shards = Shards::new(settings.timing, settings.content_retention, settings.shards);
        if let Some(latest) = latest.0 {
            let at = timestamp("shard_ticks.ticked_through", latest).map_err(codec_failed)?;
            // Empty shards decide nothing on a tick; it only sets the tick
            // they ran through, so the record never runs ahead of them.
            let _ = shards.tick(at);
        }
        let checkpoint = shards.checkpoint().map_err(|error| DurabilityError::Corrupt {
            reason: error.to_string(),
        })?;
        self.save(&checkpoint, now).await
    }
}

/// The highest recording number committed, never below the stored
/// checkpoint's (deleted tool calls lower the maximum otherwise).
async fn recorded_through(conn: &mut PgConnection) -> Result<i64, sqlx::Error> {
    let (through,): (i64,) = sqlx::query_as(
        "SELECT GREATEST( \
             COALESCE((SELECT max(recorded_seq) FROM flow.accesses), 0), \
             COALESCE((SELECT max(recorded_seq) FROM flow.tool_calls), 0), \
             COALESCE((SELECT max(recorded_through) FROM flow.checkpoints), 0))",
    )
    .fetch_one(&mut *conn)
    .await?;
    Ok(through)
}

async fn save_in(
    conn: &mut PgConnection,
    checkpoint: &Checkpoint,
    taken_at: i64,
) -> Result<(), TxError<DurabilityError>> {
    let count = u32::try_from(checkpoint.shards.len())
        .map_err(|_| Fault::corrupt("checkpoint shards", "more than a u32 counts"))?;
    let count = integer("checkpoints.shards", count).map_err(Fault::from)?;
    let format = integer("checkpoints.format", checkpoint.format).map_err(Fault::from)?;
    let through = recorded_through(conn).await?;
    for shard in &checkpoint.shards {
        let index = integer("checkpoints.shard", shard.shard).map_err(Fault::from)?;
        let ticked = shard
            .ticked_through
            .map(|at| micros("checkpoints.ticked_through", at))
            .transpose()
            .map_err(Fault::from)?;
        sqlx::query(
            "INSERT INTO flow.checkpoints \
                 (shard, format, shards, ticked_through, recorded_through, taken_at, state) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (shard) DO UPDATE SET format = EXCLUDED.format, \
                 shards = EXCLUDED.shards, ticked_through = EXCLUDED.ticked_through, \
                 recorded_through = EXCLUDED.recorded_through, \
                 taken_at = EXCLUDED.taken_at, state = EXCLUDED.state",
        )
        .bind(index)
        .bind(format)
        .bind(count)
        .bind(ticked)
        .bind(through)
        .bind(taken_at)
        .bind(&shard.state)
        .execute(&mut *conn)
        .await?;
        if let Some(ticked) = ticked {
            sqlx::query(
                "INSERT INTO flow.shard_ticks (shard, ticked_through) VALUES ($1, $2) \
                 ON CONFLICT (shard) DO UPDATE \
                 SET ticked_through = GREATEST(flow.shard_ticks.ticked_through, EXCLUDED.ticked_through)",
            )
            .bind(index)
            .bind(ticked)
            .execute(&mut *conn)
            .await?;
        }
    }
    sqlx::query("DELETE FROM flow.checkpoints WHERE shard >= $1")
        .bind(count)
        .execute(&mut *conn)
        .await?;
    sqlx::query("DELETE FROM flow.tool_calls WHERE recorded_seq <= $1")
        .bind(through)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

fn stored_checkpoint(rows: Vec<CheckpointRow>) -> Result<Option<StoredCheckpoint>, CodecError> {
    let Some(&(_, format, count, _, through, _)) = rows.first() else {
        return Ok(None);
    };
    let mut shards = Vec::with_capacity(rows.len());
    for (shard, _, _, ticked, _, state) in rows {
        shards.push(ShardSnapshot {
            shard: index("checkpoints.shard", shard)?,
            ticked_through: ticked
                .map(|at| timestamp("checkpoints.ticked_through", at))
                .transpose()?,
            state,
        });
    }
    Ok(Some(StoredCheckpoint {
        format: index("checkpoints.format", format)?,
        count: index("checkpoints.shards", count)?,
        recorded_through: unsigned("checkpoints.recorded_through", through)?,
        shards,
    }))
}

fn recorded_access(text: &str, resource: &str) -> Result<(Access, Locator), CodecError> {
    let access: Access = from_json("accesses.access", text)?;
    let resource: Resource = from_json("resources.resource", resource)?;
    Ok((access, resource.locator))
}

fn tool_call(row: &ToolCallRow) -> Result<ToolCalled, CodecError> {
    let (_, agent, call, name, at) = row;
    Ok(ToolCalled {
        agent: parse_id::<AgentId>("tool_calls.agent", agent)?,
        call: ToolCallId(call.clone()),
        name: ToolName(name.clone()),
        at: timestamp("tool_calls.at", *at)?,
    })
}

impl FlowDurability for PgFlowDurability {
    fn survives_restart(&self) -> bool {
        true
    }

    async fn hold(
        &self,
        write: &Observed<WriteCall>,
        settles_at: Timestamp,
    ) -> Result<(), DurabilityError> {
        let settles = micros("held_writes.settles_at", settles_at).map_err(codec_failed)?;
        let text = json("held write", write).map_err(codec_failed)?;
        sqlx::query(
            "INSERT INTO flow.held_writes (access_id, settles_at, write) VALUES ($1, $2, $3) \
             ON CONFLICT (access_id) DO NOTHING",
        )
        .bind(id_text(write.id))
        .bind(settles)
        .bind(text)
        .execute(&self.pool)
        .await
        .map_err(query_failed)?;
        Ok(())
    }

    async fn release(&self, access: AccessId) -> Result<(), DurabilityError> {
        sqlx::query("DELETE FROM flow.held_writes WHERE access_id = $1")
            .bind(id_text(access))
            .execute(&self.pool)
            .await
            .map_err(query_failed)?;
        Ok(())
    }

    /// The registry numbered the access as it recorded it; this stores
    /// the channel it was resolved to, once.
    async fn access_recorded(
        &self,
        access: &Access,
        _locator: &Locator,
        channel: Option<ChannelId>,
    ) -> Result<(), DurabilityError> {
        sqlx::query(
            "UPDATE flow.accesses SET resolved = true, resolved_channel = $2 \
             WHERE id = $1 AND NOT resolved",
        )
        .bind(id_text(access.id))
        .bind(channel.map(id_text))
        .execute(&self.pool)
        .await
        .map_err(query_failed)?;
        Ok(())
    }

    async fn tool_called(&self, call: &ToolCalled) -> Result<(), DurabilityError> {
        let at = micros("tool_calls.at", call.at).map_err(codec_failed)?;
        sqlx::query("INSERT INTO flow.tool_calls (agent, call_id, name, at) VALUES ($1, $2, $3, $4)")
            .bind(id_text(call.agent))
            .bind(&call.call.0)
            .bind(&call.name.0)
            .bind(at)
            .execute(&self.pool)
            .await
            .map_err(query_failed)?;
        Ok(())
    }

    async fn save(&self, checkpoint: &Checkpoint, taken_at: Timestamp) -> Result<(), DurabilityError> {
        let taken_at = micros("checkpoints.taken_at", taken_at).map_err(codec_failed)?;
        retry_serializable(&self.pool, &self.retry, |conn| {
            // The body may run again; each attempt owns its copy.
            let checkpoint = checkpoint.clone();
            Box::pin(async move { save_in(conn, &checkpoint, taken_at).await })
        })
        .await
        .map_err(finished)
    }

    async fn load(&self) -> Result<Recovered, DurabilityError> {
        let mut tx = self
            .pool
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .map_err(query_failed)?;
        let rows: Vec<CheckpointRow> = sqlx::query_as(
            "SELECT shard, format, shards, ticked_through, recorded_through, state \
             FROM flow.checkpoints ORDER BY shard",
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(query_failed)?;
        let checkpoint = stored_checkpoint(rows).map_err(codec_failed)?;
        let through = checkpoint
            .as_ref()
            .map_or(Ok(0), |stored| seq("checkpoints.recorded_through", stored.recorded_through))
            .map_err(codec_failed)?;
        let held_rows: Vec<(i64, String)> = sqlx::query_as(
            "SELECT settles_at, write FROM flow.held_writes ORDER BY settles_at, access_id",
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(query_failed)?;
        let mut held = Vec::with_capacity(held_rows.len());
        for (settles_at, text) in held_rows {
            let write: Observed<WriteCall> = from_json("held_writes.write", &text).map_err(codec_failed)?;
            let settles_at = timestamp("held_writes.settles_at", settles_at).map_err(codec_failed)?;
            held.push((write, settles_at));
        }
        let accesses: Vec<AccessRow> = sqlx::query_as(
            "SELECT a.recorded_seq, a.access, r.resource, a.resolved, a.resolved_channel \
             FROM flow.accesses a JOIN flow.resources r ON r.id = a.resource_id \
             WHERE a.recorded_seq > $1 ORDER BY a.recorded_seq",
        )
        .bind(through)
        .fetch_all(&mut *tx)
        .await
        .map_err(query_failed)?;
        let calls: Vec<ToolCallRow> = sqlx::query_as(
            "SELECT recorded_seq, agent, call_id, name, at FROM flow.tool_calls \
             WHERE recorded_seq > $1 ORDER BY recorded_seq",
        )
        .bind(through)
        .fetch_all(&mut *tx)
        .await
        .map_err(query_failed)?;
        tx.commit().await.map_err(query_failed)?;
        // Merge the two in recording order.
        let mut numbered: Vec<(i64, Recorded)> = Vec::with_capacity(accesses.len() + calls.len());
        for (number, access, resource, resolved, channel) in &accesses {
            let (access, locator) = recorded_access(access, resource).map_err(codec_failed)?;
            let resolved = match (resolved, channel) {
                (false, _) => Resolved::Unknown,
                (true, None) => Resolved::NoChannel,
                (true, Some(channel)) => Resolved::Channel(
                    parse_id("accesses.resolved_channel", channel).map_err(codec_failed)?,
                ),
            };
            numbered.push((
                *number,
                Recorded::Access {
                    access,
                    locator,
                    resolved,
                },
            ));
        }
        for row in &calls {
            numbered.push((row.0, Recorded::ToolCall(tool_call(row).map_err(codec_failed)?)));
        }
        numbered.sort_by_key(|(number, _)| *number);
        Ok(Recovered {
            checkpoint,
            held,
            inputs: numbered.into_iter().map(|(_, input)| input).collect(),
        })
    }
}

