//! The extraction step's ledger on Postgres (migration
//! `0004_extract_ledger`): [`PgExtractionLedger`] implements the step's
//! [`ExtractionLedger`] port.
//!
//! Each read is one statement. A delta's [`LedgerCommit`] is one
//! `SERIALIZABLE` transaction under `retry_serializable`: its contexts,
//! pending lists (each rewritten whole, in list order, so `made_seq` keeps
//! the order), history calls, deliveries and the `extract_done` row. Time
//! is the commit's `at` (the exchange's start), never `now()`.

use std::sync::Arc;

use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::ids::{AccessId, AgentId, ConversationId, ExchangeId};
use crosstalk_spec::observed::message::ToolCall;
use crosstalk_spec::support::Timestamp;
use crosstalk_store::{SerializableError, SerializableRetry, TxError, retry_serializable};
use sqlx::{PgConnection, PgPool};

use super::codec::{CodecError, from_json, id_text, json, micros, parse_id, timestamp};
use super::error::FlowStoreError;
use crate::extract::ConversationContext;
use crate::extract::step::ledger::stored_call;
use crate::extract::step::{
    DeliveryKey, ExtractionLedger, LedgerCommit, LedgerError, LedgerState, PendingCall,
};

/// The extraction ledger in schema `flow`. Clones share the pool.
#[derive(Debug, Clone)]
pub struct PgExtractionLedger {
    pool: PgPool,
    retry: SerializableRetry,
}

/// A pending call's row: conversation, context, call and writes.
type PendingRow = (String, String, String, String);

/// One delta's commit, encoded once for every attempt of its transaction.
#[derive(Debug)]
struct Encoded {
    exchange: String,
    at: i64,
    contexts: Vec<(String, String, String)>,
    pending: Vec<(String, String, Vec<PendingRow>)>,
    history: Vec<(String, String, Option<String>)>,
    delivered: Vec<(String, Vec<u8>)>,
}

impl PgExtractionLedger {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            retry: SerializableRetry::default(),
        }
    }

    /// Everything the ledger holds, read in one snapshot; what the model
    /// tests compare with the memory ledger.
    pub async fn state(&self) -> Result<LedgerState, FlowStoreError> {
        let mut tx = self
            .pool
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await?;
        let mut state = LedgerState::default();
        let contexts: Vec<(String, String, String, i64)> = sqlx::query_as(
            "SELECT agent, conversation, context, updated_at FROM flow.extract_contexts",
        )
        .fetch_all(&mut *tx)
        .await?;
        for (agent, conversation, context, at) in contexts {
            state.contexts.insert(
                (
                    parse_id("extract_contexts.agent", &agent)?,
                    parse_id("extract_contexts.conversation", &conversation)?,
                ),
                (
                    from_json("extract_contexts.context", &context)?,
                    timestamp("extract_contexts.updated_at", at)?,
                ),
            );
        }
        let pending: Vec<(String, String, String, String, String, String)> = sqlx::query_as(
            "SELECT agent, call_id, conversation, context, call, writes FROM flow.extract_pending \
             ORDER BY agent, call_id, made_seq",
        )
        .fetch_all(&mut *tx)
        .await?;
        for (agent, call_id, conversation, context, call, writes) in pending {
            let key = (parse_id("extract_pending.agent", &agent)?, call_id);
            state.pending.entry(key).or_default().push(pending_call((
                conversation,
                context,
                call,
                writes,
            ))?);
        }
        let history: Vec<(String, String, String)> =
            sqlx::query_as("SELECT conversation, call_id, call FROM flow.extract_history")
                .fetch_all(&mut *tx)
                .await?;
        for (conversation, call_id, call) in history {
            state.history.insert(
                (
                    parse_id("extract_history.conversation", &conversation)?,
                    call_id,
                ),
                tool_call("extract_history.call", &call)?,
            );
        }
        let delivered: Vec<(String, Vec<u8>, i64)> =
            sqlx::query_as("SELECT agent, key, at FROM flow.extract_delivered")
                .fetch_all(&mut *tx)
                .await?;
        for (agent, key, at) in delivered {
            state.delivered.insert(
                delivery_key(&agent, &key)?,
                timestamp("extract_delivered.at", at)?,
            );
        }
        let done: Vec<(String, i64)> = sqlx::query_as("SELECT exchange, at FROM flow.extract_done")
            .fetch_all(&mut *tx)
            .await?;
        for (exchange, at) in done {
            state.done.insert(
                parse_id::<ExchangeId>("extract_done.exchange", &exchange)?,
                timestamp("extract_done.at", at)?,
            );
        }
        tx.commit().await?;
        Ok(state)
    }

    async fn commit_encoded(&self, encoded: Arc<Encoded>) -> Result<(), FlowStoreError> {
        retry_serializable(&self.pool, &self.retry, move |conn| {
            let encoded = Arc::clone(&encoded);
            Box::pin(async move { write(conn, &encoded).await.map_err(TxError::Db) })
        })
        .await
        .map_err(|error: SerializableError<FlowStoreError>| match error {
            SerializableError::Aborted(error) => error,
            SerializableError::Store(error) => error.into(),
        })
    }
}

/// One delta's rows, in the caller's transaction.
async fn write(conn: &mut PgConnection, encoded: &Encoded) -> Result<(), sqlx::Error> {
    for (agent, conversation, context) in &encoded.contexts {
        sqlx::query(
            "INSERT INTO flow.extract_contexts (agent, conversation, context, updated_at) \
             VALUES ($1, $2, $3, $4) ON CONFLICT (agent, conversation) \
             DO UPDATE SET context = EXCLUDED.context, updated_at = EXCLUDED.updated_at",
        )
        .bind(agent)
        .bind(conversation)
        .bind(context)
        .bind(encoded.at)
        .execute(&mut *conn)
        .await?;
    }
    for (agent, call_id, calls) in &encoded.pending {
        sqlx::query("DELETE FROM flow.extract_pending WHERE agent = $1 AND call_id = $2")
            .bind(agent)
            .bind(call_id)
            .execute(&mut *conn)
            .await?;
        // One insert per call, in list order: each draws the next made_seq.
        for (conversation, context, call, writes) in calls {
            sqlx::query(
                "INSERT INTO flow.extract_pending \
                 (agent, call_id, conversation, context, call, writes) \
                 VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(agent)
            .bind(call_id)
            .bind(conversation)
            .bind(context)
            .bind(call)
            .bind(writes)
            .execute(&mut *conn)
            .await?;
        }
    }
    for (conversation, call_id, call) in &encoded.history {
        match call {
            Some(call) => {
                sqlx::query(
                    "INSERT INTO flow.extract_history (conversation, call_id, call) \
                     VALUES ($1, $2, $3) ON CONFLICT (conversation, call_id) \
                     DO UPDATE SET call = EXCLUDED.call",
                )
                .bind(conversation)
                .bind(call_id)
                .bind(call)
                .execute(&mut *conn)
                .await?;
            }
            None => {
                sqlx::query(
                    "DELETE FROM flow.extract_history WHERE conversation = $1 AND call_id = $2",
                )
                .bind(conversation)
                .bind(call_id)
                .execute(&mut *conn)
                .await?;
            }
        }
    }
    for (agent, key) in &encoded.delivered {
        sqlx::query(
            "INSERT INTO flow.extract_delivered (agent, key, at) VALUES ($1, $2, $3) \
             ON CONFLICT (agent, key) \
             DO UPDATE SET at = GREATEST(flow.extract_delivered.at, EXCLUDED.at)",
        )
        .bind(agent)
        .bind(key)
        .bind(encoded.at)
        .execute(&mut *conn)
        .await?;
    }
    sqlx::query(
        "INSERT INTO flow.extract_done (exchange, at) VALUES ($1, $2) ON CONFLICT (exchange) \
         DO UPDATE SET at = GREATEST(flow.extract_done.at, EXCLUDED.at)",
    )
    .bind(&encoded.exchange)
    .bind(encoded.at)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

fn encode(commit: &LedgerCommit) -> Result<Encoded, CodecError> {
    let changes = &commit.changes;
    let mut contexts = Vec::with_capacity(changes.contexts.len());
    for ((agent, conversation), context) in &changes.contexts {
        contexts.push((
            id_text(*agent),
            id_text(*conversation),
            json("extract_contexts.context", context)?,
        ));
    }
    let mut pending = Vec::with_capacity(changes.pending.len());
    for ((agent, call_id), calls) in &changes.pending {
        let mut rows = Vec::with_capacity(calls.len());
        for call in calls {
            rows.push((
                id_text(call.conversation),
                json("extract_pending.context", &call.context)?,
                stored_call::encode(&call.call),
                json("extract_pending.writes", &call.writes)?,
            ));
        }
        pending.push((id_text(*agent), call_id.clone(), rows));
    }
    let history = changes
        .history
        .iter()
        .map(|((conversation, call_id), call)| {
            (
                id_text(*conversation),
                call_id.clone(),
                call.as_ref().map(stored_call::encode),
            )
        })
        .collect();
    let delivered = changes
        .delivered
        .iter()
        .map(|key| (id_text(key.agent), key.digest.to_vec()))
        .collect();
    Ok(Encoded {
        exchange: id_text(commit.exchange),
        at: micros("extract_done.at", commit.at)?,
        contexts,
        pending,
        history,
        delivered,
    })
}

fn tool_call(what: &'static str, text: &str) -> Result<ToolCall, FlowStoreError> {
    stored_call::decode(text).map_err(|reason| FlowStoreError::Corrupt { what, reason })
}

fn pending_call(row: PendingRow) -> Result<PendingCall, FlowStoreError> {
    let (conversation, context, call, writes) = row;
    let writes: Vec<(Locator, AccessId)> = from_json("extract_pending.writes", &writes)?;
    Ok(PendingCall {
        conversation: parse_id("extract_pending.conversation", &conversation)?,
        context: from_json::<ConversationContext>("extract_pending.context", &context)?,
        call: tool_call("extract_pending.call", &call)?,
        writes,
    })
}

fn delivery_key(agent: &str, key: &[u8]) -> Result<DeliveryKey, FlowStoreError> {
    let digest: [u8; 32] = key.try_into().map_err(|_| FlowStoreError::Corrupt {
        what: "extract_delivered.key",
        reason: format!("{} bytes, not 32", key.len()),
    })?;
    Ok(DeliveryKey {
        agent: parse_id("extract_delivered.agent", agent)?,
        digest,
    })
}

impl ExtractionLedger for PgExtractionLedger {
    async fn done(&self, exchange: ExchangeId) -> Result<bool, LedgerError> {
        let found: Option<(i64,)> =
            sqlx::query_as("SELECT at FROM flow.extract_done WHERE exchange = $1")
                .bind(id_text(exchange))
                .fetch_optional(&self.pool)
                .await
                .map_err(FlowStoreError::from)?;
        Ok(found.is_some())
    }

    async fn context(
        &self,
        agent: AgentId,
        conversation: ConversationId,
    ) -> Result<Option<ConversationContext>, LedgerError> {
        let found: Option<(String,)> = sqlx::query_as(
            "SELECT context FROM flow.extract_contexts WHERE agent = $1 AND conversation = $2",
        )
        .bind(id_text(agent))
        .bind(id_text(conversation))
        .fetch_optional(&self.pool)
        .await
        .map_err(FlowStoreError::from)?;
        Ok(found
            .map(|(context,)| from_json("extract_contexts.context", &context))
            .transpose()
            .map_err(FlowStoreError::from)?)
    }

    async fn pending(
        &self,
        agent: AgentId,
        call_id: &str,
    ) -> Result<Vec<PendingCall>, LedgerError> {
        let rows: Vec<PendingRow> = sqlx::query_as(
            "SELECT conversation, context, call, writes FROM flow.extract_pending \
             WHERE agent = $1 AND call_id = $2 ORDER BY made_seq",
        )
        .bind(id_text(agent))
        .bind(call_id)
        .fetch_all(&self.pool)
        .await
        .map_err(FlowStoreError::from)?;
        Ok(rows
            .into_iter()
            .map(pending_call)
            .collect::<Result<_, _>>()?)
    }

    async fn history(
        &self,
        conversation: ConversationId,
        call_id: &str,
    ) -> Result<Option<ToolCall>, LedgerError> {
        let found: Option<(String,)> = sqlx::query_as(
            "SELECT call FROM flow.extract_history WHERE conversation = $1 AND call_id = $2",
        )
        .bind(id_text(conversation))
        .bind(call_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(FlowStoreError::from)?;
        Ok(found
            .map(|(call,)| tool_call("extract_history.call", &call))
            .transpose()?)
    }

    async fn delivered(&self, key: DeliveryKey) -> Result<bool, LedgerError> {
        let found: Option<(i64,)> =
            sqlx::query_as("SELECT at FROM flow.extract_delivered WHERE agent = $1 AND key = $2")
                .bind(id_text(key.agent))
                .bind(key.digest.to_vec())
                .fetch_optional(&self.pool)
                .await
                .map_err(FlowStoreError::from)?;
        Ok(found.is_some())
    }

    async fn commit(&self, commit: LedgerCommit) -> Result<(), LedgerError> {
        let encoded = encode(&commit).map_err(FlowStoreError::from)?;
        self.commit_encoded(Arc::new(encoded)).await?;
        Ok(())
    }

    async fn expire(&self, horizon: Timestamp) -> Result<(), LedgerError> {
        let horizon = micros("extract ledger horizon", horizon).map_err(FlowStoreError::from)?;
        let mut tx = self.pool.begin().await.map_err(FlowStoreError::from)?;
        for statement in [
            "DELETE FROM flow.extract_delivered WHERE at < $1",
            "DELETE FROM flow.extract_contexts WHERE updated_at < $1",
            "DELETE FROM flow.extract_done WHERE at < $1",
        ] {
            sqlx::query(statement)
                .bind(horizon)
                .execute(&mut *tx)
                .await
                .map_err(FlowStoreError::from)?;
        }
        tx.commit().await.map_err(FlowStoreError::from)?;
        Ok(())
    }
}
