//! [`PgConversations`]: the conversation store on Postgres (schema
//! `reconstruct`, migration `0002_conversations`).
//!
//! A threading call is one `SERIALIZABLE` transaction, retried on a
//! serialization failure: the decision (`plan`) reads through
//! `PgReads` and the write lands in the same transaction, so concurrent
//! calls (several consumers, redeliveries racing) leave what some serial
//! order would (`reconstruct.thread.serializable`).
//!
//! Seen messages (`reconstruct.delta.excludes-seen-elsewhere`, migration
//! `0003_seen_messages`) are written in the same transaction; it also
//! forgets the cluster's sightings older than the retention behind the
//! exchange. [`PgConversations::forget_seen`] sweeps every agent's.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crosstalk_spec::ids::{AgentId, ConversationId, ExchangeId, MessageHash};
use crosstalk_spec::interfaces::l3_reconstruction::{ThreadError, ThreadOutcome};
use crosstalk_spec::observed::conversation::{Conversation, ConversationOrigin};
use crosstalk_spec::observed::message::Role;
use crosstalk_spec::support::Timestamp;
use crosstalk_store::{SerializableRetry, retry_serializable};
use sqlx::{PgConnection, PgPool};

use super::config::ThreadConfig;
use super::history::{ChainHash, Entry};
use super::plan::{Extension, Planned, Target, ThreadReads, Write, plan};
use super::store::{
    ConversationStore, ResponseKey, StoredOrigin, StoredOutcome, ThreadInput, TranscriptEntry,
};
use crate::agents::codec::{
    CodecError, count, count_of, digest, digest_bytes, from_json, hash_bytes, id_of, id_text, json,
    message_hash, micros,
};
use crate::error::{StorageFailure, StoreReason, TxFailure, tx};

fn role_text(role: Role) -> &'static str {
    match role {
        Role::System => "system",
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::Tool => "tool",
    }
}

fn role_of(text: &str) -> Result<Role, CodecError> {
    match text {
        "system" => Ok(Role::System),
        "user" => Ok(Role::User),
        "assistant" => Ok(Role::Assistant),
        "tool" => Ok(Role::Tool),
        other => Err(CodecError::Json {
            column: "conversation_entries.role",
            reason: format!("unknown role {other:?}"),
        }),
    }
}

fn chain_of(column: &'static str, bytes: &[u8]) -> Result<ChainHash, CodecError> {
    digest(column, bytes).map(ChainHash)
}

fn origin_of(origin: StoredOrigin) -> ConversationOrigin {
    match origin {
        StoredOrigin::Root => ConversationOrigin::Root,
        StoredOrigin::Fork {
            parent,
            shared_prefix,
        } => ConversationOrigin::Fork {
            parent,
            shared_prefix,
        },
        StoredOrigin::Compaction { predecessor } => ConversationOrigin::Compaction { predecessor },
    }
}

fn stored_origin(origin: ConversationOrigin) -> StoredOrigin {
    match origin {
        ConversationOrigin::Root => StoredOrigin::Root,
        ConversationOrigin::Fork {
            parent,
            shared_prefix,
        } => StoredOrigin::Fork {
            parent,
            shared_prefix,
        },
        ConversationOrigin::Compaction { predecessor } => StoredOrigin::Compaction { predecessor },
    }
}

/// A conversation's id, history length, head, last system message and
/// update sequence.
type HeadRow = (String, i32, Vec<u8>, Option<Vec<u8>>, i64);

/// A transcript row: ordinal, message, role, exchange, history index,
/// output.
type EntryRow = (i32, Vec<u8>, String, String, Option<i32>, bool);

fn ids(members: &[AgentId]) -> Vec<String> {
    members.iter().map(|id| id_text(*id)).collect()
}

fn hashes(messages: &[MessageHash]) -> Vec<Vec<u8>> {
    messages.iter().map(hash_bytes).collect()
}

/// The decision's reads, inside the call's transaction; sightings before
/// `cutoff` do not count.
pub(crate) struct PgReads<'c> {
    pub(crate) conn: &'c mut PgConnection,
    pub(crate) cutoff: Timestamp,
}

impl ThreadReads for PgReads<'_> {
    async fn recorded(&mut self, exchange: ExchangeId) -> Result<Option<ThreadOutcome>, TxFailure> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT outcome FROM reconstruct.thread_records WHERE exchange = $1")
                .bind(id_text(exchange))
                .fetch_optional(&mut *self.conn)
                .await?;
        Ok(row
            .map(|(outcome,)| from_json::<StoredOutcome>("thread_records.outcome", &outcome))
            .transpose()?
            .map(ThreadOutcome::from))
    }

    async fn extension(
        &mut self,
        members: &[AgentId],
        chains: &[ChainHash],
    ) -> Result<Option<Extension>, TxFailure> {
        if chains.is_empty() {
            return Ok(None);
        }
        let position: HashMap<ChainHash, usize> = chains
            .iter()
            .enumerate()
            .map(|(index, chain)| (*chain, index))
            .collect();
        let rows: Vec<HeadRow> = sqlx::query_as(
            "SELECT id, history_len, head, last_system, updated FROM reconstruct.conversations \
             WHERE head = ANY($1) AND agent = ANY($2)",
        )
        .bind(
            chains
                .iter()
                .map(|chain| digest_bytes(&chain.0))
                .collect::<Vec<_>>(),
        )
        .bind(ids(members))
        .fetch_all(&mut *self.conn)
        .await?;
        let mut best: Option<(u32, i64, Extension)> = None;
        for (id, len, head, last_system, updated) in rows {
            let len = count_of("conversations.history_len", len)?;
            let head = chain_of("conversations.head", &head)?;
            if position.get(&head).map(|index| index + 1) != Some(len as usize) {
                continue;
            }
            let candidate = Extension {
                conversation: id_of("conversations.id", &id)?,
                len,
                last_system: last_system
                    .map(|bytes| message_hash("conversations.last_system", &bytes))
                    .transpose()?,
            };
            if best.as_ref().is_none_or(|(best_len, best_updated, _)| {
                (len, updated) > (*best_len, *best_updated)
            }) {
                best = Some((len, updated, candidate));
            }
        }
        Ok(best.map(|(_, _, extension)| extension))
    }

    async fn common_prefix(
        &mut self,
        members: &[AgentId],
        chains: &[ChainHash],
        from: usize,
    ) -> Result<Option<(ConversationId, u32)>, TxFailure> {
        let wanted: Vec<Vec<u8>> = chains
            .get(from..)
            .unwrap_or(&[])
            .iter()
            .map(|chain| digest_bytes(&chain.0))
            .collect();
        if wanted.is_empty() {
            return Ok(None);
        }
        let row: Option<(String, i32)> = sqlx::query_as(
            "SELECT c.id, e.history_index FROM reconstruct.conversation_entries e \
             JOIN reconstruct.conversations c ON c.id = e.conversation \
             WHERE e.chain = ANY($1) AND c.agent = ANY($2) \
             ORDER BY e.history_index DESC, c.updated DESC LIMIT 1",
        )
        .bind(wanted)
        .bind(ids(members))
        .fetch_optional(&mut *self.conn)
        .await?;
        Ok(row
            .map(|(id, index)| {
                Ok::<_, CodecError>((
                    id_of("conversations.id", &id)?,
                    count_of("conversation_entries.history_index", index)? + 1,
                ))
            })
            .transpose()?)
    }

    async fn response(
        &mut self,
        key: &ResponseKey,
        members: &[AgentId],
    ) -> Result<Option<(ConversationId, u32)>, TxFailure> {
        let row: Option<(String, i32)> = sqlx::query_as(
            "SELECT r.conversation, r.history_len FROM reconstruct.responses r \
             JOIN reconstruct.conversations c ON c.id = r.conversation \
             WHERE r.upstream = $1 AND r.scope = $2 AND r.response = $3 AND c.agent = ANY($4)",
        )
        .bind(&key.upstream.0)
        .bind(json(&key.scope)?)
        .bind(&key.response.0)
        .bind(ids(members))
        .fetch_optional(&mut *self.conn)
        .await?;
        Ok(row
            .map(|(id, len)| {
                Ok::<_, CodecError>((
                    id_of("responses.conversation", &id)?,
                    count_of("responses.history_len", len)?,
                ))
            })
            .transpose()?)
    }

    async fn history(
        &mut self,
        conversation: ConversationId,
        len: u32,
    ) -> Result<Vec<Entry>, TxFailure> {
        let rows: Vec<(Vec<u8>, String)> = sqlx::query_as(
            "SELECT message, role FROM reconstruct.conversation_entries \
             WHERE conversation = $1 AND history_index < $2 ORDER BY history_index",
        )
        .bind(id_text(conversation))
        .bind(i64::from(len))
        .fetch_all(&mut *self.conn)
        .await?;
        Ok(rows
            .iter()
            .map(|(message, role)| {
                Ok(Entry {
                    message: message_hash("conversation_entries.message", message)?,
                    role: role_of(role)?,
                })
            })
            .collect::<Result<_, CodecError>>()?)
    }

    async fn output_holder(
        &mut self,
        members: &[AgentId],
        message: MessageHash,
    ) -> Result<Option<ConversationId>, TxFailure> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT c.id FROM reconstruct.conversation_entries e \
             JOIN reconstruct.conversations c ON c.id = e.conversation \
             WHERE e.message = $1 AND e.output AND c.agent = ANY($2) \
             ORDER BY c.updated DESC LIMIT 1",
        )
        .bind(hash_bytes(&message))
        .bind(ids(members))
        .fetch_optional(&mut *self.conn)
        .await?;
        Ok(row
            .map(|(id,)| id_of("conversations.id", &id))
            .transpose()?)
    }

    async fn holder(
        &mut self,
        members: &[AgentId],
        messages: &[MessageHash],
    ) -> Result<Option<ConversationId>, TxFailure> {
        if messages.is_empty() {
            return Ok(None);
        }
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT c.id FROM reconstruct.conversation_entries e \
             JOIN reconstruct.conversations c ON c.id = e.conversation \
             WHERE e.message = ANY($1) AND e.history_index IS NOT NULL AND c.agent = ANY($2) \
             ORDER BY c.updated DESC LIMIT 1",
        )
        .bind(hashes(messages))
        .bind(ids(members))
        .fetch_optional(&mut *self.conn)
        .await?;
        Ok(row
            .map(|(id,)| id_of("conversations.id", &id))
            .transpose()?)
    }

    async fn latest(&mut self, members: &[AgentId]) -> Result<Option<ConversationId>, TxFailure> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT id FROM reconstruct.conversations WHERE agent = ANY($1) \
             ORDER BY updated DESC LIMIT 1",
        )
        .bind(ids(members))
        .fetch_optional(&mut *self.conn)
        .await?;
        Ok(row
            .map(|(id,)| id_of("conversations.id", &id))
            .transpose()?)
    }

    async fn seen_elsewhere(
        &mut self,
        members: &[AgentId],
        messages: &[MessageHash],
        conversation: ConversationId,
    ) -> Result<HashSet<MessageHash>, TxFailure> {
        let rows: Vec<(Vec<u8>,)> = sqlx::query_as(
            "SELECT DISTINCT message FROM reconstruct.seen_messages \
             WHERE agent = ANY($1) AND message = ANY($2) AND conversation <> $3 \
             AND seen_at >= $4",
        )
        .bind(ids(members))
        .bind(hashes(messages))
        .bind(id_text(conversation))
        .bind(micros(self.cutoff)?)
        .fetch_all(&mut *self.conn)
        .await?;
        Ok(rows
            .iter()
            .map(|(message,)| message_hash("seen_messages.message", message))
            .collect::<Result<_, CodecError>>()?)
    }

    async fn held(
        &mut self,
        conversation: ConversationId,
        messages: &[MessageHash],
    ) -> Result<HashSet<MessageHash>, TxFailure> {
        let rows: Vec<(Vec<u8>,)> = sqlx::query_as(
            "SELECT DISTINCT message FROM reconstruct.conversation_entries \
             WHERE conversation = $1 AND history_index IS NOT NULL AND message = ANY($2)",
        )
        .bind(id_text(conversation))
        .bind(hashes(messages))
        .fetch_all(&mut *self.conn)
        .await?;
        Ok(rows
            .iter()
            .map(|(message,)| message_hash("conversation_entries.message", message))
            .collect::<Result<_, CodecError>>()?)
    }
}

/// Record that `input`'s agent saw `write`'s messages, and forget the
/// cluster's sightings before `cutoff`.
async fn see(
    conn: &mut PgConnection,
    input: &ThreadInput,
    write: &Write,
    cutoff: Timestamp,
) -> Result<(), TxFailure> {
    let seen = &write.seen;
    if !seen.is_empty() {
        sqlx::query(
            "INSERT INTO reconstruct.seen_messages (agent, message, conversation, seen_at) \
             SELECT $1, m, $3, $4 FROM UNNEST($2::bytea[]) AS t(m) \
             ON CONFLICT (agent, message, conversation) \
             DO UPDATE SET seen_at = GREATEST(seen_messages.seen_at, EXCLUDED.seen_at)",
        )
        .bind(id_text(input.agent))
        .bind(hashes(seen))
        .bind(id_text(write.conversation))
        .bind(micros(input.at)?)
        .execute(&mut *conn)
        .await?;
    }
    sqlx::query("DELETE FROM reconstruct.seen_messages WHERE agent = ANY($1) AND seen_at < $2")
        .bind(ids(&input.members))
        .bind(micros(cutoff)?)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Record `write` for `input`.
async fn apply(
    conn: &mut PgConnection,
    input: &ThreadInput,
    write: &Write,
    cutoff: Timestamp,
) -> Result<(), TxFailure> {
    let (updated,): (i64,) = sqlx::query_as("SELECT nextval('reconstruct.conversation_updates')")
        .fetch_one(&mut *conn)
        .await?;
    let id = id_text(write.conversation);
    let head = write.head.map(|head| digest_bytes(&head.0));
    let last_system = write.last_system.as_ref().map(hash_bytes);
    let history_len = i32::try_from(write.history_len).map_err(|_| CodecError::Encode {
        reason: "history length beyond the stored range".to_owned(),
    })?;
    let start: i32 = match write.target {
        Target::New {
            agent,
            origin,
            base,
        } => {
            sqlx::query(
                "INSERT INTO reconstruct.conversations \
                 (id, agent, origin, history_len, head, last_system, updated) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .bind(&id)
            .bind(id_text(agent))
            .bind(json(&stored_origin(origin))?)
            .bind(history_len)
            .bind(&head)
            .bind(&last_system)
            .bind(updated)
            .execute(&mut *conn)
            .await?;
            match base {
                None => 0,
                Some((parent, k)) => {
                    let copied = sqlx::query(
                        "INSERT INTO reconstruct.conversation_entries \
                         (conversation, ordinal, message, role, exchange, history_index, chain, output) \
                         SELECT $1, ordinal, message, role, exchange, history_index, chain, output \
                         FROM reconstruct.conversation_entries \
                         WHERE conversation = $2 AND ordinal <= ( \
                             SELECT ordinal FROM reconstruct.conversation_entries \
                             WHERE conversation = $2 AND history_index = $3)",
                    )
                    .bind(&id)
                    .bind(id_text(parent))
                    .bind(i64::from(k) - 1)
                    .execute(&mut *conn)
                    .await?;
                    i32::try_from(copied.rows_affected()).map_err(|_| CodecError::Encode {
                        reason: "fork base beyond the stored range".to_owned(),
                    })?
                }
            }
        }
        Target::Existing => {
            let (next,): (i32,) = sqlx::query_as(
                "SELECT coalesce(max(ordinal) + 1, 0) FROM reconstruct.conversation_entries \
                 WHERE conversation = $1",
            )
            .bind(&id)
            .fetch_one(&mut *conn)
            .await?;
            sqlx::query(
                "UPDATE reconstruct.conversations \
                 SET history_len = $2, head = $3, last_system = $4, updated = $5 WHERE id = $1",
            )
            .bind(&id)
            .bind(history_len)
            .bind(&head)
            .bind(&last_system)
            .bind(updated)
            .execute(&mut *conn)
            .await?;
            next
        }
    };
    if !write.appended.is_empty() {
        let mut ordinals = Vec::with_capacity(write.appended.len());
        let mut messages = Vec::with_capacity(write.appended.len());
        let mut roles = Vec::with_capacity(write.appended.len());
        let mut indexes: Vec<Option<i32>> = Vec::with_capacity(write.appended.len());
        let mut chains: Vec<Option<Vec<u8>>> = Vec::with_capacity(write.appended.len());
        let mut outputs = Vec::with_capacity(write.appended.len());
        for (offset, new) in write.appended.iter().enumerate() {
            ordinals.push(start + count(offset)?);
            messages.push(hash_bytes(&new.entry.message));
            roles.push(role_text(new.entry.role).to_owned());
            match new.history {
                Some((index, chain)) => {
                    indexes.push(Some(i32::try_from(index).map_err(|_| {
                        CodecError::Encode {
                            reason: "history index beyond the stored range".to_owned(),
                        }
                    })?));
                    chains.push(Some(digest_bytes(&chain.0)));
                }
                None => {
                    indexes.push(None);
                    chains.push(None);
                }
            }
            outputs.push(new.output);
        }
        sqlx::query(
            "INSERT INTO reconstruct.conversation_entries \
             (conversation, ordinal, message, role, exchange, history_index, chain, output) \
             SELECT $1, o, m, r, $2, h, c, out \
             FROM UNNEST($3::integer[], $4::bytea[], $5::text[], $6::integer[], $7::bytea[], $8::boolean[]) \
             AS t(o, m, r, h, c, out)",
        )
        .bind(&id)
        .bind(id_text(input.exchange))
        .bind(ordinals)
        .bind(messages)
        .bind(roles)
        .bind(indexes)
        .bind(chains)
        .bind(outputs)
        .execute(&mut *conn)
        .await?;
    }
    sqlx::query(
        "INSERT INTO reconstruct.thread_records (exchange, conversation, outcome) VALUES ($1, $2, $3)",
    )
    .bind(id_text(input.exchange))
    .bind(&id)
    .bind(json(&StoredOutcome::from(write.outcome.clone()))?)
    .execute(&mut *conn)
    .await?;
    if let Some((key, len)) = &write.response {
        sqlx::query(
            "INSERT INTO reconstruct.responses (upstream, scope, response, conversation, history_len) \
             VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
        )
        .bind(&key.upstream.0)
        .bind(json(&key.scope)?)
        .bind(&key.response.0)
        .bind(&id)
        .bind(i64::from(*len))
        .execute(&mut *conn)
        .await?;
    }
    see(conn, input, write, cutoff).await
}

/// The conversation store on Postgres. Clones share the pool.
#[derive(Debug, Clone)]
pub struct PgConversations {
    pool: PgPool,
    retry: SerializableRetry,
    config: ThreadConfig,
}

impl PgConversations {
    /// The store over `pool`, whose `reconstruct` schema is migrated
    /// (`crate::agents::run_migrations`).
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            retry: SerializableRetry::default(),
            config: ThreadConfig::default(),
        }
    }

    /// Use `config` for every threading call.
    pub fn with_config(mut self, config: ThreadConfig) -> Self {
        self.config = config;
        self
    }

    /// Forget every agent's sightings older than the retention behind
    /// `now`, returning how many were forgotten. Threading forgets only the
    /// threaded cluster's; this sweeps agents that stopped calling.
    pub async fn forget_seen(&self, now: Timestamp) -> Result<u64, ThreadError> {
        let cutoff = self.config.seen_retention.cutoff(now);
        let sweep = async {
            let done = sqlx::query("DELETE FROM reconstruct.seen_messages WHERE seen_at < $1")
                .bind(micros(cutoff)?)
                .execute(&self.pool)
                .await?;
            Ok::<_, TxFailure>(done.rows_affected())
        };
        sweep
            .await
            .map_err(|failure| ThreadError::from_failure(&StorageFailure::from(failure)))
    }

    /// Use `retry` for every threading transaction.
    pub fn with_retry(mut self, retry: SerializableRetry) -> Self {
        self.retry = retry;
        self
    }
}

impl ConversationStore for PgConversations {
    async fn thread(&self, input: ThreadInput) -> Result<ThreadOutcome, ThreadError> {
        let input = Arc::new(input);
        let cutoff = self.config.seen_retention.cutoff(input.at);
        retry_serializable(&self.pool, &self.retry, |conn| {
            let input = Arc::clone(&input);
            Box::pin(async move {
                let mut reads = PgReads {
                    conn: &mut *conn,
                    cutoff,
                };
                let planned = plan(&mut reads, &input).await.map_err(tx)?;
                match planned {
                    Planned::Recorded(outcome) => Ok(outcome),
                    Planned::Write(write) => {
                        apply(conn, &input, &write, cutoff).await.map_err(tx)?;
                        Ok(write.outcome)
                    }
                }
            })
        })
        .await
        .map_err(ThreadError::from_tx)
    }

    async fn conversation(&self, id: ConversationId) -> Result<Option<Conversation>, ThreadError> {
        let read = async {
            let mut tx = self
                .pool
                .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                .await?;
            let row: Option<(String, String)> =
                sqlx::query_as("SELECT agent, origin FROM reconstruct.conversations WHERE id = $1")
                    .bind(id_text(id))
                    .fetch_optional(&mut *tx)
                    .await?;
            let Some((agent, origin)) = row else {
                return Ok::<_, TxFailure>(None);
            };
            let messages: Vec<(Vec<u8>,)> = sqlx::query_as(
                "SELECT message FROM reconstruct.conversation_entries \
                 WHERE conversation = $1 AND history_index IS NOT NULL ORDER BY history_index",
            )
            .bind(id_text(id))
            .fetch_all(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(Some(Conversation {
                id,
                agent: id_of("conversations.agent", &agent)?,
                messages: messages
                    .iter()
                    .map(|(message,)| message_hash("conversation_entries.message", message))
                    .collect::<Result<_, CodecError>>()?,
                origin: origin_of(from_json("conversations.origin", &origin)?),
            }))
        };
        read.await
            .map_err(|failure| ThreadError::from_failure(&StorageFailure::from(failure)))
    }

    async fn transcript(&self, id: ConversationId) -> Result<Vec<TranscriptEntry>, ThreadError> {
        let read = async {
            let rows: Vec<EntryRow> = sqlx::query_as(
                "SELECT ordinal, message, role, exchange, history_index, output \
                 FROM reconstruct.conversation_entries WHERE conversation = $1 ORDER BY ordinal",
            )
            .bind(id_text(id))
            .fetch_all(&self.pool)
            .await?;
            rows.iter()
                .map(|(ordinal, message, role, exchange, index, output)| {
                    Ok(TranscriptEntry {
                        ordinal: count_of("conversation_entries.ordinal", *ordinal)?,
                        message: message_hash("conversation_entries.message", message)?,
                        role: role_of(role)?,
                        exchange: id_of("conversation_entries.exchange", exchange)?,
                        history_index: index
                            .map(|index| count_of("conversation_entries.history_index", index))
                            .transpose()?,
                        output: *output,
                    })
                })
                .collect::<Result<Vec<_>, CodecError>>()
                .map_err(TxFailure::from)
        };
        read.await
            .map_err(|failure| ThreadError::from_failure(&StorageFailure::from(failure)))
    }

    async fn conversations(&self) -> Result<Vec<ConversationId>, ThreadError> {
        let read = async {
            let rows: Vec<(String,)> =
                sqlx::query_as("SELECT id FROM reconstruct.conversations ORDER BY id")
                    .fetch_all(&self.pool)
                    .await?;
            rows.iter()
                .map(|(id,)| id_of("conversations.id", id))
                .collect::<Result<Vec<_>, CodecError>>()
                .map_err(TxFailure::from)
        };
        read.await
            .map_err(|failure| ThreadError::from_failure(&StorageFailure::from(failure)))
    }
}
