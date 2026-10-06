//! The spec's `ConversationReads` over [`PgConversations`]: each read is
//! one `REPEATABLE READ READ ONLY` transaction, so it reads one snapshot.
//! The list filters and orders in SQL on the conversation columns
//! migration `0004_conversation_reads` added; a turn window reads its turn
//! rows, then exactly the entries they name.

use crosstalk_spec::interfaces::l3_reconstruction::conversations::ExchangePlacement;
use std::collections::{BTreeMap, HashMap};

use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::ids::{ConversationId, ExchangeId};
use crosstalk_spec::interfaces::l3_reconstruction::conversations::{
    ConversationQuery, ConversationReadError, ConversationReads, ReplayFilter, StoredConversation,
    TurnIndex, TurnSlice, TurnWindow,
};
use crosstalk_spec::observed::client::TrafficSource;
use crosstalk_spec::observed::conversation::Conversation;
use crosstalk_spec::paging::{ConversationList, Page, PageRequest};
use sqlx::{PgConnection, Postgres, Transaction};

use super::super::reads::{TurnRow, binding, origin_text, outcome_kind_of};
use super::{EntryRow, PgConversations, entry_of, origin_of};
use crate::agents::codec::{
    CodecError, count_of, from_json, id_of, id_text, message_hash, timestamp,
};
use crate::error::{StorageFailure, StoreReason, TxFailure};

/// A conversation row: id, agent, origin, source, started, last turn,
/// turns.
type ConversationRow = (String, String, String, String, i64, i64, i32);

/// A turn row: turn, exchange, first ordinal, entries, agent, start,
/// outcome, history end.
type TurnColumns = (i32, String, i32, i32, String, i64, String, i32);

/// The columns of a [`ConversationRow`], in order.
macro_rules! conversation_columns {
    () => {
        "id, agent, origin, source, started_at, last_turn_at, turns"
    };
}

fn failed(failure: TxFailure) -> ConversationReadError {
    ConversationReadError::from_failure(&StorageFailure::from(failure))
}

async fn snapshot(pool: &sqlx::PgPool) -> Result<Transaction<'static, Postgres>, TxFailure> {
    Ok(pool
        .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await?)
}

/// The non-system history of each of `ids`, in order.
async fn histories(
    conn: &mut PgConnection,
    ids: &[String],
) -> Result<HashMap<String, Vec<crosstalk_spec::ids::MessageHash>>, TxFailure> {
    let rows: Vec<(String, Vec<u8>)> = sqlx::query_as(
        "SELECT conversation, message FROM reconstruct.conversation_entries \
         WHERE conversation = ANY($1) AND history_index IS NOT NULL \
         ORDER BY conversation, history_index",
    )
    .bind(ids)
    .fetch_all(&mut *conn)
    .await?;
    let mut histories: HashMap<String, Vec<_>> = HashMap::new();
    for (conversation, message) in rows {
        histories
            .entry(conversation)
            .or_default()
            .push(message_hash("conversation_entries.message", &message)?);
    }
    Ok(histories)
}

/// The stored conversations `rows` name, with their histories, in order.
async fn conversations(
    conn: &mut PgConnection,
    rows: Vec<ConversationRow>,
) -> Result<Vec<StoredConversation>, TxFailure> {
    let ids: Vec<String> = rows.iter().map(|row| row.0.clone()).collect();
    let mut histories = histories(conn, &ids).await?;
    let mut stored = Vec::with_capacity(rows.len());
    for (id, agent, origin, source, started_at, last_turn_at, turns) in rows {
        let messages = histories.remove(&id).unwrap_or_default();
        stored.push(StoredConversation {
            conversation: Conversation {
                id: id_of("conversations.id", &id)?,
                agent: id_of("conversations.agent", &agent)?,
                messages,
                origin: origin_of(from_json("conversations.origin", &origin)?),
            },
            source: from_json::<TrafficSource>("conversations.source", &source)?,
            started_at: timestamp("conversations.started_at", started_at)?,
            last_turn_at: timestamp("conversations.last_turn_at", last_turn_at)?,
            turns: count_of("conversations.turns", turns)?,
        });
    }
    Ok(stored)
}

fn turn_row(columns: &TurnColumns) -> Result<TurnRow, CodecError> {
    let (_, exchange, first, entries, agent, started_at, outcome, history_end) = columns;
    Ok(TurnRow {
        exchange: id_of("conversation_turns.exchange", exchange)?,
        first_ordinal: count_of("conversation_turns.first_ordinal", *first)?,
        entries: count_of("conversation_turns.entries", *entries)?,
        agent: id_of("conversation_turns.agent", agent)?,
        started_at: timestamp("conversation_turns.started_at", *started_at)?,
        outcome: outcome_kind_of(outcome).ok_or_else(|| CodecError::Json {
            column: "conversation_turns.outcome",
            reason: format!("unknown outcome {outcome:?}"),
        })?,
        history_end: count_of("conversation_turns.history_end", *history_end)?,
    })
}

impl ConversationReads for PgConversations {
    async fn list(
        &self,
        query: &ConversationQuery,
        page: &PageRequest<ConversationList>,
    ) -> Result<Page<StoredConversation, ConversationList>, ConversationReadError> {
        let binding = binding(query);
        let after = page
            .after
            .as_ref()
            .map(|cursor| self.cursors.resume(cursor, &binding))
            .transpose()?;
        let agents: Option<Vec<String>> = query
            .agents
            .as_ref()
            .map(|agents| agents.iter().map(|agent| id_text(*agent)).collect());
        let origins: Vec<String> = query
            .origins
            .iter()
            .map(|kind| origin_text(*kind).to_owned())
            .collect();
        let (replay, corpus): (&str, Option<&str>) = match &query.replay {
            ReplayFilter::Include => ("include", None),
            ReplayFilter::Exclude => ("exclude", None),
            ReplayFilter::Only { corpus } => ("only", corpus.as_ref().map(|c| c.0.as_str())),
        };
        let limit = i64::from(page.size.get().get()) + 1;
        let read = async {
            let mut tx = snapshot(&self.pool).await?;
            let rows: Vec<ConversationRow> = sqlx::query_as(concat!(
                "SELECT ",
                conversation_columns!(),
                " FROM reconstruct.conversations \
                 WHERE ($1::text IS NULL OR id < $1) \
                   AND ($2::text[] IS NULL OR agent = ANY($2)) \
                   AND (cardinality($3::text[]) = 0 OR origin_kind = ANY($3)) \
                   AND ($4 = 'include' \
                        OR ($4 = 'exclude' AND replay_corpus IS NULL) \
                        OR ($4 = 'only' AND replay_corpus IS NOT NULL \
                            AND ($5::text IS NULL OR replay_corpus = $5))) \
                 ORDER BY id DESC LIMIT $6"
            ))
            .bind(after.map(id_text))
            .bind(agents)
            .bind(origins)
            .bind(replay)
            .bind(corpus)
            .bind(limit)
            .fetch_all(&mut *tx)
            .await?;
            let stored = conversations(&mut tx, rows).await?;
            tx.commit().await?;
            Ok::<_, TxFailure>(stored)
        };
        let rows = read.await.map_err(failed)?;
        self.cursors.page(&binding, page.size, rows)
    }

    async fn conversation(
        &self,
        id: ConversationId,
    ) -> Result<Option<StoredConversation>, ConversationReadError> {
        let read = async {
            let mut tx = snapshot(&self.pool).await?;
            let rows: Vec<ConversationRow> = sqlx::query_as(concat!(
                "SELECT ",
                conversation_columns!(),
                " FROM reconstruct.conversations WHERE id = $1"
            ))
            .bind(id_text(id))
            .fetch_all(&mut *tx)
            .await?;
            let stored = conversations(&mut tx, rows).await?;
            tx.commit().await?;
            Ok::<_, TxFailure>(stored.into_iter().next())
        };
        read.await.map_err(failed)
    }

    async fn successors(
        &self,
        id: ConversationId,
    ) -> Result<Vec<StoredConversation>, ConversationReadError> {
        let read = async {
            let mut tx = snapshot(&self.pool).await?;
            let rows: Vec<ConversationRow> = sqlx::query_as(concat!(
                "SELECT ",
                conversation_columns!(),
                " FROM reconstruct.conversations WHERE origin_of = $1 ORDER BY started_at, id"
            ))
            .bind(id_text(id))
            .fetch_all(&mut *tx)
            .await?;
            let stored = conversations(&mut tx, rows).await?;
            tx.commit().await?;
            Ok::<_, TxFailure>(stored)
        };
        read.await.map_err(failed)
    }

    async fn turns(
        &self,
        id: ConversationId,
        window: &TurnWindow,
    ) -> Result<Option<TurnSlice>, ConversationReadError> {
        let read = async {
            let mut tx = snapshot(&self.pool).await?;
            let total: Option<(i32,)> =
                sqlx::query_as("SELECT turns FROM reconstruct.conversations WHERE id = $1")
                    .bind(id_text(id))
                    .fetch_optional(&mut *tx)
                    .await?;
            let Some((total,)) = total else {
                return Ok::<_, TxFailure>(None);
            };
            let total = count_of("conversations.turns", total)?;
            let range = window.range(total);
            let rows: Vec<TurnColumns> = sqlx::query_as(
                "SELECT turn, exchange, first_ordinal, entries, agent, started_at, outcome, \
                        history_end \
                 FROM reconstruct.conversation_turns \
                 WHERE conversation = $1 AND turn >= $2 AND turn < $3 ORDER BY turn",
            )
            .bind(id_text(id))
            .bind(i64::from(range.start))
            .bind(i64::from(range.end))
            .fetch_all(&mut *tx)
            .await?;
            let turns: Vec<(u32, TurnRow)> = rows
                .iter()
                .map(|columns| {
                    Ok::<_, CodecError>((
                        count_of("conversation_turns.turn", columns.0)?,
                        turn_row(columns)?,
                    ))
                })
                .collect::<Result<_, _>>()?;
            let first = turns.first().map_or(0, |(_, row)| row.first_ordinal);
            let end = turns.last().map_or(0, |(_, row)| row.ordinals().end);
            let entries: Vec<EntryRow> = sqlx::query_as(
                "SELECT ordinal, message, role, exchange, history_index, output, carried_over \
                 FROM reconstruct.conversation_entries \
                 WHERE conversation = $1 AND ordinal >= $2 AND ordinal < $3 ORDER BY ordinal",
            )
            .bind(id_text(id))
            .bind(i64::from(first))
            .bind(i64::from(end))
            .fetch_all(&mut *tx)
            .await?;
            tx.commit().await?;
            let mut by_ordinal = BTreeMap::new();
            for row in &entries {
                let entry = entry_of(row)?;
                by_ordinal.insert(entry.ordinal, entry);
            }
            let mut read = Vec::with_capacity(turns.len());
            for (index, row) in turns {
                let mut its = Vec::with_capacity(row.entries as usize);
                for ordinal in row.ordinals() {
                    let entry = by_ordinal.get(&ordinal).copied().ok_or_else(|| {
                        StorageFailure::Inconsistent {
                            reason: format!(
                                "turn {index} of {} names ordinal {ordinal} past its transcript",
                                id.ulid_text()
                            ),
                        }
                    })?;
                    its.push(entry);
                }
                read.push(row.turn(index, its));
            }
            Ok(Some(TurnSlice { total, turns: read }))
        };
        read.await.map_err(failed)
    }

    async fn locate(
        &self,
        ids: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, ExchangePlacement>, ConversationReadError> {
        let wanted: Vec<String> = ids.ids().iter().map(|id| id_text(*id)).collect();
        let read = async {
            let rows: Vec<(String, String, i32, String)> = sqlx::query_as(
                "SELECT exchange, conversation, turn, agent FROM reconstruct.conversation_turns \
                 WHERE exchange = ANY($1)",
            )
            .bind(&wanted)
            .fetch_all(&self.pool)
            .await?;
            let mut located = BTreeMap::new();
            for (exchange, conversation, turn, agent) in rows {
                located.insert(
                    id_of("conversation_turns.exchange", &exchange)?,
                    ExchangePlacement {
                        agent: id_of("conversation_turns.agent", &agent)?,
                        conversation: id_of("conversation_turns.conversation", &conversation)?,
                        turn: TurnIndex(count_of("conversation_turns.turn", turn)?),
                    },
                );
            }
            Ok::<_, TxFailure>(located)
        };
        read.await.map_err(failed)
    }

    async fn branch_turn(
        &self,
        parent: ConversationId,
        shared_prefix: u32,
    ) -> Result<Option<TurnIndex>, ConversationReadError> {
        let read = async {
            let row: Option<(i32,)> = sqlx::query_as(
                "SELECT turn FROM reconstruct.conversation_turns \
                 WHERE conversation = $1 AND history_end <= $2 ORDER BY turn DESC LIMIT 1",
            )
            .bind(id_text(parent))
            .bind(i64::from(shared_prefix))
            .fetch_optional(&self.pool)
            .await?;
            Ok::<_, TxFailure>(
                row.map(|(turn,)| count_of("conversation_turns.turn", turn).map(TurnIndex))
                    .transpose()?,
            )
        };
        read.await.map_err(failed)
    }
}
