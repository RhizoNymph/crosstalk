//! The Postgres store's side of order-tolerant threading
//! (`super::super::order`), inside the call's `SERIALIZABLE` transaction:
//! finding the turns threaded after a late exchange's place, undoing one,
//! and threading one call.

use std::collections::HashMap;

use crosstalk_spec::ids::{AgentId, ConversationId, ExchangeId};
use crosstalk_spec::interfaces::l3_reconstruction::ThreadOutcome;
use sqlx::PgConnection;

use super::super::config::SeenRetention;
use super::super::order::{LaterTurn, OrderedStore, ReorderWindow, ThreadCall, TurnKey};
use super::super::plan::{Planned, plan};
use super::super::store::{StoredOutcome, ThreadInput};
use super::{PgReads, apply, calls, ids};
use crate::agents::codec::{CodecError, count_of, from_json, id_of, id_text, micros, timestamp};
use crate::error::{StorageFailure, TxFailure};

/// A `conversation_turns` row as a later turn: seq, exchange, end,
/// conversation, turn, first ordinal.
type LaterRow = (i64, String, i64, String, i32, i32);

fn later_of(row: LaterRow) -> Result<LaterTurn, CodecError> {
    let (seq, exchange, ended_at, conversation, turn, first_ordinal) = row;
    Ok(LaterTurn {
        seq: u64::try_from(seq).map_err(|_| CodecError::Number {
            column: "conversation_turns.seq",
            value: seq,
        })?,
        key: TurnKey {
            ended_at: timestamp("conversation_turns.ended_at", ended_at)?,
            exchange: id_of("conversation_turns.exchange", &exchange)?,
        },
        conversation: id_of("conversation_turns.conversation", &conversation)?,
        turn: count_of("conversation_turns.turn", turn)?,
        first_ordinal: count_of("conversation_turns.first_ordinal", first_ordinal)?,
    })
}

/// One threading call's step: the transaction's connection and the
/// store's settings.
pub(super) struct Step<'c> {
    pub(super) conn: &'c mut PgConnection,
    pub(super) retention: SeenRetention,
    pub(super) window: ReorderWindow,
}

impl OrderedStore for Step<'_> {
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

    async fn later(
        &mut self,
        members: &[AgentId],
        key: TurnKey,
    ) -> Result<Vec<LaterTurn>, TxFailure> {
        let rows: Vec<LaterRow> = sqlx::query_as(
            "WITH first AS ( \
                 SELECT min(seq) AS seq FROM reconstruct.conversation_turns \
                 WHERE agent = ANY($1) \
                   AND (ended_at, exchange COLLATE \"C\") > ($2, $3 COLLATE \"C\")) \
             SELECT t.seq, t.exchange, t.ended_at, t.conversation, t.turn, t.first_ordinal \
             FROM reconstruct.conversation_turns t, first \
             WHERE t.agent = ANY($1) AND t.seq >= first.seq ORDER BY t.seq",
        )
        .bind(ids(members))
        .bind(micros(key.ended_at)?)
        .bind(id_text(key.exchange))
        .fetch_all(&mut *self.conn)
        .await?;
        Ok(rows
            .into_iter()
            .map(later_of)
            .collect::<Result<_, CodecError>>()?)
    }

    async fn turn_counts(
        &mut self,
        conversations: &[ConversationId],
    ) -> Result<HashMap<ConversationId, u32>, TxFailure> {
        let wanted: Vec<String> = conversations.iter().map(|id| id_text(*id)).collect();
        let rows: Vec<(String, i32)> =
            sqlx::query_as("SELECT id, turns FROM reconstruct.conversations WHERE id = ANY($1)")
                .bind(wanted)
                .fetch_all(&mut *self.conn)
                .await?;
        Ok(rows
            .iter()
            .map(|(id, turns)| {
                Ok((
                    id_of("conversations.id", id)?,
                    count_of("conversations.turns", *turns)?,
                ))
            })
            .collect::<Result<_, CodecError>>()?)
    }

    async fn calls(
        &mut self,
        exchanges: &[ExchangeId],
    ) -> Result<HashMap<ExchangeId, ThreadCall>, TxFailure> {
        calls::calls(self.conn, exchanges).await
    }

    async fn unthread(&mut self, turn: &LaterTurn) -> Result<(), TxFailure> {
        let exchange = id_text(turn.key.exchange);
        let conversation = id_text(turn.conversation);
        let index = i32::try_from(turn.turn).map_err(|_| CodecError::Encode {
            reason: "turn index beyond the stored range".to_owned(),
        })?;
        let (turns,): (i32,) =
            sqlx::query_as("SELECT turns FROM reconstruct.conversations WHERE id = $1 FOR UPDATE")
                .bind(&conversation)
                .fetch_one(&mut *self.conn)
                .await?;
        if turns != index + 1 {
            return Err(TxFailure::Other(StorageFailure::Inconsistent {
                reason: format!("turn {index} of {conversation} is not its last of {turns}"),
            }));
        }
        for statement in [
            "DELETE FROM reconstruct.thread_records WHERE exchange = $1",
            "DELETE FROM reconstruct.seen_messages WHERE exchange = $1",
            "DELETE FROM reconstruct.responses WHERE exchange = $1",
            "DELETE FROM reconstruct.conversation_turns WHERE exchange = $1",
        ] {
            sqlx::query(statement)
                .bind(&exchange)
                .execute(&mut *self.conn)
                .await?;
        }
        if index == 0 {
            sqlx::query("DELETE FROM reconstruct.conversation_entries WHERE conversation = $1")
                .bind(&conversation)
                .execute(&mut *self.conn)
                .await?;
            sqlx::query("DELETE FROM reconstruct.conversations WHERE id = $1")
                .bind(&conversation)
                .execute(&mut *self.conn)
                .await?;
            return Ok(());
        }
        sqlx::query(
            "DELETE FROM reconstruct.conversation_entries WHERE conversation = $1 AND ordinal >= $2",
        )
        .bind(&conversation)
        .bind(i64::from(turn.first_ordinal))
        .execute(&mut *self.conn)
        .await?;
        let restored = sqlx::query(
            "UPDATE reconstruct.conversations c \
             SET history_len = p.history_end, \
                 head = (SELECT e.chain FROM reconstruct.conversation_entries e \
                         WHERE e.conversation = c.id AND e.history_index = p.history_end - 1), \
                 last_system = p.last_system, last_turn_at = p.started_at, \
                 last_ended_at = p.ended_at, last_exchange = p.exchange, turns = $2 \
             FROM reconstruct.conversation_turns p \
             WHERE c.id = $1 AND p.conversation = c.id AND p.turn = $2 - 1",
        )
        .bind(&conversation)
        .bind(index)
        .execute(&mut *self.conn)
        .await?;
        if restored.rows_affected() != 1 {
            return Err(TxFailure::Other(StorageFailure::Inconsistent {
                reason: format!("{conversation} has no turn before {index}"),
            }));
        }
        Ok(())
    }

    async fn thread_now(&mut self, input: &ThreadInput) -> Result<ThreadOutcome, TxFailure> {
        let cutoff = self.retention.cutoff(input.at);
        let planned = {
            let mut reads = PgReads {
                conn: &mut *self.conn,
                cutoff,
            };
            plan(&mut reads, input).await?
        };
        match planned {
            Planned::Recorded(outcome) => Ok(outcome),
            Planned::Write(write) => {
                apply(self.conn, input, &write, self.window.before(cutoff)).await?;
                calls::keep(self.conn, input, self.window).await?;
                Ok(write.outcome)
            }
        }
    }
}
