//! Threading calls kept to be threaded again (`thread_calls`, migration
//! `0006_threading_order`): the request as two arrays (message hashes and
//! roles), the output and summary as hashes, and the rest (the previous
//! response an increment continues, the response it files, its traffic
//! source) as JSON.

use std::collections::HashMap;

use crosstalk_spec::ids::{ExchangeId, MessageHash};
use crosstalk_spec::observed::agent::IdentityScope;
use crosstalk_spec::observed::client::{TrafficSource, UpstreamId};
use crosstalk_spec::observed::exchange::ResponseId;
use crosstalk_spec::support::Timestamp;
use serde::{Deserialize, Serialize};
use sqlx::PgConnection;

use super::super::history::Entry;
use super::super::order::{ReorderWindow, ThreadCall};
use super::super::store::{RequestKind, ResponseKey, ThreadInput};
use super::{role_of, role_text};
use crate::agents::codec::{
    CodecError, from_json, hash_bytes, id_of, id_text, json, message_hash, micros, timestamp,
};
use crate::error::TxFailure;

/// A [`ResponseKey`] as the call's JSON holds it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredKey {
    upstream: UpstreamId,
    scope: IdentityScope,
    response: ResponseId,
}

impl From<&ResponseKey> for StoredKey {
    fn from(key: &ResponseKey) -> Self {
        Self {
            upstream: key.upstream.clone(),
            scope: key.scope.clone(),
            response: key.response.clone(),
        }
    }
}

impl From<StoredKey> for ResponseKey {
    fn from(key: StoredKey) -> Self {
        Self {
            upstream: key.upstream,
            scope: key.scope,
            response: key.response,
        }
    }
}

/// The call's JSON column.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredCall {
    /// The previous response of an increment; `None` for a full history.
    previous: Option<StoredKey>,
    response: Option<StoredKey>,
    source: TrafficSource,
}

/// A `thread_calls` row: exchange, agent, start, end, request messages and
/// roles, output, summary, conversation, call JSON.
type CallRow = (
    String,
    String,
    i64,
    i64,
    Vec<Vec<u8>>,
    Vec<String>,
    Option<Vec<u8>>,
    Option<Vec<u8>>,
    String,
    String,
);

fn call_of(row: CallRow) -> Result<ThreadCall, CodecError> {
    let (exchange, agent, started, ended, messages, roles, output, summary, conversation, call) =
        row;
    let stored: StoredCall = from_json("thread_calls.call", &call)?;
    if messages.len() != roles.len() {
        return Err(CodecError::Json {
            column: "thread_calls.request_roles",
            reason: format!("{} roles for {} messages", roles.len(), messages.len()),
        });
    }
    let request = messages
        .iter()
        .zip(&roles)
        .map(|(message, role)| {
            Ok(Entry {
                message: message_hash("thread_calls.request_messages", message)?,
                role: role_of(role)?,
            })
        })
        .collect::<Result<Vec<_>, CodecError>>()?;
    let hash = |column, bytes: Option<Vec<u8>>| -> Result<Option<MessageHash>, CodecError> {
        bytes.map(|bytes| message_hash(column, &bytes)).transpose()
    };
    Ok(ThreadCall {
        exchange: id_of("thread_calls.exchange", &exchange)?,
        at: timestamp("thread_calls.started_at", started)?,
        ended_at: timestamp("thread_calls.ended_at", ended)?,
        agent: id_of("thread_calls.agent", &agent)?,
        request,
        kind: match stored.previous {
            None => RequestKind::FullHistory,
            Some(previous) => RequestKind::Increment {
                previous: previous.into(),
            },
        },
        output: hash("thread_calls.output", output)?,
        response: stored.response.map(ResponseKey::from),
        summary: hash("thread_calls.summary", summary)?,
        conversation: id_of("thread_calls.conversation", &conversation)?,
        source: stored.source,
    })
}

/// Keep `input`'s call, and forget every call of its cluster that ended
/// more than `window` before it.
pub(super) async fn keep(
    conn: &mut PgConnection,
    input: &ThreadInput,
    window: ReorderWindow,
) -> Result<(), TxFailure> {
    let call = StoredCall {
        previous: match &input.kind {
            RequestKind::FullHistory => None,
            RequestKind::Increment { previous } => Some(previous.into()),
        },
        response: input.response.as_ref().map(StoredKey::from),
        source: input.source.clone(),
    };
    let messages: Vec<Vec<u8>> = input
        .request
        .iter()
        .map(|entry| hash_bytes(&entry.message))
        .collect();
    let roles: Vec<String> = input
        .request
        .iter()
        .map(|entry| role_text(entry.role).to_owned())
        .collect();
    sqlx::query(
        "INSERT INTO reconstruct.thread_calls \
         (exchange, agent, started_at, ended_at, request_messages, request_roles, output, \
          summary, conversation, call) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
         ON CONFLICT (exchange) DO UPDATE SET agent = EXCLUDED.agent, \
             started_at = EXCLUDED.started_at, ended_at = EXCLUDED.ended_at, \
             request_messages = EXCLUDED.request_messages, \
             request_roles = EXCLUDED.request_roles, output = EXCLUDED.output, \
             summary = EXCLUDED.summary, conversation = EXCLUDED.conversation, \
             call = EXCLUDED.call",
    )
    .bind(id_text(input.exchange))
    .bind(id_text(input.agent))
    .bind(micros(input.at)?)
    .bind(micros(input.ended_at)?)
    .bind(messages)
    .bind(roles)
    .bind(input.output.as_ref().map(hash_bytes))
    .bind(input.summary.as_ref().map(hash_bytes))
    .bind(id_text(input.conversation))
    .bind(json(&call)?)
    .execute(&mut *conn)
    .await?;
    let cutoff: Timestamp = window.before(input.ended_at);
    let members: Vec<String> = input.members.iter().map(|id| id_text(*id)).collect();
    sqlx::query("DELETE FROM reconstruct.thread_calls WHERE agent = ANY($1) AND ended_at < $2")
        .bind(members)
        .bind(micros(cutoff)?)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// The kept calls of `exchanges`.
pub(super) async fn calls(
    conn: &mut PgConnection,
    exchanges: &[ExchangeId],
) -> Result<HashMap<ExchangeId, ThreadCall>, TxFailure> {
    let ids: Vec<String> = exchanges.iter().map(|id| id_text(*id)).collect();
    let rows: Vec<CallRow> = sqlx::query_as(
        "SELECT exchange, agent, started_at, ended_at, request_messages, request_roles, \
         output, summary, conversation, call \
         FROM reconstruct.thread_calls WHERE exchange = ANY($1)",
    )
    .bind(ids)
    .fetch_all(&mut *conn)
    .await?;
    let mut calls = HashMap::with_capacity(rows.len());
    for row in rows {
        let call = call_of(row)?;
        calls.insert(call.exchange, call);
    }
    Ok(calls)
}
