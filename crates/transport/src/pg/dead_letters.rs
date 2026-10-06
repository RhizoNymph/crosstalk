//! [`PgDeadLetters`]: the [`DeadLetterStore`] of a [`PgBus`](super::PgBus),
//! over `transport.dead_letters`.
//!
//! Letters are keyed by (group, `Envelope::id`), so a second letter for the
//! same envelope and group replaces the first. They are listed newest
//! envelope first, ties by group, both compared bytewise (`COLLATE "C"`),
//! which is the order `MpscBus`'s shelf uses.
//!
//! A cursor token is URL-safe text:
//!
//! | Part | Length | Content |
//! | --- | --- | --- |
//! | last id | 26 | the last served letter's `EventId` as ULID text |
//! | filter | 1 | `A` for every group, `G` for one group |
//! | last group | even | hex of the last served letter's group name |
//! | check | 16 | hex of a keyed hash over the parts before it and the filter's group |
//!
//! The check is keyed per bus value (std's SipHash `RandomState`), so a
//! token another bus value issued, or one issued for another filter, is
//! `InvalidCursor`. Like the shelf's, it is an integrity check against
//! misuse, not a MAC; the surface authenticates the cursors it serves.

use std::fmt::Write as _;
use std::hash::BuildHasher;
use std::num::NonZeroU32;
use std::sync::Arc;

use crosstalk_spec::ids::EventId;
use crosstalk_spec::interfaces::l2_transport::{
    BusError, ConsumerGroup, DeadLetter, DeadLetterStore,
};
use crosstalk_spec::paging::{Cursor, DeadLetterList, Page, PageRequest};
use crosstalk_spec::support::NonEmpty;

use super::Shared;
use super::publish::{CHANNEL, PUBLISH_LOCK};
use super::row::{EventRow, bus_error, decode_stored};

/// The dead letters of a [`PgBus`](super::PgBus). Cloning gives another
/// handle on the same table.
#[derive(Debug, Clone)]
pub struct PgDeadLetters {
    pub(crate) shared: Arc<Shared>,
}

const ID_LEN: usize = 26;
const CHECK_LEN: usize = 16;

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        // Writing to a String cannot fail.
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn unhex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(text.get(i..i + 2)?, 16).ok())
        .collect()
}

impl PgDeadLetters {
    fn check(&self, body: &str, filter: Option<&ConsumerGroup>) -> String {
        let filter = filter.map_or("", |g| g.0.as_str());
        let hash = self.shared.cursor_key.hash_one((body, filter));
        format!("{hash:0CHECK_LEN$x}")
    }

    fn token(&self, last: &DeadLetter, filter: Option<&ConsumerGroup>) -> String {
        let kind = if filter.is_some() { 'G' } else { 'A' };
        let body = format!(
            "{}{kind}{}",
            last.envelope.id.ulid_text(),
            hex(last.group.0.as_bytes())
        );
        let check = self.check(&body, filter);
        body + &check
    }

    /// The (id, group) a token positions after, if this bus value issued it
    /// for the same filter.
    fn position(
        &self,
        token: &str,
        filter: Option<&ConsumerGroup>,
    ) -> Result<(String, String), BusError> {
        let invalid = || BusError::InvalidCursor;
        if token.len() < ID_LEN + 1 + CHECK_LEN || !token.is_ascii() {
            return Err(invalid());
        }
        let (body, check) = token.split_at(token.len() - CHECK_LEN);
        if self.check(body, filter) != check {
            return Err(invalid());
        }
        let (id, rest) = body.split_at(ID_LEN);
        let (kind, group) = rest.split_at(1);
        let expected = if filter.is_some() { "G" } else { "A" };
        if kind != expected {
            return Err(invalid());
        }
        EventId::from_ulid_text(id).map_err(|_| invalid())?;
        let group = unhex(group)
            .and_then(|bytes| String::from_utf8(bytes).ok())
            .ok_or_else(invalid)?;
        Ok((id.to_owned(), group))
    }
}

type LetterRow = (String, String, i32, String);

fn letter(row: LetterRow) -> Result<DeadLetter, BusError> {
    let (group, envelope, attempts, last_error) = row;
    Ok(DeadLetter {
        group: ConsumerGroup(group),
        envelope: decode_stored(&envelope)?,
        attempts: u32::try_from(attempts)
            .ok()
            .and_then(NonZeroU32::new)
            .unwrap_or(NonZeroU32::MIN),
        last_error,
    })
}

impl DeadLetterStore for PgDeadLetters {
    /// Store the letter, replacing one for the same envelope and group. An
    /// envelope the log does not hold is stored there unrouted: no group
    /// admits it, and a later publish of its id adds nothing
    /// (`transport.publish.idempotent-on-id`), but a replay delivers it.
    async fn put(&self, letter: DeadLetter) -> Result<(), BusError> {
        let row = EventRow::encode(&letter.envelope)?;
        let attempts = i32::try_from(letter.attempts.get()).unwrap_or(i32::MAX);
        let shared = &self.shared;
        let stored: Result<(), sqlx::Error> = async {
            let mut tx = shared.pool.begin().await?;
            sqlx::query("SELECT pg_advisory_xact_lock($1)")
                .bind(PUBLISH_LOCK)
                .execute(&mut *tx)
                .await?;
            sqlx::query(
                "INSERT INTO transport.events (id, subject, at, envelope, routed) \
                 VALUES ($1, $2, $3, $4, false) ON CONFLICT (id) DO NOTHING",
            )
            .bind(&row.id)
            .bind(&row.subject)
            .bind(row.at)
            .bind(&row.envelope)
            .execute(&mut *tx)
            .await?;
            let seq: i64 = sqlx::query_scalar("SELECT seq FROM transport.events WHERE id = $1")
                .bind(&row.id)
                .fetch_one(&mut *tx)
                .await?;
            sqlx::query(
                "INSERT INTO transport.dead_letters \
                 (group_name, event_id, seq, at, envelope, attempts, last_error) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7) \
                 ON CONFLICT (group_name, event_id) DO UPDATE SET seq = excluded.seq, \
                 at = excluded.at, envelope = excluded.envelope, attempts = excluded.attempts, \
                 last_error = excluded.last_error",
            )
            .bind(&letter.group.0)
            .bind(&row.id)
            .bind(seq)
            .bind(row.at)
            .bind(&row.envelope)
            .bind(attempts)
            .bind(&letter.last_error)
            .execute(&mut *tx)
            .await?;
            tx.commit().await
        }
        .await;
        stored.map_err(|error| bus_error("put dead letter", &error))
    }

    /// Re-admit the letter for its group alone, at attempt 1, and remove
    /// it, in one transaction (`transport.deadletter.replay-consumes`). A
    /// group that never subscribed, or no longer takes the letter's
    /// subject, is `PublishRejected` and the letter stays. Unlike
    /// `MpscBus`, a replay does not wait for room in the group.
    async fn replay(&self, group: &ConsumerGroup, id: EventId) -> Result<(), BusError> {
        let shared = &self.shared;
        let text = id.ulid_text();
        let unknown = || BusError::UnknownDeadLetter {
            group: group.clone(),
            id,
        };
        let mut tx = shared
            .pool
            .begin()
            .await
            .map_err(|e| bus_error("replay", &e))?;
        let outcome: Result<Result<(), BusError>, sqlx::Error> = async {
            let letter: Option<(i64, i64)> = sqlx::query_as(
                "SELECT seq, at FROM transport.dead_letters \
                 WHERE group_name = $1 AND event_id = $2 FOR UPDATE",
            )
            .bind(&group.0)
            .bind(&text)
            .fetch_optional(&mut *tx)
            .await?;
            let Some((seq, at)) = letter else {
                return Ok(Err(unknown()));
            };
            let subjects: Option<Vec<String>> =
                sqlx::query_scalar("SELECT subjects FROM transport.groups WHERE name = $1")
                    .bind(&group.0)
                    .fetch_optional(&mut *tx)
                    .await?;
            let Some(subjects) = subjects else {
                return Ok(Err(BusError::PublishRejected {
                    reason: "no consumer group of that name has subscribed on this bus".to_owned(),
                }));
            };
            let subject: String =
                sqlx::query_scalar("SELECT subject FROM transport.events WHERE seq = $1")
                    .bind(seq)
                    .fetch_one(&mut *tx)
                    .await?;
            if !subjects.contains(&subject) {
                return Ok(Err(BusError::PublishRejected {
                    reason: format!("the group does not subscribe to {subject}"),
                }));
            }
            sqlx::query(
                "INSERT INTO transport.deliveries (group_name, seq, at, state) \
                 VALUES ($1, $2, $3, 'ready') ON CONFLICT (group_name, seq) DO NOTHING",
            )
            .bind(&group.0)
            .bind(seq)
            .bind(at)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "DELETE FROM transport.dead_letters WHERE group_name = $1 AND event_id = $2",
            )
            .bind(&group.0)
            .bind(&text)
            .execute(&mut *tx)
            .await?;
            sqlx::query("SELECT pg_notify($1, '')")
                .bind(CHANNEL)
                .execute(&mut *tx)
                .await?;
            Ok(Ok(()))
        }
        .await;
        match outcome {
            Ok(Ok(())) => {
                tx.commit().await.map_err(|e| bus_error("replay", &e))?;
                tracing::info!(group = %group.0, event = %text, "dead letter replayed");
                shared.wake_all();
                Ok(())
            }
            Ok(Err(refused)) => {
                let _ = tx.rollback().await;
                Err(refused)
            }
            Err(error) => {
                let _ = tx.rollback().await;
                Err(bus_error("replay", &error))
            }
        }
    }

    async fn list(
        &self,
        group: Option<&ConsumerGroup>,
        page: &PageRequest<DeadLetterList>,
    ) -> Result<Page<DeadLetter, DeadLetterList>, BusError> {
        let after = match &page.after {
            None => None,
            Some(cursor) => Some(self.position(cursor.token(), group)?),
        };
        let size = page.size;
        let wanted = usize::from(size.get().get());
        let limit = i64::try_from(wanted + 1).unwrap_or(i64::MAX);
        let (after_id, after_group) = after.unzip();
        let rows: Vec<LetterRow> = sqlx::query_as(
            "SELECT group_name, envelope, attempts, last_error FROM transport.dead_letters \
             WHERE ($1::text IS NULL OR group_name = $1::text COLLATE \"C\") \
               AND ($2::text IS NULL OR (event_id, group_name) < ($2::text COLLATE \"C\", $3::text COLLATE \"C\")) \
             ORDER BY event_id DESC, group_name DESC LIMIT $4",
        )
        .bind(group.map(|g| g.0.as_str()))
        .bind(after_id.as_deref())
        .bind(after_group.as_deref())
        .bind(limit)
        .fetch_all(&self.shared.pool)
        .await
        .map_err(|e| bus_error("list dead letters", &e))?;
        let mut items = rows
            .into_iter()
            .map(letter)
            .collect::<Result<Vec<_>, _>>()?;
        let more = items.len() > wanted;
        items.truncate(wanted);
        let page = match (more, items.last()) {
            (true, Some(last)) => {
                let token = self.token(last, group);
                let cursor = Cursor::from_token(token).map_err(|_| BusError::InvalidCursor)?;
                let items = NonEmpty::from_vec(items).ok_or(BusError::InvalidCursor)?;
                Page::more(size, items, cursor)
            }
            _ => Page::last(size, items),
        };
        page.map_err(|_| BusError::InvalidCursor)
    }
}

#[cfg(test)]
mod tests {
    use super::{hex, unhex};

    #[test]
    fn hex_round_trips() {
        for text in ["", "flow", "live-l5-flow", "ünïcode"] {
            let encoded = hex(text.as_bytes());
            assert_eq!(unhex(&encoded).as_deref(), Some(text.as_bytes()));
        }
        assert_eq!(unhex("abc"), None);
        assert_eq!(unhex("zz"), None);
    }
}
