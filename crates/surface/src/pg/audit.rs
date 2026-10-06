//! [`PgAuditLog`]: the audit log and its write-ahead intents in Postgres.
//!
//! Tables (`0001_surface.sql`): `surface.audit` (one row per entry, its
//! wire JSON, kind and author), `surface.audit_subjects` (the subject
//! filter's index), `surface.action_intents`.
//!
//! The rules match the reference (`InMemoryAuditLog`):
//!
//! ```text
//! append(entry)       ─ same entry under its id: no-op; another entry, or an intent holding the id: IdReused; else INSERT
//! intend(intent)      ─ id an entry has: IdReused; same intent: no-op; another intent: IdReused; else INSERT
//! complete(entry)     ─ no intent: append; else the entry must record the intent's call (id, at, caller,
//!                       action; else IdReused): INSERT entry, DELETE intent, one transaction
//! recover_interrupted ─ intents by (at, id); each: INSERT intent.interrupted(), DELETE intent, one transaction
//! query               ─ newest first by (at, id), keyset after the cursor's (at, id)
//! ```
//!
//! A cursor is `hex(at i64 BE ‖ id u128 BE)_hex(MAC)`, the MAC keyed by the
//! key derived from the deployment secret under [`AUDIT_CURSOR_LABEL`]
//! over a digest of the filter, so it survives a restart and a cursor
//! presented with another filter is `InvalidCursor`.

use std::sync::Arc;

use crosstalk_spec::ids::AuditId;
use crosstalk_spec::ids::secret::KeyedHasher;
use crosstalk_spec::interfaces::l8_surface::audit::{
    AuditAuthor, AuditBody, AuditEntry, AuditError, AuditFilter, AuditIntent, AuditIntents,
    AuditLog, AuditSubject,
};
use crosstalk_spec::paging::{AuditList, Page, PageRequest};
use crosstalk_spec::support::NonEmpty;
use crosstalk_store::{SerializableRetry, TxError, retry_serializable};
use sqlx::{PgConnection, PgPool, Row};

use super::codec::{CodecError, from_json, micros, to_json};
use super::{StorageFailure, settle};
use crate::cursor::{CursorKey, RequestDigest};

/// The label the audit log's cursor key is derived under.
pub const AUDIT_CURSOR_LABEL: &str = "crosstalk.cursor.v1.audit";

/// The audit log on Postgres. Cloning shares the pool.
#[derive(Clone)]
pub struct PgAuditLog {
    pool: PgPool,
    retry: SerializableRetry,
    cursors: Arc<CursorKey>,
}

impl std::fmt::Debug for PgAuditLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgAuditLog").finish_non_exhaustive()
    }
}

impl PgAuditLog {
    /// The log in `pool`'s database (migrated with
    /// [`super::run_migrations`]); its cursors are keyed from `secret`.
    pub fn new(pool: PgPool, retry: SerializableRetry, secret: &KeyedHasher) -> Self {
        Self {
            pool,
            retry,
            cursors: Arc::new(CursorKey::derive(secret, AUDIT_CURSOR_LABEL)),
        }
    }

    /// The pool the log reads and writes.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Run `body` in one serializable transaction, mapping a store failure
    /// into `AuditError::Store`.
    async fn transact<T: Send>(
        &self,
        body: impl for<'c> FnMut(&'c mut PgConnection) -> crosstalk_store::TxFuture<'c, T, AuditError>
        + Send,
    ) -> Result<T, AuditError> {
        retry_serializable(&self.pool, &self.retry, body)
            .await
            .map_err(|error| settle(error, |failure| store_error(&failure)))
    }
}

fn store_error(failure: &StorageFailure) -> AuditError {
    AuditError::Store {
        reason: failure.reason(),
    }
}

fn codec(error: CodecError) -> TxError<AuditError> {
    TxError::Abort(store_error(&StorageFailure::Codec(error)))
}

/// The author column: the operator's ULID text, `None` for config.
fn author_text(entry: &AuditEntry) -> Option<String> {
    match entry.by() {
        AuditAuthor::Operator(operator) => Some(operator.ulid_text()),
        AuditAuthor::Config => None,
    }
}

fn kind_text(entry: &AuditEntry) -> &'static str {
    match &entry.body {
        AuditBody::Operator(_) => "operator",
        AuditBody::Config(_) => "config",
        AuditBody::Export(_) => "export",
    }
}

fn subject_text(subject: &AuditSubject) -> Result<String, CodecError> {
    to_json("audit subject", subject)
}

/// The entry stored under `id`, if any.
async fn stored_entry(
    conn: &mut PgConnection,
    id: AuditId,
) -> Result<Option<AuditEntry>, TxError<AuditError>> {
    let row = sqlx::query("SELECT entry FROM surface.audit WHERE id = $1")
        .bind(id.ulid_text())
        .fetch_optional(&mut *conn)
        .await?;
    match row {
        None => Ok(None),
        Some(row) => {
            let text: String = row.try_get("entry")?;
            from_json("audit entry", &text).map(Some).map_err(codec)
        }
    }
}

/// The intent holding `id`, if any.
async fn held_intent(
    conn: &mut PgConnection,
    id: AuditId,
) -> Result<Option<AuditIntent>, TxError<AuditError>> {
    let row = sqlx::query("SELECT intent FROM surface.action_intents WHERE id = $1")
        .bind(id.ulid_text())
        .fetch_optional(&mut *conn)
        .await?;
    match row {
        None => Ok(None),
        Some(row) => {
            let text: String = row.try_get("intent")?;
            from_json("audit intent", &text).map(Some).map_err(codec)
        }
    }
}

/// Insert `entry` and its subjects. The caller checked the id is free.
async fn insert_entry(
    conn: &mut PgConnection,
    entry: &AuditEntry,
) -> Result<(), TxError<AuditError>> {
    let at = micros("audit entry time", entry.at).map_err(codec)?;
    let text = to_json("audit entry", entry).map_err(codec)?;
    let row = sqlx::query(
        "INSERT INTO surface.audit (id, at, kind, by_operator, entry) \
         VALUES ($1, $2, $3, $4, $5) RETURNING seq",
    )
    .bind(entry.id.ulid_text())
    .bind(at)
    .bind(kind_text(entry))
    .bind(author_text(entry))
    .bind(text)
    .fetch_one(&mut *conn)
    .await?;
    let seq: i64 = row.try_get("seq")?;
    let subjects = entry
        .subjects()
        .iter()
        .map(subject_text)
        .collect::<Result<Vec<_>, _>>()
        .map_err(codec)?;
    if !subjects.is_empty() {
        sqlx::query(
            "INSERT INTO surface.audit_subjects (audit_seq, subject) \
             SELECT $1, subject FROM unnest($2::text[]) AS subject \
             ON CONFLICT DO NOTHING",
        )
        .bind(seq)
        .bind(subjects)
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

/// [`AuditLog::append`] inside a transaction: a no-op for the entry
/// already stored under its id; `IdReused` for another entry there, or
/// while an intent holds the id.
pub(super) async fn append_in(
    conn: &mut PgConnection,
    entry: &AuditEntry,
) -> Result<(), TxError<AuditError>> {
    match stored_entry(conn, entry.id).await? {
        Some(stored) if stored == *entry => Ok(()),
        Some(_) => Err(TxError::Abort(AuditError::IdReused(entry.id))),
        None if held_intent(conn, entry.id).await?.is_some() => {
            Err(TxError::Abort(AuditError::IdReused(entry.id)))
        }
        None => insert_entry(conn, entry).await,
    }
}

/// Whether `entry` records the call `intent` announced: an operator entry
/// with the intent's id, time, caller and action.
fn completes(intent: &AuditIntent, entry: &AuditEntry) -> bool {
    match &entry.body {
        AuditBody::Operator(record) => {
            entry.id == intent.id()
                && entry.at == intent.at()
                && record.caller() == intent.caller()
                && record.action() == intent.action()
        }
        AuditBody::Config(_) | AuditBody::Export(_) => false,
    }
}

async fn delete_intent(conn: &mut PgConnection, id: AuditId) -> Result<(), TxError<AuditError>> {
    sqlx::query("DELETE FROM surface.action_intents WHERE id = $1")
        .bind(id.ulid_text())
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// The filter's digest, which a cursor is bound to: its wire JSON.
fn filter_digest(filter: &AuditFilter) -> Result<[u8; 32], AuditError> {
    let text = to_json("audit filter", filter)
        .map_err(|error| store_error(&StorageFailure::Codec(error)))?;
    Ok(RequestDigest::new("audit").bytes(text.as_bytes()).finish())
}

const POSITION_LEN: usize = 8 + 16;

fn position(at: i64, id: AuditId) -> [u8; POSITION_LEN] {
    let mut out = [0_u8; POSITION_LEN];
    out[..8].copy_from_slice(&at.to_be_bytes());
    out[8..].copy_from_slice(&id.as_ulid().to_be_bytes());
    out
}

fn parse_position(bytes: &[u8]) -> Option<(i64, AuditId)> {
    if bytes.len() != POSITION_LEN {
        return None;
    }
    let at = i64::from_be_bytes(bytes[..8].try_into().ok()?);
    let id = u128::from_be_bytes(bytes[8..].try_into().ok()?);
    Some((at, AuditId::from_ulid(id)))
}

/// The query's bind values for `filter`.
struct FilterBinds {
    any_author: bool,
    config: bool,
    operators: Vec<String>,
    subject: Option<String>,
    from: Option<i64>,
    until: Option<i64>,
}

impl FilterBinds {
    fn of(filter: &AuditFilter) -> Result<Self, CodecError> {
        let operators = filter
            .by
            .iter()
            .filter_map(|author| match author {
                AuditAuthor::Operator(operator) => Some(operator.ulid_text()),
                AuditAuthor::Config => None,
            })
            .collect();
        let (from, until) = match filter.window {
            None => (None, None),
            Some(window) => (
                Some(micros("window start", window.start())?),
                Some(micros("window end", window.end())?),
            ),
        };
        Ok(Self {
            any_author: filter.by.is_empty(),
            config: filter.by.contains(&AuditAuthor::Config),
            operators,
            subject: filter.subject.as_ref().map(subject_text).transpose()?,
            from,
            until,
        })
    }
}

impl AuditLog for PgAuditLog {
    async fn append(&mut self, entry: AuditEntry) -> Result<(), AuditError> {
        let entry = Arc::new(entry);
        self.transact(|conn| {
            let entry = Arc::clone(&entry);
            Box::pin(async move { append_in(conn, &entry).await })
        })
        .await
    }

    async fn query(
        &self,
        filter: &AuditFilter,
        page: &PageRequest<AuditList>,
    ) -> Result<Page<AuditEntry, AuditList>, AuditError> {
        let digest = filter_digest(filter)?;
        let after = match &page.after {
            None => None,
            Some(cursor) => Some(
                self.cursors
                    .resume(cursor, &digest)
                    .as_deref()
                    .and_then(parse_position)
                    .ok_or(AuditError::InvalidCursor)?,
            ),
        };
        let binds =
            FilterBinds::of(filter).map_err(|error| store_error(&StorageFailure::Codec(error)))?;
        let size = page.size.get().get();
        let rows = sqlx::query(
            "SELECT a.at, a.entry FROM surface.audit a \
             WHERE ($1 OR (a.kind = 'config' AND $2) OR a.by_operator = ANY($3::text[])) \
               AND ($4::text IS NULL OR EXISTS ( \
                    SELECT 1 FROM surface.audit_subjects s \
                    WHERE s.audit_seq = a.seq AND s.subject = $4)) \
               AND ($5::bigint IS NULL OR a.at >= $5) \
               AND ($6::bigint IS NULL OR a.at < $6) \
               AND ($7::bigint IS NULL OR (a.at, a.id) < ($7, $8::text COLLATE \"C\")) \
             ORDER BY a.at DESC, a.id DESC \
             LIMIT $9",
        )
        .bind(binds.any_author)
        .bind(binds.config)
        .bind(binds.operators)
        .bind(binds.subject)
        .bind(binds.from)
        .bind(binds.until)
        .bind(after.map(|(at, _)| at))
        .bind(after.map(|(_, id)| id.ulid_text()))
        .bind(i64::from(size) + 1)
        .fetch_all(&self.pool)
        .await
        .map_err(|error| store_error(&StorageFailure::Query(error)))?;
        let mut entries = Vec::with_capacity(rows.len());
        for row in &rows {
            let read = |error: sqlx::Error| store_error(&StorageFailure::Query(error));
            let at: i64 = row.try_get("at").map_err(read)?;
            let text: String = row.try_get("entry").map_err(read)?;
            let entry: AuditEntry = from_json("audit entry", &text)
                .map_err(|error| store_error(&StorageFailure::Codec(error)))?;
            entries.push((at, entry));
        }
        page_of(&self.cursors, page, &digest, entries)
    }
}

/// One page of `rows` (fetched one past the page size): a cursor after
/// the page's last entry when more follow.
fn page_of(
    key: &CursorKey,
    page: &PageRequest<AuditList>,
    digest: &[u8; 32],
    mut rows: Vec<(i64, AuditEntry)>,
) -> Result<Page<AuditEntry, AuditList>, AuditError> {
    let size = page.size;
    let limit = usize::from(size.get().get());
    let overflow = |what: &str| AuditError::Store {
        reason: format!("audit page: {what}"),
    };
    if rows.len() <= limit {
        let items = rows.into_iter().map(|(_, entry)| entry).collect();
        return Page::last(size, items).map_err(|_| overflow("over its size"));
    }
    rows.truncate(limit);
    let (at, last) = rows
        .last()
        .map(|(at, entry)| (*at, entry.id))
        .ok_or_else(|| overflow("empty page with more"))?;
    let next = key
        .issue(&position(at, last), digest)
        .ok_or_else(|| overflow("cursor too long"))?;
    let items = NonEmpty::from_vec(rows.into_iter().map(|(_, entry)| entry).collect())
        .ok_or_else(|| overflow("empty page with more"))?;
    Page::more(size, items, next).map_err(|_| overflow("over its size"))
}

impl AuditIntents for PgAuditLog {
    async fn intend(&mut self, intent: &AuditIntent) -> Result<(), AuditError> {
        let intent = Arc::new(intent.clone());
        self.transact(|conn| {
            let intent = Arc::clone(&intent);
            Box::pin(async move {
                if stored_entry(conn, intent.id()).await?.is_some() {
                    return Err(TxError::Abort(AuditError::IdReused(intent.id())));
                }
                match held_intent(conn, intent.id()).await? {
                    Some(held) if held == *intent => Ok(()),
                    Some(_) => Err(TxError::Abort(AuditError::IdReused(intent.id()))),
                    None => {
                        let at = micros("intent time", intent.at()).map_err(codec)?;
                        let text = to_json("audit intent", &*intent).map_err(codec)?;
                        sqlx::query(
                            "INSERT INTO surface.action_intents (id, at, intent) \
                             VALUES ($1, $2, $3)",
                        )
                        .bind(intent.id().ulid_text())
                        .bind(at)
                        .bind(text)
                        .execute(&mut *conn)
                        .await?;
                        Ok(())
                    }
                }
            })
        })
        .await
    }

    async fn complete(&mut self, entry: AuditEntry) -> Result<(), AuditError> {
        let entry = Arc::new(entry);
        self.transact(|conn| {
            let entry = Arc::clone(&entry);
            Box::pin(async move {
                let Some(intent) = held_intent(conn, entry.id).await? else {
                    return append_in(conn, &entry).await;
                };
                if !completes(&intent, &entry) {
                    return Err(TxError::Abort(AuditError::IdReused(entry.id)));
                }
                insert_entry(conn, &entry).await?;
                delete_intent(conn, entry.id).await
            })
        })
        .await
    }

    async fn recover_interrupted(&mut self) -> Result<Vec<AuditId>, AuditError> {
        let rows = sqlx::query("SELECT intent FROM surface.action_intents ORDER BY at, id")
            .fetch_all(&self.pool)
            .await
            .map_err(|error| store_error(&StorageFailure::Query(error)))?;
        let mut leftover = Vec::with_capacity(rows.len());
        for row in &rows {
            let text: String = row
                .try_get("intent")
                .map_err(|error| store_error(&StorageFailure::Query(error)))?;
            let intent: AuditIntent = from_json("audit intent", &text)
                .map_err(|error| store_error(&StorageFailure::Codec(error)))?;
            leftover.push(intent);
        }
        let mut recovered = Vec::with_capacity(leftover.len());
        for intent in leftover {
            let intent = Arc::new(intent);
            let appended = self
                .transact(|conn| {
                    let intent = Arc::clone(&intent);
                    Box::pin(async move {
                        // Another recovery may have taken it meanwhile.
                        if held_intent(conn, intent.id()).await?.is_none() {
                            return Ok(false);
                        }
                        if stored_entry(conn, intent.id()).await?.is_none() {
                            insert_entry(conn, &intent.interrupted()).await?;
                        }
                        delete_intent(conn, intent.id()).await?;
                        Ok(true)
                    })
                })
                .await?;
            if appended {
                tracing::warn!(
                    audit = %intent.id().ulid_text(),
                    operator = %intent.caller().operator().ulid_text(),
                    kind = ?intent.action().kind(),
                    at = intent.at().as_micros(),
                    "operator action interrupted by a stop; recorded as interrupted"
                );
                recovered.push(intent.id());
            }
        }
        Ok(recovered)
    }
}
