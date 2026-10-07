//! The cursors the registry's lists issue, kept in `flow.cursors`.
//!
//! A token names a row holding the list it was issued for, the request it
//! binds (the canonical channel, window or filter, as JSON) and the sort key
//! of the last item served. A token this database never issued, one issued
//! for another list, and one presented with another binding all resolve to
//! nothing, which the list reports as `InvalidCursor`. Tokens survive a
//! restart and are valid on every node sharing the database.
//!
//! Rows are never needed after a traversal ends; [`prune_cursors`] drops
//! the ones issued before a cut-off.

use crosstalk_spec::paging::{Cursor, Page, PageSize};
use crosstalk_spec::support::NonEmpty;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::{PgConnection, PgPool};

use super::codec::{from_json, json};
use super::error::{Fault, FlowStoreError};

/// The list a cursor pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum List {
    Channels,
    ChannelTransmissions,
    ResourceUse,
    Transmissions,
}

impl List {
    fn tag(self) -> &'static str {
        match self {
            List::Channels => "ch",
            List::ChannelTransmissions => "tx",
            List::ResourceUse => "ru",
            List::Transmissions => "tl",
        }
    }
}

/// The sort key a cursor resumes after, or `None` when `after` is absent.
/// `Ok(None)` for no cursor; `Err(Invalid)` for one that does not resolve.
pub(crate) async fn resolve<L, K: DeserializeOwned>(
    conn: &mut PgConnection,
    list: List,
    binding: &str,
    after: Option<&Cursor<L>>,
) -> Result<Result<Option<K>, Invalid>, Fault> {
    let Some(cursor) = after else {
        return Ok(Ok(None));
    };
    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT binding, after_key FROM flow.cursors WHERE token = $1 AND list = $2",
    )
    .bind(cursor.token())
    .bind(list.tag())
    .fetch_optional(&mut *conn)
    .await?;
    match row {
        Some((bound, key)) if bound == binding => Ok(Ok(Some(from_json("cursor key", &key)?))),
        Some(_) | None => Ok(Err(Invalid)),
    }
}

/// A cursor did not resolve for this list and binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Invalid;

/// Cut one page from `rows`, the items following the cursor in list order
/// with at most one more than the page holds: the next cursor is issued
/// only when that extra item shows more follow, so `next` is `None` exactly
/// on the last page.
pub(crate) async fn page<T, L, K: Serialize>(
    pool: &PgPool,
    list: List,
    binding: &str,
    mut rows: Vec<T>,
    size: PageSize,
    key_of: impl Fn(&T) -> K,
) -> Result<Page<T, L>, FlowStoreError> {
    let limit = usize::from(size.get().get());
    if rows.len() <= limit {
        return Page::last(size, rows).map_err(|overflow| corrupt_page(&overflow));
    }
    rows.truncate(limit);
    let items = NonEmpty::from_vec(rows).ok_or_else(|| corrupt_page(&"empty page"))?;
    let last = items
        .iter()
        .last()
        .ok_or_else(|| corrupt_page(&"empty page"))?;
    let key = json("cursor key", &key_of(last))?;
    let token: String = sqlx::query_scalar(
        "INSERT INTO flow.cursors (token, list, binding, after_key) \
         VALUES ('fl-' || $1 || '-' || nextval('flow.cursor_tokens'), $1, $2, $3) \
         RETURNING token",
    )
    .bind(list.tag())
    .bind(binding)
    .bind(&key)
    .fetch_one(pool)
    .await?;
    let next = Cursor::from_token(token).map_err(|error| corrupt_page(&error))?;
    Page::more(size, items, next).map_err(|overflow| corrupt_page(&overflow))
}

fn corrupt_page(reason: &impl std::fmt::Debug) -> FlowStoreError {
    FlowStoreError::Corrupt {
        what: "page",
        reason: format!("{reason:?}"),
    }
}

/// Drop the cursors issued more than `older_than` ago (by the database's
/// clock). A traversal still holding one gets `InvalidCursor` and starts
/// over. Returns how many were dropped.
pub async fn prune_cursors(
    pool: &PgPool,
    older_than: std::time::Duration,
) -> Result<u64, FlowStoreError> {
    let seconds = i64::try_from(older_than.as_secs()).unwrap_or(i64::MAX);
    let done = sqlx::query(
        "DELETE FROM flow.cursors WHERE issued_at < now() - ($1::bigint * interval '1 second')",
    )
    .bind(seconds)
    .execute(pool)
    .await?;
    Ok(done.rows_affected())
}
