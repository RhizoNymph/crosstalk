//! Keyset paging for every reference store.
//!
//! [`crosstalk_spec::paging`] leaves the cursor's content to the server: the
//! last sort key served and the request it was issued for, authenticated.
//! The reference stores keep that in a [`CursorBook`] instead of a MAC: a
//! token is a key into the book, so a token the book never issued, one from
//! another store, and one presented with another request are all unknown
//! (`InvalidCursor`), and a valid one gives back exactly what was recorded
//! when it was issued.
//!
//! [`page_after`] cuts one page from the items that follow the cursor, in
//! list order, and issues the next cursor only when more items follow, so
//! `next` is `None` exactly on the last page.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use crosstalk_spec::paging::{Cursor, Page, PageSize};
use crosstalk_spec::support::NonEmpty;

/// Distinguishes books, so one store's tokens are unknown to another's.
static BOOKS: AtomicU64 = AtomicU64::new(0);

/// Every cursor one store has issued, with the request it is bound to (`B`)
/// and what the next page starts after (`K`). A store whose reads take only
/// a shared lock keeps its book behind its own mutex.
#[derive(Debug)]
pub struct CursorBook<B, K> {
    book: u64,
    issued: BTreeMap<String, (B, K)>,
}

impl<B, K> Default for CursorBook<B, K> {
    fn default() -> Self {
        Self {
            book: BOOKS.fetch_add(1, Ordering::Relaxed),
            issued: BTreeMap::new(),
        }
    }
}

/// A page could not be built. Never happens for the sizes and tokens the
/// book makes; the stores report it as a store failure rather than panic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PageError {
    #[error("issued an invalid cursor token")]
    Token,
    #[error("cut more items than the page size")]
    Overflow,
}

impl<B: PartialEq, K: Clone> CursorBook<B, K> {
    /// What `cursor` was issued with, if this book issued it for a request
    /// equal to `binding`.
    pub fn resolve<L>(&self, cursor: &Cursor<L>, binding: &B) -> Option<K> {
        self.issued
            .get(cursor.token())
            .filter(|(bound, _)| bound == binding)
            .map(|(_, key)| key.clone())
    }

    /// Issue a cursor bound to `binding`, resuming after `key`.
    pub fn issue<L>(&mut self, binding: B, key: K) -> Result<Cursor<L>, PageError> {
        let token = format!("m{}x{}", self.book, self.issued.len() + 1);
        let cursor = Cursor::from_token(token.clone()).map_err(|_| PageError::Token)?;
        self.issued.insert(token, (binding, key));
        Ok(cursor)
    }
}

/// One page of `remaining`, the items after the cursor (or every item, for
/// a first page) in list order. When more items follow the page, the next
/// cursor is issued bound to `binding` and resuming after `key_of` of the
/// page's last item.
pub fn page_after<T, L, B: PartialEq, K: Clone>(
    book: &mut CursorBook<B, K>,
    mut remaining: Vec<T>,
    size: PageSize,
    binding: B,
    key_of: impl Fn(&T) -> K,
) -> Result<Page<T, L>, PageError> {
    let limit = usize::from(size.get().get());
    if remaining.len() <= limit {
        return Page::last(size, remaining).map_err(|_| PageError::Overflow);
    }
    remaining.truncate(limit);
    let items = NonEmpty::from_vec(remaining).ok_or(PageError::Overflow)?;
    let last = key_of(items.iter().last().ok_or(PageError::Overflow)?);
    let next = book.issue(binding, last)?;
    Page::more(size, items, next).map_err(|_| PageError::Overflow)
}
