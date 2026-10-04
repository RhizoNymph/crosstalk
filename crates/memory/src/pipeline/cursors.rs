//! Page cursors for the in-memory stores.
//!
//! A database store authenticates its tokens with a MAC over the last sort
//! key and a digest of the request. The in-memory stores keep a table
//! instead: a token is `<list>-<n>`, the index of a row holding the request
//! it was issued for and the last key served. A token the table never
//! issued, or one presented with another request, is refused, which is
//! exactly what `InvalidCursor` means in every list the spec pages.

use std::sync::{Mutex, PoisonError};

use crosstalk_spec::paging::{Cursor, InvalidCursorToken};

/// One issued cursor: the request it binds and the last key it served.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssuedCursor<K> {
    pub request: String,
    pub last: K,
}

/// The cursors one list has issued. Tokens are never reused.
#[derive(Debug)]
pub struct CursorTable<K> {
    list: &'static str,
    issued: Mutex<Vec<IssuedCursor<K>>>,
}

impl<K: Clone> CursorTable<K> {
    /// `list` names the list in its tokens; it must be URL-safe base64
    /// characters (`A-Z a-z 0-9 - _`).
    pub fn new(list: &'static str) -> Self {
        Self {
            list,
            issued: Mutex::new(Vec::new()),
        }
    }

    /// A cursor after `last` for the request whose canonical text is
    /// `request`.
    pub fn issue<L>(&self, request: &str, last: K) -> Result<Cursor<L>, InvalidCursorToken> {
        let mut issued = self.issued.lock().unwrap_or_else(PoisonError::into_inner);
        let token = format!("{}-{}", self.list, issued.len());
        issued.push(IssuedCursor {
            request: request.to_owned(),
            last,
        });
        Cursor::from_token(token)
    }

    /// The last key `cursor` served, when this table issued it for
    /// `request`; `None` for any other token.
    pub fn redeem<L>(&self, cursor: &Cursor<L>, request: &str) -> Option<K> {
        let index: usize = cursor
            .token()
            .strip_prefix(self.list)?
            .strip_prefix('-')?
            .parse()
            .ok()?;
        let issued = self.issued.lock().unwrap_or_else(PoisonError::into_inner);
        let row = issued.get(index)?;
        (row.request == request).then(|| row.last.clone())
    }
}
