//! The `cursor` query key of every paged list.

use std::num::NonZeroU32;

use topcoat::context::Cx;
use topcoat::router::query_params;

use crate::contract::errors::QueryError;
use crate::contract::lists::{Cursor, PageRequest};
use crate::pages::common::form::invalid;

pub const PAGE_SIZE: NonZeroU32 = match NonZeroU32::new(50) {
    Some(size) => size,
    None => NonZeroU32::MIN,
};

/// Cursors are opaque to the UI; this bounds what it passes on.
const CURSOR_MAX: usize = 512;

#[query_params]
struct CursorQuery {
    cursor: Option<String>,
}

/// A cursor from the query: printable ASCII, at most [`CURSOR_MAX`] bytes.
pub fn parse_cursor(text: Option<&str>) -> Result<Option<Cursor>, QueryError> {
    match text {
        None => Ok(None),
        Some(text) if text.len() > CURSOR_MAX => Err(invalid("cursor", "too long")),
        Some(text) if !text.bytes().all(|b| b.is_ascii_graphic()) => {
            Err(invalid("cursor", "not a cursor"))
        }
        Some(text) => Ok(Some(Cursor(text.to_owned()))),
    }
}

/// The page this request asks for.
pub fn page_request(cx: &Cx) -> Result<PageRequest, QueryError> {
    let raw = query_params::<CursorQuery>(cx).map_err(|e| invalid("cursor", e))?;
    Ok(PageRequest {
        cursor: parse_cursor(raw.cursor.as_deref())?,
        limit: PAGE_SIZE,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursors_are_bounded_printable_text() {
        assert_eq!(parse_cursor(None), Ok(None));
        assert_eq!(parse_cursor(Some("abc=")), Ok(Some(Cursor("abc=".into()))));
        assert!(parse_cursor(Some("a b")).is_err());
        assert!(parse_cursor(Some(&"x".repeat(CURSOR_MAX + 1))).is_err());
    }
}
