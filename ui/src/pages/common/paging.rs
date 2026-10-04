//! The `cursor` query key of every paged list, and page sizes.

use crosstalk_spec::paging::{Cursor, PageRequest, PageSize};
use topcoat::context::Cx;
use topcoat::router::query_params;

use crate::error::UiError;
use crate::pages::common::form::invalid;

/// The size of a list page.
pub const PAGE_SIZE: u16 = 50;

#[query_params]
struct CursorQuery {
    cursor: Option<String>,
}

/// A page size of `n` items, clamped to `1..=PageSize::MAX`.
pub fn size(n: u16) -> PageSize {
    let mut candidate = n.clamp(1, PageSize::MAX);
    loop {
        match PageSize::new(candidate) {
            Ok(size) => return size,
            // Unreachable while the spec allows 1..=MAX; fall back to 1.
            Err(_) => candidate = 1,
        }
    }
}

/// A number of items, as the pages count them.
pub trait Count {
    /// Saturates at `u16::MAX`.
    fn items(self) -> u16;
}

impl Count for u16 {
    fn items(self) -> u16 {
        self
    }
}

impl Count for std::num::NonZeroU32 {
    fn items(self) -> u16 {
        u16::try_from(self.get()).unwrap_or(u16::MAX)
    }
}

/// The first page of a list, `n` items long (clamped like [`size`]).
pub fn first<L>(n: impl Count) -> PageRequest<L> {
    PageRequest {
        size: size(n.items()),
        after: None,
    }
}

/// A cursor from the query: a token the surface could have issued (non-empty,
/// bounded, URL-safe base64). Whether it did is the surface's to say.
pub fn parse_cursor<L>(text: Option<&str>) -> Result<Option<Cursor<L>>, UiError> {
    text.map(|text| {
        Cursor::from_token(text.to_owned()).map_err(|e| invalid("cursor", format!("{e:?}")))
    })
    .transpose()
}

/// The page this request asks for, [`PAGE_SIZE`] items long.
pub fn page_request<L>(cx: &Cx) -> Result<PageRequest<L>, UiError> {
    let raw = query_params::<CursorQuery>(cx).map_err(|e| invalid("cursor", e))?;
    Ok(PageRequest {
        size: size(PAGE_SIZE),
        after: parse_cursor(raw.cursor.as_deref())?,
    })
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::paging::AlertList;

    use super::*;

    #[test]
    fn cursors_are_bounded_url_safe_tokens() {
        assert_eq!(parse_cursor::<AlertList>(None), Ok(None));
        assert_eq!(
            parse_cursor::<AlertList>(Some("abc-_9")).map(|c| c.map(|c| c.token().to_owned())),
            Ok(Some("abc-_9".to_owned()))
        );
        assert!(parse_cursor::<AlertList>(Some("a b")).is_err());
        assert!(parse_cursor::<AlertList>(Some("abc=")).is_err());
        assert!(
            parse_cursor::<AlertList>(Some(&"x".repeat(Cursor::<AlertList>::MAX_LEN + 1))).is_err()
        );
    }

    #[test]
    fn sizes_clamp_into_range() {
        assert_eq!(size(0).get().get(), 1);
        assert_eq!(size(50).get().get(), 50);
        assert_eq!(size(u16::MAX).get().get(), PageSize::MAX);
    }
}
