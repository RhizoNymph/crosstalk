//! Keyset pages over a list already in its order: cut after the page size
//! and issue a [`cursor`] after the last item served when
//! more follow.

use crosstalk_spec::paging::{Page, PageSize};
use crosstalk_spec::support::NonEmpty;

use super::StorageFailure;
use super::cursor::{self, CursorKey};
use super::tx::{Failure, fail};

/// What a page's cursor is bound to: the store's key, the list's name and
/// the request (a digest of everything the cursor must be presented with).
#[derive(Debug, Clone, Copy)]
pub struct Binding<'a> {
    pub key: &'a CursorKey,
    pub list: &'a str,
    pub request: &'a [u8],
}

impl Binding<'_> {
    /// The position `cursor` resumes after, when this binding issued it.
    pub fn resume<L>(&self, cursor: &crosstalk_spec::paging::Cursor<L>) -> Option<Vec<u8>> {
        cursor::resume(self.key, self.list, self.request, cursor)
    }
}

/// Cut `items` (in list order, every one after the cursor) into a page of
/// `size`, with a cursor after its last item when more follow. `key` is an
/// item's position, as [`Binding::resume`] returns it.
pub fn page_of<T, L, E: Failure>(
    items: Vec<T>,
    size: PageSize,
    binding: Binding<'_>,
    key: impl Fn(&T) -> Vec<u8>,
) -> Result<Page<T, L>, E> {
    let wanted = usize::from(size.get().get());
    if items.len() <= wanted {
        return Page::last(size, items).map_err(|error| overflow(&error));
    }
    let mut items = items;
    items.truncate(wanted);
    let last = items.last().map(&key).ok_or_else(empty::<E>)?;
    let next = cursor::issue(binding.key, binding.list, binding.request, &last)
        .map_err(|error| fail::<E>(StorageFailure::Invariant(error.to_string())))?;
    let items = NonEmpty::from_vec(items).ok_or_else(empty::<E>)?;
    Page::more(size, items, next).map_err(|error| overflow(&error))
}

fn empty<E: Failure>() -> E {
    fail(StorageFailure::Invariant(
        "an empty page with more after it".to_owned(),
    ))
}

fn overflow<E: Failure>(error: &impl std::fmt::Debug) -> E {
    fail(StorageFailure::Invariant(format!(
        "page overflow: {error:?}"
    )))
}
