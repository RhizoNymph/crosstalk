//! Building blocks shared by the pipeline stores (L3 to L5): the state lock,
//! the outbox stores publish their events to, a manual clock, a
//! deterministic id sequence and the cursor table that issues page tokens.
//!
//! None of these is a spec trait. They are what an in-memory store needs to
//! stand in for a database: a place for state, a transaction boundary, ids,
//! time and an outbox.

mod clock;
mod cursors;
pub mod harness;
mod ids;
mod outbox;
mod state;

pub use clock::{Clock, ManualClock};
pub use cursors::{CursorTable, IssuedCursor};

/// Page `items` (already filtered and in list order, keyed by `key`) for a
/// request: the items after `after` (exclusive, in list order, so keys are
/// strictly less in a newest-first list), at most `size` of them, with a
/// cursor when more follow. `issue` mints the cursor from the last key
/// served. The shape every newest-first keyset page has.
pub(crate) fn page_after<T, K: Copy + Ord, L>(
    items: Vec<T>,
    key: impl Fn(&T) -> K,
    after: Option<K>,
    size: crosstalk_spec::paging::PageSize,
    issue: impl FnOnce(K) -> Result<crosstalk_spec::paging::Cursor<L>, PageError>,
) -> Result<crosstalk_spec::paging::Page<T, L>, PageError> {
    use crosstalk_spec::paging::Page;
    use crosstalk_spec::support::NonEmpty;

    let limit = usize::from(size.get().get());
    let mut rest: Vec<T> = items
        .into_iter()
        .filter(|item| after.is_none_or(|after| key(item) < after))
        .collect();
    let more = rest.len() > limit;
    rest.truncate(limit);
    if !more {
        return Page::last(size, rest).map_err(|_| PageError::Overflow);
    }
    let last = rest.last().map(&key).ok_or(PageError::Overflow)?;
    let next = issue(last)?;
    let items = NonEmpty::from_vec(rest).ok_or(PageError::Overflow)?;
    Page::more(size, items, next).map_err(|_| PageError::Overflow)
}

/// Why a page could not be built. Neither can happen for a well-formed
/// request: both mean a bug in the store, reported as a store failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum PageError {
    #[error("page built with more items than its size")]
    Overflow,
    #[error("cursor token refused by its own constructor")]
    Token,
}
pub use ids::IdSequence;
pub use outbox::{Outbox, drain};
pub use state::State;
