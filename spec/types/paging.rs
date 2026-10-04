//! Cursor pagination for list queries.
//!
//! Every list is ordered by a sort key that is unique and never changes once
//! an item exists, and is served newest first (descending key):
//!
//! | List | Marker | Sort key |
//! | --- | --- | --- |
//! | channels | [`ChannelList`] | `ChannelId` |
//! | agents | [`AgentList`] | `AgentId` |
//! | alert rules | [`AlertRuleList`] | `AlertRuleId` |
//! | dead letters | [`DeadLetterList`] | (`Envelope::id`, `ConsumerGroup`) |
//! | transmissions on an edge | [`EdgeTransmissionList`] | (`Confirmed::at`, `TransmissionId`) |
//! | the audit log | [`AuditList`] | (`AuditEntry::at`, `AuditId`) |
//! | alerts | [`AlertList`] | `AlertId` |
//! | topics of a version | [`TopicList`] | `TopicId` |
//! | stored projections | [`ProjectionList`] | `ProjectionId` |
//! | search hits | [`SearchList`] | (score, `TransmissionId`) |
//! | a channel's resources | [`ResourceUseList`] | `ResourceId` |
//!
//! A search hit's score is a fixed function of the query, the embedding
//! model and the transmission (no rank fusion and no corpus statistics), so
//! it never changes during a traversal; the cursor pins the embedding model
//! and the topic-model version it was computed under.
//!
//! A [`Cursor`] holds the sort key of the last item served, so the next page
//! is "the items after that key that match the request" (keyset
//! pagination). Inserting or removing items never shifts a position, so a
//! traversal returns every item that matches throughout it exactly once,
//! and never returns an item twice, however many writes happen between
//! pages. An item inserted during a traversal appears at most once: on a
//! later page if its key sorts after the cursor, otherwise not at all.
//!
//! A cursor also binds the request it came from (which list, its filter, and
//! for an edge list or search its window and the topic-model version its
//! first page resolved; for a channel's resources its canonical channel and
//! window). The server
//! authenticates the token it issues; one it cannot verify, or one presented
//! with a different request, is rejected as an invalid cursor. The marker
//! type parameter makes presenting one list's cursor to another list a
//! compile error.
//!
//! A valid cursor whose pinned topic-model version or embedding model is no
//! longer available is not an invalid cursor: the next page fails with the
//! typed reason (`VersionNotRetained`, `Conflict(EmbeddingModelChanged)`).

use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroU16;

use crate::support::NonEmpty;

macro_rules! list_marker {
    ($($(#[$doc:meta])* $name:ident;)*) => {$(
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum $name {}
    )*};
}

list_marker! {
    /// `QueryApi::channels`.
    ChannelList;
    /// `QueryApi::agents`.
    AgentList;
    /// `QueryApi::alert_rules`.
    AlertRuleList;
    /// `QueryApi::dead_letters` and `DeadLetterStore::list`.
    DeadLetterList;
    /// `QueryApi::edge_transmissions` and `EdgeStore::transmissions`.
    EdgeTransmissionList;
    /// `QueryApi::audit` and `AuditLog::query`.
    AuditList;
    /// `QueryApi::alerts`.
    AlertList;
    /// `QueryApi::topics` and `TopicCatalog::topics`.
    TopicList;
    /// `QueryApi::projections` and `ProjectionStore::list`.
    ProjectionList;
    /// `QueryApi::search` and `SearchIndex::query`.
    SearchList;
    /// `QueryApi::channel_resources` and `ChannelRegistry::resource_use`.
    ResourceUseList;
}

/// How many items a page may hold: `1..=PageSize::MAX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageSize(NonZeroU16);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidPageSize {
    Zero,
    AboveMax { max: u16, got: u16 },
}

impl PageSize {
    pub const MAX: u16 = 500;

    pub fn new(size: u16) -> Result<Self, InvalidPageSize> {
        let size = NonZeroU16::new(size).ok_or(InvalidPageSize::Zero)?;
        if size.get() > Self::MAX {
            return Err(InvalidPageSize::AboveMax {
                max: Self::MAX,
                got: size.get(),
            });
        }
        Ok(Self(size))
    }

    pub fn get(self) -> NonZeroU16 {
        self.0
    }
}

/// An opaque position in one list `L`. Clients pass it back unchanged.
///
/// Built only through [`Cursor::from_token`]: the token is non-empty, at most
/// [`Cursor::MAX_LEN`] bytes, and URL-safe base64 (`A-Z a-z 0-9 - _`), so it
/// can travel in a query string. Its content is the server's: the last sort
/// key served, a digest of the request, and a MAC over both.
pub struct Cursor<L> {
    token: String,
    list: PhantomData<fn() -> L>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidCursorToken {
    Empty,
    TooLong {
        max: usize,
        got: usize,
    },
    /// The byte at `index` is outside the URL-safe base64 alphabet.
    BadByte {
        index: usize,
    },
}

impl<L> Cursor<L> {
    pub const MAX_LEN: usize = 1024;

    pub fn from_token(token: String) -> Result<Self, InvalidCursorToken> {
        if token.is_empty() {
            return Err(InvalidCursorToken::Empty);
        }
        if token.len() > Self::MAX_LEN {
            return Err(InvalidCursorToken::TooLong {
                max: Self::MAX_LEN,
                got: token.len(),
            });
        }
        let bad = token
            .bytes()
            .position(|b| !(b.is_ascii_alphanumeric() || b == b'-' || b == b'_'));
        if let Some(index) = bad {
            return Err(InvalidCursorToken::BadByte { index });
        }
        Ok(Self {
            token,
            list: PhantomData,
        })
    }

    pub fn token(&self) -> &str {
        &self.token
    }
}

// Manual impls: derives would require `L` itself to implement each trait.
impl<L> fmt::Debug for Cursor<L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Cursor").field(&self.token).finish()
    }
}

impl<L> Clone for Cursor<L> {
    fn clone(&self) -> Self {
        Self {
            token: self.token.clone(),
            list: PhantomData,
        }
    }
}

impl<L> PartialEq for Cursor<L> {
    fn eq(&self, other: &Self) -> bool {
        self.token == other.token
    }
}

impl<L> Eq for Cursor<L> {}

/// One page of list `L`: the first page when `after` is `None`, otherwise
/// the page after the cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageRequest<L> {
    pub size: PageSize,
    pub after: Option<Cursor<L>>,
}

/// One page of list `L`, in the list's order.
///
/// Built only through [`Page::last`] and [`Page::more`]: a page holds at most
/// the requested size, and a page with a next cursor is non-empty, so a
/// client following cursors always makes progress. `next` is `None` exactly
/// on the last page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page<T, L> {
    items: Vec<T>,
    next: Option<Cursor<L>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageOverflow {
    pub size: PageSize,
    pub got: usize,
}

impl<T, L> Page<T, L> {
    /// The final page of a traversal. May be empty.
    pub fn last(size: PageSize, items: Vec<T>) -> Result<Self, PageOverflow> {
        Self::checked(size, items, None)
    }

    /// A page with more to follow.
    pub fn more(size: PageSize, items: NonEmpty<T>, next: Cursor<L>) -> Result<Self, PageOverflow> {
        Self::checked(size, items.into_vec(), Some(next))
    }

    fn checked(
        size: PageSize,
        items: Vec<T>,
        next: Option<Cursor<L>>,
    ) -> Result<Self, PageOverflow> {
        if items.len() > usize::from(size.get().get()) {
            return Err(PageOverflow {
                size,
                got: items.len(),
            });
        }
        Ok(Self { items, next })
    }

    pub fn items(&self) -> &[T] {
        &self.items
    }

    pub fn next(&self) -> Option<&Cursor<L>> {
        self.next.as_ref()
    }

    pub fn into_parts(self) -> (Vec<T>, Option<Cursor<L>>) {
        (self.items, self.next)
    }
}
