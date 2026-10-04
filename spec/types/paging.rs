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
//! | transmissions by id | [`TransmissionList`] | `TransmissionId` |
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
//! window; for transmissions by id the selection and the version its first
//! page resolved). The server
//! authenticates the token it issues; one it cannot verify, or one presented
//! with a different request, is rejected as an invalid cursor. The marker
//! type parameter makes presenting one list's cursor to another list a
//! compile error.
//!
//! A valid cursor whose pinned topic-model version or embedding model is no
//! longer available is not an invalid cursor: the next page fails with the
//! typed reason (`VersionNotRetained`, `Conflict(EmbeddingModelChanged)`).
//!
//! On the wire ([`crate::wire`]) a [`PageSize`] is a number, a [`Cursor`]
//! its token string (the list marker is not on the wire: the server's MAC
//! binds the token to its list and request), a [`PageRequest`]
//! `{"size": 50, "after": null}` and a [`Page`] `{"items": [..], "next":
//! null}`. A page request is a [`WireRequest`].

use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroU16;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::support::NonEmpty;
use crate::wire::{Rejected, WireRequest, decode_text};

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
    /// `QueryApi::transmissions_by_id`.
    TransmissionList;
}

/// How many items a page may hold: `1..=PageSize::MAX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "u16", into = "u16")]
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

impl TryFrom<u16> for PageSize {
    type Error = Rejected<InvalidPageSize>;

    fn try_from(size: u16) -> Result<Self, Self::Error> {
        Self::new(size).map_err(|error| Rejected::new("page size", error))
    }
}

impl From<PageSize> for u16 {
    fn from(size: PageSize) -> Self {
        size.0.get()
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

// Manual impls: a derive would require `L: Serialize`, and the marker is not
// part of the wire form.
impl<L> Serialize for Cursor<L> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.token)
    }
}

/// A token [`Cursor::from_token`] accepts. Whether the server issued it,
/// for this list and request, is checked when the page is read
/// (`QueryError::InvalidCursor`), not when it is decoded.
impl<'de, L> Deserialize<'de> for Cursor<L> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        decode_text(deserializer, "cursor token", Self::from_token)
    }
}

/// One page of list `L`: the first page when `after` is `None`, otherwise
/// the page after the cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    rename_all = "snake_case",
    deny_unknown_fields,
    bound(serialize = "", deserialize = "")
)]
pub struct PageRequest<L> {
    pub size: PageSize,
    pub after: Option<Cursor<L>>,
}

impl<L> WireRequest for PageRequest<L> {}

/// One page of list `L`, in the list's order.
///
/// Built only through [`Page::last`] and [`Page::more`]: a page holds at most
/// the requested size, and a page with a next cursor is non-empty, so a
/// client following cursors always makes progress. `next` is `None` exactly
/// on the last page.
///
/// Decoding checks what a page knows about itself: at most
/// [`PageSize::MAX`] items, and items whenever there is a next cursor
/// ([`InvalidPage`]). The size it was requested with is not on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    rename_all = "snake_case",
    try_from = "RawPage<T, L>",
    bound(serialize = "T: Serialize", deserialize = "T: Deserialize<'de>")
)]
pub struct Page<T, L> {
    items: Vec<T>,
    next: Option<Cursor<L>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageOverflow {
    pub size: PageSize,
    pub got: usize,
}

/// Why a decoded page cannot be one the surface served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidPage {
    /// More items than the largest page.
    TooManyItems { max: u16, got: usize },
    /// A next cursor on an empty page: a client following it would make no
    /// progress.
    EmptyWithNext,
}

#[derive(Deserialize)]
#[serde(
    rename_all = "snake_case",
    deny_unknown_fields,
    bound(deserialize = "T: Deserialize<'de>")
)]
struct RawPage<T, L> {
    items: Vec<T>,
    next: Option<Cursor<L>>,
}

impl<T, L> TryFrom<RawPage<T, L>> for Page<T, L> {
    type Error = Rejected<InvalidPage>;

    fn try_from(raw: RawPage<T, L>) -> Result<Self, Self::Error> {
        let rejected = |error| Rejected::new("page", error);
        if raw.items.len() > usize::from(PageSize::MAX) {
            return Err(rejected(InvalidPage::TooManyItems {
                max: PageSize::MAX,
                got: raw.items.len(),
            }));
        }
        if raw.next.is_some() && raw.items.is_empty() {
            return Err(rejected(InvalidPage::EmptyWithNext));
        }
        Ok(Self {
            items: raw.items,
            next: raw.next,
        })
    }
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
