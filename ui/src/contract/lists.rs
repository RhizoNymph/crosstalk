//! Cursor pagination for every list query (item 1).

use std::num::NonZeroU32;

/// An opaque position in a list, issued by the backend.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Cursor(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageRequest {
    /// `None` starts from the beginning.
    pub cursor: Option<Cursor>,
    pub limit: NonZeroU32,
}

impl PageRequest {
    pub fn first(limit: NonZeroU32) -> Self {
        Self {
            cursor: None,
            limit,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Page<T> {
    pub items: Vec<T>,
    /// `None` on the last page.
    pub next: Option<Cursor>,
}
