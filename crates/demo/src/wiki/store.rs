//! The wiki's pages, in memory. Pure: the server's one task owns it.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::protocol::PageSlug;

/// Who wrote a page: the `x-wiki-author` header, 1 to 64 visible ASCII
/// characters, or `anonymous`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct Author(String);

impl Author {
    pub const MAX_LEN: usize = 64;

    /// `text` when it is 1 to 64 visible ASCII characters.
    pub fn new(text: &str) -> Option<Self> {
        let ok = !text.is_empty()
            && text.len() <= Self::MAX_LEN
            && text.bytes().all(|b| b.is_ascii_graphic());
        ok.then(|| Self(text.to_owned()))
    }

    pub fn anonymous() -> Self {
        Self("anonymous".to_owned())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One page: its current text, how many times it was written, and by whom
/// last.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page {
    pub text: String,
    pub version: u64,
    pub author: Author,
}

/// A page in the listing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Summary {
    pub page: String,
    pub version: u64,
    pub bytes: usize,
    pub author: Author,
}

/// What a write did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Written {
    pub version: u64,
    pub created: bool,
}

/// Why a write was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WriteError {
    #[error("page text of {size} bytes is over the {limit}-byte limit")]
    TooLarge { size: usize, limit: usize },
    #[error("the wiki is full ({limit} pages)")]
    Full { limit: usize },
}

/// Every page, by name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Wiki {
    pages: BTreeMap<PageSlug, Page>,
    max_page_bytes: usize,
    max_pages: usize,
}

impl Wiki {
    pub fn new(max_page_bytes: usize, max_pages: usize) -> Self {
        Self {
            pages: BTreeMap::new(),
            max_page_bytes,
            max_pages,
        }
    }

    /// Creates or replaces `slug`; versions count from 1.
    pub fn put(
        &mut self,
        slug: PageSlug,
        text: String,
        author: Author,
    ) -> Result<Written, WriteError> {
        if text.len() > self.max_page_bytes {
            return Err(WriteError::TooLarge {
                size: text.len(),
                limit: self.max_page_bytes,
            });
        }
        if let Some(page) = self.pages.get_mut(&slug) {
            page.version += 1;
            page.text = text;
            page.author = author;
            return Ok(Written {
                version: page.version,
                created: false,
            });
        }
        if self.pages.len() >= self.max_pages {
            return Err(WriteError::Full {
                limit: self.max_pages,
            });
        }
        self.pages.insert(
            slug,
            Page {
                text,
                version: 1,
                author,
            },
        );
        Ok(Written {
            version: 1,
            created: true,
        })
    }

    pub fn get(&self, slug: &PageSlug) -> Option<&Page> {
        self.pages.get(slug)
    }

    /// Every page, by name.
    pub fn list(&self) -> Vec<Summary> {
        self.pages
            .iter()
            .map(|(slug, page)| Summary {
                page: slug.to_string(),
                version: page.version,
                bytes: page.text.len(),
                author: page.author.clone(),
            })
            .collect()
    }

    pub fn len(&self) -> usize {
        self.pages.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }
}
