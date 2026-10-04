//! Search by text (item 23). The gateway embeds the text for semantic and
//! hybrid modes, so the UI never handles embeddings.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SearchMode {
    Text,
    Semantic,
    #[default]
    Hybrid,
}

/// A trimmed, non-empty query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchText(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("search text is empty")]
pub struct EmptySearch;

impl SearchText {
    pub fn new(raw: &str) -> Result<Self, EmptySearch> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(EmptySearch);
        }
        Ok(Self(trimmed.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchRequest {
    pub text: SearchText,
    pub mode: SearchMode,
}
