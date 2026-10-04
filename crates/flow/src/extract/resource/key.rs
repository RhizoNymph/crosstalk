//! Keys of resources behind MCP tools (a wiki page title, a document path):
//! the canonical form two agents' calls must agree on.
//!
//! What counts as the same key is the server's rule, so it is configured per
//! tool argument ([`KeyCanon`]). Surrounding whitespace is always removed.

use serde::{Deserialize, Serialize};

/// How a key is folded before it becomes a locator's target. Every option
/// defaults to off: a key is then its text, trimmed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct KeyCanon {
    /// Lower-case the key (Unicode lowercase): `Home` and `home` are one
    /// page.
    #[serde(default)]
    pub fold_case: bool,
    /// Every run of whitespace and `_` becomes one space, as wikis that
    /// treat `Release_Notes` and `Release  notes` alike do.
    #[serde(default)]
    pub fold_separators: bool,
    /// The key is a `/`-separated path: empty segments and a leading or
    /// trailing `/` are dropped, each segment is folded on its own, `.`
    /// segments are dropped and `..` removes the segment before it.
    #[serde(default)]
    pub path_like: bool,
}

/// Why a key cannot name a resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum KeyError {
    #[error("the key is empty once folded")]
    Empty,
}

impl KeyCanon {
    /// `raw` in canonical form. Idempotent: the canonical form of a
    /// canonical key is itself.
    pub fn canonical(self, raw: &str) -> Result<String, KeyError> {
        let key = if self.path_like {
            let mut segments: Vec<String> = Vec::new();
            for segment in raw.split('/') {
                match self.fold_segment(segment).as_str() {
                    "" | "." => {}
                    ".." => {
                        segments.pop();
                    }
                    _ => segments.push(self.fold_segment(segment)),
                }
            }
            segments.join("/")
        } else {
            self.fold_segment(raw)
        };
        if key.is_empty() {
            return Err(KeyError::Empty);
        }
        Ok(key)
    }

    fn fold_segment(self, raw: &str) -> String {
        let trimmed = raw.trim();
        let folded = if self.fold_separators {
            trimmed
                .split(|c: char| c.is_whitespace() || c == '_')
                .filter(|word| !word.is_empty())
                .collect::<Vec<_>>()
                .join(" ")
        } else {
            trimmed.to_owned()
        };
        if self.fold_case {
            folded.to_lowercase()
        } else {
            folded
        }
    }
}
