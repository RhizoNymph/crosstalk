//! How the spliced content is treated before the reader sees it, and the
//! match each treatment needs.
//!
//! Needs are relative to the written file's text (the value of the
//! sender's `file_text` or heredoc body), as the task that defines the
//! corpus does:
//!
//! | Variant | The file the reader views holds | Needs |
//! | --- | --- | --- |
//! | `exact` | the text | `Exact` |
//! | `whitespace` | the text with indentation as tabs, spaces doubled, two trailing spaces per line | `Normalized` |
//! | `json_string` | the text as one JSON string literal | `Normalized` (interim) |
//! | `base64` | the text base64-encoded, on one line | `Decoded([Base64])` |
//!
//! A shell read (`cat -n` through a harness that returns JSON) delivers the
//! view inside a JSON string: one more layer of string escaping, so an
//! `exact` view needs `Normalized` there too.
//!
//! TODO(docs/spec-eval-gaps): `json_string` (and the shell read's escaping)
//! becomes `Decoded([JsonString])` once the spec has `Codec::JsonString`.

use std::fmt;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use crosstalk_spec::derived::provenance::matching::Codec;
use serde::{Deserialize, Serialize};

use super::read::ReadForm;
use crate::truth::MatchNeed;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Variant {
    Exact,
    Whitespace,
    JsonString,
    Base64,
}

impl Variant {
    pub const ALL: [Self; 4] = [
        Self::Exact,
        Self::Whitespace,
        Self::JsonString,
        Self::Base64,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Whitespace => "whitespace",
            Self::JsonString => "json_string",
            Self::Base64 => "base64",
        }
    }

    /// The file text the reader views.
    pub fn render(self, content: &str) -> String {
        match self {
            Self::Exact => content.to_owned(),
            Self::Whitespace => perturb(content),
            Self::JsonString => {
                serde_json::to_string(content).unwrap_or_else(|_| format!("{content:?}"))
            }
            Self::Base64 => STANDARD.encode(content.as_bytes()),
        }
    }

    /// The weakest match the reader's view needs, read through `form`.
    pub fn needs(self, form: ReadForm) -> MatchNeed {
        match (self, form) {
            (Self::Base64, _) => MatchNeed::Decoded {
                codecs: vec![Codec::Base64],
            },
            (Self::Exact, ReadForm::EditorView { .. }) => MatchNeed::Exact,
            (Self::Exact | Self::Whitespace | Self::JsonString, _) => MatchNeed::Normalized,
        }
    }
}

impl fmt::Display for Variant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// `content` with its whitespace changed but nothing else: leading groups
/// of four spaces become tabs, every other space is doubled, and each
/// non-empty line ends in two spaces. Equal to `content` after folding,
/// never byte-equal to a non-empty line of it.
pub fn perturb(content: &str) -> String {
    let mut out = String::with_capacity(content.len() * 2);
    for line in content.split_inclusive('\n') {
        let (body, newline) = match line.strip_suffix('\n') {
            Some(body) => (body, "\n"),
            None => (line, ""),
        };
        let indent = body.len() - body.trim_start_matches(' ').len();
        let rest = &body[indent..];
        for _ in 0..indent / 4 {
            out.push('\t');
        }
        for _ in 0..indent % 4 {
            out.push(' ');
        }
        for ch in rest.chars() {
            if ch == ' ' {
                out.push_str("  ");
            } else {
                out.push(ch);
            }
        }
        if !body.is_empty() {
            out.push_str("  ");
        }
        out.push_str(newline);
    }
    out
}
