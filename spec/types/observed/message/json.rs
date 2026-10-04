//! JSON with exact numbers, and its canonical text.
//!
//! [`CanonicalJson`] is RFC 8785 text except that numbers keep
//! their exact decimal value instead of going through an IEEE double, so an
//! integer id beyond 2^53 in a tool's arguments keeps every digit
//! (`canonical.json.rfc8785-form`, `canonical.json.large-integers-exact`).
//! `serde_json` would round such numbers (without its crate-wide
//! `arbitrary_precision` feature, which changes number handling for every
//! crate in the workspace), so this module has its own parser
//! ([`Json::parse`]), value ([`Json`], numbers as [`Number`]) and writer
//! ([`Json::canonical`]).

mod number;
mod parse;
mod write;

use super::CanonicalJson;

pub use number::{MAX_EXPONENT_DIGITS, Number};
pub use parse::{JsonError, MAX_DEPTH};

/// A JSON value with exact numbers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Json {
    Null,
    Bool(bool),
    Number(Number),
    String(String),
    Array(Vec<Json>),
    /// Members in the order their names first appeared; a repeated name
    /// holds its last value. The canonical text sorts them.
    Object(Vec<(String, Json)>),
}

impl Json {
    /// Parses JSON text strictly: RFC 8259 with no extensions, unpaired
    /// surrogate escapes refused, at most [`MAX_DEPTH`] levels deep, and a
    /// repeated member name keeping its last value.
    pub fn parse(text: &str) -> Result<Self, JsonError> {
        parse::parse(text)
    }

    /// [`Json::parse`] for bytes, which must be UTF-8.
    pub fn parse_bytes(bytes: &[u8]) -> Result<Self, JsonError> {
        parse::parse_bytes(bytes)
    }

    /// The member `name` of an object; `None` for a missing member or a
    /// value that is not an object.
    pub fn get(&self, name: &str) -> Option<&Json> {
        match self {
            Self::Object(members) => members
                .iter()
                .find(|(member, _)| member == name)
                .map(|(_, value)| value),
            _ => None,
        }
    }

    /// The string, if this is one.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(text) => Some(text),
            _ => None,
        }
    }

    /// The items, if this is an array.
    pub fn as_array(&self) -> Option<&[Json]> {
        match self {
            Self::Array(items) => Some(items),
            _ => None,
        }
    }

    pub fn is_object(&self) -> bool {
        matches!(self, Self::Object(_))
    }

    /// The `u64` this is, if it is a number that is one.
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Number(number) => number.as_u64(),
            _ => None,
        }
    }

    /// The string member `type`: a content block's or event's kind.
    pub fn kind(&self) -> Option<&str> {
        self.get("type").and_then(Self::as_str)
    }

    /// This object with member `name` set to `value` (replacing an existing
    /// one in place). A value that is not an object is returned unchanged.
    pub fn with(mut self, name: &str, value: Json) -> Self {
        if let Self::Object(members) = &mut self {
            match members.iter_mut().find(|(member, _)| member == name) {
                Some(member) => member.1 = value,
                None => members.push((name.to_owned(), value)),
            }
        }
        self
    }

    /// The canonical text of this value.
    pub fn canonical(&self) -> CanonicalJson {
        CanonicalJson(self.canonical_text())
    }

    /// [`Json::canonical`] as a plain string.
    pub fn canonical_text(&self) -> String {
        let mut out = String::new();
        write::write(self, &mut out);
        out
    }
}

/// The canonical form of JSON text.
pub fn canonicalize(text: &str) -> Result<CanonicalJson, JsonError> {
    Json::parse(text).map(|value| value.canonical())
}
