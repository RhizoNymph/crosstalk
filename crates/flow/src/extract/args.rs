//! Tool-call arguments: parsing the canonical JSON and reading named
//! arguments out of it.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crosstalk_spec::observed::message::ToolArguments;

/// Why a call's arguments do not fit the tool's schema. Becomes
/// `ExtractError::Arguments`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ArgError {
    #[error("the arguments are not valid JSON")]
    InvalidJson,
    #[error("the arguments are not a JSON object")]
    NotAnObject,
    #[error("missing argument `{0}`")]
    Missing(String),
    #[error("argument `{0}` is not a string")]
    NotAString(String),
    #[error("argument `{argument}`: {reason}")]
    Invalid { argument: String, reason: String },
}

impl ArgError {
    pub fn invalid(argument: &str, reason: impl ToString) -> Self {
        Self::Invalid {
            argument: argument.to_owned(),
            reason: reason.to_string(),
        }
    }
}

/// A call's arguments, parsed: always a JSON object.
#[derive(Debug, Clone, PartialEq)]
pub struct Args(serde_json::Map<String, Value>);

impl Args {
    pub fn parse(arguments: &ToolArguments) -> Result<Self, ArgError> {
        let text = match arguments {
            ToolArguments::Json(json) => &json.0,
            ToolArguments::Invalid(_) => return Err(ArgError::InvalidJson),
        };
        match serde_json::from_str(text) {
            Ok(Value::Object(map)) => Ok(Self(map)),
            Ok(_) => Err(ArgError::NotAnObject),
            Err(_) => Err(ArgError::InvalidJson),
        }
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0.get(key)
    }

    /// The first of `keys` present, as a string.
    pub fn first_str<'a>(&'a self, keys: &[&str]) -> Result<&'a str, ArgError> {
        let (key, value) = keys
            .iter()
            .find_map(|key| self.0.get(*key).map(|value| (*key, value)))
            .ok_or_else(|| ArgError::Missing(keys.join(" or ")))?;
        value
            .as_str()
            .ok_or_else(|| ArgError::NotAString(key.to_owned()))
    }

    /// `key` as a string, `None` when absent.
    pub fn opt_str<'a>(&'a self, key: &str) -> Result<Option<&'a str>, ArgError> {
        match self.0.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(text)) => Ok(Some(text)),
            Some(_) => Err(ArgError::NotAString(key.to_owned())),
        }
    }

    /// The value at `path`, as text: a string as it is, a number as its
    /// decimal text. `None` when absent.
    pub fn text_at(&self, path: &ArgPath) -> Result<Option<String>, ArgError> {
        // An ArgPath starts with `/`: its first token names a top-level
        // argument and the rest points inside it.
        let pointer = &path.as_str()[1..];
        let (first, rest) = match pointer.find('/') {
            Some(at) => (&pointer[..at], &pointer[at..]),
            None => (pointer, ""),
        };
        let first = first.replace("~1", "/").replace("~0", "~");
        match self.0.get(&first).and_then(|value| value.pointer(rest)) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(text)) => Ok(Some(text.clone())),
            Some(Value::Number(number)) => Ok(Some(number.to_string())),
            Some(_) => Err(ArgError::NotAString(path.as_str().to_owned())),
        }
    }
}

/// Where an argument sits in a call's arguments: a JSON Pointer (RFC 6901),
/// `/title` or `/page/path`. Decoded only through [`ArgPath::new`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ArgPath(String);

/// Why text is not an [`ArgPath`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidArgPath {
    #[error("an argument path starts with `/`")]
    NoLeadingSlash,
    #[error("`~` in an argument path is followed by `0` or `1`")]
    BadEscape,
}

impl ArgPath {
    pub fn new(text: impl Into<String>) -> Result<Self, InvalidArgPath> {
        let text = text.into();
        if !text.starts_with('/') {
            return Err(InvalidArgPath::NoLeadingSlash);
        }
        let mut chars = text.chars();
        while let Some(c) = chars.next() {
            if c == '~' && !matches!(chars.next(), Some('0' | '1')) {
                return Err(InvalidArgPath::BadEscape);
            }
        }
        Ok(Self(text))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ArgPath {
    type Error = InvalidArgPath;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        Self::new(text)
    }
}

impl From<ArgPath> for String {
    fn from(path: ArgPath) -> Self {
        path.0
    }
}
