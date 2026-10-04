//! The top-level shape of a request body, for diagnosing a body the
//! normalizer refused without keeping it.
//!
//! A [`RequestShape`] holds the body's top-level keys, the kind of its
//! `system` field, and each `messages` entry's role and content kind (a
//! string, or an array and its blocks' `type`s). It never holds a value:
//! no text, no tool input, no header. Names (keys, roles, block types) are
//! client strings too, so one is kept only when it looks like a protocol
//! identifier ([`Label`]); anything else is withheld, so a shape is safe
//! to log however odd the body.

use std::fmt;

use crosstalk_spec::observed::message::json::Json;

/// The longest name a [`Label`] keeps.
pub const MAX_LABEL: usize = 48;

/// A client-supplied name (a key, a role, a block type), kept only when it
/// is at most [`MAX_LABEL`] bytes of ASCII letters, digits, `_`, `-` and
/// `.`: protocol identifiers (the longest block type is under 40 bytes)
/// are; prompt text, and API keys and tokens (longer, or with other
/// characters), are not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Label {
    Name(String),
    /// Not an identifier: withheld, only its length kept.
    Withheld {
        bytes: usize,
    },
}

impl Label {
    pub fn new(name: &str) -> Self {
        let identifier = !name.is_empty()
            && name.len() <= MAX_LABEL
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'));
        if identifier {
            Self::Name(name.to_owned())
        } else {
            Self::Withheld { bytes: name.len() }
        }
    }
}

impl fmt::Display for Label {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Name(name) => f.write_str(name),
            Self::Withheld { bytes } => write!(f, "<{bytes} bytes>"),
        }
    }
}

/// The kind of a JSON value, without the value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonKind {
    Null,
    Bool,
    Number,
    String,
    Array,
    Object,
}

impl JsonKind {
    pub fn of(value: &Json) -> Self {
        match value {
            Json::Null => Self::Null,
            Json::Bool(_) => Self::Bool,
            Json::Number(_) => Self::Number,
            Json::String(_) => Self::String,
            Json::Array(_) => Self::Array,
            Json::Object(_) => Self::Object,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Bool => "bool",
            Self::Number => "number",
            Self::String => "string",
            Self::Array => "array",
            Self::Object => "object",
        }
    }
}

impl fmt::Display for JsonKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// What a request body looks like at the top level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestShape {
    NotJson,
    NotAnObject(JsonKind),
    Object {
        /// The top-level keys, in body order.
        keys: Vec<Label>,
        /// The kind of `system`; `None` when absent.
        system: Option<JsonKind>,
        messages: MessagesShape,
    },
}

/// What `messages` looks like.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessagesShape {
    Missing,
    NotAnArray(JsonKind),
    Turns(Vec<TurnShape>),
}

/// One `messages` entry: its role and its content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnShape {
    NotAnObject(JsonKind),
    Turn {
        role: RoleShape,
        content: ContentShape,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleShape {
    Missing,
    NotAString(JsonKind),
    Role(Label),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentShape {
    Missing,
    Text,
    /// An array: each block's `type`, `None` for an item that has none.
    Blocks(Vec<Option<Label>>),
    Other(JsonKind),
}

impl RequestShape {
    /// The shape of `body`.
    pub fn of(body: &[u8]) -> Self {
        let Ok(request) = Json::parse_bytes(body) else {
            return Self::NotJson;
        };
        let Json::Object(members) = &request else {
            return Self::NotAnObject(JsonKind::of(&request));
        };
        let messages = match request.get("messages") {
            None => MessagesShape::Missing,
            Some(Json::Array(turns)) => MessagesShape::Turns(turns.iter().map(turn).collect()),
            Some(other) => MessagesShape::NotAnArray(JsonKind::of(other)),
        };
        Self::Object {
            keys: members.iter().map(|(key, _)| Label::new(key)).collect(),
            system: request.get("system").map(JsonKind::of),
            messages,
        }
    }
}

fn turn(value: &Json) -> TurnShape {
    if !value.is_object() {
        return TurnShape::NotAnObject(JsonKind::of(value));
    }
    let role = match value.get("role") {
        None => RoleShape::Missing,
        Some(Json::String(role)) => RoleShape::Role(Label::new(role)),
        Some(other) => RoleShape::NotAString(JsonKind::of(other)),
    };
    let content = match value.get("content") {
        None => ContentShape::Missing,
        Some(Json::String(_)) => ContentShape::Text,
        Some(Json::Array(blocks)) => ContentShape::Blocks(
            blocks
                .iter()
                .map(|block| block.kind().map(Label::new))
                .collect(),
        ),
        Some(other) => ContentShape::Other(JsonKind::of(other)),
    };
    TurnShape::Turn { role, content }
}

/// `keys=[model,messages] system=array messages=[user:string,
/// system:array(text)]`, or `not json`, `not an object (array)`.
impl fmt::Display for RequestShape {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotJson => f.write_str("not json"),
            Self::NotAnObject(kind) => write!(f, "not an object ({kind})"),
            Self::Object {
                keys,
                system,
                messages,
            } => {
                f.write_str("keys=")?;
                list(f, keys, |f, key| write!(f, "{key}"))?;
                match system {
                    Some(kind) => write!(f, " system={kind}")?,
                    None => f.write_str(" system=absent")?,
                }
                write!(f, " messages={messages}")
            }
        }
    }
}

impl fmt::Display for MessagesShape {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing => f.write_str("absent"),
            Self::NotAnArray(kind) => write!(f, "{kind}"),
            Self::Turns(turns) => list(f, turns, |f, turn| write!(f, "{turn}")),
        }
    }
}

impl fmt::Display for TurnShape {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAnObject(kind) => write!(f, "<{kind}>"),
            Self::Turn { role, content } => write!(f, "{role}:{content}"),
        }
    }
}

impl fmt::Display for RoleShape {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing => f.write_str("<no role>"),
            Self::NotAString(kind) => write!(f, "<role {kind}>"),
            Self::Role(label) => write!(f, "{label}"),
        }
    }
}

impl fmt::Display for ContentShape {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing => f.write_str("<no content>"),
            Self::Text => f.write_str("string"),
            Self::Blocks(blocks) => {
                f.write_str("array(")?;
                for (at, block) in blocks.iter().enumerate() {
                    if at > 0 {
                        f.write_str(",")?;
                    }
                    match block {
                        Some(label) => write!(f, "{label}")?,
                        None => f.write_str("<untyped>")?,
                    }
                }
                f.write_str(")")
            }
            Self::Other(kind) => write!(f, "{kind}"),
        }
    }
}

/// `[a,b,c]`, each item written by `item`.
fn list<T>(
    f: &mut fmt::Formatter<'_>,
    items: &[T],
    item: impl Fn(&mut fmt::Formatter<'_>, &T) -> fmt::Result,
) -> fmt::Result {
    f.write_str("[")?;
    for (at, value) in items.iter().enumerate() {
        if at > 0 {
            f.write_str(",")?;
        }
        item(f, value)?;
    }
    f.write_str("]")
}
