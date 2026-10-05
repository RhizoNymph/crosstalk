//! A strict RFC 8259 parser into [`Json`], keeping numbers exact.
//!
//! Strict: no comments, no trailing commas, no leading zeros, no raw
//! control characters in strings, no unpaired surrogate escapes (a Rust
//! string cannot hold one), nothing after the value but whitespace, and at
//! most [`MAX_DEPTH`] nested arrays and objects. A repeated object member
//! keeps its last value, as `JSON.parse` (which RFC 8785 builds on) does.

use std::collections::HashMap;
use std::fmt;

use super::Json;
use super::number::Number;

/// How deeply arrays and objects may nest.
pub const MAX_DEPTH: usize = 256;

/// Why text is not JSON this parser accepts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonError {
    NotUtf8 { offset: usize },
    UnexpectedEnd,
    Unexpected { offset: usize },
    Trailing { offset: usize },
    TooDeep { offset: usize },
    InvalidEscape { offset: usize },
    LoneSurrogate { offset: usize },
    ControlInString { offset: usize },
    ExponentOutOfRange { offset: usize },
}

impl fmt::Display for JsonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotUtf8 { offset } => write!(f, "not UTF-8 at byte {offset}"),
            Self::UnexpectedEnd => f.write_str("unexpected end of input"),
            Self::Unexpected { offset } => write!(f, "unexpected byte at offset {offset}"),
            Self::Trailing { offset } => write!(f, "content after the value at offset {offset}"),
            Self::TooDeep { offset } => {
                write!(f, "nested deeper than {MAX_DEPTH} at offset {offset}")
            }
            Self::InvalidEscape { offset } => write!(f, "invalid escape at offset {offset}"),
            Self::LoneSurrogate { offset } => {
                write!(f, "unpaired surrogate escape at offset {offset}")
            }
            Self::ControlInString { offset } => {
                write!(f, "raw control character in a string at offset {offset}")
            }
            Self::ExponentOutOfRange { offset } => {
                write!(f, "number exponent out of range at offset {offset}")
            }
        }
    }
}

impl std::error::Error for JsonError {}

pub(super) fn parse_bytes(bytes: &[u8]) -> Result<Json, JsonError> {
    let text = std::str::from_utf8(bytes).map_err(|error| JsonError::NotUtf8 {
        offset: error.valid_up_to(),
    })?;
    parse(text)
}

pub(super) fn parse(text: &str) -> Result<Json, JsonError> {
    let mut parser = Parser { text, at: 0 };
    parser.skip_whitespace();
    let value = parser.value(0)?;
    parser.skip_whitespace();
    if parser.at < text.len() {
        return Err(JsonError::Trailing { offset: parser.at });
    }
    Ok(value)
}

struct Parser<'a> {
    text: &'a str,
    at: usize,
}

impl Parser<'_> {
    fn peek(&self) -> Option<u8> {
        self.text.as_bytes().get(self.at).copied()
    }

    fn next(&mut self) -> Result<u8, JsonError> {
        let byte = self.peek().ok_or(JsonError::UnexpectedEnd)?;
        self.at += 1;
        Ok(byte)
    }

    fn unexpected(&self) -> JsonError {
        if self.at >= self.text.len() {
            JsonError::UnexpectedEnd
        } else {
            JsonError::Unexpected { offset: self.at }
        }
    }

    fn expect(&mut self, byte: u8) -> Result<(), JsonError> {
        if self.peek() == Some(byte) {
            self.at += 1;
            Ok(())
        } else {
            Err(self.unexpected())
        }
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.at += 1;
        }
    }

    fn literal(&mut self, word: &str, value: Json) -> Result<Json, JsonError> {
        if self.text[self.at..].starts_with(word) {
            self.at += word.len();
            Ok(value)
        } else {
            Err(self.unexpected())
        }
    }

    fn value(&mut self, depth: usize) -> Result<Json, JsonError> {
        match self.peek() {
            None => Err(JsonError::UnexpectedEnd),
            Some(b'n') => self.literal("null", Json::Null),
            Some(b't') => self.literal("true", Json::Bool(true)),
            Some(b'f') => self.literal("false", Json::Bool(false)),
            Some(b'"') => self.string().map(Json::String),
            Some(b'[') => self.array(depth + 1),
            Some(b'{') => self.object(depth + 1),
            Some(b'-' | b'0'..=b'9') => self.number(),
            Some(_) => Err(self.unexpected()),
        }
    }

    fn array(&mut self, depth: usize) -> Result<Json, JsonError> {
        if depth > MAX_DEPTH {
            return Err(JsonError::TooDeep { offset: self.at });
        }
        self.expect(b'[')?;
        let mut items = Vec::new();
        self.skip_whitespace();
        if self.peek() == Some(b']') {
            self.at += 1;
            return Ok(Json::Array(items));
        }
        loop {
            self.skip_whitespace();
            items.push(self.value(depth)?);
            self.skip_whitespace();
            match self.next()? {
                b',' => {}
                b']' => return Ok(Json::Array(items)),
                _ => {
                    self.at -= 1;
                    return Err(self.unexpected());
                }
            }
        }
    }

    fn object(&mut self, depth: usize) -> Result<Json, JsonError> {
        if depth > MAX_DEPTH {
            return Err(JsonError::TooDeep { offset: self.at });
        }
        self.expect(b'{')?;
        let mut members: Vec<(String, Json)> = Vec::new();
        // Member positions by name, for repeated names; only looked up,
        // never iterated, so the parse stays deterministic.
        let mut positions: HashMap<String, usize> = HashMap::new();
        self.skip_whitespace();
        if self.peek() == Some(b'}') {
            self.at += 1;
            return Ok(Json::Object(members));
        }
        loop {
            self.skip_whitespace();
            if self.peek() != Some(b'"') {
                return Err(self.unexpected());
            }
            let key = self.string()?;
            self.skip_whitespace();
            self.expect(b':')?;
            self.skip_whitespace();
            let value = self.value(depth)?;
            match positions.get(&key) {
                Some(&at) => members[at].1 = value,
                None => {
                    positions.insert(key.clone(), members.len());
                    members.push((key, value));
                }
            }
            self.skip_whitespace();
            match self.next()? {
                b',' => {}
                b'}' => return Ok(Json::Object(members)),
                _ => {
                    self.at -= 1;
                    return Err(self.unexpected());
                }
            }
        }
    }

    fn string(&mut self) -> Result<String, JsonError> {
        self.expect(b'"')?;
        let mut out = String::new();
        let mut run = self.at;
        loop {
            let byte = self.peek().ok_or(JsonError::UnexpectedEnd)?;
            match byte {
                b'"' => {
                    out.push_str(&self.text[run..self.at]);
                    self.at += 1;
                    return Ok(out);
                }
                b'\\' => {
                    out.push_str(&self.text[run..self.at]);
                    self.escape(&mut out)?;
                    run = self.at;
                }
                0x00..=0x1f => return Err(JsonError::ControlInString { offset: self.at }),
                // Every other byte, including the bytes of a multi-byte
                // character, is copied with its run; runs start and end
                // on ASCII bytes, so they are whole characters.
                _ => self.at += 1,
            }
        }
    }

    fn escape(&mut self, out: &mut String) -> Result<(), JsonError> {
        let start = self.at;
        self.at += 1;
        let byte = self.next()?;
        let decoded = match byte {
            b'"' => '"',
            b'\\' => '\\',
            b'/' => '/',
            b'b' => '\u{8}',
            b'f' => '\u{c}',
            b'n' => '\n',
            b'r' => '\r',
            b't' => '\t',
            b'u' => return self.unicode_escape(start, out),
            _ => return Err(JsonError::InvalidEscape { offset: start }),
        };
        out.push(decoded);
        Ok(())
    }

    fn hex4(&mut self, start: usize) -> Result<u16, JsonError> {
        let mut value: u16 = 0;
        for _ in 0..4 {
            let digit = match self.next()? {
                byte @ b'0'..=b'9' => byte - b'0',
                byte @ b'a'..=b'f' => byte - b'a' + 10,
                byte @ b'A'..=b'F' => byte - b'A' + 10,
                _ => return Err(JsonError::InvalidEscape { offset: start }),
            };
            value = value * 16 + u16::from(digit);
        }
        Ok(value)
    }

    fn unicode_escape(&mut self, start: usize, out: &mut String) -> Result<(), JsonError> {
        let first = self.hex4(start)?;
        let code = match first {
            0xd800..=0xdbff => {
                if !self.text[self.at..].starts_with("\\u") {
                    return Err(JsonError::LoneSurrogate { offset: start });
                }
                let second_start = self.at;
                self.at += 2;
                let second = self.hex4(second_start)?;
                if !(0xdc00..=0xdfff).contains(&second) {
                    return Err(JsonError::LoneSurrogate { offset: start });
                }
                0x10000 + ((u32::from(first) - 0xd800) << 10) + (u32::from(second) - 0xdc00)
            }
            0xdc00..=0xdfff => return Err(JsonError::LoneSurrogate { offset: start }),
            other => u32::from(other),
        };
        let decoded = char::from_u32(code).ok_or(JsonError::InvalidEscape { offset: start })?;
        out.push(decoded);
        Ok(())
    }

    fn digits(&mut self) -> &str {
        let start = self.at;
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.at += 1;
        }
        &self.text[start..self.at]
    }

    fn number(&mut self) -> Result<Json, JsonError> {
        let start = self.at;
        let negative = self.peek() == Some(b'-');
        if negative {
            self.at += 1;
        }
        let int_start = self.at;
        let int = self.digits().to_owned();
        if int.is_empty() || (int.len() > 1 && int.starts_with('0')) {
            self.at = int_start;
            return Err(self.unexpected());
        }
        let mut frac = String::new();
        if self.peek() == Some(b'.') {
            self.at += 1;
            frac = self.digits().to_owned();
            if frac.is_empty() {
                return Err(self.unexpected());
            }
        }
        let mut exp_negative = false;
        let mut exp = String::new();
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.at += 1;
            match self.peek() {
                Some(b'-') => {
                    exp_negative = true;
                    self.at += 1;
                }
                Some(b'+') => self.at += 1,
                _ => {}
            }
            exp = self.digits().to_owned();
            if exp.is_empty() {
                return Err(self.unexpected());
            }
        }
        Number::from_literal(negative, &int, &frac, exp_negative, &exp)
            .map(Json::Number)
            .map_err(|_| JsonError::ExponentOutOfRange { offset: start })
    }
}
