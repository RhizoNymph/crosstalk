//! The canonical text of a [`Json`] value (RFC 8785 but for numbers).
//!
//! - No whitespace between tokens.
//! - Object members sorted by their names' UTF-16 code units (RFC 8785
//!   section 3.2.3), so `"\u{1F600}"` (a surrogate pair from 0xD83D) sorts
//!   before `"\u{FB33}"`.
//! - Strings escaped as ECMAScript's `JSON.stringify` does: `\"`, `\\`,
//!   `\b`, `\f`, `\n`, `\r`, `\t`, other control characters as `\u00xx` in
//!   lower-case hex, and every other character written as itself.
//! - Numbers as [`super::Number::canonical`].

use super::Json;

pub(super) fn write(value: &Json, out: &mut String) {
    match value {
        Json::Null => out.push_str("null"),
        Json::Bool(true) => out.push_str("true"),
        Json::Bool(false) => out.push_str("false"),
        Json::Number(number) => out.push_str(&number.canonical()),
        Json::String(text) => write_string(text, out),
        Json::Array(items) => {
            out.push('[');
            for (at, item) in items.iter().enumerate() {
                if at > 0 {
                    out.push(',');
                }
                write(item, out);
            }
            out.push(']');
        }
        Json::Object(members) => {
            let mut sorted: Vec<&(String, Json)> = members.iter().collect();
            sorted.sort_by(|(a, _), (b, _)| a.encode_utf16().cmp(b.encode_utf16()));
            out.push('{');
            for (at, (name, member)) in sorted.into_iter().enumerate() {
                if at > 0 {
                    out.push(',');
                }
                write_string(name, out);
                out.push(':');
                write(member, out);
            }
            out.push('}');
        }
    }
}

pub(super) fn write_string(text: &str, out: &mut String) {
    out.push('"');
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{0}'..='\u{1f}' => {
                out.push_str(&format!("\\u{:04x}", u32::from(ch)));
            }
            _ => out.push(ch),
        }
    }
    out.push('"');
}
