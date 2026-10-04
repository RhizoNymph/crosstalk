//! Property bodies: each takes generated input and checks its oracle. The
//! `proptest!` entry points the invariants name are in `encoding/mod.rs`.

use proptest::prelude::*;
use proptest::test_runner::TestCaseError;

use super::generate::{GenJson, GenNumber, Style};
use crate::observed::message::MessageBody;
use crate::observed::message::encoding;
use crate::observed::message::json::{Json, canonicalize};

type Checked = Result<(), TestCaseError>;

/// Decoding a body's encoding gives the body back.
pub fn encoding_round_trip(body: &MessageBody) -> Checked {
    let bytes = encoding::encode(body);
    let decoded = encoding::decode(&bytes);
    prop_assert_eq!(decoded.as_ref(), Ok(body));
    Ok(())
}

/// Bytes near a real encoding (one edit to its JSON, written canonically
/// or not) decode only when they are exactly what `encode` writes for the
/// body they decode to.
pub fn decode_only_what_encode_writes(body: &MessageBody, seed: u64) -> Checked {
    let bytes = encoding::encode(body);
    let parsed =
        Json::parse_bytes(&bytes).map_err(|error| TestCaseError::fail(error.to_string()))?;
    let mut style = Style::new(seed);
    let edited = edit(parsed, &mut style);
    let canonical = edited.canonical_text();
    let spaced = spaced(&canonical, &mut style);
    for candidate in [canonical.as_bytes(), spaced.as_bytes()] {
        if let Ok(decoded) = encoding::decode(candidate) {
            prop_assert_eq!(
                encoding::encode(&decoded),
                candidate.to_vec(),
                "decoded {:?} from bytes encode never writes for it",
                decoded
            );
        }
    }
    Ok(())
}

/// `value` with one edit somewhere in it: a member dropped, added or
/// renamed, an item dropped or repeated, a string or literal replaced.
/// Sometimes no edit, so the original's acceptance is checked too.
fn edit(value: Json, style: &mut Style) -> Json {
    let nodes = count(&value);
    let target = style.below(nodes + 1);
    let mut seen = 0;
    edit_at(value, target, &mut seen, style)
}

fn count(value: &Json) -> usize {
    1 + match value {
        Json::Array(items) => items.iter().map(count).sum(),
        Json::Object(members) => members.iter().map(|(_, member)| count(member)).sum(),
        Json::Null | Json::Bool(_) | Json::Number(_) | Json::String(_) => 0,
    }
}

fn edit_at(value: Json, target: usize, seen: &mut usize, style: &mut Style) -> Json {
    let here = *seen;
    *seen += 1;
    if here == target {
        return edit_here(value, style);
    }
    match value {
        Json::Array(items) => Json::Array(
            items
                .into_iter()
                .map(|item| edit_at(item, target, seen, style))
                .collect(),
        ),
        Json::Object(members) => Json::Object(
            members
                .into_iter()
                .map(|(name, member)| (name, edit_at(member, target, seen, style)))
                .collect(),
        ),
        leaf => leaf,
    }
}

/// Names and tags an encoding uses, so an edit often lands on another
/// valid-looking shape rather than on noise.
const WORDS: [&str; 20] = [
    "type",
    "data",
    "text",
    "visible",
    "opaque",
    "signature",
    "media",
    "unknown",
    "tool_call",
    "server_tool_result",
    "json",
    "invalid",
    "client",
    "server",
    "success",
    "error",
    "image",
    "kind",
    "raw",
    "",
];

fn word(style: &mut Style) -> String {
    WORDS[style.below(WORDS.len())].to_owned()
}

fn edit_here(value: Json, style: &mut Style) -> Json {
    match value {
        Json::Object(mut members) => {
            match style.below(3) {
                0 if !members.is_empty() => {
                    let at = style.below(members.len());
                    members.remove(at);
                }
                1 if !members.is_empty() => {
                    let at = style.below(members.len());
                    members[at].0 = word(style);
                }
                _ => members.push((word(style), Json::Null)),
            }
            Json::Object(members)
        }
        Json::Array(mut items) => {
            if !items.is_empty() && style.chance(2) {
                let at = style.below(items.len());
                items.remove(at);
            } else if let Some(first) = items.first().cloned() {
                items.push(first);
            } else {
                items.push(Json::Null);
            }
            Json::Array(items)
        }
        Json::String(_) => match style.below(3) {
            0 => Json::Null,
            1 => Json::String("{ }".to_owned()),
            _ => Json::String(word(style)),
        },
        Json::Null | Json::Bool(_) | Json::Number(_) => Json::String(word(style)),
    }
}

/// `canonical` with a space after its first comma or colon, if any: the
/// same JSON value in text that is not canonical.
fn spaced(canonical: &str, style: &mut Style) -> String {
    let separator = if style.chance(2) { ',' } else { ':' };
    canonical.replacen(separator, &format!("{separator} "), 1)
}

/// Any spelling of a value (member order, whitespace, escapes, number
/// forms) has one canonical text: the value's.
pub fn canonical_ignores_formatting(value: &GenJson, seeds: (u64, u64)) -> Checked {
    let one = value.render(&mut Style::new(seeds.0));
    let two = value.render(&mut Style::new(seeds.1));
    let canonical =
        canonicalize(&one).map_err(|error| TestCaseError::fail(format!("{one}: {error}")))?;
    prop_assert_eq!(
        &canonical,
        &canonicalize(&two).map_err(|error| TestCaseError::fail(format!("{two}: {error}")))?
    );
    prop_assert_eq!(&canonical, &value.value().canonical());
    // Canonical text is a fixed point.
    prop_assert_eq!(&canonicalize(&canonical.0).ok(), &Some(canonical.clone()));
    Ok(())
}

/// Integers far beyond 2^53, either sign, keep their exact value through
/// canonical text, and their digits while they have at most 21.
pub fn large_integer_exact(negative: bool, digits: &str, seed: u64) -> Checked {
    let number = GenNumber {
        negative,
        digits: digits.to_owned(),
        exponent: 0,
    };
    let spelled = number.spell(&mut Style::new(seed));
    let canonical =
        canonicalize(&spelled).map_err(|error| TestCaseError::fail(error.to_string()))?;
    let reparsed =
        Json::parse(&canonical.0).map_err(|error| TestCaseError::fail(error.to_string()))?;
    prop_assert_eq!(reparsed, Json::Number(number.value()));
    if digits.len() <= 21 {
        let sign = if negative { "-" } else { "" };
        prop_assert_eq!(canonical.0, format!("{sign}{digits}"));
    }
    Ok(())
}
