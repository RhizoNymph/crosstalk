//! Generators for the encoding's property tests: JSON values and their
//! many spellings, and message bodies of every variant.
//!
//! The JSON generators are the ones `crosstalk-canonical`'s tests use for
//! provider bodies, kept here for the JSON module's own properties.

use proptest::prelude::*;

use crate::ids::MessageHash;
use crate::observed::message::json::{Json, Number};
use crate::observed::message::{
    AssistantPart, Media, MediaKind, MessageBody, Reasoning, SystemPart, Text, ToolArguments,
    ToolCall, ToolCallId, ToolExecution, ToolName, ToolOutcome, ToolResult, ToolResultContent,
    Unknown, UserPart,
};
use crate::support::{Blake3, NonEmpty};

/// Rendering choices (whitespace, member order, escapes, number
/// spellings), drawn from a seed so a failing case replays.
#[derive(Debug, Clone)]
pub struct Style {
    state: u64,
}

impl Style {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// SplitMix64.
    pub fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A number in `0..bound` (0 when `bound` is 0).
    pub fn below(&mut self, bound: usize) -> usize {
        if bound == 0 {
            return 0;
        }
        let bound = u64::try_from(bound).unwrap_or(u64::MAX);
        usize::try_from(self.next() % bound).unwrap_or(0)
    }

    /// True one time in `odds`.
    pub fn chance(&mut self, odds: usize) -> bool {
        self.below(odds) == 0
    }

    /// Insignificant whitespace, often none.
    pub fn space(&mut self) -> &'static str {
        match self.below(6) {
            0 => " ",
            1 => "\n  ",
            2 => "\t",
            _ => "",
        }
    }

    pub fn shuffle<T>(&mut self, items: &mut [T]) {
        for at in (1..items.len()).rev() {
            let other = self.below(at + 1);
            items.swap(at, other);
        }
    }
}

/// A JSON value as a test builds it: object names are distinct, so every
/// spelling of it parses to one value.
#[derive(Debug, Clone, PartialEq)]
pub enum GenJson {
    Null,
    Bool(bool),
    Number(GenNumber),
    Str(String),
    Array(Vec<GenJson>),
    Object(Vec<(String, GenJson)>),
}

/// `±digits × 10^exponent`; `digits` has no leading zero (`"0"` is zero).
#[derive(Debug, Clone, PartialEq)]
pub struct GenNumber {
    pub negative: bool,
    pub digits: String,
    pub exponent: i32,
}

impl GenNumber {
    /// The exact value, as the parser builds it.
    pub fn value(&self) -> Number {
        let (sign, exp) = if self.exponent < 0 {
            (true, self.exponent.unsigned_abs().to_string())
        } else {
            (false, self.exponent.to_string())
        };
        Number::from_literal(self.negative, &self.digits, "", sign, &exp)
            .unwrap_or_else(|_| Number::zero())
    }

    /// One spelling of the number (`1`, `1.0`, `10e-1`, `0.1E+1`, ...).
    pub fn spell(&self, style: &mut Style) -> String {
        let mut digits = self.digits.clone();
        let mut exponent = i64::from(self.exponent);
        let zeros = style.below(3);
        for _ in 0..zeros {
            digits.push('0');
            exponent -= 1;
        }
        let point = style.below(digits.len() + 1);
        let (int, frac) = digits.split_at(point);
        exponent += i64::try_from(frac.len()).unwrap_or(0);
        let int = int.trim_start_matches('0');
        let int = if int.is_empty() { "0" } else { int };
        let mut out = String::new();
        if self.negative {
            out.push('-');
        }
        out.push_str(int);
        if !frac.is_empty() {
            out.push('.');
            out.push_str(frac);
        }
        if exponent != 0 || style.chance(4) {
            out.push(if style.chance(2) { 'e' } else { 'E' });
            if exponent < 0 {
                out.push('-');
            } else if style.chance(2) {
                out.push('+');
            }
            out.push_str(&exponent.unsigned_abs().to_string());
        }
        out
    }
}

impl GenJson {
    /// The value as the parser builds it.
    pub fn value(&self) -> Json {
        match self {
            Self::Null => Json::Null,
            Self::Bool(value) => Json::Bool(*value),
            Self::Number(number) => Json::Number(number.value()),
            Self::Str(text) => Json::String(text.clone()),
            Self::Array(items) => Json::Array(items.iter().map(Self::value).collect()),
            Self::Object(members) => Json::Object(
                members
                    .iter()
                    .map(|(name, member)| (name.clone(), member.value()))
                    .collect(),
            ),
        }
    }

    /// One spelling of the value: whitespace, member order, string escapes
    /// and number spellings chosen by `style`.
    pub fn render(&self, style: &mut Style) -> String {
        let mut out = String::new();
        self.render_into(style, &mut out);
        out
    }

    fn render_into(&self, style: &mut Style, out: &mut String) {
        match self {
            Self::Null => out.push_str("null"),
            Self::Bool(true) => out.push_str("true"),
            Self::Bool(false) => out.push_str("false"),
            Self::Number(number) => out.push_str(&number.spell(style)),
            Self::Str(text) => out.push_str(&string(text, style)),
            Self::Array(items) => {
                out.push('[');
                for (at, item) in items.iter().enumerate() {
                    if at > 0 {
                        out.push(',');
                    }
                    out.push_str(style.space());
                    item.render_into(style, out);
                    out.push_str(style.space());
                }
                out.push(']');
            }
            Self::Object(members) => {
                let mut order: Vec<&(String, GenJson)> = members.iter().collect();
                style.shuffle(&mut order);
                out.push('{');
                for (at, (name, member)) in order.into_iter().enumerate() {
                    if at > 0 {
                        out.push(',');
                    }
                    out.push_str(style.space());
                    out.push_str(&string(name, style));
                    out.push_str(style.space());
                    out.push(':');
                    out.push_str(style.space());
                    member.render_into(style, out);
                    out.push_str(style.space());
                }
                out.push('}');
            }
        }
    }
}

/// `text` as a JSON string literal, escaping what must be escaped and,
/// as `style` chooses, other characters too.
pub fn string(text: &str, style: &mut Style) -> String {
    let mut out = String::from("\"");
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' if style.chance(2) => out.push_str("\\n"),
            '/' if style.chance(3) => out.push_str("\\/"),
            '\u{0}'..='\u{1f}' => out.push_str(&format!("\\u{:04X}", u32::from(ch))),
            _ if style.chance(8) => {
                let mut units = [0u16; 2];
                for unit in ch.encode_utf16(&mut units) {
                    out.push_str(&format!("\\u{unit:04x}"));
                }
            }
            _ => out.push(ch),
        }
    }
    out.push('"');
    out
}

/// Text with the characters that stress verbatim handling: edge and
/// inner whitespace, quotes, backslashes, controls, NFD sequences, astral
/// characters.
pub fn arb_text() -> impl Strategy<Value = String> {
    prop_oneof![
        any::<String>(),
        proptest::collection::vec(
            prop_oneof![
                Just(" ".to_owned()),
                Just("\t".to_owned()),
                Just("\n".to_owned()),
                Just("\r\n".to_owned()),
                Just("\"".to_owned()),
                Just("\\".to_owned()),
                Just("/".to_owned()),
                Just("\u{1}".to_owned()),
                Just("e\u{301}".to_owned()),
                Just("\u{e9}".to_owned()),
                Just("\u{1F600}".to_owned()),
                Just("\u{FEFF}".to_owned()),
                "[a-zA-Z0-9 ]{1,6}",
            ],
            0..12,
        )
        .prop_map(|pieces| pieces.concat()),
    ]
}

pub fn arb_number() -> impl Strategy<Value = GenNumber> {
    (
        any::<bool>(),
        prop_oneof![Just("0".to_owned()), "[1-9][0-9]{0,40}"],
        -30i32..30,
    )
        .prop_map(|(negative, digits, exponent)| GenNumber {
            negative,
            digits,
            exponent,
        })
}

fn arb_leaf() -> impl Strategy<Value = GenJson> {
    prop_oneof![
        Just(GenJson::Null),
        any::<bool>().prop_map(GenJson::Bool),
        arb_number().prop_map(GenJson::Number),
        arb_text().prop_map(GenJson::Str),
    ]
}

/// The members whose names have not appeared before.
fn distinct(members: Vec<(String, GenJson)>) -> Vec<(String, GenJson)> {
    let mut seen = std::collections::BTreeSet::new();
    members
        .into_iter()
        .filter(|(name, _)| seen.insert(name.clone()))
        .collect()
}

pub fn arb_json() -> impl Strategy<Value = GenJson> {
    arb_leaf().prop_recursive(4, 32, 6, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..6).prop_map(GenJson::Array),
            proptest::collection::vec((prop_oneof!["[a-z_]{1,8}", arb_text()], inner), 0..6)
                .prop_map(|members| GenJson::Object(distinct(members))),
        ]
    })
}

fn arb_unknown() -> impl Strategy<Value = Unknown> {
    ("[a-z_]{0,12}", arb_json()).prop_map(|(kind, value)| Unknown {
        kind,
        raw: value.value().canonical(),
    })
}

fn arb_media() -> impl Strategy<Value = Media> {
    (
        prop_oneof![
            Just(MediaKind::Image),
            Just(MediaKind::Audio),
            Just(MediaKind::Document)
        ],
        any::<[u8; 32]>(),
    )
        .prop_map(|(kind, digest)| Media {
            kind,
            blob: MessageHash::from_digest(Blake3::from_bytes(digest)),
        })
}

fn arb_result() -> impl Strategy<Value = ToolResult> {
    (
        arb_text(),
        proptest::collection::vec(
            prop_oneof![
                arb_text().prop_map(|text| ToolResultContent::Text(Text(text))),
                arb_media().prop_map(ToolResultContent::Media),
                arb_unknown().prop_map(ToolResultContent::Unknown),
            ],
            0..4,
        ),
        any::<bool>(),
    )
        .prop_map(|(id, content, error)| ToolResult {
            call_id: ToolCallId(id),
            content,
            outcome: if error {
                ToolOutcome::Error
            } else {
                ToolOutcome::Success
            },
        })
}

fn arb_assistant_part() -> impl Strategy<Value = AssistantPart> {
    prop_oneof![
        arb_text().prop_map(|text| AssistantPart::Text(Text(text))),
        (arb_text(), proptest::option::of(any::<String>())).prop_map(|(text, signature)| {
            AssistantPart::Reasoning(Reasoning::Visible {
                text: Text(text),
                signature,
            })
        }),
        any::<String>()
            .prop_map(|signature| AssistantPart::Reasoning(Reasoning::Opaque { signature })),
        (
            arb_text(),
            arb_text(),
            prop_oneof![
                arb_json().prop_map(|value| ToolArguments::Json(value.value().canonical())),
                any::<String>().prop_map(ToolArguments::Invalid),
            ],
            any::<bool>()
        )
            .prop_map(
                |(id, name, arguments, server)| AssistantPart::ToolCall(ToolCall {
                    id: ToolCallId(id),
                    name: ToolName(name),
                    arguments,
                    execution: if server {
                        ToolExecution::Server
                    } else {
                        ToolExecution::Client
                    },
                })
            ),
        arb_result().prop_map(AssistantPart::ServerToolResult),
        arb_unknown().prop_map(AssistantPart::Unknown),
    ]
}

/// A message body of any variant, with every part variant in its lists.
pub fn arb_body() -> impl Strategy<Value = MessageBody> {
    prop_oneof![
        proptest::collection::vec(
            prop_oneof![
                arb_text().prop_map(|text| SystemPart::Text(Text(text))),
                arb_unknown().prop_map(SystemPart::Unknown),
            ],
            0..4
        )
        .prop_map(MessageBody::System),
        proptest::collection::vec(
            prop_oneof![
                arb_text().prop_map(|text| UserPart::Text(Text(text))),
                arb_media().prop_map(UserPart::Media),
                arb_unknown().prop_map(UserPart::Unknown),
            ],
            0..4
        )
        .prop_map(MessageBody::User),
        proptest::collection::vec(arb_assistant_part(), 0..5).prop_map(MessageBody::Assistant),
        proptest::collection::vec(arb_result(), 1..4).prop_map(|results| {
            MessageBody::Tool(
                NonEmpty::from_vec(results).unwrap_or_else(|| panic!("at least one result")),
            )
        }),
    ]
}
