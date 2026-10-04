//! Generators: words, sentences, encodings and multi-agent scenarios.

use base64::Engine as _;
use crosstalk_spec::derived::provenance::matching::Codec;
use proptest::prelude::*;

/// A word of three to seven lowercase letters, sometimes accented or
/// non-Latin, so texts carry multi-byte characters.
pub fn word() -> impl Strategy<Value = String> {
    prop_oneof![
        8 => "[a-z]{3,7}",
        1 => "[a-zàéîõüß]{3,6}",
        1 => "[α-ω]{3,5}",
        1 => "[一-龥]{2,4}",
    ]
}

/// A sentence of `min..max` words.
pub fn sentence(min: usize, max: usize) -> impl Strategy<Value = String> {
    proptest::collection::vec(word(), min..max).prop_map(|words| words.join(" "))
}

/// A sentence long enough to be guaranteed a shared fingerprint with any
/// text containing it (test parameters: 11 normalized characters).
pub fn long_sentence() -> impl Strategy<Value = String> {
    sentence(10, 18)
}

/// One of the spec's codecs.
pub fn codec() -> impl Strategy<Value = Codec> {
    prop_oneof![
        Just(Codec::Base64),
        Just(Codec::Hex),
        Just(Codec::UrlEncoding),
        Just(Codec::UnicodeNormalization),
    ]
}

/// A chain of one to `max` codecs, each changing the text and none undone
/// by an earlier step of the decode (no Unicode normalization twice in a
/// row: one normalization undoes both).
pub fn codec_chain(max: usize) -> impl Strategy<Value = Vec<Codec>> {
    proptest::collection::vec(codec(), 1..=max).prop_filter("no repeated normalization", |chain| {
        chain.windows(2).all(|pair| {
            !(pair[0] == Codec::UnicodeNormalization && pair[1] == Codec::UnicodeNormalization)
        })
    })
}

/// `text` encoded with `codec`.
pub fn encode(codec: Codec, text: &str) -> String {
    match codec {
        Codec::Base64 => base64::engine::general_purpose::STANDARD.encode(text),
        Codec::Hex => text.bytes().map(|b| format!("{b:02x}")).collect(),
        Codec::UrlEncoding => text
            .bytes()
            .map(|b| {
                if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
                    char::from(b).to_string()
                } else {
                    format!("%{b:02X}")
                }
            })
            .collect(),
        Codec::UnicodeNormalization => text
            .chars()
            .map(|ch| {
                let wide = if ch.is_ascii_graphic() {
                    char::from_u32(u32::from(ch) - 0x21 + 0xFF01).unwrap_or(ch)
                } else {
                    ch
                };
                format!("{wide}\u{200b}")
            })
            .collect(),
    }
}

/// `text` encoded with each codec of `chain` in turn.
pub fn encode_chain(chain: &[Codec], text: &str) -> String {
    chain
        .iter()
        .fold(text.to_owned(), |text, codec| encode(*codec, &text))
}

/// How a read text arrives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Via {
    ToolResult,
    UserTurn,
    SystemPrompt,
}

pub fn via() -> impl Strategy<Value = Via> {
    prop_oneof![
        Just(Via::ToolResult),
        Just(Via::UserTurn),
        Just(Via::SystemPrompt)
    ]
}

/// One piece of a generated output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Piece {
    /// Fresh text.
    Fresh(String),
    /// The output of an earlier turn, by index (taken modulo the turns run).
    Copy(usize),
}

/// One generated turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnPlan {
    /// Which of three agents runs it.
    pub agent: usize,
    /// Earlier turns' outputs read as new inputs, with how they arrive, and
    /// fresh inputs.
    pub reads: Vec<(Piece, Via)>,
    /// Earlier outputs replayed as history (never scanned).
    pub history: Vec<usize>,
    /// The output's text parts.
    pub output: Vec<Piece>,
    /// Whether the output also writes its first piece through a tool call.
    pub tool_call: bool,
}

fn piece() -> impl Strategy<Value = Piece> {
    prop_oneof![
        2 => long_sentence().prop_map(Piece::Fresh),
        3 => (0usize..8).prop_map(Piece::Copy),
    ]
}

pub fn turn_plan() -> impl Strategy<Value = TurnPlan> {
    (
        0usize..3,
        proptest::collection::vec((piece(), via()), 0..3),
        proptest::collection::vec(0usize..8, 0..2),
        proptest::collection::vec(piece(), 1..4),
        any::<bool>(),
    )
        .prop_map(|(agent, reads, history, output, tool_call)| TurnPlan {
            agent,
            reads,
            history,
            output,
            tool_call,
        })
}

/// Two to six turns.
pub fn scenario() -> impl Strategy<Value = Vec<TurnPlan>> {
    proptest::collection::vec(turn_plan(), 2..7)
}

/// Whether every step of `chain` is needed to read `text` back: each
/// changes the text, and URL-encoding base64 changes more than its padding
/// (base64 decoding ignores padding, so the shorter chain would match).
pub fn effective(chain: &[Codec], text: &str) -> bool {
    let mut current = text.to_owned();
    let mut previous: Option<Codec> = None;
    for codec in chain {
        let next = encode(*codec, &current);
        if next == current {
            return false;
        }
        if *codec == Codec::UrlEncoding
            && previous == Some(Codec::Base64)
            && !current.contains(['+', '/'])
        {
            return false;
        }
        previous = Some(*codec);
        current = next;
    }
    true
}
