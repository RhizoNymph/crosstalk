//! Provenance on the wire: where a span sits (`SpanLocation`), what a
//! relayed span copied (`RelaySource`), and content matches
//! (`ContentMatch`, checked, with its `Carrier` and `MatchKind`), which
//! flow's evidence and the `ContentMatched` event carry.

use std::num::NonZeroU32;

use super::harness::{assert_golden, assert_rejected};
use super::{ULID_A, ULID_B, ULID_C, id};
use crate::derived::provenance::matching::{Carrier, Codec, ContentMatch, MatchKind};
use crate::derived::provenance::span::{RelaySource, SpanLocation};
use crate::ids::{AgentId, ExchangeId, MessageHash, SpanId};
use crate::observed::message::{PartRef, ToolCallId};
use crate::support::{Blake3, ByteRange, NonEmpty, Similarity};

const AREA: &str = "provenance";

const TOOL_RESULT_HEX: &str = "90c6783d80fad0eefa7c2c8887502d4a9d8839e043cd5f51582aa89f71cc9fb5";
const PAGE_HEX: &str = "721c9525ade2ea8903d343ef25cf68b9bf4ab0aad56bb7b01fbe48d09bc7fcf4";

fn message(hex: &str) -> MessageHash {
    MessageHash::from_digest(
        Blake3::from_hex(hex).unwrap_or_else(|error| panic!("{hex} is a digest: {error:?}")),
    )
}

fn span() -> SpanId {
    id(SpanId::from_ulid_text, ULID_C)
}

fn writer() -> AgentId {
    id(AgentId::from_ulid_text, ULID_A)
}

fn reader() -> AgentId {
    id(AgentId::from_ulid_text, ULID_B)
}

/// 1 KiB of the reader's tool result, its third part.
fn location() -> SpanLocation {
    SpanLocation {
        part: PartRef {
            message: message(TOOL_RESULT_HEX),
            index: 2,
        },
        range: ByteRange::new(120, 1144).expect("not empty"),
    }
}

fn bytes(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).unwrap_or_else(|| panic!("{n} is not zero"))
}

fn content_match(carrier: Carrier, kind: MatchKind, matched: u32) -> ContentMatch {
    ContentMatch::new(
        span(),
        writer(),
        reader(),
        id(ExchangeId::from_ulid_text, "01J9Z3R0S1T2V3W4X5Y6Z7A8B9"),
        location(),
        carrier,
        kind,
        bytes(matched),
    )
    .expect("two agents, and the match fits the read range")
}

fn tool_result() -> Carrier {
    Carrier::ToolResult(ToolCallId("toolu_01A09q90qw90lq917835lq9".into()))
}

fn every_carrier() -> Vec<Carrier> {
    fn declared(carrier: Carrier) -> Carrier {
        match carrier {
            Carrier::ToolResult(_)
            | Carrier::UserTurn
            | Carrier::SystemPrompt
            | Carrier::ReaderOutput => carrier,
        }
    }
    [
        tool_result(),
        Carrier::UserTurn,
        Carrier::SystemPrompt,
        Carrier::ReaderOutput,
    ]
    .map(declared)
    .to_vec()
}

fn every_codec() -> Vec<Codec> {
    fn declared(codec: Codec) -> Codec {
        match codec {
            Codec::Base64 | Codec::Hex | Codec::UrlEncoding | Codec::UnicodeNormalization => codec,
        }
    }
    [
        Codec::Base64,
        Codec::Hex,
        Codec::UrlEncoding,
        Codec::UnicodeNormalization,
    ]
    .map(declared)
    .to_vec()
}

fn every_kind() -> Vec<MatchKind> {
    fn declared(kind: MatchKind) -> MatchKind {
        match kind {
            MatchKind::Exact
            | MatchKind::Normalized
            | MatchKind::Decoded(_)
            | MatchKind::Semantic(_) => kind,
        }
    }
    let decoded = NonEmpty::from_vec(vec![Codec::UrlEncoding, Codec::Base64])
        .expect("two codecs, in the order applied");
    [
        MatchKind::Exact,
        MatchKind::Normalized,
        MatchKind::Decoded(decoded),
        MatchKind::Semantic(Similarity::new(0.875).expect("in range")),
    ]
    .map(declared)
    .to_vec()
}

fn every_relay_source() -> Vec<RelaySource> {
    fn declared(source: RelaySource) -> RelaySource {
        match source {
            RelaySource::Span(_) | RelaySource::Input(_) => source,
        }
    }
    [
        RelaySource::Span(span()),
        RelaySource::Input(message(PAGE_HEX)),
    ]
    .map(declared)
    .to_vec()
}

#[test]
fn provenance_goldens() {
    assert_golden(AREA, "span_location", &location());
    assert_golden(AREA, "relay_sources", &every_relay_source());
    assert_golden(AREA, "carriers", &every_carrier());
    assert_golden(AREA, "codecs", &every_codec());
    assert_golden(AREA, "match_kinds", &every_kind());
    assert_golden(
        AREA,
        "content_match",
        &content_match(tool_result(), MatchKind::Exact, 1024),
    );
    assert_golden(
        AREA,
        "content_match_semantic",
        &content_match(
            Carrier::ReaderOutput,
            MatchKind::Semantic(Similarity::new(0.875).expect("in range")),
            640,
        ),
    );
}

fn match_json(origin_agent: &str, matched_bytes: u32) -> String {
    format!(
        r#"{{"origin": "{ULID_C}", "origin_agent": "{origin_agent}", "reader": "{ULID_B}",
            "reader_exchange": "01J9Z3R0S1T2V3W4X5Y6Z7A8B9",
            "read_at": {{"part": {{"message": "{TOOL_RESULT_HEX}", "index": 2}},
                         "range": {{"start": 120, "end": 1144}}}},
            "carrier": {{"type": "user_turn"}}, "kind": {{"type": "exact"}},
            "matched_bytes": {matched_bytes}}}"#
    )
}

#[test]
fn content_matches_refuse_what_their_constructor_refuses() {
    assert_rejected::<ContentMatch>(&match_json(ULID_B, 64), "invalid content match: SelfMatch");
    assert_rejected::<ContentMatch>(
        &match_json(ULID_A, 1025),
        "invalid content match: ExceedsReadRange",
    );
    assert_rejected::<ContentMatch>(&match_json(ULID_A, 0), "invalid value");
    // The whole read range matched is the most a match can claim.
    let whole: ContentMatch =
        serde_json::from_str(&match_json(ULID_A, 1024)).expect("a match of the whole range");
    assert_eq!(whole.matched_bytes(), bytes(1024));
    assert_rejected::<ContentMatch>(
        &match_json(ULID_A, 64).replace(r#""kind""#, r#""score": 1, "kind""#),
        "unknown field `score`",
    );
    assert_rejected::<ContentMatch>(
        &match_json(ULID_A, 64).replace(r#""end": 1144"#, r#""end": 120"#),
        "invalid byte range: EmptyRange",
    );
}

#[test]
fn provenance_enums_refuse_unknown_and_invalid_variants() {
    assert_rejected::<MatchKind>(
        r#"{"type": "decoded", "data": []}"#,
        "invalid non-empty list: EmptyList",
    );
    assert_rejected::<MatchKind>(r#"{"type": "semantic", "data": 1.5}"#, "invalid similarity");
    assert_rejected::<MatchKind>(r#"{"type": "fuzzy"}"#, "unknown variant `fuzzy`");
    assert_rejected::<Codec>(r#""rot13""#, "unknown variant `rot13`");
    assert_rejected::<Carrier>(
        r#"{"type": "file_read", "data": "/tmp/notes"}"#,
        "unknown variant `file_read`",
    );
    assert_rejected::<Carrier>(r#"{"type": "tool_result", "data": 7}"#, "expected a string");
    assert_rejected::<RelaySource>(
        r#"{"type": "resource", "data": "01J9Z3K8M4Q7R2T5V6W8X9Y0ZA"}"#,
        "unknown variant `resource`",
    );
    assert_rejected::<RelaySource>(
        &format!(r#"{{"type": "span", "data": "{TOOL_RESULT_HEX}"}}"#),
        "invalid ULID text",
    );
    assert_rejected::<SpanLocation>(
        &format!(
            r#"{{"part": {{"message": "{TOOL_RESULT_HEX}", "index": 0}},
                "range": {{"start": 0, "end": 8}}, "text": "secret"}}"#
        ),
        "unknown field `text`",
    );
}
