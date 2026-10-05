//! The canonical encoding of message bodies and the canonical JSON inside
//! it (`crate::observed::message::{encoding, json}`). The functions the
//! `canonical.encoding.*` and `canonical.json.*` invariants name live
//! here, at `crosstalk_spec::tests::encoding::<name>`; each calls its body
//! in `vectors`, `json` or `props`.
//!
//! The normalizer's own uses of the encoding (hashing what it builds,
//! storing bodies, streamed numbers) are tested in `crosstalk-canonical`.

mod generate;
mod json;
mod props;
mod vectors;

use proptest::prelude::*;

use generate::{arb_body, arb_json};

/// `canonical.encoding.golden-vectors`.
#[test]
fn golden_encodings_match_pinned_bytes() {
    vectors::golden_encodings_match_pinned_bytes();
}

/// `canonical.encoding.decode-inverts-encode`.
#[test]
fn decode_refuses_bytes_encode_never_writes() {
    vectors::decode_accepts_only_encodings();
}

/// `canonical.encoding.decode-inverts-encode` and
/// `canonical.tool-call.signature-verbatim`: an absent tool-call signature
/// is omitted, a present one round-trips, an explicit `null` is refused.
#[test]
fn tool_call_signature_is_omitted_when_absent() {
    vectors::tool_call_signature_omitted_when_absent();
}

/// `canonical.json.rfc8785-form`.
#[test]
fn rfc8785_structure_vectors() {
    json::rfc8785_structure_vectors();
}

/// `canonical.json.rfc8785-form`.
#[test]
fn exact_decimal_number_vectors() {
    json::exact_decimal_number_vectors();
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    /// `canonical.encoding.round-trips`.
    #[test]
    fn encoding_round_trips(body in arb_body()) {
        props::encoding_round_trip(&body)?;
    }

    /// `canonical.json.large-integers-exact`.
    #[test]
    fn large_integers_round_trip_exactly(
        negative in any::<bool>(), digits in "[1-9][0-9]{15,60}", seed in any::<u64>()
    ) {
        props::large_integer_exact(negative, &digits, seed)?;
    }

    /// `canonical.json.rfc8785-form`.
    #[test]
    fn canonical_json_ignores_formatting(value in arb_json(), seeds in any::<(u64, u64)>()) {
        props::canonical_ignores_formatting(&value, seeds)?;
    }
}

proptest! {
    // Cheap per case, and the edits that land on a valid-looking shape are
    // a small share of them: more cases.
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// `canonical.encoding.decode-inverts-encode`.
    #[test]
    fn decode_accepts_only_what_encode_writes(body in arb_body(), seed in any::<u64>()) {
        props::decode_only_what_encode_writes(&body, seed)?;
    }
}
