//! Ids and digests on the wire: entity ids as ULID text, content ids and
//! BLAKE3 digests as lower-case hex, secret digests as `{key, digest}`.

use std::collections::BTreeMap;

use super::harness::{assert_golden, assert_rejected, assert_round_trips};
use super::{ULID_A, ULID_B, id};
use crate::ids::{
    AccountHash, AgentId, AlertId, ChannelId, ConfigHash, CredentialHash, InvalidUlidText,
    MessageHash, SecretVersion,
};
use crate::support::{Blake3, InvalidHex};

const AREA: &str = "ids";

/// 0x000102…1f, whose hex is easy to check by eye.
fn counting_digest() -> Blake3 {
    let mut bytes = [0u8; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::try_from(index).unwrap_or(u8::MAX);
    }
    Blake3::from_bytes(bytes)
}

#[test]
fn ulid_text_matches_reference_values() {
    // Values computed independently (Python, Crockford base32).
    let cases = [
        (0u128, "00000000000000000000000000"),
        (1, "00000000000000000000000001"),
        (2_089_863_487_043_351_997_724_039_247_149_597_674, ULID_A),
        (u128::MAX, "7ZZZZZZZZZZZZZZZZZZZZZZZZZ"),
    ];
    for (raw, text) in cases {
        assert_eq!(AgentId::from_ulid(raw).ulid_text(), text);
        assert_eq!(AgentId::from_ulid_text(text), Ok(AgentId::from_ulid(raw)));
        assert_eq!(
            serde_json::to_string(&AgentId::from_ulid(raw)).ok(),
            Some(format!("\"{text}\""))
        );
    }
}

#[test]
fn ulid_text_accepts_only_the_canonical_form() {
    assert_eq!(
        AgentId::from_ulid_text("01J9Z3K8M4Q7R2T5V6W8X9Y0Z"),
        Err(InvalidUlidText::Length { got: 25 })
    );
    assert_eq!(
        AgentId::from_ulid_text("01J9Z3K8M4Q7R2T5V6W8X9Y0ZAB"),
        Err(InvalidUlidText::Length { got: 27 })
    );
    // Lower case, and the letters Crockford leaves out, are refused rather
    // than folded, so an id has one text.
    assert_eq!(
        AgentId::from_ulid_text("01j9Z3K8M4Q7R2T5V6W8X9Y0ZA"),
        Err(InvalidUlidText::Character { index: 2 })
    );
    for (index, letter) in [(3, 'I'), (3, 'L'), (3, 'O'), (3, 'U')] {
        let text = format!("01J{letter}Z3K8M4Q7R2T5V6W8X9Y0ZA");
        assert_eq!(
            AgentId::from_ulid_text(&text),
            Err(InvalidUlidText::Character { index })
        );
    }
    assert_eq!(
        AgentId::from_ulid_text("80000000000000000000000000"),
        Err(InvalidUlidText::Overflow)
    );
}

#[test]
fn entity_ids_golden() {
    assert_golden(AREA, "agent_id", &id(AgentId::from_ulid_text, ULID_A));
}

#[test]
fn entity_ids_reject_anything_but_ulid_text() {
    assert_rejected::<AgentId>(r#""01j9z3k8m4q7r2t5v6w8x9y0za""#, "invalid ULID text");
    assert_rejected::<AgentId>(r#""01J9Z3K8""#, "invalid ULID text");
    assert_rejected::<AgentId>(r#""80000000000000000000000000""#, "Overflow");
    assert_rejected::<AgentId>("42", "invalid type");
    assert_rejected::<AgentId>("null", "invalid type");
}

/// A map keyed by ids is an object keyed by their text, both ways.
#[test]
fn ids_key_maps_by_their_text() {
    let map = BTreeMap::from([
        (id(ChannelId::from_ulid_text, ULID_B), 2u64),
        (id(ChannelId::from_ulid_text, ULID_A), 1u64),
    ]);
    assert_golden(AREA, "channel_id_map", &map);
    assert_round_trips(&std::collections::HashMap::from([(
        id(AlertId::from_ulid_text, ULID_A),
        true,
    )]));
    assert_rejected::<BTreeMap<ChannelId, u64>>(r#"{"not-an-id": 1}"#, "invalid ULID text");
}

#[test]
fn blake3_hex_matches_reference_value() {
    let digest = counting_digest();
    let hex = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    assert_eq!(digest.to_hex(), hex);
    assert_eq!(Blake3::from_hex(hex), Ok(digest));
    assert_eq!(
        Blake3::from_hex(&hex.to_uppercase()),
        Err(InvalidHex::Character { index: 21 })
    );
    assert_eq!(
        Blake3::from_hex(&hex[..62]),
        Err(InvalidHex::Length { got: 62 })
    );
    assert_eq!(
        Blake3::from_hex(&format!("{}g", &hex[..63])),
        Err(InvalidHex::Character { index: 63 })
    );
}

#[test]
fn content_ids_golden() {
    assert_golden(
        AREA,
        "message_hash",
        &MessageHash::from_digest(counting_digest()),
    );
    assert_round_trips(&ConfigHash::from_digest(counting_digest()));
}

#[test]
fn content_ids_reject_anything_but_lower_case_hex() {
    assert_rejected::<MessageHash>(
        r#""000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F""#,
        "invalid BLAKE3 hex",
    );
    assert_rejected::<MessageHash>(r#""0001""#, "invalid BLAKE3 hex");
    assert_rejected::<Blake3>("[0, 1, 2]", "invalid type");
}

#[test]
fn secret_digests_golden() {
    assert_golden(
        AREA,
        "credential_hash",
        &CredentialHash::from_keyed_digest(SecretVersion(3), counting_digest()),
    );
    assert_round_trips(&AccountHash::from_keyed_digest(
        SecretVersion(1),
        counting_digest(),
    ));
}

#[test]
fn secret_digests_reject_unknown_and_missing_fields() {
    let digest = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    assert_rejected::<CredentialHash>(
        &format!(r#"{{"key": 3, "digest": "{digest}", "raw": "sk-live"}}"#),
        "unknown field `raw`",
    );
    assert_rejected::<CredentialHash>(r#"{"key": 3}"#, "missing field `digest`");
    assert_rejected::<CredentialHash>(
        &format!(r#"{{"key": 70000, "digest": "{digest}"}}"#),
        "invalid value",
    );
}
