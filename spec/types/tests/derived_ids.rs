//! Ids and keys that are functions of their inputs, so a restart or a
//! redelivery reproduces them: [`EventId::derive`]
//! (`canonical.ids.derived-event-id`) and [`KeyedHasher::derive_key`]
//! (`canonical.ids.derived-key-per-purpose`).

use std::collections::BTreeSet;

use proptest::prelude::*;

use crate::ids::mint::ulid_millis;
use crate::ids::{DeploymentSecret, EventId, KeyedHasher, SecretVersion};
use crate::support::Timestamp;

/// A parent envelope id: millisecond 1_790_000_000_000, random part 0x1234.
const PARENT: u128 = (1_790_000_000_000u128 << 80) | 0x1234;

/// The derivation written out independently of the implementation: the
/// parent's millisecond, then the first 10 bytes of BLAKE3 over the domain,
/// the label's length (u32 big-endian) and bytes, the parent (u128
/// big-endian) and the ordinal (u32 big-endian).
fn expected(parent: u128, label: &str, ordinal: u32) -> u128 {
    let mut bytes = b"crosstalk.envelope.derived.v1".to_vec();
    let len = u32::try_from(label.len()).unwrap_or_else(|error| panic!("{error}"));
    bytes.extend_from_slice(&len.to_be_bytes());
    bytes.extend_from_slice(label.as_bytes());
    bytes.extend_from_slice(&parent.to_be_bytes());
    bytes.extend_from_slice(&ordinal.to_be_bytes());
    let digest = blake3::hash(&bytes);
    let mut random = [0u8; 16];
    random[6..].copy_from_slice(&digest.as_bytes()[..10]);
    ((parent >> 80) << 80) | u128::from_be_bytes(random)
}

/// The derivation is exactly the documented one, and pinned: changing it
/// would give every redelivery after an upgrade new ids, which the bus
/// would not deduplicate against envelopes published before it.
#[test]
fn derived_event_id_is_the_documented_digest() {
    let parent = EventId::from_ulid(PARENT);
    for (label, ordinal) in [("edge-updated", 0), ("transmission-confirmed", 3), ("", 0)] {
        assert_eq!(
            EventId::derive(parent, label, ordinal).as_ulid(),
            expected(PARENT, label, ordinal),
            "{label} {ordinal}"
        );
    }
    assert_eq!(
        EventId::derive(parent, "edge-updated", 0).ulid_text(),
        PINNED_EDGE_UPDATED_0
    );
}

/// `EventId::derive(PARENT, "edge-updated", 0)`, as ULID text.
const PINNED_EDGE_UPDATED_0: &str = "01M3250V00PYK2DW884Z87A8MB";

/// A redelivery derives the same id; the id sorts with its cause's
/// millisecond.
#[test]
fn derived_event_id_is_stable_and_keeps_the_parent_millisecond() {
    let parent = EventId::from_ulid(PARENT);
    let once = EventId::derive(parent, "transmission-confirmed", 1);
    let again = EventId::derive(parent, "transmission-confirmed", 1);
    assert_eq!(once, again);
    assert_eq!(ulid_millis(once.as_ulid()), ulid_millis(PARENT));
    assert_ne!(once, parent);
}

/// Labels are length-prefixed, so a label that is a prefix of another, or
/// a label whose bytes run into the parent's, still gives its own id.
#[test]
fn derived_event_ids_differ_by_label_ordinal_and_parent() {
    let parent = EventId::from_ulid(PARENT);
    let other = EventId::from_ulid(PARENT + 1);
    let ids = [
        EventId::derive(parent, "edge", 0),
        EventId::derive(parent, "edge-updated", 0),
        EventId::derive(parent, "edge-updated", 1),
        EventId::derive(other, "edge-updated", 0),
        EventId::derive(parent, "", 0),
    ];
    let distinct: BTreeSet<_> = ids.iter().copied().collect();
    assert_eq!(distinct.len(), ids.len(), "{ids:?}");
}

const LABELS: [&str; 4] = [
    "transmission-confirmed",
    "transmission-suspected",
    "edge-updated",
    "transmission-classified",
];

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// `canonical.ids.derived-event-id`: for any parent, the id is a
    /// function of (parent, label, ordinal), keeps the parent's
    /// millisecond, and the outputs of one delivery never share an id.
    #[test]
    fn derived_event_ids_are_functions_of_their_inputs(
        parent in any::<u128>(),
        ordinals in 1u32..64,
    ) {
        let parent_id = EventId::from_ulid(parent);
        let mut seen = BTreeSet::new();
        for label in LABELS {
            for ordinal in 0..ordinals {
                let id = EventId::derive(parent_id, label, ordinal);
                prop_assert_eq!(id, EventId::derive(parent_id, label, ordinal));
                prop_assert_eq!(id.as_ulid(), expected(parent, label, ordinal));
                prop_assert_eq!(id.as_ulid() >> 80, parent >> 80);
                prop_assert!(seen.insert(id), "{id:?} derived twice");
            }
        }
    }
}

fn secret(version: u16, byte: u8) -> DeploymentSecret {
    DeploymentSecret::new(SecretVersion(version), [byte; 32])
}

/// `canonical.ids.derived-key-per-purpose`: the same secret and label give
/// the same key (two hashers, as after a restart), so a cursor keyed with
/// it still verifies; another label, key or version gives another key.
#[test]
fn derived_keys_survive_a_restart_and_separate_purposes() {
    let before = KeyedHasher::new(secret(1, 7)).derive_key("crosstalk.cursor.v1.surface");
    let after = KeyedHasher::new(secret(1, 7)).derive_key("crosstalk.cursor.v1.surface");
    assert_eq!(before.as_bytes(), after.as_bytes());
    assert_eq!(before.version(), SecretVersion(1));

    let other_purpose = KeyedHasher::new(secret(1, 7)).derive_key("crosstalk.cursor.v1.agents");
    let other_key = KeyedHasher::new(secret(1, 8)).derive_key("crosstalk.cursor.v1.surface");
    let rotated = KeyedHasher::new(secret(2, 8)).derive_key("crosstalk.cursor.v1.surface");
    let keys: BTreeSet<[u8; 32]> = [&before, &other_purpose, &other_key]
        .into_iter()
        .map(|key| *key.as_bytes())
        .collect();
    assert_eq!(keys.len(), 3);
    assert_eq!(rotated.version(), SecretVersion(2));
}

/// A derived key is BLAKE3's key derivation over the secret and the label,
/// never a keyed digest: no raw value's credential or account digest can
/// equal it, so storing digests never discloses a key.
#[test]
fn derived_keys_are_not_keyed_digests() {
    let hasher = KeyedHasher::new(secret(1, 7));
    let label = "crosstalk.cursor.v1.surface";
    let key = hasher.derive_key(label);
    let digest = hasher.credential(label.as_bytes(), Timestamp::from_micros(0));
    assert_ne!(key.as_bytes(), digest.current.digest().as_bytes());

    let mut material = [7u8; 32].to_vec();
    material.extend_from_slice(label.as_bytes());
    let mut derive =
        blake3::Hasher::new_derive_key("crosstalk 2026-10 deployment secret derived key v1");
    derive.update(&material);
    assert_eq!(key.as_bytes(), derive.finalize().as_bytes());
}

/// Rotation: the key follows the current version only, so a rotation
/// invalidates what the previous key signed (decision Q4).
#[test]
fn derived_keys_follow_the_current_version_in_a_rotation() {
    let rotating =
        KeyedHasher::rotating(secret(2, 9), secret(1, 7), Timestamp::from_micros(u64::MAX))
            .unwrap_or_else(|error| panic!("{error}"));
    let label = "crosstalk.cursor.v1.surface";
    assert_eq!(
        rotating.derive_key(label).as_bytes(),
        KeyedHasher::new(secret(2, 9)).derive_key(label).as_bytes()
    );
    assert_eq!(rotating.derive_key(label).version(), SecretVersion(2));
}

/// The key is secret material: its `Debug` names the version only.
#[test]
fn derived_keys_never_format_their_bytes() {
    let key = KeyedHasher::new(secret(3, 0xab)).derive_key("crosstalk.cursor.v1.surface");
    let shown = format!("{key:?}");
    assert_eq!(shown, "DerivedKey { version: SecretVersion(3), .. }");
}
