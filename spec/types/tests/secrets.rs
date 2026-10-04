//! The deployment secret and the keyed hasher (`crate::ids::secret`):
//! digests keyed per version, rotation overlaps, and a secret that is never
//! formatted or serialized.

use crate::ids::{
    AccountHash, CredentialHash, DeploymentSecret, InvalidRotation, InvalidSecret, KeyedHasher,
    SecretVersion,
};
use crate::support::{Blake3, Timestamp};

/// BLAKE3's own test-vector key (`test_vectors.json` in the reference
/// implementation).
const VECTOR_KEY: &[u8; 32] = b"whats the Elvish word for friend";

fn key(byte: u8) -> [u8; 32] {
    let mut key = [0u8; 32];
    for (index, slot) in key.iter_mut().enumerate() {
        *slot = byte.wrapping_add(u8::try_from(index).unwrap_or(0));
    }
    key
}

fn secret(version: u16, byte: u8) -> DeploymentSecret {
    DeploymentSecret::new(SecretVersion(version), key(byte))
}

fn keyed(key: &[u8; 32], raw: &[u8]) -> Blake3 {
    Blake3::from_bytes(*blake3::keyed_hash(key, raw).as_bytes())
}

const T: Timestamp = Timestamp::from_micros(1_790_000_000_000_000);

fn before(at: Timestamp) -> Timestamp {
    Timestamp::from_micros(at.as_micros() - 1)
}

/// `canonical.ids.secret-digest-keyed`: a digest is the keyed BLAKE3 of
/// the raw value under its version's key: BLAKE3's own keyed vector, then
/// per version against `blake3::keyed_hash`, never the plain hash, and
/// different under different keys.
#[test]
fn secret_digests_use_their_versions_key() {
    let vector = KeyedHasher::new(DeploymentSecret::new(SecretVersion(1), *VECTOR_KEY));
    assert_eq!(
        vector.credential(b"", T).current.digest().to_hex(),
        "92b2b75604ed3c761f9d6f62392c8a9227ad0ea3f09573e783f1498a4ed60d26"
    );
    assert_eq!(
        Blake3::of(b"").to_hex(),
        "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262",
        "the unkeyed digest differs"
    );

    let raw = b"sk-ant-api03-REDACTED";
    for (version, byte) in [(1, 0x00), (2, 0x40), (7, 0xa0)] {
        let hasher = KeyedHasher::new(secret(version, byte));
        let credential = hasher.credential(raw, T);
        assert_eq!(credential.previous, None);
        assert_eq!(
            credential.current,
            CredentialHash::from_keyed_digest(SecretVersion(version), keyed(&key(byte), raw))
        );
        assert_ne!(
            *credential.current.digest(),
            Blake3::of(raw),
            "never unkeyed"
        );
        let account = hasher.account(b"acct-1234", T);
        assert_eq!(
            account.current,
            AccountHash::from_keyed_digest(SecretVersion(version), keyed(&key(byte), b"acct-1234"))
        );
    }
    let one = KeyedHasher::new(secret(1, 0x00)).credential(raw, T).current;
    let two = KeyedHasher::new(secret(1, 0x40)).credential(raw, T).current;
    assert_ne!(one.digest(), two.digest(), "another key, another digest");
}

/// `canonical.ids.rotation-overlap-digests`: inside the overlap, one digest
/// per loaded version, the current one first.
#[test]
fn overlap_yields_current_and_previous_digests() {
    let hasher = KeyedHasher::rotating(secret(2, 0x40), secret(1, 0x00), T)
        .unwrap_or_else(|error| panic!("{error}"));
    let raw = b"oauth-access-token";
    for at in [Timestamp::from_micros(0), before(T)] {
        let digests = hasher.credential(raw, at);
        let all: Vec<CredentialHash> = digests.all().collect();
        assert_eq!(
            all,
            vec![
                CredentialHash::from_keyed_digest(SecretVersion(2), keyed(&key(0x40), raw)),
                CredentialHash::from_keyed_digest(SecretVersion(1), keyed(&key(0x00), raw)),
            ],
            "current first, then previous"
        );
        assert_eq!(hasher.previous_version(at), Some(SecretVersion(1)));
        let accounts: Vec<SecretVersion> = hasher
            .account(b"acct", at)
            .all()
            .map(|digest| digest.key())
            .collect();
        assert_eq!(accounts, vec![SecretVersion(2), SecretVersion(1)]);
    }
    assert_eq!(hasher.current_version(), SecretVersion(2));
}

/// `canonical.ids.rotation-overlap-digests`: from the overlap's end on,
/// only the current version keys digests.
#[test]
fn after_overlap_only_current_digest() {
    let hasher = KeyedHasher::rotating(secret(2, 0x40), secret(1, 0x00), T)
        .unwrap_or_else(|error| panic!("{error}"));
    let raw = b"oauth-access-token";
    for at in [
        T,
        Timestamp::from_micros(T.as_micros() + 1),
        Timestamp::from_micros(u64::MAX),
    ] {
        let digests = hasher.credential(raw, at);
        assert_eq!(digests.previous, None, "at {at:?}");
        assert_eq!(
            digests.all().collect::<Vec<_>>(),
            vec![CredentialHash::from_keyed_digest(
                SecretVersion(2),
                keyed(&key(0x40), raw)
            )]
        );
        assert_eq!(hasher.previous_version(at), None);
    }
    // A hasher with one version never has a previous digest.
    let single = KeyedHasher::new(secret(2, 0x40));
    assert_eq!(
        single.credential(raw, Timestamp::from_micros(0)).previous,
        None
    );
}

/// A rotation's previous version is older than its current one.
#[test]
fn rotation_needs_an_older_previous_version() {
    for (current, previous) in [(2, 2), (2, 3)] {
        let refused = KeyedHasher::rotating(secret(current, 1), secret(previous, 2), T).err();
        assert_eq!(
            refused,
            Some(InvalidRotation::NotOlder {
                previous: SecretVersion(previous),
                current: SecretVersion(current),
            })
        );
    }
}

/// A secret reads from 64 hex digits of either case, and a refusal names
/// positions and lengths, never the text.
#[test]
fn secrets_read_from_hex() {
    let lower = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let upper = lower.to_uppercase();
    let raw = b"key";
    for text in [lower, upper.as_str()] {
        let parsed = DeploymentSecret::from_hex(SecretVersion(4), text)
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(parsed.version(), SecretVersion(4));
        assert_eq!(
            KeyedHasher::new(parsed).credential(raw, T).current,
            CredentialHash::from_keyed_digest(SecretVersion(4), keyed(&key(0), raw))
        );
    }
    let short = &lower[..62];
    let bad = format!("{}zz", &lower[..62]);
    assert_eq!(
        DeploymentSecret::from_hex(SecretVersion(1), short).err(),
        Some(InvalidSecret::Length { got: 62 })
    );
    let refused = DeploymentSecret::from_hex(SecretVersion(1), &bad).err();
    assert_eq!(refused, Some(InvalidSecret::NotHex { index: 62 }));
    let message = refused
        .map(|error| format!("{error} {error:?}"))
        .unwrap_or_default();
    assert!(!message.contains(&lower[..8]), "{message}");
}

/// Surrounding ASCII whitespace is ignored (an environment variable often
/// ends in a newline), and a refusal counts within the trimmed text;
/// whitespace inside the digits is still refused.
#[test]
fn secrets_read_from_hex_ignore_surrounding_whitespace() {
    let lower = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let raw = b"key";
    for text in [
        format!("{lower}\n"),
        format!("{lower}\r\n"),
        format!("  \t{lower} \n"),
    ] {
        let parsed = DeploymentSecret::from_hex(SecretVersion(2), &text)
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(
            KeyedHasher::new(parsed).credential(raw, T).current,
            CredentialHash::from_keyed_digest(SecretVersion(2), keyed(&key(0), raw))
        );
    }
    assert_eq!(
        DeploymentSecret::from_hex(SecretVersion(1), &format!(" {}\n", &lower[..62])).err(),
        Some(InvalidSecret::Length { got: 62 })
    );
    let inner = format!("\n{} {}", &lower[..31], &lower[32..]);
    assert_eq!(
        DeploymentSecret::from_hex(SecretVersion(1), &inner).err(),
        Some(InvalidSecret::NotHex { index: 31 })
    );
    assert_eq!(
        DeploymentSecret::from_hex(SecretVersion(1), " \n").err(),
        Some(InvalidSecret::Length { got: 0 })
    );
}

/// `canonical.ids.secret-never-serialized`: no formatting of a secret, a
/// hasher holding one, or a refusal to read one shows any of the key: not
/// its hex in either case, nor its bytes as a list. (That neither
/// serializes is checked at compile time in `crate::wire::confidential`.)
#[test]
fn secrets_never_show_their_key() {
    let current = key(0x5a);
    let previous = key(0xc3);
    let texts = [
        format!("{:?}", secret(1, 0xc3)),
        format!("{}", secret(1, 0xc3)),
        format!("{:#?}", secret(2, 0x5a)),
        format!("{:?}", KeyedHasher::new(secret(2, 0x5a))),
        {
            let hasher = KeyedHasher::rotating(secret(2, 0x5a), secret(1, 0xc3), T)
                .unwrap_or_else(|error| panic!("{error}"));
            format!("{hasher:?} {hasher:#?} {hasher}")
        },
    ];
    for text in &texts {
        for key in [current, previous] {
            let hex: String = key.iter().map(|byte| format!("{byte:02x}")).collect();
            assert!(!text.contains(&hex[..6]), "{text} shows the key's hex");
            assert!(
                !text.contains(&hex[..6].to_uppercase()),
                "{text} shows the key's hex"
            );
            let listed = format!("{}, {}, {}", key[0], key[1], key[2]);
            assert!(!text.contains(&listed), "{text} shows the key's bytes");
        }
        assert!(
            text.contains("version") || text.contains("SecretVersion"),
            "{text}"
        );
    }
    assert_eq!(
        texts[0],
        "DeploymentSecret { version: SecretVersion(1), .. }"
    );
    assert_eq!(texts[1], "deployment secret version 1");
}
