//! Deterministic encoders for the cipher corpus, and the match each one
//! makes a detector need.
//!
//! In reach (the spec's `Codec` names them, so a `Decoded` match can find
//! them): base64, hex, URL encoding, and base64 then URL encoding. Out of
//! reach: rot13, rotN, binary8 (each byte as eight bits, space separated)
//! and a seeded letter substitution.
//!
//! The need is computed from the actual encoding, weakest first: an
//! encoding that leaves the payload unchanged (URL encoding of a run of
//! letters, rot13 of digits) needs only `Exact`, and a chain drops a layer
//! that changed nothing. A chain lists codecs in the order a reader's text
//! is decoded: base64 then URL encoding is `[UrlEncoding, Base64]`.

use std::fmt;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use crosstalk_spec::derived::provenance::matching::Codec;
use serde::{Deserialize, Serialize};

use crate::datasets::rng::SplitMix64;
use crate::truth::{MatchNeed, Tier};

/// Which encoder, before its key is drawn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CipherKind {
    Base64,
    Hex,
    Url,
    Base64Url,
    Rot13,
    RotN,
    Binary8,
    Substitution,
}

impl CipherKind {
    pub const ALL: [Self; 8] = [
        Self::Base64,
        Self::Hex,
        Self::Url,
        Self::Base64Url,
        Self::Rot13,
        Self::RotN,
        Self::Binary8,
        Self::Substitution,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::Base64 => "base64",
            Self::Hex => "hex",
            Self::Url => "url",
            Self::Base64Url => "base64_url",
            Self::Rot13 => "rot13",
            Self::RotN => "rot_n",
            Self::Binary8 => "binary8",
            Self::Substitution => "substitution",
        }
    }

    /// Whether the spec's `Codec` can name every layer of it.
    pub fn in_reach(self) -> bool {
        matches!(self, Self::Base64 | Self::Hex | Self::Url | Self::Base64Url)
    }

    /// The encoder, with any key drawn from `rng`.
    pub fn instantiate(self, rng: &mut SplitMix64) -> Cipher {
        match self {
            Self::Base64 => Cipher::Base64,
            Self::Hex => Cipher::Hex,
            Self::Url => Cipher::Url,
            Self::Base64Url => Cipher::Base64Url,
            Self::Rot13 => Cipher::Rot { shift: 13 },
            Self::RotN => {
                // 1..=25 without 13 (that is rot13's own kind).
                let draw = rng.below(24).unwrap_or(0) as u8 + 1;
                Cipher::Rot {
                    shift: if draw >= 13 { draw + 1 } else { draw },
                }
            }
            Self::Binary8 => Cipher::Binary8,
            Self::Substitution => {
                let mut key: [u8; 26] = std::array::from_fn(|at| b'a' + at as u8);
                rng.shuffle(&mut key);
                Cipher::Substitution { key }
            }
        }
    }
}

impl fmt::Display for CipherKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// An encoder with its key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cipher {
    Base64,
    Hex,
    Url,
    Base64Url,
    /// Letters shifted by `shift` places (1..=25), case kept.
    Rot {
        shift: u8,
    },
    Binary8,
    /// `key[i]` replaces the `i`-th letter, case kept.
    Substitution {
        key: [u8; 26],
    },
}

impl Cipher {
    pub fn kind(&self) -> CipherKind {
        match self {
            Self::Base64 => CipherKind::Base64,
            Self::Hex => CipherKind::Hex,
            Self::Url => CipherKind::Url,
            Self::Base64Url => CipherKind::Base64Url,
            Self::Rot { shift: 13 } => CipherKind::Rot13,
            Self::Rot { .. } => CipherKind::RotN,
            Self::Binary8 => CipherKind::Binary8,
            Self::Substitution { .. } => CipherKind::Substitution,
        }
    }

    pub fn encode(&self, payload: &str) -> String {
        match self {
            Self::Base64 => base64(payload),
            Self::Hex => hex(payload),
            Self::Url => url(payload),
            Self::Base64Url => url(&base64(payload)),
            Self::Rot { shift } => rot(payload, *shift),
            Self::Binary8 => binary8(payload),
            Self::Substitution { key } => substitute(payload, key),
        }
    }

    /// The weakest match a detector needs to find `payload` arriving as
    /// this encoding, and the tier its label gets.
    pub fn need(&self, payload: &str) -> (MatchNeed, Tier) {
        let encoded = self.encode(payload);
        if encoded == payload {
            return (MatchNeed::Exact, Tier::Construction);
        }
        let decoded = |codecs: Vec<Codec>| (MatchNeed::Decoded { codecs }, Tier::Construction);
        match self {
            Self::Base64 => decoded(vec![Codec::Base64]),
            Self::Hex => decoded(vec![Codec::Hex]),
            Self::Url => decoded(vec![Codec::UrlEncoding]),
            Self::Base64Url => {
                let inner = base64(payload);
                if url(&inner) == inner {
                    decoded(vec![Codec::Base64])
                } else {
                    decoded(vec![Codec::UrlEncoding, Codec::Base64])
                }
            }
            Self::Rot { .. } | Self::Binary8 | Self::Substitution { .. } => (
                MatchNeed::Undecodable {
                    codec: self.kind().name().to_owned(),
                },
                Tier::OutOfReach,
            ),
        }
    }
}

pub fn base64(text: &str) -> String {
    STANDARD.encode(text.as_bytes())
}

/// Lowercase hex of the UTF-8 bytes.
pub fn hex(text: &str) -> String {
    text.bytes().map(|byte| format!("{byte:02x}")).collect()
}

/// RFC 3986 percent-encoding: unreserved characters (`A-Z a-z 0-9 - . _ ~`)
/// kept, every other byte as `%XX` (uppercase), so a space is `%20`.
pub fn url(text: &str) -> String {
    let mut out = String::with_capacity(text.len() * 3);
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// Each letter moved `shift` places along the alphabet, case kept.
pub fn rot(text: &str, shift: u8) -> String {
    let shift = shift % 26;
    text.chars()
        .map(|ch| match ch {
            'a'..='z' => char::from((ch as u8 - b'a' + shift) % 26 + b'a'),
            'A'..='Z' => char::from((ch as u8 - b'A' + shift) % 26 + b'A'),
            other => other,
        })
        .collect()
}

/// Each UTF-8 byte as eight binary digits, separated by spaces.
pub fn binary8(text: &str) -> String {
    text.bytes()
        .map(|byte| format!("{byte:08b}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Each letter replaced by its `key` letter, case kept.
pub fn substitute(text: &str, key: &[u8; 26]) -> String {
    text.chars()
        .map(|ch| match ch {
            'a'..='z' => char::from(key[usize::from(ch as u8 - b'a')]),
            'A'..='Z' => char::from(key[usize::from(ch as u8 - b'A')].to_ascii_uppercase()),
            other => other,
        })
        .collect()
}
