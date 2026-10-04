//! ULID text for entity ids: 26 characters of Crockford base32.

use crosstalk_spec::ids::{
    AccessId, AgentId, AlertId, AlertRuleId, ChannelId, ConversationId, EventId, ExchangeId,
    OperatorId, ResourceId, SpanId, TopicId, TransmissionId,
};

use crate::contract::{AuditId, MergeId, ProjectionId, SinkId};

const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const LEN: usize = 26;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidUlid {
    #[error("expected {LEN} characters, got {0}")]
    Length(usize),
    #[error("invalid character {0:?}")]
    Character(char),
    #[error("value exceeds 128 bits")]
    Overflow,
}

pub fn encode(value: u128) -> String {
    let mut out = [0u8; LEN];
    let mut rest = value;
    for slot in out.iter_mut().rev() {
        *slot = ALPHABET[(rest & 0x1f) as usize];
        rest >>= 5;
    }
    out.iter().map(|&b| char::from(b)).collect()
}

pub fn decode(text: &str) -> Result<u128, InvalidUlid> {
    let count = text.chars().count();
    if count != LEN {
        return Err(InvalidUlid::Length(count));
    }
    let mut value: u128 = 0;
    for (i, c) in text.chars().enumerate() {
        let digit = digit(c).ok_or(InvalidUlid::Character(c))?;
        // 26 digits carry 130 bits; the first digit may use only 3.
        if i == 0 && digit > 7 {
            return Err(InvalidUlid::Overflow);
        }
        value = (value << 5) | u128::from(digit);
    }
    Ok(value)
}

/// Crockford decoding: case-insensitive, with I and L read as 1 and O as 0.
fn digit(c: char) -> Option<u8> {
    let upper = c.to_ascii_uppercase();
    let upper = match upper {
        'I' | 'L' => '1',
        'O' => '0',
        other => other,
    };
    ALPHABET
        .iter()
        .position(|&b| char::from(b) == upper)
        .and_then(|p| u8::try_from(p).ok())
}

/// An id with a ULID text form.
pub trait UlidId: Sized + Copy {
    fn from_raw(raw: u128) -> Self;
    fn raw(self) -> u128;

    fn to_ulid(self) -> String {
        encode(self.raw())
    }

    fn parse_ulid(text: &str) -> Result<Self, InvalidUlid> {
        decode(text).map(Self::from_raw)
    }
}

macro_rules! ulid_ids {
    ($($name:ty),* $(,)?) => {$(
        impl UlidId for $name {
            fn from_raw(raw: u128) -> Self {
                <$name>::from_ulid(raw)
            }

            fn raw(self) -> u128 {
                self.as_ulid()
            }
        }
    )*};
}

ulid_ids!(
    AgentId,
    ConversationId,
    ExchangeId,
    SpanId,
    ResourceId,
    AccessId,
    ChannelId,
    TransmissionId,
    TopicId,
    AlertRuleId,
    AlertId,
    OperatorId,
    EventId,
    MergeId,
    ProjectionId,
    SinkId,
    AuditId,
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_extremes() {
        for value in [0, 1, u128::MAX, u128::MAX >> 1, 0x0123_4567_89ab_cdef] {
            assert_eq!(decode(&encode(value)), Ok(value));
        }
    }

    #[test]
    fn encodes_max_as_seven_then_z() {
        assert_eq!(encode(u128::MAX), "7ZZZZZZZZZZZZZZZZZZZZZZZZZ");
        assert_eq!(encode(0), "00000000000000000000000000");
    }

    #[test]
    fn decodes_crockford_aliases_case_insensitively() {
        assert_eq!(decode("0000000000000000000000000l"), Ok(1));
        assert_eq!(decode("0000000000000000000000000I"), Ok(1));
        assert_eq!(decode("o000000000000000000000000z"), Ok(31));
    }

    #[test]
    fn rejects_bad_input() {
        assert_eq!(decode("ABC"), Err(InvalidUlid::Length(3)));
        assert_eq!(
            decode("0000000000000000000000000U"),
            Err(InvalidUlid::Character('U'))
        );
        assert_eq!(
            decode("80000000000000000000000000"),
            Err(InvalidUlid::Overflow)
        );
    }

    #[test]
    fn typed_ids_round_trip() {
        let id = AgentId::from_ulid(0xdead_beef);
        assert_eq!(AgentId::parse_ulid(&id.to_ulid()), Ok(id));
    }
}
