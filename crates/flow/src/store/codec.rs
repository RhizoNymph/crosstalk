//! How spec values become column values and back.
//!
//! - Entity ids are their ULID text ([`id_text`], [`parse_id`]).
//! - Times are microseconds since the epoch as `BIGINT` ([`micros`],
//!   [`timestamp`]).
//! - Spec values are the JSON text of their wire form ([`json`],
//!   [`from_json`]), in `TEXT` columns.
//!
//! Every conversion is checked; a value that does not decode is a
//! [`CodecError`], never a panic.

use crosstalk_spec::ids::{EntityId, InvalidUlidText};
use crosstalk_spec::support::Timestamp;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// A stored value that did not convert.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    /// A value did not encode to JSON, or a column did not decode.
    #[error("{what}: {source}")]
    Json {
        /// What was being converted.
        what: &'static str,
        /// The serde error.
        #[source]
        source: serde_json::Error,
    },
    /// A column did not hold ULID text.
    #[error("{what}: not an id ({error:?})")]
    Id {
        /// The column.
        what: &'static str,
        /// Why.
        error: InvalidUlidText,
    },
    /// A time outside `BIGINT`'s range, or a negative stored time.
    #[error("{what}: time {micros} is out of range")]
    Time {
        /// The column.
        what: &'static str,
        /// The value in microseconds, as far as it was known.
        micros: i128,
    },
    /// A count that does not fit its type.
    #[error("{what}: {value} is out of range")]
    Count {
        /// The column.
        what: &'static str,
        /// The value.
        value: i128,
    },
}

/// An entity id's column value.
pub(crate) fn id_text<I: EntityId>(id: I) -> String {
    crosstalk_spec::ids::ChannelId::from_ulid(id.as_ulid()).ulid_text()
}

/// The entity id a column holds.
pub(crate) fn parse_id<I: EntityId>(what: &'static str, text: &str) -> Result<I, CodecError> {
    crosstalk_spec::ids::ChannelId::from_ulid_text(text)
        .map(|id| I::from_ulid(id.as_ulid()))
        .map_err(|error| CodecError::Id { what, error })
}

/// A time's column value.
pub(crate) fn micros(what: &'static str, at: Timestamp) -> Result<i64, CodecError> {
    i64::try_from(at.as_micros()).map_err(|_| CodecError::Time {
        what,
        micros: i128::from(at.as_micros()),
    })
}

/// The time a column holds.
pub(crate) fn timestamp(what: &'static str, micros: i64) -> Result<Timestamp, CodecError> {
    u64::try_from(micros)
        .map(Timestamp::from_micros)
        .map_err(|_| CodecError::Time {
            what,
            micros: i128::from(micros),
        })
}

/// A spec value's JSON text.
pub(crate) fn json<T: Serialize + ?Sized>(
    what: &'static str,
    value: &T,
) -> Result<String, CodecError> {
    serde_json::to_string(value).map_err(|source| CodecError::Json { what, source })
}

/// The spec value a JSON column holds.
pub(crate) fn from_json<T: DeserializeOwned>(
    what: &'static str,
    text: &str,
) -> Result<T, CodecError> {
    serde_json::from_str(text).map_err(|source| CodecError::Json { what, source })
}

/// A count read from the database as `BIGINT`.
pub(crate) fn count(what: &'static str, value: i64) -> Result<u64, CodecError> {
    u64::try_from(value).map_err(|_| CodecError::Count {
        what,
        value: i128::from(value),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crosstalk_spec::ids::{AgentId, ChannelId, ResourceId};

    #[test]
    fn ids_round_trip_through_their_text() -> Result<(), CodecError> {
        for raw in [0u128, 1, 0x0C4A_0003, u128::MAX >> 2, u128::MAX] {
            let id = ResourceId::from_ulid(raw);
            let text = id_text(id);
            assert_eq!(text.len(), 26);
            assert_eq!(parse_id::<ResourceId>("id", &text)?, id);
        }
        Ok(())
    }

    #[test]
    fn id_text_orders_like_the_id() {
        let ids = [0u128, 7, 31, 32, 1 << 80, (1 << 80) + 1, u128::MAX];
        for pair in ids.windows(2) {
            let (a, b) = (AgentId::from_ulid(pair[0]), AgentId::from_ulid(pair[1]));
            assert!(id_text(a) < id_text(b), "{a:?} < {b:?}");
        }
    }

    #[test]
    fn a_bad_id_column_is_a_typed_error() {
        assert!(matches!(
            parse_id::<ChannelId>("channels.id", "not-an-id"),
            Err(CodecError::Id {
                what: "channels.id",
                ..
            })
        ));
    }

    #[test]
    fn times_round_trip_and_refuse_what_bigint_cannot_hold() -> Result<(), CodecError> {
        let at = Timestamp::from_micros(1_700_000_000_000_000);
        assert_eq!(timestamp("t", micros("t", at)?)?, at);
        assert!(micros("t", Timestamp::from_micros(u64::MAX)).is_err());
        assert!(timestamp("t", -1).is_err());
        Ok(())
    }
}
