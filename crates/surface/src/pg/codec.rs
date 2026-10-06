//! How L8's Postgres stores write spec values into columns and read them
//! back.
//!
//! - Ids are their ULID text, in `COLLATE "C"` columns: 26 Crockford base32
//!   characters whose byte order is the ids' numeric order.
//! - Times are microseconds since the epoch in a `bigint`.
//! - Spec values are their wire JSON, decoded through their checked
//!   `Deserialize`, so a stored value is always a valid one.

use crosstalk_spec::support::Timestamp;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// A stored value that does not decode, or a value that cannot be stored.
/// Either means the database holds something this code never wrote.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CodecError {
    #[error("{what}: {micros} microseconds is outside the stored range")]
    Time { what: &'static str, micros: i128 },
    #[error("{what}: {reason}")]
    Json { what: &'static str, reason: String },
    #[error("{what}: {reason}")]
    Value { what: &'static str, reason: String },
}

/// A time as its `bigint` column holds it.
pub fn micros(what: &'static str, at: Timestamp) -> Result<i64, CodecError> {
    i64::try_from(at.as_micros()).map_err(|_| CodecError::Time {
        what,
        micros: i128::from(at.as_micros()),
    })
}

/// A value's wire JSON.
pub fn to_json<T: Serialize>(what: &'static str, value: &T) -> Result<String, CodecError> {
    serde_json::to_string(value).map_err(|error| CodecError::Json {
        what,
        reason: error.to_string(),
    })
}

/// The value stored as wire JSON, through its checked decoding.
pub fn from_json<T: DeserializeOwned>(what: &'static str, text: &str) -> Result<T, CodecError> {
    serde_json::from_str(text).map_err(|error| CodecError::Json {
        what,
        reason: error.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn times_past_the_bigint_range_are_refused() {
        assert_eq!(micros("at", Timestamp::from_micros(7)), Ok(7));
        assert!(micros("at", Timestamp::from_micros(u64::MAX)).is_err());
    }

    #[test]
    fn json_round_trips_and_names_what_failed() {
        let text = to_json("value", &vec![1_u8, 2]).unwrap_or_default();
        assert_eq!(from_json::<Vec<u8>>("value", &text), Ok(vec![1, 2]));
        assert!(matches!(
            from_json::<Vec<u8>>("value", "{"),
            Err(CodecError::Json { what: "value", .. })
        ));
    }
}
