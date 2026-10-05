//! How L6's Postgres stores write spec values into columns and read them
//! back.
//!
//! - Ids are their ULID text (`AgentId::ulid_text`), in `COLLATE "C"`
//!   columns: the text is 26 Crockford base32 characters whose byte order
//!   is the ids' numeric order, so `ORDER BY id` is id order.
//! - Times are microseconds since the epoch in a `bigint`.
//! - Spec values with a wire form (rules, alerts, routes, filters) are their
//!   wire JSON, decoded through their checked `Deserialize`, so a stored
//!   value is always a valid one.

use crosstalk_spec::ids::EntityId;
use crosstalk_spec::support::Timestamp;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// A stored value that does not decode, or a value that cannot be stored.
/// Either means the database holds something this code never wrote.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CodecError {
    #[error("{what}: not ULID text: {text:?}")]
    Ulid { what: &'static str, text: String },
    #[error("{what}: {micros} microseconds is outside the stored range")]
    Time { what: &'static str, micros: i128 },
    #[error("{what}: {reason}")]
    Json { what: &'static str, reason: String },
    #[error("{what}: {reason}")]
    Value { what: &'static str, reason: String },
}

/// An id as its column holds it.
pub fn id_text<I: EntityId + IdText>(id: I) -> String {
    id.text()
}

/// The id a column holds.
pub fn id_of<I: IdText>(what: &'static str, text: &str) -> Result<I, CodecError> {
    I::parse(text).ok_or_else(|| CodecError::Ulid {
        what,
        text: text.to_owned(),
    })
}

/// The ULID text form every entity id has; the spec gives each id type its
/// own inherent `ulid_text`/`from_ulid_text`, so this bridges them.
pub trait IdText: Sized {
    fn text(self) -> String;
    fn parse(text: &str) -> Option<Self>;
}

macro_rules! id_text {
    ($($id:ty),* $(,)?) => {
        $(
            impl IdText for $id {
                fn text(self) -> String {
                    self.ulid_text()
                }

                fn parse(text: &str) -> Option<Self> {
                    <$id>::from_ulid_text(text).ok()
                }
            }
        )*
    };
}

id_text!(
    crosstalk_spec::ids::AgentId,
    crosstalk_spec::ids::ChannelId,
    crosstalk_spec::ids::TransmissionId,
    crosstalk_spec::ids::TopicId,
    crosstalk_spec::ids::AlertId,
    crosstalk_spec::ids::AlertRuleId,
);

/// A time as its `bigint` column holds it.
pub fn micros(what: &'static str, at: Timestamp) -> Result<i64, CodecError> {
    i64::try_from(at.as_micros()).map_err(|_| CodecError::Time {
        what,
        micros: i128::from(at.as_micros()),
    })
}

/// The time a `bigint` column holds.
pub fn timestamp(what: &'static str, micros: i64) -> Result<Timestamp, CodecError> {
    u64::try_from(micros)
        .map(Timestamp::from_micros)
        .map_err(|_| CodecError::Time {
            what,
            micros: i128::from(micros),
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

/// A count or revision held in an `integer` column.
pub fn to_i32(what: &'static str, value: u32) -> Result<i32, CodecError> {
    i32::try_from(value).map_err(|_| CodecError::Value {
        what,
        reason: format!("{value} does not fit a stored integer"),
    })
}

/// A count or revision read from an `integer` column.
pub fn to_u32(what: &'static str, value: i32) -> Result<u32, CodecError> {
    u32::try_from(value).map_err(|_| CodecError::Value {
        what,
        reason: format!("{value} is negative"),
    })
}
