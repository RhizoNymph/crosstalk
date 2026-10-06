//! How spec values are stored in the topology schema, and back.
//!
//! - Ids are ULID text (`ulid_text`), which sorts as the ULID does.
//! - Times and bucket starts are Unix microseconds as `bigint`; a time past
//!   `i64::MAX` microseconds (year 294,247) cannot be stored and is a
//!   [`CodecError`].
//! - Counts are `bigint`, positive.
//! - A route is its serde JSON, which is deterministic for the enum, so
//!   equal routes are equal text and group together in SQL.
//! - An access kind is 0 (write) or 1 (read); a verdict is 0 (genuine), 1
//!   (false detection) or NULL (withdrawn).

use std::num::NonZeroU64;

use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::ids::{AgentId, ChannelId, EventId, ResourceId, TopicId, TransmissionId};
use crosstalk_spec::support::{TimeWindow, Timestamp};

/// A value that does not fit its column, or a column that does not decode.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CodecError {
    #[error("{what} {value} does not fit a bigint")]
    TooLarge { what: &'static str, value: u64 },
    #[error("stored {what} {value} is negative or zero")]
    NotPositive { what: &'static str, value: i64 },
    #[error("stored {what} is not a ULID: {text}")]
    Ulid { what: &'static str, text: String },
    #[error("stored route does not decode: {reason}")]
    Route { reason: String },
    #[error("route does not encode: {reason}")]
    RouteEncode { reason: String },
    #[error("stored {what} code {code} is unknown")]
    Code { what: &'static str, code: i16 },
    #[error("stored bucket at {start} is not a window")]
    Bucket { start: i64 },
}

/// `micros` as a `bigint`.
pub fn micros(what: &'static str, value: u64) -> Result<i64, CodecError> {
    i64::try_from(value).map_err(|_| CodecError::TooLarge { what, value })
}

/// A time as stored.
pub fn time(at: Timestamp) -> Result<i64, CodecError> {
    micros("time", at.as_micros())
}

/// A stored time.
pub fn timestamp(what: &'static str, stored: i64) -> Result<Timestamp, CodecError> {
    u64::try_from(stored)
        .map(Timestamp::from_micros)
        .map_err(|_| CodecError::NotPositive {
            what,
            value: stored,
        })
}

/// A count as stored.
pub fn count(what: &'static str, value: NonZeroU64) -> Result<i64, CodecError> {
    micros(what, value.get())
}

/// A stored count, which is positive.
pub fn stored_count(what: &'static str, stored: i64) -> Result<NonZeroU64, CodecError> {
    u64::try_from(stored)
        .ok()
        .and_then(NonZeroU64::new)
        .ok_or(CodecError::NotPositive {
            what,
            value: stored,
        })
}

/// A stored count that may be zero (a sum).
pub fn stored_sum(what: &'static str, stored: i64) -> Result<u64, CodecError> {
    u64::try_from(stored).map_err(|_| CodecError::NotPositive {
        what,
        value: stored,
    })
}

/// A version as stored.
pub fn version(version: TopicModelVersion) -> i64 {
    i64::from(version.0)
}

/// A stored version.
pub fn stored_version(stored: i64) -> Result<TopicModelVersion, CodecError> {
    u32::try_from(stored)
        .map(TopicModelVersion)
        .map_err(|_| CodecError::NotPositive {
            what: "version",
            value: stored,
        })
}

macro_rules! id_codec {
    ($store:ident, $load:ident, $ty:ty, $what:literal) => {
        #[doc = concat!("A ", $what, " id as stored.")]
        pub fn $store(id: $ty) -> String {
            id.ulid_text()
        }

        #[doc = concat!("A stored ", $what, " id.")]
        pub fn $load(text: &str) -> Result<$ty, CodecError> {
            <$ty>::from_ulid_text(text).map_err(|_| CodecError::Ulid {
                what: $what,
                text: text.to_owned(),
            })
        }
    };
}

id_codec!(agent, stored_agent, AgentId, "agent");
id_codec!(channel, stored_channel, ChannelId, "channel");
id_codec!(event, stored_event, EventId, "event");
id_codec!(resource, stored_resource, ResourceId, "resource");
id_codec!(topic, stored_topic, TopicId, "topic");
id_codec!(
    transmission,
    stored_transmission,
    TransmissionId,
    "transmission"
);

/// An edge bucket's topic column: the topic's id, or `''` for an outlier
/// (the column is part of the key, so never NULL).
pub fn bucket_topic(topic: Option<TopicId>) -> String {
    topic.map(self::topic).unwrap_or_default()
}

/// A stored edge bucket topic.
pub fn stored_bucket_topic(text: &str) -> Result<Option<TopicId>, CodecError> {
    if text.is_empty() {
        Ok(None)
    } else {
        stored_topic(text).map(Some)
    }
}

/// A route as stored.
pub fn route(route: &Route) -> Result<String, CodecError> {
    serde_json::to_string(route).map_err(|error| CodecError::RouteEncode {
        reason: error.to_string(),
    })
}

/// A stored route.
pub fn stored_route(text: &str) -> Result<Route, CodecError> {
    serde_json::from_str(text).map_err(|error| CodecError::Route {
        reason: error.to_string(),
    })
}

/// An access kind as stored.
pub fn op(op: AccessKind) -> i16 {
    match op {
        AccessKind::Write => 0,
        AccessKind::Read => 1,
    }
}

/// A stored access kind.
pub fn stored_op(code: i16) -> Result<AccessKind, CodecError> {
    match code {
        0 => Ok(AccessKind::Write),
        1 => Ok(AccessKind::Read),
        code => Err(CodecError::Code { what: "op", code }),
    }
}

/// A verdict as stored.
pub fn verdict(verdict: Option<Verdict>) -> Option<i16> {
    verdict.map(|verdict| match verdict {
        Verdict::Genuine => 0,
        Verdict::FalseDetection => 1,
    })
}

/// The bucket starting at `start`, `width` long.
pub fn bucket(start: i64, width: NonZeroU64) -> Result<TimeWindow, CodecError> {
    let begin = u64::try_from(start).map_err(|_| CodecError::Bucket { start })?;
    let end = begin
        .checked_add(width.get())
        .ok_or(CodecError::Bucket { start })?;
    TimeWindow::new(Timestamp::from_micros(begin), Timestamp::from_micros(end))
        .map_err(|_| CodecError::Bucket { start })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crosstalk_spec::derived::flow::transmission::{DelegationDirection, DirectCarrier};

    #[test]
    fn routes_round_trip_and_equal_routes_are_equal_text() {
        let routes = [
            Route::Channel(ChannelId::from_ulid(7)),
            Route::Delegation(DelegationDirection::ParentToChild),
            Route::Direct(DirectCarrier::UserTurn),
            Route::Unobserved,
        ];
        for one in &routes {
            let text = route(one).expect("encodes");
            assert_eq!(&stored_route(&text).expect("decodes"), one);
            assert_eq!(route(&one.clone()).expect("encodes"), text);
        }
    }

    #[test]
    fn ids_round_trip_as_ulid_text() {
        let id = AgentId::from_ulid(42);
        assert_eq!(stored_agent(&agent(id)), Ok(id));
        assert!(stored_agent("not a ulid").is_err());
        assert_eq!(stored_bucket_topic(""), Ok(None));
        let topic_id = TopicId::from_ulid(9);
        assert_eq!(
            stored_bucket_topic(&bucket_topic(Some(topic_id))),
            Ok(Some(topic_id))
        );
    }

    #[test]
    fn times_past_bigint_are_refused() {
        assert!(time(Timestamp::from_micros(u64::MAX)).is_err());
        assert_eq!(time(Timestamp::from_micros(5)), Ok(5));
        assert!(stored_count("n", 0).is_err());
        assert!(stored_count("n", -1).is_err());
    }
}
