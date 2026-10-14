//! How spec values sit in the `transport` tables, and how database failures
//! become [`BusError`]s.
//!
//! Envelopes are stored as the bus codec's wire JSON (`text`, never
//! `jsonb`), next to the columns the bus routes and orders by: the ULID
//! text id, the subject's wire name and the time in microseconds.

use std::time::Duration;

use crosstalk_spec::events::{Envelope, Subject};
use crosstalk_spec::interfaces::l2_transport::BusError;
use crosstalk_spec::support::Timestamp;
use crosstalk_store::{DbFailure, classify};

use crate::codec::{self, Message};

/// An envelope ready to insert into `transport.events`.
#[derive(Debug, Clone)]
pub(crate) struct EventRow {
    pub(crate) id: String,
    pub(crate) subject: String,
    pub(crate) at: i64,
    pub(crate) envelope: String,
}

impl EventRow {
    pub(crate) fn encode(envelope: &Envelope) -> Result<Self, BusError> {
        let message = codec::encode(envelope)?;
        let text = String::from_utf8(message.bytes.to_vec()).map_err(|_| BusError::Encode {
            reason: "the envelope's JSON is not UTF-8".to_owned(),
        })?;
        Ok(Self {
            id: envelope.id.ulid_text(),
            subject: subject_name(envelope.event.subject()),
            at: micros(envelope.at)?,
            envelope: text,
        })
    }
}

/// A timestamp as the `bigint` microseconds the tables hold.
pub(crate) fn micros(at: Timestamp) -> Result<i64, BusError> {
    i64::try_from(at.as_micros()).map_err(|_| BusError::Encode {
        reason: "timestamp beyond the bus's bigint range".to_owned(),
    })
}

/// A stored `bigint` time. Negative values never pass the tables' checks;
/// one would read as the epoch.
pub(crate) fn timestamp(micros: i64) -> Timestamp {
    Timestamp::from_micros(u64::try_from(micros).unwrap_or(0))
}

/// A duration in whole microseconds, saturating.
pub(crate) fn duration_micros(duration: Duration) -> i64 {
    i64::try_from(duration.as_micros()).unwrap_or(i64::MAX)
}

/// The subject's wire name (`"content_matched"`).
pub(crate) fn subject_name(subject: Subject) -> String {
    match serde_json::to_value(subject) {
        Ok(serde_json::Value::String(name)) => name,
        // A unit variant with `rename_all` always serializes as a string.
        _ => format!("{subject:?}"),
    }
}

/// The subject a stored wire name names.
pub(crate) fn subject_from_name(name: &str) -> Option<Subject> {
    serde_json::from_value(serde_json::Value::String(name.to_owned())).ok()
}

/// Sorted, distinct wire names: how `transport.groups.subjects` holds a
/// group's subject set.
pub(crate) fn subject_set(subjects: &[Subject]) -> Vec<String> {
    let mut names: Vec<String> = subjects.iter().copied().map(subject_name).collect();
    names.sort();
    names.dedup();
    names
}

/// Decode a stored envelope strictly, as a delivery from any bus is: the
/// codec's decode, which also refuses an envelope whose event is not of
/// the subject it was routed under.
pub(crate) fn decode_envelope(subject: &str, text: &str) -> Result<Envelope, BusError> {
    let subject = subject_from_name(subject).ok_or_else(|| BusError::Decode {
        reason: "the stored subject is not one this node knows".to_owned(),
    })?;
    codec::decode(&Message {
        subject,
        id: None,
        bytes: text.as_bytes().into(),
    })
}

/// Decode a stored envelope strictly, routed under its own event's
/// subject (a dead letter keeps no subject of its own).
pub(crate) fn decode_stored(text: &str) -> Result<Envelope, BusError> {
    serde_json::from_str(text).map_err(|error| BusError::Decode {
        reason: codec::reason(&error),
    })
}

/// What a failed database call means to a bus caller: the database out of
/// reach is [`BusError::Disconnected`]; anything else is refused with the
/// classified failure (never the statement or its parameters).
pub(crate) fn bus_error(op: &'static str, error: &sqlx::Error) -> BusError {
    let failure = classify(error);
    if failure.is_unavailable() {
        BusError::Disconnected
    } else {
        BusError::PublishRejected {
            reason: format!("{op}: {failure}"),
        }
    }
}

/// Whether the failure is the database being out of reach.
pub(crate) fn unavailable(error: &sqlx::Error) -> bool {
    classify(error).is_unavailable()
}

/// The classified failure, for logs.
pub(crate) fn failure(error: &sqlx::Error) -> DbFailure {
    classify(error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subject_names_round_trip() {
        for subject in [
            Subject::ExchangeCaptured,
            Subject::ContentMatched,
            Subject::Changed,
            Subject::WatermarkAdvanced,
        ] {
            let name = subject_name(subject);
            assert_eq!(subject_from_name(&name), Some(subject), "{name}");
        }
        assert_eq!(subject_name(Subject::ContentMatched), "content_matched");
        assert_eq!(subject_from_name("no_such_subject"), None);
    }

    #[test]
    fn subject_sets_are_sorted_and_distinct() {
        let set = subject_set(&[
            Subject::WatermarkAdvanced,
            Subject::Changed,
            Subject::Changed,
        ]);
        assert_eq!(
            set,
            vec!["changed".to_owned(), "watermark_advanced".to_owned()]
        );
    }
}

/// A server-side cancel at a pipeline bound (`statement_timeout`,
/// `lock_timeout`) reads as the database not answering in time, as the
/// publish's own client-side timeout does: `Disconnected`, which the spool
/// takes and a stage retries, never a refusal.
#[cfg(test)]
mod timeout_tests {
    use std::borrow::Cow;
    use std::error::Error as StdError;
    use std::fmt;

    use crosstalk_spec::interfaces::l2_transport::BusError;
    use crosstalk_store::DbFailure;

    use super::{bus_error, failure};

    #[derive(Debug)]
    struct ServerError(&'static str);

    impl fmt::Display for ServerError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "SQLSTATE {}", self.0)
        }
    }

    impl StdError for ServerError {}

    impl sqlx::error::DatabaseError for ServerError {
        fn message(&self) -> &str {
            "canceling statement"
        }
        fn code(&self) -> Option<Cow<'_, str>> {
            Some(Cow::Borrowed(self.0))
        }
        fn as_error(&self) -> &(dyn StdError + Send + Sync + 'static) {
            self
        }
        fn as_error_mut(&mut self) -> &mut (dyn StdError + Send + Sync + 'static) {
            self
        }
        fn into_error(self: Box<Self>) -> Box<dyn StdError + Send + Sync + 'static> {
            self
        }
        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }

    fn server(code: &'static str) -> sqlx::Error {
        sqlx::Error::Database(Box::new(ServerError(code)))
    }

    #[test]
    fn a_statement_or_lock_timeout_is_disconnected() {
        for code in ["57014", "55P03"] {
            let error = server(code);
            assert!(failure(&error).is_timeout(), "{code}");
            assert_eq!(bus_error("ack", &error), BusError::Disconnected, "{code}");
        }
    }

    #[test]
    fn a_refused_statement_is_still_a_rejection() {
        let error = server("42P01");
        assert_eq!(
            failure(&error),
            DbFailure::Server {
                sqlstate: Some("42P01".to_owned())
            }
        );
        assert!(
            matches!(bus_error("ack", &error), BusError::PublishRejected { reason } if reason.contains("42P01"))
        );
    }
}
