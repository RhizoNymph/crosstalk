//! τ²-bench's message times: ISO 8601 local times written by Python's
//! `datetime.isoformat()` (`2025-06-04T12:22:38.915138`), read as UTC. The
//! fraction may be absent or of any length; a `Z` or `+00:00` suffix is
//! accepted, any other offset is not.

use crosstalk_spec::support::Timestamp;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{text:?} is not an ISO 8601 UTC time")]
pub struct TimeError {
    pub text: String,
}

/// The time `text` names.
pub fn parse_time(text: &str) -> Result<Timestamp, TimeError> {
    let error = || TimeError {
        text: text.to_owned(),
    };
    let body = text
        .strip_suffix('Z')
        .or_else(|| text.strip_suffix("+00:00"))
        .unwrap_or(text);
    let (seconds, fraction) = match body.split_once('.') {
        Some((seconds, fraction)) => (seconds, fraction),
        None => (body, ""),
    };
    if seconds.len() != 19 || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return Err(error());
    }
    let micros: String = fraction.chars().chain("000000".chars()).take(6).collect();
    Timestamp::parse_rfc3339(&format!("{seconds}.{micros}Z")).map_err(|_| error())
}
