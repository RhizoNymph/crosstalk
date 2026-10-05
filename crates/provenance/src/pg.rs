//! Postgres conversions every L4 table shares: ids and hashes as `bytea`,
//! times, offsets and fingerprints as `bigint`, and driver errors
//! classified.

use crosstalk_spec::derived::provenance::fingerprint::Fingerprint;
use crosstalk_spec::ids::{EntityId, MessageHash};
use crosstalk_spec::support::{Blake3, Timestamp};
use crosstalk_store::{DbFailure, classify};

/// A value read from or bound to Postgres that does not fit its column.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{what} out of range")]
pub struct OutOfRange {
    pub what: &'static str,
}

/// An entity id as 16 big-endian bytes.
pub fn id_bytes<I: EntityId>(id: I) -> Vec<u8> {
    id.as_ulid().to_be_bytes().to_vec()
}

/// An entity id from 16 big-endian bytes.
pub fn id_from<I: EntityId>(bytes: &[u8]) -> Result<I, OutOfRange> {
    let array: [u8; 16] = bytes.try_into().map_err(|_| OutOfRange { what: "an id" })?;
    Ok(I::from_ulid(u128::from_be_bytes(array)))
}

/// A message hash as its 32 digest bytes.
pub fn hash_bytes(hash: MessageHash) -> Vec<u8> {
    hash.digest().as_bytes().to_vec()
}

/// A message hash from 32 digest bytes.
pub fn hash_from(bytes: &[u8]) -> Result<MessageHash, OutOfRange> {
    let array: [u8; 32] = bytes.try_into().map_err(|_| OutOfRange {
        what: "a message hash",
    })?;
    Ok(MessageHash::from_digest(Blake3::from_bytes(array)))
}

/// A fingerprint's bit pattern as a `bigint`.
pub fn fingerprint_i64(fingerprint: Fingerprint) -> i64 {
    i64::from_ne_bytes(fingerprint.0.to_ne_bytes())
}

/// A fingerprint from its `bigint` bit pattern.
pub fn fingerprint_from(value: i64) -> Fingerprint {
    Fingerprint(u64::from_ne_bytes(value.to_ne_bytes()))
}

/// A time as microseconds in a `bigint`.
pub fn time_i64(at: Timestamp) -> Result<i64, OutOfRange> {
    i64::try_from(at.as_micros()).map_err(|_| OutOfRange {
        what: "a timestamp",
    })
}

/// A time from microseconds in a `bigint`.
pub fn time_from(value: i64) -> Result<Timestamp, OutOfRange> {
    u64::try_from(value)
        .map(Timestamp::from_micros)
        .map_err(|_| OutOfRange {
            what: "a timestamp",
        })
}

/// `now - retention` as a `bigint` bound: what counts at `now` is at or
/// after it.
pub fn horizon(now: Timestamp, retention_micros: u64) -> i64 {
    let horizon = i128::from(now.as_micros()) - i128::from(retention_micros);
    i64::try_from(horizon).unwrap_or(if horizon < 0 { i64::MIN } else { i64::MAX })
}

/// A `u32` from a `bigint` or `integer` column.
pub fn u32_from(value: i64, what: &'static str) -> Result<u32, OutOfRange> {
    u32::try_from(value).map_err(|_| OutOfRange { what })
}

/// How a driver error reads to the layer: reaching the database, or the
/// statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    /// Lost connection, pool timeout, serialization failure or deadlock:
    /// retrying later can succeed.
    Transient(DbFailure),
    /// The statement was refused.
    Refused(DbFailure),
}

pub fn failure(error: &sqlx::Error) -> Failure {
    let failure = classify(error);
    if failure.is_unavailable() || failure.is_retryable() {
        Failure::Transient(failure)
    } else {
        Failure::Refused(failure)
    }
}
