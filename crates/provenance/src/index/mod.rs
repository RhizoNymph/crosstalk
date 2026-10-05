//! The fingerprint index on Postgres: [`PgFingerprintIndex`], the spec's
//! `FingerprintIndex` over the `provenance.postings`, `observations` and
//! `observed` tables.
//!
//! It agrees with `crosstalk-memory`'s reference index on every operation
//! (the model-based harness runs both): postings of originated spans,
//! one observation per scanned text counting each of its distinct
//! fingerprints once, frequencies over the retention window before the
//! `now` each call is given, the boilerplate cutoff on insert and on
//! lookup, observations aged out on every write, and shard ownership
//! checked before anything is written.

mod pg;

pub use self::pg::PgFingerprintIndex;
