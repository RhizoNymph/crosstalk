//! The content-addressed blob store: the spec's [`BlobStore`] over the local
//! filesystem ([`FsBlobStore`]) and in memory ([`MemoryBlobStore`]).
//!
//! A body is stored under its [`MessageHash`], the BLAKE3 digest of its
//! bytes ([`message_hash`]); the store computes the key, so callers cannot
//! choose it, and every node names the same bytes with the same hash. Both
//! stores:
//!
//! - make `put` idempotent: putting stored bytes again, or putting them
//!   concurrently, returns `Ok` with the same hash;
//! - rehash on every `get`, and return [`BlobError::Corrupt`] (never the
//!   bytes) when the stored bytes do not hash to their key;
//! - return `Ok(None)` for a hash with no stored body;
//! - never put a body's bytes into a log record or an error value: logs
//!   and errors name blobs by hash only.
//!
//! **Retention.** The spec's `BlobStore` has no delete, and its invariant
//! `transport.blob.get-matches-map-model` describes a grow-only store, while
//! `BlobStore::get` says a `None` for a hash an event names means content
//! retention dropped the body. Neither store offers a deletion hook until
//! the spec defines content retention; a test that needs a dropped body
//! simply never puts it.
//!
//! [`BlobStore`]: crosstalk_spec::interfaces::l2_transport::BlobStore
//! [`MessageHash`]: crosstalk_spec::ids::MessageHash
//! [`BlobError::Corrupt`]: crosstalk_spec::interfaces::l2_transport::BlobError::Corrupt

mod digest;
mod fs;
mod memory;

#[cfg(test)]
mod tests;

pub use digest::message_hash;
pub use fs::{FsBlobStore, OpenError};
pub use memory::MemoryBlobStore;
