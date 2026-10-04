//! Writing a normalized exchange's bodies to the blob store.
//!
//! Each message is stored as its canonical encoding, never as provider wire
//! bytes, so the key the blob store computes is the message's hash
//! (`canonical.capture.blob-is-canonical-encoding`); each media blob is
//! stored as its decoded bytes. A store whose key disagrees with the hash
//! the exchange carries is reported, not trusted. Publishing
//! `ExchangeCaptured` only after [`store`] returns `Ok` is the capture
//! task's (roadmap P3).

use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::interfaces::l2_transport::{BlobError, BlobStore};

use crate::assemble::Normalization;
use crate::encoding;

/// Why a normalized exchange's bodies are not all stored.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreError {
    #[error("the blob store failed: {error:?}")]
    Blob { error: BlobError },
    #[error("the blob store keyed a body by {stored:?}, not its hash {expected:?}")]
    HashMismatch {
        expected: MessageHash,
        stored: MessageHash,
    },
}

/// Puts every message body and media blob of `normalization` into `blobs`,
/// in order, stopping at the first failure.
pub async fn store<B: BlobStore>(
    blobs: &B,
    normalization: &Normalization,
) -> Result<(), StoreError> {
    let exchange = normalization.exchange.exchange.meta.id;
    for message in &normalization.exchange.messages {
        put(blobs, message.hash, &encoding::encode(&message.body)).await?;
    }
    for media in &normalization.media {
        put(blobs, media.hash, &media.bytes).await?;
    }
    tracing::debug!(
        exchange = %exchange.ulid_text(),
        messages = normalization.exchange.messages.len(),
        media = normalization.media.len(),
        "exchange bodies stored"
    );
    Ok(())
}

async fn put<B: BlobStore>(
    blobs: &B,
    expected: MessageHash,
    bytes: &[u8],
) -> Result<(), StoreError> {
    let stored = blobs
        .put(bytes)
        .await
        .map_err(|error| StoreError::Blob { error })?;
    if stored != expected {
        tracing::warn!(
            expected = %expected.digest().to_hex(),
            stored = %stored.digest().to_hex(),
            "blob store keyed a body by another hash"
        );
        return Err(StoreError::HashMismatch { expected, stored });
    }
    Ok(())
}
