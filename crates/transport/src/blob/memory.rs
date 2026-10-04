//! [`MemoryBlobStore`]: the blob store in process memory, for tests and the
//! simulation.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::interfaces::l2_transport::{BlobError, BlobStore};

use super::digest::{matches, message_hash};

type Blobs = HashMap<MessageHash, Arc<[u8]>>;

/// A grow-only map from hash to bytes behind one mutex. Clones share the
/// map, so a clone handed to another task (or another simulated node) reads
/// what this one put. The lock is never held across an await.
#[derive(Debug, Clone, Default)]
pub struct MemoryBlobStore {
    blobs: Arc<Mutex<Blobs>>,
}

impl MemoryBlobStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of distinct bodies stored.
    pub fn len(&self) -> Result<usize, BlobError> {
        Ok(self.lock()?.len())
    }

    pub fn is_empty(&self) -> Result<bool, BlobError> {
        Ok(self.lock()?.is_empty())
    }

    /// A poisoned lock means a thread panicked while holding it; every
    /// critical section here is a single map operation, so that is a bug,
    /// reported as an unavailable store rather than a second panic.
    fn lock(&self) -> Result<MutexGuard<'_, Blobs>, BlobError> {
        self.blobs
            .lock()
            .map_err(|_poisoned| BlobError::Unavailable {
                reason: "memory blob store lock poisoned".to_owned(),
            })
    }

    fn put_now(&self, bytes: &[u8]) -> Result<MessageHash, BlobError> {
        let hash = message_hash(bytes);
        let mut blobs = self.lock()?;
        let stored = !blobs.contains_key(&hash);
        blobs.entry(hash).or_insert_with(|| Arc::from(bytes));
        drop(blobs);
        tracing::debug!(blob = %hash.digest().to_hex(), len = bytes.len(), stored, "blob put");
        Ok(hash)
    }

    fn get_now(&self, hash: MessageHash) -> Result<Option<Vec<u8>>, BlobError> {
        let Some(bytes) = self.lock()?.get(&hash).cloned() else {
            return Ok(None);
        };
        if !matches(hash, &bytes) {
            tracing::warn!(blob = %hash.digest().to_hex(), "stored blob does not match its hash");
            return Err(BlobError::Corrupt(hash));
        }
        Ok(Some(bytes.to_vec()))
    }

    /// Store `bytes` under `hash` without checking that they match it, so
    /// tests can exercise corruption detection.
    #[cfg(test)]
    pub(in crate::blob) fn insert_unchecked(&self, hash: MessageHash, bytes: &[u8]) {
        if let Ok(mut blobs) = self.blobs.lock() {
            blobs.insert(hash, Arc::from(bytes));
        }
    }
}

impl BlobStore for MemoryBlobStore {
    async fn put(&self, bytes: &[u8]) -> Result<MessageHash, BlobError> {
        self.put_now(bytes)
    }

    async fn get(&self, hash: MessageHash) -> Result<Option<Vec<u8>>, BlobError> {
        self.get_now(hash)
    }
}
