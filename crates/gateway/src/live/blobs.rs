//! The blob store a [`Live`](super::Live) process stores bodies in: in
//! memory (tests, demos, dataset replay) or on the local filesystem (a UI
//! that keeps what it captured across restarts).

use std::path::PathBuf;

use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::interfaces::l2_transport::{BlobError, BlobStore};
use crosstalk_transport::blob::{FsBlobStore, MemoryBlobStore, OpenError};

/// Which blob store to open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobConfig {
    Memory,
    /// An [`FsBlobStore`] rooted here, created if missing.
    Fs {
        root: PathBuf,
    },
}

/// One of the two blob stores, behind the one `BlobStore` the pipeline and
/// the surface share. Clones share the store.
#[derive(Debug, Clone)]
pub enum LiveBlobs {
    Memory(MemoryBlobStore),
    Fs(FsBlobStore),
}

impl LiveBlobs {
    /// Open the store `config` names.
    pub async fn open(config: &BlobConfig) -> Result<Self, OpenError> {
        match config {
            BlobConfig::Memory => Ok(Self::Memory(MemoryBlobStore::new())),
            BlobConfig::Fs { root } => FsBlobStore::open(root.clone()).await.map(Self::Fs),
        }
    }
}

impl BlobStore for LiveBlobs {
    async fn put(&self, bytes: &[u8]) -> Result<MessageHash, BlobError> {
        match self {
            Self::Memory(store) => store.put(bytes).await,
            Self::Fs(store) => store.put(bytes).await,
        }
    }

    async fn get(&self, hash: MessageHash) -> Result<Option<Vec<u8>>, BlobError> {
        match self {
            Self::Memory(store) => store.get(hash).await,
            Self::Fs(store) => store.get(hash).await,
        }
    }
}
