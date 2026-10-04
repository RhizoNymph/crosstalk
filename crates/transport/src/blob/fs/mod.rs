//! [`FsBlobStore`]: the blob store on a local (or shared) filesystem.

mod io;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::interfaces::l2_transport::{BlobError, BlobStore};

pub use io::OpenError;
use io::{Fault, PutOutcome, Root};

/// Bodies as files under one directory, content-addressed by BLAKE3:
/// `<root>/<first two hex digits>/<other 62>`.
///
/// Writes are atomic (a temporary file in the same directory, synced, then
/// renamed into place and the directory synced), so a reader never sees a
/// partial body and an acknowledged put survives a crash. Every `get`
/// rehashes the file and reports [`BlobError::Corrupt`] on a mismatch; a
/// later `put` of the right bytes repairs the file. Several stores (in this
/// process or others) may share a root.
///
/// Each operation is one blocking task on tokio's blocking pool; nothing
/// else in the store touches threads. It must be used inside a tokio
/// runtime.
#[derive(Debug, Clone)]
pub struct FsBlobStore {
    root: Arc<Root>,
}

impl FsBlobStore {
    /// Open the store at `root`, creating the directory if it is missing.
    pub async fn open(root: impl Into<PathBuf>) -> Result<Self, OpenError> {
        let root = root.into();
        let opened = blocking(move || io::open(&root))
            .await
            .map_err(|Cancelled| OpenError::Cancelled)??;
        tracing::info!(root = %opened.path().display(), "blob store opened");
        Ok(Self {
            root: Arc::new(opened),
        })
    }

    /// The resolved, absolute store directory.
    pub fn root(&self) -> &Path {
        self.root.path()
    }
}

impl BlobStore for FsBlobStore {
    async fn put(&self, bytes: &[u8]) -> Result<MessageHash, BlobError> {
        let root = Arc::clone(&self.root);
        let len = bytes.len();
        // The blocking task needs owned bytes.
        let bytes = bytes.to_vec();
        let (hash, outcome) = blocking(move || io::put(&root, &bytes)).await??;
        let blob = hash.digest().to_hex();
        if outcome == PutOutcome::Repaired {
            tracing::warn!(blob = %blob, len, "replaced a corrupt blob file");
        } else {
            tracing::debug!(blob = %blob, len, outcome = outcome.as_str(), "blob put");
        }
        Ok(hash)
    }

    async fn get(&self, hash: MessageHash) -> Result<Option<Vec<u8>>, BlobError> {
        let root = Arc::clone(&self.root);
        Ok(blocking(move || io::get(&root, hash)).await??)
    }
}

/// The blocking task did not run to completion because the runtime is
/// shutting down.
struct Cancelled;

impl From<Cancelled> for BlobError {
    fn from(Cancelled: Cancelled) -> Self {
        Self::Unavailable {
            reason: "the runtime shut down before the blob operation finished".to_owned(),
        }
    }
}

impl From<Fault> for BlobError {
    fn from(fault: Fault) -> Self {
        match fault {
            Fault::Corrupt(hash) => {
                tracing::warn!(blob = %hash.digest().to_hex(), "stored blob does not match its hash");
                Self::Corrupt(hash)
            }
            other => {
                tracing::error!(error = %other, "blob store operation failed");
                Self::Unavailable {
                    reason: other.to_string(),
                }
            }
        }
    }
}

/// Run `work` on tokio's blocking pool: the store's one boundary between
/// async and threads. A panic in `work` is a bug in this module and is
/// resumed on the caller rather than reported as a store error.
async fn blocking<T, F>(work: F) -> Result<T, Cancelled>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    match tokio::task::spawn_blocking(work).await {
        Ok(value) => Ok(value),
        Err(error) => match error.try_into_panic() {
            Ok(payload) => std::panic::resume_unwind(payload),
            Err(_cancelled) => Err(Cancelled),
        },
    }
}
