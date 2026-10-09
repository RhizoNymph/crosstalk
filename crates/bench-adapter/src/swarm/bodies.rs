//! Message bodies by hash: the gateway's blob store holds each body's
//! canonical encoding under its `MessageHash`, and the exchange log names
//! only hashes. [`Bodies`] reads and decodes them; [`BlobBodies`] over the
//! gateway's `FsBlobStore`, [`MemoryBodies`] over messages in memory
//! (tests). [`Cached`] keeps each decoded body once.

use std::collections::HashMap;
use std::path::Path;

use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::interfaces::l2_transport::{BlobError, BlobStore};
use crosstalk_spec::observed::message::{Message, encoding};
use crosstalk_transport::blob::FsBlobStore;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BodyError {
    #[error("the blob store has no body for {0:?}")]
    Missing(MessageHash),
    #[error("the body stored for {hash:?} is not a canonical message: {reason}")]
    Undecodable { hash: MessageHash, reason: String },
    #[error("reading body {hash:?}: {reason}")]
    Store { hash: MessageHash, reason: String },
}

/// Somewhere message bodies can be read from.
pub trait Bodies {
    fn message(&mut self, hash: MessageHash) -> Result<Message, BodyError>;

    /// A media blob's raw bytes (a `Media` part names it by hash).
    fn media(&mut self, hash: MessageHash) -> Result<Vec<u8>, BodyError>;
}

/// Messages held in memory.
#[derive(Debug, Clone, Default)]
pub struct MemoryBodies {
    messages: HashMap<MessageHash, Message>,
}

impl MemoryBodies {
    pub fn new(messages: impl IntoIterator<Item = Message>) -> Self {
        Self {
            messages: messages
                .into_iter()
                .map(|message| (message.hash, message))
                .collect(),
        }
    }
}

impl Bodies for MemoryBodies {
    fn message(&mut self, hash: MessageHash) -> Result<Message, BodyError> {
        self.messages
            .get(&hash)
            .cloned()
            .ok_or(BodyError::Missing(hash))
    }

    /// Holds no media.
    fn media(&mut self, hash: MessageHash) -> Result<Vec<u8>, BodyError> {
        Err(BodyError::Missing(hash))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OpenBodiesError {
    #[error("starting the blob reader's runtime: {0}")]
    Runtime(#[source] std::io::Error),
    #[error("opening the blob store at {path}: {source}")]
    Open {
        path: String,
        #[source]
        source: crosstalk_transport::blob::OpenError,
    },
}

/// The gateway's blob directory, read through its own `FsBlobStore` on a
/// private single-threaded runtime.
pub struct BlobBodies {
    runtime: tokio::runtime::Runtime,
    store: FsBlobStore,
}

impl BlobBodies {
    pub fn open(root: &Path) -> Result<Self, OpenBodiesError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .map_err(OpenBodiesError::Runtime)?;
        let store = runtime
            .block_on(FsBlobStore::open(root.to_owned()))
            .map_err(|source| OpenBodiesError::Open {
                path: root.display().to_string(),
                source,
            })?;
        Ok(Self { runtime, store })
    }
}

impl Bodies for BlobBodies {
    fn message(&mut self, hash: MessageHash) -> Result<Message, BodyError> {
        let bytes = self
            .runtime
            .block_on(self.store.get(hash))
            .map_err(|error: BlobError| BodyError::Store {
                hash,
                reason: format!("{error:?}"),
            })?
            .ok_or(BodyError::Missing(hash))?;
        let body = encoding::decode(&bytes).map_err(|error| BodyError::Undecodable {
            hash,
            reason: format!("{error:?}"),
        })?;
        Ok(Message { hash, body })
    }

    fn media(&mut self, hash: MessageHash) -> Result<Vec<u8>, BodyError> {
        let bytes = self
            .runtime
            .block_on(self.store.get(hash))
            .map_err(|error: BlobError| BodyError::Store {
                hash,
                reason: format!("{error:?}"),
            })?
            .ok_or(BodyError::Missing(hash))?;
        Ok(bytes.to_vec())
    }
}

/// Any [`Bodies`], each body decoded once.
pub struct Cached<B> {
    inner: B,
    seen: HashMap<MessageHash, Message>,
}

impl<B: Bodies> Cached<B> {
    pub fn new(inner: B) -> Self {
        Self {
            inner,
            seen: HashMap::new(),
        }
    }

    /// A media blob's bytes, uncached.
    pub fn media(&mut self, hash: MessageHash) -> Result<Vec<u8>, BodyError> {
        self.inner.media(hash)
    }

    pub fn get(&mut self, hash: MessageHash) -> Result<&Message, BodyError> {
        if !self.seen.contains_key(&hash) {
            let message = self.inner.message(hash)?;
            self.seen.insert(hash, message);
        }
        self.seen.get(&hash).ok_or(BodyError::Missing(hash))
    }
}
