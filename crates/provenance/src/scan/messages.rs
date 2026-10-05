//! Where the scanner reads message bodies: the blob store, decoded with the
//! spec's canonical encoding.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::interfaces::l2_transport::{BlobError, BlobStore};
use crosstalk_spec::observed::message::{Message, encoding};

/// Why a body could not be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LoadError {
    /// The store could not be reached; retrying later can succeed.
    #[error("the blob store is unavailable: {reason}")]
    Unavailable { reason: String },
    /// The stored bytes do not hash to their key.
    #[error("the body of {0:?} is corrupt")]
    Corrupt(MessageHash),
    /// The bytes are not a canonical message encoding.
    #[error("the body of {0:?} is not a message encoding")]
    Undecodable(MessageHash),
}

impl LoadError {
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Unavailable { .. })
    }
}

/// Message bodies by hash.
pub trait MessageSource {
    /// The message stored under `hash`; `None` when no body is stored.
    fn message(
        &self,
        hash: MessageHash,
    ) -> impl Future<Output = Result<Option<Message>, LoadError>> + Send;
}

/// Bodies from a spec `BlobStore`.
#[derive(Debug, Clone)]
pub struct BlobMessages<B> {
    blobs: B,
}

impl<B> BlobMessages<B> {
    pub fn new(blobs: B) -> Self {
        Self { blobs }
    }
}

impl<B: BlobStore + Sync> MessageSource for BlobMessages<B> {
    async fn message(&self, hash: MessageHash) -> Result<Option<Message>, LoadError> {
        let bytes = match self.blobs.get(hash).await {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return Ok(None),
            Err(BlobError::Unavailable { reason }) => {
                return Err(LoadError::Unavailable { reason });
            }
            Err(BlobError::Corrupt(hash)) => return Err(LoadError::Corrupt(hash)),
        };
        let body = encoding::decode(&bytes).map_err(|_| LoadError::Undecodable(hash))?;
        // `decode` accepts exactly the bytes `encode` writes, so the bytes'
        // digest is the body's hash.
        let stored = encoding::hash_bytes(&bytes);
        if stored != hash {
            return Err(LoadError::Corrupt(hash));
        }
        Ok(Some(Message { hash, body }))
    }
}

/// Bodies held in memory (tests).
#[derive(Debug, Clone, Default)]
pub struct MemoryMessages {
    messages: Arc<std::sync::RwLock<HashMap<MessageHash, Message>>>,
}

impl MemoryMessages {
    pub fn new() -> Self {
        Self::default()
    }

    /// Store `message` under its hash.
    pub fn put(&self, message: Message) {
        self.messages
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(message.hash, message);
    }

    /// The message stored under `hash`.
    pub fn get(&self, hash: MessageHash) -> Option<Message> {
        self.messages
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&hash)
            .cloned()
    }

    /// Forget `hash` (content retention dropped it).
    pub fn drop_body(&self, hash: MessageHash) {
        self.messages
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&hash);
    }
}

impl MessageSource for MemoryMessages {
    async fn message(&self, hash: MessageHash) -> Result<Option<Message>, LoadError> {
        Ok(self
            .messages
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&hash)
            .cloned())
    }
}
