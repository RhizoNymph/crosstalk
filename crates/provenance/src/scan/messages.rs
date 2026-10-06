//! Where the scanner reads message bodies: the blob store, decoded with the
//! spec's canonical encoding.
//!
//! Every exchange's request history is read again on each scan, so
//! [`BlobMessages`] keeps recently decoded bodies with the bytes they were
//! decoded from: a read whose stored bytes equal the kept ones is the kept
//! message (decoding is a function of the bytes), and anything else is
//! decoded and checked as before.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::{Arc, Mutex};

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

/// How many encoded bytes [`BlobMessages`] keeps decoded bodies for.
pub const DECODED_BUDGET: usize = 128 << 20;

/// Bodies from a spec `BlobStore`. Clones share the store and the kept
/// decoded bodies.
#[derive(Debug, Clone)]
pub struct BlobMessages<B> {
    blobs: B,
    decoded: Arc<Mutex<Decoded>>,
}

impl<B> BlobMessages<B> {
    pub fn new(blobs: B) -> Self {
        Self::with_budget(blobs, DECODED_BUDGET)
    }

    /// Keeping decoded bodies for at most `budget` encoded bytes.
    pub fn with_budget(blobs: B, budget: usize) -> Self {
        Self {
            blobs,
            decoded: Arc::new(Mutex::new(Decoded::new(budget))),
        }
    }

    fn decoded(&self) -> std::sync::MutexGuard<'_, Decoded> {
        self.decoded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
        if let Some(message) = self.decoded().get(hash, &bytes) {
            return Ok(Some(message));
        }
        let body = encoding::decode(&bytes).map_err(|_| LoadError::Undecodable(hash))?;
        // `decode` accepts exactly the bytes `encode` writes, so the bytes'
        // digest is the body's hash.
        let stored = encoding::hash_bytes(&bytes);
        if stored != hash {
            return Err(LoadError::Corrupt(hash));
        }
        let message = Message { hash, body };
        self.decoded().put(hash, bytes, message.clone());
        Ok(Some(message))
    }
}

/// Decoded bodies by hash, with the bytes each was decoded from, least
/// recently used evicted first within a budget of bytes.
#[derive(Debug)]
struct Decoded {
    entries: HashMap<MessageHash, DecodedEntry>,
    /// Each entry's last use, oldest first.
    uses: BTreeMap<u64, MessageHash>,
    clock: u64,
    held: usize,
    budget: usize,
}

#[derive(Debug)]
struct DecodedEntry {
    bytes: Vec<u8>,
    message: Message,
    used: u64,
}

impl Decoded {
    fn new(budget: usize) -> Self {
        Self {
            entries: HashMap::new(),
            uses: BTreeMap::new(),
            clock: 0,
            held: 0,
            budget,
        }
    }

    /// The message kept for `hash` when it was decoded from exactly
    /// `bytes`.
    fn get(&mut self, hash: MessageHash, bytes: &[u8]) -> Option<Message> {
        self.clock += 1;
        let now = self.clock;
        let entry = self.entries.get_mut(&hash)?;
        if entry.bytes != bytes {
            return None;
        }
        self.uses.remove(&entry.used);
        entry.used = now;
        self.uses.insert(now, hash);
        Some(entry.message.clone())
    }

    fn put(&mut self, hash: MessageHash, bytes: Vec<u8>, message: Message) {
        let weight = bytes.len();
        if weight > self.budget {
            return;
        }
        self.clock += 1;
        let used = self.clock;
        if let Some(replaced) = self.entries.insert(
            hash,
            DecodedEntry {
                bytes,
                message,
                used,
            },
        ) {
            self.uses.remove(&replaced.used);
            self.held -= replaced.bytes.len();
        }
        self.uses.insert(used, hash);
        self.held += weight;
        while self.held > self.budget {
            let Some((_, oldest)) = self.uses.pop_first() else {
                break;
            };
            if let Some(evicted) = self.entries.remove(&oldest) {
                self.held -= evicted.bytes.len();
            }
        }
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
