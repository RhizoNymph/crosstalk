//! `BlobMessages`' kept decoded bodies: a kept body is returned only for
//! the bytes it was decoded from.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::interfaces::l2_transport::{BlobError, BlobStore};
use crosstalk_spec::observed::message::{Message, encoding};
use crosstalk_testkit::build::message::{message, user_text};

use super::fixtures::sentence;
use crate::scan::messages::{BlobMessages, LoadError, MessageSource};

/// A blob store whose bytes a test can swap or drop under a hash.
#[derive(Debug, Clone, Default)]
struct Swappable {
    blobs: Arc<Mutex<HashMap<MessageHash, Vec<u8>>>>,
}

impl Swappable {
    fn set(&self, hash: MessageHash, bytes: Option<Vec<u8>>) {
        let mut blobs = self.blobs.lock().expect("lock");
        match bytes {
            Some(bytes) => blobs.insert(hash, bytes),
            None => blobs.remove(&hash),
        };
    }
}

impl BlobStore for Swappable {
    async fn put(&self, bytes: &[u8]) -> Result<MessageHash, BlobError> {
        let hash = encoding::hash_bytes(bytes);
        self.set(hash, Some(bytes.to_vec()));
        Ok(hash)
    }

    async fn get(&self, hash: MessageHash) -> Result<Option<Vec<u8>>, BlobError> {
        Ok(self.blobs.lock().expect("lock").get(&hash).cloned())
    }
}

fn stored(store: &Swappable, seed: &str) -> Message {
    let message = message(user_text(&sentence(seed)));
    store.set(message.hash, Some(encoding::encode(&message.body)));
    message
}

#[tokio::test]
async fn a_kept_body_is_the_decoded_body() {
    let store = Swappable::default();
    let messages = BlobMessages::new(store.clone());
    let one = stored(&store, "one");
    assert_eq!(messages.message(one.hash).await, Ok(Some(one.clone())));
    assert_eq!(messages.message(one.hash).await, Ok(Some(one)));
}

#[tokio::test]
async fn changed_or_dropped_bytes_are_not_answered_from_the_kept_body() {
    let store = Swappable::default();
    let messages = BlobMessages::new(store.clone());
    let one = stored(&store, "one");
    let two = stored(&store, "two");
    assert_eq!(messages.message(one.hash).await, Ok(Some(one.clone())));
    // Another body's bytes under the first hash: corrupt, as without a cache.
    store.set(one.hash, Some(encoding::encode(&two.body)));
    assert_eq!(
        messages.message(one.hash).await,
        Err(LoadError::Corrupt(one.hash))
    );
    // Gone: none.
    store.set(one.hash, None);
    assert_eq!(messages.message(one.hash).await, Ok(None));
    // Back: decoded again.
    store.set(one.hash, Some(encoding::encode(&one.body)));
    assert_eq!(messages.message(one.hash).await, Ok(Some(one)));
}

#[tokio::test]
async fn a_tiny_budget_still_reads_every_body() {
    let store = Swappable::default();
    let messages = BlobMessages::with_budget(store.clone(), 1);
    let bodies: Vec<Message> = ["a", "b", "c"]
        .iter()
        .map(|seed| stored(&store, seed))
        .collect();
    for _ in 0..2 {
        for body in &bodies {
            assert_eq!(messages.message(body.hash).await, Ok(Some(body.clone())));
        }
    }
}
