//! Recording wrappers: a blob store and a bus that report, in call order,
//! every put and every published envelope on a channel, then forward to
//! the store or bus they wrap.

use std::future::Future;

use crosstalk_spec::events::{Envelope, Subject};
use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::interfaces::l2_transport::{
    BlobError, BlobStore, BusError, ConsumerGroup, EventBus, RetryPolicy,
};
use tokio::sync::mpsc;

/// One put the wrapped store answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Put {
    pub bytes: Vec<u8>,
    /// Whether the store reported it stored.
    pub ok: bool,
}

/// A blob store that reports every put it answers.
#[derive(Debug, Clone)]
pub struct RecordingStore<B> {
    inner: B,
    puts: mpsc::UnboundedSender<Put>,
}

impl<B> RecordingStore<B> {
    pub fn new(inner: B) -> (Self, mpsc::UnboundedReceiver<Put>) {
        let (puts, recorded) = mpsc::unbounded_channel();
        (Self { inner, puts }, recorded)
    }
}

impl<B: BlobStore + Sync> BlobStore for RecordingStore<B> {
    async fn put(&self, bytes: &[u8]) -> Result<MessageHash, BlobError> {
        let result = self.inner.put(bytes).await;
        // The receiver outlives every put in these tests.
        let _ = self.puts.send(Put {
            bytes: bytes.to_vec(),
            ok: result.is_ok(),
        });
        result
    }

    fn get(
        &self,
        hash: MessageHash,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, BlobError>> + Send {
        self.inner.get(hash)
    }
}

/// A bus that reports every envelope it accepted, in the order it
/// accepted them.
#[derive(Debug, Clone)]
pub struct RecordingBus<E> {
    inner: E,
    published: mpsc::UnboundedSender<Envelope>,
    /// Milliseconds below which each publish is held back, by its
    /// envelope id: a slow bus whose acceptance order is not its call
    /// order unless calls are serialized. Zero for none.
    jitter_ms: u64,
}

impl<E> RecordingBus<E> {
    pub fn new(inner: E) -> (Self, mpsc::UnboundedReceiver<Envelope>) {
        Self::slow(inner, 0)
    }

    /// Each publish waits `id mod jitter_ms` milliseconds (simulated time)
    /// before it reaches `inner`.
    pub fn slow(inner: E, jitter_ms: u64) -> (Self, mpsc::UnboundedReceiver<Envelope>) {
        let (published, recorded) = mpsc::unbounded_channel();
        (
            Self {
                inner,
                published,
                jitter_ms,
            },
            recorded,
        )
    }
}

impl<E: EventBus + Sync> EventBus for RecordingBus<E> {
    type Subscription = E::Subscription;

    async fn publish(&self, envelope: Envelope) -> Result<(), BusError> {
        if self.jitter_ms > 0 {
            let wait = envelope.id.as_ulid() % u128::from(self.jitter_ms);
            let wait = u64::try_from(wait).unwrap_or(0);
            tokio::time::sleep(std::time::Duration::from_millis(wait)).await;
        }
        let copy = envelope.clone();
        self.inner.publish(envelope).await?;
        let _ = self.published.send(copy);
        Ok(())
    }

    fn subscribe(
        &self,
        subjects: &[Subject],
        group: ConsumerGroup,
        retry: RetryPolicy,
    ) -> impl Future<Output = Result<Self::Subscription, BusError>> + Send {
        self.inner.subscribe(subjects, group, retry)
    }
}

/// Everything on `receiver` now, without waiting.
pub fn drain<T>(receiver: &mut mpsc::UnboundedReceiver<T>) -> Vec<T> {
    let mut items = Vec::new();
    while let Ok(item) = receiver.try_recv() {
        items.push(item);
    }
    items
}
