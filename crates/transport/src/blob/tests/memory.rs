//! `MemoryBlobStore`: idempotence, shared clones, missing bodies and
//! concurrent puts.

use crosstalk_spec::interfaces::l2_transport::BlobStore;
use tokio::task::JoinSet;

use super::oracle;
use crate::blob::MemoryBlobStore;

#[tokio::test]
async fn putting_the_same_bytes_twice_stores_one_body() {
    let store = MemoryBlobStore::new();
    let first = store.put(b"repeated").await.expect("first put");
    let second = store.put(b"repeated").await.expect("second put");
    assert_eq!(first, second);
    assert_eq!(store.len(), Ok(1));
    assert_eq!(store.get(first).await, Ok(Some(b"repeated".to_vec())));
}

#[tokio::test]
async fn get_of_a_hash_never_put_is_none() {
    let store = MemoryBlobStore::new();
    assert_eq!(store.is_empty(), Ok(true));
    assert_eq!(store.get(oracle(b"never stored")).await, Ok(None));
}

#[tokio::test]
async fn clones_share_one_store() {
    let store = MemoryBlobStore::new();
    let other = store.clone();
    let hash = store.put(b"shared").await.expect("put");
    assert_eq!(other.get(hash).await, Ok(Some(b"shared".to_vec())));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_puts_of_the_same_bytes_all_succeed() {
    let store = MemoryBlobStore::new();
    let bytes = b"stored by everyone at once".to_vec();
    let mut puts = JoinSet::new();
    for _ in 0..64 {
        let store = store.clone();
        let bytes = bytes.clone();
        puts.spawn(async move { store.put(&bytes).await });
    }
    let expected = oracle(&bytes);
    while let Some(joined) = puts.join_next().await {
        assert_eq!(joined.expect("task"), Ok(expected));
    }
    assert_eq!(store.len(), Ok(1));
    assert_eq!(store.get(expected).await, Ok(Some(bytes)));
}
