//! `FsBlobStore`: layout, idempotence, durability across reopen, repair,
//! missing bodies, concurrent puts and error values.

use std::collections::BTreeSet;
use std::path::Path;

use crosstalk_spec::interfaces::l2_transport::{BlobError, BlobStore};
use tokio::task::JoinSet;

use super::{file_of, fresh_fs, oracle};
use crate::blob::{FsBlobStore, OpenError};

/// Every entry under `dir`, recursively, relative to it.
fn entries(dir: &Path) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(next) = pending.pop() {
        for entry in std::fs::read_dir(&next).expect("read_dir") {
            let path = entry.expect("entry").path();
            let relative = path.strip_prefix(dir).expect("under dir");
            found.insert(relative.to_string_lossy().into_owned());
            if path.is_dir() {
                pending.push(path);
            }
        }
    }
    found
}

#[tokio::test]
async fn put_writes_the_body_at_its_layout_path() {
    let (dir, store) = fresh_fs().await;
    let hash = store.put(b"a message body").await.expect("put");
    let hex = hash.digest().to_hex();
    let file = dir.path().join(&hex[..2]).join(&hex[2..]);
    assert_eq!(std::fs::read(&file).expect("body file"), b"a message body");
    assert_eq!(
        entries(dir.path()),
        BTreeSet::from([hex[..2].to_owned(), format!("{}/{}", &hex[..2], &hex[2..])]),
    );
}

#[tokio::test]
async fn the_empty_body_is_stored_and_read_back() {
    let (_dir, store) = fresh_fs().await;
    let hash = store.put(b"").await.expect("put");
    assert_eq!(hash, oracle(b""));
    assert_eq!(store.get(hash).await, Ok(Some(Vec::new())));
}

#[tokio::test]
async fn putting_the_same_bytes_twice_is_idempotent() {
    let (dir, store) = fresh_fs().await;
    let first = store.put(b"repeated").await.expect("first put");
    let file = file_of(dir.path(), b"repeated");
    let written = std::fs::metadata(&file)
        .expect("metadata")
        .modified()
        .expect("mtime");
    let second = store.put(b"repeated").await.expect("second put");
    assert_eq!(first, second);
    assert_eq!(store.get(first).await, Ok(Some(b"repeated".to_vec())));
    // The second put found the body in place and wrote nothing.
    let after = std::fs::metadata(&file)
        .expect("metadata")
        .modified()
        .expect("mtime");
    assert_eq!(written, after);
    assert_eq!(entries(dir.path()).len(), 2, "one shard and one file");
}

#[tokio::test]
async fn get_of_a_hash_never_put_is_none() {
    let (_dir, store) = fresh_fs().await;
    // No shard directory exists yet.
    assert_eq!(store.get(oracle(b"never stored")).await, Ok(None));
}

#[tokio::test]
async fn get_is_none_when_the_shard_exists_but_the_file_does_not() {
    let (dir, store) = fresh_fs().await;
    let stored = store.put(b"stored").await.expect("put");
    // Find another body in the same shard.
    let shard = &stored.digest().to_hex()[..2];
    let sibling = (0u32..)
        .map(|n| n.to_le_bytes())
        .find(|candidate| &blake3::hash(candidate).to_hex()[..2] == shard)
        .expect("some u32 shares the shard");
    assert!(dir.path().join(shard).is_dir());
    assert_eq!(store.get(oracle(&sibling)).await, Ok(None));
}

#[tokio::test]
async fn a_body_removed_from_disk_reads_as_none() {
    // What content retention leaves behind: the event names the hash, the
    // body is gone, and get reports absence rather than an error.
    let (dir, store) = fresh_fs().await;
    let hash = store.put(b"dropped later").await.expect("put");
    std::fs::remove_file(file_of(dir.path(), b"dropped later")).expect("remove");
    assert_eq!(store.get(hash).await, Ok(None));
}

#[tokio::test]
async fn put_survives_reopening_the_store() {
    let dir = tempfile::tempdir().expect("tempdir");
    let hash = {
        let store = FsBlobStore::open(dir.path()).await.expect("open");
        store.put(b"durable").await.expect("put")
    };
    let reopened = FsBlobStore::open(dir.path()).await.expect("reopen");
    assert_eq!(reopened.get(hash).await, Ok(Some(b"durable".to_vec())));
}

#[tokio::test]
async fn two_stores_on_one_root_read_each_others_puts() {
    let (dir, first) = fresh_fs().await;
    let second = FsBlobStore::open(dir.path()).await.expect("second store");
    let hash = first.put(b"shared").await.expect("put");
    assert_eq!(second.get(hash).await, Ok(Some(b"shared".to_vec())));
}

#[tokio::test]
async fn a_put_of_the_right_bytes_repairs_a_corrupt_file() {
    let (dir, store) = fresh_fs().await;
    let hash = store.put(b"original").await.expect("put");
    let file = file_of(dir.path(), b"original");
    std::fs::write(&file, b"tampered").expect("tamper");
    assert_eq!(store.get(hash).await, Err(BlobError::Corrupt(hash)));
    assert_eq!(store.put(b"original").await, Ok(hash));
    assert_eq!(store.get(hash).await, Ok(Some(b"original".to_vec())));
}

#[tokio::test]
async fn open_creates_a_missing_root_and_resolves_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let nested = dir.path().join("a").join("b");
    let store = FsBlobStore::open(&nested).await.expect("open");
    assert!(nested.is_dir());
    assert!(store.root().is_absolute());
    assert_eq!(
        store.root(),
        std::fs::canonicalize(&nested).expect("canonical")
    );
}

#[tokio::test]
async fn open_refuses_a_root_that_is_a_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let file = dir.path().join("not-a-dir");
    std::fs::write(&file, b"").expect("file");
    match FsBlobStore::open(&file).await {
        Err(OpenError::Create { path, .. }) => assert_eq!(path, file),
        other => panic!("expected a create error, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_puts_of_the_same_bytes_all_succeed() {
    let (dir, first) = fresh_fs().await;
    // Two stores on one root stand in for two nodes sharing a filesystem.
    let second = FsBlobStore::open(dir.path()).await.expect("second store");
    let bytes = b"stored by everyone at once".to_vec();
    let mut puts = JoinSet::new();
    for n in 0..64 {
        let store = if n % 2 == 0 {
            first.clone()
        } else {
            second.clone()
        };
        let bytes = bytes.clone();
        puts.spawn(async move { store.put(&bytes).await });
    }
    let expected = oracle(&bytes);
    while let Some(joined) = puts.join_next().await {
        assert_eq!(joined.expect("task"), Ok(expected));
    }
    assert_eq!(first.get(expected).await, Ok(Some(bytes)));
    // One shard, one body, and no temporary file left behind.
    assert_eq!(entries(dir.path()).len(), 2, "{:?}", entries(dir.path()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_puts_of_distinct_bytes_all_land() {
    let (dir, store) = fresh_fs().await;
    let mut puts = JoinSet::new();
    for n in 0u32..128 {
        let store = store.clone();
        puts.spawn(async move { (n, store.put(&n.to_le_bytes()).await) });
    }
    while let Some(joined) = puts.join_next().await {
        let (n, result) = joined.expect("task");
        assert_eq!(result, Ok(oracle(&n.to_le_bytes())));
    }
    for n in 0u32..128 {
        let bytes = n.to_le_bytes();
        assert_eq!(store.get(oracle(&bytes)).await, Ok(Some(bytes.to_vec())));
    }
    let leftovers: Vec<_> = entries(dir.path())
        .into_iter()
        .filter(|entry| entry.ends_with(".tmp"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
}

#[tokio::test]
async fn blob_errors_omit_payload_bytes() {
    // A shard path that is a file makes every read and write under it fail.
    let (dir, store) = fresh_fs().await;
    let payload = b"SECRET-PROMPT-TEXT the model must never see in logs";
    let hash = oracle(payload);
    let hex = hash.digest().to_hex();
    std::fs::write(dir.path().join(&hex[..2]), b"").expect("block the shard");

    let errors = [
        store.put(payload).await.expect_err("put fails"),
        store.get(hash).await.expect_err("get fails"),
    ];
    for error in errors {
        let BlobError::Unavailable { reason } = &error else {
            panic!("expected Unavailable, got {error:?}");
        };
        let shown = format!("{error:?} {reason}");
        assert!(!shown.contains("SECRET-PROMPT-TEXT"), "{shown}");
        assert!(shown.contains(&hex[..2]), "names the file: {shown}");
    }
}
