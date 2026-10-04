//! Blob store tests. The three property tests at this module's root are the
//! implementation evidence of the blob invariants (INV-105, INV-106,
//! INV-108) and run against both stores; `fs` and `memory` hold each
//! store's own behaviour.
//!
//! Every oracle hashes with the `blake3` crate directly and finds files by
//! the documented layout, never through the store's own helpers.

mod fs;
mod memory;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::interfaces::l2_transport::{BlobError, BlobStore};
use crosstalk_spec::support::Blake3;
use proptest::prelude::*;
use tempfile::TempDir;

use super::{FsBlobStore, MemoryBlobStore, message_hash};

/// The hash the spec says `bytes` have: BLAKE3 of exactly those bytes.
fn oracle(bytes: &[u8]) -> MessageHash {
    MessageHash::from_digest(Blake3::from_bytes(*blake3::hash(bytes).as_bytes()))
}

/// Where the layout says the body of `bytes` lives under `root`.
fn file_of(root: &Path, bytes: &[u8]) -> PathBuf {
    let hex = blake3::hash(bytes).to_hex();
    root.join(&hex[..2]).join(&hex[2..])
}

/// A single-threaded runtime for one property case. The blocking pool still
/// runs `FsBlobStore`'s file operations.
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a current-thread runtime builds")
}

async fn fresh_fs() -> (TempDir, FsBlobStore) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let store = FsBlobStore::open(dir.path())
        .await
        .expect("the store opens");
    (dir, store)
}

fn body() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..512)
}

/// A change that always makes stored bytes differ from the original.
#[derive(Debug, Clone, Copy)]
enum Damage {
    /// XOR the byte at `index % len` with a non-zero mask.
    Flip { index: usize, mask: u8 },
    /// Keep only the first `keep % len` bytes.
    Truncate { keep: usize },
    /// Add one byte at the end.
    Append(u8),
}

fn damage() -> impl Strategy<Value = Damage> {
    prop_oneof![
        (any::<usize>(), 1..=u8::MAX).prop_map(|(index, mask)| Damage::Flip { index, mask }),
        any::<usize>().prop_map(|keep| Damage::Truncate { keep }),
        any::<u8>().prop_map(Damage::Append),
    ]
}

impl Damage {
    fn apply(&self, bytes: &[u8]) -> Vec<u8> {
        let mut damaged = bytes.to_vec();
        match (*self, bytes.len()) {
            (Self::Flip { index, mask }, len @ 1..) => damaged[index % len] ^= mask,
            (Self::Truncate { keep }, len @ 1..) => damaged.truncate(keep % len),
            (Self::Append(byte), _) | (Self::Flip { mask: byte, .. }, 0) => damaged.push(byte),
            (Self::Truncate { .. }, 0) => damaged.push(0),
        }
        damaged
    }
}

// INV-108 transport.blob.put-returns-blake3
proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn put_returns_blake3_of_bytes(bytes in body()) {
        let expected = oracle(&bytes);
        runtime().block_on(async {
            let memory = MemoryBlobStore::new();
            prop_assert_eq!(memory.put(&bytes).await, Ok(expected));
            let (_dir, fs) = fresh_fs().await;
            prop_assert_eq!(fs.put(&bytes).await, Ok(expected));
            prop_assert_eq!(message_hash(&bytes), expected);
            // The spec's hex is the text blake3 writes, and reads back.
            let hex = blake3::hash(&bytes).to_hex();
            prop_assert_eq!(expected.digest().to_hex(), hex.as_str());
            prop_assert_eq!(Blake3::from_hex(hex.as_str()), Ok(*expected.digest()));
            Ok(())
        })?;
    }
}

// INV-105 transport.blob.corrupt-detected
proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn get_detects_corrupted_bytes(bytes in body(), damage in damage()) {
        let hash = oracle(&bytes);
        let damaged = damage.apply(&bytes);
        prop_assert_ne!(&damaged, &bytes);
        runtime().block_on(async {
            let memory = MemoryBlobStore::new();
            memory.put(&bytes).await.expect("put");
            memory.insert_unchecked(hash, &damaged);
            prop_assert_eq!(memory.get(hash).await, Err(BlobError::Corrupt(hash)));

            let (dir, fs) = fresh_fs().await;
            fs.put(&bytes).await.expect("put");
            std::fs::write(file_of(dir.path(), &bytes), &damaged).expect("overwrite the body");
            prop_assert_eq!(fs.get(hash).await, Err(BlobError::Corrupt(hash)));
            // A store reopened on the same files rehashes too.
            let reopened = FsBlobStore::open(dir.path()).await.expect("reopen");
            prop_assert_eq!(reopened.get(hash).await, Err(BlobError::Corrupt(hash)));
            Ok(())
        })?;
    }
}

/// One step of a model run, naming bodies by their index in the pool.
#[derive(Debug, Clone)]
enum Op {
    Put(usize),
    Get(usize),
    /// Get a hash that is (almost certainly) no pool body's.
    GetUnknown([u8; 32]),
}

fn op(pool: usize) -> impl Strategy<Value = Op> {
    prop_oneof![
        (0..pool).prop_map(Op::Put),
        (0..pool).prop_map(Op::Get),
        any::<[u8; 32]>().prop_map(Op::GetUnknown),
    ]
}

fn model_case() -> impl Strategy<Value = (Vec<Vec<u8>>, Vec<Op>)> {
    prop::collection::vec(prop::collection::vec(any::<u8>(), 0..64), 1..8).prop_flat_map(|pool| {
        let ops = prop::collection::vec(op(pool.len()), 0..40);
        (Just(pool), ops)
    })
}

/// Run `ops` on `store` and on the grow-only map model, comparing every
/// observable result, then every body the model holds.
async fn check_against_model<S: BlobStore>(
    store: &S,
    pool: &[Vec<u8>],
    ops: &[Op],
) -> Result<(), TestCaseError> {
    let mut model: HashMap<MessageHash, Vec<u8>> = HashMap::new();
    for op in ops {
        match *op {
            Op::Put(index) => {
                let bytes = &pool[index];
                let hash = oracle(bytes);
                prop_assert_eq!(store.put(bytes).await, Ok(hash));
                model.insert(hash, bytes.clone());
            }
            Op::Get(index) => {
                let hash = oracle(&pool[index]);
                prop_assert_eq!(store.get(hash).await, Ok(model.get(&hash).cloned()));
            }
            Op::GetUnknown(digest) => {
                let hash = MessageHash::from_digest(Blake3::from_bytes(digest));
                prop_assert_eq!(store.get(hash).await, Ok(model.get(&hash).cloned()));
            }
        }
    }
    for (hash, bytes) in &model {
        prop_assert_eq!(store.get(*hash).await, Ok(Some(bytes.clone())));
    }
    Ok(())
}

// INV-106 transport.blob.get-matches-map-model
proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn blob_store_matches_map_model((pool, ops) in model_case()) {
        runtime().block_on(async {
            check_against_model(&MemoryBlobStore::new(), &pool, &ops).await?;
            let (_dir, fs) = fresh_fs().await;
            check_against_model(&fs, &pool, &ops).await
        })?;
    }
}

#[test]
fn hashes_match_published_blake3_vectors() {
    // BLAKE3 test vectors for "" and "abc".
    assert_eq!(
        message_hash(b"").digest().to_hex(),
        "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
    );
    assert_eq!(
        message_hash(b"abc").digest().to_hex(),
        "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85"
    );
}

#[test]
fn every_damage_changes_the_bytes() {
    let cases: [&[u8]; 3] = [b"", b"x", b"hello"];
    let damages = [
        Damage::Flip { index: 7, mask: 1 },
        Damage::Truncate { keep: 9 },
        Damage::Append(0),
    ];
    for bytes in cases {
        for damage in damages {
            assert_ne!(damage.apply(bytes), bytes, "{damage:?} on {bytes:?}");
        }
    }
}
