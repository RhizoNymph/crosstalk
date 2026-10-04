# Blob store

The L2 content-addressed blob store: the spec's `BlobStore`
(`spec/types/interfaces/l2_transport.rs`) implemented on the filesystem
and in memory, in `crosstalk-transport`'s `blob` module. Roadmap item P2.2.
This is the blob section of the transport crate's documentation; the event
bus half is in `docs/features/transport.md`.

## Scope

- `FsBlobStore`: message bodies as files under one directory, keyed by
  the BLAKE3 hash of their bytes, written atomically and durably, verified
  on every read.
- `MemoryBlobStore`: the same contract in process memory, for tests and the
  simulation.
- `message_hash`: the content key, `MessageHash` over unkeyed BLAKE3.
- Tests for the blob invariants' property evidence, and for idempotence,
  corruption, concurrent writes, missing bodies and error values.

## Non-scope

- Postgres (`PgBlobStore`) and object storage (`ObjectStoreBlobs`), which
  the spec's module doc names. Their integration evidence (INV-104 to 107,
  INV-109) stays pending.
- Content retention. The spec has no retention policy for bodies and no
  way to delete one (see below), so neither store has a deletion hook.
- Cleaning up temporary files a crashed process left behind.
- The event bus, dead letters and retries (`docs/features/transport.md`).

## Contract

The `BlobStore` contract, as both stores implement it:

| Call | Result |
| --- | --- |
| `put(bytes)` | `Ok(h)`, where `h` is `MessageHash::from_digest(BLAKE3(bytes))`. The store computes the key. Putting bytes that are already stored, or that another put is storing at the same time, returns `Ok(h)` as well. |
| `get(h)`, body stored and intact | `Ok(Some(bytes))` |
| `get(h)`, nothing stored under `h` | `Ok(None)`. For a hash an event names, this means content retention dropped the body, which the evidence page shows as `Excerpted::BodyDropped`. |
| `get(h)`, stored bytes do not hash to `h` | `Err(BlobError::Corrupt(h))`, never the bytes |
| any I/O failure | `Err(BlobError::Unavailable { reason })`. The reason names the operation, the file and the OS error, never the bytes. |

The spec's `Blake3::to_hex` (64 lower-case hex digits, most significant
byte first) produces the same text as `blake3::Hash::to_hex`, and
`Blake3::from_hex` reads that text back. The tests check both.

## Data and control flow

### `FsBlobStore`

```text
put(bytes) ──copy──> spawn_blocking ──> io::put(root, bytes)
                                         h = BLAKE3(bytes); file = <root>/<h[..2]>/<h[2..]>
                                         read(file):
                                           same bytes ─> fsync shard dir ─> (h, AlreadyStored)
                                           other bytes ─> outcome Repaired ─┐
                                           not found   ─> outcome Written  ─┤
                                         create shard dir if missing (fsync root when new)
                                         create_new <shard>/.<h[2..]>.<pid>-<n>.tmp
                                         write_all, fsync file, close
                                         rename over file (temp removed on any error)
                                         fsync shard dir
             <── (h, outcome) ── tracing: debug (warn when Repaired), hash and length only
get(h)      ──> spawn_blocking ──> io::get(root, h)
                                         read(file): not found ─> None
                                         BLAKE3(bytes) != h   ─> Fault::Corrupt(h)
             <── Fault → BlobError (Corrupt(h) | Unavailable{reason})
```

- **The async boundary.** Each `put` and `get` (and `open`) is one
  `tokio::task::spawn_blocking` call around plain `std::fs` code in
  `fs/io.rs`, through the one helper `blocking`. Nothing else in the
  store uses threads, and `io.rs` contains no async code. Hashing, which
  is CPU work, also runs on the blocking pool. A panic in the blocking code
  is a bug, so it is resumed on the caller. A task the runtime cancelled at
  shutdown becomes `Unavailable`.
- **Atomicity.** A reader sees either no file or a whole body, because the
  body is renamed into place only after it has been written and synced.
  Temporary names start with `.` and end in `.tmp`, so no hash names them.
  They are unique within a process (pid plus a process-wide `AtomicU64`)
  and are opened with `create_new`, retried up to 16 times on a collision
  with another process's leftover.
- **Durability.** The temporary file is synced before the rename and the
  shard directory after it, and the root is synced when a shard directory
  is new. A put that finds its bytes already in place still syncs the
  shard directory, so its `Ok` is durable even when a concurrent writer's
  rename has not yet been synced.
- **Idempotence and concurrency.** Identical bytes already in place are
  left alone, so nothing is written. Concurrent puts of the same bytes each
  write their own temporary file and rename it over the same name with
  identical content. Any interleaving leaves one complete file, and every
  put returns `Ok(h)`. Several stores, in one process or several, may
  share a root.
- **Repair.** If a file holds other bytes than its name says, `get`
  reports `Corrupt(h)`, and a `put` of the right bytes replaces the file,
  logging a warning.

### `MemoryBlobStore`

`Arc<Mutex<HashMap<MessageHash, Arc<[u8]>>>>`. Clones share the map. Each
call takes the lock for one map operation and never holds it across an
await. `put` inserts if absent, and `get` copies the body out and rehashes
it, so corruption is reported here as well. Tests can produce corruption
through the `#[cfg(test)]` method `insert_unchecked`. A poisoned lock
becomes `Unavailable`.

## Content retention: a spec gap

`BlobStore::get`'s doc, `Excerpted::BodyDropped` and INV-698 say that a
missing body for a hash an event names means content retention dropped it.
The spec, however, defines nothing that drops a body:

- `BlobStore` has only `put` and `get`;
- INV-106 (`transport.blob.get-matches-map-model`) models the store as a
  **grow-only** map, and its rationale says "blobs are never deleted: the
  interface has no delete";
- no configuration type, policy or event names content retention, unlike
  topic-version retention (`aggregates::retention`).

So neither store has a deletion hook. Adding one needs a spec decision
first: what decides that a body is dropped (an age, or no retained event
naming it), whether `BlobStore` gains a delete or retention is a separate
trait, and how INV-106 changes. A test that needs a dropped body never puts
it, or with `FsBlobStore` deletes its file
(`a_body_removed_from_disk_reads_as_none`).

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `crates/transport/src/blob/mod.rs` | Module doc (the contract, retention), re-exports | `FsBlobStore`, `OpenError`, `MemoryBlobStore`, `message_hash` |
| `crates/transport/src/blob/digest.rs` | The content key | `message_hash(&[u8]) -> MessageHash`; `matches` (crate-private) |
| `crates/transport/src/blob/fs/mod.rs` | The async store: `BlobStore` impl, the `spawn_blocking` boundary, mapping faults to `BlobError`, logging | `FsBlobStore::{open, root}` |
| `crates/transport/src/blob/fs/io.rs` | Every blocking filesystem operation: layout, atomic write, verified read | `OpenError` (`Create`, `Resolve`, `NotADirectory`, `Cancelled`); private `Root`, `Fault` (`Io { op, path, source }`, `Corrupt`, `TempNamesExhausted`), `Op`, `PutOutcome`, `TempFile` |
| `crates/transport/src/blob/memory.rs` | The in-memory store | `MemoryBlobStore::{new, len, is_empty}` |
| `crates/transport/src/blob/tests/mod.rs` | Invariant evidence (property tests over both stores), oracles, BLAKE3 test vectors | `put_returns_blake3_of_bytes`, `get_detects_corrupted_bytes`, `blob_store_matches_map_model` |
| `crates/transport/src/blob/tests/fs.rs` | Filesystem store behaviour | layout, idempotence, missing bodies, reopen, two stores on one root, repair, open errors, concurrent puts, error values |
| `crates/transport/src/blob/tests/memory.rs` | Memory store behaviour | idempotence, missing bodies, shared clones, concurrent puts |

Dependencies, pinned in the root `[workspace.dependencies]`:

- `blake3 =1.8.7`;
- `thiserror =2.0.21`;
- `tokio =1.53.1` with feature `rt`, plus `rt-multi-thread` and `macros` for tests;
- `tracing =0.1.44`;
- dev-dependencies `proptest =1.11.0` (no default features, `std` only) and `tempfile =3.27.0`.

## Invariant evidence

| Invariant | Evidence | State |
| --- | --- | --- |
| INV-105 `transport.blob.corrupt-detected` | property `crosstalk_transport::blob::tests::get_detects_corrupted_bytes` (both stores, plus a reopened `FsBlobStore`) | implemented, agent-reviewed |
| INV-106 `transport.blob.get-matches-map-model` | property `crosstalk_transport::blob::tests::blob_store_matches_map_model` (random put/get sequences, both stores, against a `HashMap` model) | implemented, agent-reviewed |
| INV-108 `transport.blob.put-returns-blake3` | property `crosstalk_transport::blob::tests::put_returns_blake3_of_bytes` | implemented, agent-reviewed |
| INV-104 `transport.blob.concurrent-put` | dst `crosstalk_transport::dst::concurrent_puts_of_same_bytes_succeed` | pending until `crosstalk-sim` exists. The tokio tests `concurrent_puts_of_the_same_bytes_all_succeed` in `tests/fs.rs` and `tests/memory.rs` cover the same property without simulation. |
| INV-104, 105, 106, 107, 109 integration | Postgres and object-store paths | pending; these stores are not implemented yet |
| INV-111 `transport.confidentiality.no-payload-in-logs` | property `crosstalk_transport::tests::error_values_omit_payload_bytes` | shared with the bus. `blob::tests::fs::blob_errors_omit_payload_bytes` covers the blob side. |

The three property paths were moved from `crosstalk_transport::tests::` to
`crosstalk_transport::blob::tests::`, which keeps the blob tests in the
blob module.

## Invariants and constraints

- The key is always BLAKE3 of the exact bytes. Callers cannot choose it.
- `get` never returns bytes that do not hash to the requested key.
- A reader never sees a partial body, and a put that returned `Ok` has
  been synced to disk, including its directory entries.
- Logs and error values identify blobs by hash, path and length, never by
  their content.
- Blocking filesystem work happens only inside `spawn_blocking`.
  `FsBlobStore` must therefore be used inside a tokio runtime.
- No `unsafe`, and no `unwrap` or `expect` outside tests.
- Directory fsync relies on opening a directory as a file, which works on
  Linux and other Unix systems. The store is not built for Windows.
