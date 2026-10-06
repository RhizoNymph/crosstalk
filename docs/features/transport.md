# Transport (L2): the in-process bus and the blob store

`crosstalk-transport` implements the L2 traits of
`spec/types/interfaces/l2_transport.rs` for a single process: `EventBus`
and `Subscription` (`MpscBus`, `MpscSubscription`), `DeadLetterStore`
(`DeadLetters`), consumed `RetryPolicy`s, and an envelope-level dedup
wrapper (`Dedup` over `HandledIds`), roadmap item P2.1; and the
content-addressed `BlobStore` on the filesystem and in memory
(`FsBlobStore`, `MemoryBlobStore`, in the `blob` module), roadmap item
P2.2, described in [Blob store](#blob-store) below. The single-node
gateway runs on it, and every layer's simulation tests use it (as a
dev-dependency) with a seeded delivery order.

The sections up to [Blob store](#blob-store) cover the in-process bus.
The durable bus on Postgres (`PgBus`, the `pg` module) is in
[pg_bus.md](pg_bus.md), and the publish spool in front of it
(`SpoolingBus`, the `spool` module) in [publish_spool.md](publish_spool.md);
the conformance suite in `src/conformance/` runs the group semantics
below over both buses.

## Scope

- Consumer groups: each group gets every envelope published under one of
  its subjects after its first subscribe; the subscriptions of a group
  share them, one holder at a time.
- At-least-once delivery with ack and nack, ack timeouts, and redelivery
  after a nack, a timeout or the drop of the subscription holding a
  delivery (a consumer crash).
- Retries with backoff from the group's `RetryPolicy`; after
  `max_attempts` deliveries the envelope becomes a `DeadLetter`, stored
  before the bus lets go of it. Dead letters are listed newest envelope
  first with cursors, and replayed to their group alone.
- Encoding: envelopes cross the bus as their wire JSON, even in process,
  and are decoded strictly on every delivery.
- Bounded memory: a group holds at most `group_capacity` envelopes, and
  `publish` waits for room.
- Structured config (`BusConfig`) with defaults.
- The `Dedup` wrapper and its in-memory handled-id record.

## Non-scope

- `BlobStore`, which is the [Blob store](#blob-store) section.
- The multi-node `JetStreamBus` and the `integration` evidence of the
  transport invariants on NATS JetStream. `PgBus` and the spool have their
  own pages.
- Durability across process restarts: nothing on this bus survives the
  process (`transport.durability.publish-persisted` is JetStream's;
  `PgBus`, in [pg_bus.md](pg_bus.md), is the durable single-node bus).
- A shared, durable handled-id record for `Dedup` across nodes (a
  Postgres `HandledIds`); `MemoryHandledIds` is process-local.
- The `lint:transport-no-payload-in-logs` rule.
- The consumers' own ack-after-publish discipline
  (`transport.consumer.ack-after-outputs`), which each pipeline layer
  upholds and tests.
- The general simulation harness (`crosstalk-sim`, P1.3). This crate's
  `dst` tests use tokio's paused clock directly with small local fault
  helpers (see below).

## Data and control flow

```text
MpscBus::publish(envelope)
  codec::encode ── serde_json ──▶ Message { subject: event.subject(), id, bytes: Arc<[u8]> }
  ── Command::Publish ──▶ bus task (one owner of all state)
       for each group whose subject set contains the subject:
         room (held < group_capacity, nobody waiting)? admit as Ready entry
         else park in the group's waiting queue; publish replies only once
         every target group admitted it (backpressure, never drop)

MpscSubscription::next()
  ── Command::Next ──▶ bus task parks the waiter
  bus task: ready entry + waiter → Handout { DeliveryId, attempt, Message }
            entry: Ready → Held { delivery, sub, deadline = now + ack_timeout }
  subscription: codec::decode (strict; subject must match the routed one)
      ok   → Some(Ok(Delivery))
      fail → Command::Terminate (entry released, warn logged) → Some(Err(Decode)), once

ack(id)   → holder check → entry released → Ok          (else UnknownDelivery)
nack(id)  → holder check → fail(Nack { retry_after, reason })
timeout   → timer AckDeadline → fail(Timeout)
drop(sub) → drops channel → every entry it holds → fail(Dropped)

fail: deliveries < max_attempts → Delayed { until = now + delay } → timer → Ready
        delay: nack: retry_after clamped to initial_backoff..=max_backoff
               timeout, drop: initial_backoff * 2^(deliveries-1), capped at max_backoff
      deliveries == max_attempts → decode → DeadLetter { group, envelope, attempts, last_error }
        last_error: the final nack's reason, "ack timeout after N ms", or
                    "subscription dropped while holding the delivery"
        shelf.put ok  → entry released
        shelf.put err → Exhausted { letter } (still counted, never delivered),
                        retried every dead_letter_retry

DeadLetters::replay(group, id)
  letter? no → UnknownDeadLetter
  room in group? admit a fresh entry (attempt restarts at 1), then remove the
  letter, and fail every other waiting replay of it; no room → wait like a publish
```

The bus task's loop is `select! { biased; due timer; dropped
subscription; command }`, followed by admitting waiting publishes and
replays where there is room and handing ready entries to waiting
subscriptions. Handles hold the command sender; subscriptions hold a weak
one, so the task stops when every `MpscBus`, `DeadLetters` and
`MemoryHandledIds` handle is dropped or `MpscBus::shutdown` is called,
and subscriptions then see `None`.

### Cancel safety

`MpscSubscription::next` is cancel-safe: dropping its future before it
completes loses no delivery. The subscription keeps the reply channel of
its outstanding request across calls, so a delivery the bus granted to a
dropped call is returned by the next call, on the same attempt and
without waiting for an ack timeout; an undecodable delivery whose
termination was cut short is terminated and reported by the next call.
`Dedup::next` keeps the delivery it is checking (or the duplicate it is
acking) on the wrapper the same way. Wrappers that pull from `next` under
a timeout, such as the simulation kit's faulty subscription, rely on it
(`tests::bus::a_dropped_next_loses_no_delivery`,
`a_dropped_dedup_next_loses_no_delivery`).

### Undecodable payloads

A payload that does not decode (an unknown field or variant from a newer
node, or a foreign publisher's bytes through `publish_encoded`) has no
`Envelope`, so it has no `DeliveryId` a consumer could nack and cannot
become a `DeadLetter`, which holds an `Envelope`. The bus follows
`transport.codec.undecodable-not-redelivered`: each group is told once,
as `Err(BusError::Decode)`, and the message is terminated and logged
(group, subject, entry sequence, decoder position). `wire_contract.md`
states the same rule; dead-lettering it would take a spec change to
`DeadLetter`.

### Order

No order is promised, within a subject or across subjects
(`transport.ordering.unconstrained`). `DeliveryOrder::Fifo` hands ready
entries out in the order they became ready; `DeliveryOrder::Shuffled {
seed }` picks one uniformly with a seeded SplitMix64, so simulation tests
reach every order and the same seed reproduces the same run.

### Dedup

`Dedup<S, H>` wraps any `Subscription`. `next` asks `H` whether the
group handled the envelope's id; if so it acks the delivery and takes
the next one. `ack` writes the id to `H`, then acks; `nack` passes
through. Consumer logic calls `ack` only after it handled the delivery.

## Configuration

`BusConfig` decodes from JSON (or YAML) with every field optional and
unknown fields refused; durations are `<what>_micros`.

| Field | Default | Meaning |
| --- | --- | --- |
| `group_capacity` | 1024 | envelopes a group holds (ready, delayed, held, exhausted) |
| `command_buffer` | 256 | the bus task's command queue |
| `ack_timeout_micros` | 30 s | how long a delivery may be held |
| `dead_letter_retry_micros` | 1 s | wait before retrying a failed dead-letter put |
| `order` | `{"type": "fifo"}` | or `{"type": "shuffled", "data": {"seed": n}}` |
| `retry` | 5 attempts, 100 ms to 30 s | the policy the gateway subscribes with |

Zero capacities or durations and invalid retry policies are decode
errors.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `crates/transport/src/lib.rs` | Crate doc, re-exports, and `pub mod blob` (files in [Blob store](#blob-store)) | — |
| `src/config.rs` | `BusConfig` and its checked decode | `BusConfig`, `DeliveryOrder`, `NonZeroDuration`, `InvalidBusConfig` |
| `src/codec.rs` | Envelope to bytes and back; payload-free error reasons | `Message`, `encode`, `decode` (crate) |
| `src/rng.rs` | Seeded SplitMix64 | `SplitMix64` (crate) |
| `src/bus/mod.rs` | Handles and trait impls | `MpscBus`, `DeadLetters`, `MemoryHandledIds`, `StartError` |
| `src/bus/actor.rs` | The bus task: commands, timers, failure handling, dispatch | `Actor` (crate) |
| `src/bus/command.rs` | Commands and replies | `GroupDepth`; `Command`, `Handout`, `SubId` (crate) |
| `src/bus/group.rs` | One group's entries, queues and waiters | `Group`, `Entry`, `EntryState`, `Waiting` (crate) |
| `src/bus/letters.rs` | The dead-letter shelf and its cursors | `Shelf` (crate) |
| `src/bus/subscription.rs` | `Subscription` over the bus task | `MpscSubscription` |
| `src/dedup.rs` | The dedup wrapper | `Dedup`, `HandledIds` |
| `src/testing.rs` | Test fixtures and proptest strategies | (tests only) |
| `src/tests/` | Unit and property tests; invariant evidence at `crosstalk_transport::tests::*` | — |
| `src/dst/` | Simulation tests; invariant evidence at `crosstalk_transport::dst::*` (the spool's in `dst/spool.rs`) | — |
| `src/conformance/` | The bus conformance suite, once over `MpscBus` and once over `PgBus` | — |
| `src/pg/`, `src/spool/`, `src/integration/` | `PgBus`, the spool, Postgres tests: see [pg_bus.md](pg_bus.md) and [publish_spool.md](publish_spool.md) | — |

### Simulation tests

`src/dst/` runs every test on tokio's paused clock on one thread. The
seeded `scenario` runs two groups (two consumers and one) while a
publisher publishes 24 envelopes at random times; each consumer, per
delivery, acks, acks after the ack timeout, nacks, stalls or crashes and
resubscribes, as its seeded `Behaviour` says. The checks read the log of
deliveries and the dead letters, and a failure names its seed
(`CROSSTALK_DST_SEED=<n>` reruns one). `same_seed_replays_the_same_run`
pins determinism and `scenario_exercises_every_fault` that every fault
occurs.

Fault helpers that should move to `crosstalk-sim` once it lands:
`Behaviour` (the consumer fault model), the scenario driver and its
seed handling (`for_seeds`, `seeds`), `nack_until_dead`, and the
dead-letter store outage (`MpscBus::start_with_failing_puts`, a
`cfg(test)` hook on the shelf, which the sim would need as a public fault
injection point). The seeded delivery order is already public
(`DeliveryOrder::Shuffled`).

## Invariants and constraints

Implementation evidence in this crate (all reviewed `agent = "true"`):

| Invariant | Evidence |
| --- | --- |
| `transport.ack.ends-redelivery` | `dst::acked_envelope_never_redelivered` |
| `transport.ack.unknown-delivery` | `tests::ack_of_unheld_delivery_is_unknown`, `dst::ack_after_ack_timeout_is_unknown` |
| `transport.backpressure.bounded-queue`, `publish-waits` | `dst::mpsc_queue_never_exceeds_capacity` |
| `transport.confidentiality.no-payload-in-logs` | `tests::error_values_omit_payload_bytes` (the lint is not written); the blob side is in [Blob store](#blob-store) |
| `transport.deadletter.last-error-is-nack-reason` | `dst::dead_letter_last_error_is_final_nack_reason` |
| `transport.deadletter.not-redelivered` | `dst::dead_lettered_envelope_not_redelivered` |
| `transport.deadletter.record-contents` | `dst::dead_letter_records_group_envelope_and_attempts` |
| `transport.deadletter.replay-consumes` | `dst::replay_removes_dead_letter` |
| `transport.deadletter.replay-redelivers` | `dst::replay_redelivers_to_one_group` |
| `transport.deadletter.replay-unknown` | `tests::replay_of_unknown_dead_letter_is_rejected`, `dst::concurrent_replays_of_one_letter_succeed_once` |
| `transport.deadletter.stored-before-release` | `dst::exhausted_delivery_dead_lettered_before_release` |
| `transport.dedup.at-most-once`, `duplicate-acked`, `suppress-only-handled` | `dst::dedup_hands_each_id_once_per_group`, `dst::dedup_acks_suppressed_duplicates`, `dst::dedup_never_suppresses_unhandled_id` |
| `transport.delivery.at-least-once` | `dst::every_published_envelope_reaches_every_group` |
| `transport.delivery.attempt-counts-deliveries` | `dst::attempt_counts_deliveries_per_group` |
| `transport.delivery.envelope-unchanged` | `tests::delivered_envelope_equals_published` |
| `transport.delivery.redelivered-until-acked` | `dst::unacked_delivery_is_redelivered_until_budget` |
| `transport.delivery.single-holder` | `dst::one_holder_per_envelope_per_group` |
| `transport.delivery.subject-filter` | `tests::subscription_yields_only_subscribed_subjects` |
| `transport.nack.retry-after` | `dst::nack_delays_redelivery` |
| `transport.ordering.unconstrained` | `dst::sim_bus_reaches_every_pairwise_order` |
| `transport.retry.backoff-ceiling` | `dst::redelivery_available_within_max_backoff` |
| `transport.retry.backoff-floor` | `dst::redelivery_waits_at_least_initial_backoff` |
| `transport.subscribe.group-retry-mismatch` | `tests::subscribe_rejects_different_retry_policy` |
| `transport.subscribe.group-subject-mismatch` | `tests::subscribe_rejects_different_subject_set` |

Constraints the implementation keeps:

- All bus state has one owner, the bus task; handles reach it only
  through channels (tokio `mpsc` and `oneshot`). No locks, no threads.
- `next` is cancel-safe, on `MpscSubscription` and on `Dedup`.
- An entry is in exactly one state (`Ready`, `Delayed`, `Held`,
  `Exhausted`); ack and nack check the holder and the delivery id.
- A group's held envelopes never exceed `group_capacity`; publishes and
  replays wait in order for room.
- `Delivery::attempt` is the entry's delivery count, restarted only by a
  replay.
- Nothing iterates a hash map where order matters, and the task's
  `select!` is biased, so a run under the paused clock is reproducible.
- Errors and logs never carry payloads: decode and encode reasons name
  only serde_json's category and position, logs carry group, subject,
  event id, delivery id and entry sequence, and nack reasons are stored
  in dead letters but never logged.
- Dead-letter cursors are fixed-length tokens checked with a per-bus
  keyed hash: a token from another bus, another group filter or edited
  text is `InvalidCursor`. The check is not a cryptographic MAC.
- `MemoryHandledIds` and the dead letters live as long as the bus task
  and are never pruned.

## Blob store

The L2 content-addressed blob store: the spec's `BlobStore`
(`spec/types/interfaces/l2_transport.rs`) implemented on the filesystem
and in memory, in `crosstalk-transport`'s `blob` module. Roadmap item P2.2.

### Scope

- `FsBlobStore`: message bodies as files under one directory, keyed by
  the BLAKE3 hash of their bytes, written atomically and durably, verified
  on every read.
- `MemoryBlobStore`: the same contract in process memory, for tests and the
  simulation.
- `message_hash`: the content key, `MessageHash` over unkeyed BLAKE3.
- Tests for the blob invariants' property evidence, and for idempotence,
  corruption, concurrent writes, missing bodies and error values.

### Non-scope

- Postgres (`PgBlobStore`) and object storage (`ObjectStoreBlobs`), which
  the spec's module doc names. Their integration evidence (INV-104 to 107,
  INV-109) stays pending.
- Content retention. The spec has no retention policy for bodies and no
  way to delete one (see below), so neither store has a deletion hook.
- Cleaning up temporary files a crashed process left behind.
- The event bus, dead letters and retries (the sections above).

### Contract

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

### Data and control flow

#### `FsBlobStore`

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

#### `MemoryBlobStore`

`Arc<Mutex<HashMap<MessageHash, Arc<[u8]>>>>`. Clones share the map. Each
call takes the lock for one map operation and never holds it across an
await. `put` inserts if absent, and `get` copies the body out and rehashes
it, so corruption is reported here as well. Tests can produce corruption
through the `#[cfg(test)]` method `insert_unchecked`. A poisoned lock
becomes `Unavailable`.

### Content retention: a spec gap

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

### Files

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

### Invariant evidence

| Invariant | Evidence | State |
| --- | --- | --- |
| INV-105 `transport.blob.corrupt-detected` | property `crosstalk_transport::blob::tests::get_detects_corrupted_bytes` (both stores, plus a reopened `FsBlobStore`) | implemented, agent-reviewed |
| INV-106 `transport.blob.get-matches-map-model` | property `crosstalk_transport::blob::tests::blob_store_matches_map_model` (random put/get sequences, both stores, against a `HashMap` model) | implemented, agent-reviewed |
| INV-108 `transport.blob.put-returns-blake3` | property `crosstalk_transport::blob::tests::put_returns_blake3_of_bytes` | implemented, agent-reviewed |
| INV-104 `transport.blob.concurrent-put` | dst `crosstalk_transport::dst::concurrent_puts_of_same_bytes_succeed` | pending until `crosstalk-sim` exists. The tokio tests `concurrent_puts_of_the_same_bytes_all_succeed` in `tests/fs.rs` and `tests/memory.rs` cover the same property without simulation. |
| INV-104, 105, 106, 107, 109 integration | Postgres and object-store paths | pending; these stores are not implemented yet |
| INV-111 `transport.confidentiality.no-payload-in-logs` | the bus's property evidence, above | `blob::tests::fs::blob_errors_omit_payload_bytes` covers the blob side |

The three property paths were moved from `crosstalk_transport::tests::` to
`crosstalk_transport::blob::tests::`, which keeps the blob tests in the
blob module.

### Invariants and constraints

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
