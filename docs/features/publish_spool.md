# Publish spool: SpoolingBus

`crosstalk-transport`'s `spool` module: `SpoolingBus<B>`, an `EventBus`
decorator that appends to an fsynced on-disk spool every envelope its
inner bus cannot take (the database down), and sends the spool, in order
and under the same envelope ids, when the inner bus answers again.
Decision Q5 of [Postgres stores](postgres_stores.md) (its "The publish
spool" section is the design), workstream W1. `serve` wraps
[`PgBus`](pg_bus.md) in it whenever a database is configured (wiring,
config section, metrics and the `crosstalk spool --discard-corrupt`
subcommand: W8, [postgres_mode](postgres_mode.md)).

## Scope

- The decorator and its states (`Direct`, `Spooling`, `Draining`,
  `Corrupt`), changed only under its publish mutex.
- The on-disk format: `LOCK`, segments with records, the `cursor`; the
  fsync points; recovery of a torn tail; corruption barriers.
- The drainer task: probing, batched drain, the switch back to `Direct`.
- Bounds (`max_bytes`, `SpoolFull`), stats (`SpoolStats`, `oldest_at`).
- `discard_corrupt` for the operator command.
- `DrainTarget`, what the decorator needs of its inner bus, implemented
  for `PgBus` and `MpscBus`.

## Non-scope

- The gateway's `spool` config section (and its `dir` under the data
  directory check, the free-space warning), `/readyz`, `/healthz`,
  `/metrics`, the `spool_full` capture outcome, the `crosstalk spool`
  subcommand and `PgFrontierSource`'s use of `oldest_at`: W8.
- End-to-end outage tests through the proxy: W9.
- Spooling anything but `BusError::Disconnected`: every other error of
  the inner bus passes through.

## On disk

`<dir>/` (the gateway puts it at `<data dir>/spool/`):

- `LOCK`: `File::try_lock`ed for the spool's life; a second open is
  `SpoolError::Locked`.
- `segment-<first record number, 20 digits>.log`: header `CTSPOOL1`, then
  records:

  ```text
  u32 LE  payload length (at most 16 MiB)
  [u8;16] first 16 bytes of BLAKE3(record number LE ‖ payload)
  u64 LE  record number
  payload Envelope wire JSON (the bus codec's bytes)
  ```

- `cursor`: `{"last": n, "segment": s, "offset": o}`, the last record the
  inner bus holds, its segment and the byte after it.

Fsync points: an append writes and `fdatasync`s its record before
`publish` returns `Ok`; a new segment's header is synced, then the
directory; the cursor is replaced by write `cursor.tmp`, `fdatasync`,
rename, directory `fsync`, after the inner bus committed the batch; a
segment is removed only once the cursor is past it, then the directory is
synced. Every file operation runs on the blocking pool
(`spawn_blocking`), under the log's mutex, so a cancelled caller never
leaves the files and the index apart.

## Data and control flow

```text
publish(envelope)                       under the publish mutex
  Direct  → inner.publish
            Ok / any error but Disconnected → returned as is
            Disconnected → state Spooling, then append
  else    → append (behind the backlog)
  append: encode (bus codec); bytes + record (+ header if rolling) > max_bytes
          → SpoolFull { bytes } (nothing written, no wait); else write + fdatasync
          → Ok; a disk error → Disconnected (the partial write is cut back)

drainer task
  Direct    → wait for an append
  Spooling  → inner.probe() every `probe`; Ok → (publish mutex) Draining
  Draining  → batch = next drain_batch records (never past a corruption barrier)
              read + re-check each record, decode strictly
              inner.publish_batch(batch)   one transaction on PgBus, idempotent on ids
                Ok → cursor past the batch, forget it, remove drained segments
                Disconnected → (publish mutex) Spooling
              nothing left → (publish mutex) still empty? remove every segment → Direct
  Corrupt   → drain what precedes the barrier, then idle; appends continue

open(dir)   LOCK; remove a leftover cursor.tmp; read cursor; for each segment:
            before the cursor's segment → removed; else read from the cursor (or
            the header) on:
              intact record → indexed (number must increase)
              end of the LAST segment: short record, or bad checksum ending the
                file, or a header cut short → torn append: truncated, counted
              anything else → corruption barrier (segment, offset); kept on disk
            state: barrier → Corrupt; records → Spooling; else Direct
```

- Ids: the envelope's id and `at` are minted before the spool sees it, and
  the record holds its bytes unchanged, so a drained record has the id it
  was published with. A crash after a batch commits and before the cursor
  moves resends the batch, which `PgBus` absorbs
  (`transport.publish.idempotent-on-id`).
- Order: once anything is spooled, every publish appends behind it until
  the drainer finds the spool empty under the publish mutex, so envelopes
  reach the inner bus in publish order.
- A torn record never returned `Ok` (its `fdatasync` never completed), so
  truncating it loses nothing acknowledged. Its record number is reused by
  the next append; no acknowledged record ever had it.
- After an open, the first append starts a new segment.
- `discard_corrupt(config)` (offline, takes the `LOCK`) truncates the
  barrier's segment at the corrupt record (or removes it, for a bad
  header), dropping that record and everything after it in the segment.
- `MpscBus` as an inner bus is not idempotent on ids; a drain repeated
  after a crash could deliver twice there. Production uses `PgBus`.

## Configuration

`SpoolConfig::new(dir)` (defaults) or `SpoolConfig::with_limits(dir,
max_bytes, segment_bytes, drain_batch, probe)`, checked: a segment holds
at least one record and is no larger than `max_bytes`.

| Bound | Default |
| --- | --- |
| `max_bytes` | 1 GiB (segment files together, headers included) |
| `segment_bytes` | 64 MiB |
| `drain_batch` | 256 records |
| `probe` | 1 s |

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `crates/transport/src/spool/mod.rs` | the decorator, `DrainTarget`, `discard_corrupt` | `SpoolingBus`, `DrainTarget`, `discard_corrupt` |
| `src/spool/drain.rs` | the drainer task | (crate) |
| `src/spool/log.rs` | files, index, recovery, append, drain reads, cursor moves | `Discarded`; `SpoolLog` (crate) |
| `src/spool/record.rs` | the record format | (crate) |
| `src/spool/cursor.rs` | the cursor file, atomic replacement | (crate) |
| `src/spool/config.rs` | bounds | `SpoolConfig`, `InvalidSpoolConfig` |
| `src/spool/state.rs` | state and stats | `SpoolState`, `SpoolStats` |
| `src/spool/error.rs` | errors | `SpoolError`, `SpoolOp` |
| `src/spool/tests.rs` | unit tests and `LogBus` (a test inner bus, idempotent on ids, that goes down) | (tests) |
| `src/dst/spool.rs` | the seeded spool simulation | (tests) |
| `crates/testkit/src/db_link.rs` | `DbLink`, the cuttable relay the integration tests cut the database with | `DbLink` |

## Tests

- Unit (`spool::tests`, `spool::record::tests`): records round-trip and
  every prefix is short, every flipped bit bad; every prefix of a torn
  tail (and a mis-checksummed last record, and a cut header) is
  truncated; a bad record mid-segment, or ending a segment that is not
  the last, is a corruption barrier; a discarded corruption lets the rest
  drain; the cursor survives a crash after each replacement step; segments
  roll, are removed once drained, and a reopen resumes after the cursor;
  an append past the bound is `SpoolFull`, writes nothing and does not
  wait; a second open is `Locked`; states and order over `LogBus`; only
  `Disconnected` is spooled; a reopened spool drains what it held.
- DST (`dst::spool_*`, paused clock, 24 seeds each): random publishes with
  random times, outages, and crashes (plain, mid-append with the record
  torn at a random byte, after a batch commit before the cursor, before
  the switch to `Direct`). Checks: every `Ok` envelope reaches the inner
  bus unchanged, once, in publish order; resends happened and added
  nothing; `oldest_at` never later than an unsent `Ok` envelope's `at`.
  The file work runs on real threads, so a seed fixes the operations but
  not every interleaving.
- Integration over `PgBus` through `DbLink` (Postgres):
  `spool_drain_after_outage_publishes_each_id_once`,
  `spool_publish_survives_restart_while_database_down`.

## Invariants and constraints

| Invariant | Evidence (agent-reviewed: ran) |
| --- | --- |
| INV-1206 `transport.spool.ok-means-durable` | `dst::spool_ok_survives_seeded_crashes` (ran); `integration::spool_publish_survives_restart_while_database_down` (Postgres, not run) |
| INV-1207 `transport.spool.drained-once-under-its-id` | `dst::spool_drains_each_record_once_under_its_id` (ran); `integration::spool_drain_after_outage_publishes_each_id_once` (Postgres, not run) |
| INV-1208 `transport.spool.no-overtaking` | `dst::spool_backlog_is_never_overtaken` (ran) |
| INV-1209 `transport.spool.torn-tail-only` | `spool::tests::every_prefix_of_a_torn_tail_is_truncated`, `spool::tests::a_bad_record_mid_segment_is_corrupt` (ran) |
| INV-1210 `transport.spool.bounded` | `spool::tests::append_past_the_bound_is_spool_full` (ran); the e2e evidence is W9's |

- `publish` returns `Ok` only once the envelope is in the inner bus or
  `fdatasync`ed in the spool.
- States change only under the publish mutex; nothing published after a
  spooled record reaches the inner bus before it.
- The segment files never hold more than `max_bytes`; a full spool refuses
  at once.
- Only a torn last record of the last segment is ever discarded
  automatically; any other bad record stops draining and stays on disk.
- `oldest_at` is the minimum `at` over the records not yet drained.
- Logs name events, segments and offsets, never payload bytes.
