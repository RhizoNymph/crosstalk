# Transport (L2): the in-process bus

`crosstalk-transport` implements the L2 traits of
`spec/types/interfaces/l2_transport.rs` for a single process: `EventBus`
and `Subscription` (`MpscBus`, `MpscSubscription`), `DeadLetterStore`
(`DeadLetters`), consumed `RetryPolicy`s, and an envelope-level dedup
wrapper (`Dedup` over `HandledIds`). Roadmap item P2.1. The single-node
gateway runs on it, and every layer's simulation tests use it (as a
dev-dependency) with a seeded delivery order.

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

- `BlobStore` (P2.2, branch `feat/blob-store`).
- The multi-node `JetStreamBus` and every `integration` evidence of the
  transport invariants (NATS JetStream, Postgres).
- Durability across process restarts: nothing on this bus survives the
  process (`transport.durability.publish-persisted` is JetStream's).
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
(group, subject, entry sequence, decoder position). This differs from the
"nack, retry, dead letter" wording in `wire_contract.md` and the roadmap;
dead-lettering it would take a spec change to `DeadLetter`.

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
| `crates/transport/src/lib.rs` | Crate doc and re-exports | — |
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
| `src/dst/` | Simulation tests; invariant evidence at `crosstalk_transport::dst::*` | — |

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
| `transport.confidentiality.no-payload-in-logs` | `tests::error_values_omit_payload_bytes` (the lint is not written) |
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
