# Flow correlator and consumer (L5, P5)

`crosstalk-flow`'s correlator (`crates/flow/src/correlate/`) and its bus
consumer (`crates/flow/src/consumer/`): co-access plus content match gives
a transmission. A write by one agent and a later read of the same resource
by another form a `CoAccess`; a `ContentMatched` from L4 whose origin span
is one of the write's spans and whose match sits in the read's tool result
confirms the transmission they opened. Only transmissions between
different agents exist, and only they create or confirm channels
([channel_semantics.md](channel_semantics.md)). This is the critical path
to milestone M2 (two agents share a wiki page; crosstalk discovers the
channel and confirms the transmission), which is replayed from public
datasets with corpus timestamps.

## Scope

- `WindowedCorrelator`, the spec's `Correlator`: channel transmissions
  (open, suspect, confirm, extend, expire, late confirmation, a new
  transmission for content after a discard), non-channel routes
  (`Delegation`, `Direct`, `Unobserved`) in the spec's precedence, evidence
  that arrives in any order, ids derived from what they name.
- The pairing rules in one module (`correlate::pairing`), over the spec's
  `WriteOutcome` on `AccessOp::Write`.
- Correlator shards keyed by medium (canonical channel, or resource on no
  channel), the routing between them, and the handoff of a medium's
  evidence on discovery, on promotion and whenever an access resolves a
  resource to a channel.
- The flow consumer: recording accesses (`ChannelTraffic::add_resource`,
  `record_access`, `AccessRecorded`), holding writes until their outcome is
  final, feeding the shards, and applying every decision through the spec's
  store traits (`ChannelTraffic::discover`, `record_transmission`,
  `TransmissionStore::save`), then publishing `ChannelCrossAccessed`,
  `TransmissionConfirmed` and `TransmissionSuspected`.
- The consumer's config section (`FlowConfig`), with the settle window
  (`CorrelationTiming::settle_after`) derived from it, and the content
  retention (`ContentRetention`) bounding content-confirmed pairing.
- Restart durability (P7.3, [postgres_stores.md](postgres_stores.md),
  workstream W4): checkpoints of the shards with deferred acks, held writes
  and tool calls kept as they are taken, the restore that re-feeds what the
  checkpoint has not seen, envelope ids derived from the events, and the
  durable hand-over from the extraction step (`FlowInputs`); see
  [Restart durability](#restart-durability).

## Non-scope

- `ResourceExtractor` (feat/flow-extract) and the step that turns a
  `ConversationDelta`'s tool calls and results into extracted accesses: its
  output reaches the consumer as the local input `Extracted` (see Spec
  gaps).
- The Postgres stores (feat/flow-store). Development and tests run against
  the `crosstalk-memory` reference stores, through the traits only.
- Channel detection beyond what `record_transmission` does: turning
  dormant, `DeclaredChannelUnused`, idle windows
  (`flow.channel.dormant-by-timestamp`, `flow.channel.unused-by-timestamp`).
- Policy: `PolicyChanged`, `set_policy`, policy routing of confirmed
  transmissions (`flow.policy.*`).
- Wiring the consumer into `crosstalk_gateway::pipeline` (a later step).
  The consumer follows the exchange log's pattern: a group name, a subject
  list and a `run` over a `Subscription`.
- Wiring the durable consumer into the gateway (W8): the gateway's L5
  stage still runs the volatile consumer and acks each delivery at once.
- Correlation across processes: two consumer processes correlate their
  own inputs; the registry keeps one channel per resource between them.

## Data and control flow

```text
extraction step ── Extracted ──▶ FlowConsumer::handle_extracted
  Write { outcome: None } ─▶ HeldWrites ── WriteResult ─────────────┐
                                   └─ tick ≥ write_settles_at: Unknown ┤
  Read / Write { outcome } ───────────────────────────────────────────┤
bus (group "flow"): ExchangeCaptured, ContentMatched, ChannelDiscovered, │
  ChannelPromoted, AgentMerged, AgentUnmerged ─▶ handle_event          │
ticker (Settings::tick_every, injected Clock) ─▶ tick(now)             │
                                                                       ▼
                       one queue of steps, run in order (a failed step stays at its head)
  Read/Write ─ add_resource (resource id derived from locator and first sighting;
               DuplicateLocator/DuplicateResource ─▶ the stored one, on its lookup's channel)
             ─ on a channel: Shards::rekey(resource ─▶ channel)
             ─ record_access ─▶ Publish AccessRecorded { access, channel }
             ─ Correlate (not a rejected write) ─▶ Shards::access
  Content    ─ AgentReads::cluster for both agents (kinship) ─▶ Shards::content
  Exchange   ─▶ Shards::exchange (the reader exchange's start)
  Discovered ─▶ Shards::rekey(resource ─▶ channel)    Promoted ─▶ Shards::rekey(superseded ─▶ promoted)
  Tick       ─▶ Shards::tick
  Decide (each Decided the shards return):
    OpenChannel on Resource(r) ─ discover(id derived from r, r, transmission, opened_at)
                                 ─ Shards::rekey(Resource(r) ─▶ Channel(c)) before anything else
    save(transmission) ─▶ Record: record_transmission (channel routes) ─▶ Publish the event
```

**Correlator.** One `WindowedCorrelator` per shard, holding one `Medium`
per medium key: the pairable writes and the reads, the open channel
transmissions by (reader exchange, writer), discarded identities with
their count, and the tool-result matches its reads carried.

- A write that pairs (`pairing::outcome`) and a later read by another agent
  within `correlation_window` form a co-access (`pairing::co_access`, which
  is `CoAccess::new` after the outcome check). It joins the open
  transmission of the read's exchange and the writer, or opens one:
  `OpenChannel { on: Channel(c) | Resource(r) }`, `opened_at` the read's
  time.
- **The window bounds access-only pairing only**
  (`flow.correlator.content-confirms-past-window`, INV-1120). A held
  tool-result match explained by a write of its sender (`pairing::links`)
  pairs that write with the read that carried it whatever the lag, up to
  the content retention (`pairing::content_co_access`: `CoAccess::new`
  with `ContentRetention` as its window; default 30 days, L4's span index
  retention). Every settle first pairs each held match this way
  (`decide::pair_content`), joining its identity's open transmission
  (idempotently: same co-access set in any order) or opening one, then
  decides: the transmission confirms when the read's window closes. A
  dead-drop wiki read a day after it was written confirms; past the
  retention, nothing opens; a write and a read with no such match still
  pair only within the window.
- **A reread refreshes, it does not retransmit**
  (`flow.correlator.reread-refreshes-delivery`, INV-1122). Each medium
  records, per (sender span, reader), the confirmed transmission that
  first delivered it and the last read that carried it
  (`Medium::delivered`). Transmissions are decided in read order
  (`opened_at`, then slot), so a reread is decided after the read it
  repeats whatever order the evidence arrived in. A held match whose span
  another transmission already delivered to the reader refreshes that
  delivery and is dropped (`decide::refresh_repeats`; before pairing,
  `refresh_delivered` for a reread in another exchange); the reread's
  transmission confirms or extends with new content only. A reread within
  the window still opens on its co-access (opening is eager) and, with
  nothing new, is suspected and discarded; a reread past the window opens
  nothing. Only confirmed transmissions are exported, so a reread adds
  none. Delivery records move with a handoff (the earlier transmission
  wins) and are dropped `content_retention + keep` after their last read.
- A tool-result match whose read is known is held in the read's medium;
  one whose read is not known yet waits (`uncarried`) until the read
  arrives or the window of its reader exchange closes.
- When a transmission's window closes (`window_closes_at(read.at)`), every
  held match that one of its writes explains (`pairing::links`: the
  match's origin agent wrote it and its span is among the write's spans)
  confirms it at once (`Confirm`, all such matches, every co-access).
  Otherwise it is suspected (`Suspect`, every co-access), and discarded at
  `expires_at(since)`. While suspected, an explained match confirms it
  immediately; once confirmed, each new explained match extends it.
- A held match explained by a write after its identity's transmission was
  discarded opens a new transmission (the next generation of the identity,
  a new id), confirmed at once.
- A match no write of its sender explains is a shared upstream source: it
  confirms and extends nothing, and the channel transmission stays
  suspected.
- Matches between parent and child (`Kinship`, from `AgentReads`) take
  `Delegation` whatever carried them. A user turn or system prompt is
  `Direct`, the reader's output `Unobserved`, a tool result whose read never
  arrived by its window's close `Direct(ToolResult(name))`, the name from
  `Extracted::ToolCall`. These are collected per identity and opened
  confirmed (`OpenConfirmed`) when the reader exchange's window closes,
  extended afterwards.
- Input arriving after its window closed (by the last tick) is decided at
  once. So transmission states depend only on the inputs and the ticks,
  not on their arrival order within a window.

**Ids.** Transmission ids derive from the identity (reader exchange,
sender, route key and generation) and the opening time; discovered channel
ids from the resource and the opening time; resource ids from the locator
and the first sighting. A redelivery, a retry or a replay mints the same
id, so every store write keyed by it is idempotent.

**Sharding and handoff.** `Shards` hashes a medium key to a shard
(FNV-1a, stable across nodes). An access goes to its medium's shard; a
tool-result match to the shard of the read that carried it, or, until that
read is known, to its reader exchange's shard, from which the read takes
it when it arrives. `Shards::rekey` takes a medium's evidence from its
shard and the target shard absorbs it, pairing writes and reads of the
same resource the two media held apart. The consumer rekeys when it
discovers a channel (before the next step), on `ChannelDiscovered` (another
consumer's discovery), on `ChannelPromoted` (each superseded channel into
the promoted one), and whenever an access resolves a resource to a
channel.

**Time.** The correlator reads no clock. A tick's time is the consumer's
injected `Clock` at the tick (the simulation's or the replay's clock), and
`FlowConsumer::tick(now)` drives one directly. Corpus timestamps far in
the past settle exactly as live traffic when the replay ticks its own
clock; ticking such evidence with wall time would close every window at
once, suspect and discard every channel transmission, and confirm only
through the content-after-discard path.

**Applying decisions.** Each `Decided` becomes the transmission's next
stored state only if the stored state admits it (`lifecycle::advance`);
a decision whose state is already stored goes on to the record and publish
steps, so a step retried after its save committed completes. Events are
published after the store write commits, in commit order.

## Restart durability

The consumer keeps what survives a restart in a `FlowDurability` port
(`consumer::durability`): `Volatile` (memory mode: nothing survives, every
delivery acked once its steps ran), `MemoryDurability` (the Postgres
tables' model, for simulations) and `crate::store::PgFlowDurability`
(migration `0003_restart.sql`). A consumer built with
`FlowConsumer::with_durability` is durable; `FlowConsumer::new` stays
volatile.

```text
extraction step ──deliver(batch)──▶ FlowInputs (DurableInputs: mpsc + oneshot reply)
  handle_batch: Hold ─▶ FlowDurability::hold (held_writes row) ─▶ HeldWrites
                Read / Write ─▶ add_resource, record_access (accesses.recorded_seq)
                               ─▶ access_recorded (accesses.resolved_channel) ─▶ AccessRecorded ─▶ shards
                Release / settle tick ─▶ record_access ─▶ Unhold (row deleted)
                ToolCalled ─▶ tool_calls row ─▶ shards
  reply Ok once the queue is empty, Backlogged otherwise (the caller retries its delta)
bus delivery ─▶ handle_event ─▶ shards; the delivery stays unacked (durable)
checkpoint (every checkpoint_every, or max_unacked deliveries waiting; only with an empty queue)
  Shards::checkpoint ─▶ FlowDurability::save: one transaction writes every shard's
  checkpoints row (format, shards, ticked_through, recorded_through = highest recording
  number committed, state) and its shard_ticks row, and drops the tool calls it covers
  ─▶ ack every delivery taken before it
restore (start, before any input)
  load: checkpoint, held writes, accesses and tool calls with recording number > recorded_through
  Shards::restore (format or shard count it cannot read: FlowRestoreError::IncompatibleSnapshot)
  held writes ─▶ HeldWrites (one whose access was re-fed: its row dropped)
  re-feed in recording order: an access into the medium it was resolved to then
  (hand-off to that channel first, AccessRecorded again, correlate); a tool call named again
  ─▶ subscribe: the bus redelivers every unacked delivery
```

- **Snapshot.** Each shard's `CorrelatorState` (every field of a
  `WindowedCorrelator` but its configuration: media, reads, uncarried,
  timeless and direct matches, exchange starts, tool names, kinship, seen
  accesses and matches, last tick) and the consumer's read routing for the
  media whose home is that shard, as JSON (`SNAPSHOT_FORMAT` 1; maps as
  lists of pairs). `Shards::restore` rebuilds exactly the shards a
  checkpoint was taken from.
- **Only when idle.** `FlowConsumer::checkpoint` refuses (`Busy`) while a
  step waits in the queue: a decision stored but not published, or a held
  write released but not dropped, would otherwise be covered and its
  delivery acked.
- **Re-feed in the recorded medium.** A channel transmission's id derives
  from the medium it opened in, so a re-fed access must pair where it
  first did. The consumer stores each access's resolution right after
  recording it (`FlowDurability::access_recorded`); the restore re-feeds it
  there, or resolves it again when the resolution was never stored.
- **Repeated decisions.** `OpenChannel` for a stored transmission still
  hands its resource's evidence to the stored channel; `Confirm` (or
  `OpenConfirmed`) for a transmission stored confirmed with other content
  extends it with the missing matches and republishes the stored
  confirmation (`apply::reconfirm`), as the extensions the uninterrupted
  consumer applied one by one.
- **Hand-off carries exchange starts.** `WindowedCorrelator::absorb` learns
  the start of every handed access's exchange and decides the matches
  waiting for it, so a match routed to a channel shard before or after the
  hand-off ends the same (found by the restart DST; no bench replay output
  changed).
- **Envelope ids** (`consumer::publish::envelope_id`): `AccessRecorded` per
  access, `TransmissionConfirmed` and `TransmissionSuspected` per
  transmission (one each; a repeat that saw more lands on the first's id),
  `ChannelCrossAccessed` per co-access read and event digest; the envelope
  `at` stays the clock's reading. `FlowDeps` no longer takes an entropy
  seed.
- **Reset** (`PgFlowDurability::reset_correlator`): empty shards at the
  latest stored tick, covering everything recorded, under this binary's
  format; what `crosstalk migrate --reset-correlator` runs (W8).

## Pairing rules and write outcomes

Everything that decides whether evidence counts is in
`correlate/pairing.rs`, bound to the eval spec (#58) types:

| Rule | Where |
| --- | --- |
| `WriteOutcome` sits on `AccessOp::Write` | the spec's `WriteOutcome`, re-exported; `pairing::outcome` returns the field; `pairing::write_op` stores it |
| Rejected writes never pair (INV-958) | the consumer records a rejected write but never correlates it; `co_access` and `links` refuse one |
| Unknown pairs like Delivered (INV-960) | `WriteOutcome::pairs`; no numeric confidence |
| Held writes settle at `write_settles_at` (INV-959) | `pairing::write_settles_at` delegates to `CorrelationTiming::write_settles_at` |
| Shared upstream stays suspected (INV-963) | a carried match confirms only through `links`, which requires a paired write of the sender holding the span |
| Shared public content stays suspected (INV-1087) | agents fetching one page none of them wrote: matches among their results confirm nothing, and with no writer nothing opens (`tests/shared_web.rs`, AI Village-shaped synthetic bash web reads) |
| Retry after a rejected write (INV-962) | the rejected write is never in a medium, so a match on the relayed span links to the retry alone |
| `ExtractedAccess::op` carries the outcome | the extraction step maps it into `Extracted::Write { outcome }` |

**Shared upstream with no writer.** A carried tool-result match whose
read resource no write of its sender explains, on a resource no agent
wrote within the window, is held in the read's medium and opens nothing:
there is no co-access to suspect on. Opening a `Suspected` transmission
"with the read as evidence" (requested after the AgentDojo `read_file`
results) is not representable without a spec change:
`TransmissionState::Suspected` and `TransmissionUpdate::Suspect` hold
`NonEmpty<CoAccess>`; `CoAccess::new` needs a pairing write by a
different agent on the same resource before the read (INV-249, INV-958);
the only ways to open a transmission are `OpenChannel`, which carries a
co-access (INV-276), and `OpenConfirmed`, which INV-963 forbids for such
a match; the lifecycle has no edge into `Suspected` other than from
`AwaitingContent` (INV-1045, INV-289); and a channel route needs a
channel, which a resource gets only from a cross-agent co-access or a
declaration (INV-853). Options for the spec: a suspicion backed by
content (`Suspected` over an evidence enum with a content-and-read
variant, and an `OpenSuspected` update), or a non-channel suspected route.

Evidence for INV-959 and INV-963 exists under the names the eval PR's
TOML files use where they fit:
`consumer::tests::dst::{rejected_write_never_pairs, write_without_result_settles_unknown}`,
`correlate::tests::channel::shared_upstream_match_keeps_transmission_suspected`,
`correlate::tests::props::match_without_sender_write_confirms_nothing`.

## Spec gaps

- **Extracted accesses.** No spec event carries an extracted access with
  its agent, exchange and time. The consumer takes `Extracted`
  (`consumer/input.rs`): reads, writes with or without their outcome, a
  held write's result, and tool call names.
- **Reader exchange start.** `ContentMatch` does not carry its reader
  exchange's `started_at`, which `Confirmed::at` must be for a non-channel
  route. The consumer learns it from `ExchangeCaptured` (and from
  accesses); a match waits until it is known.
- **Tool name.** `Carrier::ToolResult` names the call id; the
  `DirectCarrier::ToolResult` needs the tool name. It comes from
  `Extracted::ToolCall`; unknown names become `"unknown"`.
- **Parent links.** No event carries `Agent::parent`; the consumer reads
  `AgentReads::cluster` for both agents of a match and drops what it knows
  on `AgentMerged`/`AgentUnmerged`.
- **Access store.** The eval PR's batched `AccessStore` is not on this
  base; accesses are recorded with `ChannelTraffic::record_access`.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `crates/flow/src/correlate/mod.rs` | Module map and diagram | re-exports |
| `crates/flow/src/correlate/pairing.rs` | Every pairing rule | `WriteOutcome`, `outcome`, `write_op`, `write_settles_at`, `co_access`, `content_co_access`, `NoPair`, `carried_by`, `links` |
| `crates/flow/src/correlate/retention.rs` | How long content can still confirm a write | `ContentRetention` (`new`, `default_for`, `get`), `InvalidRetention`, `DEFAULT_CONTENT_RETENTION` |
| `crates/flow/src/correlate/route.rs` | Route precedence | `choose`, `Carriage`, `RouteChoice`, `RouteKey` |
| `crates/flow/src/correlate/kinship.rs` | Parent links for `Delegation` | `Kin`, `Kinship` |
| `crates/flow/src/correlate/lifecycle.rs` | Which update may follow which | `Stage`, `UpdateKind`, `advance`, `Illegal` |
| `crates/flow/src/correlate/key.rs` | Medium (shard) key | `MediumKey` |
| `crates/flow/src/correlate/ids.rs` | Derived ids | `Derive`, `transmission_id` |
| `crates/flow/src/correlate/medium.rs` | One medium's evidence | `Medium` |
| `crates/flow/src/correlate/decide.rs` | Pairing (access and content), opening and deciding channel transmissions | `pair`, `settle` |
| `crates/flow/src/correlate/windowed.rs` | The shard's correlator | `WindowedCorrelator` (`new`, `with_retention`, `state`, `from_state`), `CorrelatorState`, `Decided`, `ReadPart`, `MediumEvidence`, `UNKNOWN_TOOL` |
| `crates/flow/src/consumer/mod.rs` | The consumer: steps, events, ticks, batches, run loop with deferred acks | `FlowConsumer` (`new`, `with_durability`, `handle_batch`, `answer`, `run`), `FlowDeps`, `GROUP`, `group`, `SUBJECTS` |
| `crates/flow/src/consumer/input.rs` | The local input and the extraction step's seam | `Extracted`, `Observed`, `WriteCall`, `ReadResult`, `ToolCalled`, `FlowInputs`, `NotDurable`, `DurableInputs`, `ExtractedBatch`, `InputSource` |
| `crates/flow/src/consumer/durability.rs` | What survives a restart: the port | `FlowDurability`, `Volatile`, `MemoryDurability`, `Recovered`, `Recorded`, `Resolved`, `DurabilityError` |
| `crates/flow/src/consumer/checkpoint.rs` | Snapshot types and errors | `Checkpoint`, `ShardSnapshot`, `StoredCheckpoint`, `SNAPSHOT_FORMAT`, `Incompatible`, `FlowRestoreError`, `CheckpointError` |
| `crates/flow/src/consumer/restore.rs` | Checkpoint, restore, re-feed | `FlowConsumer::{checkpoint, restore}`, `Checkpointed`, `Restored` |
| `crates/flow/src/correlate/snapshot.rs` | Serde for the correlator's state | `pairs`, `non_channel`, `timeless` |
| `crates/flow/src/consumer/held.rs` | Writes held until their outcome | `HeldWrites` |
| `crates/flow/src/consumer/shards.rs` | Shards and routing, handoff | `Shards` |
| `crates/flow/src/consumer/resources.rs` | Resources and accesses | `resource_id` |
| `crates/flow/src/consumer/apply.rs` | Decisions to stores and events | `discovered_channel_id` |
| `crates/flow/src/consumer/publish.rs` | Envelopes under ids derived from the event | `Publisher`, `PublishError`, `envelope_id` |
| `crates/flow/src/consumer/settings.rs` | Config | `FlowConfig`, `Settings` (`content_retention` among them), `InvalidFlowConfig` (`ContentRetention` among them) |
| `crates/flow/src/consumer/error.rs` | Step failures | `StepError` (`is_permanent`) |
| `crates/flow/src/correlate/tests/` | Unit and property tests; shared fixtures | — |
| `crates/flow/src/consumer/tests/` | Wiki scenario, rules, simulations | — |
| `crates/flow/src/dst/` | Restart simulations and the checkpoint rules | — |
| `crates/flow/src/integration/checkpoint.rs` | `PgFlowDurability` on Postgres | — |

`FlowConfig` (`flow` section, milliseconds, every field defaulted):
`{"correlation_window_ms": 600000, "evidence_window_ms": 120000,
"suspected_ttl_ms": 1800000, "content_retention_ms": 2592000000,
"shards": 1, "tick_ms": 1000, "checkpoint_ms": 10000,
"checkpoint_unacked": 512}`; unknown fields are refused, zero durations,
zero shards and a zero `checkpoint_unacked` rejected, and a `content_retention_ms` shorter than
`correlation_window_ms` rejected (`InvalidFlowConfig::ContentRetention`).
`LiveConfig::flow` and the gateway config's `flow` section carry it
unchanged.

## Invariants and constraints

- Correlator: `flow.correlator.order-insensitive` (INV-252),
  `flow.correlator.shard-affinity` (INV-253),
  `flow.correlator.resource-shard-handoff` (INV-855); routes INV-269 to
  INV-275; transmissions INV-277 to INV-290; timing INV-573, INV-574,
  INV-576. Their evidence is here and reviewed by the agent.
- Consumer: `flow.channel.confirmation-advances-canonical-detection`
  (INV-740, unit), `flow.registry.at-most-one-channel-per-resource`
  (INV-852, dst), `flow.channel.resource-only-until-cross-agent`
  (INV-853, dst).
- `flow.correlator.content-confirms-past-window` (INV-1120): content
  confirms within the retention whatever the correlation window; the
  window bounds access-only pairing (`correlate::tests::content_age`,
  `consumer::tests::wiki::a_dead_drop_read_a_day_later_confirms`,
  `consumer::tests::rules::content_retention_is_configured`).
- `flow.correlator.reread-refreshes-delivery` (INV-1122):
  `correlate::tests::rereads`.
  `flow.coaccess.within-window` (INV-250) holds for both: a co-access's
  lag never exceeds the window it was built with.
- New (INV-X): `flow.consumer.stored-transmission-never-regresses`,
  `flow.correlator.settles-on-the-injected-clock`,
  `flow.consumer.resource-evidence-follows-its-channel`.
- Restart: `flow.consumer.restore-equivalent` (INV-1215, dst, flipped),
  `flow.checkpoint.ticks-with-state` (INV-1216: checked after every DST
  checkpoint; its Postgres evidence `integration::checkpoint` awaits a run
  against node0), INV-1202's flow path
  (`dst::redelivery_republishes_the_same_envelope_ids`). New (INV-X):
  `flow.restore.refeed-in-recorded-medium`,
  `flow.correlator.handoff-carries-exchange-starts`,
  `flow.checkpoint.incompatible-refuses-start`,
  `flow.consumer.acks-after-checkpoint`,
  `flow.consumer.held-writes-survive-restart`. `PgShardTicks` has no
  writer of its own: the checkpoint's transaction is the only one.
- `flow.correlator.no-io` (INV-251) holds by construction (no clock, no
  store, no channel in `correlate/`); its lint does not exist yet, so its
  evidence is not flipped.
- Retention: a shard drops evidence older than `settle_after +
  correlation_window` (`keep`) before its last tick, except a write that
  carries spans, which it keeps per medium until `content_retention +
  keep` before it, so a read still within `keep` finds every write it may
  pair with by content. A write without spans can explain no content. So
  memory grows with the writes holding spans in the retention, not with
  reads or matches. A match for an exchange whose start never arrives
  waits indefinitely.
- A transient store or bus failure stalls the queue at that step until the
  next input or tick retries it; a permanent one is logged at error and
  dropped. Events are published at least once.

## Tests

- `correlate::tests::{channel, routes, pairing, handoff}`: unit tests per
  rule; `correlate::tests::content_age`: content an hour, a day and 29
  days after the write confirms (with hourly ticks collecting garbage in
  between), 31 days does not, a configured retention bounds it,
  access-only pairing keeps the window, every delivery order agrees;
  `correlate::tests::rereads`: two reads of one page version by B give
  one confirmed transmission in every order, a reread a day later opens
  nothing, a reread with new content confirms the new content alone; `correlate::tests::props`: generated evidence (three agents, two
  resources, every carrier, an optional parent link) in shuffled orders
  with interleaved ticks: order insensitivity, forward-only lifecycles,
  routes, identity, one sender per transmission, the settle bound, shared
  upstream.
- `consumer::tests::wiki`: the M2 shape (A writes a wiki page, B reads it,
  the match confirms; one discovered channel and one confirmed transmission
  in the memory stores; events in commit order), the same replayed from
  2019 on a replay clock, a later reader meeting the earlier write, and a
  dead drop read a day later confirmed.
- `consumer::tests::rules`: held and rejected writes, a declared channel,
  late confirmation after a promotion, delegation from `AgentReads`, retries
  in order, stale updates, config.
- `consumer::tests::dst`: `crosstalk-sim` under paused time with matches
  and exchanges reordered, duplicated, dropped, redelivered and late, 24
  seeds each.
- `dst`: a seeded scenario (five agents, one a sub-agent, three wiki pages,
  held writes with and without results, late and repeated matches,
  tool-result, user-turn and delegation matches, ticks every 5 s) run
  uninterrupted and with crashes and outages (the bus and the durability
  port refusing, so steps wait when the crash comes) at seeded points;
  transmissions, recorded channel traffic and transmission events must be
  equal (48 seeds by default, `CROSSTALK_FLOW_DST_SEEDS`; 600 passed);
  checkpoints change no decision; every input handled twice publishes no
  new envelope id. `dst::checkpoint`: exact snapshot round trip,
  incompatible checkpoints, `Busy`, held writes and tool calls across a
  restart, deferred acks in `run`, the checkpoint settings.
- `integration::checkpoint` (Postgres): tick records never ahead of the
  stored checkpoint through checkpoints and a reset; a restore reads back
  held writes and exactly the inputs after the checkpoint, in recording
  order, with their resolutions; a consumer restored over Postgres settles
  a write held before its restart.
