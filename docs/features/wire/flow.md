# Wire contract: flow

Part of the [wire contract](../wire_contract.md), whose conventions,
harness and authority rules apply here unchanged. This page covers the
flow area (`derived/flow/`, `interfaces/l5_flow.rs` and
`l5_flow/verdicts.rs`, `events/detect.rs`): resources, accesses, channels,
transmissions, verdicts and the L4/L5 bus events, plus the crate-wide
duration convention that started here.

## Scope

- Request: `ResourcePattern` (the pattern of `PromoteChannel` and of
  `promotion_preview`), one request golden per variant.
- Responses and bus payloads: `Host`, `Locator`, `Resource`, `Access`,
  `AccessOp`, `AccessKind`, `Extraction`, `CoAccess`, `Transmission`,
  `Route`, `DelegationDirection`, `DirectCarrier`, `TransmissionState`,
  `Confirmed`, `Classification`, `Verdict`, `VerdictRevision`,
  `TransmissionVerdict`, `VerdictLog`, `Channel`, `Seed`, `ChannelOrigin`,
  `DeclaredHistory`, `DeclaredDetection`, `TrafficDetection`,
  `DetectionKind`, `Confirmation`, `CrossTraffic`, `ListingKind`,
  `PromotionCoverage`, `DetectEvent`. The stamped ones
  (`TransmissionVerdict`, `VerdictLog`, `Declaration`, `Supersession`,
  `Policy`, `Decision`, `PolicyAuthor`, `PolicyDecision`, `PolicyHistory`)
  are never requests (`wire/authority.rs`).

## Non-scope

`Promotion` (authority, built in process), `Evidence`, `NonChannelRoute`,
`Crossing` and `Listing` (both derived at the read and never sent: a row
carries its traffic and derives its listing), `Judgeable`, `NotJudgeable`, `CurrentVerdict`, `VerdictRecorded`,
`Observed`, `PromotionPlan`, `Registered`, `PromotionRefusal`,
`TrafficVerdict`, `Recorded`, `CorrelationTiming` (config), and
everything in `l5_flow.rs` and `l5_flow/verdicts.rs` (traits,
`ExtractedAccess`, `ChannelLookup`, `OpensOn`, `Discovery`, `Promoted`,
`TransmissionUpdate`, `ChannelWithTraffic`, `DetectionUpdate`, the
store errors that map into `ActionError` or `QueryError`): no wire root
reaches them, so they have no serde.

## Durations

A `std::time::Duration` on the wire is its whole microseconds as a JSON
number, in a field named `<what>_micros` (`wire/duration.rs`, applied
with `#[serde(with = "crate::wire::duration")]`). The convention is
crate-wide; `CoAccess::lag` is the only wire duration, so its private
field is `lag_micros` (`"lag_micros": 30250000`, where stage 0 wrote
serde's `{"secs", "nanos"}`); the accessor keeps the name `lag`. Encoding
refuses a duration with a fraction of a microsecond or more than
`u64::MAX` microseconds (`UnfitDuration`); decoding accepts the integers
`0..=u64::MAX` and nothing else. The config durations
(`CorrelationTiming`, `RetryPolicy`, `LiveConfig`) and the `retry_after`
argument of `Subscription::nack` are not wire data. A source-scanning test
(`every_wire_duration_field_uses_the_convention`) fails if a serialized
`Duration` field lacks the attribute or a field using it is not named
`_micros`. Every golden that holds a co-access (the detect envelopes, the
bus index, transmission evidence) carries `lag_micros`.

## Channels, discovery and traffic

A channel exists once a transmission between two different agents goes
through it, so the flow shapes carry no state for a resource before that:

- `Seed` is `{"resource", "first_transmission", "opened_at"}`: the
  resource on no channel and the cross-agent transmission that discovered
  the channel, with the time that transmission opened (what
  `ChannelOrigin::created_at` returns for a discovered, promoted or
  superseded channel). It replaced `first_access`.
- `TrafficDetection` is `active` or `dormant`, each `{"since",
  "last_transmission"}`; `DetectionKind` is `awaiting_traffic`, `unused`,
  `active` or `dormant`. The `observed` and `candidate` states are gone.
- `CoAccess` is `{"write", "writer", "read", "lag_micros"}`: `writer` is
  the write access's agent as attributed, which `Transmission::crossing`
  and a channel's transmission rows read; decoding cannot check it against
  the reader, which the co-access does not hold.
- `DetectEvent::AccessRecorded` is `{"access", "channel"}` with `channel`
  `null` for a resource on no channel; `ChannelDiscovered` is
  `{"channel", "seed"}`, published by the registry when a discovery
  creates a channel.
- `Confirmation` (`"unconfirmed"`, `"confirmed"`) and `ListingKind`
  (`"confirmed"`, `"unconfirmed"`, `"declaration"`) are strings;
  `CrossTraffic` is `{"confirmed", "unconfirmed"}`, two counts. They are
  defined here (`derived/flow/channel/confirmation.rs`) and travel in the
  surface's channel rows, filters and queues, so their goldens are in
  [surface reads](surface_reads.md) (`confirmations`, `cross_traffic`) and
  [surface actions](surface_actions.md) (`listing_kinds`).

## Checked types and what decoding checks

| Type | Decoding |
| --- | --- |
| `Confirmed` | `{"content", "co_access", "at"}`, no sender: `Confirmed::new` rebuilds it as the content's origin agent and refuses several origins or readers; a `from` key is an unknown field |
| `VerdictLog` | `{"transmission", "records": [{"revision", "record"}]}`: each record with its revision, so the UI can line the log up with `VerdictSet` events and verdict export rows. Decoded through `VerdictLog::from_records`, which refuses a gap, repeat or reordering of revisions (`UnexpectedRevision`), a record about another transmission (`Record`) and a record repeating the current verdict (`Unchanged`), each an `InvalidVerdictLog` |
| `PolicyHistory` | `{"entries": [..]}` through `PolicyHistory::from_entries` (`OutOfOrder`, `Duplicate`) |
| `CoAccess` | cannot rerun `CoAccess::new`, which reads the two accesses and the window; checks what the value holds: two different accesses (`WrongOperations`) and a positive lag (`ReadNotAfterWrite`) |
| `PromotionCoverage` | cannot rerun `coverage`, which reads the registry; checks what the value holds (`InvalidCoverage`): no channel superseded twice, each sample strictly newest first, no resource on both sides |
| `TransmissionVerdict` | field by field: its one check reads the transmission's state, which the record names only by id |
| `VerdictRevision` | the number; `0` is refused (`NonZeroU32`) |

## Goldens

One golden per state of `TransmissionState` (inside a `Transmission`), per
origin of `Channel`, per variant of `ResourcePattern` and per
`DetectEvent` variant, each event inside a full `Envelope` (`{"id", "at",
"event": {"type": "detect", "data": {"type": <variant>, "data": ..}}}`);
lists through an exhaustive `match` for `Locator`, `Route`,
`DirectCarrier`, `DelegationDirection`, `DeclaredDetection`,
`TrafficDetection`, `DetectionKind`, `Policy`, `PolicyKind`, `AccessKind`,
`Extraction` and `Verdict`. `AccessRecorded` has a second golden with no
channel (`event_access_recorded_on_no_channel`). The fixtures follow one story: a planner
writes a wiki page, a coder reads it 30.25 s later, and the planner's text
is found in the coder's tool result.

## Files

| File | Role |
| --- | --- |
| `spec/types/wire/duration.rs` | The `_micros` duration encoding (`micros`, `UnfitDuration`), used through `#[serde(with)]` |
| `spec/types/derived/flow/` | The flow types and their decode mirrors (`VerdictLog::from_records`, `InvalidVerdictLog`, `InvalidCoverage`) |
| `spec/types/tests/wire/flow/` | `mod.rs` (fixtures), `resources.rs`, `channels.rs`, `transmissions.rs`, `verdicts.rs`, `events.rs` (detect envelopes), `duration.rs` (the convention and its source scan) |
| `spec/types/tests/golden/flow/` | 53 goldens |

## Invariants

`canonical.wire.duration-encoding`, `flow.wire.verdict-log-decode`,
`flow.wire.confirmed-sender-derived` and `flow.wire.partial-decode-checks`;
the general wire invariants take the area's goldens and rejections as
evidence.
