# Wire contract

The spec types are the wire format. The gateway sends them to the operator
UI as JSON over HTTP and SSE, takes the UI's requests as JSON, and in
cluster mode sends bus events between nodes over NATS as JSON. This
feature defines how every type encodes, which types a client may send,
which values only the server may produce, and the golden files that pin
each shape so a format change is always a reviewed diff. The conventions
live in `spec/types/wire/`; each type's serde derive or impl sits beside
the type.

## Scope

- The JSON conventions: field naming, enum tagging, ids, digests,
  timestamps, numbers, options, collections, maps keyed by ids.
- Strict decoding: unknown fields and unknown variants are errors.
- Checked types decoding only through their checked constructors (the raw
  mirror and `TryFrom` pattern) and the `Rejected` error that carries a
  constructor's refusal into serde.
- The request, response and authority split: `WireRequest`,
  `decode_request`, `DecodeError` and its `InputError::MalformedRequest`
  mapping, and the compile-time checks that keep `Caller` and
  server-stamped records out of requests.
- The golden-file harness: one JSON file per shape, the
  `CROSSTALK_BLESS=1` rewrite, the rejection and request checks.
- The reference area, converted completely: ids and digests, the support
  types, paging, the alert inbox (`alerts`, `alert`) and the query and
  action errors.
- Stage 0 of the rest: every type reachable from a wire root (a
  `QueryApi` argument or result, the operator action and its outcome, the
  live feed's items, `Envelope`, an export's lines, `AuditEntry`) has its
  recipe's derives and, if checked, its decode mirror, so every area
  compiles against every other; its goldens, rejection tests and docs
  follow per area.
- How the UI consumes the contract.

## Non-scope

- HTTP routing (paths, methods, which argument travels in the path, the
  query string or the body) and status codes beyond "an undecodable
  request is a 400 with its `QueryError`". The spec pins the JSON of each
  argument and result, not the routes.
- SSE framing and NATS subjects: the live feed's and the bus's own
  features fix those; this feature fixes the JSON payload they carry.
- Binary encodings: the projection frame's layout (`ProjectionFrame`) and
  the export digest's canonical row encoding are defined with their types.
- Goldens and rejection tests of the remaining areas (observed facts,
  provenance, flow, topology, analysis, the rest of the surface, export,
  bus events), written by follow-up workstreams; stage 0 gave their types
  the recipe's impls only.
- Types no wire root reaches keep no serde impls: traits, in-process
  values (the proxy hot path, the message bodies the blob store holds,
  readers' copies and drafts), store errors that map into `QueryError`,
  config, and `Projection`, whose frame is binary.
- Schema evolution beyond "change the golden and upgrade every node
  together": there is no versioned envelope and no tolerant reader.

## Data and control flow

```text
UI (topcoat, crosstalk-spec types)                 gateway
  request value: T: WireRequest
    serde_json::to_vec(&T) ── HTTP body / query ──▶ extractor<T: WireRequest>
                                                      decode_request::<T>(bytes)
                                                        ├─ Ok(T) ─▶ QueryApi / OperatorActions (with Caller
                                                        │           from the verified session, never the body)
                                                        └─ Err(DecodeError) ─▶ QueryError::from / ActionError::from
                                                                               = InvalidInput(MalformedRequest)
  response: serde_json::from_slice::<R>  ◀── JSON ── serde_json::to_vec(&R) (R: Serialize + Deserialize)
  error:    serde_json::from_slice::<QueryError>  ◀── JSON ── QueryError (adjacently tagged)
  live:     UiEvent per SSE `data:` line   ◀── SSE ── LiveFeed

node A ── NATS: Envelope JSON ──▶ node B: serde_json::from_slice::<Envelope>
                                     unknown field or variant ─▶ decode error ─▶ nack, retry, dead letter
```

### Conventions

| Rust | JSON |
| --- | --- |
| struct | object, snake_case keys (the Rust field names), every field present |
| enum with any data-carrying variant | adjacently tagged: `{"type": "<variant in snake_case>", "data": <payload>}`; a unit variant is `{"type": "<variant>"}` |
| enum whose variants are all unit | snake_case string, `"acknowledged"` |
| entity id (`AgentId`, `AlertId`, …) | 26 upper-case Crockford base32 characters, `"01J9Z3K8M4Q7R2T5V6W8X9Y0ZA"` (`ulid_text`, `from_ulid_text`) |
| content id (`MessageHash`, `PromptHash`, `ConfigHash`), `Blake3` | 64 lower-case hex digits |
| secret digest (`CredentialHash`, `AccountHash`) | `{"key": <secret version>, "digest": "<hex>"}` |
| `Timestamp` | RFC 3339 UTC at fixed microsecond precision, `"2026-10-04T12:34:56.789012Z"` |
| `Watermark` | its timestamp |
| number newtype (`TopicModelVersion`, `SecretVersion`) | the number |
| `NonZeroU16`, `NonZeroU32`, … | number; `0` is a decode error |
| `Option<T>` | `T` or `null`; `None` is always written |
| `Vec<T>`, `NonEmpty<T>`, `IdBatch<T>` | array (`NonEmpty`: never empty; `IdBatch`: ascending, distinct) |
| map keyed by an id | object keyed by the id's text |
| tuple | fixed-length array |
| `Cursor<L>` | its token string (the list marker is not on the wire) |
| `PageRequest<L>` | `{"size": 50, "after": null}` |
| `Page<T, L>` | `{"items": [..], "next": null}` |
| `Watermarked<T>` | `{"watermark": "<timestamp>", "value": <T>}` |
| checked type | the shape of its fields, decoded through its constructor |

Enums whose variants are all unit are strings rather than objects
because they are values: filters carry them in query strings
(`AlertStateKind`), sets hold them (`Permission`), and maps can be keyed
by them. Enums with data are adjacently tagged so a variant's payload is
always under `data`, whatever its shape (a newtype's id, a struct's
fields), and nested errors read the same at every level:

```json
{"type": "conflict", "data": {"type": "rule_stale", "data": {"rule": "01J9Z3K8M4Q7R2T5V6W8X9Y0ZA"}}}
```

**Ids** have one text each: lower case and the letters Crockford leaves
out (I, L, O, U) are refused rather than folded, and a first digit above 7
(more than 128 bits) is refused. **Digests** are lower-case hex only.
**Timestamps** are exactly 27 characters, upper-case `T` and `Z`, six
fractional digits; offsets, other precisions, leap seconds, impossible
dates and times before the epoch are refused. A timestamp after
9999-12-31T23:59:59.999999Z has no RFC 3339 text and fails to encode. The
civil-date arithmetic is hand-written (Hinnant's `days_from_civil` and
`civil_from_days`) rather than taken from `jiff`: the format is one fixed
shape, the parser must refuse every other shape, and a general RFC 3339
parser would need the same shape checks around it; the arithmetic is
checked against day-by-day counting over the whole range.

**Numbers** are JSON numbers: `u64` counts and the floats of
`Similarity` and `Share`. The UI is Rust, so `u64` is exact; a JavaScript
consumer would need a big-integer reader for counts above 2^53. No bare
`u128` is on the wire (ids are text).

### Strict decoding

Every struct and every adjacently tagged enum sets
`deny_unknown_fields`, and serde refuses unknown variants, so:

- a node that receives an event from a newer node with a field or variant
  it does not know fails to decode it, so the consumer nacks the delivery
  and, once its retries are exhausted, it is dead-lettered
  (`l2_transport`), where an operator sees it, instead of being applied
  without the new data;
- a client request with a misspelt field, or with a field the server
  stamps (an author, a time), is a `MalformedRequest`, never a value with
  the field silently dropped.

A format change is therefore a coordinated upgrade of every node and the
UI, and its golden diff is where it is reviewed. Two alternate spellings
serde still accepts are not unknown fields or variants: a unit variant
written as `{"type": "open", "data": null}`, and an all-unit enum written
as `{"open": null}`. The goldens pin the canonical forms; nothing the
gateway writes uses the others.

### Checked types

A checked type (private fields, a constructor returning `Result`) never
derives `Deserialize` on itself. It derives `Serialize` (its fields are its
wire shape) and deserializes through a private raw mirror of its fields
and `TryFrom` into its constructor:

```rust
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawTimeWindow")]
pub struct TimeWindow { start: Timestamp, end: Timestamp }

#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawTimeWindow { start: Timestamp, end: Timestamp }

impl TryFrom<RawTimeWindow> for TimeWindow {
    type Error = Rejected<EmptyWindow>;
    fn try_from(raw: RawTimeWindow) -> Result<Self, Self::Error> {
        Self::new(raw.start, raw.end).map_err(|error| Rejected::new("time window", error))
    }
}
```

A one-value checked type uses `#[serde(try_from = "u16", into = "u16")]`
(`PageSize`, `Similarity`, `Share`); a text one uses
`wire::decode_text` (`NonBlank`, `DisplayText`, `Cursor`, ids, digests,
timestamps). Decoding accepts exactly what the constructor accepts,
including its normalization: `NonBlank` and `DisplayText` trim, `IdBatch`
sorts and drops repeats. Serde requires a `try_from` error to be
`Display`; `Rejected<E>` wraps the constructor's typed error with what was
being decoded, so the domain error enums stay plain. A decoded `Page`
checks what it can know about itself (at most `PageSize::MAX` items, items
whenever there is a next cursor); the size it was requested with is not
on the wire.

### Requests, responses and authority

- **Requests** implement `WireRequest` (`Serialize + DeserializeOwned`):
  in the reference area the entity ids, `TimeWindow`, `IdBatch<T>`,
  `PageRequest<L>`, `AlertFilter`, and `Option` of any request; since
  stage 0 also `TopologyFilter`, `TopicVersionSelector`, `Weighting`,
  `EdgeSelector`, `SeriesGrid`, `SeriesGrouping`, `AgentFilter`,
  `ResourcePattern`, `ProjectionParams`, `ChannelFilter`,
  `AlertRuleFilter`, `SearchRequest`, `AuditFilter`,
  `TransmissionSelection`, `ExcerptWindow` and `ExportRequest`. The
  gateway's HTTP layer decodes client input only through
  `decode_request::<T: WireRequest>`, so implementing the trait is the one
  decision that lets a client send a type. An axum extractor generic over
  `T: WireRequest` calls it on the body (or on the JSON of a query
  parameter) and rejects with `QueryError::from(DecodeError)`
  (`ActionError::from` on an action route), a `400` with that JSON.
- **Responses and bus events** derive `Serialize` and `Deserialize`
  (the UI and other nodes decode them) and are never `WireRequest`.
- **Authority** never comes from the client. `Caller` implements neither
  serde trait (an audit record writes the caller it keeps as a private
  `RecordedCaller`, `{"operator": .., "permissions": [..]}`, which a
  client decoding the record reads back into a `Caller`): the extractor builds it from the verified session through
  `OperatorDirectory::caller`, and responses name its `OperatorId`. So do
  `RequestIdentity`, `OperatorDirectory` and `Promotion` (built in process
  from a `PromoteChannel`, the caller and the acceptance time). Every
  record the surface stamps with an author or a time is a response or bus
  payload but never a request: `MergeRequest` and `MergeAuthor`,
  `MergeRecord`, `Reversal`, `MergeVeto`, `TransmissionVerdict`,
  `VerdictLog`, `Pin`, `PolicyDecision`, `Decision`, `PolicyAuthor`,
  `Declaration`, `PolicyHistory`, `PromotionPreview`, `OperatorAction`,
  `OperatorRecord`, `AuditEntry`, `ExportHeader`, `ExportRecord`,
  `ProjectionInfo`, `AlertRuleDef`, `Alert`, `AlertState`, `Envelope`,
  `Supersession`, `SupersededInto`, `Policy`, `Retention`, `AlertRule`,
  `VerdictRow`, `ConfigChange`, `ConfigRecord`, `Operator` and
  `PermissionSet`.
  `wire/authority.rs` asserts all of this at compile time (a hand-written
  `assert_not_impl!`, the `static_assertions` technique), and its
  `compile_fail` doctests show decoding a `Caller` or a `Promotion` does
  not build.

`OperatorAction` holds a `MergeRequest`, whose author
`OperatorAction::merge_agents` stamps from the caller, so it cannot be
what a client sends. The action a client sends is a separate request type
without the author, which the surface turns into an `OperatorAction` with
the caller; defining it is part of the surface's conversion.

### Errors

`QueryError`, `ActionError`, `ConflictKind`, `InputError`, `FitFailure`
are adjacently tagged; `Permission`, `ProjectionStatusKind` and
`DecodeErrorKind` are strings. An action error encodes exactly as the
query error `QueryError::from` makes of it, so a client reads both with
one decoder. `InputError::MalformedRequest { kind, reason }` is the one
new variant: `kind` is `syntax` (not JSON, or trailing input), `eof` (cut
short) or `data` (JSON of the wrong shape, including a refused checked
value), and `reason` is serde_json's message with line and column. An
undecodable request reaches no store, and an undecodable action never
becomes an action, so it is not audited.

### Golden files

`spec/types/tests/golden/<area>/<name>.json` holds the exact JSON of one
value, written by `serde_json::to_string_pretty` with a trailing newline.
`tests/wire/harness.rs`:

- `assert_golden(area, name, &value)`: the value encodes to the file byte
  for byte, and the file decodes back to an equal value;
- `assert_request_golden`: the same for a `WireRequest`, which also
  decodes through `decode_request` and carries no stamp key (`by`, `at`,
  `author`, `caller`, `permissions`, `requested_by`, `created`,
  `accepted_at`) at any depth; `assert_request_golden_allowing` names a
  key that is the client's to choose (`AuditFilter::by`, which authors to
  list), with the reason beside the call;
- `assert_rejected::<T>(json, reason)`: valid JSON that decodes to no
  value, refused as a data error whose message contains `reason`;
- `assert_round_trips(&value)` for values with no golden of their own;
- `every_golden_is_pretty_json_with_one_trailing_newline` keeps hand
  edits in the encoder's layout.

Enums with many variants have one golden listing a value of every
variant, built through an exhaustive `match` with no wildcard, so a new
variant does not compile until it is added to its golden. After an
intended format change:

```sh
CROSSTALK_BLESS=1 cargo test --manifest-path spec/Cargo.toml wire
git diff spec/types/tests/golden
```

Blessing writes every golden its tests reach and still checks that each
decodes back. The diff is the review of the format change.

### How the UI consumes it

The UI (`ui/`, on `topcoat`) depends on `crosstalk-spec` by path with the
same `serde` and `serde_json` pins, so it decodes the gateway's JSON into
the spec types themselves:

- Its HTTP client encodes a request with `serde_json::to_vec` of a
  `WireRequest` value and decodes the response with
  `serde_json::from_slice` into the method's result type (`Page<Alert,
  AlertList>`, `Option<Alert>`, …), and an error body into `QueryError` or
  `ActionError`.
- The stand-ins in `ui/src/contract/` are deleted as their areas land, and
  the UI imports the spec's types. Where a stand-in's shape differs from
  the spec's, the spec's is the contract: the UI's `ConflictKind` variants
  carry no data and its `InputError` is `Field { field, reason }`, while
  the spec's carry the ids involved and `MalformedRequest { kind, reason }`
  respectively; the UI's `Alert` and `AlertState` match the spec's.
- The fixture backend's JSON fixtures are replaced by the goldens: a
  fixture test reads `spec/types/tests/golden/<area>/<name>.json`
  (`include_str!`), decodes it into the spec type and renders from it, so
  the UI is tested against the exact bytes the gateway sends, and a golden
  change breaks the UI's build or tests in the same review.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `spec/Cargo.toml`, `spec/Cargo.lock` | `serde = "=1.0.229"` (derive) and `serde_json = "=1.0.151"`, the UI's pins; the lockfile resolves to the UI's versions | — |
| `spec/types/wire/mod.rs` | The conventions, the request marker and decoder, the refusal wrapper, the text decoder, the negative trait assertion macro | `WireRequest`, `decode_request`, `DecodeError`, `DecodeErrorKind`, `Rejected`, `decode_text` (crate), `assert_not_impl!` (module) |
| `spec/types/wire/time.rs` | `Timestamp`'s RFC 3339 text and its serde impls | `Timestamp::rfc3339`, `Timestamp::parse_rfc3339`, `InvalidTimestamp`, `TimestampField`, `TooLateForText`, `MAX`, `MAX_TEXT`, `TEXT_LEN` |
| `spec/types/wire/authority.rs` | The authority rules and their compile-time checks and `compile_fail` doctests | — |
| `spec/types/ids.rs` | Ids' text forms and serde impls; entity ids are requests | `from_ulid_text`, `InvalidUlidText` |
| `spec/types/support.rs` | The building blocks' wire forms | `Blake3::to_hex`, `Blake3::from_hex`, `InvalidHex`, `EmptyList`, `ShareOutOfRange`; `TimeWindow` is a request |
| `spec/types/paging.rs` | Page sizes, cursors, page requests (requests) and pages (checked when decoded) | `InvalidPage` |
| `spec/types/batch.rs` | `IdBatch` as an array, decoded through `IdBatch::new`; a request | — |
| `spec/types/aggregates/alert.rs` | `Alert`, `AlertState`, `AlertSubject`, `SuppressReason` | — |
| `spec/types/aggregates/watermark.rs` | `Watermarked<T>` | — |
| `spec/types/aggregates/topic.rs`, `aggregates/projection/mod.rs` | What the errors carry: `TopicModelVersion`, `EmbeddingModel`, `FitFailure`, `ProjectionStatusKind` | — |
| `spec/types/interfaces/l8_surface.rs` | `AlertFilter` (a request), `AlertStateKind` | — |
| `spec/types/interfaces/l8_surface/errors.rs` | The error enums, adjacently tagged | `InputError::MalformedRequest` |
| `spec/types/interfaces/l8_surface/permissions.rs` | `Permission` as a string, `PermissionSet` as an array; `Caller` never serialized, an audit record's copy written as `RecordedCaller` | `RecordedCaller` (surface-private) |
| `spec/types/interfaces/l8_surface/query_errors.rs` | `From<DecodeError>` for `QueryError` and `ActionError` | — |
| `spec/types/tests/wire/harness.rs` | The golden harness | `assert_golden`, `assert_encodes`, `assert_request_golden`, `assert_request_golden_allowing`, `assert_rejected`, `assert_round_trips`, `authority_key`, `BLESS`, `AUTHORITY_KEYS` |
| `spec/types/tests/wire/{ids,time,support,paging,alerts,errors,requests}.rs` | One module per area: goldens, rejections, reference values | — |
| `spec/types/tests/golden/{ids,support,paging,alerts,errors}/` | The goldens of the reference area | — |

## Invariants and constraints

- Every wire type round-trips: decoding the JSON of a value gives an
  equal value (`canonical.wire.round-trip`).
- A checked type decodes only through its constructor, so invalid JSON is
  a decode error and never a value (`canonical.wire.checked-decode`).
- Decoding refuses unknown fields and unknown variants
  (`canonical.wire.strict-decode`).
- Client input is decoded only as a `WireRequest`; `Caller` never
  serializes, and no server-stamped record is a request
  (`surface.wire.authority-not-decoded`).
- An undecodable request is `InvalidInput(MalformedRequest)` with the
  decoder's kind and reason (`surface.query.undecodable-request-invalid-input`).
- Every golden value encodes to its file byte for byte; any change to a
  wire type's JSON fails its golden test until blessed
  (`canonical.wire.goldens-pin-format`).
- Entity ids are ULID text, content ids and digests lower-case hex, each
  with one accepted text (`canonical.wire.id-encoding`); timestamps are
  RFC 3339 UTC with six fractional digits, with one accepted text, and
  have no text after year 9999 (`canonical.wire.timestamp-encoding`).
- `None` is always written as `null`; no field is skipped when empty, so
  every shape is fixed.
- The spec's only dependencies are `serde` and `serde_json`, pinned
  exactly; no other crate is added for the wire.

## Observed

The observed facts and provenance on the wire: exchanges and their client
context, agent identity and the merge log, harness claims, spans' locations,
content matches, and the ingest bus events that carry them between nodes.
Areas `observed` and `provenance`.

**Wire data.** Every type below is a response or bus payload; none is a
`WireRequest` (no client sends observed facts).

- `observed/exchange.rs`: `Exchange`, `ExchangeMeta`, `Continuation`,
  `ExchangeOutcome`, `ExchangeFailure`, `TokenUsage`, `ConnectionId`,
  `ResponseId`, `ModelName` and the string enums `WireProtocol`,
  `Transport`, `StopReason`. `ExchangeStage` is in memory only.
- `observed/client.rs`: `ClientContext` and everything it holds
  (`IngressMode`, `Upstream`, `UpstreamKind`, `Vendor`, `InferenceServer`,
  `CredentialRef`, `CredentialScheme`, `HarnessClaim`, `HarnessFamily`,
  `HarnessIds`, `RequestClass`, `PreviousDigests`, `RouteName`,
  `UpstreamId`). `Dialect`, `Stability` and `EndpointKind` are derived in
  process and get no serde.
- `observed/agent.rs`, `agent/claims.rs`, `agent/merge.rs`: `Agent`,
  `AgentState`, `ActiveAgentState`, `IdentityEvidence`, `IdentityScope`,
  `MergeAuthor`, `MergedInto`, `SeenClaim`, `Reversal`, and the checked
  `MergeRequest`, `MergeRecord`, `MergeVeto`, `ClaimSet`. `MergeRequest`,
  `MergeAuthor`, `MergeRecord`, `Reversal` and `MergeVeto` are stamped
  (`wire/authority.rs`). `MergeConflict`, `AlreadyReverted`,
  `InvalidMergeTransition`, `RenameMerged` and `Strength` stay in process.
- `observed/message.rs`: only `PartRef`, `ToolCallId` and `ToolName`. The
  message bodies live in the blob store and never travel as JSON.
- `derived/provenance/span.rs`: `SpanLocation`, `RelaySource`. `Span`,
  `SpanState`, `Origin`, `SpanEvent` and `OriginatedSpan` stay in process.
- `derived/provenance/matching.rs`: `ContentMatch` (checked), `Carrier`,
  `MatchKind`, `Codec`.
- `events/ingest.rs`: `IngestEvent` and `ConversationDelta`.
- `interfaces/l3_reconstruction*.rs`: nothing. Its traits, `Resolution`,
  `ThreadOutcome`, `ResolveError` and `AgentReadError` are in process;
  `ResolveError` reaches the client only as `ConflictKind` and `QueryError`.

**`ConnectionId` is ULID text.** It was a transparent `u128`, a bare JSON
number that no JavaScript reader holds exactly and that broke the "no bare
u128" rule. A connection is an entity the proxy node creates when it
accepts a WebSocket upgrade, and it is stored in
`Continuation::Increment` and compared across nodes, so it is a ULID
minted there and travels as the entity ids' 26-character text
(`ConnectionId::ulid_text`, `from_ulid_text`, through the crate-visible
helpers in `ids.rs`). Lower-case hex is for content ids and digests, which
name content; a connection names no content. It is not a `WireRequest`.

**Checked types decode through their constructors.**

| Type | Refused on decode | Normalized on decode |
| --- | --- | --- |
| `MergeRequest` | one agent as source and target (`SelfMerge`) | — |
| `MergeRecord` | a self-merge (`InvalidMergeRecord::SelfMerge`); a second reversal, which needs a repeated `reverted` key (`duplicate field`) | — |
| `MergeVeto` | an agent paired with itself (`SelfMerge`) | the pair is ordered, lower id in `a` |
| `ClaimSet` | a claim listed twice, at the same or another time (`DuplicateClaim`) | entries are ordered latest first, ties by family, User-Agent, version |
| `ContentMatch` | origin agent is the reader (`SelfMatch`); more matched bytes than the read range (`ExceedsReadRange`); zero matched bytes | — |

`MergeRecord` decodes in the order the log builds it: `MergeRequest::new`,
`MergeRecord::new`, then `MergeRecord::revert` with the stored reversal,
instead of setting the reversal field by field. Not rechecked, because no
constructor checks it: that a reversal's `restored` lists agents of the
record's `repointed` in its order, and that it comes after the merge.
`Agent`, `AgentState` and `MergedInto` are plain: whether a merged agent's
target is canonical needs the merge table, which a value cannot see.

**Ingest events in envelopes.** Each `IngestEvent` variant has a golden
holding a whole `Envelope` (`envelope_<variant>.json`), so the bus framing
(`id`, `at`, `{"type": "ingest", "data": {"type": <variant>, "data": ..}}`)
is pinned with each payload; a rename is pinned with a label and cleared.
The golden names come from an exhaustive match, so a new variant does not
compile until it has a golden.

**Renamed for the wire.** `CredentialScheme::OAuthAccessToken` became
`OauthAccessToken` (the Rust guideline writes a contraction as one word),
so it encodes as `"oauth_access_token"` rather than
`"o_auth_access_token"`. `OpenAi…` names still encode as `open_ai…`.

**Strictness gap found.** Besides the two alternate spellings above, serde
decodes a struct from a positional JSON array (`[{"message": ..,
"index": 0}, {"start": 0, "end": 8}]` is a `SpanLocation`), across the
crate. It is not an unknown field or variant; the goldens pin the object
form.

| File | Role |
| --- | --- |
| `spec/types/tests/wire/observed/{mod,exchange,identity,ingest}.rs` | Goldens and rejections for exchanges and client context, identity and the merge log, and ingest events in envelopes |
| `spec/types/tests/wire/provenance.rs` | Goldens and rejections for span locations, relay sources and content matches |
| `spec/types/tests/golden/observed/`, `golden/provenance/` | The goldens |

Evidence for `canonical.wire.checked-decode`, `goldens-pin-format`,
`id-encoding`, `round-trip` and `strict-decode`, and for
`reconstruct.merge-record.revert-once`, `reconstruct.merge-veto.distinct-pair`
and `reconstruct.claims.distinct-ordered`.
