# Wire contract

The spec types are the wire format. The gateway sends them to the operator
UI as JSON over HTTP and SSE, takes the UI's requests as JSON, and in
cluster mode sends bus events between nodes over NATS as JSON. This
feature defines how every type encodes, which types a client may send,
which values only the server may produce, and the golden files that pin
each shape so a format change is always a reviewed diff. The conventions
live in `spec/types/wire/`; each type's serde derive or impl sits beside
the type.

This page holds the conventions, the harness, the authority rules and how
the UI consumes the contract. Each area's reference (which of its types
travel, what decoding checks, its goldens) is a page under
[`wire/`](#areas).

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
- The golden-file harness: one JSON file per shape (one JSONL file for an
  export's lines), the `CROSSTALK_BLESS=1` rewrite, the rejection and
  request checks.
- Every type reachable from a wire root (a `QueryApi` argument or result,
  the action a client sends and its outcome, the live feed's items,
  `Envelope` and every `BusEvent`, an export's lines, `AuditEntry`): its
  encoding, its decode checks, its goldens. The reference area (ids and
  digests, the support types, paging, the alert inbox, the query and
  action errors) is on this page; the rest are the [area pages](#areas).
- The live feed's SSE framing (the JSON each SSE field carries) and the
  projection's split into JSON and binary.
- How the UI consumes the contract, and which spec type or method answers
  each of its remaining stand-ins and workarounds.

## Non-scope

- HTTP routing (paths, methods, which argument travels in the path, the
  query string or the body), status codes, authentication headers, and
  the HTTP side of the live feed, the projection frame and exports: the
  [HTTP API](http_api.md) binds this contract to HTTP, with its own route
  table, status goldens and the few wire types it adds (the `POST` read
  bodies and the 401's `AuthError`).
- NATS subjects and stream names: the bus's own feature fixes those; this
  feature fixes the JSON payload they carry.
- Binary encodings: the projection frame's layout (`ProjectionFrame`), the
  export digest's canonical row encoding and Parquet pages are defined
  with their types.
- Types no wire root reaches keep no serde impls: traits, in-process
  values (the proxy hot path, the message bodies the blob store holds,
  readers' copies and drafts), store errors that map into `QueryError`,
  config, and `Projection`, whose frame is binary. Each area page lists
  its own.
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
  live:     LiveItem per SSE event (`event:` its type, `id:` its cursor, `data:` its JSON)  ◀── SSE ── LiveFeed
  action:   ActionRequest ── decode_request ──▶ into_action(&Caller) ──▶ OperatorAction ──▶ OperatorActions::act

node A ── NATS: Envelope JSON ──▶ node B: serde_json::from_slice::<Envelope>
                                     unknown field or variant ─▶ decode error ─▶ Err(BusError::Decode) once per group, then dropped and logged
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
| `std::time::Duration` | whole microseconds, a number, in a field named `<what>_micros`: `"lag_micros": 30250000` ([flow](wire/flow.md#durations)) |
| `Watermark` | its timestamp |
| number newtype (`TopicModelVersion`, `SecretVersion`) | the number |
| `NonZeroU16`, `NonZeroU32`, … | number; `0` is a decode error |
| float | a number, only behind a checked type that refuses NaN and infinities (`Similarity`, `Share`, `Finite`, `Embedding`) |
| `Option<T>` | `T` or `null`; `None` is always written |
| `Vec<T>`, `NonEmpty<T>`, `IdBatch<T>` | array (`NonEmpty`: never empty; `IdBatch`: ascending, distinct) |
| map keyed by an id | a `BTreeMap`: object keyed by the id's text, in ascending id order |
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
`Similarity`, `Share`, `Finite` and `Embedding`, each finite by
construction and on decode (`canonical.wire.finite-floats`). The UI is
Rust, so `u64` is exact; a JavaScript consumer would need a big-integer
reader for counts above 2^53. No bare `u128` is on the wire (ids, and
`ConnectionId`, are text).

### Strict decoding

Every struct and every adjacently tagged enum sets
`deny_unknown_fields`, and serde refuses unknown variants, so:

- a node that receives an event from a newer node with a field or variant
  it does not know fails to decode it. An undecodable payload has no
  `Envelope`, so it has no delivery to nack and cannot become a
  `DeadLetter`, which holds an `Envelope`. Instead each consumer group
  gets it once as `Err(BusError::Decode)`, and the message is then
  dropped and logged, never redelivered (INV-110,
  `transport.codec.undecodable-not-redelivered`). An operator sees it in
  the log, and it is never applied without the new data;
- a client request with a misspelt field, or with a field the server
  stamps (an author, a time), is a `MalformedRequest`, never a value with
  the field silently dropped.

A format change is therefore a coordinated upgrade of every node and the
UI, and its golden diff is where it is reviewed.

Three leniencies remain, accepted and documented rather than closed,
because none of them adds, drops or misreads data:

- a unit variant written as `{"type": "open", "data": null}`;
- an all-unit enum written as `{"open": null}`;
- a struct written as a JSON array of its fields in declaration order
  (`[{"message": .., "index": 0}, {"start": 0, "end": 8}]` decodes as a
  `SpanLocation`). Serde's derived visitor accepts a sequence for every
  struct; refusing it would take a hand-written `Deserialize` per type.

None is an unknown field or variant, so strictness holds; a checked type's
constructor runs on whichever spelling it is decoded from, so its checks
hold too. The goldens pin the canonical forms, and nothing the gateway
writes uses the others.

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
  the entity ids, `TimeWindow`, `IdBatch<T>`, `PageRequest<L>`, `Option`
  of any request, and `AlertFilter`, `TopologyFilter`,
  `TopicVersionSelector`, `Weighting`, `EdgeSelector`, `SeriesGrid`,
  `SeriesGrouping`, `AgentFilter`, `ResourcePattern`, `TopicModelVersion`,
  `ProjectionParams`, `UserRule`, `ChannelFilter`, `AlertRuleFilter`,
  `SearchRequest`, `AuditFilter`, `ConsumerGroup`,
  `TransmissionSelection`, `ExcerptWindow`, `ExportRequest`,
  `ActionRequest`, and the [HTTP API](http_api.md)'s `POST` read bodies
  (`GraphBody`, `OverviewBody`, `EdgeTransmissionsBody`,
  `TransmissionsBody`, `SeriesBody`, `SearchBody`, `FitProjectionBody`). The
  gateway's HTTP layer decodes client input only through
  `decode_request::<T: WireRequest>`, so implementing the trait is the one
  decision that lets a client send a type. An axum extractor generic over
  `T: WireRequest` calls it on the body (or on the JSON of a query
  parameter) and rejects with `QueryError::from(DecodeError)`
  (`ActionError::from` on an action route), a `400` with that JSON
  ([HTTP API](http_api.md#arguments) says where each argument travels).
- **Responses and bus events** derive `Serialize` and `Deserialize`
  (the UI and other nodes decode them) and are never `WireRequest`.
- **Authority** never comes from the client. `Caller` implements neither
  serde trait, and no decoded value becomes one (an audit record keeps a
  `CallerSnapshot`, `{"operator": .., "permissions": [..]}`, plain data
  that is not a `Caller`; see [surface actions](wire/surface_actions.md#audit-log)):
  the extractor builds it from the verified session through
  `OperatorDirectory::caller`, and responses name its `OperatorId`. So do
  `RequestIdentity`, `OperatorDirectory` and `Promotion` (built in process
  from a `PromoteChannel`, the caller and the acceptance time). Every
  record the surface stamps with an author or a time is a response or bus
  payload but never a request: `MergeRequest` and `MergeAuthor`,
  `MergeRecord`, `Reversal`, `MergeVeto`, `TransmissionVerdict`,
  `VerdictLog`, `Pin`, `PolicyDecision`, `Decision`, `PolicyAuthor`,
  `Declaration`, `PolicyHistory`, `PromotionPreview`, `OperatorAction`,
  `OperatorRecord`, `AuditEntry`, `ExportHeader`, `ExportRecord`,
  `ExportLine`, `CallerSnapshot`, `ProjectionInfo`, `ProjectionSpec`,
  `AlertRuleDef`, `Alert`, `AlertState`, `Envelope`, `BusEvent`,
  `DeadLetter`, `Supersession`, `SupersededInto`, `Policy`, `Retention`,
  `AlertRule`, `VerdictRow`, `ConfigChange`, `ConfigRecord`, `Operator`,
  `PermissionSet` and `Present` (the gateway's clock and config). The table in `wire/authority.rs` names what each
  one holds that the server stamps, with one assertion per type.
  `wire/authority.rs` asserts all of this at compile time (a hand-written
  `assert_not_impl!`, the `static_assertions` technique), and its
  `compile_fail` doctests show decoding a `Caller` or a `Promotion` does
  not build.

`OperatorAction` holds a `MergeRequest`, whose author
`OperatorAction::merge_agents` stamps from the caller, so it cannot be
what a client sends. The action a client sends is `ActionRequest`, the
same actions without the author, which `ActionRequest::into_action`
turns into an `OperatorAction` with the caller
([surface actions](wire/surface_actions.md#actions-request-action-outcome)).

### Errors

`QueryError`, `ActionError`, `ConflictKind`, `InputError`, `FitFailure`
are adjacently tagged; `Permission`, `ProjectionStatusKind` and
`DecodeErrorKind` are strings. An action error encodes exactly as the
query error `QueryError::from` makes of it, so a client reads both with
one decoder. `InputError::UnsupportedFormat { format }` names an export
format the gateway does not write (`Present::export_formats`).
`InputError::MalformedRequest { kind, reason }` is the one
variant the HTTP layer produces itself: `kind` is `syntax` (not JSON, or trailing input), `eof` (cut
short) or `data` (JSON of the wrong shape, including a refused checked
value), and `reason` is serde_json's message with line and column. An
undecodable request reaches no store, and an undecodable action never
becomes an action, so it is not audited.

### Golden files

`spec/types/tests/golden/<area>/<name>.json` holds the exact JSON of one
value, written by `serde_json::to_string_pretty` with a trailing newline.
An export's lines have one `.jsonl` golden
(`surface_reads/export/export_complete.jsonl`): one compact JSON value per
line, each ended by `\n`. `tests/wire/harness.rs`:

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
- `every_golden_is_pretty_json_with_one_trailing_newline` (in
  `tests/wire/mod.rs`) keeps hand edits in the encoder's layout: pretty
  JSON for a `.json` golden, compact lines for a `.jsonl` one, and no
  other file under `golden/`.

Enums with many variants have one golden listing a value of every
variant, built through an exhaustive `match` with no wildcard, so a new
variant does not compile until it is added to its golden. After an
intended format change:

```sh
CROSSTALK_BLESS=1 cargo test -p crosstalk-spec wire
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
- It imports the spec's types for everything it reads and sends. What it
  still declares itself (`ui/src/contract/`, its alert-state helper and
  its workarounds) has a spec answer listed below, so those go too.
- It sends actions as `ActionRequest` (never `OperatorAction`, which holds
  a stamped merge author), and reads the audit log's callers as
  `CallerSnapshot` values, which never become a `Caller`.
- The fixture backend's JSON fixtures are replaced by the goldens: a
  fixture test reads `spec/types/tests/golden/<area>/<name>.json`
  (`include_str!`), decodes it into the spec type and renders from it, so
  the UI is tested against the exact bytes the gateway sends, and a golden
  change breaks the UI's build or tests in the same review.

### What replaces the UI's stand-ins

For deleting what is left of the UI's own contract on `feat/ui`: the
module `ui/src/contract/` (`present.rs`, `formats.rs`), the helper
`ui/src/backend/alert_state.rs`, and the workarounds `docs/features/ui.md`
lists under "Remaining gaps". Every other type the UI reads or sends is
already the spec's. Paths are under `spec/types/`; `l8/` is
`interfaces/l8_surface/`, `agg/` is `aggregates/`.

**The stand-in module and helper.** Each becomes spec API, so the files
are deleted.

| UI stand-in | Spec answer |
| --- | --- |
| `contract/present.rs`: `Present::bucket_width` | `QueryApi::present(&Caller)` (View) returns a `Present` (l8/present.rs) whose `bucket_width` equals L7's `EdgeStore::bucket_width`; a client reads it once per view instead of from a trait the backend implements |
| `contract/present.rs`: `Present::now` | `Present::now`, the gateway's wall clock when it answered (never before the watermark) |
| `contract/formats.rs`: `ExportFormats::export_formats` | `Present::export_formats`: an `ExportFormats` (l8/export/request.rs; non-empty, distinct, in offer order, `["jsonl"]` on the wire). An export in another format is `InvalidInput(UnsupportedFormat { format })` after the permission check and before anything is read, audited as refused, so the fixture's Parquet refusal changes from `Store` to that |
| `backend/alert_state.rs`: `kind`, `is_active` | `AlertState::kind` and `AlertState::is_active` (agg/alert/mod.rs); `AlertStateKind` moved there, still re-exported as `l8_surface::AlertStateKind`, and gained `ALL` and `is_active` |

**The workarounds.** What each one in the UI's list maps to after this
branch.

| UI workaround | After this branch |
| --- | --- |
| Projection frames have no channel column; `data::projection::point_channels` reads `transmissions_by_id` once per page of channel-routed points | `ProjectedPoint::route` is a `PointRoute` (agg/projection/mod.rs): its kind and, for a channel route, the canonical channel when the sample was read. The frame (format 2, agg/projection/frame.rs) adds a channels table and a channel index column (`NO_CHANNEL` off channel routes), so `point_channels` goes; one `channel_names` batch over `FrameTables::channels` names them, resolving a channel superseded since to the channel in force. Shape changes for the UI: `point.route == RouteKind::Channel` becomes `point.route.kind()` or `point.route.channel()`, and an export's point row carries `{"type": "channel", "data": "<id>"}` |
| No `QueryApi::alert_rule(id)`; `pages::common::rules::rule` lists every rule | `QueryApi::alert_rule(&Caller, AlertRuleId)` (View): the `AlertRuleDef` `alert_rules` lists under the id, `None` for an id no rule had |
| `MergedInto` has no time or author; alias rows search `AgentCluster::merges()` | Not duplicated into `MergedInto`: `AgentCluster::merge_of(alias)` (agg/agents/mod.rs) returns the record that merged it (`MergeRecord::at`, `by`), and `AgentCluster::new` refuses a cluster missing an alias's unreverted record (`AliasMergeMissing`), so the lookup never fails for an alias |
| `AlertRuleConfig::default_remap_threshold` not exposed; `DEFAULT_REMAP` is 0.80 | `Present::default_remap_threshold` |
| The rules' current topic version not exposed; the rule form uses `topic_versions().active()` | `Present::current_rule_version`, the version `CreateRule` and `UpdateRule` check against. It is not the active version: it moves on `TopicVersionReady`, before L7 activates the version, so a form built from `active()` can be refused with `TopicVersionNotCurrent` |
| Frame retention not exposed | `Present::frame_retention_micros`, a `FrameRetention` (agg/projection/mod.rs); a ready projection expires at `FrameRetention::expires_at(fitted_at)` |
| `SearchMode` has no `Default`; `DEFAULT_MODE` is `Hybrid` | `SearchMode::default()` is `Hybrid` |
| Semantic rule text has no length bound | `UserRule::SemanticQuery::text` and `SemanticQuery::text` are a `RuleQueryText` (`QueryText<RULE_QUERY_MAX_CHARS>`, 1,000 characters; support.rs), which a form checks with `RuleQueryText::new`; decoding refuses longer text. `InvalidInput(QueryTooLong)` stays for text within the bound the model still refuses |
| `TopologyGraph` has public fields and no checked constructor; the fixture calls `check_nodes` | `TopologyGraph::new(TopologyGraphParts)` (agg/edge.rs) checks edges, shares and nodes; fields are private behind `window()`, `weighting()`, `topic_version()`, `nodes()`, `edges()`, `into_parts()`. `check` and `check_nodes` are gone: the fixture builds its graphs with `new`. The JSON is unchanged |
| No `From<PinError>` or `From<CatalogError>` for `ActionError`; `pins::refusal` maps them | Both in l8/query_errors.rs: unknown `NotFound`, fitting `Conflict(TopicVersionFitting)`, dropped `Conflict(TopicVersionDropped)` |
| No `From<VerdictError>` for `ActionError`; `triage::set_verdict` builds the refusals | In l8/query_errors.rs, the same variants as the query mapping |
| No `ActionError` mapping for `RegistryError`; `channels` checks the supersession itself | `From<RegistryError> for ActionError`: `Superseded` is `Conflict(ChannelSuperseded)`, `UnknownChannel` `NotFound` |
| `ConfigChange` has no variant for sinks or retention | `SetSink { sink, kind, name }` (never the endpoint), `RemoveSink`, `SetTopicRetention(RetentionPolicy)`, `SetFrameRetention { frame_retention_micros }`, and `AuditSubject::Sink` (l8/audit.rs) |
| No `Send` bounds on the traits | Not this branch: `docs/spec-send-traits` converts every trait method |

**Left as they are.** `ChannelNode::locator_summary` stays preformatted
(the UI names channel nodes from `channel_names`, which it needs anyway);
`EdgeTransmission` still has no state or verdict (a page's rows are one
`transmissions_by_id` call away, by their ids, when a view needs them); `Watermark`'s field stays
public, since a watermark does not know the bucket width it must align
to, so no constructor of its own can check the boundary. The open
semantics the UI's list names (self-edges in search, `edge_transmissions`
alignment, `channels` over all time, a watched-topic rule on an unknown
version) are not answered here.

## Areas

| Page | Covers | Golden directories |
| --- | --- | --- |
| this page | ids and digests, timestamps, the support types, paging, the alert inbox, the query and action errors, decoding requests | `ids`, `support`, `paging`, `alerts`, `errors` |
| [observed.md](wire/observed.md) | exchanges and client context, agent identity and the merge log, harness claims, span locations, content matches, ingest events | `observed`, `provenance` |
| [flow.md](wire/flow.md) | resources, accesses, channels, transmissions, verdicts, detect events; the duration convention | `flow` |
| [topology.md](wire/topology.md) | the topology aggregates and their requests, agent read models, the bus framing and the exhaustive `BusEvent` index, dead letters | `topology`, `agents`, `bus` |
| [analysis.md](wire/analysis.md) | alert rules, topics, retention, projections (info JSON and frame bytes), search, insight events; finite floats | `rules`, `topics`, `projections`, `insight` |
| [surface_actions.md](wire/surface_actions.md) | `ActionRequest` and `OperatorAction`, the audit log and `CallerSnapshot`, the live feed and its SSE framing, operators, sinks, list filters, overview | `surface_actions` |
| [surface_reads.md](wire/surface_reads.md) | channel, transmission and evidence read models, excerpts, the gateway's present and config, export requests, manifests, rows and JSONL lines | `surface_reads` |
| [http_api.md](http_api.md) | the HTTP binding: the route table, the status of every error, the `POST` read bodies, the 401's `AuthError` | `http` |

Coverage the goldens guarantee across the areas: every `BusEvent` variant
has a golden inside a full envelope in its layer's area plus the
exhaustive index in `bus/`; every `UiEvent`, `ActionRequest`,
`OperatorAction` and `AuditBody` variant has a golden; every
`ConflictKind` and `InputError` variant (including `MalformedRequest`,
`EmptySelection`, `ExcerptContextTooLong`, `TooManyIds`, `SelfMerge` and
`UnsupportedFormat`)
is in `errors/conflict_kinds` and `errors/input_errors`. Each is built
behind an exhaustive `match`, so a new variant does not compile until it
is in its golden.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `spec/Cargo.toml`, `Cargo.toml`, `Cargo.lock` | `serde = "=1.0.229"` (derive) and `serde_json = "=1.0.151"`, the UI's pins, declared in the root `[workspace.dependencies]`; the workspace lockfile resolves to the UI's versions | — |
| `spec/types/wire/mod.rs` | The conventions (and the three leniencies), the request marker and decoder, the refusal wrapper, the text decoder, the negative trait assertion macro | `WireRequest`, `decode_request`, `DecodeError`, `DecodeErrorKind`, `Rejected`, `decode_text` (crate), `assert_not_impl!` (module) |
| `spec/types/wire/time.rs` | `Timestamp`'s RFC 3339 text and its serde impls | `Timestamp::rfc3339`, `Timestamp::parse_rfc3339`, `InvalidTimestamp`, `TimestampField`, `TooLateForText`, `MAX`, `MAX_TEXT`, `TEXT_LEN` |
| `spec/types/wire/duration.rs` | Durations as whole microseconds in `_micros` fields | `micros`, `UnfitDuration` |
| `spec/types/wire/authority.rs` | The authority rules: one table of every authority and stamped type, one `assert_not_impl!` per type, and the `compile_fail` doctests | — |
| `spec/types/ids.rs` | Ids' text forms and serde impls; entity ids are requests; the ULID helpers `ConnectionId` reuses | `from_ulid_text`, `InvalidUlidText` |
| `spec/types/support.rs` | The building blocks' wire forms | `Blake3::to_hex`, `Blake3::from_hex`, `InvalidHex`, `EmptyList`, `ShareOutOfRange`, `Finite`, `NotFinite`; `TimeWindow` is a request |
| `spec/types/paging.rs` | Page sizes, cursors, page requests (requests) and pages (checked when decoded) | `InvalidPage` |
| `spec/types/batch.rs` | `IdBatch` as an array, decoded through `IdBatch::new`; a request | — |
| `spec/types/aggregates/alert/mod.rs` | `Alert`, `AlertState`, `AlertSubject`, `SuppressReason` (the reference area's alert inbox); rules are in `alert/rules.rs` ([analysis](wire/analysis.md)) | — |
| `spec/types/aggregates/watermark.rs` | `Watermarked<T>` | — |
| `spec/types/aggregates/topic.rs`, `aggregates/projection/mod.rs` | What the errors carry: `TopicModelVersion`, `EmbeddingModel`, `FitFailure`, `ProjectionStatusKind` | — |
| `spec/types/interfaces/l8_surface.rs` | `AlertFilter` (a request), `AlertStateKind`; `agent_names` and `channel_names` as `BTreeMap`s | — |
| `spec/types/interfaces/l8_surface/errors.rs` | The error enums, adjacently tagged | `InputError::MalformedRequest` |
| `spec/types/interfaces/l8_surface/permissions.rs` | `Permission` as a string, `PermissionSet` as an array; `Caller` never serialized; an audit record's `CallerSnapshot` | `CallerSnapshot`, `NoPermissions` |
| `spec/types/interfaces/l8_surface/query_errors.rs` | `From<DecodeError>` for `QueryError` and `ActionError` | — |
| `spec/types/tests/wire/harness.rs` | The golden harness | `assert_golden`, `assert_encodes`, `assert_request_golden`, `assert_request_golden_allowing`, `assert_rejected`, `assert_round_trips`, `authority_key`, `golden_root`, `BLESS`, `AUTHORITY_KEYS` |
| `spec/types/tests/wire/mod.rs` | The fixtures' ids and times, and the golden layout check | `id`, `ts`, `ULID_A`, `ULID_B`, `ULID_C` |
| `spec/types/tests/wire/{ids,time,support,paging,alerts,errors,requests}.rs` | The reference area: goldens, rejections, reference values, `decode_request` | — |
| `spec/types/tests/wire/{observed/,provenance.rs,flow/,topology/,agents.rs,bus.rs,analysis/,surface_actions/,surface_reads/}` | The areas' tests (see each [area page](#areas)) | — |
| `spec/types/tests/golden/<area>/` | 375 goldens: 374 `.json`, 1 `.jsonl` (11 of them the HTTP binding's, under `http/`) | — |

## Invariants and constraints

- Every wire type round-trips: decoding the JSON of a value gives an
  equal value (`canonical.wire.round-trip`).
- A checked type decodes only through its constructor, so invalid JSON is
  a decode error and never a value (`canonical.wire.checked-decode`).
  Where a constructor reads what the value does not hold, decoding checks
  what the value can know about itself, and the area page says so
  (`flow.wire.partial-decode-checks`, `topology.wire.graph-decode-checked`,
  `surface.export.trailer-self-consistent`).
- Decoding refuses unknown fields and unknown variants
  (`canonical.wire.strict-decode`); the three leniencies above are the only
  other spellings accepted.
- Client input is decoded only as a `WireRequest`; `Caller` never
  serializes, nothing converts a decoded `CallerSnapshot` into one, and no
  server-stamped record is a request (`surface.wire.authority-not-decoded`).
  Every `OperatorAction` has exactly one `ActionRequest` form
  (`surface.wire.action-request-covers-actions`).
- An undecodable request is `InvalidInput(MalformedRequest)` with the
  decoder's kind and reason (`surface.query.undecodable-request-invalid-input`).
- Every golden value encodes to its file byte for byte; any change to a
  wire type's JSON fails its golden test until blessed
  (`canonical.wire.goldens-pin-format`).
- Entity ids (and `ConnectionId`) are ULID text, content ids and digests
  lower-case hex, each with one accepted text (`canonical.wire.id-encoding`);
  timestamps are RFC 3339 UTC with six fractional digits, with one
  accepted text, and have no text after year 9999
  (`canonical.wire.timestamp-encoding`); durations are whole microseconds
  in `_micros` fields (`canonical.wire.duration-encoding`); every float is
  finite (`canonical.wire.finite-floats`).
- Every map keyed by an id is a `BTreeMap`, so one value has one encoding
  (`topology.agent-traffic.ordered-keys`, `surface.query.name-maps-ordered`).
- A bus event's inner tag is its subject (`transport.wire.event-tag-is-subject`).
- `None` is always written as `null`; no field is skipped when empty, so
  every shape is fixed.
- The spec's only dependencies are `serde` and `serde_json`, pinned
  exactly; no other crate is added for the wire.

The areas' own invariants are listed on their pages.
