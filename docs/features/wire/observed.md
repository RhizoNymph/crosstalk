# Wire contract: observed facts and provenance

Part of the [wire contract](../wire_contract.md). This page covers the
observed facts and provenance on the wire: exchanges and their client
context, agent identity and the merge log, harness claims, spans'
locations, content matches, and the ingest bus events that carry them
between nodes. Areas `observed` and `provenance`.

## Scope

Every type below is a response or bus payload; none is a `WireRequest`
(no client sends observed facts).

- `observed/exchange.rs`: `Exchange`, `ExchangeMeta`, `Continuation`,
  `ExchangeOutcome`, `ExchangeFailure`, `TokenUsage`, `ConnectionId`,
  `ResponseId`, `ModelName` and the string enums `WireProtocol`,
  `Transport`, `StopReason`.
- `observed/client.rs`: `ClientContext` and everything it holds
  (`IngressMode`, `Upstream`, `UpstreamKind`, `Vendor`, `InferenceServer`,
  `CredentialRef`, `CredentialScheme`, `HarnessClaim`, `HarnessFamily`,
  `HarnessIds`, `RequestClass`, `PreviousDigests`, `RouteName`,
  `UpstreamId`).
- `observed/agent.rs`, `agent/claims.rs`, `agent/merge.rs`: `Agent`,
  `AgentState`, `ActiveAgentState`, `IdentityEvidence`, `IdentityScope`,
  `MergeAuthor`, `MergedInto`, `SeenClaim`, `Reversal`, and the checked
  `MergeRequest`, `MergeRecord`, `MergeVeto`, `ClaimSet`. `MergeRequest`,
  `MergeAuthor`, `MergeRecord`, `Reversal` and `MergeVeto` are stamped
  (`wire/authority.rs`).
- `observed/message.rs`: only `PartRef`, `ToolCallId` and `ToolName`.
- `derived/provenance/span.rs`: `SpanLocation`, `RelaySource`.
- `derived/provenance/matching.rs`: `ContentMatch` (checked), `Carrier`,
  `MatchKind`, `Codec`.
- `events/ingest.rs`: `IngestEvent` and `ConversationDelta`.

## Non-scope

In process only, with no serde: `ExchangeStage`; `Dialect`, `Stability`
and `EndpointKind` (derived in process); `MergeConflict`,
`AlreadyReverted`, `InvalidReversal`, `InvalidMergeTransition`,
`RenameMerged` and `Strength`; the message bodies (they live in the blob
store and never travel as JSON); `Span`, `SpanState`, `Origin`,
`SpanEvent` and `OriginatedSpan`; and everything in
`interfaces/l3_reconstruction*.rs` (its traits, `Resolution`,
`ThreadOutcome`, `ResolveError` and `AgentReadError`; `ResolveError`
reaches the client only as `ConflictKind` and `QueryError`).

## `ConnectionId` is ULID text

It was a transparent `u128`, a bare JSON number that no JavaScript reader
holds exactly and that broke the "no bare u128" rule. A connection is an
entity the proxy node creates when it accepts a WebSocket upgrade, and it
is stored in `Continuation::Increment` and compared across nodes, so it is
a ULID minted there and travels as the entity ids' 26-character text
(`ConnectionId::ulid_text`, `from_ulid_text`, through the crate-visible
ULID helpers in `ids.rs`; its serde impls are hand-written in
`observed/exchange.rs`). Lower-case hex is for content ids and digests,
which name content; a connection names no content. It is not a
`WireRequest`.

## Checked types decode through their constructors

| Type | Refused on decode | Normalized on decode |
| --- | --- | --- |
| `MergeRequest` | one agent as source and target (`SelfMerge`) | — |
| `MergeRecord` | a self-merge (`InvalidMergeRecord::SelfMerge`); a reversal dated before the merge (`Reversal(BeforeMerge)`) or restoring an agent its merge did not repoint, out of order or twice (`Reversal(NotRepointed)`); a second reversal, which needs a repeated `reverted` key (`duplicate field`) | — |
| `MergeVeto` | an agent paired with itself (`SelfMerge`) | the pair is ordered, lower id in `a` |
| `ClaimSet` | a claim listed twice, at the same or another time (`DuplicateClaim`) | entries are ordered latest first, ties by family, User-Agent, version |
| `ContentMatch` | origin agent is the reader (`SelfMatch`); more matched bytes than the read range (`ExceedsReadRange`); zero matched bytes | — |

`MergeRecord` decodes in the order the log builds it: `MergeRequest::new`,
`MergeRecord::new`, then `MergeRecord::revert` with the stored reversal,
instead of setting the reversal field by field. `revert` refuses a
reversal dated before the merge and one whose `restored` is not a
subsequence of the record's `repointed` (`InvalidReversal`), in memory and
on decode alike (`reconstruct.merge-record.reversal-within-merge`).
`Agent`, `AgentState` and `MergedInto` are plain: whether a merged agent's
target is canonical needs the merge table, which a value cannot see.

## Ingest events in envelopes

Each `IngestEvent` variant has a golden holding a whole `Envelope`
(`envelope_<variant>.json`), so the bus framing (`id`, `at`,
`{"type": "ingest", "data": {"type": <variant>, "data": ..}}`) is pinned
with each payload; a rename is pinned with a label and cleared. The golden
names come from an exhaustive match, so a new variant does not compile
until it has a golden.

## Renamed for the wire

`CredentialScheme::OAuthAccessToken` became `OauthAccessToken` (the Rust
guideline writes a contraction as one word), so it encodes as
`"oauth_access_token"` rather than `"o_auth_access_token"`. `OpenAi…`
names still encode as `open_ai…`.

## Files

| File | Role |
| --- | --- |
| `spec/types/observed/exchange.rs` | `ConnectionId`'s hand-written ULID-text serde |
| `spec/types/observed/agent/merge.rs` | `MergeRecord` decoded through `MergeRequest::new`, `new` and `revert`; `InvalidMergeRecord`, `InvalidReversal` |
| `spec/types/tests/wire/observed/{mod,exchange,identity,ingest}.rs` | Goldens and rejections for exchanges and client context, identity and the merge log, and ingest events in envelopes |
| `spec/types/tests/wire/provenance.rs` | Goldens and rejections for span locations, relay sources and content matches |
| `spec/types/tests/golden/observed/`, `golden/provenance/` | 40 and 7 goldens |

## Invariants

`reconstruct.merge-record.revert-once`,
`reconstruct.merge-record.reversal-within-merge`,
`reconstruct.merge-veto.distinct-pair` and
`reconstruct.claims.distinct-ordered` take the area's decode tests as
evidence, as do the general wire invariants (including
`canonical.wire.id-encoding` for `ConnectionId`).
