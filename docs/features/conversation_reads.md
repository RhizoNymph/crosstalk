# Conversation reads

The read side behind the operator UI's conversation view
([conversation_view.md](conversation_view.md)): one agent's conversations,
one conversation's head, its turns by citeable index window with their
structure and provenance marks, their text, the readers of one span, and
where a batch of exchanges or spans sits. Spec additions in L1, L3, L4, L5
and L8, implemented by the stores of those layers, the surface service, the
HTTP server and the client.

Designed in revision 3 of `docs/handoff/conversation-view-spec.md` (branch
`feat/ui-conversation-view`); this page records what landed, including
where it deviates from that design (see "Deviations").

## Scope

- L8 `QueryApi`: `conversations`, `conversation`, `conversation_turns`,
  `span_readers`, `exchange_turns`, `span_points` (View) and
  `conversation_text`, `part_text` (Content), their routes, request types,
  read models and error mappings.
- L1 exchange store (`ExchangeStore`, `ExchangeReads`): every captured
  exchange record without its bodies, by id and by time.
- L3 conversation reads (`ConversationReads`): the spec lift of the
  threading store's transcript, with the carried-over flag, a per-turn
  index (conversation, turn) → first ordinal, the conversation's traffic
  source and turn times, successors and branch turns.
- L4 provenance reads (`ProvenanceReads: SpanIndex`): scan status, output
  spans of every origin with their state (relayed and forwarded included),
  matches by reader exchange, and a span's readers, paged.
- L5 `TransmissionStore::holding`: the transmission holding each content
  match.
- Invariants INV-1000..1029.

## Non-scope

- The UI pages, fixture backend and links (phase B of
  [conversation_view.md](conversation_view.md), owned by the UI).
- Live updates: no `Changed` variant announces turns; the UI re-reads.
- Common (boilerplate) spans: never shown.
- Counting suspected transmissions (co-access evidence only) as a
  conversation's traffic: only transmissions holding content matches can
  be found from a turn.

## Spec surface

### Requests (wire, `WireRequest`)

| Type | Where | JSON |
| --- | --- | --- |
| `ConversationFilter { agent, origins, replay }` | `l8_surface::conversation` | `{"agent": null, "origins": [], "replay": {"type": "include"}}` |
| `ReplayFilter::{Include, Exclude, Only { corpus }}` | `l3_reconstruction::conversations` (re-exported) | `{"type": "only", "data": {"corpus": null}}` |
| `OriginKind::{Root, Fork, Compaction}` | `observed::conversation` | `"fork"` |
| `TurnWindow { from: TurnIndex, size: PageSize }` | `l3_reconstruction::conversations` | `{"from": 20, "size": 20}` |
| `TextLimit` (`1..=65_536`, default 8192) | `l8_surface::conversation::text` | `8192` |
| `TextSlice { from, limit }` | same | `{"from": 8192, "limit": 8192}` |
| `PartTextBody { part: PartRef, slice }` | `l8_surface::http::bodies` | body of `POST /query/part-text` |
| `IdBatch<ExchangeId>`, `IdBatch<SpanId>` | `batch` | bodies of the batch locates |
| `PageRequest<ConversationList>`, `PageRequest<SpanReaderList>` | `paging` | cursor pages |

### Methods and routes

| Method | Route | Permission | Returns |
| --- | --- | --- | --- |
| `conversations(filter, page)` | `GET /conversations?filter=&page=` | View | `Page<ConversationRow, ConversationList>` |
| `conversation(id)` | `GET /conversations/{id}` | View | `Option<ConversationHead>` |
| `conversation_turns(id, window)` | `GET /conversations/{id}/turns?window=` | View | `Option<TurnPage>` |
| `span_readers(span, page)` | `GET /spans/{id}/readers?page=` | View | `Option<Page<Reader, SpanReaderList>>` |
| `exchange_turns(ids)` | `POST /query/exchange-turns` (body: id array) | View | `BTreeMap<ExchangeId, TurnPoint>` |
| `span_points(ids)` | `POST /query/span-points` (body: id array) | View | `BTreeMap<SpanId, SpanPoint>` |
| `conversation_text(id, window, limit)` | `GET /conversations/{id}/text?window=&limit=` | Content | `Option<ConversationText>` |
| `part_text(part, slice)` | `POST /query/part-text` (`{"part", "slice"}`) | Content | `Option<PartText>` |

Query arguments are JSON in the query string, as for every route.
`None` answers 404 as elsewhere.

### Read models (`l8_surface::conversation`)

- `ConversationRow { id, agent, origin: OriginLink, started_at,
  last_turn_at, turns, source: TrafficSource }`.
- `OriginLink::{Root, Fork { parent, parent_agent, shared_prefix,
  branch_turn }, Compaction { predecessor, predecessor_agent,
  carried_over }}`.
- `ConversationHead { row, traffic: ConversationTraffic { received, sent },
  successors: Vec<Successor>, delegated_from: Option<DelegationLink>,
  claims: ClaimSet }`; `Successor { conversation, agent, kind:
  SuccessorKind::{Fork { shared_prefix, branch_turn }, Compaction},
  started_at }`; `DelegationLink { transmission, parent: SpanPoint, child:
  TurnPoint }`.
- `TurnPoint { conversation, turn: TurnIndex }`; `SpanPoint { span, agent,
  exchange, turn: Option<TurnPoint>, location: SpanLocation }`.
- `turn::TurnPage { conversation, total, turns: Vec<Turn> }`; `Turn {
  index, exchange, agent, started_at, protocol, transport, model, harness,
  ingress, continuation: TurnContinuation, outcome: TurnOutcome, inputs:
  Vec<TurnMessage>, output: Option<TurnMessage>, provenance: ScanStatus }`.
- `TurnContinuation::{FullHistory, Increment { connection, history:
  IncrementHistory::{Resolved, Unseen} }}`; `TurnOutcome::{Completed {
  finished_at, stop, usage }, Failed { failed_at, failure }}`.
- `TurnMessage { hash, role, placement: MessagePlacement::{New,
  CarriedOver, Output}, parts: MessageParts::{Shown(Vec<PartShape>),
  BodyDropped(Vec<PartMarks>)} }`; `PartShape { index, kind: PartKind,
  text_bytes, inbound: Vec<Inbound>, spans: Vec<OutputSpan> }`;
  `PartMarks { index, inbound, spans }`.
- `PartKind::{Text, Reasoning { visible }, ToolCall { call, name,
  execution }, ToolResult { call, outcome }, Media(MediaKind), Unknown {
  kind }}`.
- `Inbound { range, matched_bytes, kind: MatchKind, carrier: Carrier,
  origin: SpanPoint, transmission: Option<TransmissionMark> }`;
  `TransmissionMark { id, route (resolved), state: TransmissionStateKind }`.
- `OutputSpan { span, range, origin: SpanOrigin }`; `SpanOrigin::{
  Originated { status: OriginatedStatus, read_by: ReadBy }, Forwarded {
  input, status: ForwardStatus, read_by }, Relayed(RelayedFrom::{Span(
  SpanPoint), Input(MessageHash)}) }`.
- `ReadBy { first (≤ 8, newest first), total }` (checked); `Reader { agent,
  exchange, turn, read_at, carrier, kind, transmission }`.
- `text::ConversationText { conversation, turns: Vec<TurnText { index,
  inputs: Vec<MessageText>, output } > }`; `MessageText { hash, body:
  BodyText::{Shown(Vec<Option<PartText>>), BodyDropped} }`; `PartText {
  from, text, part_len }` (`cut`, `remaining`).
- `l4_provenance::reads::ScanStatus::{Pending, Scanned { at }, Indexed {
  at }, Failed { at, failure: ScanFailureKind }}`; `ForwardStatus::{Pending,
  Indexed { at }, Expired { indexed_at, at }}`.

Errors: `InputError::TextLimitOutOfRange { max, got }`,
`PartWithoutText { index }`, `SliceOutsideText { from, part_len }` (422);
`TooManyIds` for batches over `IdBatch::MAX`; `From` impls for
`ConversationReadError`, `ProvenanceReadError`, `ExchangeStoreError`,
`SpanIndexError`, `TextError` and `InvalidTextLimit`.

### Store traits

| Trait | Module | Methods |
| --- | --- | --- |
| `ExchangeStore` | `l1_canonical::exchanges` | `put(StoredExchange)` (idempotent by id, first kept) |
| `ExchangeReads` | same | `exchanges(&IdBatch<ExchangeId>)`, `list(&ExchangeQuery { window }, &PageRequest<ExchangeList>)` |
| `ConversationReads` | `l3_reconstruction::conversations` | `list(&ConversationQuery, page)`, `conversation(id)`, `successors(id)`, `turns(id, &TurnWindow) -> Option<TurnSlice>`, `locate(&IdBatch<ExchangeId>)`, `branch_turn(parent, shared_prefix)` |
| `ProvenanceReads: SpanIndex` | `l4_provenance::reads` | `output_spans(&IdBatch<ExchangeId>)`, `matches_read_in(&IdBatch<ExchangeId>)`, `readers(span, page) -> Option<ReaderPage>`, `scan_status(&IdBatch<ExchangeId>)` |
| `TransmissionStore::holding` | `l5_flow::transmissions` | `holding(&BTreeSet<MatchKey>) -> BTreeMap<MatchKey, TransmissionId>` |

Store-side values: `TranscriptEntry` (with `carried_over`),
`ThreadOutcomeKind` (`ThreadOutcome::kind`), `StoredTurn { index,
exchange, agent, started_at, outcome, history_end, entries }`,
`StoredConversation { conversation, source, started_at, last_turn_at,
turns }`, `ConversationQuery { agents, origins, replay }` (`admits`),
`TurnSlice { total, turns }`, `StoredSpan { span, forward }`,
`ReaderPage { total, page }`, `MatchKey { origin, reader_exchange,
read_at }`, `StoredExchange { exchange, warnings }`, `ExchangeQuery`.

Paging markers: `ConversationList` (`ConversationId` desc),
`SpanReaderList` (reader exchange start, match id; desc), `ExchangeList`
(`started_at`, `ExchangeId`; desc). Turns are addressed by index window,
not cursor: indexes are dense and immutable, so a window is stable and a
turn link citeable.

Serde added to `Role`, `MediaKind`, `ToolExecution`, `ToolOutcome`
(snake_case strings); `Ord` to `ByteRange`, `PartRef` and `SpanLocation`
(for `MatchKey`); `TrafficSource` (`IngressMode::source`) and
`OriginKind` (`ConversationOrigin::kind`, `::source`); path segments for
`ConversationId` and `SpanId`.

## Data and control flow

```text
conversations(filter, page)
  require View
  agents = AgentReads::cluster(filter.agent) → canonical + aliases (unknown → empty page)
  ConversationReads::list(ConversationQuery { agents, origins, replay }, page)
  per row: canonical agent; OriginLink (fork: parent's canonical agent and
           branch_turn; compaction: predecessor's agent, carried-over count
           from turn 0's entries)

conversation(id)
  require View
  ConversationReads::conversation(id) → row
  successors(id); every turn in windows of PageSize::MAX:
    ExchangeReads::exchanges → harness claims (ClaimSet::observe at started_at)
    ProvenanceReads::matches_read_in(turns) + output_spans(turns) → readers(span)
    TransmissionStore::holding(keys) → distinct received / sent ids
    delegated_from: earliest turn whose received transmission routes
                    Delegation(ParentToChild)

conversation_turns(id, window)
  require View
  ConversationReads::turns(id, window) → TurnSlice (entries by ordinal)
  ExchangeReads::exchanges(turn exchanges)          → header fields
  BlobStore::get(each message)                      → part shapes (no text)
  ProvenanceReads::{scan_status, matches_read_in, output_spans}(turn exchanges)
  ProvenanceReads::readers(span, first page of ReadBy::INLINE) per indexed span
  SpanIndex::spans(origins and relay sources) + ConversationReads::locate → SpanPoints
  TransmissionStore::holding(match keys) → transmission(id) → TransmissionMark
  AgentDirectory::canonical on every agent id

conversation_text(id, window, limit)
  require Content; same turns and entries; BlobStore::get; PartText::cut per part
part_text(part, slice)
  require Content; BlobStore::get(part.message); PartText::cut
exchange_turns(ids) → ConversationReads::locate
span_points(ids)    → SpanIndex::spans + locate + canonical
span_readers(span)  → ProvenanceReads::readers + locate + holding
```

Writes:

- Capture (L1) puts each exchange in the `ExchangeStore` before it
  publishes `ExchangeCaptured`.
- L3 threading records, in the same atomic step as the threading
  decision, each appended entry's `carried_over` flag and one turn row
  (conversation, turn, exchange, first ordinal, entry count, agent, start,
  outcome kind, history length after it); a new conversation records its
  traffic source and start.
- L4 already keeps every classified span (originated, relayed with its
  `RelaySource`, common) beside its matches and scan status; the reads
  serve them.

## Files

Spec (stage A):

| File | Role |
| --- | --- |
| `spec/types/interfaces/l1_canonical/exchanges.rs` | `StoredExchange`, `ExchangeQuery`, `ExchangeStore`, `ExchangeReads`, `ExchangeStoreError` |
| `spec/types/interfaces/l3_reconstruction/conversations.rs` | `TurnIndex`, `TurnPoint`, `TurnWindow`, `ReplayFilter`, `TranscriptEntry`, `ThreadOutcomeKind`, `StoredTurn`, `StoredConversation`, `ConversationQuery`, `TurnSlice`, `ConversationReads`, `ConversationReadError` |
| `spec/types/interfaces/l4_provenance/reads.rs` | `ScanStatus`, `ScanFailureKind`, `ForwardStatus`, `StoredSpan`, `ReaderPage`, `ProvenanceReads`, `ProvenanceReadError` |
| `spec/types/interfaces/l5_flow/transmissions.rs` | `MatchKey`, `TransmissionStore::holding` |
| `spec/types/interfaces/l8_surface/conversation.rs` | filter, row, head, origin, successors, delegation, points |
| `spec/types/interfaces/l8_surface/conversation/turn.rs` | turn page, turn, messages, parts, marks, `ReadBy`, `Reader` |
| `spec/types/interfaces/l8_surface/conversation/text.rs` | `TextLimit`, `TextSlice`, text models, `PartText::cut`, `TextError` |
| `spec/types/interfaces/l8_surface.rs` | the eight `QueryApi` methods |
| `spec/types/interfaces/l8_surface/http/{routes,bodies,path,status}.rs` | routes, `PartTextBody`, path ids, statuses |
| `spec/types/interfaces/l8_surface/{errors,query_errors,permissions}.rs` | input errors, `From` impls, permission docs |
| `spec/types/{paging,observed/*,support,derived/provenance/span}.rs` | markers, serde and ordering additions, `TrafficSource`, `OriginKind` |
| `spec/types/tests/conversation.rs` | checked types and mappings |
| `spec/types/tests/wire/surface_reads/conversation.rs` | goldens under `golden/surface_reads/conversation/` |
| `spec/types/tests/send/conversations.rs` | `Send` futures of the new traits |

Implementation (stage B) is listed in "Implementation" below.

## Invariants and constraints

INV-1000..1029 (`spec/invariants/INV-10[0-2]?-*.toml`):
list-canonical, list-order, list-filters, turns-window,
turn-index-stable, turns-rebuild-history, entries-request-order,
inputs-are-transcript, carried-over, output-is-response, origin-resolved,
successors-complete, inbound-are-matches, inbound-transmission,
output-spans, read-by, traffic-source, view-no-text, text-content,
text-aligns, text-slice, body-dropped, locate, scan status-after-commit,
exchange store-read, merge-split, claims-only, increment-unseen,
delegated-from, traffic-counts.

- Every stored agent id stays as attributed; reads resolve through
  `AgentDirectory` each time.
- View reads never carry message text; Content reads are refused before
  any store is read.
- A turn's entries are contiguous in its conversation's transcript; a
  fork's base belongs to no turn.
- Nothing in these reads is watermarked.

## Deviations from revision 3

1. `ConversationRow::traffic` moved to `ConversationHead::traffic`: counts
   need every turn's matches and holdings, too costly per list row.
2. `ConversationReads::list` takes a store-side `ConversationQuery` with
   the resolved cluster (`agents`), not the wire `ConversationFilter`; the
   cursor binds the cluster, so a merge between pages is `InvalidCursor`.
   `StoredConversation` carries `source`, `started_at`, `last_turn_at`
   and `turns` (L3 records them) so rows need no L1 read.
3. `StoredTurn` adds `started_at` and `history_end`; `ConversationReads`
   adds `branch_turn`; `TurnSlice` replaces the `(u32, Vec<StoredTurn>)`
   tuple; the outcome kind is `ThreadOutcomeKind`.
4. `ExchangeStore`/`ExchangeReads`: no `threaded` field or write (where L3
   threaded an exchange is `ConversationReads::locate`, so L1 stays
   independent of L3) and no `conversation` filter; one error type,
   `ExchangeStoreError`; the store input is `ExchangeQuery`, not on the
   wire.
5. `ProvenanceReads` methods are batched by `IdBatch<ExchangeId>` and
   named `output_spans`, `matches_read_in`, `readers`, `scan_status`
   (landed L4 records: per-exchange status with `Indexed` and `Failed`).
   The turn's status is that `ScanStatus` (rev. 3 `ProvenanceStatus` had
   only `Pending`/`Scanned`).
6. `SpanOrigin` gains `Forwarded { input, status, read_by }`: landed L4
   indexes spans relayed from an input under the forwarding agent
   (`provenance.index.forwarded-indexed`), so they have readers.
   `SpanIndex::spans` records originated and forwarded spans.
7. `TransmissionStore::holding` keys on `MatchKey` (origin span, reader
   exchange and read location), not `(SpanId, ExchangeId)`: two matches of
   one span in one reader exchange can belong to different transmissions
   (different routes).
8. `MessageParts::BodyDropped(Vec<PartMarks>)` instead of an empty part
   list: a dropped body keeps its marks.
9. Rev. 3 `Placement` is `MessagePlacement` (L3 already has `Placement`).
10. `OriginLink::Compaction` adds `predecessor_agent`.
11. `TextLimitTooLarge` is `TextLimitOutOfRange` (zero is refused too);
    `part_text` refusals are `PartWithoutText` and `SliceOutsideText`.
12. Batch locates take the id array as the whole body (`Arg::body`), as
    `agent_names` does, not a field `ids`.
13. `SpanReaderList`'s key is (reader exchange start, match id): a span can
    be matched several times in one reader exchange.
14. `ConversationTraffic` counts content-holding transmissions only.

## Implementation

Stage B (see the commit log of `feat/conversation-reads`).
