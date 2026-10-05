# Handoff: conversation reads for the operator UI

For `crosstalk-impl`, owner of the spec (`spec/types/`, `spec/invariants/`)
on `integration/impl` / `staging`. Non-blocking: schedule it after the
channel-semantics port, the surface service and the follow-mode P0s. The UI
feature it serves is designed in `docs/features/conversation_view.md` on
branch `feat/ui-conversation-view`; no UI code is written against it until
this lands.

Written against `integration/impl` at `1da2c3b` (`spec/types/observed/`,
`interfaces/l3_reconstruction*`, `interfaces/l4_provenance.rs`,
`derived/provenance/`, `interfaces/l5_flow/transmissions.rs`,
`interfaces/l8_surface*`, `paging.rs`). `feat/reconstruct` (P4.1) and
`feat/provenance` (P4.2) were not pushed when this was written, so the
sections marked **depends on P4.1/P4.2** say what they need from those
branches rather than assume their shapes. Invariant numbers come from the
block crosstalk-impl assigned to this feature: **INV-1000..1029**.

**Revision 2** (after crosstalk-impl's reply) folds in the eval session's
spec PR `docs/spec-eval-gaps` (crosstalk-rollouts, INV-950..999). It was not
on origin when this was revised, so its shapes are taken from the relayed
description:
- L4: `ProvenanceReads` extends that PR's `SpanIndex::span` and defines no
  second span lookup.
- L5: `TransmissionStore::holding` sits beside its `AccessStore::access` and
  relies on its evidence read covering `Suspected` and `Discarded`.
- L1: `ExchangeReads` becomes the spec's exchange store and read, replacing
  today's JSONL stopgap.
- Replayed traffic (`IngressMode::Replay { corpus }`) is labelled and
  filterable.

P4.1 (ordered messages across roles) and P4.2 (persisted scan status, match
indexes by reader and by origin) were passed to their agents. The sections
that depend on them stay marked until crosstalk-impl reconciles them.

**Revision 3** (accepted in principle by crosstalk-impl; scheduled after
the e2e detection proof) aligns with what landed on `origin/staging`
(79dd38b):
- **L3:** `crates/reconstruct` keeps every conversation message in order
  under an ordinal, system turns included (`ConversationStore::transcript`,
  `TranscriptEntry`). `ConversationReads` is now the spec lift of that store
  with a paged turn read. The P4.1 ask (ordered messages across roles) is
  met by the transcript, so the proposed `ConversationDelta::new_messages`
  is withdrawn and the delta stays as it is.
- **L4:** `SpanIndex::spans(&IdBatch<SpanId>) -> BTreeMap<SpanId,
  IndexedSpan>`, where `IndexedSpan { exchange, author, location }` and the
  author is as recorded; `record(&OriginatedSpan)` writes it.
  `ProvenanceReads` extends it. Per-exchange and per-message scan status and
  the match indexes by reader message and by origin span come with the
  provenance merge, and the method names below defer to its final shape.
- **L5:** `AccessStore::accesses(&IdBatch<AccessId>)` in
  `interfaces::l5_flow::channels`.
- **Replay:** `observed::client::CorpusId` (a `String` newtype) in
  `IngressMode::Replay { corpus }`.
- **L1:** `ExchangeReads` and `ExchangeStore` are still not in the spec, so
  this proposal of them stands.

## Why

The operator UI wants a view of one agent's conversation: its exchanges in
order, what each turn read and wrote, and where text came from and went to
(this tool result holds text agent B originated, and here is that
transmission; this output was later read by agents C and D; a sub-agent was
delegated to here; harness claims; the conversation's origin; compaction
boundaries; WebSocket continuation increments). Investigators use it to
follow a thread across agents.

The observed model already has everything this needs at rest:
`Conversation` and `ConversationOrigin` (`observed/conversation.rs`), the
L3 `Threader` and `ConversationDelta` (`events/ingest.rs`), L4 spans with
`Origin::{Originated, Relayed(RelaySource), Common}` and `ContentMatch`
(`derived/provenance/`), and L5 transmissions whose identity is (reader
exchange, sender, route). What is missing is the **read side**: `QueryApi`
has no conversation method, and no store trait reads conversations,
exchanges, spans or content matches back. The evidence page assumes "L4's
span records" but no trait names them.

## Summary of the proposal

| Where | Addition |
| --- | --- |
| `interfaces/l8_surface/conversation.rs` (new), `conversation/turn.rs`, `conversation/text.rs` | Read models: `ConversationRow`, `ConversationHead`, `OriginLink`, `Successor`, `TurnPage`, `Turn`, `TurnMessage`, `PartShape`, marks (`Inbound`, `OutputSpan`, `Reader`), `ConversationText`, `PartText`; request types `ConversationFilter`, `TurnWindow`, `TextLimit`, `TextSlice` |
| `interfaces/l8_surface.rs` | Seven `QueryApi` methods (below): five View, two Content |
| `interfaces/l8_surface/http/routes.rs` | Seven routes |
| `interfaces/l8_surface/errors.rs` | `InputError::TextLimitTooLarge { max, got }`; `TooManyIds` reused |
| `interfaces/l8_surface/query_errors.rs` | `From<ConversationReadError>`, `From<ProvenanceReadError>`, `From<ExchangeReadError>`, `From<TextError>` for `QueryError` |
| `paging.rs` | Markers `ConversationList` (key `ConversationId`), `SpanReaderList` (key (`ExchangeId`, `SpanId`)), `ExchangeList` (key (`ExchangeMeta::started_at`, `ExchangeId`)) |
| `interfaces/l1_canonical.rs` | `ExchangeReads`: the spec's L1 exchange store and read (records by id, paged by conversation or by time), replacing the JSONL stopgap |
| `interfaces/l3_reconstruction/conversations.rs` (new) | `ConversationReads`: the spec lift of `crates/reconstruct`'s `ConversationStore` (`conversation`, `transcript`), adding list, successors, a paged turn read and locate |
| `interfaces/l4_provenance/reads.rs` (new) | `ProvenanceReads: SpanIndex`: extends `SpanIndex::spans` (batch `IndexedSpan`s) with output spans of an exchange, matches by reader and by origin, and scan status, as the provenance merge shapes them |
| `interfaces/l5_flow/transmissions.rs` | `TransmissionStore::holding(matches)` lookup; co-access details come from `AccessStore::accesses` (`l5_flow::channels`), not redefined |
| `observed/message.rs` | `Serialize`/`Deserialize` (snake_case strings) on `Role`, `MediaKind`, `ToolExecution`, `ToolOutcome` |
| `spec/invariants/` | INV-1000..1029 |

## QueryApi methods

Every method checks the permission first and returns `Forbidden` without
reading anything when it is missing, as every `QueryApi` method does. None
is `Watermarked`: conversations and spans are L3 and L4 state, which L7's
watermark does not settle. Provenance completeness is reported per turn
instead (`ProvenanceStatus`).

```rust
pub trait QueryApi {
    // ...existing methods...

    /// View. A page of the conversations `filter` admits, newest first
    /// (`ConversationId` descending). With `filter.agent`, the conversations
    /// whose stored agent resolves to the same canonical agent as
    /// `filter.agent` (an alias's conversations included, so a merge shows
    /// them together and an unmerge splits them on the next read).
    fn conversations(
        &self,
        caller: &Caller,
        filter: &ConversationFilter,
        page: &PageRequest<ConversationList>,
    ) -> impl Future<Output = Result<Page<ConversationRow, ConversationList>, QueryError>> + Send;

    /// View. The head of the conversation page: the row, its origin with
    /// links resolved, its successors (forks and compactions of it), the
    /// delegation that started it, and the claims seen on its exchanges.
    /// `None` for an unknown id.
    fn conversation(
        &self,
        caller: &Caller,
        id: ConversationId,
    ) -> impl Future<Output = Result<Option<ConversationHead>, QueryError>> + Send;

    /// View. The turns `window` names, in threading order, with their
    /// structure and provenance marks and no message text. `None` for an
    /// unknown conversation. A window starting at or past the last turn is
    /// an empty page carrying `total`, not an error.
    fn conversation_turns(
        &self,
        caller: &Caller,
        id: ConversationId,
        window: &TurnWindow,
    ) -> impl Future<Output = Result<Option<TurnPage>, QueryError>> + Send;

    /// View. Every reader of one originated span, newest reader exchange
    /// first: the rest of a `ReadBy` that did not fit inline. `None` for an
    /// unknown span.
    fn span_readers(
        &self,
        caller: &Caller,
        span: SpanId,
        page: &PageRequest<SpanReaderList>,
    ) -> impl Future<Output = Result<Option<Page<Reader, SpanReaderList>>, QueryError>> + Send;

    /// View. For each exchange of `ids` that has been threaded, keyed by
    /// that id, the conversation and turn it is. Unthreaded and unknown
    /// ids are left out. Bounded as `agent_names` is.
    fn exchange_turns(
        &self,
        caller: &Caller,
        ids: &IdBatch<ExchangeId>,
    ) -> impl Future<Output = Result<BTreeMap<ExchangeId, TurnPoint>, QueryError>> + Send;

    /// View. For each span of `ids` L4 holds, keyed by that id, where it
    /// sits: its agent (canonical), exchange, and conversation turn when
    /// that exchange is threaded. Unknown ids are left out.
    fn span_points(
        &self,
        caller: &Caller,
        ids: &IdBatch<SpanId>,
    ) -> impl Future<Output = Result<BTreeMap<SpanId, SpanPoint>, QueryError>> + Send;

    /// Content. The text of the turns `window` names, aligned with
    /// `conversation_turns` for the same window: per turn the same
    /// messages, per message the same parts, in the same order. Each
    /// part's text is clipped to `limit` bytes. A body content retention
    /// dropped is `BodyText::BodyDropped` and the rest is returned. `None`
    /// for an unknown conversation. A limit over `TextLimit::MAX` is
    /// refused before the call as `InvalidInput(TextLimitTooLarge)`.
    fn conversation_text(
        &self,
        caller: &Caller,
        id: ConversationId,
        window: &TurnWindow,
        limit: TextLimit,
    ) -> impl Future<Output = Result<Option<ConversationText>, QueryError>> + Send;

    /// Content. A slice of one part's text (`Message::part_text`): the
    /// "show more" of a clipped part. `None` when the blob store has no
    /// such message; a part with no text, or a slice not on character
    /// boundaries, is `InvalidInput`.
    fn part_text(
        &self,
        caller: &Caller,
        part: PartRef,
        slice: TextSlice,
    ) -> impl Future<Output = Result<Option<PartText>, QueryError>> + Send;
}
```

### Routes

| Route | Method, path | Args | Permission |
| --- | --- | --- | --- |
| `Conversations` | `GET /conversations` | query `filter`, `page` | View |
| `Conversation` | `GET /conversations/{id}` | path `id` | View |
| `ConversationTurns` | `GET /conversations/{id}/turns` | path `id`, query `window` | View |
| `SpanReaders` | `GET /spans/{id}/readers` | path `id`, query `page` | View |
| `ExchangeTurns` | `POST /query/exchange-turns` | field `ids` | View |
| `SpanPoints` | `POST /query/span-points` | field `ids` | View |
| `ConversationText` | `GET /conversations/{id}/text` | path `id`, query `window`, `limit` | Content |
| `PartText` | `POST /query/part-text` | field `part`, `slice` | Content |

The batch reads are `POST` like `agent_names`/`channel_names` (an
`IdBatch` does not fit a query string).

## Read models

All in `interfaces/l8_surface/conversation*.rs`. Wire derives follow the
wire contract: structs `#[serde(rename_all = "snake_case",
deny_unknown_fields)]`, data enums adjacently tagged (`tag = "type",
content = "data"`), unit enums snake_case strings. Request types implement
`WireRequest`; responses do not. Checked types decode through their
constructors (`try_from = "Raw…"`).

### Requests

```rust
/// Which conversations `conversations` lists. `{"agent": "01J…", "origins": []}`.
/// Empty `origins` means every origin.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ConversationFilter {
    pub agent: Option<AgentId>,
    pub origins: Vec<OriginKind>,
    /// Default `Include`. `{"type": "include"}`, `{"type": "exclude"}`,
    /// `{"type": "only", "data": {"corpus": null}}` for every replayed
    /// conversation, or a corpus name for one corpus.
    pub replay: ReplayFilter,
}
impl WireRequest for ConversationFilter {}

/// Which conversations to keep by their `TrafficSource`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReplayFilter {
    #[default]
    Include,
    Exclude,
    Only { corpus: Option<CorpusId> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OriginKind { Root, Fork, Compaction }

/// A turn's position in its conversation: the index of its delta in
/// threading order, from 0. Dense and immutable once threaded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TurnIndex(pub u32);

/// Turns `from .. from + size`. `{"from": 0, "size": 20}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct TurnWindow {
    pub from: TurnIndex,
    pub size: PageSize,
}
impl WireRequest for TurnWindow {}

/// How many bytes of each part's text `conversation_text` returns:
/// `1..=MAX`. On the wire a number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct TextLimit(NonZeroU32);
impl TextLimit {
    pub const MAX: u32 = 65_536;
    pub const DEFAULT: Self = /* 8192 */;
    pub fn new(bytes: u32) -> Result<Self, InvalidTextLimit>;
}

/// Bytes `from .. from + limit` of one part's text.
/// `{"from": 8192, "limit": 8192}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct TextSlice {
    pub from: u32,
    pub limit: TextLimit,
}
impl WireRequest for TextSlice {}
```

**Why turns are addressed by index, not by cursor.** Every other list
pages with keyset cursors so a traversal is stable under concurrent
inserts. A conversation's turns are append-only with dense, immutable
indexes (a delta is only ever appended; `reconstruct.thread.rethread-idempotent`
keeps a redelivered exchange from adding a second turn), so an index range
is already stable, and unlike an authenticated cursor it is citeable: the
UI's URL carries `turn=37`, and the same link opens the same turn for
anyone. Turns read oldest first, which is also the reading order; the
"newest first" rule of `paging.rs` stays for lists whose sort key is an id.

### Conversation list and head

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ConversationRow {
    pub id: ConversationId,
    /// `AgentDirectory::canonical(Conversation::agent)`.
    pub agent: AgentId,
    pub origin: OriginLink,
    /// The first turn's `ExchangeMeta::started_at`.
    pub started_at: Timestamp,
    /// The last turn's `started_at`.
    pub last_turn_at: Timestamp,
    /// Turns threaded when read.
    pub turns: u32,
    pub traffic: ConversationTraffic,
    /// Live, or replayed from a dataset corpus: the first turn's
    /// `ClientContext::ingress`.
    pub source: TrafficSource,
}

/// Where a conversation's traffic came from. `CorpusId` is
/// `observed::client::CorpusId`, as in `IngressMode::Replay { corpus }`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case", deny_unknown_fields)]
pub enum TrafficSource {
    /// Through the gateway as reverse or forward proxy.
    Live,
    Replay { corpus: CorpusId },
}

/// Distinct transmissions, any state but `Discarded`, counted once each.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ConversationTraffic {
    /// Whose reader exchange is one of this conversation's turns.
    pub received: u32,
    /// Holding a content match whose origin span is in one of this
    /// conversation's outputs.
    pub sent: u32,
}

/// `ConversationOrigin` with its links resolved for display.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case", deny_unknown_fields)]
pub enum OriginLink {
    Root,
    Fork {
        parent: ConversationId,
        /// The parent's canonical agent.
        parent_agent: AgentId,
        shared_prefix: u32,
        /// The last turn of `parent` whose history lies wholly inside the
        /// shared prefix: where the branch leaves the parent. `None` when
        /// the prefix ends inside the parent's first turn.
        branch_turn: Option<TurnIndex>,
    },
    Compaction {
        predecessor: ConversationId,
        /// Messages of the first request whose hash is in the
        /// predecessor's stored history.
        carried_over: u32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ConversationHead {
    pub row: ConversationRow,
    /// Conversations whose origin names this one, oldest first: forks
    /// (with their branch turn here) and compactions.
    pub successors: Vec<Successor>,
    /// The delegation that started this conversation: a transmission
    /// routed `Delegation(ParentToChild)` whose reader exchange is one of
    /// its turns, earliest turn first. `None` when none is known.
    pub delegated_from: Option<DelegationLink>,
    /// `ClaimSet` of the `ClientContext::harness` claims on its turns.
    /// Claims, never identity.
    pub claims: ClaimSet,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Successor {
    pub conversation: ConversationId,
    pub agent: AgentId,
    pub kind: SuccessorKind,
    pub started_at: Timestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case", deny_unknown_fields)]
pub enum SuccessorKind {
    Fork { shared_prefix: u32, branch_turn: Option<TurnIndex> },
    Compaction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct DelegationLink {
    pub transmission: TransmissionId,
    /// The parent's span: its agent, exchange and turn.
    pub parent: SpanPoint,
    /// The child's turn that read it.
    pub child: TurnPoint,
}
```

### Turns

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct TurnPoint {
    pub conversation: ConversationId,
    pub turn: TurnIndex,
}

/// Where a span sits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct SpanPoint {
    pub span: SpanId,
    /// The span's author: the agent recorded when it was indexed
    /// (`IndexedSpan::author` from `SpanIndex::spans`), resolved through
    /// `AgentDirectory::canonical` at read time. Never stored resolved.
    pub agent: AgentId,
    pub exchange: ExchangeId,
    /// `None` while the exchange is not threaded.
    pub turn: Option<TurnPoint>,
    pub location: SpanLocation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct TurnPage {
    pub conversation: ConversationId,
    /// Turns threaded when read. `turns` covers
    /// `window.from .. min(window.from + window.size, total)`.
    pub total: u32,
    pub turns: Vec<Turn>,
}

/// One exchange of the conversation: what was new in its request, its
/// output, and where that text came from and went.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Turn {
    pub index: TurnIndex,
    pub exchange: ExchangeId,
    /// Canonical agent of the exchange's attributed agent (it can differ
    /// from the head's after a merge threaded an alias's exchange here).
    pub agent: AgentId,
    pub started_at: Timestamp,
    pub protocol: WireProtocol,
    pub transport: Transport,
    pub model: ModelName,
    /// `ClientContext::harness`: what the request said about itself.
    pub harness: Option<HarnessClaim>,
    /// `ClientContext::ingress`: how the exchange reached the gateway
    /// (reverse proxy route, forward proxy host, or replay corpus).
    pub ingress: IngressMode,
    pub continuation: TurnContinuation,
    pub outcome: TurnOutcome,
    /// The request's messages new to the conversation, in request order,
    /// any role: user turns, tool results, and every system message the
    /// transcript records for this exchange (a new or changed top-level
    /// prompt, or a system turn inside the history), where the request
    /// placed it. These are the exchange's transcript entries other than
    /// its output, by ordinal. A compaction's first turn also lists its
    /// carried-over messages, flagged.
    pub inputs: Vec<TurnMessage>,
    /// The response, or a failed exchange's partial response.
    pub output: Option<TurnMessage>,
    pub provenance: ProvenanceStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case", deny_unknown_fields)]
pub enum TurnContinuation {
    FullHistory,
    /// A WebSocket (or `previous_response_id`) turn that sent only its
    /// increment.
    Increment {
        connection: Option<ConnectionId>,
        history: IncrementHistory,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IncrementHistory {
    /// The previous response resolved to the turn before this one.
    Resolved,
    /// The gateway never saw the previous response (it went around the
    /// proxy, or under another scope): threaded as a new conversation
    /// holding only the increment.
    Unseen,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case", deny_unknown_fields)]
pub enum TurnOutcome {
    Completed { finished_at: Timestamp, stop: StopReason, usage: Option<TokenUsage> },
    Failed { failed_at: Timestamp, failure: ExchangeFailure },
}

/// Whether L4 has finished with this turn's delta. Inbound marks and output
/// spans are complete once `Scanned`; readers of its output spans keep
/// arriving afterwards.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProvenanceStatus {
    Pending,
    Scanned { at: Timestamp },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct TurnMessage {
    pub hash: MessageHash,
    pub role: Role,
    pub placement: Placement,
    /// One per part, in `PartRef::index` order.
    pub parts: Vec<PartShape>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Placement {
    /// New to the conversation in this turn.
    New,
    /// In a compaction's first request and in the predecessor's history.
    CarriedOver,
    /// The turn's output.
    Output,
}

/// One part's structure and marks. No text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct PartShape {
    pub index: u16,
    pub kind: PartKind,
    /// Length of `Message::part_text`, `None` for a part with no text.
    pub text_bytes: Option<u32>,
    /// Content matches read here, ordered by range start: on an input
    /// part, every match whose `read_at` is this part; on an output part,
    /// the `ReaderOutput` matches.
    pub inbound: Vec<Inbound>,
    /// On an output part: its non-`Common` spans, ordered by range start.
    pub spans: Vec<OutputSpan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case", deny_unknown_fields)]
pub enum PartKind {
    Text,
    Reasoning { visible: bool },
    ToolCall { call: ToolCallId, name: ToolName, execution: ToolExecution },
    ToolResult { call: ToolCallId, outcome: ToolOutcome },
    Media(MediaKind),
    /// The provider's block type of an unrecognized block.
    Unknown { kind: String },
}

/// Text another agent originated, found in this part.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Inbound {
    /// `ContentMatch::read_at().range`, in this part's text.
    pub range: ByteRange,
    pub matched_bytes: NonZeroU32,
    pub kind: MatchKind,
    pub carrier: Carrier,
    pub origin: SpanPoint,
    /// The stored transmission holding this match, if one does.
    pub transmission: Option<TransmissionMark>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct TransmissionMark {
    pub id: TransmissionId,
    /// `Route::resolved`: a delegation is `Delegation(direction)`.
    pub route: Route,
    pub state: TransmissionStateKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct OutputSpan {
    pub span: SpanId,
    pub range: ByteRange,
    pub origin: SpanOrigin,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case", deny_unknown_fields)]
pub enum SpanOrigin {
    /// This agent wrote it.
    Originated { status: OriginatedStatus, read_by: ReadBy },
    /// Copied from an earlier span (another agent's or its own) or an
    /// input that is not a span.
    Relayed(RelayedFrom),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case", deny_unknown_fields)]
pub enum OriginatedStatus {
    /// Classified, fingerprints not written yet.
    Pending,
    Indexed { at: Timestamp },
    Propagated { indexed_at: Timestamp, first_hit_at: Timestamp, hits: NonZeroU32 },
    /// Past retention: no later reader will be detected.
    Expired { at: Timestamp },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case", deny_unknown_fields)]
pub enum RelayedFrom {
    Span(SpanPoint),
    /// An input message that is not an indexed span (a fetched page). The
    /// UI looks for it among this conversation's turns.
    Input(MessageHash),
}

/// Readers of one originated span: up to `ReadBy::INLINE` of them, newest
/// reader exchange first, and how many there are. The rest are
/// `span_readers`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawReadBy")]
pub struct ReadBy {
    first: Vec<Reader>,
    total: u32,
}
impl ReadBy {
    pub const INLINE: usize = 8;
    /// Refuses more than `INLINE` readers, or more readers than `total`.
    pub fn new(first: Vec<Reader>, total: u32) -> Result<Self, InvalidReadBy>;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Reader {
    /// Canonical `ContentMatch::reader`.
    pub agent: AgentId,
    pub exchange: ExchangeId,
    pub turn: Option<TurnPoint>,
    pub read_at: SpanLocation,
    pub carrier: Carrier,
    pub kind: MatchKind,
    pub transmission: Option<TransmissionMark>,
}
```

**Delegation** needs no type of its own. L5's route precedence puts
`Delegation` first, so a sub-agent's task prompt read by the child is a
`Reader` whose transmission route is `Delegation(ParentToChild)` on the
parent's tool-call span, and the child's answer coming back is an
`Inbound` with `Delegation(ChildToParent)` on the parent's tool result. The
UI renders those as delegation markers; `ConversationHead::delegated_from`
gives the child's page its "spawned by" link.

### Text (Content)

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ConversationText {
    pub conversation: ConversationId,
    pub turns: Vec<TurnText>,
}

/// Aligned with `Turn`: `inputs` then `output`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct TurnText {
    pub index: TurnIndex,
    pub inputs: Vec<MessageText>,
    pub output: Option<MessageText>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct MessageText {
    pub hash: MessageHash,
    pub body: BodyText,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case", deny_unknown_fields)]
pub enum BodyText {
    /// One per part, in part order.
    Shown(Vec<Option<PartText>>),
    /// Content retention dropped the body (as `Excerpted::BodyDropped`).
    BodyDropped,
}

/// A slice of one part's text: bytes `from .. from + text.len()` of
/// `Message::part_text`, cut on character boundaries, so the marks' byte
/// ranges index it directly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawPartText")]
pub struct PartText {
    from: u32,
    text: String,
    /// The whole part text's length.
    part_len: u32,
}
impl PartText {
    /// `part[from..]` clipped to at most `limit` bytes at a character
    /// boundary (at least one character when one remains).
    pub fn cut(part: &str, from: u32, limit: TextLimit) -> Result<Self, TextError>;
    pub fn remaining(&self) -> u32; // part_len - from - text.len()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextError {
    /// The part has no text (`NoPartText`).
    Part(NoPartText),
    /// `from` is past the end or not on a character boundary.
    Slice { from: u32, part_len: u32 },
}
```

`TextError` from a request (`part_text`) is `InvalidInput`; from stored
records (`conversation_text` naming a part its own read model listed) it is
a fault and becomes `Store`, as `EvidenceError` does.

## What it reads, and what is missing

| Read model field | Source | Status |
| --- | --- | --- |
| Conversation list by canonical agent, origin, successors | L3 conversation table (`Conversation`, `ConversationOrigin`) and `AgentDirectory` | **Missing:** no read trait. New `ConversationReads` (below). Successors need an index on `origin.parent`/`origin.predecessor`. |
| Turn sequence, `TurnIndex` | L3: `ConversationStore::transcript` (every message by ordinal, each with its exchange) and the stored threading outcomes | **Partly there** (`crates/reconstruct`, not the spec). A turn is one of the conversation's own exchanges in transcript order; a fork's inherited entries (those carrying the parent's exchange) are its base, not turns. Missing: a read of one turn window without loading the whole transcript, which needs an index (conversation, turn index) → first ordinal; and the read in the spec. |
| `inputs` in request order across roles, `Placement::CarriedOver` | L3 transcript entries of the turn's exchange (`role`, `output`, ordinal order) | **Order: covered** by the transcript, mid-conversation system turns included. **Carried over:** not flagged in `TranscriptEntry`. Either the store flags it (preferred: L3 already decides it when threading the compaction) or the read computes it as the compaction's turn-0 entries whose hash is in the predecessor's history. |
| Exchange meta, continuation, outcome, harness claim, ingress | L1: the `Exchange` record (`ExchangeCaptured`) | **Missing:** exchanges only go to a stopgap JSONL log. New `ExchangeReads` (L1 store and read, Postgres behind it), shaped like `NormalizedExchange` without the bodies (they stay in the blob store). |
| Message bodies, part kinds, `text_bytes`, text | Blob store (`BlobStore::get`, `Message::part_text`) | Exists. Part shapes are computed from the body at read time (View reads bodies but returns no text). |
| `IncrementHistory` | L1 `Continuation` + L3 stored outcome | **Derivable:** an `Increment` exchange whose stored outcome is `Starts` had an unseen previous response (`reconstruct.thread.unknown-previous-starts`); otherwise `Resolved`. |
| Output spans and their state | L4 span records | **Partly there:** `SpanIndex` records originated spans only (relayed and common ones are absent). `spans_of(exchange)` needs every non-common span of an output with its current state, relayed ones and their `RelaySource` included, so L4 must keep relayed spans too (from `SpanRelayed`). **Depends on the provenance merge.** |
| `Inbound` marks | L4 `ContentMatched` by reader | **Landing with the provenance merge:** the match index by reader message. `matches_read_in` reads it for the turn's input and output messages. |
| `ReadBy`, `span_readers` | L4 `ContentMatched` by `origin` | **Landing with the provenance merge:** the match index by origin span. `readers(span, page)` needs a paged read and a count over it. |
| `TrafficSource`, `Turn::ingress`, `ReplayFilter` | `ClientContext::ingress` (`IngressMode::Replay { corpus: CorpusId }`, on staging) | Read from the exchange record. Filtering the list by source needs the conversation table to record its first turn's source (or join to L1). |
| `ProvenanceStatus` | L4: per-exchange scan status | **Landing with the provenance merge** (per exchange and per message). `Scanned { at }` is the per-exchange status; per-message status is not needed by the view. |
| `TransmissionMark` | L5: the transmission holding a content match | **Missing:** transmissions are read by id only. New `TransmissionStore::holding`. Co-access details stay with `AccessStore::accesses`. A mark links to the evidence page in every state, `Suspected` and `Discarded` included (`surface.evidence.every-state`). |
| `ConversationTraffic` | L5 + L4 + L3 joined | **Missing:** needs the two lookups above per conversation; a materialized `(conversation, direction, transmission)` table in the surface's read side, or computed per page with the indexes. Acceptable to ship it as two counts recomputed on read at first. |
| `SpanPoint` | `SpanIndex::spans` (`IndexedSpan { exchange, author, location }`) + `AgentDirectory` + L3 `locate` | **Covered.** The author is as recorded (`provenance.span-index.author-as-recorded`); the surface resolves it to the canonical agent at read time, so a later merge or unmerge shows on the next read. Only originated spans are recorded, which is all a `SpanPoint` ever names (inbound origins, relay sources and `/spans/{id}` links are originated spans). |
| Claims | `ClientContext::harness` per exchange; `ClaimSet` per conversation | Computed from exchange records; no new store. |

### New store traits

```rust
// interfaces/l1_canonical.rs: the spec's L1 exchange store (Postgres),
// replacing the JSONL stopgap. Written by capture in the transaction that
// publishes `ExchangeCaptured`.

/// One stored exchange: `NormalizedExchange` without the bodies, which live
/// in the blob store under the hashes `exchange` names. `exchange.meta.client`
/// is the full `ClientContext` (ingress, upstream, claims, harness ids).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct StoredExchange {
    pub exchange: Exchange,
    pub warnings: Vec<NormalizeWarning>,
    /// Set once L3 threads it (`ExchangeStage::Threaded`).
    pub threaded: Option<TurnPoint>,
}

/// `{"window": null, "conversation": null}`. With `conversation`, that
/// conversation's exchanges; with `window`, those started in it.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ExchangeFilter {
    pub window: Option<TimeWindow>,
    pub conversation: Option<ConversationId>,
}

pub trait ExchangeStore {
    /// Store a captured exchange. Idempotent by id.
    fn put(&mut self, exchange: StoredExchange)
        -> impl Future<Output = Result<(), ExchangeReadError>> + Send;
    /// Record where L3 threaded `id`.
    fn threaded(&mut self, id: ExchangeId, at: TurnPoint)
        -> impl Future<Output = Result<(), ExchangeReadError>> + Send;
}

pub trait ExchangeReads {
    /// The stored exchanges of `ids`; unknown ids left out.
    fn exchanges(
        &self,
        ids: &IdBatch<ExchangeId>,
    ) -> impl Future<Output = Result<BTreeMap<ExchangeId, StoredExchange>, ExchangeReadError>> + Send;

    /// The exchanges `filter` admits, newest first by
    /// (`started_at`, `ExchangeId`).
    fn list(
        &self,
        filter: &ExchangeFilter,
        page: &PageRequest<ExchangeList>,
    ) -> impl Future<Output = Result<Page<StoredExchange, ExchangeList>, ExchangeReadError>> + Send;
}
pub enum ExchangeReadError { Store { reason: String }, InvalidCursor }

// interfaces/l3_reconstruction/conversations.rs: the spec lift of
// crates/reconstruct's `ConversationStore` reads (`conversation`,
// `transcript`), which already keep every message by ordinal.

/// `crates/reconstruct`'s `TranscriptEntry`, lifted into the spec, plus
/// the carried-over flag.
pub struct TranscriptEntry {
    /// From 0, counting every message, system turns included.
    pub ordinal: u32,
    pub message: MessageHash,
    pub role: Role,
    /// The exchange that added it (the parent's, for a fork's inherited
    /// messages).
    pub exchange: ExchangeId,
    /// Its place in the non-system history; `None` for a system message.
    pub history_index: Option<u32>,
    pub output: bool,
    /// A compaction's turn 0: its hash is in the predecessor's history.
    pub carried_over: bool,
}

/// One of the conversation's own exchanges and its messages.
pub struct StoredTurn {
    pub index: TurnIndex,
    pub exchange: ExchangeId,
    /// The attributed agent, as recorded.
    pub agent: AgentId,
    /// The exchange's transcript entries, by ordinal: its new messages in
    /// request order (any role), then its output.
    pub entries: Vec<TranscriptEntry>,
    /// The stored threading outcome's kind (`Starts`, `Extends`, `Forks`,
    /// `Compacts`); with the exchange's `Continuation` it gives
    /// `IncrementHistory`.
    pub outcome: OutcomeKind,
}

pub trait ConversationReads {
    /// Conversations whose stored agent resolves to `agent`'s canonical
    /// agent (every conversation when `None`), `ConversationId` descending.
    fn list(
        &self,
        filter: &ConversationFilter,
        page: &PageRequest<ConversationList>,
    ) -> impl Future<Output = Result<Page<Conversation, ConversationList>, ConversationReadError>> + Send;

    fn conversation(
        &self,
        id: ConversationId,
    ) -> impl Future<Output = Result<Option<Conversation>, ConversationReadError>> + Send;

    /// Conversations whose origin names `id`, oldest first.
    fn successors(
        &self,
        id: ConversationId,
    ) -> impl Future<Output = Result<Vec<Conversation>, ConversationReadError>> + Send;

    /// How many turns `id` has, and the turns of `window` (contiguous),
    /// read without loading the whole transcript.
    fn turns(
        &self,
        id: ConversationId,
        window: &TurnWindow,
    ) -> impl Future<Output = Result<Option<(u32, Vec<StoredTurn>)>, ConversationReadError>> + Send;

    /// Where each threaded exchange of `ids` sits.
    fn locate(
        &self,
        ids: &IdBatch<ExchangeId>,
    ) -> impl Future<Output = Result<BTreeMap<ExchangeId, TurnPoint>, ConversationReadError>> + Send;
}
pub enum ConversationReadError { Store { reason: String }, InvalidCursor }

// interfaces/l4_provenance/reads.rs
// Extends `SpanIndex` (on staging): `spans(&IdBatch<SpanId>)` returns
// `IndexedSpan { exchange, author, location }` for originated spans, the
// author as recorded. No second span lookup: `span_points` is
// `SpanIndex::spans` plus `AgentDirectory` for the author. The methods
// below read the scan status and match indexes the provenance merge adds;
// their names and signatures defer to that merge's final shape.
pub trait ProvenanceReads: SpanIndex {
    /// Every non-common span of `exchange`'s output, relayed ones included,
    /// with its recorded author and current state.
    fn spans_of(&self, exchange: ExchangeId)
        -> impl Future<Output = Result<Vec<Span>, ProvenanceReadError>> + Send;
    /// Every content match read in `exchange`'s messages (the index by
    /// reader message, over the exchange's input and output messages).
    fn matches_read_in(&self, exchange: ExchangeId)
        -> impl Future<Output = Result<Vec<ContentMatch>, ProvenanceReadError>> + Send;
    /// Matches whose origin is `span`, newest reader exchange first, and
    /// their total.
    fn readers(&self, span: SpanId, page: &PageRequest<SpanReaderList>)
        -> impl Future<Output = Result<(u32, Page<ContentMatch, SpanReaderList>), ProvenanceReadError>> + Send;
    /// The per-exchange scan status: when L4 committed `exchange`'s delta,
    /// if it has.
    fn scanned(&self, exchange: ExchangeId)
        -> impl Future<Output = Result<Option<Timestamp>, ProvenanceReadError>> + Send;
}
pub enum ProvenanceReadError { Store { reason: String }, InvalidCursor }

// interfaces/l5_flow/transmissions.rs
// Co-access details come from `AccessStore::accesses(&IdBatch<AccessId>)`
// in `l5_flow::channels`, not redefined here.
pub trait TransmissionStore {
    // ...save, transmission...
    /// For each match of `matches` (by origin span and reader exchange) the
    /// transmission holding it, if any. Transmission identity is (reader
    /// exchange, sender, route), so at most one holds a given match.
    fn holding(&self, matches: &[(SpanId, ExchangeId)])
        -> impl Future<Output = Result<BTreeMap<(SpanId, ExchangeId), TransmissionId>, TransmissionStoreError>> + Send;
}
```

Every method is `Send` like the rest. `query_errors.rs` maps each
`Store` to `QueryError::Store` and each `InvalidCursor` to
`QueryError::InvalidCursor`.

### System messages mid-conversation

The view lists a turn's new messages of any role in the order the request
held them, because a harness can put a system message mid-conversation.
`ConversationDelta::new_system` carries only the request's first system
message, but `crates/reconstruct` keeps every message by ordinal, system
turns included, so the transcript gives the order and the delta stays as
it is. (Revision 1 asked for `ConversationDelta::new_messages`; that is
withdrawn.)

## Permissions

| Method | Permission | Why |
| --- | --- | --- |
| `conversations`, `conversation`, `conversation_turns`, `span_readers`, `exchange_turns`, `span_points` | View | Ids, counts, times, roles, part kinds, byte lengths and ranges, tool names and call ids, provider block type names, models, harness claims. No message text. Tool names already reach View through `DirectCarrier::ToolResult`; claims through agent rows. |
| `conversation_text`, `part_text` | Content | Message text, tool arguments included. |

`Permission::View`'s and `Permission::Content`'s doc lists gain the new
reads.

## Proposed invariants (INV-1000..1029)

Canonical ids in responses and permission-before-read are already
surface-wide rules and are not restated here.

Ids follow the `surface.conversation.*` pattern; `reconstruct.*` and
`provenance.*` ones belong to those layers.

| INV | Id | Statement |
| --- | --- | --- |
| 1000 | `surface.conversation.list-canonical` | With `filter.agent`, `conversations` lists a conversation iff `canonical(conversation.agent) == canonical(filter.agent)`, each once; rows name canonical agents. |
| 1001 | `surface.conversation.list-order` | `conversations` is `ConversationId` descending; a traversal returns every matching conversation existing throughout it exactly once (keyset, as `paging.rs`). |
| 1002 | `surface.conversation.list-origin-filter` | Non-empty `origins` keeps exactly the conversations whose origin kind is listed. |
| 1003 | `surface.conversation.turns-window` | `conversation_turns` returns turns with indexes exactly `from .. min(from + size, total)`, ascending and contiguous; `from >= total` is an empty page, not an error. |
| 1004 | `reconstruct.conversation.turn-index-stable` | Once threaded, a turn's index and exchange never change, and a redelivered exchange adds no turn. |
| 1005 | `surface.conversation.turns-rebuild-history` | Over turns `0 .. total`, concatenating each turn's non-system `inputs` (any placement) then `output` gives the stored history after the base (empty for `Root` and `Compaction`, the shared prefix for `Fork`); restates INV-152 and INV-390 on the read model. |
| 1006 | `surface.conversation.inputs-request-order` | A turn's `inputs` are in the order its request held them, across roles. |
| 1007 | `surface.conversation.inputs-are-transcript` | A turn's inputs, then its output, are exactly the transcript entries `ConversationStore::transcript` holds for the turn's exchange, in ordinal order, system ones included; no message appears in two turns of one conversation. |
| 1008 | `surface.conversation.carried-over` | `Placement::CarriedOver` appears only on a `Compaction` conversation's turn 0, exactly on the request's messages whose hash is in the predecessor's stored history; `OriginLink::Compaction::carried_over` counts them. |
| 1009 | `surface.conversation.output-is-response` | A turn's `output` is the exchange's response when `Completed`, its partial response when `Failed`, `None` otherwise; `Placement::Output` appears only there. |
| 1010 | `surface.conversation.origin-resolved` | `OriginLink` is the stored `ConversationOrigin` with its links resolved; a `Fork`'s `branch_turn` is the greatest parent turn whose cumulative history count is at most `shared_prefix`. |
| 1011 | `surface.conversation.successors-complete` | `successors` lists exactly the conversations whose origin names this one, oldest first. |
| 1012 | `surface.conversation.inbound-are-matches` | A turn's `Inbound` marks are exactly the content matches whose `reader_exchange` is the turn's exchange, each on the part and range of its `read_at` (output parts for `ReaderOutput`). |
| 1013 | `surface.conversation.inbound-transmission` | An `Inbound` (or `Reader`) carries `transmission` iff a stored transmission holds that match, and it is that transmission with its route resolved. |
| 1014 | `surface.conversation.output-spans` | An output part's `spans` are exactly L4's non-`Common` spans located in it, ordered by range start, with the origin and status of the span's current state. |
| 1015 | `surface.conversation.read-by` | An originated span's `ReadBy::total` is the number of matches whose origin is the span; `first` is the newest `min(total, INLINE)` of them; `span_readers` traverses all `total` exactly once. |
| 1016 | `surface.conversation.traffic-source` | A conversation's `source` is `Replay { corpus }` iff its first turn's `ClientContext::ingress` is `Replay { corpus }`; `ReplayFilter::Exclude` keeps exactly the `Live` ones, `Only { corpus }` exactly the replayed ones (of that corpus when named), `Include` all. |
| 1017 | `surface.conversation.view-no-text` | View conversation reads carry no message text: no part text, tool arguments, tool result text or media bytes. |
| 1018 | `surface.conversation.text-content` | `conversation_text` and `part_text` require Content and read nothing without it. |
| 1019 | `surface.conversation.text-aligns` | For one window, `conversation_text` returns the same turns, and per turn the same messages and parts in the same order, as `conversation_turns`. |
| 1020 | `surface.conversation.text-slice` | A `PartText` is bytes `from .. from + len` of `Message::part_text`, cut on character boundaries, at most the limit (at least one character when any remain), with the part's full length; mark ranges index the same bytes. |
| 1021 | `surface.conversation.body-dropped` | A body content retention dropped is `BodyDropped` and the rest of the read is returned. |
| 1022 | `surface.conversation.locate` | `exchange_turns` maps an exchange to `(c, i)` iff turn `i` of `c` is that exchange; `span_points` names the span's own exchange and that turn, and `AgentDirectory::canonical` of its recorded author as of the read. |
| 1023 | `provenance.scan.status-after-commit` | A turn is `Scanned` only once L4 has committed every span of its output and every match read in its exchange; until then marks may be partial. |
| 1024 | `canonical.exchange.store-read` | `ExchangeReads::exchanges` returns each stored exchange as `put` stored it, with `threaded` set iff L3 threaded it there; `list` is (`started_at`, `ExchangeId`) descending, a keyset traversal returns each admitted exchange once, and `conversation` admits exactly the exchanges whose `threaded` names that conversation. |
| 1025 | `surface.conversation.merge-split` | Merges and unmerges change no stored conversation, turn, span or match, and every agent id is stored as recorded (a span's author as recorded when it was indexed); conversation reads resolve each through `AgentDirectory` on every read, so the next read follows a merge or unmerge. |
| 1026 | `surface.conversation.claims-only` | Turn `harness` fields and the head's `claims` come only from `ClientContext::harness`; the head's `ClaimSet` is the union of its turns' claims at their start times. |
| 1027 | `surface.conversation.increment-unseen` | A turn is `Increment { history: Unseen }` iff its exchange is an increment whose previous response did not resolve; it is turn 0 of a `Root` conversation. |
| 1028 | `surface.conversation.delegated-from` | `delegated_from` is set iff some transmission routed `Delegation(ParentToChild)` has a reader exchange among the conversation's turns, and names the earliest such turn's. |
| 1029 | `surface.conversation.traffic-counts` | `ConversationTraffic::received` and `sent` count each non-`Discarded` transmission once, as `Inbound`/`Reader` marks across all turns name them. |

## Open questions (recommendations in bold)

0. **Replayed conversations: decided by the user.** They are shown,
   labelled "replayed: <corpus>" on the row, head and each turn, with a
   filter (`ReplayFilter`, default `Include`).

1. **Live updates: decided by the user, none in v1.** Conversations grow
   on every exchange, and `Changed` announces neither agent activity nor
   turns. No new `Changed` variant is added; the UI re-reads the head on
   demand.
   A `Changed::Conversation(id)` coalesced by the feed can follow if
   follow mode wants it.
2. **`ConversationTraffic` cost.** **Recommend computing it on read** at
   first; materialize later if the agent-conversations list gets slow.
3. **Common spans.** Boilerplate spans are left out of `spans`.
   **Recommend leaving them out**; a count can be added if investigators ask.
4. **Text of carried-over messages.** They are repeated on the compaction's
   first turn so the boundary shows what survived. **Decided by the user:
   kept, flagged, and shown folded**, since the summary alone hides what
   was kept.
