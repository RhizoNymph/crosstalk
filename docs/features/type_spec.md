# Type specification

## Scope

- Every entity in the gateway's data model, its lifecycle states and the
  data known in each state.
- The events that cross the bus between layers.
- The trait each layer of the abstraction stack exposes, and its errors.
- Tests for invariants enforced by checked constructors.

## Non-scope

- Implementations of any trait.
- Serialization formats, database schemas, wire encodings. Implementation
  crates add serde and sqlx on their copies of these types.
- Lifecycle simulation: `design/lifecycles/cascade.yaml`, outside the
  repository, models the same lifecycles for the stateviz simulator.

## Data and control flow

The types follow data through the stack:

1. **L0 ingress.** A `ProviderAdapter` matches the request (`RequestHead`),
   decodes a `WireRequest`, and supplies a `ResponseFramer`. The framer's
   `FrameProgress` drives the in-flight `ExchangeStage`
   (`Forwarded` → `Responding` → `Completed` | `Failed`). The finished
   exchange becomes a `RawExchange` on an in-process channel.
2. **L1 canonicalization.** A `Normalizer` turns a `RawExchange` into a
   `NormalizedExchange`: an `Exchange` that references messages by
   `MessageHash`, plus the `Message`s. Bodies go to the `BlobStore` first,
   then `IngestEvent::ExchangeCaptured` is published.
3. **L2 transport.** Every event is an `Envelope { id, at, BusEvent }`.
   Consumers subscribe by `Subject` within a `ConsumerGroup` and ack or
   nack each `Delivery`.
4. **L3 reconstruction.** The `IdentityResolver` gives a `Resolution`
   (known agent, new agent, or conflict); the `Threader` gives a
   `ThreadOutcome` holding a `ConversationDelta`, which is published.
5. **L4 provenance.** The `Segmenter` cuts the delta's output into
   `SpanDraft`s classified by `Origin`. Originated spans are fingerprinted
   and inserted into the `FingerprintIndex`. New inputs are run through the
   `Decoder`s, fingerprinted and looked up. Hits on another agent's span
   become a `ContentMatch` (`DetectEvent::ContentMatched`).
6. **L5 flow.** `ResourceExtractor`s turn tool calls and results into
   `ExtractedAccess`es. The `ChannelRegistry` maps each `Locator` to a known,
   declared or new channel, and the access is stored as an `Access`. The
   `Correlator` turns accesses, content matches and clock ticks into
   `TransmissionUpdate`s, which move a `Transmission` through
   `TransmissionState`. Channel and transmission events are published.
7. **L6 analysis.** For each `TransmissionConfirmed`, the `Embedder` and
   `TopicModel` produce a `Classification` (`TransmissionClassified`).
   `AlertRuleEval`s turn events into `AlertDraft`s, which `AlertTriage`
   opens or deduplicates (`TriageOutcome`).
8. **L7 topology.** The `EdgeStore` applies each `EdgeContribution` to its
   `EdgeKey` bucket and answers `TopologyGraph` queries with per-edge
   `Share`s.
9. **L8 surface.** `QueryApi` serves the topology, search, transmissions,
   topics and projections. `OperatorActions` publish `PolicyChanged` and
   agent merges back down the stack, and `AlertSink`s deliver alerts.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `spec/Cargo.toml` | Builds the spec as a library so it type-checks and its tests run | crate `crosstalk-spec` |
| `spec/types/mod.rs` | Crate root, tier overview | — |
| `spec/types/ids.rs` | Typed ids | `AgentId`, `ExchangeId`, `SpanId`, `ChannelId`, `TransmissionId`, … `MessageHash`, `KeyHash`, `PromptHash` |
| `spec/types/support.rs` | Shared building blocks | `NonEmpty`, `Timestamp`, `TimeWindow`, `ByteRange`, `Blake3`, `Similarity`, `Share` |
| `spec/types/observed/message.rs` | Canonical messages | `Message`, `MessageBody`, `Role`, `AssistantPart`, `UserPart`, `ToolCall`, `ToolResult`, `PartRef` |
| `spec/types/observed/exchange.rs` | Exchanges and their pipeline stage | `Exchange`, `ExchangeMeta`, `ExchangeOutcome`, `ExchangeStage`, `Provider` |
| `spec/types/observed/agent.rs` | Agent identity | `Agent`, `IdentityEvidence`, `AgentState`, `MergeAuthor` |
| `spec/types/observed/conversation.rs` | Threaded conversations | `Conversation`, `ConversationOrigin` |
| `spec/types/derived/provenance/span.rs` | Spans and their lifecycle | `Span`, `SpanLocation`, `Origin`, `RelaySource`, `SpanState` |
| `spec/types/derived/provenance/fingerprint.rs` | Fingerprints and index hits | `Fingerprint`, `WinnowParams`, `PositionedFingerprint`, `FingerprintHit` |
| `spec/types/derived/provenance/matching.rs` | Content matches | `ContentMatch`, `MatchKind`, `Codec`, `Carrier`, `SelfMatch` |
| `spec/types/derived/flow/resource.rs` | Resources and patterns | `Resource`, `Locator`, `ResourcePattern`, `Host` |
| `spec/types/derived/flow/access.rs` | Accesses | `Access`, `AccessOp`, `AccessKind`, `Extraction` |
| `spec/types/derived/flow/evidence.rs` | Communication evidence | `Evidence`, `CoAccess` |
| `spec/types/derived/flow/transmission.rs` | Transmissions and their lifecycle | `Transmission`, `Route`, `TransmissionState`, `Confirmed`, `Classification` |
| `spec/types/derived/flow/channel/mod.rs` | Channels | `Channel`, `ChannelOrigin` |
| `spec/types/derived/flow/channel/detection.rs` | Channel detection lifecycle | `DeclaredDetection`, `TrafficDetection` |
| `spec/types/derived/flow/channel/policy.rs` | Channel policy and traffic routing | `Policy`, `Decision`, `PolicyAuthor`, `TrafficVerdict` |
| `spec/types/aggregates/edge.rs` | Topology edges | `EdgeKey`, `EdgeStats`, `Edge`, `Weighting`, `TopologyFilter`, `TopologyGraph` |
| `spec/types/aggregates/topic.rs` | Embeddings and topics | `Embedding`, `Topic`, `TopicModelVersion`, `TopicAssignment`, `Assignment` |
| `spec/types/aggregates/alert.rs` | Alert rules and alerts | `AlertRule`, `AlertRuleKind`, `AlertDraft`, `TriageOutcome`, `Alert`, `AlertState` |
| `spec/types/events/mod.rs` | Bus envelope and subjects | `Envelope`, `BusEvent`, `Subject` |
| `spec/types/events/{ingest,detect,insight}.rs` | Events by producing layer | `IngestEvent`, `ConversationDelta`, `DetectEvent`, `InsightEvent` |
| `spec/types/interfaces/l0_ingress.rs` … `l8_surface.rs` | One module per layer | the traits listed in the data flow above, and their error enums |
| `spec/types/tests/` | Invariant tests | — |

## Invariants and constraints

- A message's role is its `MessageBody` variant: tool calls appear only in
  assistant messages, tool results only in tool messages. Normalizers split
  provider messages that mix roles.
- `MessageHash` is the BLAKE3 of a message's canonical encoding; messages are
  immutable after hashing. An `Exchange` references messages by hash only,
  and every referenced message is in the blob store before
  `ExchangeCaptured` is published.
- An agent always has at least one piece of identity evidence. A merged
  agent's target is never itself and never another merged agent.
- Only originated spans are fingerprinted and indexed. Common spans are
  never indexed.
- A `ContentMatch` never has the same agent as origin and reader
  (`ContentMatch::new` rejects it).
- A `Confirmed` transmission has at least one content match, and all its
  matches share one sender and one reader (`Confirmed::new`). The sender is
  known only from that state on.
- A `CoAccess` joins a write and a later read of one resource by two
  different agents.
- A declared channel's detection is `DeclaredDetection` and a discovered
  channel's is `TrafficDetection`, so a declared, never-used channel is
  representable and a discovered, never-accessed one is not.
- Confirmed traffic on a channel raises an alert unless its policy is
  sanctioned (`Policy::on_traffic`).
- Deduplication is a triage outcome, not an alert state.
- In a `TopologyGraph`, edge shares sum to 1 unless there are no edges.
- `TimeWindow` and `ByteRange` are never empty. `Similarity` and `Share` are
  never NaN or outside `0..=1`.
- Bus delivery is at least once. Consumers are idempotent on the envelope id
  and on entity ids.
- The spec has no dependencies; `cargo check` and `cargo test` on
  `spec/Cargo.toml` must stay clean.
