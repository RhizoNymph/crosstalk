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

1. **L0 ingress.** The `UpstreamRouter` resolves the upstream: by route in
   reverse-proxy mode, or by intercepted host in forward-proxy mode (other
   hosts are tunnelled untouched). The `ClientIdentifier` hashes the
   credential (`CredentialRef`), the account (`AccountHash`) and reads the
   harness headers (`HarnessClaim`, `HarnessIds`, `RequestClass`) into a
   `ClientContext`. A `ProviderAdapter` classifies the endpoint
   (`EndpointKind`): only `Generation` is captured. It decodes a `WireRequest`
   and supplies a `ResponseFramer` (HTTP, SSE) or a `WebSocketTap` (one per
   connection, one exchange per turn). `FrameEvent`s drive the in-flight
   `ExchangeStage`. Each finished exchange becomes a `RawExchange` on an
   in-process channel.
2. **L1 canonicalization.** A `Normalizer` for the exchange's
   `WireProtocol`, handling its `Dialect`, turns a `RawExchange` into a
   `NormalizedExchange`: an `Exchange` that references messages by
   `MessageHash`, plus the `Message`s and any `NormalizeWarning`s. Tool
   arguments are canonical JSON so echoed messages hash the same; unknown
   blocks are kept as `Unknown` parts. Bodies go to the `BlobStore` first,
   then `IngestEvent::ExchangeCaptured` is published.
3. **L2 transport.** Every event is an `Envelope { id, at, BusEvent }`.
   Consumers subscribe by `Subject` within a `ConsumerGroup` and ack or
   nack each `Delivery`.
4. **L3 reconstruction.** The `IdentityResolver` gives a `Resolution`
   (known agent, new agent, or conflict) from the most specific
   `IdentityEvidence`, with harness ids scoped by `IdentityScope`; it also
   applies `MergeRequest`s, and the `AgentDirectory` resolves merged ids.
   The `Threader` resolves `Continuation::Increment` exchanges through the
   stored response chain and gives a `ThreadOutcome` holding a
   `ConversationDelta` (new inputs, new system prompt, output), which is
   published.
5. **L4 provenance.** The `Segmenter` cuts the delta's output into
   `SpanDraft`s classified by `Origin`. Originated spans are fingerprinted
   and inserted into the `FingerprintIndex` (which accepts only an
   `OriginatedSpan`). New inputs and the output are run through the
   `Decoder`s, fingerprinted and looked up, with an optional
   `SemanticMatcher` for paraphrase. Hits on another agent's span become a
   `ContentMatch` (`DetectEvent::ContentMatched`); a hit in the output that
   no visible input explains has carrier `ReaderOutput`.
6. **L5 flow.** `ResourceExtractor`s turn tool calls and results into
   `ExtractedAccess`es. The `ChannelRegistry` maps each `Locator` to a known,
   declared or new channel, and the access is stored as an `Access`. The
   `Correlator` turns accesses, content matches and clock ticks into
   `TransmissionUpdate`s, which move a `Transmission` through
   `TransmissionState`, choosing its `Route` (`Delegation`, `Channel`,
   `Direct`, `Unobserved`, in that precedence). Channel and transmission
   events are published.
7. **L6 analysis.** For each `TransmissionConfirmed`, the `Embedder` and
   `TopicModel` produce a versioned `Classification`
   (`TransmissionClassified`); a re-fit re-classifies everything and then
   publishes `TopicVersionReady`. `AlertRuleEval`s turn envelopes into
   `AlertDraft`s, which `AlertTriage` opens or deduplicates
   (`TriageOutcome`), and suppresses on sanctioning or rule disabling.
8. **L7 topology.** The `EdgeStore` applies each `EdgeContribution` to its
   `EdgeKey` bucket (per topic-model version), activates a version once it is
   complete, and answers `TopologyGraph` queries over canonical agents with
   per-edge `Share`s.
9. **L8 surface.** `QueryApi` serves channels, alerts, the topology,
   search, transmissions, topics and projections to an authenticated
   `Caller` with `Permission`s. `OperatorActions` publish `PolicyChanged`
   (stamping author and time from the caller) and agent merges back down the
   stack, and `AlertSink`s deliver alerts.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `spec/Cargo.toml` | Builds the spec as a library so it type-checks and its tests run | crate `crosstalk-spec` |
| `spec/types/mod.rs` | Crate root, tier overview | — |
| `spec/types/ids.rs` | Typed ids | `AgentId`, `ExchangeId`, `SpanId`, `ChannelId`, `TransmissionId`, … `MessageHash`, `PromptHash`, `CredentialHash`, `AccountHash` |
| `spec/types/support.rs` | Shared building blocks | `NonEmpty`, `Timestamp`, `TimeWindow`, `ByteRange`, `Blake3`, `Similarity`, `Share` |
| `spec/types/observed/client.rs` | Ingress, upstream, credential and harness facts | `IngressMode`, `Upstream`, `UpstreamKind`, `Dialect`, `CredentialScheme`, `CredentialRef`, `HarnessClaim`, `HarnessIds`, `RequestClass`, `ClientContext`, `EndpointKind` |
| `spec/types/observed/message.rs` | Canonical messages | `Message`, `MessageBody`, `Role`, `AssistantPart`, `UserPart`, `ToolCall`, `ToolArguments`, `CanonicalJson`, `ToolResult`, `Unknown`, `PartRef` |
| `spec/types/observed/exchange.rs` | Exchanges and their pipeline stage | `Exchange`, `ExchangeMeta`, `WireProtocol`, `Transport`, `Continuation`, `ResponseId`, `ExchangeOutcome`, `ExchangeFailure`, `ExchangeStage` |
| `spec/types/observed/agent.rs` | Agent identity | `Agent`, `IdentityEvidence`, `IdentityScope`, `Strength`, `AgentState`, `MergeRequest`, `MergeAuthor` |
| `spec/types/observed/conversation.rs` | Threaded conversations | `Conversation`, `ConversationOrigin` |
| `spec/types/derived/provenance/span.rs` | Spans and their lifecycle | `Span`, `SpanLocation`, `Origin`, `RelaySource`, `SpanState`, `SpanEvent`, `OriginatedSpan` |
| `spec/types/derived/provenance/fingerprint.rs` | Fingerprints and index hits | `Fingerprint`, `WinnowParams`, `PositionedFingerprint`, `FingerprintHit` |
| `spec/types/derived/provenance/matching.rs` | Content matches | `ContentMatch`, `MatchKind`, `Codec`, `Carrier`, `InvalidMatch` |
| `spec/types/derived/flow/resource.rs` | Resources and patterns | `Resource`, `Locator`, `ResourcePattern`, `Host` |
| `spec/types/derived/flow/access.rs` | Accesses | `Access`, `AccessOp`, `AccessKind`, `Extraction` |
| `spec/types/derived/flow/evidence.rs` | Communication evidence | `Evidence`, `CoAccess`, `InvalidCoAccess` |
| `spec/types/derived/flow/transmission.rs` | Transmissions and their lifecycle | `Transmission`, `Route`, `DelegationDirection`, `DirectCarrier`, `TransmissionState`, `Confirmed`, `Classification` |
| `spec/types/derived/flow/channel/mod.rs` | Channels | `Channel`, `ChannelOrigin` |
| `spec/types/derived/flow/channel/detection.rs` | Channel detection lifecycle | `DeclaredDetection`, `TrafficDetection` |
| `spec/types/derived/flow/channel/policy.rs` | Channel policy and traffic routing | `Policy`, `Decision`, `PolicyAuthor`, `TrafficVerdict` |
| `spec/types/aggregates/edge.rs` | Topology edges | `EdgeKey`, `TopicSlot`, `EdgeStats`, `Edge`, `Weighting`, `RouteKind`, `TopologyFilter`, `TopologyGraph` |
| `spec/types/aggregates/topic.rs` | Embeddings and topics | `Embedding`, `EmbeddingModel`, `Topic`, `TopicModelVersion`, `TopicAssignment`, `Assignment` |
| `spec/types/aggregates/alert.rs` | Alert rules and alerts | `AlertRule`, `AlertRuleKind`, `AlertRuleDef`, `RuleStatus`, `AlertDraft`, `TriageOutcome`, `Alert`, `AlertState` |
| `spec/types/events/mod.rs` | Bus envelope and subjects | `Envelope`, `BusEvent`, `Subject` |
| `spec/types/events/{ingest,detect,insight}.rs` | Events by producing layer | `IngestEvent`, `ConversationDelta`, `DetectEvent`, `InsightEvent` |
| `spec/types/interfaces/l0_ingress.rs` … `l8_surface.rs` | One module per layer | the traits listed in the data flow above, and their error enums |
| `spec/types/tests/` | Invariant tests | — |
| `docs/research/harness-wire-protocols.md` | What each supported harness and server sends, with sources | — |

## Invariants and constraints

- The proxy forwards requests and responses unchanged and never refreshes,
  mints, rewrites or strips credentials. Raw credentials are hashed with a
  keyed BLAKE3 and never stored.
- Only `EndpointKind::Generation` requests produce exchanges.
- Forward-proxy mode intercepts TLS only for hosts on an
  `InterceptAllowlist`, which can never contain a vendor auth host. An
  unrouted reverse-proxy request is answered locally with 421.
- Credential and account digests are keyed BLAKE3 tagged with the
  `SecretVersion` that computed them; rotation overlaps two versions.
- A `RetryPolicy` has a non-zero initial backoff no greater than its
  maximum; exhausted deliveries are dead-lettered, never redelivered on
  their own.
- Identity scope is the account, else a stable credential, else the
  upstream; a session id resolves only to the session's main agent.
  Response-id lookups for increments stay within upstream and scope.
- A transmission opened and confirmed in one step has a `NonChannelRoute`.
- `TransmissionClassified` carries its `ClassificationCause`; content alert
  rules evaluate only first confirmations, so re-fits never re-raise
  alerts.
- Harness headers are claims: session and agent ids count as identity
  evidence only within their `IdentityScope`, and the harness name never
  does. Rotating credentials and prompt fingerprints never establish an
  agent alone.
- Merges are aliases resolved at read time; a merge request is never a
  self-merge.
- Tool arguments are canonical JSON, so an echoed message hashes like the
  original.

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
- A `ContentMatch` never has the same agent as origin and reader, and its
  non-zero `matched_bytes` never exceeds its read range
  (`ContentMatch::new`).
- A span's state changes only through `SpanState::advance`; only an
  `OriginatedSpan` can be indexed.
- A `Confirmed` transmission has at least one content match, and all its
  matches share one sender and one reader (`Confirmed::new`). The sender is
  known only from that state on.
- A `CoAccess` joins a write and a later read of one resource by two
  different agents within the window (`CoAccess::new`).
- A transmission is one (reader exchange, sender, route); later matches
  extend it. Route precedence is Delegation, Channel, Direct, Unobserved.
- No `EdgeKey` is a self-edge (`EdgeKey::new`); every `Embedding` has its
  model's dimension and unit norm (`Embedding::new`).
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
