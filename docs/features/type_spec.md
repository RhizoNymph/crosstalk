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
   (`EndpointKind`): only `Generation` is captured. All of this reads only
   the request head, and the request is forwarded upstream as soon as it is
   routed. `decode_request` (gzip and zstd included) runs concurrently on a
   tee of the body, off the hot path, and yields a `WireRequest`. When the
   response head arrives, the adapter builds a `ResponseFramer` from the
   `ResponseHead` (its content type gives the `ResponseFraming`: SSE or a
   whole body) and its own protocol; a WebSocket connection gets a
   `WebSocketTap` instead (one exchange per turn). `FrameEvent`s drive the
   in-flight `ExchangeStage`. The `DecodedRequest` attaches to the exchange
   when it is ready, and the finished exchange becomes a `RawExchange` on an
   in-process channel. If decoding fails, the exchange was still forwarded
   and relayed, and is counted as uncaptured.
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
   published. A conversation's stored history is non-system messages only:
   each delta appends its new inputs and then its output. A compaction's
   history starts with its first request's non-system messages in request
   order (carried-over messages included) and that exchange's output, and
   its first delta's new inputs are those messages minus the ones whose hash
   is in the predecessor's history.
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
   events are published. Each policy decision, from config (a declaration
   or reload) or from `PolicyChanged`, becomes a `PolicyDecision` that
   `ChannelRegistry::set_policy` records in the channel's `PolicyHistory`
   (ordered by decision time, idempotent on redelivery) while setting the
   channel's policy to the history's current entry in the same transaction.
   A `PolicyChanged` carrying `Unreviewed(None)` holds no decision and is
   acked without effect.
7. **L6 analysis.** For each `TransmissionConfirmed`, the `Embedder` and
   `TopicModel` produce a versioned `Classification`
   (`TransmissionClassified`); a re-fit re-classifies everything and then
   publishes `TopicVersionReady`. `AlertRuleEval`s turn envelopes into
   `AlertDraft`s, which `AlertTriage` opens or deduplicates
   (`TriageOutcome`), and suppresses on sanctioning or rule disabling. Every
   stored change to an alert bumps its `AlertRevision` and publishes
   `AlertChanged`.
8. **L7 topology.** The `EdgeStore` applies each `EdgeContribution` to its
   `EdgeKey` bucket (per topic-model version), activates a version once it is
   complete (then publishes `TopicVersionActivated`), and answers
   `TopologyGraph` queries over canonical agents with per-edge `Share`s.
9. **L8 surface.** `QueryApi` serves channels, policy histories, alerts,
   the topology, search, transmissions, topics, projections and the audit
   log to an authenticated `Caller` with `Permission`s. `OperatorActions`
   check `OperatorAction::required_permission`, publish `PolicyChanged`
   (stamping author and time from the caller) and agent merges back down the
   stack, acknowledge and resolve alerts (publishing `AlertChanged`), and
   return an `ActionEffect`; `AlertSink`s deliver alerts.
   - **Audit log.** Every `act` call leaves one `AuditRecord` (the caller,
     the `OperatorAction` value, the time and an `AuditOutcome`: `Applied`,
     `Unchanged`, `Rejected(Rejection)` or `Forbidden`). `Applied` and
     `Unchanged` records are written in the action's transaction. The
     `AuditLog` is append-only; `QueryApi::audit` pages it newest first with
     an `AuditFilter` and needs `Permission::Audit`.
   - **Live feed.** A feed writer (consumer group `live`) turns `AlertOpened`,
     `AlertChanged`, `EdgeUpdated`, `ChannelDiscovered`,
     `ChannelCrossAccessed`, `DeclaredChannelUnused`, `PolicyChanged`,
     `TransmissionConfirmed` and `TopicVersionActivated` into `LiveUpdate`s,
     computes each one's `LiveScope`, and appends it to the feed log, which
     numbers entries per `FeedEpoch`. `LiveFeed::subscribe` takes the
     caller, an `UpdateKinds` set, a `TopologyFilter` and a `Resume` point
     (from `Last-Event-ID`); it refuses kinds whose permission the caller
     lacks (`Content` for `TransmissionConfirmed`, `View` otherwise).
     `FeedWindow::resume` decides between replaying from the cursor and a
     `LiveItem::Resync` (refetch everything). Each stream filters entries by
     kind and `LiveScope::admitted_by`, sends heartbeats carrying its newest
     cursor, and ends with `LiveEnd::Lagged` when its bounded buffer fills,
     so a slow client never blocks the feed or other clients.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `spec/Cargo.toml` | Builds the spec as a library so it type-checks and its tests run | crate `crosstalk-spec` |
| `spec/types/mod.rs` | Crate root, tier overview | — |
| `spec/types/ids.rs` | Typed ids | `AgentId`, `ExchangeId`, `SpanId`, `ChannelId`, `TransmissionId`, … `AuditId`, `MessageHash`, `PromptHash`, `CredentialHash`, `AccountHash` |
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
| `spec/types/derived/flow/channel/policy.rs` | Channel policy, its history and traffic routing | `Policy`, `Decision`, `PolicyAuthor`, `PolicyKind`, `PolicyDecision` (checked from `Policy`), `PolicyHistory` (checked), `Recorded`, `TrafficVerdict` |
| `spec/types/aggregates/edge.rs` | Topology edges | `EdgeKey`, `TopicSlot`, `EdgeStats`, `Edge`, `Weighting`, `RouteKind`, `TopologyFilter`, `TopologyGraph` |
| `spec/types/aggregates/topic.rs` | Embeddings and topics | `Embedding`, `EmbeddingModel`, `Topic`, `TopicModelVersion`, `TopicAssignment`, `Assignment` |
| `spec/types/aggregates/alert.rs` | Alert rules and alerts | `AlertRule`, `AlertRuleKind`, `AlertRuleDef`, `RuleStatus`, `AlertDraft`, `TriageOutcome`, `Alert`, `AlertState`, `AlertRevision` |
| `spec/types/events/mod.rs` | Bus envelope and subjects | `Envelope`, `BusEvent`, `Subject` |
| `spec/types/events/{ingest,detect,insight}.rs` | Events by producing layer | `IngestEvent`, `ConversationDelta`, `DetectEvent`, `InsightEvent` (including `AlertChanged`, `TopicVersionActivated`) |
| `spec/types/interfaces/l0_ingress.rs` … `l8_surface.rs` | One module per layer | the traits listed in the data flow above, and their error enums; `l8_surface.rs` also holds `Caller`, `Permission`, `OperatorAction`, `ActionKind` |
| `spec/types/interfaces/l8_surface/live.rs` | The live feed (SSE) | `LiveFeed`, `LiveStream`, `LiveUpdate`, `LiveUpdateKind`, `UpdateKinds` (checked), `ChannelChange`, `LiveScope`, `ScopeKeys`, `LiveCursor`, `FeedEpoch`, `Resume`, `FeedWindow` (checked), `ResumePlan`, `ResyncReason`, `LiveItem`, `LiveEnd`, `LiveSubscription`, `LiveConfig` (checked) |
| `spec/types/interfaces/l8_surface/audit.rs` | The audit log | `AuditLog`, `AuditRecord` (checked), `AuditOutcome`, `OutcomeKind`, `Rejection`, `ActionEffect`, `AuditFilter`, `AuditQuery`, `AuditPage`, `MAX_AUDIT_PAGE` |
| `spec/types/tests/` | Invariant tests | — |
| `docs/research/harness-wire-protocols.md` | What each supported harness and server sends, with sources | — |

## Invariants and constraints

- The proxy forwards requests and responses unchanged and never refreshes,
  mints, rewrites or strips credentials. Raw credentials are hashed with a
  keyed BLAKE3 and never stored.
- Only `EndpointKind::Generation` requests produce exchanges.
- A request is forwarded as soon as it is routed; nothing on the hot path
  waits for its body to decode. Request decoding runs concurrently on a tee
  of the body, and the response framer is built from the `ResponseHead`
  and the adapter's protocol, never from the decoded request. A
  `RawExchange` holds a `DecodedRequest`, so an exchange whose request fails
  to decode is never captured; it is still forwarded and is counted as
  uncaptured.
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
- A conversation's stored history holds non-system messages only. A
  `Compaction` conversation's history is its first request's non-system
  messages in request order, carried-over messages included, then that
  exchange's output, then each later delta's `new_inputs` and output. Its
  first delta's `new_inputs` are that request's non-system messages minus
  the carried-over ones (hash in the predecessor's history), in order.

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
- Every policy decision, config or operator, is kept in the channel's
  `PolicyHistory`, ordered by decision time with no duplicates
  (`PolicyHistory::from_entries`, `PolicyHistory::record`); the channel's
  policy is always `PolicyHistory::current`, so the latest decision by time
  wins whatever order events arrive in. A history entry is a
  `PolicyDecision`, which cannot be `Unreviewed(None)`.
- Every operator action call that returns `Ok`, `Forbidden`, `NotFound` or
  `BadRequest` leaves exactly one `AuditRecord` whose outcome maps back to
  that result (`AuditOutcome::of`, `AuditOutcome::result`). A record is
  `Forbidden` exactly when its caller lacks the action's required
  permission (`AuditRecord::new`). The audit log is append-only.
- Alert revisions are consecutive per alert, starting at 1 for
  `AlertOpened`.
- A live stream delivers only kinds the caller may query (checked at
  subscribe) and entries its `TopologyFilter` admits. A resume cursor is
  replayed only when every later entry is retained and from the same epoch;
  otherwise the stream starts with `Resync`. A slow stream ends with
  `Lagged`; it never drops items or blocks others. `UpdateKinds` is never
  empty, `FeedWindow`'s floor never exceeds its head, and `LiveConfig`'s
  retention outlasts its heartbeat.
- Deduplication is a triage outcome, not an alert state.
- In a `TopologyGraph`, edge shares sum to 1 unless there are no edges.
- `TimeWindow` and `ByteRange` are never empty. `Similarity` and `Share` are
  never NaN or outside `0..=1`.
- Bus delivery is at least once. Consumers are idempotent on the envelope id
  and on entity ids.
- The spec has no dependencies; `cargo check` and `cargo test` on
  `spec/Cargo.toml` must stay clean.
