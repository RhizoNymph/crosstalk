# Type specification

## Scope

- Every entity in the gateway's data model, its lifecycle states and the
  data known in each state.
- The events that cross the bus between layers.
- The trait each layer of the abstraction stack exposes, and its errors.
- Tests for invariants enforced by checked constructors.
- The query surface the UI reads and acts through (L8) is its own feature:
  [query_surface.md](query_surface.md), with its read models in
  [read_models.md](read_models.md) and export in [export.md](export.md).
- Serialization: the types are the JSON wire format between the gateway,
  the operator UI and other gateway nodes, by the conventions of
  `spec/types/wire/`, with a golden file per shape. Its own feature:
  [wire_contract.md](wire_contract.md).

## Non-scope

- Implementations of any trait.
- Database schemas: implementation crates add sqlx. Binary encodings
  other than the projection frame's layout, which is part of the type
  (`ProjectionFrame::encode` and `decode`; its HTTP framing is the
  [HTTP API](http_api.md)'s), and
  the export digest's canonical row encoding ([export.md](export.md)).
- Lifecycle simulation: `design/lifecycles/cascade.yaml`, outside the
  repository, models the same lifecycles for the stateviz simulator.

## Data and control flow

The types follow data through the stack:

1. **L0 ingress.** The `UpstreamRouter` resolves the upstream: by route in
   reverse-proxy mode, or by intercepted host in forward-proxy mode (other
   hosts are tunnelled untouched). The `ClientIdentifier` hashes the
   credential (`CredentialRef`) and the account (`AccountHash`) at the
   exchange's start time (which decides whether a secret rotation's
   overlap is still open), and reads the
   harness headers (`HarnessClaim`, `HarnessIds`, `RequestClass`) into a
   `ClientContext`. A `ProviderAdapter` classifies the endpoint
   (`EndpointKind`): only `Generation` is captured. All of this reads only
   the request head, and the request is forwarded upstream as soon as it is
   routed. `decode_request` (gzip and zstd included) runs concurrently on a
   tee of the body, off the hot path, and yields a `HarnessRequest` (or a
   `BodyDecodeError`). When the response head arrives, the adapter builds
   a `ResponseFramer` from the `ResponseHead` (its content type gives the
   `ResponseFraming`: SSE or a whole body) and its own protocol; a
   WebSocket connection gets a `WebSocketTap` instead (one exchange per
   turn). `FrameEvent`s drive the in-flight `ExchangeStage`. The
   `DecodedRequest` attaches to the exchange when it is ready, and the
   finished exchange becomes a `RawExchange` on an in-process channel. If decoding fails, the exchange was still forwarded
   and relayed, and is counted as uncaptured.
2. **L1 canonicalization.** A `Normalizer` for the exchange's
   `WireProtocol`, handling its `Dialect`, turns a `RawExchange` into a
   `NormalizedExchange`: an `Exchange` that references messages by
   `MessageHash`, plus the `Message`s, the `MediaBlob`s their `Media`
   parts name and any `NormalizeWarning`s (checked by
   `NormalizedExchange::check`). A message's hash is the BLAKE3 of its
   body's canonical encoding (`observed::message::encoding`, over the
   exact-number canonical JSON of `observed::message::json`), and every
   layer that reads a stored body decodes it with the same module. Tool
   arguments are canonical JSON so echoed messages hash the same; unknown
   blocks are kept as `Unknown` parts. Bodies and media go to the
   `BlobStore` first, then `IngestEvent::ExchangeCaptured` is published.
3. **L2 transport.** Every event is an `Envelope { id, at, BusEvent }`.
   Consumers subscribe by `Subject` within a `ConsumerGroup` and ack or
   nack each `Delivery`. Exhausted deliveries become `DeadLetter`s, which
   `DeadLetterStore::list` pages through and `replay` redelivers.
4. **L3 reconstruction.** The `IdentityResolver` gives a `Resolution`
   (known agent, new agent, or conflict) from the most specific
   `IdentityEvidence`, with harness ids scoped by `IdentityScope`; it also
   applies `MergeRequest`s, and the `AgentDirectory` resolves merged ids.
   Merges are a log (`observed::agent::merge`): each merge appends an
   immutable `MergeRecord { id, from, into, by, at, repointed }` and returns
   it. `MergeRequest::conflict` refuses it first: two different ids that
   already resolve to one canonical agent are `MergeIntoSelf` (whatever
   else holds), and otherwise a merged source or target is `AgentMerged`;
   a request naming one id twice cannot be built (`SelfMerge`). Both agents
   must be canonical; the source becomes
   `AgentState::Merged(MergedInto)` holding the record's id and its prior
   `ActiveAgentState` (Registered, Provisional or Established), and every
   agent merged into the source is repointed to the target and listed in
   `repointed`. `AgentMerged` is published. An operator unmerge
   (`IdentityResolver::unmerge`) names one `MergeId` and reverts exactly
   that record: the source returns to its prior state, each repointed agent
   that nothing moved since points at the source again (`Agent::restore`,
   which forgets the repoints after it), the record is marked reverted
   (`MergeRecord::revert` refuses a second revert, a reversal dated before
   the merge, and a `restored` list that is not a subsequence of the
   record's `repointed`: `InvalidReversal`), a `MergeVeto` between source and target is
   recorded, and `AgentUnmerged` lists the restored agents. Records can be
   reverted in any order. The resolver refuses to merge clusters a veto
   separates; an operator merge between them clears those vetoes. Stored
   records are untouched, so graphs join or split on their next read, and
   any cache of `AgentDirectory::canonical` must apply `AgentMerged` and
   `AgentUnmerged` before serving later reads. `IdentityResolver::rename`
   sets or clears an active agent's `label` (an `AgentLabel`) and publishes
   `AgentRenamed`; renaming a merged agent is refused, not redirected.
   Labels are never identity evidence, and merges and unmerges change no
   label. The UI derives a display name for an agent with no label.
   For every exchange that carries a
   `HarnessClaim`, `ClaimStore::record` adds it with the exchange's start
   time to the attributed agent's `ClaimSet` (distinct claims, latest time
   kept, so redelivery and reordering change nothing). `ClaimStore::claims`
   reads a canonical agent's claims as `ClaimSet::union` over every agent
   resolving to it, so merges and unmerges change no stored claim.
   Resolution has two halves: an `EvidenceDeriver` (a computation) derives
   the `IdentityEvidence` an exchange carries, and
   `IdentityResolver::resolve(&NonEmpty<IdentityEvidence>)` answers which
   stored agents hold its most specific items (`New`, `Known` with the
   evidence the agent lacks, or `Conflict`). The consumer then writes
   through `AgentLifecycle` (`l3_reconstruction/lifecycle.rs`): `create`
   (`NewAgent`, from config `Registered` or from traffic `Provisional`,
   recording the first exchange's activity in the same transaction),
   `advance` (`Advance::FirstTraffic` or `Establish`, the only forward
   moves) and `attach_evidence`; each publishes `Changed::Agent`.
   `ActivityStore::record` keeps, the same way, the latest exchange start
   per attributed agent, and `last_seen` reads the latest over a canonical
   agent's cluster. `AgentReads` (`l3_reconstruction/agents.rs`) serves the
   surface's agent reads from one snapshot: `list` (canonical agents an
   `AgentFilter` admits, as `AgentProfile`s), `cluster` (an
   `AgentCluster`, following a merged id) and `names` (an `IdBatch`).
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
   `SemanticMatcher` for paraphrase. Every index call that measures the
   retention window (`insert`, `lookup`, `frequency`, `observe`, `evict`)
   takes `now` as an argument, the time of the exchange being scanned.
   Hits on another agent's span become a
   `ContentMatch` (`DetectEvent::ContentMatched`); a hit in the output that
   no visible input explains has carrier `ReaderOutput`. Every
   `SpanLocation` (a span's, or a match's `read_at`) is a byte range into
   `Message::part_text` of the part it names (`observed/message/text.rs`:
   a text part's text, visible reasoning, a tool call's argument text, a
   tool result's text contents joined with `TOOL_RESULT_SEPARATOR`), on
   its character boundaries, so the surface can cut evidence excerpts from
   the stored bodies.
6. **L5 flow.** `ResourceExtractor`s turn tool calls and results into
   `ExtractedAccess`es. The `ChannelRegistry` maps each `Locator` to a known,
   declared or new channel, and the consumer writes what it saw through
   `ChannelTraffic` (`l5_flow/channels.rs`): `discover` (a channel for a
   `New` locator), `add_resource`, `record_access`, `set_detection`
   (`DetectionUpdate`) and `confirm` (advancing the canonical channel's
   detection); `ChannelReads` returns a stored channel by id and a
   `ChannelFilter`ed page of them. Each transmission state the correlator
   decides is stored with `TransmissionStore::save`
   (`l5_flow/transmissions.rs`), which keeps its verdict log. The
   `Correlator` turns accesses, content matches and clock ticks into
   `TransmissionUpdate`s, which move a `Transmission` through
   `TransmissionState`, choosing its `Route` (`Delegation`, `Channel`,
   `Direct`, `Unobserved`, in that precedence). Its windows come from a
   `CorrelationTiming`: a read waits `evidence_window` for content, then
   stays `Suspected` for `suspected_ttl`; `Confirmed::at` is the reader
   exchange's start, so a late match confirms into the read's time. Channel and transmission
   events are published. Each policy decision, from config (a declaration
   or reload) or from `PolicyChanged`, becomes a `PolicyDecision` that
   `ChannelRegistry::set_policy` records in the channel's `PolicyHistory`
   (ordered by decision time, idempotent on redelivery) while setting the
   channel's policy to the history's current entry in the same transaction.
   A `PolicyChanged` carrying `Unreviewed(None)` holds no decision and is
   acked without effect. An operator can promote a discovered channel
   (`ChannelRegistry::promote` with a `Promotion`, see
   [query_surface.md](query_surface.md#promotion-and-supersession)): it keeps its
   id, resources and `TrafficDetection`, gains a non-overlapping pattern
   that must match its seed, its origin becomes `Declared` with
   `DeclaredHistory::Promoted` recording the seed, the operator's policy
   decision is recorded in its history, and every other discovered channel
   whose seed the pattern matches becomes `Superseded` by it. Lookups never
   return a superseded channel, so each `AccessRecorded { access, channel }`
   names a canonical channel. A superseded channel's detection is frozen: a
   confirmation of a transmission whose stored route names it, late or
   not, advances the detection of the channel it resolves to
   (`TrafficDetection::Active::last_transmission` on the superseding
   channel), as read-time resolution counts the route there. `ChannelRegistry::promotion_coverage` answers
   the surface's promotion preview without changing anything: it runs the
   same `promotion::plan` over the same stored channels (`promotion::coverage`)
   and splits every resource the channel and the channels it would supersede
   hold by whether the pattern matches.
   A suspected transmission is discarded only when its window expires
   (`TransmissionState::expire`). An operator verdict
   (`TransmissionVerdicts::set`) is appended to the transmission's
   `VerdictLog` without touching its state, and `VerdictSet` is published
   (see [query_surface.md](query_surface.md#verdicts-and-detection-quality)).
7. **L6 analysis.** For each `TransmissionConfirmed`, the `Embedder` and
   `TopicModel` produce a versioned `Classification`
   (`TransmissionClassified`). Re-fits run one at a time. The
   `TopicCatalog` records a re-fit's version as `Fitting`, and
   `TopicModel::fit` fits that version over the documents (text and
   embedding) at a time it is given; `TopicError::Backend` and
   `LayoutError::Backend` are the sidecar's failures, never a recorded
   `FitFailure` ([topics_sidecar.md](topics_sidecar.md)). When the fit
   returns it stores the `TopicLineage` from the predecessor (the version
   before it in the `TopicVersionHistory`): for each older topic, the new
   topic with the most similar centroid (`LineageEntry::best`, ties to the
   lower id) and every other new topic at or above the lineage floor. The
   re-fit then re-classifies everything and publishes `TopicVersionReady`,
   and the version becomes `Ready`. On `TopicVersionReady`, every watched
   topic rule that is current on the predecessor is carried over with
   `AlertRuleDef::remap` (`TopicLineage::remap`), which yields its new
   `TopicWatch` (`Current` under the new version, or `Stale` naming the
   unmapped topics), so the UI's lineage and the rules cannot disagree; the
   rule's `RuleStatus` is left as the operator set it. When `alerts` starts
   with an embedder whose model differs from a current semantic rule's, the
   rule becomes `QueryWatch::Stale` (`AlertRuleDef::embedding_model_changed`). `TopicVersionActivated { version, previous }`
   from L7 makes the version `Active` and every older one `Superseded`;
   the catalog then enforces its `RetentionPolicy` (see
   [query_surface.md](query_surface.md#retention-and-watermarks)), as it
   does after an unpin and when `analyze` starts, and publishes
   `TopicVersionDropped` itself, from the transaction that marks each
   version dropped. The fit lifecycle is the catalog's write side,
   `TopicLifecycle` (`l6_analysis/lifecycle.rs`): `begin_fit`,
   `complete_fit` (topics and the lineage), `fail_fit`, `mark_ready`,
   `mark_active` (a `CatalogActivation`) and `assign` (a
   `StoredAssignment`). `TopicSizes` count topic
   assignments per topic (outliers apart), optionally over a window. The
   `SearchIndex` takes a `TopologyFilter`, pages hits in (score, id) order
   and reports the topic-model version it resolved (`SearchResults`); its
   write side is `SearchCorpus` (`l6_analysis/corpus.rs`: `index` an
   `IndexedTransmission`, `remove`, `judge`, `set_model`, `drop_model`).
   Projection jobs go through the `ProjectionStore`: a fitter claims the
   oldest queued job, reads its `Sample` from the `ProjectionSource`, lays it
   out with the seeded, deterministic `LayoutFitter`, and stores the
   `ProjectionFrame` (see [query_surface.md](query_surface.md#projections));
   a frame that does not belong to its job is `FrameMismatch`. `AlertRuleEval`s turn envelopes into
   `AlertDraft`s, which `AlertTriage` opens or deduplicates
   (`TriageOutcome`), or drops as `RuleInactive` when the rule stopped
   evaluating, and suppresses on sanctioning or rule disabling, and on a
   `VerdictSet` holding `FalseDetection` suppresses every active alert
   about that transmission (`SuppressReason::OperatorRejected`), after which
   triage opens nothing about it (`TriageOutcome::OperatorRejected`) while
   that verdict is current. Every suppression is stamped with the time the
   caller passes (the triggering event's). Every stored
   change to an alert bumps its `AlertRevision` and publishes
   `AlertChanged`. The same store implements `AlertRuleMaintenance`
   (`topic_version_ready`, `embedding_model_changed`), `AlertActions`
   (`acknowledge`, `resolve`, refusing an open alert with
   `NotAcknowledged`) and `AlertReads` (`rule`, `rules`, `alert`, `alerts`,
   `rule_version`) (`l6_analysis/alerts.rs`). Rules live in an `AlertRuleSet`: the five `BuiltinRule`s,
   each exactly once under a fixed reserved id, which operators can only
   enable or disable, and user rules (`WatchedTopic`, `SemanticQuery`)
   under server-assigned ids. Each `AlertRuleDef` has a name, its sinks
   (`SinkId`s), a `RuleStatus` and, for user rules, its creator. Operators
   manage rules through `AlertRuleStore`: `create` resolves a `UserRule`
   into a current `RuleDefinition` (a watched-topic rule must name the
   current topic version and existing topics, and takes the configured
   default remap threshold when it has none; a semantic query's text is
   embedded and stored with its embedding, which carries the model) and
   returns the new id; `update` replaces a user rule's name, definition and
   sinks, and on a stale rule retargets and enables it; `set_enabled`
   enables or disables any rule, except that enabling a stale rule is
   refused (`RuleError::Stale`, changing nothing) and disabling is always
   allowed. A rule keeps its kind and is never deleted. Staleness (`TopicWatch::Stale`, `QueryWatch::Stale`, reported as
   a `StaleReason`) is separate from status and no operator action sets it.
   Every stored change to a rule bumps its `RuleRevision` and publishes
   `AlertRuleChanged`.
8. **L7 topology.** The `EdgeStore` applies each `EdgeContribution` (which
   carries its `ClassificationCause`) to its `EdgeKey` bucket (per
   topic-model version, `BucketWidth` wide), records each
   `TopicVersionReady` count (`version_ready`, the first kept), activates a
   version once it is complete (it has processed as many distinct `Refit`
   classifications under it as `TopicVersionReady` counts; `Confirmation`
   classifications under it do not count; `activate` returns an
   `Activation`) and publishes `TopicVersionActivated` from the switching
   transaction, and
   answers `TopologyGraph` queries over canonical agents with per-edge
   `Share`s. A series query takes a `SeriesGrid` (a bucket-aligned window
   cut into `SeriesStep`s, each a whole number of buckets), a `Weighting`, a
   `SeriesGrouping` (total, topic, route kind, edge) and a `TopologyFilter`,
   and returns a `TopologySeries`: one value per step per group, counted
   exactly as a graph over that step. Summing every value gives
   `TopologyGraph::total` for the same window, weighting, filter and topic
   version, and grouped by edge each series sums to that edge's stat.
   `EdgeStore::totals` returns what a graph counts without building it
   (`EdgeTotals::of` the graph: its transmissions, matched bytes and
   distinct channels, under its version), for the overview.
   `EdgeStore::transmissions` lists the contributions behind one edge
   (`EdgeSelector`) from the same stored rows, a page at a time
   (`EdgeTransmissionPage`), with the first page's topic version pinned in
   the cursor. Buckets hold detector output only; the store keeps a copy
   of current verdicts (`EdgeStore::judge`) and subtracts false detections
   at query time when the filter excludes them. Activation deletes nothing;
   a version's buckets go only on `TopicVersionDropped`
   (`EdgeStore::drop_version`). The topology consumer recomputes the
   watermark from a `FrontierSource` at least once per bucket width
   (`EdgeStore::advance_watermark`); the store publishes `WatermarkAdvanced`
   on each strict advance, and refuses a contribution into a final bucket
   (`LateContribution`). Graph, totals, series and drill-down results come
   back `Watermarked`. `EdgeStore::apply_access` counts each `AccessRecorded`
   into an `AccessEdge` bucket (agent, channel, op, bucket), idempotent on
   the access id, and `EdgeStore::channel_topology` answers the
   channel-centred graph. `EdgeStore::agent_traffic` gives listed agents'
   transmissions in and out over a window, equal to their node counts in
   `graph` under the default filter (for agent rows). Every query resolves stored agents and
   channels through the two directories and fills graph nodes from
   `NodeFacts` (`AgentFacts`, `ChannelFacts`; a synchronous cache of L3's
   and L5's facts, with fixed defaults for a node it has not seen).
   Readers outside L7 read its exposed watermark through `WatermarkRead`. Reads (graph,
   totals, channel topology, series, drill-down, watermark) fail with
   `EdgeQueryError`; writes with `EdgeError`.
9. **L8 surface.** `QueryApi` serves every read the UI makes,
   `OperatorActions::act` takes every operator action and forwards it to
   the layer that owns its effect, `LiveFeed` streams id-only change
   events built from every store's `Changed`, and `AuditLog` records every
   action call, config change and export, each for a `Caller` the
   `OperatorDirectory` built; the directory is stored by an
   `OperatorStore` (`load`, `operators`, `caller`). `AlertSink`s deliver
   each alert to the sinks its rule lists, and a `SinkRegistry` records
   each delivery (`record_delivery`, `sinks`). The surface is its own feature:
   [query_surface.md](query_surface.md), with the read models it serves
   in [read_models.md](read_models.md) and export in
   [export.md](export.md).

### Conventions for the layer traits

- **Stores have a spec write side.** Every write a consumer, the surface
  or config makes to a stateful store is a spec trait method (the
  `AgentLifecycle`, `ChannelTraffic`, `TransmissionStore`,
  `TopicLifecycle`, `SearchCorpus`, `AlertRuleMaintenance`, `AlertActions`,
  `OperatorStore` and `SinkRegistry` traits, and the write methods of the
  existing traits), so the in-memory and Postgres stores implement the
  same traits and a model-based harness drives either through the spec
  alone. Reads of other layers' caches go through spec read traits
  (`AgentDirectory`, `ChannelDirectory`, `NodeFacts`, `WatermarkRead`).
- **A store publishes what it decides.** A write that commits a change
  publishes, from its transaction's outbox, `Changed` for every entity it
  changed and the bus events of decisions taken inside the store (merges,
  promotions, verdict records, triage outcomes, rules going stale,
  retention drops, edge activation, watermark advances), and returns a
  typed outcome; no store method returns an event for its caller to
  publish. Events announcing a consumer's own computation (a lookup's
  `New` behind `ChannelDiscovered`, correlator updates, classifications,
  new evidence behind `AgentSeen`) are the consumer's, published once the
  write commits. `TopicVersionDropped` is the topic catalog's.
- **Time is an argument.** Every store method whose effect or answer
  depends on the time takes it as a `Timestamp` (`at` for what it records,
  `now` for a window or lease measured from it); no store reads a clock.
  The caller passes the triggering event's time or a reading of the
  `Clock` it was handed.

- **Async methods return `Send` futures.** Every async trait method in
  `interfaces/` is declared in its desugared form,
  `fn name(&self, ...) -> impl Future<Output = T> + Send` (`&mut self` where
  the component owns its state), never as a bare `async fn`. An
  implementation still writes `async fn`; the compiler checks that its
  future is `Send`. Code generic over a trait (a UI or client generic over
  `QueryApi`, a consumer loop generic over `EventBus`) can then hand the
  futures to `tokio::spawn` or an axum handler without return-type
  notation. A bare `async fn` would compile only against one concrete type
  the compiler can see through.
- **Streams and handles are `Send + 'static`.** The associated types a task
  keeps across awaits or hands to another task are bounded so:
  `EventBus::Subscription`, `LiveFeed::Stream`, `QueryApi::ExportRows`,
  `ExportSource::Rows`, and `ProviderAdapter::Framer` and `Tap` (whose
  methods are synchronous).
- **What this asks of an implementation.** An `async fn` future holds its
  arguments, `&self` included, so an implementation whose `&self` methods
  are async is `Sync` in practice, and one with `&mut self` methods is
  `Send`. A generic implementation states those bounds itself (`SealedRows`
  requires its source and hasher to be `Send`). `RuleContext` is `Sync`
  because `AlertRuleEval::evaluate` borrows a context into its future. The
  traits do not require `Self: Send + Sync`; a host that shares one
  implementation across tasks adds those bounds where it does
  (`Arc<Q>` with `Q: QueryApi + Send + Sync + 'static`).
- **Object safety is unchanged.** A trait with async methods was not
  dyn-compatible as `async fn` and is not as `impl Future`; the
  synchronous traits (`UpstreamRouter`, `ClientIdentifier`,
  `ResponseFramer`, `WebSocketTap`, `AgentDirectory`, `ChannelDirectory`,
  `RowHasher` and the like) are untouched.
- `tests/send.rs` checks all of this at compile time
  (`canonical.interface.send-futures`): an uninhabited `Dummy` implements
  every trait with `async fn`, and a function generic over each trait
  passes every method's future to `assert_send` and every associated
  stream to `assert_send_static`, so dropping a bound fails the build.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `spec/Cargo.toml` | Builds the spec as a library so it type-checks and its tests run; a workspace member and the boundary crate every implementation crate depends on; its only dependencies are `serde`, `serde_json` and `blake3` (message, media and keyed secret digests), pinned exactly in the root `[workspace.dependencies]` (the root `Cargo.lock` is committed); `proptest` is its one dev-dependency | crate `crosstalk-spec` |
| `spec/types/wire/` | The JSON wire contract: conventions, `WireRequest`, `decode_request`, `Rejected`, timestamps' RFC 3339 text, the authority assertions ([wire_contract.md](wire_contract.md)) | `WireRequest`, `decode_request`, `DecodeError`, `DecodeErrorKind`, `Rejected`, `time`, `authority` |
| `spec/types/mod.rs` | Crate root, tier overview | — |
| `spec/types/ids.rs` | Typed ids, and their wire text | `AgentId`, `ExchangeId`, `SpanId`, `ChannelId`, `TransmissionId`, … `AuditId`, `MergeId`, `ProjectionId`, `SinkId`, `ConfigHash`, `MessageHash`, `PromptHash`, `CredentialHash`, `AccountHash`; every entity id's `ulid_text` (Crockford base32) and `from_ulid_text`, `InvalidUlidText`; `EntityId` (every entity id and `ConnectionId`, for minting) |
| `spec/types/ids/mint.rs` | The ULID generator ([spec_primitives.md](spec_primitives.md)) | `UlidGenerator` (`next_ulid`, `mint`, `next_at`, `mint_at`), `RandomSource`, `SeededRandom` (`new`, `from_entropy`), `UlidExhausted`, `ulid_millis`, `MAX_ULID_MILLIS` |
| `spec/types/ids/secret.rs` | The deployment secret and the keyed hasher ([spec_primitives.md](spec_primitives.md)) | `DeploymentSecret` (`new`, `from_hex`, `version`; `InvalidSecret`), `KeyedHasher` (`new`, `rotating`, `credential`, `account`; `InvalidRotation`), `SecretDigests` |
| `spec/types/support.rs` | Shared building blocks, each with its wire form | `NonEmpty` (`EmptyList`), `NonBlank`, `DisplayText` (checked), `QueryText` (checked: trimmed, non-empty, at most `MAX` characters, line breaks allowed; `InvalidQueryText`), `Capped` (checked: at most `MAX` shown, exact total), `Change`, `Timestamp`, `Clock` (the injected wall clock, `now`; `SystemClock` reads the OS clock; see [sim.md](sim.md)), `TimeWindow`, `ByteRange`, `Blake3` (`of`, `to_hex`, `from_hex`, `InvalidHex`), `hex`, `from_hex`, `Similarity`, `Share` (`ShareOutOfRange`), `Watermark` |
| `spec/types/observed/client.rs` | Ingress, upstream, credential and harness facts | `IngressMode`, `Upstream`, `UpstreamKind`, `Dialect`, `CredentialScheme`, `CredentialRef`, `HarnessClaim`, `HarnessIds`, `RequestClass`, `ClientContext`, `EndpointKind` |
| `spec/types/observed/message.rs` | Canonical messages | `Message` (`new`; decoded only with its body's hash, `MessageHashMismatch`), `MessageBody` (serde in its encoding's shape), `Role`, `AssistantPart`, `UserPart`, `Reasoning` (`Visible { text, signature }`, `Opaque`), `Media`, `MediaBlob` (checked: hash of its bytes; `InvalidMediaBlob`), `ToolCall`, `ToolArguments`, `CanonicalJson`, `ToolResult`, `Unknown`, `PartRef` |
| `spec/types/observed/message/encoding.rs`, `encoding/mirror.rs` | The canonical encoding of a body and its hash ([spec_primitives.md](spec_primitives.md)) | `encode`, `decode` (`DecodeError`), `hash`, `hash_bytes`, `message` |
| `spec/types/observed/message/json.rs`, `json/` | JSON with exact numbers; canonical text ([spec_primitives.md](spec_primitives.md)) | `Json`, `Number`, `JsonError`, `canonicalize`, `MAX_DEPTH`, `MAX_EXPONENT_DIGITS` |
| `spec/types/observed/message/text.rs` | The text a span location indexes | `Message::part_text`, `Message::part_count`, `NoPartText`, `TOOL_RESULT_SEPARATOR` |
| `spec/types/observed/exchange.rs` | Exchanges and their pipeline stage | `Exchange`, `ExchangeMeta`, `WireProtocol`, `Transport`, `Continuation`, `ResponseId`, `ExchangeOutcome`, `ExchangeFailure`, `ExchangeStage`, `TokenUsage` (checked: cache counts within `input`, reasoning within `output`; `TokenCounts`, `InvalidTokenUsage`) |
| `spec/types/observed/agent.rs` | Agent identity and labels | `Agent` (`rename`), `AgentLabel`, `IdentityEvidence`, `IdentityScope`, `Strength`, `AgentState`, `ActiveAgentState`, `MergeRequest`, `MergeAuthor` |
| `spec/types/observed/agent/merge.rs` | The merge log, exact unmerge and vetoes | `MergeConflict`, `MergeRequest::conflict`, `MergeRecord` (checked, `revert`; `InvalidReversal`, `InvalidMergeRecord`), `Reversal`, `MergedInto`, `Agent::merge_away`, `Agent::repoint`, `Agent::revert`, `Agent::restore`, `MergeVeto` (checked, `separates`) |
| `spec/types/observed/agent/claims.rs` | Harness claims seen per agent | `SeenClaim`, `ClaimSet` (checked; `observe`, `union`), `DuplicateClaim` |
| `spec/types/observed/conversation.rs` | Threaded conversations | `Conversation`, `ConversationOrigin` |
| `spec/types/derived/provenance/span.rs` | Spans and their lifecycle | `Span`, `SpanLocation`, `Origin`, `RelaySource`, `SpanState`, `SpanEvent`, `OriginatedSpan` |
| `spec/types/derived/provenance/fingerprint.rs` | Fingerprints and index hits | `Fingerprint`, `WinnowParams`, `PositionedFingerprint`, `FingerprintHit` |
| `spec/types/derived/provenance/matching.rs` | Content matches | `ContentMatch`, `MatchKind`, `Codec`, `Carrier`, `InvalidMatch` |
| `spec/types/derived/flow/resource.rs` | Resources and patterns | `Resource`, `Locator`, `ResourcePattern` (`matches`, `overlaps`), `Host` |
| `spec/types/derived/flow/access.rs` | Accesses | `Access`, `AccessOp`, `AccessKind`, `Extraction` |
| `spec/types/derived/flow/evidence.rs` | Communication evidence | `Evidence`, `CoAccess`, `InvalidCoAccess` |
| `spec/types/derived/flow/timing.rs` | The correlator's windows | `CorrelationTiming` (checked: `window_closes_at`, `expires_at`, `settle_after`), `InvalidTiming` |
| `spec/types/derived/flow/transmission.rs` | Transmissions and their lifecycle | `Transmission`, `Route` (`resolved`), `DelegationDirection`, `DirectCarrier`, `TransmissionState` (`expire`, `confirmed`, `co_accesses`), `Confirmed`, `Classification` |
| `spec/types/derived/flow/channel/mod.rs` | Channels, promotion and supersession | `Channel` (`canonical`), `ChannelOrigin` (`promoted`, `superseded`, `seed`, `detection_kind`), `Supersession`, `NotPromotable`, `NotSupersedable`, `Declaration`, `DeclaredHistory`, `Seed` |
| `spec/types/derived/flow/channel/detection.rs` | Channel detection lifecycle | `DeclaredDetection`, `TrafficDetection`, `DetectionKind` |
| `spec/types/derived/flow/channel/policy.rs` | Channel policy, its history and traffic routing | `Policy`, `Decision`, `PolicyAuthor`, `PolicyKind`, `PolicyDecision` (checked from `Policy`), `PolicyHistory` (checked), `Recorded`, `TrafficVerdict` |
| `spec/types/aggregates/edge.rs` | Topology edges, their totals and their drill-down | `EdgeKey`, `TopicSlot`, `EdgeStats`, `Edge`, `Weighting`, `RouteKind`, `TopologyGraph` (checked: built only by `new` from `TopologyGraphParts`, its wire shape; accessors `window`, `weighting`, `topic_version`, `nodes`, `edges`, `into_parts`; `InvalidGraph`), `EdgeTotals` (`of`), `EdgeSelector`, `EdgeTransmission`, `EdgeTransmissionPage`; re-exports `TopologyFilter` |
| `spec/types/aggregates/series.rs` | Time series over the edge table | `BucketWidth`, `SeriesStep`, `SeriesGrid`, `SeriesGrouping`, `SeriesEdge`, `Series`, `SeriesGroups`, `TopologySeries`, `TopologyGraph::total`, `Weighting::stat`, `RouteKind::of` |
| `spec/types/aggregates/topic.rs` | Embeddings and topics | `Embedding`, `EmbeddingModel`, `Topic` (term weights `Finite`), `TopicModelVersion` (a wire request), `TopicAssignment`, `Assignment` (neither on the wire) |
| `spec/types/aggregates/topic_history.rs` | Topic-model versions, sizes and lineage | `TopicVersionStatus`, `CompletedFit`, `FitRecord`, `TopicVersionInfo` (`with_retention`), `TopicVersionHistory`, `TopicSize`, `TopicSizes`, `LineageLink`, `LineageEntry`, `TopicLineage` (`remap` to a `TopicWatch`), `RemapError` |
| `spec/types/aggregates/alert/rules.rs` | Alert rules (re-exported from `aggregates::alert`) | `BuiltinRule`, `UserRule` (a wire request), `RuleDefinition`, `RuleName`, `RuleQueryText` (a semantic rule's text, at most `RULE_QUERY_MAX_CHARS`), `AlertRuleConfig`, `SemanticQuery`, `TopicWatch`, `QueryWatch`, `StaleReason`, `ContentRule`, `AlertRule`, `AlertRuleKind`, `AlertRuleDef` (checked, decoded through `builtin` or `load`; `evaluates`, `set_enabled` refusing a stale rule with `StaleRule`, `update`, `remap`, `embedding_model_changed`), `InvalidRuleDef`, `AlertRuleSet`, `RuleStatus`, `RuleRevision` |
| `spec/types/aggregates/alert/mod.rs` | Alerts | `AlertSubject`, `AlertDraft`, `TriageOutcome` (incl. `OperatorRejected`), `Alert`, `AlertState` (`kind`, `is_active`), `AlertStateKind` (`ALL`, `is_active`; re-exported by L8), `SuppressReason` (incl. `OperatorRejected`), `AlertRevision` |
| `spec/types/events/mod.rs` | Bus envelope and subjects | `Envelope`, `BusEvent`, `Subject` |
| `spec/types/events/{ingest,detect,insight}.rs` | Events by producing layer | `IngestEvent` (including `AgentMerged`, `AgentUnmerged`, `AgentRenamed`), `ConversationDelta`, `DetectEvent` (including `VerdictSet`), `InsightEvent` (including `AlertChanged`, `AlertRuleChanged`, `TopicVersionActivated`, `TopicVersionDropped`, `WatermarkAdvanced`) |
| `spec/types/interfaces/l0_ingress.rs` … `l7_topology.rs` | One module per pipeline layer | the traits listed in the data flow above, and their error enums: `IdentityResolver::merge`, `unmerge`, `rename` and `resolve` (over derived evidence), `EvidenceDeriver`, `AgentDirectory`, `ClaimStore`, `ResolveError` (with `MergeIntoSelf`, `of_conflict`), in `l3_reconstruction/agents.rs` `AgentReads`, `ActivityStore`, `AgentReadError`, and in `l3_reconstruction/lifecycle.rs` `AgentLifecycle` (`NewAgent`, `AgentOrigin`, `Advance`, `AgentLifecycleError`) (L3); `ChannelDirectory`, `ChannelRegistry::declare`, `set_policy`, `policy_history`, `promote` (`Promoted`, `PromoteError`), `promotion_coverage` and `resource_use`, in `l5_flow/channels.rs` `ChannelTraffic` (`DetectionUpdate`, `TrafficError`) and `ChannelReads`, in `l5_flow/transmissions.rs` `TransmissionStore` (`TransmissionStoreError`) (L5); `AlertTriage::transmission_judged`, `TopicCatalog` (with `pin`, `unpin`, `enforce_retention`, paged `topics`), in `l6_analysis/lifecycle.rs` `TopicLifecycle` (`StoredAssignment`, `CatalogActivation`, `TopicLifecycleError`), in `l6_analysis/corpus.rs` `SearchCorpus` (`IndexedTransmission`, `CorpusError`), in `l6_analysis/alerts.rs` `AlertRuleMaintenance`, `AlertActions` (`AlertActionError`) and `AlertReads` (`AlertReadError`), `SearchIndex` (paged), `ProjectionStore`, `ProjectionSource`, `LayoutFitter`, `Sample` (its rows carry a `PointRoute`), `SearchError`, `ProjectionStoreError`, `ProjectionJobError`, `AlertRuleStore`, `RuleError` (incl. `Stale`) (L6); `EdgeStore::judge`, `apply_access` (`AccessContribution`), `version_ready`, `activate` (`Activation`), `totals`, `channel_topology`, `agent_traffic`, `series`, `transmissions`, `drop_version`, `watermark`, `advance_watermark`, `FrontierSource`, `NodeFacts` (`AgentFacts`, `ChannelFacts`), `WatermarkRead`, `EdgeError` (writes) and `EdgeQueryError` (reads) (L7). L8 is in [query_surface.md](query_surface.md) |
| `spec/types/tests/` | Invariant tests: `observed.rs`, `infrastructure.rs`, `agents.rs` (a reference merge table and a seeded random walk over merges and reverts), `provenance.rs`, `flow.rs`, `policy.rs`, `rules.rs` (built-in and user rules), `aggregates.rs`, `series.rs`, `topic_history.rs`, `support.rs`, `encoding/` (the message encoding and canonical JSON: pinned vectors in `tests/golden/encoding/`, round trips, the exact-inverse decode, RFC 8785), `secrets.rs` (keyed digests, rotation overlaps, a secret never shown), `minting.rs` (ULID monotonicity and uniqueness); the surface's tests are listed in [query_surface.md](query_surface.md), [read_models.md](read_models.md) and [export.md](export.md); the wire contract's (`tests/wire/`, with goldens in `tests/golden/`) in [wire_contract.md](wire_contract.md) | — |
| `spec/types/tests/send.rs`, `tests/send/{pipeline,detection,surface}.rs` | Compile-time check that every async trait method's future is `Send` and every associated stream or handle `Send + 'static`: an uninhabited `Dummy` implementing each trait with `async fn`, and one check function generic over each trait | `Dummy`, `assert_send`, `assert_send_static`, `arg` (test-only) |
| `spec/invariants/` | One TOML file per invariant, with its evidence (see its README) | — |
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
  `SecretVersion` that computed them; rotation overlaps two versions. Only
  `KeyedHasher` reads a `DeploymentSecret`'s key, and neither serializes,
  clones or formats it.
- A `UlidGenerator`'s ids strictly increase whatever its clock reads or
  the time `next_at` is given; it
  reads time from the injected `Clock` and randomness from a
  `RandomSource`, so it is deterministic under simulation.
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
  self-merge, and a merge of two ids of one cluster is refused as
  `MergeIntoSelf` before any other merge refusal.
- Merges are a log of immutable records; both agents of a merge are
  canonical, so chains are never longer than one. An agent is merged by
  exactly one unreverted record, the one that names it as source.
- An unmerge reverts one record exactly, at most once: its source returns
  to its prior `ActiveAgentState`, every agent it repointed points at the
  source again unless it was unmerged or merged afresh since, a
  `MergeVeto` keeps the pair apart from the resolver, and one
  `AgentUnmerged` lists the restored agents. Reverting the latest record is
  the identity on the merge table and on every topology graph.
- A record's reversal is dated no earlier than its merge and restores only
  agents the merge repointed, in the record's order, each at most once
  (`reconstruct.merge-record.reversal-within-merge`); decoding a stored
  record checks the same, through `revert`.
- Labels are display only: never identity evidence, untouched by merges
  and unmerges, and only an active agent can be renamed. An `AgentLabel` is
  trimmed, non-empty, at most 64 characters and free of control
  characters.
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
- `MessageHash` is the BLAKE3 of a message's canonical encoding
  (`observed::message::encoding`), and `decode` accepts exactly the bytes
  `encode` writes; messages are immutable after hashing. An `Exchange` references messages by hash only,
  and every referenced message is in the blob store before
  `ExchangeCaptured` is published, so a body `BlobStore::get` no longer
  returns was dropped by content retention.
- Every `SpanLocation` L4 records indexes `Message::part_text` of its part
  and starts and ends on that text's character boundaries.
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
- A channel declared before traffic has `DeclaredDetection`; a discovered
  or promoted channel has `TrafficDetection`. So a declared, never-used
  channel is representable and a discovered or promoted, never-accessed one
  is not. Promotion keeps the channel's id, resources and detection, records
  the operator's policy decision, and supersedes exactly the other
  discovered channels whose seed its pattern matches; its pattern matches
  its seed and overlaps no other declared pattern (`ResourcePattern::overlaps`
  is exact). A `Promotion`'s declaration and decision share one operator and
  time (`Promotion::new`). `promotion::plan` reads only the declaration, and
  `promotion::coverage` is `plan` plus the held resources split by the
  pattern, each once, so a preview and a promotion in the same state agree.
  Each side is a `Capped` of at most `COVERAGE_CAP` (200) newest
  resources with its exact total; the superseded channels are complete.
- A `Capped<T, MAX>` shows at most `MAX` items and a total no smaller than
  what it shows (`Capped::new`), so a capped list is never mistaken for a
  complete one.
- A superseded channel's detection never changes after the promotion: a
  confirmation routed through it advances the superseding channel's
  detection instead.
- Only a suspected transmission can be discarded, and only by expiry.
- Confirmed traffic on a channel raises an alert unless its policy is
  sanctioned (`Policy::on_traffic`).
- Every policy decision, config or operator, is kept in the channel's
  `PolicyHistory`, ordered by decision time with no duplicates
  (`PolicyHistory::from_entries`, `PolicyHistory::record`); the channel's
  policy is always `PolicyHistory::current`, so the latest decision by time
  wins whatever order events arrive in. A history entry is a
  `PolicyDecision`, which cannot be `Unreviewed(None)`.
- Alert revisions are consecutive per alert, starting at 1 for
  `AlertOpened`.
- Deduplication is a triage outcome, not an alert state.
- Each built-in rule exists exactly once, under its fixed reserved id, and
  can only be enabled or disabled; user rules never take a reserved id.
  Rules are never deleted and keep their kind.
- A semantic rule's query text is trimmed, non-empty and at most
  `RULE_QUERY_MAX_CHARS` (1,000) characters (`RuleQueryText`); the
  embedding model may still refuse shorter text (`QueryTooLong`).
- Only user rules can be stale, and staleness is separate from the
  operator's enabled or disabled status: a re-fit or an embedding model
  change makes a rule stale, and only an update makes it current again,
  which also enables it. Enabling a stale rule is refused
  (`AlertRuleDef::set_enabled`, `StaleRule`) and disabling one is always
  allowed; a rule that goes stale while enabled keeps its status. A rule
  evaluates only when enabled and current.
  Once a disable returns, the rule has no active alerts and triage opens
  none for it.
- The edge store publishes `TopicVersionActivated { version, previous }`
  exactly once per switch, from the transaction in which
  `EdgeStore::activate` switches graph and series queries (it returns
  `Activation::Switched` then), and never for a version older than the
  active one.
- `TopicVersionDropped` is published by the topic catalog alone, once per
  dropped version, from the transaction that marks it dropped and deletes
  its assignments.
- A refused store write changes nothing and publishes nothing; every
  store method that depends on the time takes it as an argument.
- In a `TopologyGraph`, edge shares sum to 1 unless there are no edges;
  every graph is built by `TopologyGraph::new`, which checks shares,
  edges and nodes, so no value breaks a rule.
- A `ClaimSet` holds each claim once, newest first with a total tie order;
  observing is idempotent and order-independent, and a canonical agent's
  claims are the union over its aliases. A `ResourceUse` has a writer or
  reader and no agent twice per list (`ResourceUse::new`).
- A `SeriesStep` is a whole number of buckets (`SeriesStep::new`). A
  `SeriesGrid` starts on a bucket boundary and is a whole number of steps,
  at most `SeriesGrid::MAX_POINTS` (`SeriesGrid::new`). A `TopologySeries`
  has one value per grid point in every series, distinct keys, no all-zero
  grouped series and no self-edge series (`TopologySeries::new`). A series
  over a grid built for another bucket width is rejected.
- For the same window, weighting, filter and topic version, the sum of a
  series' values is `TopologyGraph::total`, and grouped by edge each series
  sums to that edge's stat. Series concatenate over adjacent grids, and a
  coarser step sums runs of finer points.
- A `TopicVersionHistory` starts at version 0, which alone is unfitted;
  versions strictly increase; exactly one is `Active`; older ones are
  `Superseded` by the first newer activation, at its time; newer ones are
  `Ready` or `Fitting`, and only the newest may be `Fitting`. A version's
  timestamps never decrease (`TopicVersionInfo::new`,
  `TopicVersionHistory::new`). A failed fit leaves no version.
- A `TopicLineage` goes from a version to the next one in the history, with
  one entry per older topic. Each entry's best link is the newer topic with
  the most similar centroid, ties to the lower id, kept even below the
  floor; the other links are the topics at or above the floor, in lineage
  order (`LineageEntry::new`, `TopicLineage::new`).
- Watched-topic rules are remapped only by `TopicLineage::remap` over the
  stored lineage: every topic to its best link if that reaches the
  threshold (`TopicWatch::Current`), otherwise the rule becomes
  `TopicWatch::Stale` with the unmapped topics; its `RuleStatus` is left
  as the operator set it. The lineage is stored before
  `TopicVersionReady`.
- `TopicSizes` list every topic of the version once (`TopicSizes::new`) and
  count topic assignments, so unlike series they keep transmissions between
  agents later merged into one.
- `CorrelationTiming` durations are non-zero. `Confirmed::at` is the
  reader exchange's start. The watermark is
  `align_down(min(ticked_through − settle_after, oldest_pending))`, never
  decreases, and moves in whole buckets; once exposed, buckets of activated
  versions ending at or before it never change (`apply` returns
  `LateContribution`).
- `TimeWindow` and `ByteRange` are never empty. `Similarity` and `Share` are
  never NaN or outside `0..=1`.
- Bus delivery is at least once. Consumers are idempotent on the envelope id
  and on entity ids.
- Every async trait method in `interfaces/` returns a `Send` future, and
  every associated stream or per-connection handle is `Send + 'static`
  (`canonical.interface.send-futures`; checked by `tests/send.rs`). No
  method is a bare `async fn`.
- The spec's only dependencies are `serde`, `serde_json` (both pinned
  exactly to the UI's versions) and `blake3`, with the lockfile committed; `cargo check`
  and `cargo test` on `spec/Cargo.toml` must stay clean. Every type that
  crosses a process boundary follows the wire contract
  ([wire_contract.md](wire_contract.md)): it round-trips, a checked type
  decodes only through its constructor, decoding is strict, and a golden
  file pins its JSON.
