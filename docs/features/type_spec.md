# Type specification

## Scope

- Every entity in the gateway's data model, its lifecycle states and the
  data known in each state.
- The events that cross the bus between layers.
- The trait each layer of the abstraction stack exposes, and its errors.
- Tests for invariants enforced by checked constructors.
- The query surface a frontend reads: paginated lists (the audit log,
  alerts, search hits, topics and projections included), the one filter
  that links the graph, series, search, projection and edge drill-down and
  the topic-model version it resolves, stored projections and their
  columnar frame, time series, the topic-model version history and the
  channel policy history.
- The typed query errors, and how every store error behind a query maps
  to one.
- The live update feed (SSE) and the append-only audit log.
- Operator actions, each with one required permission: policy, channel
  promotion, agent merges, unmerges and labels, alert triage, transmission
  dismissal, alert rule management and dead-letter replay.

## Non-scope

- Implementations of any trait.
- Serialization formats, database schemas, wire encodings. Implementation
  crates add serde and sqlx on their copies of these types. The one
  exception is the projection frame, whose binary layout is part of the
  type (`ProjectionFrame::encode` and `decode`); its HTTP framing is not.
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
   nack each `Delivery`. Exhausted deliveries become `DeadLetter`s, which
   `DeadLetterStore::list` pages through and `replay` redelivers.
4. **L3 reconstruction.** The `IdentityResolver` gives a `Resolution`
   (known agent, new agent, or conflict) from the most specific
   `IdentityEvidence`, with harness ids scoped by `IdentityScope`; it also
   applies `MergeRequest`s, and the `AgentDirectory` resolves merged ids.
   A merge records on the source's `Merged` its prior `MergeableState` and
   the agents it repointed; a merge into a merged agent is redirected to
   that agent's target and recorded as a merge followed by a repoint. An
   operator unmerge (`IdentityResolver::unmerge`) returns the agent to its
   prior state and points every agent repointed through it back at it
   (`Merged::restore_through`), then publishes `AgentUnmerged`; stored
   records are untouched, so graphs split again on their next read.
   `IdentityResolver::set_label` appends to the canonical agent's
   `LabelLog`; labels are never identity evidence, merges and unmerges
   change no log, and `LabelView` shows the target's label with differing
   alias labels as history.
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
   acked without effect. An operator can promote a discovered channel
   (`ChannelRegistry::promote`): it keeps its id, resources, policy and
   `TrafficDetection`, gains a non-overlapping pattern that must match its
   seed, and its origin becomes `Declared` with `DeclaredHistory::Promoted`
   recording the seed. An operator can dismiss a suspected transmission
   (`TransmissionReview::dismiss`): the request goes through the owning
   correlator shard (`Correlator::on_dismiss`), so it is ordered with late
   matches, and the transmission becomes `Discarded` with
   `DiscardReason::Dismissed`; `TransmissionDismissed` is published.
7. **L6 analysis.** For each `TransmissionConfirmed`, the `Embedder` and
   `TopicModel` produce a versioned `Classification`
   (`TransmissionClassified`). Re-fits run one at a time. The
   `TopicCatalog` records a re-fit's version as `Fitting`; when the fit
   returns it stores the `TopicLineage` from the predecessor (the version
   before it in the `TopicVersionHistory`): for each older topic, the new
   topic with the most similar centroid (`LineageEntry::best`, ties to the
   lower id) and every other new topic at or above the lineage floor. The
   re-fit then re-classifies everything and publishes `TopicVersionReady`,
   and the version becomes `Ready`. On `TopicVersionReady`, every watched
   topic rule that is current on the predecessor is carried over with
   `TopicLineage::remap`, which yields its new `TopicWatch` (`Current` under
   the new version, or `Stale` naming the unmapped topics), so the UI's
   lineage and the rules cannot disagree; the rule's `RuleStatus` is left
   as the operator set it. `TopicVersionActivated { version, previous }`
   from L7 makes the version `Active` and every older one `Superseded`. `TopicSizes` count topic
   assignments per topic (outliers apart), optionally over a window. The
   `SearchIndex` takes a `TopologyFilter`, pages hits in (score, id) order
   and reports the topic-model version it resolved (`SearchResults`).
   Projection jobs go through the `ProjectionStore`: a fitter claims the
   oldest queued job, reads its `Sample` from the `ProjectionSource`, lays it
   out with the seeded, deterministic `LayoutFitter`, and stores the
   `ProjectionFrame` (see Projections below). `AlertRuleEval`s turn envelopes into
   `AlertDraft`s, which `AlertTriage` opens or deduplicates
   (`TriageOutcome`), or drops as `RuleInactive` when the rule stopped
   evaluating, and suppresses on sanctioning, rule disabling or
   `TransmissionDismissed`. Every stored change to an alert bumps its
   `AlertRevision` and publishes `AlertChanged`. Operators manage rules
   through `AlertRuleStore`: create and update content rules from a
   `RuleRequest` (a watched-topic request must name the current topic-model
   version; a semantic query's text is embedded), and enable or disable any
   rule (`RuleStatus`). A rule keeps its kind; updating a stale watched-topic
   rule is the only way it becomes current.
8. **L7 topology.** The `EdgeStore` applies each `EdgeContribution` to its
   `EdgeKey` bucket (per topic-model version, `BucketWidth` wide), activates
   a version once it is complete and publishes `TopicVersionActivated`, and
   answers `TopologyGraph` queries over canonical agents with per-edge
   `Share`s. A series query takes a `SeriesGrid` (a bucket-aligned window
   cut into `SeriesStep`s, each a whole number of buckets), a `Weighting`, a
   `SeriesGrouping` (total, topic, route kind, edge) and a `TopologyFilter`,
   and returns a `TopologySeries`: one value per step per group, counted
   exactly as a graph over that step. Summing every value gives
   `TopologyGraph::total` for the same window, weighting, filter and topic
   version, and grouped by edge each series sums to that edge's stat.
   `EdgeStore::transmissions` lists the contributions behind one edge
   (`EdgeSelector`) from the same stored rows, a page at a time
   (`EdgeTransmissionPage`), with the first page's topic version pinned in
   the cursor. Graph, series and edge-transmission queries fail with
   `EdgeQueryError`; `apply` and `activate` with `EdgeError`.
9. **L8 surface.** `QueryApi` serves channels, policy histories, agents,
   alert rules, dead letters, alerts, the topology, series, the topic
   history (versions, sizes, lineage), the transmissions behind an edge,
   search, transmissions, topics, projection jobs and projections and the
   audit log to an authenticated `Caller` with `Permission`s (View for
   structure, Content for anything derived from message text, projections
   included, Operate for dead letters, Audit for the audit log). Every
   query fails with a typed `QueryError` (see Errors below). Series and the topic history need `View`: they carry
   ids, counts, times and similarities but no text, and topic labels stay
   behind `Content`. `OperatorActions::act` checks
   `OperatorAction::required_permission` before any effect, then publishes
   `PolicyChanged` or forwards the action down the stack: merges, unmerges
   and labels to L3, channel promotion and transmission dismissal to L5,
   alert rule management to L6; acknowledging and resolving an alert
   publishes `AlertChanged`. It stamps every author and time from the
   caller and returns an `ActionEffect`. `AlertSink`s deliver alerts.
   - **Audit log.** Every `act` call leaves one `AuditRecord` (the caller,
     the `OperatorAction` value, the time and an `AuditOutcome`: `Applied`,
     `Unchanged`, `Rejected(Rejection)` or `Forbidden`). `Applied` and
     `Unchanged` records are written in the action's transaction. The
     `AuditLog` is append-only; `QueryApi::audit` reads it as a list (below)
     with an `AuditFilter` and needs `Permission::Audit`.
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

### Lists and pagination

Channels, agents, alert rules, alerts, dead letters, edge transmissions,
search hits, a version's topics, projection jobs and the audit log are read
a `Page` at a time. A `PageRequest<L>` holds a `PageSize` (1 to 500) and,
after the first page, the `Cursor<L>` from the previous page. `L` is a
marker per list (`ChannelList`, `AgentList`, `AlertRuleList`, `AlertList`,
`DeadLetterList`, `EdgeTransmissionList`, `SearchList`, `TopicList`,
`ProjectionList`, `AuditList`), so a cursor only fits its own list. Each
list is ordered by a unique sort key that never changes, descending (ids,
`(Confirmed::at, TransmissionId)` for an edge, `(AuditRecord::at, AuditId)`
for the audit log, `(score, TransmissionId)` for search, whose score is a
fixed function of query, model and transmission), and the cursor holds the
last key served (keyset pagination), so concurrent inserts and removals
never make a traversal skip or repeat an item. The cursor also holds a
digest of the request and a MAC; one presented with another request is
`InvalidCursor`. A cursor whose pinned topic version or embedding model is
gone fails with that typed reason instead (`VersionNotRetained`,
`Conflict(EmbeddingModelChanged)`). Whole values with their own invariants
(a policy history, the version history, topic sizes, a lineage, a graph, a
series) are not paged. A page with a next cursor is never empty, so following
cursors always ends. List filters (`ChannelFilter`, `AgentFilter`,
`AlertRuleFilter`, in `l8_surface/lists.rs`, and `AuditFilter`) are defined
by their `matches` methods; empty lists do not restrict. `AlertRuleFilter`
selects on the operator-set `RuleStatus` and, separately, on staleness, so
a stale-rule list includes disabled stale rules.

### Linked views

`topology`, `series`, `search`, `edge_transmissions` and `fit_projection`
take the same `TopologyFilter` (`aggregates/filter.rs`, re-exported from
`aggregates::edge`). Each view reduces a confirmed transmission to a
`FilterSubject` (canonical sender and reader when the view is computed,
route, topic under the resolved version, latest verdict) and keeps it when
`TopologyFilter::admits` holds:

| Field | Admits a transmission when |
| --- | --- |
| `agents` | the canonical sender or reader equals the canonical form of a listed agent |
| `channels` | its route is `Channel(c)` with `c` listed; other routes never match |
| `route_kinds` | `RouteKind::from(route)` is listed |
| `topics` | its topic under the resolved version is listed; outliers and unclassified transmissions never match |
| `false_detections` | `Include`, or `Exclude` and its latest verdict is not `FalseDetection` |

Empty lists do not restrict and non-empty fields combine with AND. The
window is separate and always tested against `Confirmed::at`. For the graph
and series the subject is each transmission counted into an edge, for
search each hit, for a projection each sampled point (at fit time), and for
the drill-down each row.

### Topic versions

Every linked view is computed under one concrete topic-model version and
reports it. The filter's `topic_version` (`TopicVersionSelector`) picks it:
`Current` is the `TopicCatalog`'s active version when the view is computed,
and `Pinned(v)` is `v`. The store serving the view resolves it with
`TopicVersionSelector::resolve` against the catalog's history and its own
retention:

| `Pinned(v)` where `v` is | Result |
| --- | --- |
| active, or superseded after being active and still retained | `v` |
| superseded after being active, no longer retained | `VersionNotRetained { version }` |
| ready, or superseded without ever being active | `Conflict(TopicVersionNotActivated)` |
| fitting | `Conflict(TopicVersionFitting)` |
| unknown | `NotFound` |

A paged view resolves on its first page and its cursor pins the result;
later pages use it whatever `Current` now is, and fail with
`VersionNotRetained` if its data is dropped meanwhile. A non-empty `topics`
list must name topics of the resolved version only
(`TopologyFilter::topics_outside`), otherwise the view fails with
`Conflict(TopicsNotInVersion { version, topics })`, so a filter built from
an older version is refused instead of silently matching nothing. To link
views, a client takes the version the first response reports and pins it in
every other request. `QueryApi::topics` takes the same selector but, being a
catalog read, also accepts a ready version that was never activated.

### Errors

Every query returns `QueryError`: `Store` (retry may succeed), `NotFound`,
`Forbidden { missing }`, `VersionNotRetained`, `Conflict(ConflictKind)`
(the state does not allow the request), `InvalidInput(InputError)` (invalid
whatever the state), `InvalidCursor`, `ProjectionNotRetained`. Operator
actions return the subset `ActionError`. How each store error becomes a
`QueryError` is defined once, by the `From` impls in
`l8_surface/query_errors.rs`, for `EdgeQueryError`, `SearchError`,
`CatalogError`, `ProjectionStoreError`, `EmbedError` (embedding a search's
text), `VersionUnavailable` and `BusError` (the dead-letter list). No query
error is a free-text classification: `Store`'s reason is diagnostic only.

### Projections

UMAP is randomized and depends on its sample, so projections are fitted
once, stored and read back exactly; a cited view always reproduces.

1. `QueryApi::fit_projection(caller, window, filter, params)` needs
   Content. It resolves the filter's version (errors as for any linked
   view), builds a `ProjectionSpec` (window, filter pinned to that version,
   `ProjectionParams`, current `EmbeddingModel`), records a queued
   `ProjectionInfo` (`ProjectionStore::enqueue`, refused with
   `Conflict(ProjectionQueueFull)` beyond 16 pending jobs) and returns its
   `ProjectionId` at once. `ProjectionParams` (checked) holds the sample size
   (`ProjectionLimit`, 1 to 100,000), UMAP's neighbours (2 to 200) and
   minimum distance (in thousandths, 0 to 1,000, so it serializes exactly)
   and the seed.
2. A fitter claims the oldest queued job (`Queued` to `Fitting`, under a
   lease). `ProjectionSource::sample` reads every transmission confirmed in
   the window that the pinned filter admits (agents resolved at that
   moment) and that has an embedding from the spec's model; that count is
   `matching`. It keeps the `limit` with the smallest sample key, a BLAKE3
   keyed by the seed over the transmission id, in ascending key order, and
   records the current `Watermark`. `LayoutFitter::fit` lays them out, a
   pure function of embeddings, order and params. The fitter builds the
   frame with `ProjectionFrame::from_points` and `complete` stores it with
   the `Ready(Fitted)` status in one transaction. Deterministic problems
   (`FitFailure`: too few points, version or model dropped, a non-finite
   layout) make the job `Failed`; anything else leaves it to be requeued
   when its lease lapses.
3. `projection_status` and `projections` (paged) report jobs.
   `projection(caller, id)` returns a `Projection`: the ready job and its
   frame, identical on every read. Queued or fitting is
   `Conflict(ProjectionNotReady)`, failed is `Conflict(ProjectionFailed)`,
   expired is `ProjectionNotRetained`, unknown is `NotFound`.
4. Frames are kept for `projection.frame_retention_days` (default 180)
   after fitting, then dropped (`Expired`); the job record and its spec are
   kept, so a citation still says exactly what was fitted and it can be
   fitted again with the same seed. The catalog keeps every version's
   topics, so a frame's topic ids always resolve to labels.

`ProjectionInfo` (checked) keeps its timestamps in order, a fit's watermark
no later than its start and exactly `min(matching, limit)` points; its
transitions (`start`, `requeue`, `complete`, `fail`, `expire`) refuse moves
outside the lifecycle. `Projection::new` checks that the frame's header
agrees with the ready job.

Points are frozen at fit time: canonical agents, route kind, topic under
the pinned version and `Confirmed::at` as they were when the sample was
read. For two fits with the same seed and sample size, a point sampled by
the wider one is sampled by any narrower one that still admits it.

**Frame.** A `ProjectionFrame` is the projection as columns: transmission
ids, confirmation times, packed `f32` x/y pairs, and `u32` indices into
tables of senders, readers, route kinds and topics (`OUTLIER` for an
outlier). `ProjectionFrame::new` checks that every column has one entry per
point, the count is `min(matching, limit)`, indices are in range, tables
are distinct and in order of first use (so equal points give equal bytes),
no transmission repeats and coordinates are finite. The binary layout
(format 1, little-endian) is a 64-byte header (magic `XTPF`, format, table
lengths, projection id, topic version, count, sample size, watermark,
matching) followed by the id sections, the `u64` times, the coordinates,
the four index columns and the route kind bytes, padded to 8 bytes, with
every section aligned for typed-array views; the full table is in
`aggregates/projection/frame.rs`. `encode` writes it and `decode` accepts
exactly what `encode` can produce.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `spec/Cargo.toml` | Builds the spec as a library so it type-checks and its tests run | crate `crosstalk-spec` |
| `spec/types/mod.rs` | Crate root, tier overview | — |
| `spec/types/ids.rs` | Typed ids | `AgentId`, `ExchangeId`, `SpanId`, `ChannelId`, `TransmissionId`, … `AuditId`, `MessageHash`, `PromptHash`, `CredentialHash`, `AccountHash` |
| `spec/types/support.rs` | Shared building blocks | `NonEmpty`, `NonBlank`, `Timestamp`, `TimeWindow`, `ByteRange`, `Blake3`, `Similarity`, `Share` |
| `spec/types/paging.rs` | Cursor pagination for list queries | `PageSize`, `Cursor`, `PageRequest`, `Page`, `PageOverflow`, `ChannelList`, `AgentList`, `AlertRuleList`, `AlertList`, `DeadLetterList`, `EdgeTransmissionList`, `SearchList`, `TopicList`, `ProjectionList`, `AuditList` |
| `spec/types/observed/client.rs` | Ingress, upstream, credential and harness facts | `IngressMode`, `Upstream`, `UpstreamKind`, `Dialect`, `CredentialScheme`, `CredentialRef`, `HarnessClaim`, `HarnessIds`, `RequestClass`, `ClientContext`, `EndpointKind` |
| `spec/types/observed/message.rs` | Canonical messages | `Message`, `MessageBody`, `Role`, `AssistantPart`, `UserPart`, `ToolCall`, `ToolArguments`, `CanonicalJson`, `ToolResult`, `Unknown`, `PartRef` |
| `spec/types/observed/exchange.rs` | Exchanges and their pipeline stage | `Exchange`, `ExchangeMeta`, `WireProtocol`, `Transport`, `Continuation`, `ResponseId`, `ExchangeOutcome`, `ExchangeFailure`, `ExchangeStage` |
| `spec/types/observed/agent.rs` | Agent identity, merge records and exact unmerge | `Agent`, `IdentityEvidence`, `IdentityScope`, `Strength`, `AgentState`, `Merged`, `MergeableState`, `MergeRequest`, `MergeAuthor` |
| `spec/types/observed/agent/label.rs` | Display labels | `AgentLabel`, `Labeled`, `LabelChange`, `LabelLog`, `LabelView`, `PastLabel` |
| `spec/types/observed/conversation.rs` | Threaded conversations | `Conversation`, `ConversationOrigin` |
| `spec/types/derived/provenance/span.rs` | Spans and their lifecycle | `Span`, `SpanLocation`, `Origin`, `RelaySource`, `SpanState`, `SpanEvent`, `OriginatedSpan` |
| `spec/types/derived/provenance/fingerprint.rs` | Fingerprints and index hits | `Fingerprint`, `WinnowParams`, `PositionedFingerprint`, `FingerprintHit` |
| `spec/types/derived/provenance/matching.rs` | Content matches | `ContentMatch`, `MatchKind`, `Codec`, `Carrier`, `InvalidMatch` |
| `spec/types/derived/flow/resource.rs` | Resources and patterns | `Resource`, `Locator`, `ResourcePattern`, `Host` |
| `spec/types/derived/flow/access.rs` | Accesses | `Access`, `AccessOp`, `AccessKind`, `Extraction` |
| `spec/types/derived/flow/evidence.rs` | Communication evidence | `Evidence`, `CoAccess`, `InvalidCoAccess` |
| `spec/types/derived/flow/transmission.rs` | Transmissions and their lifecycle | `Transmission`, `Route`, `DelegationDirection`, `DirectCarrier`, `TransmissionState` (`dismiss`, `expire`), `DiscardReason`, `Dismissal`, `Confirmed`, `Classification` |
| `spec/types/derived/flow/channel/mod.rs` | Channels and promotion | `Channel`, `ChannelOrigin` (`promoted`), `Declaration`, `DeclaredHistory`, `Seed` |
| `spec/types/derived/flow/channel/detection.rs` | Channel detection lifecycle | `DeclaredDetection`, `TrafficDetection` |
| `spec/types/derived/flow/channel/policy.rs` | Channel policy, its history and traffic routing | `Policy`, `Decision`, `PolicyAuthor`, `PolicyKind`, `PolicyDecision` (checked from `Policy`), `PolicyHistory` (checked), `Recorded`, `TrafficVerdict` |
| `spec/types/aggregates/edge.rs` | Topology edges and their drill-down | `EdgeKey`, `TopicSlot`, `EdgeStats`, `Edge`, `Weighting`, `RouteKind`, `TopologyGraph`, `EdgeSelector`, `EdgeTransmission`, `EdgeTransmissionPage`; re-exports `TopologyFilter` |
| `spec/types/aggregates/filter.rs` | The filter shared by every linked view, and topic-version resolution | `TopologyFilter` (`admits`, `topics_outside`, `pinned`), `FilterSubject`, `TopicVersionSelector` (`resolve`), `VersionUnavailable`, `FalseDetections` |
| `spec/types/aggregates/projection/mod.rs` | Stored projection jobs | `ProjectionLimit`, `ProjectionParams` (checked), `ProjectionSpec`, `FitFailure`, `Fitted`, `ProjectionStatus`, `ProjectionInfo` (checked, with transitions), `ProjectedPoint`, `Projection` (checked) |
| `spec/types/aggregates/projection/frame.rs` | The columnar projection frame and its binary layout | `ProjectionFrame` (checked; `from_points`, `encode`, `decode`), `FrameHeader`, `FrameTables`, `FrameColumns`, `InvalidFrame`, `FrameDecodeError`, `MAGIC`, `FORMAT`, `OUTLIER` |
| `spec/types/aggregates/series.rs` | Time series over the edge table | `BucketWidth`, `SeriesStep`, `SeriesGrid`, `SeriesGrouping`, `SeriesEdge`, `Series`, `SeriesGroups`, `TopologySeries`, `TopologyGraph::total`, `Weighting::stat`, `RouteKind::of` |
| `spec/types/aggregates/topic.rs` | Embeddings and topics | `Embedding`, `EmbeddingModel`, `Topic`, `TopicModelVersion`, `TopicAssignment`, `Assignment` |
| `spec/types/aggregates/topic_history.rs` | Topic-model versions, sizes and lineage | `TopicVersionStatus`, `CompletedFit`, `FitRecord`, `TopicVersionInfo`, `TopicVersionHistory`, `TopicSize`, `TopicSizes`, `LineageLink`, `LineageEntry`, `TopicLineage` (`remap` to a `TopicWatch`), `RemapError` |
| `spec/types/aggregates/alert.rs` | Alert rules and alerts | `AlertRule`, `AlertRuleKind`, `TopicWatch`, `WatchedTopics`, `ContentRule`, `AlertRuleDef` (`evaluates`, `update`), `RuleStatus`, `AlertDraft`, `TriageOutcome`, `Alert`, `AlertState`, `SuppressReason`, `AlertRevision` |
| `spec/types/events/mod.rs` | Bus envelope and subjects | `Envelope`, `BusEvent`, `Subject` |
| `spec/types/events/{ingest,detect,insight}.rs` | Events by producing layer | `IngestEvent` (including `AgentUnmerged`), `ConversationDelta`, `DetectEvent` (including `TransmissionDismissed`), `InsightEvent` (including `AlertChanged`, `TopicVersionActivated`) |
| `spec/types/interfaces/l0_ingress.rs` … `l8_surface.rs` | One module per layer | the traits listed in the data flow above, and their error enums: `IdentityResolver::unmerge` and `set_label` (L3); `ChannelRegistry::set_policy`, `policy_history` and `promote`, `TransmissionReview`, `DismissError` (L5); `TopicCatalog` (`topics` paged), `SearchIndex` (paged), `ProjectionStore`, `ProjectionSource`, `LayoutFitter`, `Sample`, `SearchError`, `ProjectionStoreError`, `ProjectionJobError`, `AlertRuleStore`, `RuleRequest`, `RuleError` (L6); `EdgeStore::series` and `EdgeStore::transmissions`, `EdgeQueryError` (L7); the list, series, topic-history, policy-history and audit queries on `QueryApi`, and `Caller`, `Permission`, `OperatorAction` (`required_permission`, `kind`), `ActionKind` (L8) |
| `spec/types/interfaces/l8_surface/lists.rs` | Surface list filters, the search request and the topic page | `ChannelFilter`, `AgentFilter`, `AgentStateKind`, `AlertRuleFilter`, `SearchRequest`, `SearchMode`, `TopicPage` |
| `spec/types/interfaces/l8_surface/query_errors.rs` | How each store error becomes a `QueryError` | `From` impls for `VersionUnavailable`, `EdgeQueryError`, `SearchError`, `EmbedError`, `CatalogError`, `ProjectionStoreError`, `BusError` |
| `spec/types/interfaces/l8_surface/live.rs` | The live feed (SSE) | `LiveFeed`, `LiveStream`, `LiveUpdate`, `LiveUpdateKind`, `UpdateKinds` (checked), `ChannelChange`, `LiveScope`, `ScopeKeys`, `LiveCursor`, `FeedEpoch`, `Resume`, `FeedWindow` (checked), `ResumePlan`, `ResyncReason`, `LiveItem`, `LiveEnd`, `LiveSubscription`, `LiveConfig` (checked) |
| `spec/types/interfaces/l8_surface/audit.rs` | The audit log | `AuditLog`, `AuditRecord` (checked), `AuditOutcome`, `OutcomeKind`, `Rejection`, `ActionEffect`, `AuditFilter`, `AuditError` |
| `spec/types/tests/` | Invariant tests (`series.rs`, `topic_history.rs` for the series and topic history; `filter.rs`, `paging.rs`, `projection.rs`, `projection_frame.rs`, `topic_version.rs`, `query_errors.rs` for the query surface; `live.rs`, `audit.rs`, `policy.rs` for the live feed, audit log and policy history; `agents.rs` holds a reference merge table for exact unmerge; `surface.rs` for operator actions) | — |
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
- An unmerge restores exactly: the agent returns to its `Merged::prior`
  (always Provisional or Established), every agent repointed through it
  points at it again unless it was unmerged or merged afresh since, and one
  `AgentUnmerged` lists them. Merge then unmerge is the identity on the
  merge table and on every topology graph.
- Labels are display only: never identity evidence, set on the canonical
  agent, untouched by merges and unmerges. An `AgentLabel` is trimmed,
  non-empty, at most 64 characters and free of control characters, and a
  `LabelLog` is in time order.
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
- A channel declared before traffic has `DeclaredDetection`; a discovered
  or promoted channel has `TrafficDetection`. So a declared, never-used
  channel is representable and a discovered or promoted, never-accessed one
  is not. Promotion keeps the channel's id, resources, policy and detection;
  its pattern matches its seed and overlaps no other declared pattern.
- Only a suspected transmission can be discarded, by expiry or by an
  operator's dismissal; a dismissal and a late match on it are ordered in
  the correlator shard, so exactly one applies.
- Confirmed traffic on a channel raises an alert unless its policy is
  sanctioned (`Policy::on_traffic`).
- Every policy decision, config or operator, is kept in the channel's
  `PolicyHistory`, ordered by decision time with no duplicates
  (`PolicyHistory::from_entries`, `PolicyHistory::record`); the channel's
  policy is always `PolicyHistory::current`, so the latest decision by time
  wins whatever order events arrive in. A history entry is a
  `PolicyDecision`, which cannot be `Unreviewed(None)`.
- Every operator action call that returns `Ok` or an `ActionError` other
  than `Store` leaves exactly one `AuditRecord` whose outcome maps back to
  that result (`AuditOutcome::of`, `AuditOutcome::result`); a `Store` error
  leaves at most one. A record is
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
- Only watched-topic rules can be stale, and staleness is separate from
  the operator's enabled or disabled status. A rule evaluates only when
  enabled and current; updating is the only way out of stale; a rule keeps
  its kind; operators create and edit content rules only. Once a disable
  returns, the rule has no active alerts and triage opens none for it.
- Every operator action names one permission
  (`OperatorAction::required_permission`, one exhaustive match): Govern for
  identity, policy and alert rules; Triage for alerts and dismissals;
  Operate for the pipeline. View, Content and Audit are read permissions
  that no action needs. The surface stamps author and time from the
  caller, and `ActionKind` and the audit log cover every action.
- L7 publishes `TopicVersionActivated { version, previous }` exactly once
  per switch, only after `EdgeStore::activate` has switched graph and
  series queries, and never for a version older than the active one.
- In a `TopologyGraph`, edge shares sum to 1 unless there are no edges.
- Every linked view (graph, series, search, projection fit, edge
  drill-down) applies one `TopologyFilter` as `TopologyFilter::admits`
  defines, with agents resolved through merges when the view is computed
  and topics under one resolved version, which the response reports.
  `TopicVersionSelector::resolve` is the only resolution; a paged view's
  cursor pins its first page's version. A filter naming topics outside the
  resolved version is refused (`TopicsNotInVersion`), never answered empty.
- Every query error is a typed `QueryError`; each store error maps to
  exactly one variant through the `From` impls in `query_errors.rs`.
- List pages hold at most their `PageSize` (1 to 500) items; a page with a
  next cursor is non-empty (`Page::more`). Cursors are typed by list and
  bound to their request; keyset ordering on immutable unique keys keeps a
  traversal exactly-once under concurrent writes.
- An `EdgeSelector` is never a self-edge. A full drill-down of an edge lists
  exactly the transmissions the graph counts into it, under the topic
  version pinned by its first page.
- A projection is fitted once and stored: `projection(id)` returns the
  same frame on every read until its frame expires. Its spec records the
  window, the filter pinned to its version, the params (seed included) and
  the embedding model; its `Fitted` record the watermark and counts. A
  `ProjectionFrame` holds exactly `min(matching, limit)` points with a
  sample size of 1 to 100,000, consistent column lengths, in-range indices,
  canonical tables, no transmission twice and finite coordinates; `decode`
  accepts exactly what `encode` produces. `ProjectionInfo` timestamps never
  go backwards and transitions follow the job lifecycle.
- Lists of dead letters need Operate and the audit log needs Audit; edge
  drill-down rows carry no message content and need View.
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
- `TimeWindow` and `ByteRange` are never empty. `Similarity` and `Share` are
  never NaN or outside `0..=1`.
- Bus delivery is at least once. Consumers are idempotent on the envelope id
  and on entity ids.
- The spec has no dependencies; `cargo check` and `cargo test` on
  `spec/Cargo.toml` must stay clean.
