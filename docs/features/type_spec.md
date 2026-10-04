# Type specification

## Scope

- Every entity in the gateway's data model, its lifecycle states and the
  data known in each state.
- The events that cross the bus between layers.
- The trait each layer of the abstraction stack exposes, and its errors.
- Tests for invariants enforced by checked constructors.
- The query surface a frontend reads: paginated lists (the audit log
  included), the one filter that links the graph, search, projection and
  edge drill-down, the projection's points, time series, the topic-model
  version history and the channel policy history.
- Graph node metadata (labels, states, parents, harness claims, counts), the
  channel-centred (bipartite) topology with access edges, and per-resource
  use of a channel.
- Channel promotion with supersession, resolved as aliases at read time.
- The live update feed (SSE) and the append-only audit log.
- Operator actions, each with one required permission: policy, channel
  promotion (with its policy and supersession), agent merges, unmerges and
  labels, alert triage, transmission dismissal, alert rule management and
  dead-letter replay.

## Non-scope

- Implementations of any trait.
- Serialization formats, database schemas, wire encodings. Implementation
  crates add serde and sqlx on their copies of these types.
- Lifecycle simulation: `design/lifecycles/cascade.yaml`, outside the
  repository, models the same lifecycles for the stateviz simulator.
- How buckets become final (the watermark's definition) and the feed
  updates a promotion produces; graph responses only report the store's
  watermark and the bus carries `ChannelPromoted`.
- Undoing a promotion or a supersession.

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
   alias labels as history. For every exchange that carries a
   `HarnessClaim`, `ClaimStore::record` adds it with the exchange's start
   time to the attributed agent's `ClaimSet` (distinct claims, latest time
   kept, so redelivery and reordering change nothing). `ClaimStore::claims`
   reads a canonical agent's claims as `ClaimSet::union` over every agent
   resolving to it, so merges and unmerges change no stored claim.
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
   (`ChannelRegistry::promote` with a `Promotion`, see below): it keeps its
   id, resources and `TrafficDetection`, gains a non-overlapping pattern
   that must match its seed, its origin becomes `Declared` with
   `DeclaredHistory::Promoted` recording the seed, the operator's policy
   decision is recorded in its history, and every other discovered channel
   whose seed the pattern matches becomes `Superseded` by it. Lookups never
   return a superseded channel, so each `AccessRecorded { access, channel }`
   names a canonical channel. An operator can dismiss a suspected transmission
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
   lower id) and every other new topic at or above the lineage floor. Each
   fit also fits a 2-D layout; later transmissions are placed into it. The
   re-fit then re-classifies everything and publishes `TopicVersionReady`,
   and the version becomes `Ready`. On `TopicVersionReady`, every watched
   topic rule that is current on the predecessor is carried over with
   `TopicLineage::remap`, which yields its new `TopicWatch` (`Current` under
   the new version, or `Stale` naming the unmapped topics), so the UI's
   lineage and the rules cannot disagree; the rule's `RuleStatus` is left
   as the operator set it. `TopicVersionActivated { version, previous }`
   from L7 makes the version `Active` and every older one `Superseded`. `TopicSizes` count topic
   assignments per topic (outliers apart), optionally over a window. The
   `SearchIndex` and the `ProjectionIndex` take a `TopologyFilter` and
   report the topic-model version they evaluated topics under
   (`SearchResults`, `Projection`). `AlertRuleEval`s turn envelopes into
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
   the cursor. `EdgeStore::apply_access` counts each `AccessRecorded` into
   an `AccessEdge` bucket (agent, channel, op, bucket), idempotent on the
   access id, and `EdgeStore::channel_topology` answers the channel-centred
   graph (below). Every query resolves stored agents and channels through
   the two directories and fills graph nodes from the agent store, the
   claim store and the channel registry.
9. **L8 surface.** `QueryApi` serves channels, policy histories, agents,
   alert rules, dead letters, alerts, the topology, series, the topic
   history (versions, sizes, lineage), the transmissions behind an edge,
   search, transmissions, topics, projections and the audit log to an
   authenticated `Caller` with `Permission`s (View for structure, including
   the channel-centred topology and a channel's resources; Content
   for anything derived from message text, Operate for dead letters, Audit
   for the audit log). Series and the topic history need `View`: they carry
   ids, counts, times and similarities but no text, and topic labels stay
   behind `Content`. `OperatorActions::act` checks
   `OperatorAction::required_permission` before any effect, then publishes
   `PolicyChanged` or forwards the action down the stack: merges, unmerges
   and labels to L3, channel promotion (as a `Promotion` stamped with the
   caller and time) and transmission dismissal to L5,
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

Channels, agents, alert rules, dead letters, edge transmissions and the
audit log are read a `Page` at a time. A `PageRequest<L>` holds a
`PageSize` (1 to 500) and, after the first page, the `Cursor<L>` from the
previous page. `L` is a marker per list (`ChannelList`, `AgentList`,
`AlertRuleList`, `DeadLetterList`, `EdgeTransmissionList`, `AuditList`),
so a cursor only fits its own list. Each list is ordered newest first by a
unique sort key that never changes (ids, `(Confirmed::at, TransmissionId)`
for an edge, `(AuditRecord::at, AuditId)` for the audit log), and the
cursor holds the last key served (keyset pagination), so concurrent inserts
and removals never make a traversal skip or repeat an item. The cursor also
holds a digest of the request and a MAC; one presented with another request
is `InvalidCursor`. A page with a next cursor is never empty, so following
cursors always ends. List filters (`ChannelFilter`, `AgentFilter`,
`AlertRuleFilter`, in `l8_surface/lists.rs`, and `AuditFilter`) are defined
by their `matches` methods; empty lists do not restrict. `AlertRuleFilter`
selects on the operator-set `RuleStatus` and, separately, on staleness, so
a stale-rule list includes disabled stale rules.

### Linked views

`topology`, `search`, `projection` and `edge_transmissions` take the same
`TopologyFilter` (`aggregates/filter.rs`, re-exported from
`aggregates::edge`). Each view reduces a confirmed transmission to a
`FilterSubject` (canonical sender and reader at query time, route with its
channel resolved through supersession, topic
under the response's topic version) and keeps it when
`TopologyFilter::admits` holds:

| Field | Admits a transmission when |
| --- | --- |
| `agents` | the canonical sender or reader equals the canonical form of a listed agent |
| `channels` | its route is `Channel(c)` with `c` (canonical) the canonical form of a listed channel; other routes never match |
| `route_kinds` | `RouteKind::from(route)` is listed |
| `topics` | its topic under the response's version is listed; outliers and unclassified transmissions never match |

Empty lists do not restrict and non-empty fields combine with AND. The
window is separate and always tested against `Confirmed::at`. For the graph
the subject is each transmission counted into an edge, for search each hit,
for the projection each point, and for the drill-down each row. Every
response reports its topic-model version; a client links two responses only
when the versions agree, and a filter holding an old version's topic ids
matches nothing.

### Projection

`QueryApi::projection` takes a `ProjectionRequest` (window, filter,
`ProjectionLimit` of 1 to 50,000, and optionally the `ProjectionToken` of
points the client already holds) and returns a `Projection`. The token names
one fitted layout (topic version and revision); within a token a
transmission's coordinates and topic never change, and a request naming a
token that is no longer current gets `StaleProjection { current }` instead
of points. Each `ProjectedPoint` carries its canonical sender and reader,
`RouteKind`, topic (its slot under the token's version, `Projection::slot`),
`Confirmed::at` and coordinates, so a client colours and links points
without lookups. When more transmissions match than the limit, the points
are the `limit` with the smallest keyed sample hash, so the sample is fixed
per token and survives narrowing. `Projection::new` checks that it holds
exactly `min(matching, limit)` points, none twice, all finite.

### Graph nodes

`TopologyGraph::nodes` and `BipartiteGraph::nodes` describe what the graph
draws, so the UI needs no lookup per node (`aggregates/node.rs`):

- `GraphNode::Agent(AgentNode { id, label, state_kind, parent, claims,
  transmissions_in, transmissions_out })`. `label` is the canonical agent's
  current display label, an `AgentLabel`. `state_kind` is a `CanonicalStateKind`
  (no `Merged`). `parent` is the canonical parent, never the agent itself.
  `claims` is the `ClaimSet` union over the agent's aliases, shown as
  claimed. The counts are the transmissions of the response's edges into
  and out of the agent.
- `GraphNode::Channel(ChannelNode { id, label, origin_kind, detection_kind,
  policy_kind, locator_summary })`, in the channel-centred view only.
  `origin_kind` is a `CanonicalOriginKind` (no superseded origin); `label`
  is `None` until channels carry display labels.

Which nodes appear: every edge endpoint (in the channel-centred view also
every access channel and transmission route channel) and every canonical
ancestor of an agent among them, each once and nothing else, so each
`parent` names a node in the same response. `TopologyGraph::check_nodes`
and `BipartiteGraph::new` check this and the counts; canonicity is checked
at query time.

### Channel-centred view

Splitting `Route::Channel` edges into A→C→B would show only writes someone
read, and the early stage of a hijacked wiki is writes nobody has read yet.
So accesses have their own aggregate (`aggregates/access.rs`): an
`AccessEdge { agent, channel, op, bucket, accesses }`, bucketed like
`EdgeKey` with no topic, maintained by L7 from `AccessRecorded`.
`QueryApi::channel_topology(caller, window, weighting, filter)` returns a
`BipartiteGraph { nodes, accesses, transmissions, watermark, topic_version }`:

- `accesses`: access buckets in the window, resolved to canonical agents and
  channels, kept by `TopologyFilter::admits_access`, summed per (agent,
  channel, op). Each share is its count over all access counts, normalized
  apart from transmissions and independent of the weighting.
- `transmissions`: exactly `topology`'s edges for the same window, weighting
  and filter.
- `watermark`: the edge store's watermark when the response was computed.

The filter on accesses (`admits_access`): agents and channels match the
access's canonical agent and channel; `route_kinds` admits accesses when it
lists `Channel`; an access has no topic, so `topics` keeps the accesses of
channels that carried, in the window, a channel-routed confirmed
transmission with a listed topic (after the `false_detections` setting).

`QueryApi::channel_resources(caller, channel, window, page)` pages a
channel's resources newest first (`ResourceUseList`, keyed by
`ResourceId`), each a `ResourceUse { resource, writers, readers }` with
canonical agents and their access counts in the window (merged aliases
summed, most accesses first). A superseded channel answers for its
superseding channel, named in the `ResourceUsePage`, whose resources include
those of every channel it superseded.

### Promotion and supersession

`OperatorAction::PromoteChannel { channel, pattern, policy, note }` (Govern)
becomes a `Promotion` (pattern and policy decision, both authored by the
caller at the accept time) and `ChannelRegistry::promote`, which follows
`promotion::plan`:

1. Refusals, in order: unknown channel (`NotFound`); superseded
   (`Conflict(ChannelSuperseded { channel, by })`); already declared
   (`Conflict(ChannelNotDiscovered)`); pattern misses the seed locator
   (`InvalidInput(PatternMissesSeed)`); pattern overlaps another declared
   pattern, by `ResourcePattern::overlaps` (`Conflict(PatternOverlaps)`).
2. In one transaction: the channel's origin is promoted (same id), the
   decision is recorded in its `PolicyHistory`, and every other discovered
   channel whose seed the pattern matches becomes
   `ChannelOrigin::Superseded { seed, detection, supersession: { by, at } }`.
3. One `DetectEvent::ChannelPromoted { channel, declaration, policy,
   superseded }` after commit; the action returns `ChannelPromoted(channel)`.

A superseded channel keeps its id, seed, resources, policy history and the
detection it had; accepts no new resources (lookups of its resources return
the superseding channel); and refuses promotion and policy changes
(`Conflict(ChannelSuperseded)`, checked through `ChannelDirectory` before
`PolicyChanged` is published). `ChannelDirectory::canonical` resolves it in
one step: superseding channels are declared, so never superseded. Routes
(`Route::resolved`), filters (`TopologyFilter::admits`), alert subjects
(`AlertSubject::resolved`), graph nodes, edges and access buckets all
resolve through it at read time. Alerts stay stored under the superseded id;
the alert inbox's channel filter and sanction suppression compare resolved
subjects, while deduplication compares stored ones. A transmission
confirmed on a superseded channel is judged by the superseding channel's
policy.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `spec/Cargo.toml` | Builds the spec as a library so it type-checks and its tests run | crate `crosstalk-spec` |
| `spec/types/mod.rs` | Crate root, tier overview | — |
| `spec/types/aliases.rs` | Read-time resolution of merged agents and superseded channels | `Aliases`, `Resolve`, `NoAliases` |
| `spec/types/ids.rs` | Typed ids | `AgentId`, `ExchangeId`, `SpanId`, `ChannelId`, `TransmissionId`, … `AuditId`, `MessageHash`, `PromptHash`, `CredentialHash`, `AccountHash` |
| `spec/types/support.rs` | Shared building blocks | `NonEmpty`, `NonBlank`, `Timestamp`, `TimeWindow`, `ByteRange`, `Blake3`, `Similarity`, `Share` |
| `spec/types/paging.rs` | Cursor pagination for list queries | `PageSize`, `Cursor`, `PageRequest`, `Page`, `PageOverflow`, `ChannelList`, `AgentList`, `AlertRuleList`, `DeadLetterList`, `EdgeTransmissionList`, `AuditList`, `ResourceUseList` |
| `spec/types/observed/client.rs` | Ingress, upstream, credential and harness facts | `IngressMode`, `Upstream`, `UpstreamKind`, `Dialect`, `CredentialScheme`, `CredentialRef`, `HarnessClaim`, `HarnessIds`, `RequestClass`, `ClientContext`, `EndpointKind` |
| `spec/types/observed/message.rs` | Canonical messages | `Message`, `MessageBody`, `Role`, `AssistantPart`, `UserPart`, `ToolCall`, `ToolArguments`, `CanonicalJson`, `ToolResult`, `Unknown`, `PartRef` |
| `spec/types/observed/exchange.rs` | Exchanges and their pipeline stage | `Exchange`, `ExchangeMeta`, `WireProtocol`, `Transport`, `Continuation`, `ResponseId`, `ExchangeOutcome`, `ExchangeFailure`, `ExchangeStage` |
| `spec/types/observed/agent.rs` | Agent identity, merge records and exact unmerge | `Agent`, `IdentityEvidence`, `IdentityScope`, `Strength`, `AgentState`, `Merged`, `MergeableState`, `MergeRequest`, `MergeAuthor` |
| `spec/types/observed/agent/claims.rs` | Harness claims seen per agent | `SeenClaim`, `ClaimSet` (checked; `observe`, `union`), `DuplicateClaim` |
| `spec/types/observed/agent/label.rs` | Display labels | `AgentLabel`, `Labeled`, `LabelChange`, `LabelLog`, `LabelView`, `PastLabel` |
| `spec/types/observed/conversation.rs` | Threaded conversations | `Conversation`, `ConversationOrigin` |
| `spec/types/derived/provenance/span.rs` | Spans and their lifecycle | `Span`, `SpanLocation`, `Origin`, `RelaySource`, `SpanState`, `SpanEvent`, `OriginatedSpan` |
| `spec/types/derived/provenance/fingerprint.rs` | Fingerprints and index hits | `Fingerprint`, `WinnowParams`, `PositionedFingerprint`, `FingerprintHit` |
| `spec/types/derived/provenance/matching.rs` | Content matches | `ContentMatch`, `MatchKind`, `Codec`, `Carrier`, `InvalidMatch` |
| `spec/types/derived/flow/resource.rs` | Resources and patterns | `Resource`, `Locator`, `ResourcePattern` (`matches`, `overlaps`), `Host` |
| `spec/types/derived/flow/access.rs` | Accesses | `Access`, `AccessOp`, `AccessKind`, `Extraction` |
| `spec/types/derived/flow/evidence.rs` | Communication evidence | `Evidence`, `CoAccess`, `InvalidCoAccess` |
| `spec/types/derived/flow/transmission.rs` | Transmissions and their lifecycle | `Transmission`, `Route` (`resolved`), `DelegationDirection`, `DirectCarrier`, `TransmissionState` (`dismiss`, `expire`), `DiscardReason`, `Dismissal`, `Confirmed`, `Classification` |
| `spec/types/derived/flow/channel/mod.rs` | Channels, promotion and supersession | `Channel` (`canonical`), `ChannelOrigin` (`promoted`, `superseded`, `seed`, `detection_kind`), `Supersession`, `NotPromotable`, `NotSupersedable`, `Declaration`, `DeclaredHistory`, `Seed` |
| `spec/types/derived/flow/channel/promotion.rs` | What a promotion does and refuses | `Promotion` (checked), `Registered`, `plan`, `PromotionPlan`, `PromotionRefusal` |
| `spec/types/derived/flow/channel/detection.rs` | Channel detection lifecycle | `DeclaredDetection`, `TrafficDetection`, `DetectionKind` |
| `spec/types/derived/flow/channel/policy.rs` | Channel policy, its history and traffic routing | `Policy`, `Decision`, `PolicyAuthor`, `PolicyKind`, `PolicyDecision` (checked from `Policy`), `PolicyHistory` (checked), `Recorded`, `TrafficVerdict` |
| `spec/types/aggregates/access.rs` | Access buckets, the channel-centred graph and resource use | `AccessEdge`, `WeightedAccess`, `BipartiteParts`, `BipartiteGraph` (checked), `InvalidBipartite`, `AgentAccesses`, `ResourceUse` (checked), `ResourceUsePage` |
| `spec/types/aggregates/node.rs` | Graph nodes | `GraphNode`, `NodeId`, `AgentNode`, `ChannelNode`, `CanonicalStateKind`, `CanonicalOriginKind`, `InvalidNodes`, `TopologyGraph::check_nodes` |
| `spec/types/aggregates/edge.rs` | Topology edges and their drill-down | `EdgeKey`, `TopicSlot`, `EdgeStats`, `Edge`, `Weighting`, `RouteKind`, `TopologyGraph` (with `nodes`), `EdgeSelector`, `EdgeTransmission`, `EdgeTransmissionPage`; re-exports `TopologyFilter` |
| `spec/types/aggregates/filter.rs` | The filter shared by every linked view | `TopologyFilter`, `FilterSubject`, `TopologyFilter::admits`, `AccessSubject`, `TopologyFilter::admits_access` |
| `spec/types/aggregates/projection.rs` | The 2-D projection of embeddings | `ProjectionToken`, `ProjectionLimit`, `ProjectedPoint`, `Projection`, `InvalidProjection` |
| `spec/types/aggregates/series.rs` | Time series over the edge table | `BucketWidth`, `SeriesStep`, `SeriesGrid`, `SeriesGrouping`, `SeriesEdge`, `Series`, `SeriesGroups`, `TopologySeries`, `TopologyGraph::total`, `Weighting::stat`, `RouteKind::of` |
| `spec/types/aggregates/topic.rs` | Embeddings and topics | `Embedding`, `EmbeddingModel`, `Topic`, `TopicModelVersion`, `TopicAssignment`, `Assignment` |
| `spec/types/aggregates/topic_history.rs` | Topic-model versions, sizes and lineage | `TopicVersionStatus`, `CompletedFit`, `FitRecord`, `TopicVersionInfo`, `TopicVersionHistory`, `TopicSize`, `TopicSizes`, `LineageLink`, `LineageEntry`, `TopicLineage` (`remap` to a `TopicWatch`), `RemapError` |
| `spec/types/aggregates/alert.rs` | Alert rules and alerts | `AlertRule`, `AlertRuleKind`, `TopicWatch`, `WatchedTopics`, `ContentRule`, `AlertRuleDef` (`evaluates`, `update`), `RuleStatus`, `AlertDraft`, `TriageOutcome`, `Alert`, `AlertState`, `SuppressReason`, `AlertRevision` |
| `spec/types/events/mod.rs` | Bus envelope and subjects | `Envelope`, `BusEvent`, `Subject` |
| `spec/types/events/{ingest,detect,insight}.rs` | Events by producing layer | `IngestEvent` (including `AgentUnmerged`), `ConversationDelta`, `DetectEvent` (including `TransmissionDismissed`), `InsightEvent` (including `AlertChanged`, `TopicVersionActivated`) |
| `spec/types/interfaces/l0_ingress.rs` … `l8_surface.rs` | One module per layer | the traits listed in the data flow above, and their error enums: `IdentityResolver::unmerge` and `set_label`, `ClaimStore` (L3); `ChannelDirectory`, `ChannelRegistry::set_policy`, `policy_history`, `promote` (`Promoted`, `PromoteError`) and `resource_use`, `TransmissionReview`, `DismissError` (L5); `TopicCatalog`, `ProjectionIndex`, `AlertRuleStore`, `RuleRequest`, `RuleError` (L6); `EdgeStore::series`, `EdgeStore::transmissions`, `EdgeStore::apply_access` (`AccessContribution`) and `EdgeStore::channel_topology` (L7); the list, series, topic-history, policy-history, channel-topology, channel-resources and audit queries on `QueryApi`, the error mappings from `PromotionRefusal`, `PromoteError` and `RegistryError`, and `Caller`, `Permission`, `OperatorAction` (`required_permission`, `kind`), `ActionKind` (L8) |
| `spec/types/interfaces/l8_surface/lists.rs` | Surface list filters and the projection request | `ChannelFilter`, `AgentFilter`, `AgentStateKind`, `AlertRuleFilter`, `ProjectionRequest` |
| `spec/types/interfaces/l8_surface/live.rs` | The live feed (SSE) | `LiveFeed`, `LiveStream`, `LiveUpdate`, `LiveUpdateKind`, `UpdateKinds` (checked), `ChannelChange`, `LiveScope`, `ScopeKeys`, `LiveCursor`, `FeedEpoch`, `Resume`, `FeedWindow` (checked), `ResumePlan`, `ResyncReason`, `LiveItem`, `LiveEnd`, `LiveSubscription`, `LiveConfig` (checked) |
| `spec/types/interfaces/l8_surface/audit.rs` | The audit log | `AuditLog`, `AuditRecord` (checked), `AuditOutcome`, `OutcomeKind`, `Rejection`, `ActionEffect`, `AuditFilter`, `AuditError` |
| `spec/types/tests/` | Invariant tests (`series.rs`, `topic_history.rs` for the series and topic history; `filter.rs`, `paging.rs`, `projection.rs` for the query surface; `live.rs`, `audit.rs`, `policy.rs` for the live feed, audit log and policy history; `agents.rs` holds a reference merge table for exact unmerge; `surface.rs` for operator actions; `channels.rs` for supersession, promotion planning, pattern overlap and read-time resolution; `graph.rs` for graph nodes, the channel-centred graph, resource use and harness claims) | — |
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
  is not. Promotion keeps the channel's id, resources and detection, records
  the operator's policy decision, and supersedes exactly the other
  discovered channels whose seed its pattern matches; its pattern matches
  its seed and overlaps no other declared pattern (`ResourcePattern::overlaps`
  is exact). A `Promotion`'s declaration and decision share one operator and
  time (`Promotion::new`).
- Only a discovered channel can be superseded (`ChannelOrigin::superseded`),
  and a superseded one cannot be promoted or take a policy decision.
  Resolution is one step: `canonical(canonical(c)) = canonical(c)`. Lookups
  never return a superseded channel. Routes, filters, alert subjects, graph
  nodes, edges and access buckets resolve through supersession at read
  time; nothing stored is rewritten.
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
- A graph's nodes are exactly its endpoints (and, in the channel-centred
  view, its access and route channels) plus the canonical ancestors of its
  agents, once each, every parent among them, with agent counts equal to the
  transmission edges' (`TopologyGraph::check_nodes`, `BipartiteGraph::new`).
  No node is a merged agent or a superseded channel.
- A `BipartiteGraph` has distinct access and transmission edges, no
  self-edge, and access and transmission shares each normalized on their
  own (`BipartiteGraph::new`). Its transmissions equal `topology`'s edges for
  the same arguments; its accesses include writes nobody read.
- A `ClaimSet` holds each claim once, newest first with a total tie order;
  observing is idempotent and order-independent, and a canonical agent's
  claims are the union over its aliases. A `ResourceUse` has a writer or
  reader and no agent twice per list (`ResourceUse::new`).
- Every linked view (graph, search, projection, edge drill-down) applies
  one `TopologyFilter` as `TopologyFilter::admits` defines, with agents
  resolved through merges at query time and topics under the version the
  response reports.
- List pages hold at most their `PageSize` (1 to 500) items; a page with a
  next cursor is non-empty (`Page::more`). Cursors are typed by list and
  bound to their request; keyset ordering on immutable unique keys keeps a
  traversal exactly-once under concurrent writes.
- An `EdgeSelector` is never a self-edge. A full drill-down of an edge lists
  exactly the transmissions the graph counts into it, under the topic
  version pinned by its first page.
- A `Projection` holds exactly `min(matching, limit)` points with a limit of
  1 to 50,000, no transmission twice and finite coordinates. Within one
  `ProjectionToken` points never move or change topic.
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
