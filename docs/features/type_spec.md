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
- Operator verdicts on transmissions (`Genuine`, `FalseDetection`), kept
  beside the detector's state as an append-only log, and the detection
  quality report that tallies them against the detector's calls.
- The live update feed (SSE): id-only `UiEvent`s telling the UI what to
  re-query, fed by `Changed` notifications from the stores.
- The append-only audit log of operator action calls and config changes,
  filtered by author, subject and time.
- The operator directory and how a request becomes a `Caller`, including
  trusted (single-user, no login) mode.
- Operator actions, each with one required permission: policy, channel
  promotion, agent merges, unmerges of one merge record, and renames, alert
  triage, transmission verdicts, alert rule management (built-in and user
  rules, and the sinks they deliver to), topic-version pins and dead-letter
  replay.

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
   nack each `Delivery`. Exhausted deliveries become `DeadLetter`s, which
   `DeadLetterStore::list` pages through and `replay` redelivers.
4. **L3 reconstruction.** The `IdentityResolver` gives a `Resolution`
   (known agent, new agent, or conflict) from the most specific
   `IdentityEvidence`, with harness ids scoped by `IdentityScope`; it also
   applies `MergeRequest`s, and the `AgentDirectory` resolves merged ids.
   Merges are a log (`observed::agent::merge`): each merge appends an
   immutable `MergeRecord { id, from, into, by, at, repointed }` and returns
   it. Both agents must be canonical; the source becomes
   `AgentState::Merged(MergedInto)` holding the record's id and its prior
   `ActiveAgentState` (Registered, Provisional or Established), and every
   agent merged into the source is repointed to the target and listed in
   `repointed`. `AgentMerged` is published. An operator unmerge
   (`IdentityResolver::unmerge`) names one `MergeId` and reverts exactly
   that record: the source returns to its prior state, each repointed agent
   that nothing moved since points at the source again (`Agent::restore`,
   which forgets the repoints after it), the record is marked reverted
   (a second revert is refused), a `MergeVeto` between source and target is
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
   (`ChannelRegistry::promote`): it keeps its id, resources, policy and
   `TrafficDetection`, gains a non-overlapping pattern that must match its
   seed, and its origin becomes `Declared` with `DeclaredHistory::Promoted`
   recording the seed. A suspected transmission is discarded only when its
   window expires (`TransmissionState::expire`).
   An operator verdict (`TransmissionVerdicts::set`) is appended to the
   transmission's `VerdictLog` without touching its state, and
   `VerdictSet` is published (see Verdicts below).
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
   `AlertRuleDef::remap` (`TopicLineage::remap`), which yields its new
   `TopicWatch` (`Current` under the new version, or `Stale` naming the
   unmapped topics), so the UI's lineage and the rules cannot disagree; the
   rule's `RuleStatus` is left as the operator set it. When `alerts` starts
   with an embedder whose model differs from a current semantic rule's, the
   rule becomes `QueryWatch::Stale` (`AlertRuleDef::embedding_model_changed`). `TopicVersionActivated { version, previous }`
   from L7 makes the version `Active` and every older one `Superseded`;
   the catalog then enforces its `RetentionPolicy` (see "Retention and
   watermarks" below), as it does after an unpin and on start. `TopicSizes` count topic
   assignments per topic (outliers apart), optionally over a window. The
   `SearchIndex` and the `ProjectionIndex` take a `TopologyFilter` and
   report the topic-model version they evaluated topics under
   (`SearchResults`, `Projection`). `AlertRuleEval`s turn envelopes into
   `AlertDraft`s, which `AlertTriage` opens or deduplicates
   (`TriageOutcome`), or drops as `RuleInactive` when the rule stopped
   evaluating, and suppresses on sanctioning or rule disabling, and on a
   `VerdictSet` holding `FalseDetection` suppresses every active alert
   about that transmission (`SuppressReason::OperatorRejected`), after which
   triage opens nothing about it (`TriageOutcome::OperatorRejected`) while
   that verdict is current. Every stored
   change to an alert bumps its `AlertRevision` and publishes
   `AlertChanged`. Rules live in an `AlertRuleSet`: the five `BuiltinRule`s,
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
   enables or disables any rule. A rule keeps its kind and is never
   deleted. Staleness (`TopicWatch::Stale`, `QueryWatch::Stale`, reported as
   a `StaleReason`) is separate from status and no operator action sets it.
   Every stored change to a rule bumps its `RuleRevision` and publishes
   `AlertRuleChanged`.
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
   the cursor. Buckets hold detector output only; the store keeps a copy
   of current verdicts (`EdgeStore::judge`) and subtracts false detections
   at query time when the filter excludes them. Activation deletes nothing;
   a version's buckets go only on `TopicVersionDropped`
   (`EdgeStore::drop_version`). The topology consumer recomputes the
   watermark from a `FrontierSource` at least once per bucket width
   (`EdgeStore::advance_watermark`), publishes `WatermarkAdvanced` on each
   strict advance, and refuses a contribution into a final bucket
   (`LateContribution`). Graph, series and drill-down results come back
   `Watermarked`.
9. **L8 surface.** `QueryApi` serves channels, policy histories, agents,
   alert rules, dead letters, alerts, the topology, series, the topic
   history (versions, sizes, lineage), the transmissions behind an edge,
   search, transmissions, topics, projections, verdict logs, detection
   quality and the audit log to an
   authenticated `Caller` with `Permission`s (View for structure, Content
   for anything derived from message text, Operate for dead letters, Audit
   for the audit log). `topology`, `series`, `edge_transmissions` and
   `topic_sizes` return `Watermarked` results, the watermark read from L7
   before the data, and `watermark` returns the current one. Series and the topic history need `View`: they carry
   ids, counts, times and similarities but no text, and topic labels stay
   behind `Content`. `OperatorActions::act` checks
   `OperatorAction::required_permission` before any effect, then publishes
   `PolicyChanged` or forwards the action down the stack: merges
   (`ActionOutcome::Merged(MergeId)`), unmerges and renames to L3, channel
   promotion and verdicts to L5, rule creation
   (`ActionOutcome::RuleCreated`), updates and enabling, and topic-version
   pins (`PinTopicVersion`, `UnpinTopicVersion`, Govern) to L6;
   acknowledging and resolving an alert publishes `AlertChanged` and
   `Changed::Alert`. It stamps every author and time from the caller and
   returns an `ActionOutcome`. `AlertSink`s deliver each alert to the sinks
   its rule lists, and `QueryApi::sinks` (Govern) reports each configured
   sink as a `SinkInfo` with its last delivery.
   - **Callers and the operator directory.** Config's `AccessConfig` is
     either `Trusted(TrustedOperator)` (one operator, every permission, no
     login) or `Authenticated(operators)`. On each load
     `OperatorDirectory::load(previous, config)` returns the new directory
     and the `ConfigChange`s that produced it (`SetAccessMode` first when the
     mode changed, then `SetOperator`/`RemoveOperator` by id; nothing for an
     unchanged config). An operator config drops stays listed with no
     permissions, so its name still labels history. Each request's verified
     session becomes a `RequestIdentity`, and `OperatorDirectory::caller`
     builds its `Caller`: always the trusted operator with
     `PermissionSet::ALL` in trusted mode; otherwise the named operator with
     its configured permissions, or `Unauthenticated`. `Caller`'s fields are
     private, so nothing else can build one. `QueryApi::operators` (View)
     returns the directory, former operators included.
   - **Audit log.** An `AuditEntry { id, at, body }` is either
     `AuditBody::Operator(OperatorRecord)` (the `Caller`, the
     `OperatorAction` and an `AuditOutcome`: `Succeeded(ActionOutcome)`,
     `Rejected(Rejection)` or `Forbidden { missing }`, the exact inverse of
     `act`'s result) or `AuditBody::Config(ConfigRecord)` (the loaded
     config's `ConfigHash`, a typed `ConfigChange` and a `ConfigOutcome`).
     `AuditEntry::by` derives the author (`Config` or `Operator(id)`) from
     the body, so a config change never poses as an operator action.
     `AuditEntry::subjects` lists the entities touched: the ids the action or
     change names (`OperatorAction::subjects`, `ConfigChange::subjects`) and
     any id the outcome created (`ActionOutcome::subject`, e.g. a
     `MergeId`). Operator entries are written in the action's transaction;
     config entries in the change's transaction, and only for real changes.
     The `AuditLog` is append-only; `QueryApi::audit(caller,
     AuditFilter { by, subject, window }, page)` reads it as a list (below)
     and needs `Permission::Audit`.
   - **Live feed.** Every store whose entities a query returns publishes
     `BusEvent::Changed` after each committed change: `Agent` (L3), `Channel`
     (L5, including recorded policy decisions, new resources, dormancy and
     config declarations), `Alert`, `Rule`, `TopicVersion` and `Projection`
     (L6, and L8 for acknowledge and resolve), `Watermark` (L7). The feed
     writer (consumer group `live`) appends `UiEvent::from` each one to the
     feed log, numbered per `FeedEpoch`, before acking. A `UiEvent`
     (`AlertChanged`, `ChannelChanged`, `AgentChanged`, `RuleChanged`,
     `Watermark`, `TopicVersionReady`, `ProjectionReady`) carries an id only;
     the UI re-queries. `LiveFeed::subscribe(caller, resume)` needs View;
     each stream passes over events the caller may not receive
     (`UiEvent::visible_to`: `ProjectionReady` needs Content). There is no
     server-side filter: the UI drops ids it is not showing.
     `FeedWindow::resume` decides between replaying from the `Last-Event-ID`
     cursor and a `LiveItem::Resync` (re-query everything). Streams send
     heartbeats carrying their newest cursor, end with `LiveEnd::Lagged`
     when their bounded buffer fills, so a slow client never blocks the feed
     or other clients, and end with `SessionEnded` when the session ends or
     a config load changes the operator.

### Lists and pagination

Channels, agents, alert rules, dead letters, edge transmissions and the
audit log are read a `Page` at a time. A `PageRequest<L>` holds a
`PageSize` (1 to 500) and, after the first page, the `Cursor<L>` from the
previous page. `L` is a marker per list (`ChannelList`, `AgentList`,
`AlertRuleList`, `DeadLetterList`, `EdgeTransmissionList`, `AuditList`),
so a cursor only fits its own list. Each list is ordered newest first by a
unique sort key that never changes (ids, `(Confirmed::at, TransmissionId)`
for an edge, `(AuditEntry::at, AuditId)` for the audit log), and the
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
`FilterSubject` (canonical sender and reader at query time, route, topic
under the response's topic version) and keeps it when
`TopologyFilter::admits` holds:

| Field | Admits a transmission when |
| --- | --- |
| `agents` | the canonical sender or reader equals the canonical form of a listed agent |
| `channels` | its route is `Channel(c)` with `c` listed; other routes never match |
| `route_kinds` | `RouteKind::from(route)` is listed |
| `topics` | its topic under the response's version is listed; outliers and unclassified transmissions never match |
| `false_detections` | `Include` (the default) always; `Exclude` unless the view's copy of the transmission's current verdict is `FalseDetection` |

Empty lists do not restrict and non-empty fields combine with AND. The
window is separate and always tested against `Confirmed::at`. For the graph
the subject is each transmission counted into an edge, for search each hit,
for the projection each point, and for the drill-down each row. Every
response reports its topic-model version; a client links two responses only
when the versions agree, and a filter holding an old version's topic ids
matches nothing.

### Verdicts and detection quality

A verdict (`derived/flow/verdict.rs`) is an operator's judgement of a
transmission, `Genuine` or `FalseDetection`, on a separate axis from
`TransmissionState`. The detector's output is never changed, so verdicts
are ground-truth labels for measuring the detector.

1. **Action.** `OperatorAction::SetVerdict { transmission, verdict, note }`
   needs `Triage` (not `Content` as well: the action reveals no text, and
   the text to judge from is behind the `Content` queries). `verdict: None`
   withdraws. The surface stamps the caller and time and calls
   `TransmissionVerdicts::set` (L5).
2. **State check.** Only a judgeable state takes a verdict:
   `TransmissionState::judgeable` gives `Judgeable::Suspected`,
   `Discarded` (the detector's negative call, so `Genuine` there is a false
   negative) or `Confirmed` (for `Confirmed`, `Classified`, `Aggregated`).
   `Detected` and `AwaitingContent` are `NotJudgeable`, which the surface
   returns as `Conflict(TransmissionNotJudgeable)`; an unknown transmission
   is `NotFound`. `TransmissionVerdict::new` takes the transmission and
   runs the check, so a record for an unjudgeable state cannot be built.
   Every state after a judgeable one is judgeable, so verdicts need no
   ordering with the correlator.
3. **Log.** The record is appended to the transmission's `VerdictLog`
   (append-only; withdrawal appends a `None` record). The record at index
   `i` has `VerdictRevision` `i + 1` and the last record is the current
   verdict. A request whose verdict is already current appends nothing and
   returns `Unchanged`; otherwise `Applied`, and one `VerdictSet
   { transmission, verdict, revision, by, at }` is written to the outbox
   in the same transaction.
4. **Readers.** Triage, the edge store, search and the projection each
   keep a `CurrentVerdict` per transmission from `VerdictSet`;
   `CurrentVerdict::observe` keeps the highest revision, so redelivery and
   reordering never roll a verdict back.
   - **Alerts.** A newer `FalseDetection` suppresses every active alert
     whose subject is the transmission, of any rule
     (`SuppressReason::OperatorRejected`). While it is current, triage
     returns `TriageOutcome::OperatorRejected` for drafts about it. A
     `Genuine` verdict or a withdrawal reopens nothing.
   - **Linked views.** `FilterSubject::false_detection` is
     `CurrentVerdict::is_false_detection` of the view's copy, read at query
     time.
   - **Edge store.** Verdicts are subtracted at query time, not stored as
     a bucket dimension. Buckets hold detector output only, so a verdict
     never rewrites a bucket or unsettles a settled one, and an `Include`
     query never depends on verdicts. With `Exclude`, a graph or series
     reads the buckets and subtracts the stored contributions of the
     transmissions its copy holds as `FalseDetection` (and the rest of the
     filter admits); the drill-down skips their rows. A verdict changed
     after aggregation shows in the next query after `EdgeStore::judge`
     returns, in every window, with nothing to rebuild. An `Exclude`
     result is as of the verdicts the store held when it ran: operator
     judgement has no settling point.
5. **Queries.** `QueryApi::verdicts` returns a transmission's log and
   `QueryApi::detection_quality(window)` a `DetectionQuality`
   (`aggregates/quality.rs`), both with `View`: ids, verdicts, notes,
   counts, route kinds and match classes, no message text.

`DetectionQuality::tally` is the reference definition. It counts each
transmission whose `opened_at` (every state has it and none changes it) is
in the window and whose state is judgeable, once, in the `QualityRow` of
its `RouteKind` and `QualityMatch`, under `genuine`, `false_detection` or
`unlabeled` (never judged or withdrawn) by its current verdict:

| `QualityMatch` | `genuine` | `false_detection` |
| --- | --- | --- |
| `Content(class)`: confirmed, by its strongest match | true positive | false positive |
| `Suspected`: access evidence only | missed so far | correctly not confirmed |
| `Discarded`: expired | false negative | true negative |

A confirmed transmission with several matches counts under the strongest
`MatchClass` (`Exact`, `Normalized`, `Decoded`, `Semantic`, in that
order): it is as credible as its best evidence. State and verdict are both
read at query time. Rows are unique per key, never all zero, and ordered
(`DetectionQuality::new`).

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

### Retention and watermarks

**Retention** (`aggregates/retention.rs`). A `RetentionPolicy` (config,
`keep_last` at least 2) keeps the active version, every newer one, the
`keep_last` newest versions that have been active, and every pinned
version; `RetentionPolicy::to_drop` lists the rest, all superseded. Each
`TopicVersionInfo` carries a `Retention`: `Retained { pin }` or
`Dropped { at }`.

| Data | Retained version | Dropped version |
| --- | --- | --- |
| edge buckets and stored contributions (L7) | kept | deleted; graph, series and drill-down return `VersionNotRetained` |
| topic assignments (L6) | kept | deleted |
| sizes over a window | from assignments | `VersionNotRetained` |
| all-time sizes | from assignments | frozen at the drop |
| topics and lineage | kept | kept |
| history entry | kept | kept, marked `Dropped` |

The catalog enforces the policy after `TopicVersionActivated`, after an
unpin and on start: it marks each `to_drop` version dropped (freezing its
all-time sizes), then publishes `TopicVersionDropped`; only then do L6 and
L7 delete data. Pins and drops are serialized. `PinTopicVersion` and
`UnpinTopicVersion` need Govern. Pinning returns `Unchanged` when already
pinned, `NotFound` for an unknown version, `Conflict(TopicVersionFitting)`
for a fitting one (a fit can still fail, and pending versions are kept
anyway) and `Conflict(TopicVersionDropped)` for a dropped one. Unpinning a
version without a pin, dropped or not, is `Unchanged`.

**Watermarks** (`aggregates/watermark.rs`). Buckets are keyed by
`Confirmed::at`, so late matches and suspected-to-confirmed upgrades add
to closed buckets. L7 computes

```text
watermark = align_down( min( ticked_through − (evidence_window + suspected_ttl), oldest_pending ) )
```

from a `PipelineFrontier`: `ticked_through` is the earliest last tick of
the correlator shards, and `oldest_pending` the earliest event time of an
exchange in flight at the proxy or of an unacked or dead-lettered delivery
in `reconstruct`, `provenance`, `flow`, `analyze` or `topology` (re-fit
classifications aside). Caught up, that is `now − settle_after` rounded
down to a bucket; a lagging consumer or a dead letter holds it back. The
exposed watermark never decreases and advances in whole buckets, each
strict advance published once as `WatermarkAdvanced`, at most one per
recompute and in steady state one per bucket width. Once `W` is exposed, no
bucket of an activated version ending at or before `W` changes. Every
aggregate response (`TopologyGraph`, `TopologySeries`, `TopicSizes`,
`EdgeTransmissionPage`) is `Watermarked` with the watermark read before
its data. Other aggregates use the same value read the same way: the
projection, and the channel graph, whose access and transmission counts
are keyed by event time, so L7's watermark (read before the data) is a
sound, conservative bound for them too. Results before the watermark can
still change through what is resolved at query time: merges, verdicts and
the active topic version.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `spec/Cargo.toml` | Builds the spec as a library so it type-checks and its tests run | crate `crosstalk-spec` |
| `spec/types/mod.rs` | Crate root, tier overview | — |
| `spec/types/ids.rs` | Typed ids | `AgentId`, `ExchangeId`, `SpanId`, `ChannelId`, `TransmissionId`, … `AuditId`, `MessageHash`, `PromptHash`, `CredentialHash`, `AccountHash` |
| `spec/types/support.rs` | Shared building blocks | `NonEmpty`, `NonBlank`, `DisplayText` (checked), `Change`, `Timestamp`, `TimeWindow`, `ByteRange`, `Blake3`, `Similarity`, `Share` |
| `spec/types/paging.rs` | Cursor pagination for list queries | `PageSize`, `Cursor`, `PageRequest`, `Page`, `PageOverflow`, `ChannelList`, `AgentList`, `AlertRuleList`, `DeadLetterList`, `EdgeTransmissionList`, `AuditList` |
| `spec/types/observed/client.rs` | Ingress, upstream, credential and harness facts | `IngressMode`, `Upstream`, `UpstreamKind`, `Dialect`, `CredentialScheme`, `CredentialRef`, `HarnessClaim`, `HarnessIds`, `RequestClass`, `ClientContext`, `EndpointKind` |
| `spec/types/observed/message.rs` | Canonical messages | `Message`, `MessageBody`, `Role`, `AssistantPart`, `UserPart`, `ToolCall`, `ToolArguments`, `CanonicalJson`, `ToolResult`, `Unknown`, `PartRef` |
| `spec/types/observed/exchange.rs` | Exchanges and their pipeline stage | `Exchange`, `ExchangeMeta`, `WireProtocol`, `Transport`, `Continuation`, `ResponseId`, `ExchangeOutcome`, `ExchangeFailure`, `ExchangeStage` |
| `spec/types/observed/agent.rs` | Agent identity and labels | `Agent` (`rename`), `AgentLabel`, `IdentityEvidence`, `IdentityScope`, `Strength`, `AgentState`, `ActiveAgentState`, `MergeRequest`, `MergeAuthor` |
| `spec/types/observed/agent/merge.rs` | The merge log, exact unmerge and vetoes | `MergeRecord` (checked, `revert`), `Reversal`, `MergedInto`, `Agent::merge_away`, `Agent::repoint`, `Agent::revert`, `Agent::restore`, `MergeVeto` (checked, `separates`) |
| `spec/types/observed/conversation.rs` | Threaded conversations | `Conversation`, `ConversationOrigin` |
| `spec/types/derived/provenance/span.rs` | Spans and their lifecycle | `Span`, `SpanLocation`, `Origin`, `RelaySource`, `SpanState`, `SpanEvent`, `OriginatedSpan` |
| `spec/types/derived/provenance/fingerprint.rs` | Fingerprints and index hits | `Fingerprint`, `WinnowParams`, `PositionedFingerprint`, `FingerprintHit` |
| `spec/types/derived/provenance/matching.rs` | Content matches | `ContentMatch`, `MatchKind`, `Codec`, `Carrier`, `InvalidMatch` |
| `spec/types/derived/flow/resource.rs` | Resources and patterns | `Resource`, `Locator`, `ResourcePattern`, `Host` |
| `spec/types/derived/flow/access.rs` | Accesses | `Access`, `AccessOp`, `AccessKind`, `Extraction` |
| `spec/types/derived/flow/evidence.rs` | Communication evidence | `Evidence`, `CoAccess`, `InvalidCoAccess` |
| `spec/types/derived/flow/timing.rs` | The correlator's windows | `CorrelationTiming` (checked: `window_closes_at`, `expires_at`, `settle_after`), `InvalidTiming` |
| `spec/types/derived/flow/transmission.rs` | Transmissions and their lifecycle | `Transmission`, `Route`, `DelegationDirection`, `DirectCarrier`, `TransmissionState` (`expire`), `Confirmed`, `Classification` |
| `spec/types/derived/flow/verdict.rs` | Operator verdicts beside the detector's state | `Verdict`, `Judgeable`, `NotJudgeable`, `TransmissionState::judgeable`, `TransmissionVerdict` (checked), `VerdictRevision`, `VerdictLog` (checked append), `VerdictRecorded`, `CurrentVerdict` (`observe`, `is_false_detection`), `Observed` |
| `spec/types/derived/flow/channel/mod.rs` | Channels and promotion | `Channel`, `ChannelOrigin` (`promoted`), `Declaration`, `DeclaredHistory`, `Seed` |
| `spec/types/derived/flow/channel/detection.rs` | Channel detection lifecycle | `DeclaredDetection`, `TrafficDetection` |
| `spec/types/derived/flow/channel/policy.rs` | Channel policy, its history and traffic routing | `Policy`, `Decision`, `PolicyAuthor`, `PolicyKind`, `PolicyDecision` (checked from `Policy`), `PolicyHistory` (checked), `Recorded`, `TrafficVerdict` |
| `spec/types/aggregates/edge.rs` | Topology edges and their drill-down | `EdgeKey`, `TopicSlot`, `EdgeStats`, `Edge`, `Weighting`, `RouteKind`, `TopologyGraph`, `EdgeSelector`, `EdgeTransmission`, `EdgeTransmissionPage`; re-exports `TopologyFilter` |
| `spec/types/aggregates/filter.rs` | The filter shared by every linked view | `TopologyFilter`, `FilterSubject`, `TopologyFilter::admits` |
| `spec/types/aggregates/projection.rs` | The 2-D projection of embeddings | `ProjectionToken`, `ProjectionLimit`, `ProjectedPoint`, `Projection`, `InvalidProjection` |
| `spec/types/aggregates/quality.rs` | Verdicts tallied against the detector's calls | `MatchClass` (`strongest`), `QualityMatch`, `QualityRow`, `DetectionQuality` (checked, `tally`), `InvalidQuality` |
| `spec/types/aggregates/series.rs` | Time series over the edge table | `BucketWidth`, `SeriesStep`, `SeriesGrid`, `SeriesGrouping`, `SeriesEdge`, `Series`, `SeriesGroups`, `TopologySeries`, `TopologyGraph::total`, `Weighting::stat`, `RouteKind::of` |
| `spec/types/aggregates/retention.rs` | Retention of topic-model versions | `RetentionPolicy` (checked: `protected`, `to_drop`), `Pin`, `Retention`, `PinChange`, `PinError`, `DropError`, `TopicVersionHistory::pin`, `unpin`, `mark_dropped` |
| `spec/types/aggregates/watermark.rs` | When a bucket is final | `PipelineFrontier`, `Watermark::settled`, `finalizes`, `advance`, `Watermarked`; re-exports `Watermark` |
| `spec/types/aggregates/topic.rs` | Embeddings and topics | `Embedding`, `EmbeddingModel`, `Topic`, `TopicModelVersion`, `TopicAssignment`, `Assignment` |
| `spec/types/aggregates/topic_history.rs` | Topic-model versions, sizes and lineage | `TopicVersionStatus`, `CompletedFit`, `FitRecord`, `TopicVersionInfo` (`with_retention`), `TopicVersionHistory`, `TopicSize`, `TopicSizes`, `LineageLink`, `LineageEntry`, `TopicLineage` (`remap` to a `TopicWatch`), `RemapError` |
| `spec/types/aggregates/alert.rs` | Alert rules and alerts | `BuiltinRule`, `UserRule`, `RuleDefinition`, `RuleName`, `AlertRuleConfig`, `SemanticQuery`, `TopicWatch`, `QueryWatch`, `StaleReason`, `ContentRule`, `AlertRule`, `AlertRuleKind`, `AlertRuleDef` (checked; `evaluates`, `set_enabled`, `update`, `remap`, `embedding_model_changed`), `AlertRuleSet`, `RuleStatus`, `RuleRevision`, `AlertDraft`, `TriageOutcome` (incl. `OperatorRejected`), `Alert`, `AlertState`, `SuppressReason` (incl. `OperatorRejected`), `AlertRevision` |
| `spec/types/events/mod.rs` | Bus envelope and subjects | `Envelope`, `BusEvent`, `Subject` |
| `spec/types/events/changed.rs` | Change notifications for the live feed | `Changed` |
| `spec/types/events/{ingest,detect,insight}.rs` | Events by producing layer | `IngestEvent` (including `AgentMerged`, `AgentUnmerged`, `AgentRenamed`), `ConversationDelta`, `DetectEvent` (including `VerdictSet`), `InsightEvent` (including `AlertChanged`, `AlertRuleChanged`, `TopicVersionActivated`, `TopicVersionDropped`, `WatermarkAdvanced`) |
| `spec/types/interfaces/l0_ingress.rs` … `l8_surface.rs` | One module per layer | the traits listed in the data flow above, and their error enums: `IdentityResolver::merge`, `unmerge` and `rename`, `AgentDirectory` (L3); `ChannelRegistry::set_policy`, `policy_history` and `promote` (L5); `AlertTriage::transmission_judged`, `TopicCatalog` (with `pin`, `unpin`, `enforce_retention`), `ProjectionIndex`, `AlertRuleStore`, `RuleError` (L6); `EdgeStore::judge`, `series`, `transmissions`, `drop_version`, `watermark`, `advance_watermark` and `FrontierSource` (L7); the list, series, topic-history, policy-history and audit queries on `QueryApi`, `QueryApi::verdicts`, `detection_quality`, `operators` and `sinks`, `SinkInfo`, `SinkKind`, and `Caller` (checked: built only by the directory), `Permission`, `PermissionSet`, `OperatorAction` (`required_permission`, `kind`, `subjects`), `ActionKind`, `ActionOutcome` (`subject`) (L8) |
| `spec/types/interfaces/l5_flow/verdicts.rs` | The L5 verdict store | `TransmissionVerdicts` (`set`, `log`, `quality`), `VerdictError` |
| `spec/types/interfaces/l8_surface/lists.rs` | Surface list filters and the projection request | `ChannelFilter`, `AgentFilter`, `AgentStateKind`, `AlertRuleFilter`, `ProjectionRequest` |
| `spec/types/interfaces/l8_surface/live.rs` | The live feed (SSE) | `LiveFeed`, `LiveStream`, `UiEvent` (`from(Changed)`, `required_permission`, `visible_to`), `LiveCursor`, `FeedEpoch`, `Resume`, `FeedWindow` (checked), `ResumePlan`, `ResyncReason`, `LiveItem`, `LiveEnd`, `LiveConfig` (checked) |
| `spec/types/interfaces/l8_surface/audit.rs` | The audit log | `AuditLog`, `AuditEntry` (`by`, `subjects`), `AuditBody`, `OperatorRecord` (checked), `ConfigRecord`, `ConfigChange`, `ConfigOutcome`, `AuditAuthor`, `AuditSubject`, `AuditOutcome`, `OutcomeKind`, `Rejection`, `AuditFilter`, `AuditError` |
| `spec/types/interfaces/l8_surface/operators.rs` | The operator directory and access config | `AccessConfig`, `AccessMode`, `TrustedOperator`, `OperatorConfig`, `OperatorName` (checked), `Operator`, `OperatorDirectory` (checked: `load`, `caller`), `RequestIdentity`, `Unauthenticated`, `InvalidAccessConfig` |
| `spec/types/tests/` | Invariant tests (`series.rs`, `topic_history.rs` for the series and topic history; `filter.rs`, `paging.rs`, `projection.rs` for the query surface; `live.rs`, `audit.rs`, `operators.rs`, `policy.rs` for the live feed, audit log, operator directory and policy history; `agents.rs` holds a reference merge table and a seeded random walk over merges and reverts; `rules.rs` for built-in and user rules; `surface.rs` for operator actions; `verdicts.rs`, `quality.rs` for verdicts and detection quality; `retention.rs`, `watermark.rs` for retention and watermarks) | — |
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
- Merges are a log of immutable records; both agents of a merge are
  canonical, so chains are never longer than one. An agent is merged by
  exactly one unreverted record, the one that names it as source.
- An unmerge reverts one record exactly, at most once: its source returns
  to its prior `ActiveAgentState`, every agent it repointed points at the
  source again unless it was unmerged or merged afresh since, a
  `MergeVeto` keeps the pair apart from the resolver, and one
  `AgentUnmerged` lists the restored agents. Reverting the latest record is
  the identity on the merge table and on every topology graph.
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
- Only a suspected transmission can be discarded, and only by expiry.
- A verdict never changes a transmission's state. Only `Suspected`,
  `Discarded` and the confirmed states take one
  (`TransmissionState::judgeable`, `TransmissionVerdict::new`), and every
  state after a judgeable one is judgeable. A `VerdictLog` is append-only
  with revisions equal to positions; the last record is current, and a
  repeat of the current verdict appends nothing. Each appended record
  publishes one `VerdictSet`; readers keep the highest revision
  (`CurrentVerdict::observe`).
- A current `FalseDetection` verdict suppresses the transmission's active
  alerts (`OperatorRejected`) and keeps triage from opening new ones;
  withdrawing it reopens nothing.
- Verdicts are never stored in edge buckets. `FalseDetections::Exclude` is
  `Include` minus the contributions of transmissions whose current verdict
  is `FalseDetection`, in graphs, series and drill-down alike.
- `DetectionQuality` counts each judgeable transmission opened in the
  window once, by route kind, strongest match class (or suspected,
  discarded) and current verdict (`DetectionQuality::tally`); rows are
  unique and never all zero.
- Confirmed traffic on a channel raises an alert unless its policy is
  sanctioned (`Policy::on_traffic`).
- Every policy decision, config or operator, is kept in the channel's
  `PolicyHistory`, ordered by decision time with no duplicates
  (`PolicyHistory::from_entries`, `PolicyHistory::record`); the channel's
  policy is always `PolicyHistory::current`, so the latest decision by time
  wins whatever order events arrive in. A history entry is a
  `PolicyDecision`, which cannot be `Unreviewed(None)`.
- Every operator action call that returns `Ok` or an `ActionError` other
  than `Store` leaves exactly one operator `AuditEntry` whose outcome maps
  back to that result (`AuditOutcome::of`, `AuditOutcome::result`). An
  `OperatorRecord` is `Forbidden` exactly when its caller lacks the action's
  required permission, and then names it (`OperatorRecord::new`). Every
  change a config load makes is one config entry, committed with the
  change; an unchanged load records nothing. An entry's author is derived
  from its body. The audit log is append-only.
- A `Caller` is built only by `OperatorDirectory::caller` and holds exactly
  its operator's configured, non-empty permissions. In trusted mode every
  request gets the trusted operator with every permission, and it is the
  only operator with any. Former operators stay listed with no permissions
  and get no `Caller`.
- Alert revisions are consecutive per alert, starting at 1 for
  `AlertOpened`.
- A live event carries only an id, and is published by the owning store
  only after the change is visible to the query it names, at least once per
  committed change; so a client that re-queries on each event converges on
  the stored state whatever the order or duplication. Subscribing needs
  View; `ProjectionReady` reaches only callers with Content. A resume cursor
  is replayed only when every later entry is retained and from the same
  epoch; otherwise the stream starts with `Resync`. A slow stream ends with
  `Lagged`; it never drops items or blocks others. `FeedWindow`'s floor
  never exceeds its head, and `LiveConfig`'s retention outlasts its
  heartbeat.
- Deduplication is a triage outcome, not an alert state.
- Each built-in rule exists exactly once, under its fixed reserved id, and
  can only be enabled or disabled; user rules never take a reserved id.
  Rules are never deleted and keep their kind.
- Only user rules can be stale, and staleness is separate from the
  operator's enabled or disabled status: a re-fit or an embedding model
  change makes a rule stale, and only an update makes it current again,
  which also enables it. A rule evaluates only when enabled and current.
  Once a disable returns, the rule has no active alerts and triage opens
  none for it.
- Every operator action names one permission
  (`OperatorAction::required_permission`, one exhaustive match): Govern for
  identity, policy, alert rules and topic-version pins; Triage for alerts
  and verdicts;
  Operate for the pipeline. View, Content and Audit are read permissions
  that no action needs. The surface stamps author and time from the
  caller, and `ActionKind` and the audit log cover every action.
- L7 publishes `TopicVersionActivated { version, previous }` exactly once
  per switch, only after `EdgeStore::activate` has switched graph and
  series queries, and never for a version older than the active one.
- In a `TopologyGraph`, edge shares sum to 1 unless there are no edges.
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
  drill-down rows carry no message content and need View, as do verdict
  logs and detection quality.
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
- A `RetentionPolicy` keeps at least 2 versions. Only superseded versions
  are dropped, a fitting version is never pinned, a pin is no earlier than
  its version was ready, and a dropped version has no pin
  (`TopicVersionInfo::with_retention`). `to_drop` never lists the active,
  a newer, a recent or a pinned version, and `mark_dropped` accepts nothing
  else. Activation deletes nothing; data is deleted only after the catalog
  marks the version dropped, and a query never sees a version half
  deleted.
- `CorrelationTiming` durations are non-zero. `Confirmed::at` is the
  reader exchange's start. The watermark is
  `align_down(min(ticked_through − settle_after, oldest_pending))`, never
  decreases, and moves in whole buckets; once exposed, buckets of activated
  versions ending at or before it never change (`apply` returns
  `LateContribution`). Every aggregate response carries the watermark read
  before its data.
- `TimeWindow` and `ByteRange` are never empty. `Similarity` and `Share` are
  never NaN or outside `0..=1`.
- Bus delivery is at least once. Consumers are idempotent on the envelope id
  and on entity ids.
- The spec has no dependencies; `cargo check` and `cargo test` on
  `spec/Cargo.toml` must stay clean.
