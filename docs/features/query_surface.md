# Query surface

The UI's contract with the gateway: what it reads (`QueryApi`), what an
operator can do (`OperatorActions`), how it hears about changes
(`LiveFeed`), and what is recorded about both (`AuditLog`). The pipeline
that produces the data is in [type_spec.md](type_spec.md); the read models
for agents, channels, transmissions and the overview are in
[read_models.md](read_models.md); export is in [export.md](export.md).

## Scope

- Callers: how a request becomes a `Caller` through the operator
  directory, including trusted (single-user, no login) mode, and the
  permission each query and action needs.
- The queries' shared contract: paginated lists (channels, agents, alert
  rules, alerts, dead letters, the audit log, edge transmissions,
  transmission rows by id, search hits, a version's topics, projection
  jobs, a channel's resources, a channel's cross-agent transmissions), the linked views sharing one
  `TopologyFilter` and one resolved topic-model version (topology, the
  channel-centred topology, series, search, edge drill-down, projection
  fits), the topic history, stored projections and their columnar frame,
  verdict logs and detection quality, and every aggregate's watermark. The
  read models built on it are in [read_models.md](read_models.md).
- Typed errors: `QueryError` and `ActionError`, and how every store error
  behind a query or an action maps to one.
- Operator actions, each with one required permission: channel policy and
  promotion (with supersession), agent merges, unmerges of one merge record
  and renames, alert triage, transmission verdicts, alert rule management
  (built-in and user rules, and the sinks they deliver to), topic-version
  pins and dead-letter replay.
- The live feed (SSE): id-only `UiEvent`s telling the UI what to re-query,
  fed by `Changed` notifications from every store.
- The present: the gateway's clock and the config a client needs before
  it builds a request (bucket width, export formats, the topic version
  rules are written against, the default remap threshold, the frame
  retention), and one alert rule by id.
- The append-only audit log of operator action calls, config changes and
  exports, filtered by author, subject and time.
- Read-time resolution of merged agents and superseded channels in every
  response and request.
- Export's place in the contract: its permission, errors and audit entries
  (the design is in [export.md](export.md)).

## Non-scope

- The pipeline that produces what is queried: ingest, detection, analysis
  and aggregation ([type_spec.md](type_spec.md)).
- The UI itself, and session verification: a verified session arrives
  as a `RequestIdentity`.
- HTTP routing and framing, statuses and credentials: the
  [HTTP API](http_api.md).
- The JSON encoding of the surface's types and which of them a client may
  send: the [wire contract](wire_contract.md). The projection frame's
  binary layout is part of its type (`ProjectionFrame::encode` and
  `decode`), and so is the canonical row encoding an export's digest is
  defined over (`ExportRow::encode`).
- Undoing a promotion or a supersession.

## Data and control flow

### Generic clients

A UI or client may be generic over `QueryApi` (and `OperatorActions`,
`LiveFeed`), rather than written against one concrete backend: a fixture,
an in-process surface and an HTTP client can then sit behind the same
code. Every method returns `impl Future<Output = ..> + Send`, and
`QueryApi::ExportRows` and `LiveFeed::Stream` are `Send + 'static`, so
generic code can spawn the futures on tokio's multi-threaded runtime and
move a live stream or an export into its own task. The bound it adds is
on the backend value it shares, for example
`Arc<Q>` with `Q: QueryApi + Send + Sync + 'static`. The convention and
its compile-time check are in [type_spec.md](type_spec.md#conventions-for-the-layer-traits).

### Callers and permissions

Config's `AccessConfig` is either `Trusted(TrustedOperator)` (one operator,
every permission, no login) or `Authenticated(operators)`. On each load
`OperatorDirectory::load(previous, config)` returns the new directory and
the `ConfigChange`s that produced it (`SetAccessMode` first when the mode
changed, then `SetOperator`/`RemoveOperator` by id; nothing for an
unchanged config). An operator config drops stays listed with no
permissions, so its name still labels history. Each request's verified
session becomes a `RequestIdentity`, and `OperatorDirectory::caller` builds
its `Caller`: always the trusted operator with `PermissionSet::ALL` in
trusted mode; otherwise the named operator with its configured
permissions, or `Unauthenticated`. `Caller`'s fields are private and
nothing outside the directory can build one, so tests build callers through
a directory too. `QueryApi::operators` (View) returns the directory, former
operators included. `QueryApi::me` returns the caller's own `Operator`:
its id, its name from the directory, and the permissions the caller was
authenticated with (`Caller::permissions`, never empty). It needs no
permission, so any caller, one without View included, can learn who it is
and what it may do. In trusted mode every request's caller is the trusted
operator, so `me` answers with it and `PermissionSet::ALL` whatever the
request carried. `NotFound` only if the directory does not hold the
caller's operator, which cannot happen for a caller the directory built
(INV-1077).

Every query but `me` and every action checks one `Permission` before reading or changing
anything, and returns `Forbidden { missing }` without effect when the
caller lacks it. View is structure: ids, counts, times, similarities, the
topology (agent-centred and channel-centred, with node metadata and
harness claims), series, edge drill-down rows, transmission rows by id,
the overview's counts, channels (rows, names and promotion previews) and
their resources, policy histories, agents (rows, details and names),
rules (the list and one by id), alerts, the topic history, verdict logs
and detection quality, and the present (clock and config); no
message text and no topic labels. Content is anything derived from message
text: transmissions and their evidence, search, topics, projections and
their jobs, and exports that include content or read a projection (other
exports need View). Govern is identity, policy, rules and
their sinks (`QueryApi::sinks` too, since a delivery error can name an
endpoint) and topic-version pins; Triage is working alerts and judging
transmissions; Operate is the pipeline (dead letters); Audit is the audit
log.

### Operator actions

`OperatorActions::act(caller, action)` checks
`OperatorAction::required_permission`, stamps every author and time from
the caller and the time it accepted the action (callers cannot supply
them), forwards the action to the layer that owns its effect, and returns an
`ActionOutcome` or an `ActionError`. `OperatorAction`, `ActionKind` and
`kind`, `required_permission` and `subjects` (`l8_surface/actions.rs`)
match every variant with no wildcard arm.

A client never sends an `OperatorAction`, whose merge holds an author. It
sends an `ActionRequest` (`l8_surface/actions/request.rs`, a `WireRequest`):
one variant per action, of the same name and fields, with
`MergeAgents { from, into }` and no author. The HTTP layer decodes it and
calls `ActionRequest::into_action(&caller)`, which makes the caller's
operator the merge's author and moves every other request across
unchanged; a merge naming one agent twice is `SelfMerge`, returned as
`InvalidInput(SelfMerge)` without calling `act`. `ActionRequest::of` is the
inverse, so each action has exactly one request form.

| Action | Permission | Effect | Success | Audit subjects |
| --- | --- | --- | --- | --- |
| `SetPolicy { channel, policy, note }` | Govern | publishes `PolicyChanged` (L5 records it); a superseded channel is refused first | `Applied` | the channel |
| `MergeAgents(MergeRequest)` (built by `OperatorAction::merge_agents`) | Govern | `IdentityResolver::merge` (L3) | `Merged(MergeId)` | both agents, then the merge |
| `Unmerge { merge }` | Govern | `IdentityResolver::unmerge` (L3) | `Applied` | the merge |
| `RenameAgent { agent, label }` | Govern | `IdentityResolver::rename` (L3) | `Applied` / `Unchanged` | the agent |
| `PromoteChannel { channel, pattern, policy, note }` | Govern | `ChannelRegistry::promote` with a `Promotion` (L5) | `ChannelPromoted { channel, superseded }` | the channel and every channel it superseded |
| `Acknowledge { alert }`, `Resolve { alert, note }` | Triage | `AlertActions::acknowledge`, `resolve` (L6), which publish `AlertChanged` and `Changed::Alert`; resolving an open alert is `Conflict(AlertNotAcknowledged)`, acting on a resolved or suppressed one `Conflict(AlertNotActive)` | `Applied` / `Unchanged` | the alert |
| `SetVerdict { transmission, verdict, note }` | Triage | `TransmissionVerdicts::set` (L5) | `Applied` / `Unchanged` | the transmission |
| `CreateRule { name, rule, sinks }` | Govern | `AlertRuleStore::create` (L6) | `RuleCreated(AlertRuleId)` | the new rule |
| `UpdateRule { id, name, rule, sinks }`, `SetRuleEnabled { id, enabled }` | Govern | `AlertRuleStore::update`, `set_enabled` at the acceptance time (L6); enabling a stale rule is `Conflict(RuleStale)`, disabling is always allowed | `Applied` / `Unchanged` | the rule |
| `PinTopicVersion { version }`, `UnpinTopicVersion { version }` | Govern | `TopicCatalog::pin`, `unpin` at the acceptance time (L6) | `Applied` / `Unchanged` | the version |
| `ReplayDeadLetter { group, id }` | Operate | `DeadLetterStore::replay` (L2) | `Applied` | none |

No action needs View, Content or Audit, which are read permissions.
`SetVerdict` needs Triage alone, not Content as well: it reveals no text,
and the text to judge from is behind the Content queries. A refusal by the
owning layer comes back as a typed `ActionError` (see Errors). `AlertSink`s
deliver each alert to the sinks its rule lists.

### Audit log

An `AuditEntry { id, at, body }` is either `AuditBody::Operator(OperatorRecord)`
(a `CallerSnapshot` of the `Caller`, the `OperatorAction` and an `AuditOutcome`:
`Succeeded(ActionOutcome)`, `Rejected(Rejection)` or `Forbidden { missing }`,
the exact inverse of `act`'s result for every outcome and error) or
`AuditBody::Config(ConfigRecord)` (the loaded config's `ConfigHash`, a
typed `ConfigChange` and a `ConfigOutcome`) or `AuditBody::Export(ExportRecord)`
(a `CallerSnapshot`, the `ExportRequest` and an `ExportEvent`: `Refused` with the
`QueryError` returned, `Started` with the header, `Ended` with the trailer,
or `Abandoned`; see [export.md](export.md)). A `CallerSnapshot` is the
caller's operator and the permissions it held, as plain data: the UI
decodes audit entries, and what it decodes is a snapshot, never a `Caller`
that could act. `OperatorRecord::new` makes an
entry `Forbidden` exactly when its caller lacks the action's permission.
`AuditEntry::by` derives the author (`Config` or `Operator(id)`) from the
body, so a config change never poses as an operator action; an export's
author is its caller's operator.
`AuditEntry::subjects` lists the entities touched: the ids the action or
change names (`OperatorAction::subjects`, `ConfigChange::subjects`) and the
ids the outcome names (`ActionOutcome::subjects`: a created rule or merge
record, or every channel a promotion superseded, so a superseded channel's
audit history leads to the promotion that retired it), or for an export
`AuditSubject::Export(id)` and, for a projection export,
`AuditSubject::Projection(id)`. Operator entries are
written in the action's transaction; config entries in the change's
transaction, and only for real changes. A request the surface refuses
before it becomes an action (a merge naming one agent twice, `SelfMerge`)
is not an action call and leaves no entry. The `AuditLog` is append-only;
`QueryApi::audit(caller, AuditFilter { by, subject, window }, page)` reads
it as a list and needs Audit.

### Live feed

Every store whose entities a query returns publishes `BusEvent::Changed`
after each committed change, never before the change is visible to the
query that reads it (`events/changed.rs` has the full table):

| `Changed` | Published by, after | `UiEvent` | Re-query |
| --- | --- | --- | --- |
| `Agent(AgentId)` | L3: creation (and the new agent's canonical parent), state change, merge (source, target, repointed agents), unmerge (source, former target, restored agents), for a merge or unmerge also the agents whose stored parent is one of those and the source's canonical parent, rename; not activity (claims, last seen) | `AgentChanged` | `agents`, `agent`, `agent_names` |
| `Channel(ChannelId)` | L5: discovery, declaration, new resource, detection change, recorded policy decision; a promotion announces the promoted channel and every channel it superseded (`Changed::promotion`) | `ChannelChanged` | `channel`, `channels`, `channel_names`, `policy_history`, `channel_resources`, an open `promotion_preview`, `overview` |
| `Verdict(TransmissionId)` | L5 verdict store: a verdict set or withdrawn | `VerdictChanged` | `verdicts`, `detection_quality`, `transmissions_by_id`, views excluding false detections |
| `Alert(AlertId)` | L6 alert store: triage (open, deduplicate, suppress), acknowledge and resolve (`AlertActions`) | `AlertChanged` | `alerts`, `alert`, `overview` |
| `Rule(AlertRuleId)` | L6 rule store: create, update, enable or disable, turning stale | `RuleChanged` | `alert_rules` |
| `TopicVersion(TopicModelVersion)` | L6 catalog: ready, active, superseded; pinned, unpinned, dropped | `TopicVersionReady` | `topic_versions`, then topic-scoped queries |
| `Projection(ProjectionId)` | L6 projection store: a job ready or failed, a frame expired | `ProjectionReady` | `projection_status`, then `projection` |
| `Watermark(Watermark)` | L7: the watermark advancing | `Watermark` | every `Watermarked` query |

The feed writer (consumer group `live`) appends `UiEvent::from` each one
to the feed log, numbered per `FeedEpoch`, before acking. A `UiEvent`
carries an id only, and the UI re-queries it, so events may arrive in any
order or twice and the last re-query returns the stored state. Bus events
that carry facts (`VerdictSet`, `ChannelPromoted`, `AlertRuleChanged`,
`WatermarkAdvanced`, …) are for other consumers; the feed reads only
`Changed`. `LiveFeed::subscribe(caller, resume)` needs View; each stream
passes over events the caller may not receive (`UiEvent::visible_to`:
`ProjectionReady` names a job only Content queries return). There is no
server-side filter and no alias resolution in the feed: the UI drops ids
it is not showing, and because the stores announce every id a merge,
unmerge or promotion re-points, a client showing an alias learns it now
resolves elsewhere and re-queries. `FeedWindow::resume` decides between
replaying from the `Last-Event-ID` cursor and a `LiveItem::Resync`
(re-query everything). Each `LiveItem` is one SSE event named by its
variant (`LiveItem::event_name`), with its cursor's text as the event id
and its JSON as the data; a stream's last event is `end` with the
`LiveEnd` and no id (see `l8_surface/live.rs`). Streams send heartbeats
carrying their newest cursor, end with `LiveEnd::Lagged` when their bounded buffer fills, so a
slow client never blocks the feed or other clients, and end with
`SessionEnded` when the session ends or a config load changes the operator.
`LiveEnd::Unreachable` is client-only: a client of the feed (the HTTP
client) ends its stream with it when it runs out of reconnect attempts. A
surface never ends a stream with it, and a server sends
`LiveEnd::served()` of an end, which is never `Unreachable`.

### The present

`QueryApi::present(caller)` (View) returns a `Present`
(`l8_surface/present.rs`): what a client needs before it can build a
valid request, which no other query gives.

| Field | Is | The client uses it to |
| --- | --- | --- |
| `now` | the gateway's wall clock when it answered, never before the watermark | end a default window ("the last 24 hours") at the present, not at the watermark, which trails the newest data by the settling delay |
| `bucket_width` | `EdgeStore::bucket_width` | snap windows and the time brush to buckets and build `SeriesGrid`s, so no view is refused `UnalignedWindow` or `BucketWidthMismatch` |
| `export_formats` | the formats `export` writes, in offer order (`ExportFormats`: non-empty, distinct) | offer only those; another is `InvalidInput(UnsupportedFormat)` |
| `current_rule_version` | the topic-model version the alerts consumer last made current, which `AlertRuleStore::create` and `update` check watched-topic rules against | pick a rule's topics from it; it moves on `TopicVersionReady`, before L7 activates the version, so the active version is not it |
| `default_remap_threshold` | `AlertRuleConfig::default_remap_threshold` | show what a rule without a threshold takes |
| `frame_retention_micros` | how long a ready projection's frame is kept after its fit (`FrameRetention`, config `projection.frame_retention_days`) | say when a projection expires: `FrameRetention::expires_at(fitted_at)` |

Only `now` changes between reads; `current_rule_version` changes when the
alerts consumer handles `TopicVersionReady` (the feed's
`TopicVersionReady` is the cue to re-read), and the rest change with a
config load, the retentions audited as `ConfigChange`s. `present` reads
no buckets, so it is not `Watermarked`. Fields the UI's workaround list
names that `Present` does not hold answer elsewhere: a merged agent's
merge time and author are its merge record's (`AgentCluster::merge_of`),
and a projected point's channel is in the frame (`PointRoute`).

`QueryApi::alert_rule(caller, id)` (View) returns the `AlertRuleDef`
`alert_rules` lists under `id`, built-in or user, stale or not; rule ids
are never aliased and no rule is deleted, so `None` means no rule ever
had the id.

Over HTTP they are `GET /present` and `GET /alert-rules/{id}`
([HTTP API](http_api.md#route-table)).

### Lists and pagination

Channels, agents, alert rules, alerts, dead letters, edge transmissions,
transmission rows by id, search hits, a version's topics, projection jobs,
a channel's resources and the audit log are read a `Page` at a time. A
`PageRequest<L>` holds a `PageSize` (1 to 500) and, after the first page,
the `Cursor<L>` from the previous page. `L` is a marker per list
(`ChannelList`, `AgentList`, `AlertRuleList`, `AlertList`,
`DeadLetterList`, `EdgeTransmissionList`, `TransmissionList`,
`SearchList`, `TopicList`, `ProjectionList`, `AuditList`,
`ResourceUseList`), so a cursor only fits its own list. Each list is
ordered by a unique sort key that never changes, descending (ids,
`(Confirmed::at, TransmissionId)` for an edge, `(AuditEntry::at, AuditId)`
for the audit log, `(score, TransmissionId)` for search, whose score is a
fixed function of query, model and transmission), and the cursor holds the
last key served (keyset pagination), so concurrent inserts and removals
never make a traversal skip or repeat an item. The cursor also holds a
digest of the request and a MAC; one presented with another request is
`InvalidCursor`. A cursor whose pinned topic version or embedding model is
gone fails with that typed reason instead (`VersionNotRetained`,
`Conflict(EmbeddingModelChanged)`). A channel's resources page under
`ResourceUseList`, keyed by `ResourceId`, bound to the canonical channel
and window. Whole values with their own invariants (a policy history, the
version history, topic sizes, a lineage, a graph, a series) are not paged.
A page with a next cursor is never empty, so following cursors always ends.
List filters (`ChannelFilter`, `AgentFilter`, `AlertRuleFilter`, in
`l8_surface/lists.rs`, and `AuditFilter`) are defined by their `matches`
methods; empty lists do not restrict. The channel and agent filters are
described with their rows in [read_models.md](read_models.md).
`AlertRuleFilter` selects on the operator-set `RuleStatus` and,
separately, on staleness, so a stale-rule list includes disabled stale
rules.

### Linked views

`topology`, `channel_topology`, `series`, `search`, `edge_transmissions`
and `fit_projection` take the same `TopologyFilter` (`aggregates/filter.rs`, re-exported from
`aggregates::edge`). Each view reduces a confirmed transmission to a
`FilterSubject` (canonical sender and reader when the view is computed,
route with its channel resolved through supersession, topic under the
resolved version, latest verdict) and keeps it when
`TopologyFilter::admits` holds:

| Field | Admits a transmission when |
| --- | --- |
| `agents` | the canonical sender or reader equals the canonical form of a listed agent |
| `channels` | its route is `Channel(c)` with `c` (canonical) the canonical form of a listed channel; other routes never match |
| `route_kinds` | `RouteKind::from(route)` is listed |
| `topics` | its topic under the resolved version is listed; outliers and unclassified transmissions never match |
| `false_detections` | `Include` (the default) always; `Exclude` unless the view's copy of the transmission's current verdict is `FalseDetection` |
| `unconfirmed_channels` | always: a transmission view counts confirmed transmissions, whose channel they confirm; it decides only which channels count (the channel-centred view's accesses and channel nodes, the overview's channel queues) |

Whatever the filter, `admits` never admits a subject whose sender and
reader are one canonical agent: a transmission whose agents have since
merged into one counts in no view (`topology.filter.cross-agent-only`,
[channel_semantics.md](channel_semantics.md)). Empty lists do not
restrict and non-empty fields combine with AND. The
window is separate and always tested against `Confirmed::at`. For the graph
and series the subject is each transmission counted into an edge, for
search each hit, for a projection each sampled point (at fit time), and for
the drill-down each row. `admits` takes an `Aliases` (`aliases.rs`), so the
filter's listed agents and channels resolve through merges and
supersession as the subject's do: a listed id that was merged away or
superseded still selects what it became. The channel-centred view's access
edges use `TopologyFilter::admits_access` (below).

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

### Verdicts and detection quality

A verdict (`derived/flow/verdict.rs`) is an operator's judgement of a
transmission, `Genuine` or `FalseDetection`, on a separate axis from
`TransmissionState`. The detector's output is never changed, so verdicts
are ground-truth labels for measuring the detector.

1. **Action.** `OperatorAction::SetVerdict { transmission, verdict, note }`
   (Triage) sets a verdict, or withdraws it with `verdict: None`, through
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
   in the same transaction, with one `Changed::Verdict` for the feed.
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
in the window, whose state is judgeable and that still crosses agents
under the read's aliases (`flow.quality.cross-agent-only`: one whose
agents have since merged into one is no detector call to count), once, in the `QualityRow` of
its `RouteKind` and `QualityMatch`, under `genuine`, `false_detection` or
`unlabeled` (never judged or withdrawn) by its current verdict:

| `QualityMatch` | `genuine` | `false_detection` |
| --- | --- | --- |
| `Content { class, carrier }`: confirmed, by its strongest match's class and carrier kind | true positive | false positive |
| `Suspected`: access evidence only | missed so far | correctly not confirmed |
| `Discarded`: expired | false negative | true negative |

A confirmed transmission with several matches counts under the strongest
`MatchClass` (`Exact`, `Normalized`, `Decoded`, `Semantic`, in that
order): it is as credible as its best evidence. The row also names that
match's `CarrierKind` (the first match of the strongest class, in stored
order), so precision reads per carrier. State and verdict are both
read at query time. Rows are unique per key, never all zero, and ordered
(`DetectionQuality::new`).

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
   records the current `Watermark` (`EdgeStore::watermark`, read before
   the sample). `LayoutFitter::fit` lays them out, a
   pure function of embeddings, order and params. The fitter builds the
   frame with `ProjectionFrame::from_points` and `complete` stores it with
   the `Ready(Fitted)` status in one transaction. Deterministic problems
   (`FitFailure`: too few points, version or model dropped, a non-finite
   layout) make the job `Failed`; anything else (a store error, or
   `LayoutError::Backend` when the layout sidecar times out or fails)
   leaves it to be requeued when its lease lapses.
3. `projection_status` and `projections` (paged) report jobs.
   `projection(caller, id)` returns a `Projection`: the ready job and its
   frame, identical on every read. Queued or fitting is
   `Conflict(ProjectionNotReady)`, failed is `Conflict(ProjectionFailed)`,
   expired is `ProjectionNotRetained`, unknown is `NotFound`. A
   `Projection` has no JSON form: over HTTP the job record is the JSON
   `ProjectionInfo` and the frame is `application/octet-stream`
   (`ProjectionFrame::encode`); the UI joins them with `Projection::new`
   ([wire/analysis.md](wire/analysis.md#queryapiprojection)).
4. Frames are kept for `projection.frame_retention_days` (default 180;
   `FrameRetention`, reported by `present`) after fitting, then dropped
   (`Expired`); the job record and its spec are
   kept, so a citation still says exactly what was fitted and it can be
   fitted again with the same seed. The catalog keeps every version's
   topics, so a frame's topic ids always resolve to labels.

`ProjectionInfo` (checked) keeps its timestamps in order, a fit's watermark
no later than its start and exactly `min(matching, limit)` points; its
transitions (`start`, `requeue`, `complete`, `fail`, `expire`) refuse moves
outside the lifecycle. `Projection::new` checks that the frame's header
agrees with the ready job.

Points are frozen at fit time: canonical agents, route (a `PointRoute`:
the route kind and, for a channel route, the canonical channel), topic
under the pinned version and `Confirmed::at` as they were when the sample
was read. A client that needs the channel in force now names the frame's
channels table with one `channel_names` batch. For two fits with the same seed and sample size, a point sampled by
the wider one is sampled by any narrower one that still admits it.

**Frame.** A `ProjectionFrame` is the projection as columns: transmission
ids, confirmation times, packed `f32` x/y pairs, and `u32` indices into
tables of senders, readers, route kinds, topics (`OUTLIER` for an
outlier) and channels (`NO_CHANNEL` for a point whose route is not a
channel). `ProjectionFrame::new` checks that every column has one entry
per point, the count is `min(matching, limit)`, indices are in range,
tables are distinct and in order of first use (so equal points give equal
bytes), a point has a channel exactly when its route kind is `Channel`, no
transmission repeats and coordinates are finite. The binary layout
(format 2, little-endian) is an 80-byte header (magic `XTPF`, format,
table lengths, projection id, topic version, count, sample size,
watermark, matching, the channels table length, 12 reserved zero bytes)
followed by the id sections (transmissions, senders, readers, topics,
channels), the `u64` times, the coordinates, the five index columns and
the route kind bytes, padded to 8 bytes, with every section aligned for
typed-array views; the full table is in `aggregates/projection/frame.rs`.
`encode` writes it and `decode` accepts exactly what `encode` can produce,
refusing format 1 (which had no channels) and non-zero reserved bytes.

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

The catalog enforces the policy after `TopicVersionActivated`
(`TopicLifecycle::mark_active`), after an unpin and when `analyze` starts
(`TopicCatalog::enforce_retention`): in one transaction it marks each
`to_drop` version dropped (freezing its all-time sizes), deletes its topic
assignments and publishes `TopicVersionDropped` from that transaction, so
the catalog is the event's one publisher; L7 deletes its buckets on the
event. Pins and drops are serialized. `PinTopicVersion` and
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
strict advance published once as `WatermarkAdvanced` (for bus consumers)
and `Changed::Watermark` (for the feed), at most one per recompute and in
steady state one per bucket width. Once `W` is exposed, no bucket of an
activated, retained version ending at or before `W` changes. Every
aggregate response (`TopologyGraph`, `BipartiteGraph`, `TopologySeries`,
`TopicSizes`, `EdgeTransmissionPage`, `ResourceUsePage`, `OverviewCounts`,
a page of `ChannelRow`s and one `ChannelRow`) is `Watermarked`
with `EdgeStore::watermark` read before its data: accesses and
transmissions are both keyed by event time, so L7's watermark is a sound,
conservative bound for the access buckets and L5's resource use too. A
page of `AgentRow`s and an `AgentDetail` carry the watermark
`EdgeStore::agent_traffic` read before the buckets of their counts. A
stored projection is not wrapped: its frame is fixed when fitted and
carries the watermark read when its sample was read (`Fitted::watermark`,
`Projection::watermark`). Results before the watermark can still change
through what is resolved at query time: merges, supersessions, verdicts
and the active topic version.

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
  confirmation, policy_kind, locator_summary })`, in the channel-centred
  view only, for channels listed as channels. `origin_kind` is a
  `CanonicalOriginKind` (no superseded origin); `confirmation` is
  `Confirmed` when a transmission edge routes through it and its
  listing's otherwise (an unconfirmed channel is drawn marked); `label` is
  `None` until channels carry display labels.

The facts come from `NodeFacts` (`l7_topology.rs`): a synchronous cache
of L3's and L5's facts the edge store reads inside its own snapshot, like
the directories. A node it has not seen yet is drawn with fixed defaults
(an agent provisional, top-level, unlabelled and without claims; a channel,
which only a transmission edge can then draw, discovered, active,
unreviewed and confirmed and summarized by its id, with none of its
accesses drawn) (`topology.node-facts.unknown-channel-defaults`).

Which nodes appear: every edge endpoint (in the channel-centred view also
every access channel and transmission route channel) and every canonical
ancestor of an agent among them, each once and nothing else, so each
`parent` names a node in the same response. `TopologyGraph::new` and
`BipartiteGraph::new` check this and the counts; canonicity is checked
at query time.

### Channel-centred view

Splitting `Route::Channel` edges into A→C→B would show only writes someone
read, and once a channel exists its writes nobody has read yet matter (a
hijacked wiki keeps being written to). So accesses have their own
aggregate (`aggregates/access.rs`): an `AccessEdge { agent, resource, op,
bucket, accesses }`, bucketed like `EdgeKey` with no topic, maintained by
L7 from `AccessRecorded` whether or not the resource is on a channel yet.
A resource is not a channel until a cross-agent transmission goes through
it ([channel_semantics.md](channel_semantics.md)), so a bucket's resource
is resolved at the read to the canonical channel holding it now
(`NodeFacts::channel_of`), and the accesses before discovery join that
channel's buckets without rewriting one.
`QueryApi::channel_topology(caller, window, weighting, filter)` returns a
`Watermarked<BipartiteGraph { nodes, accesses, transmissions, topic_version }>`:

- `accesses`: access buckets in the window, resolved to canonical agents and
  to the canonical channel holding each resource, left out when that is no
  channel or a channel not listed as one (a hidden channel, a declaration
  without cross-agent traffic: `ChannelFacts::listing` is not
  `Listing::Channel`), kept by `TopologyFilter::admits_access`, summed per
  (agent, channel, op) (`topology.bipartite.listed-channels-only`). Each share is its count over all access counts, normalized
  apart from transmissions and independent of the weighting.
- `transmissions`: exactly `topology`'s edges for the same window, weighting
  and filter.
- the watermark, read before the buckets as for `topology`.

The filter on accesses (`admits_access`): agents and channels match the
access's canonical agent and channel; `route_kinds` admits accesses when it
lists `Channel`; an access has no topic, so `topics` keeps the accesses of
channels that carried, in the window, a channel-routed confirmed
transmission with a listed topic. `false_detections` never drops an access
by itself, since an access is an observed read or write, not a detection;
it applies to the transmissions, so the store builds an access's
`channel_topics` only from the transmissions it keeps, and under `Exclude`
a topic carried to a channel only by false detections does not keep that
channel's accesses. `unconfirmed_channels` under `Exclude` drops the
accesses (and so the node) of a channel whose cross-agent traffic is all
unconfirmed (`topology.filter.access-admission-confirmation`).

`QueryApi::channel_resources(caller, channel, window, page)` returns a
`Watermarked` page of a channel's resources newest first
(`ResourceUseList`, keyed by `ResourceId`), each a `ResourceUse { resource,
writers, readers }` with canonical agents and their access counts in the
window (merged aliases summed, most accesses first). A superseded channel
answers for its superseding channel, named in the `ResourceUsePage`, whose
resources include those of every channel it superseded.

### Promotion and supersession

`OperatorAction::PromoteChannel { channel, pattern, policy, note }` (Govern)
becomes a `Promotion` (pattern and policy decision, both authored by the
caller at the accept time) and `ChannelRegistry::promote`, which follows
`promotion::plan` for the promotion's declaration (the promotion preview
runs the same plan without effect; see
[read_models.md](read_models.md#channels)):

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
   superseded }` after commit, and `Changed::Channel` for the promoted
   channel and each superseded one (`Changed::promotion`); the action
   returns `ChannelPromoted { channel, superseded }` (`SupersededChannels`:
   sorted, each once), whose audit entry names every channel involved.

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
policy, and its confirmation advances the superseding channel's detection
(`TrafficDetection::Active::last_transmission`) while the superseded
channel's stays frozen, even when the content arrives after the promotion
(`l5_flow`, "Detection follows resolution"): the route counts on the
canonical channel everywhere else, so that is the detection that records
it.

### Read models

Agent rows and details, channel rows and the promotion preview, a
channel's cross-agent transmissions (`channel_transmissions`, the review
list of an unconfirmed channel), batch names, transmission rows by id
(never a transmission within one agent), the evidence behind a
transmission, the overview's counts and one alert by id are reads on this
contract: each
takes a `Caller`, checks one permission, pages with `crate::paging` where
it lists, resolves aliases at read time and is `Watermarked` where its data
comes from settled buckets. They are described in
[read_models.md](read_models.md), including how the overview's active
channels agree with the channel and agent rows. What counts as a channel
(listed, unconfirmed, a declaration, hidden after a merge) and as a
transmission (between different agents only) is in
[channel_semantics.md](channel_semantics.md); `alerts` leaves out the
alerts about a hidden channel or a transmission within one agent
(`AlertSubject::shown`), which `alert(id)` still returns.

### Export

`QueryApi::export(caller, request)` (View, or Content when the request
includes content or names a projection) reads L7's watermark, plans the
export (`ExportSource::plan`: the filter's version resolved and pinned as
for any linked view, the window cut at the watermark, agent and channel
resolution and verdicts captured once, rows counted), refuses more than
`ExportLimits::max_rows` with `Conflict(ExportTooLarge)`, audits the start
and returns `Export { header, rows }`. The stream (`ExportStream`) yields
rows and then exactly one trailer: `Complete` with the row count and a
format-independent digest, or `Failed` with why. It is a stream, not a
paged list. The header plays the role of `Watermarked`: it carries the
watermark read first. A transmissions export's row is the
`TransmissionSummary` `transmissions_by_id` lists, and its quoted text is
the evidence page's `MatchQuotes` cut with no context, so an export and
the UI agree. The full design is in [export.md](export.md).

### Errors

Every query returns `QueryError`: `Store` (retry may succeed), `NotFound`,
`Forbidden { missing }`, `VersionNotRetained`, `Conflict(ConflictKind)`
(the state does not allow the request), `InvalidInput(InputError)` (invalid
whatever the state), `InvalidCursor`, `ProjectionNotRetained`. Operator
actions return the subset `ActionError`, which `QueryError::from` keeps
variant for variant (`l8_surface/errors.rs`).

Both enums also have the client-only `Unavailable { kind: UnavailableKind,
reason }`: the call never reached a surface that answered it, because of
a `401` (`Unauthenticated`), a failed connect or send (`Transport`), a
failed body read (`Body`) or no response in time (`Timeout`). Only a
client of the surface produces it (`crosstalk-client`). No surface
returns it, the store error mappings never produce it, and a server
answers `served()` of an error, which turns `Unavailable` into `Store`
with the same reason and status (`surface.http.client-unavailable-typed`).
An action that fails this way had no effect and was not audited;
`AuditOutcome::of` would record it as `Rejected(Failed)`, the `Store` it
is served as. How each store error becomes
one is defined once, by the `From` impls in `l8_surface/query_errors.rs`:
for queries `VersionUnavailable`, `EdgeQueryError`, `SearchError`,
`EmbedError` (embedding a search's text), `CatalogError`,
`ProjectionStoreError`, `RegistryError`, `VerdictError`, `AuditError`,
`AlertReadError` (rules and alerts), `TransmissionStoreError` (stored
transmissions), `SinkRegistryError` (`sinks`), `OperatorStoreError`
(`operators`),
`BusError` (the dead-letter list), `BlobError` and `EvidenceError` (the
evidence: every cause is `Store`, its reason naming it), `AgentReadError`
(agent reads), `ExportPlanError` (planning an export), and the request
values the surface builds before reading: `TooManyIds` (a name lookup's
`IdBatch`), `InvalidSelection` (`EmptySelection`, or `TooManyIds` with the
selection's bound), `InvalidWindow` (`ExcerptContextTooLong`) and
`UnsupportedFormat` (an export format outside `present`'s
`export_formats`: `InvalidInput(UnsupportedFormat)`); for actions, one
per store behind each action, every refusal named in
`surface.action.rejections-typed`: `RegistryError` (`SetPolicy`:
`UnknownChannel` to `NotFound`, `Superseded` to
`Conflict(ChannelSuperseded)`, `OverlappingDeclaration` to
`Conflict(PatternOverlaps)`), `PromotionRefusal` and `PromoteError`
(`PromoteChannel`), `ResolveError` (merges, unmerges and renames:
`UnknownAgent` and `UnknownMerge` to `NotFound`, `AgentMerged`,
`MergeIntoSelf` and `MergeAlreadyReverted` to the same-named conflicts, a
resolver-only `Vetoed` to `Store`), `VerdictError` (`SetVerdict`:
`UnknownTransmission` to `NotFound`, `NotJudgeable` to
`Conflict(TransmissionNotJudgeable)`), `RuleError` (rule management;
enabling a stale rule is `Conflict(RuleStale)`, since only `UpdateRule`
can retarget it), `AlertActionError` (`Acknowledge`, `Resolve`:
`UnknownAlert` to `NotFound`, `NotActive` to `Conflict(AlertNotActive)`,
`NotAcknowledged`, resolving an open alert, to
`Conflict(AlertNotAcknowledged)`), `CatalogError` and `PinError` (`PinTopicVersion`,
`UnpinTopicVersion`: unknown to `NotFound`, fitting to
`Conflict(TopicVersionFitting)`, dropped to
`Conflict(TopicVersionDropped)`) and `SelfMerge`
(`InvalidInput(SelfMerge)`). A store error that reaches both a query and
an action maps to the same variant either way, except a dropped version
(`VersionNotRetained` for a query, `Conflict(TopicVersionDropped)` for a
pin) and a cursor error, which an action cannot cause and so reads as
`Store` (`surface.action.refusals-read-as-queries`); for
both, `DecodeError`: client input the HTTP layer cannot decode as the
route's request type (`wire::decode_request`) is
`InvalidInput(MalformedRequest { kind, reason })`, and never reaches a
store or the audit log ([wire_contract.md](wire_contract.md)). The
promotion preview reads `PromoteError` through that same action mapping
(`PromotionPreview::from_registry`), keeping conflicts as its answer and
converting the rest with `QueryError::from`, so it adds no mapping of its
own. `InputError::TooManyIds` is one variant for every request naming
too many ids: `agent_names` and `channel_names` over `IdBatch::MAX`, and
`transmissions_by_id` over `TransmissionSelection::MAX`, each with the
bound that applied. An export over the configured row limit is
`Conflict(ExportTooLarge)`; a failure after an export has started is
recorded in its trailer, not returned. The edge store's writes fail with
`EdgeError`, which never reaches a query, and so do the consumer-side
write errors (`AgentLifecycleError`, `TrafficError`,
`TopicLifecycleError`, `CorpusError`) and the operator store's
`OperatorLoadError` and `CallerError` (an authentication failure, answered
by the HTTP binding's `AuthError`).

Retention shows up by what was dropped: `VersionNotRetained` for a
topic-model version's buckets or assignments (from
`VersionUnavailable::NotRetained` or `CatalogError::VersionNotRetained`)
and `ProjectionNotRetained` for a projection's frame. A message body
dropped by content retention is not an error: the evidence shows that
side as `Excerpted::BodyDropped`. An action reads no
dropped data, so pinning a dropped version is
`Conflict(TopicVersionDropped)`. `InputError::QueryTooLong` covers any text
too long to embed, a search's or a semantic rule's. No query error is a
free-text classification: `Store`'s reason is diagnostic only.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `spec/types/aliases.rs` | Read-time resolution of merged agents and superseded channels | `Aliases`, `Resolve`, `NoAliases` |
| `spec/types/paging.rs` | Cursor pagination for list queries | `PageSize`, `Cursor`, `PageRequest`, `Page`, `PageOverflow`, `ChannelList`, `AgentList`, `AlertRuleList`, `AlertList`, `DeadLetterList`, `EdgeTransmissionList`, `TransmissionList`, `SearchList`, `TopicList`, `ProjectionList`, `AuditList`, `ResourceUseList` |
| `spec/types/derived/flow/verdict.rs` | Operator verdicts beside the detector's state | `Verdict`, `Judgeable`, `NotJudgeable`, `TransmissionState::judgeable`, `TransmissionVerdict` (checked), `VerdictRevision`, `VerdictLog` (checked append), `VerdictRecorded`, `CurrentVerdict` (`observe`, `is_false_detection`), `Observed` |
| `spec/types/derived/flow/channel/promotion.rs` | What a promotion does and refuses, and what it would cover | `Promotion` (checked), `Registered`, `plan` (takes the `Declaration`), `PromotionPlan`, `PromotionRefusal`, `coverage`, `PromotionCoverage` (built only by `coverage`), `COVERAGE_CAP`, `CappedResources` |
| `spec/types/aggregates/access.rs` | Access buckets, the channel-centred graph and resource use | `AccessEdge`, `WeightedAccess`, `BipartiteParts`, `BipartiteGraph` (checked), `InvalidBipartite`, `AgentAccesses`, `ResourceUse` (checked), `ResourceUsePage` |
| `spec/types/aggregates/node.rs` | Graph nodes | `GraphNode`, `NodeId`, `AgentNode`, `ChannelNode`, `CanonicalStateKind`, `CanonicalOriginKind`, `InvalidNodes`; the node rules `TopologyGraph::new` and `BipartiteGraph::new` run |
| `spec/types/aggregates/filter.rs` | The filter shared by every linked view, and topic-version resolution | `TopologyFilter` (`admits`, `admits_access`, `topics_outside`, `pinned`), `FilterSubject`, `AccessSubject`, `TopicVersionSelector` (`resolve`), `VersionUnavailable`, `FalseDetections` |
| `spec/types/aggregates/projection/mod.rs` | Stored projection jobs | `ProjectionLimit`, `ProjectionParams` (checked), `ProjectionSpec`, `FitFailure`, `Fitted`, `ProjectionStatus`, `ProjectionInfo` (checked, with transitions), `ProjectedPoint`, `PointRoute` (`of`, `from_parts`, `kind`, `channel`), `FrameRetention` (`expires_at`), `Projection` (checked) |
| `spec/types/aggregates/projection/frame.rs` | The columnar projection frame and its binary layout | `ProjectionFrame` (checked; `from_points`, `encode`, `decode`), `FrameHeader`, `FrameTables`, `FrameColumns`, `InvalidFrame` (incl. `ChannelRouteMismatch`), `FrameDecodeError` (incl. `NonZeroReserved`), `MAGIC`, `FORMAT` (2), `HEADER_LEN` (80), `RESERVED_AT`, `OUTLIER`, `NO_CHANNEL` |
| `spec/types/aggregates/quality.rs` | Verdicts tallied against the detector's calls | `MatchClass` (`strongest`, `strongest_match`), `QualityMatch` (`Content { class, carrier }`), `QualityRow`, `DetectionQuality` (checked, `tally`), `InvalidQuality` |
| `spec/types/aggregates/retention.rs` | Retention of topic-model versions | `RetentionPolicy` (checked: `protected`, `to_drop`; on the wire only in `ConfigChange::SetTopicRetention`), `Pin`, `Retention`, `PinChange`, `PinError`, `DropError`, `TopicVersionHistory::pin`, `unpin`, `mark_dropped` |
| `spec/types/aggregates/watermark.rs` | When a bucket is final | `PipelineFrontier`, `Watermark::settled`, `finalizes`, `advance`, `Watermarked`; re-exports `Watermark` |
| `spec/types/events/changed.rs` | Change notifications for the live feed | `Changed` (`promotion`) |
| `spec/types/interfaces/l5_flow/verdicts.rs` | The L5 verdict store | `TransmissionVerdicts` (`set`, `log`, `quality`), `VerdictError` |
| `spec/types/interfaces/l8_surface.rs` | The query API and operator actions | `QueryApi` (every read, including the read models, `present`, `alert_rule` and `export`), `OperatorActions`, `AlertFilter`; re-exports `AlertStateKind` (from `aggregates::alert`, with `AlertState::kind` and `is_active`), `Present` and the action, error, permission and sink types |
| `spec/types/interfaces/l8_surface/present.rs` | The gateway's clock and the config a client builds requests with | `Present` (`now`, `bucket_width`, `export_formats`, `current_rule_version`, `default_remap_threshold`, `frame_retention_micros`) |
| `spec/types/interfaces/l8_surface/permissions.rs` | Who is asking and what they may do | `Caller` (built only by the directory; never serialized), `CallerSnapshot` (checked: `of`, `new`; what an audit record keeps of its caller; wire data, never a request), `NoPermissions`, `Permission`, `PermissionSet` (wire form: an array in `Permission::ALL` order) |
| `spec/types/interfaces/l8_surface/sinks.rs` | Alert delivery | `AlertSink`, `SinkInfo`, `SinkKind`, `SinkError` |
| `spec/types/interfaces/l8_surface/actions.rs` | Operator actions | `OperatorAction` (`merge_agents`, `kind`, `required_permission`, `subjects`; wire data, never a request), `ActionKind` (`ALL`, `index`, `required_permission`, which `OperatorAction::required_permission` returns), `ActionOutcome` (`subjects`), `SupersededChannels` |
| `spec/types/interfaces/l8_surface/actions/request.rs` | The action a client sends | `ActionRequest` (a `WireRequest`; `into_action`, `of`, `kind`) |
| `spec/types/interfaces/l8_surface/errors.rs` | Why a query or action failed; adjacently tagged on the wire ([wire_contract.md](wire_contract.md)) | `QueryError`, `ActionError` (both with the client-only `Unavailable`, `served`, `is_client_only`), `UnavailableKind` (`ALL`), `ConflictKind` (incl. `AlertNotAcknowledged`, `RuleStale`, `MergeIntoSelf`, `ExportTooLarge`), `InputError` (incl. `SelfMerge`, `EmptySelection`, `ExcerptContextTooLong`, `TooManyIds`, `UnsupportedFormat`, `MalformedRequest`) |
| `spec/types/interfaces/l8_surface/lists.rs` | Surface list filters, the search request and the topic page | `ChannelFilter`, `OriginFilter` ([read_models.md](read_models.md)), `AgentFilter` and `AgentText` (re-exported), `AlertRuleFilter`, `SearchRequest`, `SearchMode` (default `Hybrid`), `TopicPage` |
| `spec/types/interfaces/l8_surface/query_errors.rs` | How each store error and refused request value becomes a `QueryError` or an `ActionError` | `From` impls for `VersionUnavailable`, `EdgeQueryError`, `SearchError`, `EmbedError`, `CatalogError`, `ProjectionStoreError`, `RegistryError`, `VerdictError`, `AuditError`,
`AlertReadError` (rules and alerts), `TransmissionStoreError` (stored
transmissions), `SinkRegistryError` (`sinks`), `OperatorStoreError`
(`operators`), `BusError`, `BlobError`, `EvidenceError`, `AgentReadError`, `ExportPlanError`, `TooManyIds`, `InvalidSelection`, `InvalidWindow`, `UnsupportedFormat` (to `QueryError`), `RegistryError`, `VerdictError`, `CatalogError`, `PinError`, `PromotionRefusal`, `PromoteError`, `RuleError`, `ResolveError`, `SelfMerge` (to `ActionError`) and `DecodeError` (to both) |
| `spec/types/interfaces/l8_surface/live.rs` | The live feed (SSE) and its framing | `LiveFeed`, `LiveStream`, `UiEvent` (`from(Changed)`, incl. `VerdictChanged` and `ProjectionReady { id: ProjectionId }`, `required_permission`, `visible_to`), `LiveCursor` (wire form: its text), `FeedEpoch`, `Resume`, `FeedWindow` (checked), `ResumePlan`, `ResyncReason`, `LiveItem` (`event_name`, `cursor`), `LiveEnd` (`EVENT_NAME`, client-only `Unreachable`, `served`, `is_client_only`), `LiveConfig` (checked) |
| `spec/types/interfaces/l8_surface/export/` | Streamed exports with a manifest ([export.md](export.md)) | `ExportRequest`, `ExportDataset`, `ExportFormats` (checked; `check` gives `UnsupportedFormat`), `ExportHeader`, `ExportTrailer`, `ExportStream`, `ExportSealer`, `verify_export`, `ExportRecord`, `ExportPlanError` |
| `spec/types/interfaces/l8_surface/audit.rs` | The audit log | `AuditLog`, `AuditEntry` (`by`, `subjects`), `AuditBody` (incl. `Export`), `OperatorRecord` (checked; keeps a `CallerSnapshot`), `ConfigRecord`, `ConfigChange` (incl. `SetSink`, `RemoveSink`, `SetTopicRetention`, `SetFrameRetention`), `ConfigOutcome`, `AuditAuthor`, `AuditSubject` (incl. `Sink`), `AuditOutcome`, `OutcomeKind`, `Rejection`, `AuditFilter`, `AuditError` |
| `spec/types/interfaces/l8_surface/operators.rs` | The operator directory and access config | `AccessConfig`, `AccessMode`, `TrustedOperator`, `OperatorConfig`, `OperatorName` (checked), `Operator`, `OperatorDirectory` (checked: `load`, `caller`), `RequestIdentity`, `Unauthenticated`, `InvalidAccessConfig` |
| `spec/types/tests/` | Tests for the surface's invariants: `filter.rs`, `paging.rs`, `topic_version.rs`, `query_errors.rs`, `projection.rs`, `projection_frame.rs` (query surface); `live.rs`, `events.rs` (the feed and every event's source); `audit.rs`, `operators.rs`, `surface.rs` (audit log, callers, actions); `action_errors.rs` (the action refusals of the registry, verdict store and catalog); `alerts.rs` (alert state kinds); `verdicts.rs`, `quality.rs`; `retention.rs`, `watermark.rs`; `channels.rs`, `pattern_overlap.rs`, `graph.rs` (supersession, promotion, pattern overlap, graph nodes, the channel-centred graph). The read models' tests are listed in [read_models.md](read_models.md), export's in [export.md](export.md) | — |

## Invariants and constraints

- Every `QueryApi`, `OperatorActions`, `LiveFeed`, `LiveStream`,
  `AuditLog`, `AlertSink`, `ExportStream`, `RowSource` and `ExportSource`
  method returns a `Send` future, and `QueryApi::ExportRows`,
  `LiveFeed::Stream` and `ExportSource::Rows` are `Send + 'static`
  (`canonical.interface.send-futures`), so a client can be generic over
  the surface.
- Only a discovered channel can be superseded (`ChannelOrigin::superseded`),
  and a superseded one cannot be promoted or take a policy decision.
  Resolution is one step: `canonical(canonical(c)) = canonical(c)`. Lookups
  never return a superseded channel. Routes, filters, alert subjects, graph
  nodes, edges and access buckets resolve through supersession at read
  time; nothing stored is rewritten. A confirmation of a transmission
  routed through a superseded channel advances the superseding channel's
  detection, never the superseded one's, which stays frozen.
- A channel exists once a transmission between different agents goes
  through it, and nothing between two ids of one merged agent is counted
  or listed anywhere; the invariants are listed in
  [channel_semantics.md](channel_semantics.md#invariants-and-constraints).
  `channel_transmissions` needs View
  (`surface.query.channel-transmissions-need-view`).
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
- A live event carries only an id, and is published by the owning store
  only after the change is visible to the query it names, at least once per
  committed change; so a client that re-queries on each event converges on
  the stored state whatever the order or duplication. Every store announces its changes
  (`Changed`), a promotion every channel it superseded and a merge or
  unmerge every agent it re-points, and every `UiEvent` has exactly one
  `Changed` source. Subscribing needs View; `ProjectionReady` reaches only
  callers with Content. A resume cursor
  is replayed only when every later entry is retained and from the same
  epoch; otherwise the stream starts with `Resync`. A slow stream ends with
  `Lagged`; it never drops items or blocks others. `FeedWindow`'s floor
  never exceeds its head, and `LiveConfig`'s retention outlasts its
  heartbeat.
- Every operator action has exactly one request form, the
  `ActionRequest` variant of the same kind, which holds no stamped field;
  `ActionRequest::into_action` stamps the caller as a merge's author and
  refuses a self-merge with `SelfMerge`
  (`surface.wire.action-request-covers-actions`). Each live item is one
  SSE event named by its variant, with its cursor's text as the id
  (`surface.live.sse-frame-matches-item`).
- Every operator action names one permission
  (`OperatorAction::required_permission`, one exhaustive match): Govern for
  identity, policy, alert rules and topic-version pins; Triage for alerts
  and verdicts; Operate for the pipeline. View, Content and Audit are read permissions
  that no action needs. The surface stamps author and time from the
  caller; `kind`, `required_permission` and `subjects` match every action
  with no wildcard, and the audit log records every one.
- A graph's nodes are exactly its endpoints (and, in the channel-centred
  view, its access and route channels) plus the canonical ancestors of its
  agents, once each, every parent among them, with agent counts equal to the
  transmission edges' (`TopologyGraph::new`, `BipartiteGraph::new`). A
  `TopologyGraph` is built only by `TopologyGraph::new`, so every value
  keeps its edge, share and node rules (`topology.graph.checked-construction`).
  No node is a merged agent or a superseded channel.
- A `BipartiteGraph` has distinct access and transmission edges, no
  self-edge, and access and transmission shares each normalized on their
  own (`BipartiteGraph::new`). Its transmissions equal `topology`'s edges for
  the same arguments; its accesses include writes nobody read.
- Every linked view (graph, channel-centred graph, series, search,
  projection fit, edge drill-down) applies one `TopologyFilter` as
  `TopologyFilter::admits` defines (`admits_access` for access edges, which
  `false_detections` never drops), with agents resolved through merges and
  channels through supersession when the view is computed
  and topics under one resolved version, which the response reports.
  `TopicVersionSelector::resolve` is the only resolution; a paged view's
  cursor pins its first page's version. A filter naming topics outside the
  resolved version is refused (`TopicsNotInVersion`), never answered empty.
- Enabling a stale alert rule is refused with `Conflict(RuleStale)` and
  changes nothing (`AlertRuleDef::set_enabled`, `RuleError::Stale`);
  disabling any rule is always allowed, and only `UpdateRule` makes a stale
  rule current again, enabling it. A rule that goes stale while enabled
  keeps its status.
- Only a client produces `QueryError::Unavailable`,
  `ActionError::Unavailable` and `LiveEnd::Unreachable`; a server answers
  their `served()` form (`Store`, `ShuttingDown`), never them
  (`surface.http.client-unavailable-typed`).
- Every query error is a typed `QueryError` and every action error a typed
  `ActionError`, which converts to `QueryError` variant for variant; each
  store error maps to exactly one variant through the one `From` impl per
  store error type in `query_errors.rs`. Every action's refusals have one
  (`RegistryError`, `PromoteError`, `ResolveError`, `VerdictError`,
  `RuleError`, `CatalogError`, `PinError`), and a refusal reads the same
  through either mapping except a dropped version and a cursor error
  (`surface.action.rejections-typed`,
  `surface.action.refusals-read-as-queries`). Edge store reads fail with
  `EdgeQueryError` and writes with `EdgeError`.
  A request value the surface builds before reading (an id batch, a
  selection, an excerpt window) that its checked constructor refuses is
  `InvalidInput`, with one `TooManyIds` for every bound on ids.
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
  canonical tables, a channel exactly on channel-routed points, no
  transmission twice and finite coordinates; `decode`
  accepts exactly what `encode` produces. `ProjectionInfo` timestamps never
  go backwards and transitions follow the job lifecycle.
- `present` (View) reports the gateway's wall clock (never before the
  watermark), L7's bucket width, the export formats it writes (non-empty,
  distinct), the topic version rule writes are checked against, the
  default remap threshold and the frame retention the store applies
  (`surface.present.*`). An export in another format is
  `InvalidInput(UnsupportedFormat)` before anything is read
  (`surface.export.unsupported-format-refused`). `alert_rule(id)` is the
  rule `alert_rules` lists under `id`
  (`surface.query.alert-rule-matches-list`).
- A config change to a sink records its id, kind and name, never its
  endpoint (`surface.audit.sink-endpoint-not-recorded`); sink and
  retention changes are audited like every other config change
  (`surface.audit.config-sinks-and-retention`).
- Lists of dead letters need Operate and the audit log needs Audit; edge
  drill-down rows carry no message content and need View, as do verdict
  logs and detection quality.
- A `RetentionPolicy` keeps at least 2 versions. Only superseded versions
  are dropped, a fitting version is never pinned, a pin is no earlier than
  its version was ready, and a dropped version has no pin
  (`TopicVersionInfo::with_retention`). `to_drop` never lists the active,
  a newer, a recent or a pinned version, and `mark_dropped` accepts nothing
  else. Activation deletes nothing; data is deleted only after the catalog
  marks the version dropped, and a query never sees a version half
  deleted.
- An export needs View, or Content when it includes content or reads a
  projection, checked before anything is read; reads only data settled
  before the watermark read at its start, under resolution captured then;
  and always ends with one trailer, `Complete` only when every planned row
  was sent. Every export call is audited. See [export.md](export.md).
- Every `Watermarked` response (`topology`, `channel_topology`, `series`,
  `overview`, `edge_transmissions`, `channel_resources`, `channel`,
  `channels`, `topic_sizes`) carries the watermark `EdgeStore::watermark`
  returned before any of its data was read; `agents` and `agent` carry the
  one `EdgeStore::agent_traffic` read before the buckets of their counts; a
  stored projection carries the watermark its sample was read under.
- The read models' invariants (agent and channel rows, names, the
  promotion preview, transmission rows, evidence, the overview) are in
  [read_models.md](read_models.md); export's are in [export.md](export.md).
- Client input is decoded only as a `WireRequest` type
  (`wire::decode_request`); a `Caller` never serializes, and no record the
  surface stamps with an author or time is a request. Input that does not
  decode is `InvalidInput(MalformedRequest)`. The JSON of every type is in
  [wire_contract.md](wire_contract.md).
