# Crosstalk type specification

The data model of the gateway, written as Rust so it type-checks. The crate
in `spec/Cargo.toml` (`crosstalk-spec`) builds these modules as a library
and runs their tests. It is a member of the root workspace and the shared
boundary crate: every implementation crate under `crates/` depends on it,
and layer crates depend on each other only through it
(`docs/features/workspace.md`). The types are also the JSON wire format
between the gateway, the operator UI and other gateway nodes (`wire/`, and
`docs/features/wire_contract.md` with its area pages under
`docs/features/wire/`).

From the repository root:

```sh
cargo check -p crosstalk-spec
cargo test  -p crosstalk-spec
# after an intended change to a type's JSON: rewrite its golden files, then review the diff
CROSSTALK_BLESS=1 cargo test -p crosstalk-spec wire
# every workspace check: fmt, clippy, tests, docs and the invariant validator
scripts/check.sh
```

## Layout

```text
spec/types/
├── mod.rs                 crate root: the three tiers, aliases, events, interfaces
├── aliases.rs             Aliases (read-time resolution of merged agents and superseded channels), Resolve, NoAliases
├── batch.rs               IdBatch (checked: distinct, ascending, at most 1,000; a WireRequest), TooManyIds: the one id batch of every name lookup (agents, channels)
├── ids.rs                 typed ids: ULID entity ids (incl. AuditId, ExportId, MergeId, ProjectionId, SinkId; ulid_text, from_ulid_text, InvalidUlidText; WireRequests), BLAKE3 content ids (incl. ConfigHash), secret digests
├── support.rs             NonEmpty, NonBlank, DisplayText (checked), Capped (checked: capped list with exact total), Change, Timestamp, TimeWindow (a WireRequest), ByteRange, Blake3 (hex), Similarity, Share, Finite (an f32 never NaN or infinite), Watermark; each with its wire form
├── paging.rs              PageSize, Cursor (typed by list), PageRequest (a WireRequest), Page (checked, also when decoded: InvalidPage), one marker per list (incl. AuditList, AlertList, SearchList, TopicList, ProjectionList, ResourceUseList, TransmissionList)
├── wire/                  the JSON wire contract: conventions, requests and authority
│   ├── mod.rs             conventions, WireRequest, decode_request, DecodeError, DecodeErrorKind, Rejected (checked constructors' refusals as decode errors)
│   ├── time.rs            Timestamp as RFC 3339 UTC at microsecond precision (rfc3339, parse_rfc3339), InvalidTimestamp, TooLateForText, MAX
│   ├── duration.rs        Duration as whole microseconds in a `<what>_micros` field (serde `with` module; micros, UnfitDuration)
│   └── authority.rs       compile-time checks: Caller never serializes; server-stamped records are never WireRequests
├── observed/              facts from the wire
│   ├── client.rs          IngressMode, Upstream, Dialect, CredentialRef, HarnessClaim, EndpointKind; wire data but Dialect, Stability, EndpointKind (in process)
│   ├── message.rs         Message, MessageBody (role-shaped), parts, CanonicalJson, PartRef; on the wire only PartRef, ToolCallId, ToolName (bodies stay in the blob store)
│   ├── message/
│   │   └── text.rs        Message::part_text (what a span location indexes), part_count, NoPartText, TOOL_RESULT_SEPARATOR
│   ├── exchange.rs        Exchange, WireProtocol, Transport, Continuation, ExchangeOutcome, ConnectionId (ULID text on the wire), ExchangeStage (in memory, no serde)
│   ├── agent.rs           Agent (rename), AgentLabel, IdentityEvidence, IdentityScope, AgentState, ActiveAgentState, MergeRequest (checked, stamped: never a WireRequest)
│   ├── agent/
│   │   ├── claims.rs      SeenClaim, ClaimSet (checked; observe, union over aliases; decoding orders the entries and refuses a repeated claim)
│   │   └── merge.rs       MergeRecord (checked; revert refuses a reversal before the merge or restoring an agent it did not repoint, InvalidReversal; decodes through new and revert, InvalidMergeRecord), MergedInto, Reversal, MergeVeto (checked; decoding orders the pair), MergeConflict (MergeRequest::conflict): the merge log and exact unmerge
│   └── conversation.rs    Conversation, ConversationOrigin
├── derived/               inferences, each carrying its evidence
│   ├── provenance/
│   │   ├── span.rs        Span, SpanLocation, Origin, SpanState, SpanEvent, OriginatedSpan; on the wire only SpanLocation, RelaySource
│   │   ├── fingerprint.rs Fingerprint, WinnowParams, FingerprintHit
│   │   └── matching.rs    ContentMatch (checked, also when decoded), MatchKind, Codec, Carrier
│   └── flow/
│       ├── resource.rs    Resource, Locator, ResourcePattern (matches, overlaps; a WireRequest)
│       ├── access.rs      Access, AccessOp, Extraction
│       ├── evidence.rs    Evidence, CoAccess (checked; `lag_micros` on the wire, decode checks two accesses and a positive lag)
│       ├── timing.rs      CorrelationTiming (checked; config, not wire data): evidence window, suspected TTL, settle_after
│       ├── transmission.rs Transmission, Route (resolved), TransmissionState (expire, confirmed, co_accesses), Confirmed (no sender on the wire; rebuilt on decode)
│       ├── verdict.rs     Verdict, Judgeable (TransmissionState::judgeable), TransmissionVerdict (checked; never a request), VerdictLog (from_records, InvalidVerdictLog; records carry revisions on the wire; never a request), CurrentVerdict
│       └── channel/
│           ├── mod.rs     Channel (canonical), ChannelOrigin (promoted, superseded), Supersession, Declaration (never a request), DeclaredHistory, Seed
│           ├── promotion.rs Promotion (checked; never serialized), Registered, plan, PromotionPlan, PromotionRefusal, coverage, PromotionCoverage (resource samples; decode checks InvalidCoverage), COVERAGE_CAP
│           ├── detection.rs DeclaredDetection, TrafficDetection (a superseded channel's is frozen), DetectionKind
│           └── policy.rs  Policy, PolicyKind (re-exported by L8), PolicyDecision, PolicyHistory (checked; never a request), TrafficVerdict
├── aggregates/            recomputable summaries
│   ├── access.rs          AccessEdge (not wire), WeightedAccess, BipartiteGraph (checked; wire form BipartiteParts), ResourceUse (checked), ResourceUsePage
│   ├── agents/
│   │   ├── mod.rs         AgentProfile (checked), AgentTraffic, AgentRow, AgentCluster (checked), AgentLookup, AgentDetail, AgentName: canonical agent rows and details (responses)
│   │   └── filter.rs      AgentFilter (a WireRequest; matches, text_matches), AgentText
│   ├── alert/
│   │   ├── mod.rs         AlertSubject (resolved), AlertDraft, TriageOutcome, Alert, AlertState, AlertRevision, SuppressReason; re-exports every rule type
│   │   └── rules.rs       BuiltinRule, UserRule (a WireRequest), RuleDefinition, TopicWatch, QueryWatch, StaleReason, AlertRuleDef (checked, decoded through builtin or load; set_enabled refuses a stale rule: StaleRule), AlertRuleSet (not serialized), RuleRevision
│   ├── edge.rs            EdgeKey (checked), EdgeSelector (checked; a WireRequest), Weighting (a WireRequest), TopicSlot, EdgeStats, TopologyGraph (with nodes; check, decoded through it), EdgeTotals (of), EdgeTransmissionPage
│   ├── filter.rs          TopologyFilter (shared by every linked view; a WireRequest): FilterSubject, admits, AccessSubject, admits_access, TopicVersionSelector (resolve; a WireRequest), VersionUnavailable
│   ├── node.rs            GraphNode, AgentNode, ChannelNode, CanonicalStateKind, CanonicalOriginKind, TopologyGraph::check_nodes
│   ├── projection/
│   │   ├── mod.rs         ProjectionParams (checked; a WireRequest), ProjectionSpec (decoded pinned), ProjectionInfo (checked, transitions), Fitted, FitFailure, ProjectedPoint (Finite coordinates), Projection (checked; not serialized: its info is JSON, its frame binary)
│   │   └── frame.rs       ProjectionFrame (checked; binary layout, encode, decode; served as application/octet-stream, no serde)
│   ├── quality.rs         DetectionQuality (checked, tally), QualityRow, QualityMatch, MatchClass
│   ├── retention.rs       RetentionPolicy (checked, to_drop; config, not serialized), Pin (stamped), Retention, pin/unpin/mark_dropped on TopicVersionHistory
│   ├── series.rs          BucketWidth, SeriesStep (checked), SeriesGrid (checked; a WireRequest), SeriesGrouping (a WireRequest), TopologySeries (checked), SeriesGroups
│   ├── topic.rs           Embedding (checked), EmbeddingModel, TopicModelVersion (a WireRequest), Topic (Finite term weights), TopicAssignment (not serialized)
│   ├── topic_history.rs   TopicVersionHistory (on the wire without its active index), TopicVersionInfo (checked, with retention), TopicSizes (checked), TopicLineage (checked, remap); responses
│   └── watermark.rs       PipelineFrontier (not wire), Watermark::settled, Watermarked
├── events/                what crosses the bus
│   ├── mod.rs             Envelope, BusEvent (never requests: the node stamps them), Subject (a string, the event tag)
│   ├── changed.rs         Changed: which entity a query returns changed (every store, for the live feed); Changed::promotion
│   ├── ingest.rs          L1/L3: ExchangeCaptured, ConversationDelta, AgentSeen, AgentMerged, AgentUnmerged, AgentRenamed (bus payloads; a golden per variant inside an Envelope)
│   ├── detect.rs          L4/L5: span, match, access (with its channel), channel (incl. ChannelPromoted) and transmission events (incl. VerdictSet); bus payloads inside an Envelope, never requests
│   └── insight.rs         L6–L8: TransmissionClassified, TopicVersionReady, TopicVersionActivated, TopicVersionDropped, WatermarkAdvanced, EdgeUpdated, AlertOpened, AlertChanged, AlertRuleChanged, PolicyChanged; golden inside a full Envelope each
├── interfaces/            one module per layer: traits and their errors
│   ├── l0_ingress.rs      UpstreamRouter, ClientIdentifier, ProviderAdapter, HarnessRequest and BodyDecodeError (what capture decodes from a request body, in process; not the JSON wire's), ResponseHead, ResponseFramer, WebSocketTap
│   ├── l1_canonical.rs    Normalizer, NormalizedExchange, NormalizeWarning
│   ├── l2_transport.rs    EventBus, Subscription, RetryPolicy, ConsumerGroup (a WireRequest), DeadLetter, DeadLetterStore (list, replay), BlobStore (None: dropped by retention)
│   ├── l3_reconstruction.rs IdentityResolver (merge, unmerge, rename), AgentDirectory, ClaimStore, Threader, ResolveError (incl. MergeIntoSelf)
│   ├── l3_reconstruction/
│   │   └── agents.rs      AgentReads (list, cluster, names), ActivityStore, AgentReadError
│   ├── l4_provenance.rs   Segmenter, Decoder, Fingerprinter, FingerprintIndex, SemanticMatcher
│   ├── l5_flow.rs         ResourceExtractor, ChannelDirectory, ChannelRegistry (policy history, promote with supersession, promotion coverage, resource use), Correlator; detection follows resolution
│   ├── l5_flow/
│   │   └── verdicts.rs    TransmissionVerdicts (set, log, quality), VerdictError
│   ├── l6_analysis.rs     Embedder, TopicModel, TopicCatalog (pins, retention), SearchIndex, ProjectionStore, ProjectionSource, LayoutFitter, AlertRuleEval, AlertTriage, AlertRuleStore; SearchHit and SearchResults are its only wire types
│   ├── l7_topology.rs     EdgeStore (graph, totals, channel topology, access buckets, agent traffic as a BTreeMap, series, edge drill-down, judge, drop_version, watermark), FrontierSource, EdgeError (writes), EdgeQueryError (reads)
│   ├── l8_surface.rs      QueryApi (every read, incl. the read models and export; agent_names and channel_names as BTreeMaps in id order), OperatorActions (act on a stamped ActionRequest), AlertFilter (a WireRequest); re-exports the action, error, permission and sink types
│   └── l8_surface/
│       ├── permissions.rs Caller (built only by the directory; never serialized), CallerSnapshot (checked; an audit record's plain copy of a caller, never a WireRequest), Permission, PermissionSet (an array in Permission::ALL order)
│       ├── operators.rs   AccessConfig (trusted or authenticated), OperatorDirectory (checked), Operator, OperatorName (checked text); the directory and config never serialized
│       ├── actions.rs     OperatorAction (merge_agents, kind, required_permission, subjects; stamped, never a WireRequest), ActionKind, ActionOutcome (subjects), SupersededChannels
│       ├── actions/request.rs ActionRequest (a WireRequest: one variant per action, no author; into_action stamps the caller, of, kind)
│       ├── errors.rs      QueryError, ActionError, ConflictKind (incl. RuleStale, MergeIntoSelf, ExportTooLarge), InputError (incl. SelfMerge, EmptySelection, ExcerptContextTooLong, TooManyIds, MalformedRequest); adjacently tagged on the wire
│       ├── query_errors.rs the From impls: each store error, refused request value and undecodable request (DecodeError) to one QueryError or ActionError
│       ├── lists.rs       ChannelFilter (a WireRequest, with OriginFilter and a counts-only window), AgentFilter (re-exported), AlertRuleFilter (a WireRequest), SearchRequest (a WireRequest), TopicPage
│       ├── channels.rs    ChannelRow (checked), ChannelStanding, ChannelActivity, ChannelCounts (tally, routed), SupersededInto (checked), ChannelName (checked), ChannelShape, resolve_names, PromotionPreview (from_registry; decode refuses a non-promotion conflict); responses only
│       ├── summary.rs     TransmissionSummary (of), SummaryState (per-state shape), Delivery, TopicUnder, TransmissionStateKind, TransmissionSelection (checked; a WireRequest), TransmissionPage
│       ├── evidence.rs    TransmissionEvidence (assemble; decoded through it), MatchEvidence, MatchQuotes, AccessDetail (checked), InvalidEvidence, InvalidTransmissionEvidence, EvidenceError
│       ├── excerpt.rs     ExcerptWindow (checked; DEFAULT, MATCH_ONLY; a WireRequest), Excerpt (checked: boundaries, bounds, counts that fit a part; cut), Excerpted (of; BodyDropped), ExcerptError, CutError
│       ├── overview.rs    OverviewCounts, QueueCounts (tally)
│       ├── live.rs        LiveFeed, UiEvent (id only, from Changed), LiveItem (event_name: the SSE event; its cursor is the SSE id), LiveCursor (its text on the wire), LiveEnd (EVENT_NAME), FeedWindow (checked), LiveConfig (checked; neither serialized)
│       ├── audit.rs       AuditLog, AuditEntry, AuditBody (operator, config, export), OperatorRecord (checked; keeps a CallerSnapshot), AuditOutcome, ConfigChange, AuditSubject, AuditFilter (a WireRequest)
│       ├── sinks.rs       AlertSink, SinkInfo (last_delivery adjacently tagged: succeeded or failed), SinkKind, SinkError
│       └── export/        QueryApi::export: one dataset streamed between a header and a trailer
│           ├── mod.rs     module docs and re-exports
│           ├── request.rs ExportRequest (checked; required_permission; a WireRequest), ExportDataset, ExportScope, ExportFormat, ExportLimits
│           ├── rows.rs    ExportRow and the row of each dataset (TransmissionRow: a confirmed TransmissionSummary, quotes from the evidence; Finite topic weights), RowKey (row order), projection_rows, verdict_rows
│           ├── manifest.rs ExportHeader (checked), ExportBasis, settled_window, GatewayVersion, ExportTrailer (decode checked: InvalidTrailer), ExportEnd, ExportFailure
│           ├── framing.rs ExportLine (one JSONL line: header, row or trailer), read_jsonl (the reference reader), JsonlExport, JsonlError, the Parquet footer keys
│           ├── digest.rs  canonical row encoding, RowHasher, ExportDigest (format-independent)
│           ├── seal.rs    ExportSealer (row checks, the only trailer builder), verify_export, Incomplete
│           ├── stream.rs  ExportStream (trailer always last), Export, SealedRows, RowSource, ExportSource, ExportPlanError
│           └── record.rs  ExportRecord (checked; keeps a CallerSnapshot), ExportEvent: exports in the audit log
└── tests/                 tests for the invariants checked at runtime, one module per subject
    ├── wire/              the wire contract: harness.rs (goldens, CROSSTALK_BLESS, rejection and request checks), mod.rs (the golden layout check), one module per area
    └── golden/            one file per wire shape, <area>/<name>.json; one JSONL golden (surface_reads/export/export_complete.jsonl: a complete export, line by line)
```

## Conventions

- **Invalid states are unrepresentable where the type system allows it.**
  Examples: a message's role is its body variant, so a tool call can only
  appear in an assistant message. A confirmed transmission holds a
  `NonEmpty<ContentMatch>`. A channel declared before traffic and a
  discovered or promoted channel have different detection enums. Only a
  user rule can be stale, and each built-in rule has its own slot in the
  rule set, so a second instance cannot be built. A verdict record can only
  be built for a transmission whose state takes one
  (`TransmissionVerdict::new`).
- **Opaque newtypes for server-issued values.** Cursors have private
  fields; clients only hand them back. A cursor's list is a type
  parameter, so one list's cursor does not fit another.
- **Errors are typed end to end.** Each store's error enum maps to
  `QueryError` (or, behind an action, `ActionError`) through one `From` impl
  in `l8_surface/query_errors.rs`, and so does each checked request value
  the surface builds before reading (an id batch, a selection, an excerpt
  window), so adding a variant forces a decision about what the UI sees. Reads and writes of the edge store fail with
  different enums (`EdgeQueryError`, `EdgeError`).
- **Exhaustive matches, no wildcards.** `OperatorAction::kind`,
  `required_permission` and `subjects`, `UiEvent::from` and the error
  mappings match every variant, and the tests list every variant behind an
  exhaustive match, so a new action, event or error does not compile until
  each is decided.
- **Checked constructors for the rest.** When an invariant spans values
  (a content match's reader is not its origin agent; every match in a
  confirmed transmission has one sender), the type has private fields and a
  constructor that returns a `Result`. Each has a test in `tests/`.
- **Lifecycles are state enums with per-state data.** Each entity's state
  enum carries only what is known in that state. A transmission's sender is
  unknown until content evidence arrives, so it lives in `Confirmed`.
- **Ids never cross types.** Entity ids are ULIDs and content ids are BLAKE3
  digests, each its own newtype.
- **The types are the wire format.** The only dependencies are `serde`
  and `serde_json`, pinned exactly (the UI's pins, declared once in the
  workspace's `[workspace.dependencies]`; the root `Cargo.lock` is
  committed). Structs are objects with snake_case keys; enums with data
  are adjacently tagged (`{"type": "snake_case", "data": ..}`) and
  all-unit enums are snake_case strings; entity ids are ULID text, digests
  lower-case hex, timestamps RFC 3339 UTC at microsecond precision,
  durations whole microseconds in `_micros` fields; floats are always
  behind a checked finite type and id-keyed maps are `BTreeMap`s.
  Decoding is strict (`deny_unknown_fields`) and a checked type decodes
  only through its constructor (a private raw mirror and `TryFrom`, with
  `wire::Rejected` as the error). What a client may send is a
  `wire::WireRequest`; a `Caller` never serializes and nothing the server
  stamps is a request. A golden file in `tests/golden/` pins every shape.
  See `wire/mod.rs` and `docs/features/wire_contract.md`.
- **Error enums are plain.** Implementations derive `thiserror::Error` on
  their copies; serde's `Display` requirement is met by `wire::Rejected`.

## Harnesses, upstreams and credentials

The model supports Claude Code, Codex, pi and oh-my-pi against vendor APIs,
subscription backends (Claude Pro/Max, ChatGPT/Codex, GitHub Copilot, Gemini
Code Assist) and self-hosted vLLM or SGLang. See
`docs/research/harness-wire-protocols.md` for what each sends. In short:

- **Wire protocol, upstream and dialect are separate axes.** Adapters and
  normalizers are per `WireProtocol` and handle every `Dialect`; the dialect
  follows from the configured `UpstreamKind`.
- **Two ingress modes.** Reverse proxy (the harness's base URL points at a
  route) and forward proxy (`HTTPS_PROXY` with TLS interception for
  allowlisted hosts only), for upstreams a harness cannot redirect.
- **Three transports.** HTTP, SSE, and WebSocket, where one connection
  carries many exchanges and each turn sends only an increment
  (`Continuation::Increment`).
- **Credentials are hashed and classified.** API keys are stable, OAuth and
  exchanged tokens rotate, server keys are shared. Only stable credentials
  identify an agent on their own.
- **Harness headers are claims.** Session and agent ids count as identity
  evidence only within the credential or account they arrive with; the
  harness name is never evidence.
- **Requests are forwarded before they are decoded.** Routing and identity
  read only the head. The body decodes concurrently, off the hot path, and
  the response framer is chosen from the response head. A request that
  fails to decode is still forwarded, just not captured.
- **Only generation is captured.** Token counting, model listing, probes and
  side routes are forwarded and not captured.
- **Merges and supersessions are aliases.** Stored records keep their agent
  and channel ids and readers resolve them through `AgentDirectory` and
  `ChannelDirectory` (`aliases.rs`), whose caches must apply every merge and
  unmerge. Merges are a log of `MergeRecord`s; an operator unmerge reverts
  one record exactly and records a `MergeVeto`. A promotion supersedes the
  discovered channels its pattern covers; nothing undoes a supersession. A
  superseded channel's detection is frozen, and a confirmation routed
  through it advances its superseding channel's.
- **Harness claims are aggregated for display.** L3 keeps the distinct
  claims seen per attributed agent; a canonical agent shows the union over
  its aliases. Claims are never identity evidence.
- **Agent rows are canonical.** The agents list shows canonical agents
  only, with claims and last-seen times unioned over their aliases and
  transmission counts over the query's window, equal to their graph node
  counts; an agent's detail follows a merged id to its canonical agent and
  says so. A merge of two ids of one cluster is `MergeIntoSelf`.
- **Labels are display only.** An agent label is never identity evidence,
  and only an active agent can be renamed.
- **Verdicts sit beside detection.** An operator's `Genuine` or
  `FalseDetection` verdict never changes a transmission's state; it is an
  append-only label used to exclude false detections from views, suppress
  their alerts and measure the detector (`DetectionQuality`).

## Mapping from the lifecycle definition

`design/lifecycles/cascade.yaml` (next to the repository) simulates these
lifecycles, with scenarios for channel discovery, sanctioned and unused
channels, suspected transmissions, delegation, direct relays, late content,
policy resets, topic re-fits, promotion with supersession (and a late
confirmation on the superseded channel), merges and exact unmerges with
vetoes, verdicts set and revised, stale rules refused on enable and
retargeted by update, topic-version pins and retention, and projection
jobs requeued after a crash. The model agrees with these types on
behaviour; the rows below are where it represents something differently,
either to work around the simulator or because the types keep it
in-process. In general, a refused operator action is a trigger the target
drops in its current state, where the types return a typed `ActionError`,
and the `Changed` notifications and the live feed are not modelled.

| Cascade | Here |
| --- | --- |
| **Agents** | |
| `Agent.active.registered` | `AgentState::Registered`. Scenarios pre-declare every agent, so one first seen in traffic starts `registered` there and `Provisional` here |
| `Agent.active` (`registered`, `provisional`, `established`) and `merged`; `unmerge` and `rename` re-enter `active` through its deep history `prior` | `ActiveAgentState`, and `AgentState::Merged(MergedInto { prior, .. })`; `Agent::revert` restores `prior`, and `Agent::rename` changes only the `label` |
| `Agent.merge`, fired by the operator (target in the payload's `into`) or by `ResolverMergeFound`; the target-is-canonical check is a lone guard, always taken | `IdentityResolver::merge(MergeRequest { by: Operator \| Resolver })`, which refuses a merged source or target (`Conflict(AgentMerged)`) |
| `MergeRecord` machine, spawned on `AgentMerged` (`unwritten` → `applied` → `reverted`); `MergeRecord.revert` is the operator's `Unmerge` | `MergeRecord`, written in the merge's transaction and returned as its `MergeId`; `MergeRecord::revert` refuses a second revert (`Conflict(MergeAlreadyReverted)`) |
| `ResolverMergeFound`, `MergeReverted` | in-process in L3: the resolver's merge decision, and `MergeRecord::revert` inside `IdentityResolver::unmerge` |
| no repointing: an agent merged into a source that is merged again keeps pointing where it did | `Agent::repoint` and `Agent::restore`, with `MergeRecord::repointed` and `Reversal::restored` (instance fields cannot change in the simulator) |
| `MergeVeto` machine per directed agent pair (`none`, `vetoed`), consulted by the resolver when an exchange's `sameAs` stand-in carries the other agent's strong evidence; an operator merge from `from` into `into` clears it | `MergeVeto` stores the pair unordered and `separates` tests whole clusters; an operator merge deletes every veto between the two clusters. The cascade checks only the declared pair, in its declared direction |
| `AgentUnmerged`, `AgentRenamed` handled by `ReadModels` with no rules | applied by the `AgentDirectory` cache and the graph nodes' labels; no entity changes state |
| **Channels** | |
| `Channel.undiscovered` | no record: a channel exists once declared or discovered |
| `Channel` top-level states `declared`, `discovered`, `promoted`, `superseded`, each holding its detection; the traffic detection transitions are written out once per origin that runs them | `ChannelOrigin` (`Declared` with `DeclaredHistory::BeforeTraffic` or `Promoted`, `Discovered`, `Superseded`) holding `DeclaredDetection` or `TrafficDetection` |
| `Channel.declared.awaiting_traffic` / `unused`; `declared.observed` … `dormant` | `DeclaredDetection::AwaitingTraffic` / `Unused` / `InUse(TrafficDetection)` |
| `Channel.discovered.*`, `promoted.*` | `TrafficDetection`; promotion keeps the detection leaf (`ChannelOrigin::promoted`) |
| `Channel.superseded.*`: the leaf it had, with no transitions out (detection frozen) | `ChannelOrigin::Superseded { seed, detection, supersession: Supersession { by, at } }` |
| `Channel.promote`, refused (dropped) in a declared, promoted or superseded channel; the pattern check is a lone guard | `PromoteChannel { channel, pattern, policy, note }` → `Promotion` → `ChannelRegistry::promote` and `promotion::plan`, refusing `ChannelSuperseded`, `ChannelNotDiscovered`, `PatternMissesSeed`, `PatternOverlaps` |
| `Channel.supersededBy` stand-in; `ChannelPromoted` supersedes every channel naming the promoted one | `promotion::plan`: every other discovered channel whose seed the pattern matches |
| the promotion's policy decision, chosen by the `ChannelPolicy.promotion` stand-in through literal selectors (`allow`, `disallow`, `reset`) | the `PolicyDecision` in the `Promotion`, recorded in the promoted channel's `PolicyHistory` in the promotion's transaction |
| an `Exchange.resource` after a promotion names the promoted channel | `ChannelRegistry::lookup` returns `Known(canonical)` for a superseded channel's resources, so new accesses land on the superseding channel |
| a late `Channel.confirm` on a superseded channel is dropped and not forwarded | differs: the superseded channel's detection stays frozen, but the confirmation advances the superseding channel's detection (`TrafficDetection::Active::last_transmission`), as read-time resolution counts the route and applies the policy there (`l5_flow`, "Detection follows resolution"). `cascade.yaml` still drops it |
| `ChannelPolicy` machine | `Policy` on `Channel`, with `Policy::on_traffic`; `unreviewed.never_reviewed` / `unreviewed.reset` are `Unreviewed(None)` / `Unreviewed(Some(_))`; its `unused` trigger is the `SanctionedUnused` rule's policy check |
| `ChannelPolicy.superseded.*`: the policy frozen where it was, taking no `allow`, `disallow` or `reset` | the superseded channel keeps its `Policy` and `PolicyHistory`; `ChannelRegistry::set_policy` refuses (`Superseded { channel, by }`) and the surface returns `Conflict(ChannelSuperseded)` |
| `SupersededTraffic`, forwarded to the superseding channel's policy (a one-step cycle marked `bounded`) | in-process: confirmed traffic on a superseded channel is judged by the policy of `ChannelDirectory::canonical`; the alert keeps the superseded channel as its stored subject |
| `ChannelSanctioned` | `PolicyChanged { policy: Sanctioned(_) }`; other policy changes are applied by the policy transition alone |
| `Alert.supersededBy`, copied from the channel's stand-in when the alert is raised; `ChannelSanctioned` suppresses alerts whose `subject` or `supersededBy` is the channel | `AlertTriage::channel_sanctioned` compares resolved subjects (`AlertSubject::resolved`) at suppression time |
| **Transmissions and verdicts** | |
| `ContentMatched` / `DelegationMatched` / `DirectMatched` / `OutputMatched`, selected by `Exchange` stand-in fields | one `ContentMatched` carrying a `Carrier`; the correlator chooses the route (delegation direction and the direct carrier are not modelled) |
| no sender field; `originAgent` in the confirming match's payload | `Confirmed::from()`, known only once confirmed |
| `Transmission.judgeable` compound (`suspected`, `confirmed`, `classified`, `aggregated`, `discarded`); verdict triggers drop in `detected` and `awaiting_content` | `TransmissionState::judgeable` (`Judgeable` / `NotJudgeable`), checked by `TransmissionVerdict::new`; the surface returns `Conflict(TransmissionNotJudgeable)` |
| `Transmission.judgeable.aggregated` is not final (re-fits loop through it) | `Aggregated` is final; a re-fit records a new `TopicAssignment` |
| `Transmission.judgeable.discarded` is final for detection but takes verdict self-loops (through the parent) | `Discarded` is terminal and judgeable |
| `TransmissionOpened`, which spawns the transmission's `Verdict` | no bus event: a transmission's `VerdictLog` is empty until its first record |
| `Verdict` machine: `unlogged`, then `no_verdict` / `genuine` / `false_detection`; each transition is one appended record | the last record of the `VerdictLog` (`None` when never judged or withdrawn), with consecutive `VerdictRevision`s, which the cascade does not count |
| `JudgedGenuine`, `JudgedFalseDetection`, `JudgementWithdrawn` | in-process in `TransmissionVerdicts::set`: the state check passed |
| `VerdictGenuine` / `VerdictFalseDetection` / `VerdictWithdrawn` | `VerdictSet { verdict: Some(Genuine) \| Some(FalseDetection) \| None, revision, .. }` |
| a repeated verdict: the transmission's self-loop is taken and the `Verdict` drops the trigger | `Unchanged`: nothing appended, nothing published |
| `VerdictGenuine` and `VerdictWithdrawn` handled with no rules | every reader updates its `CurrentVerdict`; triage reopens nothing |
| `TransmissionClassified` / `TransmissionReclassified` | `TransmissionClassified { cause: Confirmation \| Refit }` |
| a late `match` is dropped in `discarded` | late content opens a new transmission; the simulator only shows this for content from a later exchange |
| a confirmation on an `active` channel is dropped | it updates `TrafficDetection::Active::last_transmission` |
| no correlator buffering | a tool-result match whose call yields no access opens `Direct(ToolResult)` when its window closes |
| a late match is held back with manual queue steps (`{ step: 1 }`) in the promotion scenario | the `provenance` and `flow` consumer groups lag independently, so a match can reach the correlator after its window closed |
| **Alerts and rules** | |
| `Alert.fired` / `deduplicated` / `rejected` / `inactive` | `AlertDraft` / `TriageOutcome::Deduplicated` / `OperatorRejected` / `RuleInactive`; the simulator always takes the first guard (`open`), so the other three are never reached |
| `Alert.subjectKind` / `subject` | `AlertSubject`: the channel for new-channel, traffic and sanctioned-unused alerts, the transmission for suspected-transmission and content alerts |
| `ContentRule` machine: `unsaved`, then `current.{enabled,disabled}` and `stale.{enabled,disabled}` | a user `AlertRuleDef` (`ContentRule::WatchedTopic` / `SemanticQuery`): `current` / `stale` is `TopicWatch` or `QueryWatch` `Current` / `Stale`, the children are `RuleStatus`; `unsaved` is no record before `AlertRuleStore::create` |
| `ContentRule.enable` drops in either `stale` leaf | `AlertRuleDef::set_enabled` refuses with `StaleRule`, `AlertRuleStore::set_enabled` with `RuleError::Stale`, and the surface returns `Conflict(RuleStale)`; the rule is unchanged |
| `ContentRule.update` from `stale` to `current.enabled`, and from `current` back to the status it had | `AlertRuleStore::update` / `AlertRuleDef::update`: a stale rule is retargeted and enabled, a current one keeps its status |
| `ContentRule.unmappedIn` stand-in: on `TopicVersionReady` the rules naming the version take `remap_stale` before the others take `remap` | `AlertRuleDef::remap` with `TopicLineage::remap` over the stored lineage and the rule's threshold |
| `ContentRule.embedding_model_changed`, fired by `Config` | `AlertRuleDef::embedding_model_changed` when alerts starts with an embedder of another model |
| `RuleDisabled`; other rule changes are `AlertRuleChanged`, handled with no rules | `AlertRuleChanged`; a disable also calls `AlertTriage::rule_disabled` in the same transaction (`SuppressReason::RuleDisabled`) |
| built-in rules are `AlertEngine` rules that always fire | `BuiltinRule`s can be enabled and disabled like user rules |
| **Topic versions and projections** | |
| `TopicVersion` machine; `planned` is a re-fit that has not started, `abandoned` a failed fit | `TopicVersionInfo` in the `TopicVersionHistory`; a failed fit leaves no version |
| `TopicVersion.fitting.running` / `fitting.classifying` | both `TopicVersionStatus::Fitting` |
| `TopicVersion` `ready`, `active`, `superseded`, each `unpinned` or `pinned`, and `dropped` | `TopicVersionStatus` with `Retention::Retained { pin }`, and `Retention::Dropped` |
| `TopicModelRefitted` | the analyze consumer's in-process fit, after the lineage is stored; `TopicVersionReady` is the bus event |
| `TopicVersion.activate` on `TopicVersionReady` | L7 activates a version once its buckets are complete; until then it is `Ready` and a pinned view of it is `Conflict(TopicVersionNotActivated)` |
| `TopicVersion.supersededBy` / `droppedAfter` stand-ins, selected on `TopicVersionActivated` | `TopicVersionActivated { version, previous }` supersedes every older version; `RetentionPolicy::to_drop` decides drops from `keep_last`, pins and activation |
| `TopicVersionUnpinned`, after which `enforce_retention` is a lone guard, always taken | in-process in the catalog; `to_drop` keeps a version still among the `keep_last` most recent active ones, which the simulator would drop |
| `TopicVersion.pin` drops in `planned`, `fitting`, `dropped` and when pinned; `unpin` drops when unpinned | `NotFound`, `Conflict(TopicVersionFitting)`, `Conflict(TopicVersionDropped)`, `Unchanged` |
| `Projection` machine: `unrequested`, `queued`, `fitting`, `ready`, `failed`, `expired`; `Fitter` fires claim, complete and fail, and `Clock` the lease lapse and frame retention | `ProjectionInfo` / `ProjectionStatus` with `start`, `requeue`, `complete`, `fail` and `expire`; `unrequested` is no record before `ProjectionStore::enqueue`, whose `Conflict(ProjectionQueueFull)` is a lone guard here |
| `Projection.version_dropped` from `queued` on `TopicVersionDropped` | `Failed { started_at: None, .. }`; a fitting job finds the version gone when it reads its sample |
| **Elsewhere** | |
| `ResponseCompleted`, `ExchangeFailed`, `ExchangeNormalized` | in-process on the proxy node (`RawExchange`, `NormalizedExchange`) |
| guarded triggers take their first guard (`Span.classify`, `Agent.evidence`) | the guard is decided by the data: a reader-output span is `Relayed`, and an agent is `Established` only with corroborating evidence |
| no watermark | the `Watermark` has no lifecycle; it decides when buckets are final, and no machine models buckets |
