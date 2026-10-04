# Crosstalk type specification

The data model of the gateway, written as Rust so it type-checks. The crate
in `spec/Cargo.toml` builds these modules as a library and runs their tests;
it is not part of the gateway's build.

```sh
cargo check --manifest-path spec/Cargo.toml
cargo test  --manifest-path spec/Cargo.toml
```

## Layout

```text
spec/types/
├── mod.rs                 crate root: the three tiers, events, interfaces
├── ids.rs                 typed ids: ULID entity ids (incl. AuditId), BLAKE3 content ids
├── support.rs             NonEmpty, NonBlank, Timestamp, TimeWindow, ByteRange, Similarity, Share
├── paging.rs              PageSize, Cursor (typed by list), PageRequest, Page (checked), list markers (incl. AuditList)
├── observed/              facts from the wire
│   ├── client.rs          IngressMode, Upstream, Dialect, CredentialRef, HarnessClaim, EndpointKind
│   ├── message.rs         Message, MessageBody (role-shaped), parts, CanonicalJson, PartRef
│   ├── exchange.rs        Exchange, WireProtocol, Transport, Continuation, ExchangeOutcome, ExchangeStage
│   ├── agent.rs           Agent, IdentityEvidence, IdentityScope, AgentState, Merged, MergeRequest
│   ├── agent/
│   │   └── label.rs       AgentLabel, LabelLog, LabelView (display labels)
│   └── conversation.rs    Conversation, ConversationOrigin
├── derived/               inferences, each carrying its evidence
│   ├── provenance/
│   │   ├── span.rs        Span, SpanLocation, Origin, SpanState, SpanEvent, OriginatedSpan
│   │   ├── fingerprint.rs Fingerprint, WinnowParams, FingerprintHit
│   │   └── matching.rs    ContentMatch, MatchKind, Codec, Carrier
│   └── flow/
│       ├── resource.rs    Resource, Locator, ResourcePattern
│       ├── access.rs      Access, AccessOp, Extraction
│       ├── evidence.rs    Evidence, CoAccess (checked)
│       ├── transmission.rs Transmission, Route, TransmissionState, DiscardReason, Dismissal, Confirmed
│       └── channel/
│           ├── mod.rs     Channel, ChannelOrigin, Declaration, DeclaredHistory, Seed
│           ├── detection.rs DeclaredDetection, TrafficDetection
│           └── policy.rs  Policy, PolicyKind (re-exported by L8), PolicyDecision, PolicyHistory (checked), TrafficVerdict
├── aggregates/            recomputable summaries
│   ├── edge.rs            EdgeKey (checked), EdgeSelector (checked), TopicSlot, EdgeStats, TopologyGraph, EdgeTransmissionPage
│   ├── filter.rs          TopologyFilter (shared by every linked view), FilterSubject, admits, TopicVersionSelector (resolve), VersionUnavailable
│   ├── projection/
│   │   ├── mod.rs         ProjectionParams (checked), ProjectionSpec, ProjectionInfo (checked, transitions), Fitted, FitFailure, Projection (checked)
│   │   └── frame.rs       ProjectionFrame (checked; binary layout, encode, decode)
│   ├── series.rs          BucketWidth, SeriesStep, SeriesGrid, TopologySeries (checked), SeriesGroups
│   ├── topic.rs           Embedding (checked), EmbeddingModel, Topic, TopicAssignment
│   ├── topic_history.rs   TopicVersionHistory, TopicSizes, TopicLineage (checked, remap)
│   └── alert.rs           AlertRule, TopicWatch, ContentRule, AlertRuleDef, RuleStatus, AlertDraft, TriageOutcome, Alert, AlertState, AlertRevision
├── events/                what crosses the bus
│   ├── mod.rs             Envelope, BusEvent, Subject
│   ├── ingest.rs          L1/L3: ExchangeCaptured, ConversationDelta, AgentSeen, AgentMerged, AgentUnmerged
│   ├── detect.rs          L4/L5: span, match, access, channel and transmission events (incl. TransmissionDismissed)
│   └── insight.rs         L6–L8: TransmissionClassified, TopicVersionReady, TopicVersionActivated, EdgeUpdated, AlertOpened, AlertChanged, PolicyChanged
├── interfaces/            one module per layer: traits and their errors
│   ├── l0_ingress.rs      UpstreamRouter, ClientIdentifier, ProviderAdapter, ResponseHead, ResponseFramer, WebSocketTap
│   ├── l1_canonical.rs    Normalizer, NormalizedExchange, NormalizeWarning
│   ├── l2_transport.rs    EventBus, Subscription, RetryPolicy, DeadLetterStore (list, replay), BlobStore
│   ├── l3_reconstruction.rs IdentityResolver (merge, unmerge, set_label), AgentDirectory, Threader
│   ├── l4_provenance.rs   Segmenter, Decoder, Fingerprinter, FingerprintIndex, SemanticMatcher
│   ├── l5_flow.rs         ResourceExtractor, ChannelRegistry (policy history, promote), Correlator, TransmissionReview
│   ├── l6_analysis.rs     Embedder, TopicModel, TopicCatalog, SearchIndex, ProjectionStore, ProjectionSource, LayoutFitter, AlertRuleEval, AlertTriage, AlertRuleStore
│   ├── l7_topology.rs     EdgeStore (graph, series, edge drill-down), EdgeError, EdgeQueryError
│   ├── l8_surface.rs      Caller, Permission, QueryApi (lists, linked views, series, topic history, policy history, projections, audit), QueryError, ConflictKind, InputError, OperatorAction, ActionKind, OperatorActions, AlertSink
│   └── l8_surface/
│       ├── lists.rs       ChannelFilter, AgentFilter, AlertRuleFilter, SearchRequest, TopicPage
│       ├── query_errors.rs From impls: each store error to one QueryError
│       ├── live.rs        LiveFeed, LiveUpdate, UpdateKinds, LiveScope, LiveCursor, FeedWindow, LiveConfig
│       └── audit.rs       AuditLog, AuditRecord (checked), AuditOutcome, AuditFilter
└── tests/                 tests for the invariants checked at runtime
```

## Conventions

- **Invalid states are unrepresentable where the type system allows it.**
  Examples: a message's role is its body variant, so a tool call can only
  appear in an assistant message. A confirmed transmission holds a
  `NonEmpty<ContentMatch>`. A channel declared before traffic and a
  discovered or promoted channel have different detection enums. Only a
  watched-topic rule can be stale.
- **Opaque newtypes for server-issued values.** Cursors have private
  fields; clients only hand them back. A cursor's list is a type
  parameter, so one list's cursor does not fit another.
- **Errors are typed end to end.** Each store's error enum maps to
  `QueryError` through one `From` impl, so adding a variant forces a
  decision about what the UI sees.
- **Checked constructors for the rest.** When an invariant spans values
  (a content match's reader is not its origin agent; every match in a
  confirmed transmission has one sender), the type has private fields and a
  constructor that returns a `Result`. Each has a test in `tests/`.
- **Lifecycles are state enums with per-state data.** Each entity's state
  enum carries only what is known in that state. A transmission's sender is
  unknown until content evidence arrives, so it lives in `Confirmed`.
- **Ids never cross types.** Entity ids are ULIDs and content ids are BLAKE3
  digests, each its own newtype.
- **No dependencies.** Error enums are plain; implementations derive
  `thiserror::Error`, and serde and sqlx derives, on their copies.

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
- **Merges are aliases.** Stored records keep their agent ids and readers
  resolve them through `AgentDirectory`. An operator unmerge restores the
  merge table exactly from the `Merged` record.
- **Labels are display only.** An agent label is never identity evidence.

## Mapping from the lifecycle definition

`design/lifecycles/cascade.yaml` (next to the repository) simulates these
lifecycles, with scenarios for channel discovery, sanctioned and unused
channels, suspected transmissions, delegation, direct relays, late content,
policy resets and topic re-fits. The model agrees with these types on
behaviour; the rows below are where it represents something differently,
either to work around the simulator or because the types keep it in-process:

| Cascade | Here |
| --- | --- |
| `Agent.registered` | `AgentState::Registered`. Scenarios pre-declare every agent, so one first seen in traffic starts `registered` there and `Provisional` here |
| `Channel.undiscovered` | no record: a channel exists once declared or discovered |
| `Channel.declared` / `unused` | `DeclaredDetection::AwaitingTraffic` / `Unused` (under `DeclaredHistory::BeforeTraffic`) |
| `Channel.observed` … `dormant` | `TrafficDetection` |
| `ChannelPolicy` machine | `Policy` on `Channel`, with `Policy::on_traffic`; `unreviewed.never_reviewed` / `unreviewed.reset` are `Unreviewed(None)` / `Unreviewed(Some(_))`; its `unused` trigger is the `SanctionedUnused` rule's policy check |
| `ChannelSanctioned` | `PolicyChanged { policy: Sanctioned(_) }`; other policy changes are applied by the policy transition alone |
| `Alert.fired` / `deduplicated` | `AlertDraft` / `TriageOutcome::Deduplicated` |
| `Alert.subjectKind` / `subject` | `AlertSubject`: the channel for new-channel, traffic and sanctioned-unused alerts, the transmission for suspected-transmission and content alerts |
| `ContentRule` machine | `AlertRuleDef` with `WatchedTopic` / `SemanticQuery`; its `enabled` is `RuleStatus::Enabled` and its `stale` is `TopicWatch::Stale` (`Disabled` and `RuleDisabled` suppression are not modelled) |
| `TopicModel` machine, `TopicModelRefitted` | the analyze consumer's in-process fit; `TopicVersionReady` is the bus event |
| `ResponseCompleted`, `ExchangeFailed`, `ExchangeNormalized` | in-process on the proxy node (`RawExchange`, `NormalizedExchange`) |
| `ContentMatched` / `DelegationMatched` / `DirectMatched` / `OutputMatched`, selected by `Exchange` stand-in fields | one `ContentMatched` carrying a `Carrier`; the correlator chooses the route (delegation direction and the direct carrier are not modelled) |
| `TransmissionClassified` / `TransmissionReclassified` | `TransmissionClassified { cause: Confirmation \| Refit }` |
| no sender field; `originAgent` in the confirming match's payload | `Confirmed::from()`, known only once confirmed |
| `Transmission.aggregated` is not final (re-fits loop through it) | `Aggregated` is final; a re-fit records a new `TopicAssignment` |
| a late `match` is dropped in `discarded` | late content opens a new transmission; the simulator only shows this for content from a later exchange |
| a confirmation on an `active` channel is dropped | it updates `TrafficDetection::Active::last_transmission` |
| no correlator buffering | a tool-result match whose call yields no access opens `Direct(ToolResult)` when its window closes |
| guarded triggers take their first guard (`Span.classify`, `Agent.evidence`) | the guard is decided by the data: a reader-output span is `Relayed`, and an agent is `Established` only with corroborating evidence |
