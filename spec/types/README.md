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
├── ids.rs                 typed ids: ULID entity ids, BLAKE3 content ids
├── support.rs             NonEmpty, Timestamp, TimeWindow, ByteRange, Similarity, Share
├── observed/              facts from the wire
│   ├── message.rs         Message, MessageBody (role-shaped), parts, PartRef
│   ├── exchange.rs        Exchange, ExchangeMeta, ExchangeOutcome, ExchangeStage
│   ├── agent.rs           Agent, IdentityEvidence, AgentState
│   └── conversation.rs    Conversation, ConversationOrigin
├── derived/               inferences, each carrying its evidence
│   ├── provenance/
│   │   ├── span.rs        Span, SpanLocation, Origin, SpanState
│   │   ├── fingerprint.rs Fingerprint, WinnowParams, FingerprintHit
│   │   └── matching.rs    ContentMatch, MatchKind, Codec, Carrier
│   └── flow/
│       ├── resource.rs    Resource, Locator, ResourcePattern
│       ├── access.rs      Access, AccessOp, Extraction
│       ├── evidence.rs    Evidence, CoAccess
│       ├── transmission.rs Transmission, Route, TransmissionState, Confirmed
│       └── channel/
│           ├── mod.rs     Channel, ChannelOrigin
│           ├── detection.rs DeclaredDetection, TrafficDetection
│           └── policy.rs  Policy, Decision, TrafficVerdict
├── aggregates/            recomputable summaries
│   ├── edge.rs            EdgeKey, EdgeStats, TopologyGraph, Weighting
│   ├── topic.rs           Embedding, Topic, TopicAssignment
│   └── alert.rs           AlertRule, AlertDraft, TriageOutcome, Alert, AlertState
├── events/                what crosses the bus
│   ├── mod.rs             Envelope, BusEvent, Subject
│   ├── ingest.rs          L1/L3: ExchangeCaptured, ConversationDelta, AgentSeen, AgentMerged
│   ├── detect.rs          L4/L5: span, match, access, channel and transmission events
│   └── insight.rs         L6–L8: TransmissionClassified, EdgeUpdated, AlertOpened, PolicyChanged
├── interfaces/            one module per layer: traits and their errors
│   ├── l0_ingress.rs      ProviderAdapter, ResponseFramer, RawExchange
│   ├── l1_canonical.rs    Normalizer, NormalizedExchange
│   ├── l2_transport.rs    EventBus, Subscription, BlobStore
│   ├── l3_reconstruction.rs IdentityResolver, Threader
│   ├── l4_provenance.rs   Segmenter, Decoder, Fingerprinter, FingerprintIndex
│   ├── l5_flow.rs         ResourceExtractor, ChannelRegistry, Correlator
│   ├── l6_analysis.rs     Embedder, TopicModel, SearchIndex, AlertRuleEval, AlertTriage
│   ├── l7_topology.rs     EdgeStore
│   └── l8_surface.rs      QueryApi, OperatorActions, AlertSink
└── tests/                 tests for the invariants checked at runtime
```

## Conventions

- **Invalid states are unrepresentable where the type system allows it.**
  Examples: a message's role is its body variant, so a tool call can only
  appear in an assistant message. A confirmed transmission holds a
  `NonEmpty<ContentMatch>`. A declared channel and a discovered channel have
  different detection enums.
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

## Mapping from the lifecycle definition

`design/lifecycles/cascade.yaml` (next to the repository) simulates these
lifecycles. Some of its states exist only to work around the simulator, and
some of its events are in-process here rather than on the bus:

| Cascade | Here |
| --- | --- |
| `Agent.unseen` | `AgentState::Registered` (declared in config); agents first seen in traffic start `Provisional` |
| `Channel.undiscovered` | no record: a channel exists once declared or discovered |
| `Channel.declared` / `unused` | `DeclaredDetection::AwaitingTraffic` / `Unused` |
| `Channel.observed` … `dormant` | `TrafficDetection` |
| `ChannelPolicy` machine | `Policy` on `Channel`, with `Policy::on_traffic` |
| `Alert.fired` / `deduplicated` | `AlertDraft` / `TriageOutcome::Deduplicated` |
| `ResponseCompleted`, `ExchangeFailed`, `ExchangeNormalized` | in-process on the proxy node (`RawExchange`, `NormalizedExchange`) |
| `Transmission.fromAgent` field | `Confirmed::from()`, known only once confirmed |
