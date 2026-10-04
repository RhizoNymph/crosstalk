//! The interface each layer of the abstraction stack exposes.
//!
//! One module per layer. Each module holds the layer's traits, the types
//! that only cross that layer's boundary, and the layer's error enums.
//! Implementations derive `thiserror::Error` on the error enums; they are
//! plain enums here so the spec has no dependencies.
//!
//! Components that keep state (the correlator, a directory's cache) own it
//! inside one task and receive work over channels. Traits take `&mut self`
//! for that state rather than hiding it behind locks.
//!
//! Async methods are declared as `fn ...(...) -> impl Future<Output = T> +
//! Send`, never as a bare `async fn`, so code generic over a trait can run
//! its futures on a multi-threaded executor; implementations still write
//! `async fn`. Associated streams and per-connection handles are
//! `Send + 'static`. `tests/send.rs` checks both at compile time.
//!
//! | Layer | Module | Triggered by | Emits |
//! |---|---|---|---|
//! | L0 | [`l0_ingress`] | harness request, upstream chunks | `RawExchange` (in-process) |
//! | L1 | [`l1_canonical`] | `RawExchange` | `ExchangeCaptured` |
//! | L2 | [`l2_transport`] | `publish` from any layer | deliveries to consumer groups |
//! | L3 | [`l3_reconstruction`] | `ExchangeCaptured` | `ConversationDelta`, `AgentSeen`, `AgentMerged` |
//! | L4 | [`l4_provenance`] | `ConversationDelta` | `SpanOriginated`, `SpanRelayed`, `ContentMatched` |
//! | L5 | [`l5_flow`] | `ConversationDelta`, `ContentMatched`, clock, policy | channel and transmission events |
//! | L6 | [`l6_analysis`] | `TransmissionConfirmed`, `TopicVersionActivated`, clock, detect events | `TransmissionClassified`, `TopicVersionReady`, `AlertOpened`, `AlertChanged` |
//! | L7 | [`l7_topology`] | `TransmissionClassified`, `TopicVersionReady`, `AccessRecorded` | `EdgeUpdated`, `TopicVersionActivated` |
//! | L8 | [`l8_surface`] | `AlertOpened`, `Changed`, operator, config | `PolicyChanged`, agent merges, alert actions (through the stores that own them), SSE `UiEvent`s |
//!
//! Every store whose entities a surface query returns (L3 agents, L5
//! channels and verdicts, L6 alerts, rules, topic versions and projection
//! jobs, L7's watermark) also publishes [`Changed`](crate::events::changed::Changed)
//! after every committed change to one of them, for the live feed.
//!
//! **Stores have a spec write side.** Every write a layer's consumer, the
//! surface or config makes to a stateful store is a method of a spec trait
//! (agent creation and state changes, channel discovery and traffic,
//! transmissions, topic fits and assignments, search indexing, rule upkeep,
//! alert acknowledgement, edge activation, the operator directory, sink
//! deliveries). An in-memory store and a Postgres store implement the same
//! traits, so a model-based harness drives either through the spec alone.
//! A store's reads of another layer's state go through that layer's spec
//! read traits (`AgentDirectory`, `ChannelDirectory`, `NodeFacts`,
//! `WatermarkRead`), never through an implementation's hooks.
//!
//! **A store publishes what it decides.** A store write that commits a
//! change appends, in the transaction that makes it and so never before it
//! is visible, `Changed` for every entity it changed and every bus event
//! its method documents: the events announcing a decision taken inside the
//! store (a merge, a promotion plan, a verdict record, a triage outcome, a
//! rule going stale, retention dropping a version, an edge version
//! switching, the watermark advancing). It returns a typed outcome (a
//! `Change`, a domain outcome or the id it created), never an event for its
//! caller to publish. A refused or unchanged write publishes nothing. An
//! event that announces a decision a consumer computed before the write (a
//! lookup's `New` behind `ChannelDiscovered`, a correlator update behind
//! `TransmissionConfirmed`, new evidence behind `AgentSeen`, a
//! classification, a fit's `TopicVersionReady`) is published by that
//! consumer once the write has committed.
//!
//! **Time is an argument.** Every store method whose effect or answer
//! depends on the time takes it as a [`Timestamp`](crate::support::Timestamp)
//! argument (`at` for the time a write records, `now` for the instant a
//! retention window or lease is measured from). No store reads a clock. The
//! component calling the store takes the time from the event it is
//! handling, or reads the [`Clock`](crate::support::Clock) it was handed at
//! wiring, so a replay or a simulation gives the same results.

pub mod l0_ingress;
pub mod l1_canonical;
pub mod l2_transport;
pub mod l3_reconstruction;
pub mod l4_provenance;
pub mod l5_flow;
pub mod l6_analysis;
pub mod l7_topology;
pub mod l8_surface;
