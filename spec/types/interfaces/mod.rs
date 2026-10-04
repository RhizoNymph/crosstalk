//! The interface each layer of the abstraction stack exposes.
//!
//! One module per layer. Each module holds the layer's traits, the types
//! that only cross that layer's boundary, and the layer's error enums.
//! Implementations derive `thiserror::Error` on the error enums; they are
//! plain enums here so the spec has no dependencies.
//!
//! Components that keep state (the correlator, the identity resolver's
//! cache) own it inside one task and receive work over channels. Traits take
//! `&mut self` for that state rather than hiding it behind locks.
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
//! | L7 | [`l7_topology`] | `TransmissionClassified`, `TopicVersionReady` | `EdgeUpdated`, `TopicVersionActivated` |
//! | L8 | [`l8_surface`] | `AlertOpened`, `EdgeUpdated`, live feed subjects, operator | `PolicyChanged`, `AlertChanged`, agent merges, SSE |

pub mod l0_ingress;
pub mod l1_canonical;
pub mod l2_transport;
pub mod l3_reconstruction;
pub mod l4_provenance;
pub mod l5_flow;
pub mod l6_analysis;
pub mod l7_topology;
pub mod l8_surface;
