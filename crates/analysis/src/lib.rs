//! L6 analysis for crosstalk: embeddings, topics, search, alert rules and
//! triage, and projections.
//!
//! Implements [`crosstalk_spec::interfaces::l6_analysis`].
//!
//! Roadmap: P6.2 (L6 search and alerts) and P6.3 (L6 topics and projections). A
//! layer crate: it depends on the spec, never on another layer crate.
//!
//! [`remote`] holds the adapters over HTTP services: the topic model and
//! layout fitter over the Python topics sidecar, and the OpenAI-compatible
//! embedder.

pub mod remote;
