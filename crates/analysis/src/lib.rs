//! L6 analysis for crosstalk: embeddings, topics, search, alert rules and
//! triage, and projections.
//!
//! Implements [`crosstalk_spec::interfaces::l6_analysis`].
//!
//! Roadmap: P6.2 (L6 search and alerts) and P6.3 (L6 topics and projections). A
//! layer crate: it depends on the spec, never on another layer crate.
//!
//! - [`remote`]: the adapters over HTTP services: the topic model and
//!   layout fitter over the Python topics sidecar, and the
//!   OpenAI-compatible embedder.
//! - [`search`]: `SearchIndex`, `SearchCorpus` and `ProjectionSource` on
//!   Postgres (full-text and pgvector).
//! - [`alerts`]: the alert store on Postgres (`AlertRuleStore`,
//!   `AlertTriage`, `AlertRuleMaintenance`, `AlertActions`, `AlertReads`),
//!   rule evaluation (`AlertRuleEval`) and the `alerts` bus consumer.
//! - [`pg`]: what the Postgres stores share (migrations, codec, cursors,
//!   the outbox).

pub mod alerts;
pub mod pg;
pub mod remote;
pub mod search;
