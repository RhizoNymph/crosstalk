//! A saved node0 bench run, read from the gateway's own files: the demo
//! swarm's truth header (its run window and agents' names), the gateway's
//! exchange log and blob store, the transmissions export and each
//! transmission's evidence, and the conversation reads, as `ct-bench-detect
//! from-export` and `replay` consume them ([`crate::from_export`]).
//!
//! ```text
//! truth.jsonl (swarm --ground-truth) ─▶ truth_file::read   header (run window, scenario), agents' names
//! exchange-log.jsonl + blobs/ ─▶ exchange_log, bodies      the captured exchanges (window::split: only
//!                                                          those inside the run window)
//! export.jsonl + evidence.jsonl ─▶ detected                the gateway's transmissions and their evidence
//! exchange-turns.json + span-points.json ─▶ queried        L3's placement of each exchange, L4's span points
//! fetch: swarm-fetch (export, evidence) and fetch (the /query reads) over L8 HTTP
//! replay: the log through the live composition in memory, its export, evidence and reads
//! ```
//!
//! The exchange log accumulates across runs and a reused seed reuses
//! session ids, so the log is first cut to the run window ([`window`]):
//! exchanges outside it are left out of the capture, and a transmission
//! read only outside it is not written.
//!
//! Labels are not made here: the bench's demo-swarm converter labels the
//! capture from the truth file.

pub mod bodies;
pub mod detected;
pub mod exchange_log;
pub mod fetch;
pub mod queried;
pub mod replay;
pub mod schema;
pub mod truth_file;
pub mod window;

/// The prefix of every swarm-benchmark dataset id: a run is written under
/// `demo-swarm/<scenario>` ([`schema::Scenario::dataset`]).
pub const DATASET_PREFIX: &str = "demo-swarm";

/// The model name the world's agents are declared with (the swarm's
/// agents talk to an Anthropic-shaped upstream).
pub const MODEL: &str = "anthropic/claude";

#[derive(Debug, thiserror::Error)]
pub enum SwarmError {
    #[error("truth file {path}: {source}")]
    Truth {
        path: String,
        #[source]
        source: truth_file::TruthFileError,
    },
    #[error(transparent)]
    ExchangeLog(#[from] exchange_log::ExchangeLogError),
    #[error(transparent)]
    Bodies(#[from] bodies::OpenBodiesError),
    #[error(transparent)]
    Replay(#[from] replay::ReplayError),
    #[error("export {path}: {source}")]
    Detected {
        path: String,
        #[source]
        source: detected::DetectedError,
    },
}
