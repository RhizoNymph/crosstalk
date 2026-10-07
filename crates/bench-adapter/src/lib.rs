//! crosstalk's adapter to the a2a-transmission-bench: `ct-bench-detect`,
//! crosstalk's side of the bench's detector contract (separation design
//! §4, §4.1). Datasets, labels, scoring and gates live in the bench
//! (a2a-transmission-bench, format tag `a2a-bench-format-v1.0.0`); this
//! crate only detects.
//!
//! ```text
//! ct-bench-detect --input DIR --output FILE [--mode live|pipeline] [flags]
//!   input::InputDir (manifest.json, messages.jsonl, exchanges.jsonl; lockstep, world order checked)
//!   per world:
//!     convert::world    bench messages → spec Messages (part text checked equal, P1 live)
//!                       bench exchanges → checked NormalizedExchanges (Replay ingress, ids carried)
//!     live:     LiveDetector::detect_exchanges (fresh composition) → RawDetection
//!               directory::BenchDirectory + to_bench::predictions::rows → attribution,
//!               unattributed and transmission rows (MessageHash → MessageId through the index)
//!     pipeline: PipelineDetector::ingest → no_consumers { ingested }
//!     a world that cannot be processed → failed { "<code>: <detail>" } ([`WorldFailure`])
//!   PredictionsWriter (check_predictions per world) → header (config::live_info / pipeline_info,
//!   Manifest::digest of the manifest read) → read back (to_bench::verify::predictions_file)
//!
//! ct-bench-detect from-export --run DIR --out DIR     from_export: a saved node0 run
//! ct-bench-detect replay      --run DIR --out DIR     from_export over a replayed run
//! ct-bench-detect fetch       --api URL --truth FILE --out DIR   the /query responses
//! ct-bench-detect swarm-fetch --api URL --truth FILE --out DIR   the export and evidence
//! ```
//!
//! - [`convert`], [`input`], [`run`], [`directory`], [`config`]: the
//!   detector contract over a bench input directory.
//! - [`detect`] and [`gateway`]: the gateway's live composition
//!   (`LiveBackend`) and bare pipeline as detectors; [`reads`] reads a
//!   detection's spans, accesses and channels through the spec's read
//!   traits.
//! - [`to_bench`]: spec values as bench rows (messages, exchanges,
//!   prediction rows, the predictions writer).
//! - [`swarm`] and [`from_export`]: a saved node0 bench run (the gateway's
//!   exchange log, blobs, export, evidence and conversation reads) as a bench
//!   input directory and the gateway's predictions; fetching them over L8.

pub mod config;
pub mod convert;
pub mod detect;
pub mod directory;
pub mod from_export;
pub mod gateway;
pub mod input;
pub mod location;
pub mod reads;
pub mod run;
pub mod swarm;
pub mod to_bench;

use std::fmt;

/// The stable code a failed world's reason starts with, before a colon.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FailureCode {
    /// A bench part's text is not the converted spec part's.
    PartTextMismatch,
    /// A bench row has no spec form (a media kind the spec lacks, a
    /// response of two messages, an unreadable failure text, …), or the
    /// detection's rows do not convert back.
    Conversion,
    /// The composition refused an exchange, or could not be built.
    Ingest,
    /// Settling the composition failed.
    Settle,
    /// The world's inputs do not check, or a store read failed.
    Read,
    /// A transmission names an access or part the world does not hold.
    UnlocatedAccess,
}

impl FailureCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PartTextMismatch => "part_text_mismatch",
            Self::Conversion => "conversion",
            Self::Ingest => "ingest",
            Self::Settle => "settle",
            Self::Read => "read",
            Self::UnlocatedAccess => "unlocated_access",
        }
    }
}

impl fmt::Display for FailureCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Why one world was written `failed`: its code and a free-text detail.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{code}: {detail}")]
pub struct WorldFailure {
    pub code: FailureCode,
    pub detail: String,
}

impl WorldFailure {
    pub fn new(code: FailureCode, detail: impl fmt::Display) -> Self {
        Self {
            code,
            detail: detail.to_string(),
        }
    }

    /// The `failed { reason }` text: `<code>: <detail>`.
    pub fn reason(&self) -> String {
        self.to_string()
    }
}

/// The adapter's own version, as a manifest's converter names it.
pub fn converter_version() -> String {
    format!("ct-bench-detect {}", env!("CARGO_PKG_VERSION"))
}
