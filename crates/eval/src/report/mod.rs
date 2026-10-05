//! Reports: the score as JSON and as a table, with gate outcomes.

pub mod gates;
pub mod table;

use serde::{Deserialize, Serialize};

pub use gates::{Check, Gate, GateOutcome, GateStatus, Gates};

use crate::keys::DatasetId;
use crate::pipeline::Unscored;
use crate::score::{
    Counts, FalsePositive, Miss, RowKey, Score, Totals, TransmissionRow, ViolationRow,
};

/// One row with its derived rates.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReportRow {
    #[serde(flatten)]
    pub key: RowKey,
    #[serde(flatten)]
    pub counts: Counts,
    pub precision: Option<f64>,
    pub recall: Option<f64>,
}

/// Counts summed over every content row, with their rates.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Summary {
    #[serde(flatten)]
    pub counts: Counts,
    pub precision: Option<f64>,
    pub recall: Option<f64>,
}

/// Everything a run produced, ready to write.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Report {
    pub dataset: DatasetId,
    pub detector: String,
    pub totals: Totals,
    pub overall: Summary,
    pub rows: Vec<ReportRow>,
    pub transmissions: Vec<TransmissionRow>,
    pub violations: Vec<ViolationRow>,
    pub gates: Vec<GateOutcome>,
    /// Worlds that could not be scored, with why.
    pub failures: Vec<String>,
    /// Worlds the detector took in but could not detect in yet (the
    /// gateway pipeline before its detection consumers exist).
    pub unscored: Unscored,
    pub misses: Vec<Miss>,
    pub false_positives: Vec<FalsePositive>,
}

impl Report {
    pub fn new(
        dataset: DatasetId,
        detector: &str,
        score: Score,
        gates: Vec<GateOutcome>,
        failures: Vec<String>,
        unscored: Unscored,
    ) -> Self {
        let overall = score.total(&crate::score::Selector::default());
        let rows = score
            .rows
            .into_iter()
            .map(|row| ReportRow {
                precision: row.counts.precision(),
                recall: row.counts.recall(),
                key: row.key,
                counts: row.counts,
            })
            .collect();
        Self {
            overall: Summary {
                precision: overall.precision(),
                recall: overall.recall(),
                counts: overall,
            },
            dataset,
            detector: detector.to_owned(),
            totals: score.totals,
            rows,
            transmissions: score.transmissions,
            violations: score.violations,
            gates,
            failures,
            unscored,
            misses: score.misses,
            false_positives: score.false_positives,
        }
    }

    /// Whether any gate failed.
    pub fn gates_failed(&self) -> bool {
        self.gates.iter().any(GateOutcome::failed)
    }
}
