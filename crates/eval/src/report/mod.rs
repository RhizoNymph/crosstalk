//! Reports: the score as JSON and as a table, with gate outcomes.

pub mod gates;
pub mod table;

use serde::{Deserialize, Serialize};

pub use gates::{Check, Gate, GateDetector, GateOutcome, GateStatus, Gates};

use crate::keys::DatasetId;
use crate::pipeline::Unscored;
use crate::predict::EvidenceClass;
use crate::score::{
    AccessOnlyControlRow, Counts, FalsePositive, Miss, RowKey, Score, SourceCount, Totals,
    TransmissionRow, ViolationRow,
};
use crate::truth::Tier;

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

impl Summary {
    pub fn of(counts: Counts) -> Self {
        Self {
            precision: counts.precision(),
            recall: counts.recall(),
            counts,
        }
    }
}

/// Everything a run produced, ready to write.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Report {
    pub dataset: DatasetId,
    pub detector: String,
    pub totals: Totals,
    /// Every row but the out-of-reach and forwarding ones.
    pub overall: Summary,
    /// Rows of out-of-reach labels: expected, but missed by design.
    pub out_of_reach: Summary,
    /// Rows of forwarding labels (`Tier::Forwarding`): the sender relayed
    /// its own tool output. Kept apart from `overall`, found only with L4's
    /// forwarding on.
    pub forwarding: Summary,
    /// Labels only access-only evidence lines up with, kept apart from
    /// `overall`: no content prediction found them.
    pub access_only: AccessOnly,
    pub rows: Vec<ReportRow>,
    pub transmissions: Vec<TransmissionRow>,
    /// Negative-control violations by content-class predictions: what the
    /// violation gates check.
    pub violations: Vec<ViolationRow>,
    /// Access-only predictions under a negative control, by class:
    /// reported apart, never gated ([`Score::access_only_under_controls`]).
    #[serde(default)]
    pub access_only_under_controls: Vec<AccessOnlyControlRow>,
    pub gates: Vec<GateOutcome>,
    /// Worlds that could not be scored, with why.
    pub failures: Vec<String>,
    /// Worlds the detector took in but could not detect in yet (the
    /// gateway pipeline before its detection consumers exist).
    pub unscored: Unscored,
    pub misses: Vec<Miss>,
    pub false_positives: Vec<FalsePositive>,
    /// The false-positive rate and its sources, when the run had negative
    /// controls.
    pub background: Option<Background>,
}

/// What access evidence (suspected or discarded predictions) found.
///
/// - `labels` of `expected`: content labels (out-of-reach ones aside)
///   that only a suspected or discarded prediction aligned with. The
///   detector saw the co-access but never matched content; they are
///   missed in `overall`, never found, and this is the recall the access
///   pattern alone would have had on top of it.
/// - `found_access` of `expected_access`: access-only labels
///   (`Expectation::AccessOnly`, INV-963: content on a resource its sender
///   never wrote stays suspected), which only access evidence finds and
///   `overall` never counts.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct AccessOnly {
    /// Missed content labels a suspected or discarded prediction aligned
    /// with.
    pub labels: u64,
    /// Every in-reach content label (`overall`'s expected).
    pub expected: u64,
    /// `labels / expected`; `None` with no label.
    pub recall: Option<f64>,
    /// In-reach access-only labels.
    pub expected_access: u64,
    /// Access-only labels a suspected or discarded prediction found.
    pub found_access: u64,
    /// `found_access / expected_access`; `None` with no such label.
    pub access_recall: Option<f64>,
}

impl AccessOnly {
    /// From `overall` (content rows) and `access` (the in-reach rows of
    /// access-only labels).
    pub fn of(overall: &Counts, access: &Counts) -> Self {
        Self {
            labels: overall.suspected,
            expected: overall.expected,
            recall: (overall.expected > 0)
                .then(|| overall.suspected as f64 / overall.expected as f64),
            expected_access: access.expected,
            found_access: access.found,
            access_recall: access.recall(),
        }
    }
}

/// What a run's negative controls say: how often the detector reported
/// a transmission per exchange it read, and the shared texts it fell on.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Background {
    /// Every false positive, out-of-reach and forwarding rows included.
    pub false_positives: u64,
    pub exchanges: u64,
    pub per_1k_exchanges: f64,
    pub sources: Vec<SourceCount>,
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
        let content = crate::score::Selector::default();
        let mut overall = Counts::default();
        let mut out_of_reach = Counts::default();
        let mut forwarding = Counts::default();
        for row in score.rows.iter().filter(|row| content.matches(&row.key)) {
            match row.key.tier {
                Some(Tier::OutOfReach) => out_of_reach.add(&row.counts),
                Some(Tier::Forwarding) => forwarding.add(&row.counts),
                _ => overall.add(&row.counts),
            }
        }
        // Access-only labels sit in the `suspected` rows (their predictions'
        // counts there are not labels and are left out).
        let mut access = Counts::default();
        for row in score.rows.iter().filter(|row| {
            row.key.class == EvidenceClass::Suspected && row.key.tier != Some(Tier::OutOfReach)
        }) {
            access.expected += row.counts.expected;
            access.found += row.counts.found;
            access.missed += row.counts.missed;
        }
        let false_positives =
            overall.false_positive + out_of_reach.false_positive + forwarding.false_positive;
        let background =
            (score.totals.negative_controls > 0 && score.totals.exchanges > 0).then(|| {
                Background {
                    false_positives,
                    exchanges: score.totals.exchanges,
                    per_1k_exchanges: false_positives as f64 * 1000.0
                        / score.totals.exchanges as f64,
                    sources: score.sources,
                }
            });
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
            access_only: AccessOnly::of(&overall, &access),
            overall: Summary::of(overall),
            out_of_reach: Summary::of(out_of_reach),
            forwarding: Summary::of(forwarding),
            dataset,
            detector: detector.to_owned(),
            totals: score.totals,
            rows,
            transmissions: score.transmissions,
            violations: score.violations,
            access_only_under_controls: score.access_only_under_controls,
            gates,
            failures,
            unscored,
            misses: score.misses,
            false_positives: score.false_positives,
            background,
        }
    }

    /// Whether any gate failed.
    pub fn gates_failed(&self) -> bool {
        self.gates.iter().any(GateOutcome::failed)
    }
}
