//! Scoring predictions against truth.
//!
//! Every prediction is judged through the alignment rule ([`align`]); every
//! positive label is found (some prediction aligns with it) or missed. Counts
//! are kept per [`RowKey`]: dataset × route kind × carrier × match class ×
//! tier.
//!
//! - A label is counted in the row of its own route, carrier, the class it
//!   needs, and its tier: `expected`, then `found` or `missed`. Recall comes
//!   from these.
//! - A prediction is counted in the row of its own route, carrier and class,
//!   with the tier of the label it aligned with (or of the control it
//!   violated, or of the world's coverage): `correct`, `false_positive` or
//!   `unjudged`. Precision comes from these. Unjudged predictions (no label,
//!   partial coverage) have no tier.
//! - Only content evidence finds a label. A suspected or discarded
//!   prediction (a co-access with no content match) is counted in its own
//!   row (class `suspected` or `discarded`), and a label it aligns with but
//!   no content prediction does is `missed` and also `suspected`: the
//!   detector saw the access pattern but never confirmed it. Selectors and
//!   the overall summary read content rows unless they name an access
//!   class.
//!
//! Unlike the spec's `DetectionQuality`, which only sees transmissions the
//! detector opened, the scorer sees total misses: a label no prediction
//! aligns with is `missed`. [`quality`] builds `DetectionQuality` from the
//! same judgements, and the transmission rows here agree with it.

pub mod align;
pub mod judge;
pub mod quality;
pub mod sources;

use std::cmp::Ordering;
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub use judge::{Judge, Outcome};
pub use sources::SourceCount;

use crate::corpus::World;
use crate::keys::{DatasetId, SourceRef};
use crate::predict::{EvidenceClass, Prediction};
use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::aggregates::quality::QualityMatch;
use crosstalk_spec::ids::TransmissionId;

use crate::location::SpanLocationExt;
use crate::truth::kinds::cmp_route;
use crate::truth::{CarrierKind, ExpectedTransmission, NegativeReason, Tier};

/// The breakdown key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RowKey {
    pub dataset: DatasetId,
    pub route: RouteKind,
    pub carrier: CarrierKind,
    pub class: EvidenceClass,
    /// `None` for unjudged predictions.
    pub tier: Option<Tier>,
}

/// Counts in one row.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Counts {
    /// Positive labels.
    pub expected: u64,
    /// Labels some content prediction aligned with.
    pub found: u64,
    /// Labels no content prediction aligned with.
    pub missed: u64,
    /// Missed labels that a suspected or discarded prediction aligned with.
    pub suspected: u64,
    /// Predictions.
    pub predicted: u64,
    /// Predictions that aligned with a label.
    pub correct: u64,
    /// Predictions judged wrong.
    pub false_positive: u64,
    /// Predictions with no label under partial coverage.
    pub unjudged: u64,
}

impl Counts {
    /// `correct / (correct + false_positive)`; `None` with no judged
    /// prediction.
    pub fn precision(&self) -> Option<f64> {
        ratio(self.correct, self.correct + self.false_positive)
    }

    /// `found / expected`; `None` with no label.
    pub fn recall(&self) -> Option<f64> {
        ratio(self.found, self.expected)
    }

    pub fn add(&mut self, other: &Self) {
        self.expected += other.expected;
        self.found += other.found;
        self.missed += other.missed;
        self.suspected += other.suspected;
        self.predicted += other.predicted;
        self.correct += other.correct;
        self.false_positive += other.false_positive;
        self.unjudged += other.unjudged;
    }
}

fn ratio(numerator: u64, denominator: u64) -> Option<f64> {
    if denominator == 0 {
        None
    } else {
        // Counts stay far below 2^52, so the conversion is exact.
        Some(numerator as f64 / denominator as f64)
    }
}

/// One detector transmission's tally key: what `DetectionQuality` rows by.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransmissionKey {
    pub dataset: DatasetId,
    pub route: RouteKind,
    /// The detector's call: confirmed by its strongest match's class and
    /// carrier, suspected or discarded.
    pub quality: QualityMatch,
}

impl Ord for RowKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.dataset
            .cmp(&other.dataset)
            .then_with(|| cmp_route(self.route, other.route))
            .then_with(|| self.carrier.cmp(&other.carrier))
            .then_with(|| self.class.cmp(&other.class))
            .then_with(|| self.tier.cmp(&other.tier))
    }
}

impl PartialOrd for RowKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TransmissionKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.dataset
            .cmp(&other.dataset)
            .then_with(|| cmp_route(self.route, other.route))
            .then_with(|| self.quality.cmp(&other.quality))
    }
}

impl PartialOrd for TransmissionKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Transmissions by the verdict the truth implies: the scorer's view of
/// `DetectionQuality`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransmissionCounts {
    pub genuine: u64,
    pub false_detection: u64,
    pub unlabeled: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Row {
    pub key: RowKey,
    pub counts: Counts,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransmissionRow {
    pub key: TransmissionKey,
    pub counts: TransmissionCounts,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViolationRow {
    pub dataset: DatasetId,
    pub reason: NegativeReason,
    pub count: u64,
}

/// A missed label, for citing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Miss {
    pub expectation: ExpectedTransmission,
}

/// A prediction judged false, for citing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FalsePositive {
    pub prediction: Prediction,
    pub violated: Option<NegativeReason>,
    /// The violated control's source record.
    pub control: Option<SourceRef>,
    /// The reader's text at the prediction's location, cut to
    /// [`EXCERPT_CHARS`] characters.
    pub excerpt: Option<String>,
}

/// How much of a false positive's text a report cites.
pub const EXCERPT_CHARS: usize = 240;

fn excerpt(world: &World, prediction: &Prediction) -> Option<String> {
    let exchange = world.exchange(prediction.reader_exchange)?;
    let message = exchange.message(prediction.read_at.message())?;
    let text = prediction.read_at.text(message).ok()?;
    Some(text.chars().take(EXCERPT_CHARS).collect())
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Totals {
    pub worlds: u64,
    pub agents: u64,
    pub exchanges: u64,
    pub expectations: u64,
    pub negative_controls: u64,
    pub predictions: u64,
}

/// The finished score.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Score {
    pub totals: Totals,
    pub rows: Vec<Row>,
    pub transmissions: Vec<TransmissionRow>,
    pub violations: Vec<ViolationRow>,
    /// At most the scorer's example cap of each.
    pub misses: Vec<Miss>,
    pub false_positives: Vec<FalsePositive>,
    /// The shared texts negative-control violations fell on, largest
    /// first, tallied over every violation ([`sources`]).
    pub sources: Vec<SourceCount>,
}

/// What a row selector picks; `None` matches anything, except that an
/// unset `class` matches content classes only: access-only rows
/// (`suspected`, `discarded`) are selected by naming them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Selector {
    pub dataset: Option<DatasetId>,
    pub route: Option<RouteKind>,
    pub carrier: Option<CarrierKind>,
    pub class: Option<EvidenceClass>,
    pub tier: Option<Tier>,
}

impl Selector {
    pub fn matches(&self, key: &RowKey) -> bool {
        self.dataset.as_ref().is_none_or(|d| *d == key.dataset)
            && self.route.is_none_or(|r| r == key.route)
            && self.carrier.is_none_or(|c| c == key.carrier)
            && self
                .class
                .map_or(key.class.is_content(), |c| c == key.class)
            && self.tier.is_none_or(|t| Some(t) == key.tier)
    }
}

impl Score {
    /// The sum of every row `selector` matches.
    pub fn total(&self, selector: &Selector) -> Counts {
        let mut sum = Counts::default();
        for row in self.rows.iter().filter(|row| selector.matches(&row.key)) {
            sum.add(&row.counts);
        }
        sum
    }

    /// Negative-control violations matching `dataset` (any when `None`).
    pub fn violation_count(
        &self,
        dataset: Option<&DatasetId>,
        reason: Option<NegativeReason>,
    ) -> u64 {
        self.violations
            .iter()
            .filter(|row| dataset.is_none_or(|d| *d == row.dataset))
            .filter(|row| reason.is_none_or(|r| r == row.reason))
            .map(|row| row.count)
            .sum()
    }
}

/// Accumulates scores world by world.
pub struct Scorer {
    example_cap: usize,
    totals: Totals,
    rows: BTreeMap<RowKey, Counts>,
    transmissions: BTreeMap<TransmissionKey, TransmissionCounts>,
    violations: BTreeMap<(DatasetId, NegativeReason), u64>,
    misses: Vec<Miss>,
    false_positives: Vec<FalsePositive>,
    sources: sources::SourceTally,
}

impl Scorer {
    /// `example_cap` bounds how many misses and false positives are kept for
    /// citing; counts are always complete.
    pub fn new(example_cap: usize) -> Self {
        Self {
            example_cap,
            totals: Totals::default(),
            rows: BTreeMap::new(),
            transmissions: BTreeMap::new(),
            violations: BTreeMap::new(),
            misses: Vec::new(),
            false_positives: Vec::new(),
            sources: sources::SourceTally::default(),
        }
    }

    /// Scores one world's predictions.
    pub fn add_world(&mut self, world: &World, predictions: &[Prediction]) {
        let dataset = world.dataset().clone();
        let judge = Judge::new(world);
        self.totals.worlds += 1;
        self.totals.agents += world.agents().len() as u64;
        self.totals.exchanges += world.exchanges().len() as u64;
        self.totals.expectations += judge.positives().len() as u64;
        self.totals.negative_controls += judge.negatives().len() as u64;
        self.totals.predictions += predictions.len() as u64;

        let mut found = vec![false; judge.positives().len()];
        let mut suspected = vec![false; judge.positives().len()];
        let mut by_transmission: BTreeMap<TransmissionId, (RouteKind, QualityMatch, Verdicts)> =
            BTreeMap::new();
        for prediction in predictions {
            let (outcome, control) = judge.judge(prediction);
            let tier = match outcome {
                Outcome::Correct { expectation, tier } => {
                    if prediction.class.is_content() {
                        found[expectation] = true;
                    } else {
                        suspected[expectation] = true;
                    }
                    Some(tier)
                }
                Outcome::False { tier, .. } => Some(tier),
                Outcome::Unjudged => None,
            };
            let counts = self
                .rows
                .entry(RowKey {
                    dataset: dataset.clone(),
                    route: prediction.route.kind(),
                    carrier: prediction.carrier,
                    class: prediction.class,
                    tier,
                })
                .or_default();
            counts.predicted += 1;
            let entry = by_transmission.entry(prediction.transmission).or_insert((
                prediction.route.kind(),
                prediction.quality,
                Verdicts::default(),
            ));
            match outcome {
                Outcome::Correct { .. } => {
                    counts.correct += 1;
                    entry.2.correct = true;
                }
                Outcome::False { violated, .. } => {
                    counts.false_positive += 1;
                    entry.2.wrong = true;
                    if let Some(reason) = violated {
                        *self
                            .violations
                            .entry((dataset.clone(), reason))
                            .or_default() += 1;
                        self.sources
                            .add(reason, &excerpt(world, prediction).unwrap_or_default());
                    }
                    if self.false_positives.len() < self.example_cap {
                        self.false_positives.push(FalsePositive {
                            prediction: prediction.clone(),
                            violated,
                            control: control.map(|control| control.label().source.clone()),
                            excerpt: excerpt(world, prediction),
                        });
                    }
                }
                Outcome::Unjudged => counts.unjudged += 1,
            }
        }
        for (route, quality, verdicts) in by_transmission.into_values() {
            let counts = self
                .transmissions
                .entry(TransmissionKey {
                    dataset: dataset.clone(),
                    route,
                    quality,
                })
                .or_default();
            match (verdicts.correct, verdicts.wrong) {
                (true, _) => counts.genuine += 1,
                (false, true) => counts.false_detection += 1,
                (false, false) => counts.unlabeled += 1,
            }
        }
        for ((expected, found), suspected) in judge.positives().iter().zip(found).zip(suspected) {
            let label = expected.label();
            let counts = self
                .rows
                .entry(RowKey {
                    dataset: dataset.clone(),
                    route: label.route.kind(),
                    carrier: label.carrier,
                    class: EvidenceClass::from(label.needs.class()),
                    tier: Some(label.tier),
                })
                .or_default();
            counts.expected += 1;
            if found {
                counts.found += 1;
            } else {
                counts.missed += 1;
                if suspected {
                    counts.suspected += 1;
                }
                if self.misses.len() < self.example_cap {
                    self.misses.push(Miss {
                        expectation: (*expected).clone(),
                    });
                }
            }
        }
    }

    pub fn finish(self) -> Score {
        Score {
            totals: self.totals,
            rows: self
                .rows
                .into_iter()
                .map(|(key, counts)| Row { key, counts })
                .collect(),
            transmissions: self
                .transmissions
                .into_iter()
                .map(|(key, counts)| TransmissionRow { key, counts })
                .collect(),
            violations: self
                .violations
                .into_iter()
                .map(|((dataset, reason), count)| ViolationRow {
                    dataset,
                    reason,
                    count,
                })
                .collect(),
            misses: self.misses,
            false_positives: self.false_positives,
            sources: self.sources.top(),
        }
    }
}

/// What a transmission's matches were judged: a transmission is genuine when
/// any match is correct, a false detection when none is and some is false,
/// unlabeled otherwise. This is the verdict [`quality`] derives too.
#[derive(Debug, Clone, Copy, Default)]
struct Verdicts {
    correct: bool,
    wrong: bool,
}
