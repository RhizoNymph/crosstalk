//! Judging one prediction against one world's truth, through the alignment
//! rule. Shared by the scorer and the `DetectionQuality` bridge, so both
//! decide every prediction the same way.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::align::{aligns, specificity, violates};
use crate::corpus::{Coverage, World};
use crosstalk_spec::ids::ExchangeId;

use crate::keys::AgentKey;
use crate::predict::Prediction;
use crate::truth::{Expectation, ExpectedTransmission, NegativeControl, NegativeReason, Tier};

/// What a prediction turned out to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum Outcome {
    /// It aligns with the label at this index of the world's positives.
    Correct { expectation: usize, tier: Tier },
    /// It aligns with no label and the world's truth says it is wrong:
    /// either it falls under a negative control or the world's coverage is
    /// complete.
    False {
        violated: Option<NegativeReason>,
        tier: Tier,
    },
    /// It aligns with no label and the world's truth is a sample.
    Unjudged,
}

/// One world's labels, indexed for judging.
pub struct Judge<'w> {
    coverage: Coverage,
    positives: Vec<&'w ExpectedTransmission>,
    by_reader: BTreeMap<(&'w AgentKey, ExchangeId), Vec<usize>>,
    negatives: Vec<&'w NegativeControl>,
}

impl<'w> Judge<'w> {
    pub fn new(world: &'w World) -> Self {
        let mut positives = Vec::new();
        let mut negatives = Vec::new();
        for expectation in world.truth() {
            match expectation {
                Expectation::Transmission(expected) => positives.push(expected),
                Expectation::NoTransmission(control) => negatives.push(control),
                Expectation::AgentCluster(_) => {}
            }
        }
        negatives.sort_by_key(|control| specificity(control));
        let mut by_reader: BTreeMap<(&AgentKey, ExchangeId), Vec<usize>> = BTreeMap::new();
        for (at, expected) in positives.iter().enumerate() {
            let label = expected.label();
            by_reader
                .entry((&label.to, label.reader_exchange))
                .or_default()
                .push(at);
        }
        Self {
            coverage: world.coverage(),
            positives,
            by_reader,
            negatives,
        }
    }

    pub fn positives(&self) -> &[&'w ExpectedTransmission] {
        &self.positives
    }

    pub fn negatives(&self) -> &[&'w NegativeControl] {
        &self.negatives
    }

    /// The outcome of `prediction`: the first label it aligns with, else the
    /// most specific negative control it violates, else what the world's
    /// coverage makes of an unlabelled prediction.
    pub fn judge(&self, prediction: &Prediction) -> (Outcome, Option<&'w NegativeControl>) {
        let candidates = self
            .by_reader
            .get(&(&prediction.to, prediction.reader_exchange))
            .map(Vec::as_slice)
            .unwrap_or_default();
        if let Some(&at) = candidates
            .iter()
            .find(|&&at| aligns(prediction, self.positives[at]))
        {
            let tier = self.positives[at].label().tier;
            return (
                Outcome::Correct {
                    expectation: at,
                    tier,
                },
                None,
            );
        }
        if let Some(control) = self
            .negatives
            .iter()
            .find(|control| violates(prediction, control))
        {
            let label = control.label();
            return (
                Outcome::False {
                    violated: Some(label.reason),
                    tier: label.tier,
                },
                Some(control),
            );
        }
        match self.coverage {
            Coverage::Complete { tier } => (
                Outcome::False {
                    violated: None,
                    tier,
                },
                None,
            ),
            Coverage::Partial => (Outcome::Unjudged, None),
        }
    }
}
