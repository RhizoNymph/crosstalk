//! Judging one prediction against one world's truth, through the alignment
//! rule. Shared by the scorer and the `DetectionQuality` bridge, so both
//! decide every prediction the same way.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::align::{aligns, exempts, specificity, violates};
use crate::corpus::{Coverage, World};
use crosstalk_spec::ids::ExchangeId;

use crate::keys::AgentKey;
use crate::predict::Prediction;
use crate::truth::{
    Exemption, Expectation, ExpectedTransmission, NegativeControl, NegativeReason, Tier,
};

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
    /// It aligns with no label and the world's truth is a sample, or it
    /// falls under an exemption.
    Unjudged,
}

/// What evidence a positive label expects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Expects {
    /// A content match ([`Expectation::Transmission`]).
    Content,
    /// Access evidence only ([`Expectation::AccessOnly`], INV-963).
    Access,
}

/// One positive label and the evidence it expects.
#[derive(Debug, Clone, Copy)]
pub struct Positive<'w> {
    pub expected: &'w ExpectedTransmission,
    pub expects: Expects,
}

/// One world's labels, indexed for judging.
pub struct Judge<'w> {
    coverage: Coverage,
    positives: Vec<Positive<'w>>,
    by_reader: BTreeMap<(&'w AgentKey, ExchangeId), Vec<usize>>,
    negatives: Vec<&'w NegativeControl>,
    exemptions: Vec<&'w Exemption>,
}

impl<'w> Judge<'w> {
    pub fn new(world: &'w World) -> Self {
        let mut positives = Vec::new();
        let mut negatives = Vec::new();
        let mut exemptions = Vec::new();
        for expectation in world.truth() {
            match expectation {
                Expectation::Transmission(expected) => positives.push(Positive {
                    expected,
                    expects: Expects::Content,
                }),
                Expectation::AccessOnly(expected) => positives.push(Positive {
                    expected: expected.transmission(),
                    expects: Expects::Access,
                }),
                Expectation::NoTransmission(control) => negatives.push(control),
                Expectation::Unjudged(exemption) => exemptions.push(exemption),
                Expectation::AgentCluster(_) => {}
            }
        }
        negatives.sort_by_key(|control| specificity(control));
        let mut by_reader: BTreeMap<(&AgentKey, ExchangeId), Vec<usize>> = BTreeMap::new();
        for (at, positive) in positives.iter().enumerate() {
            let label = positive.expected.label();
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
            exemptions,
        }
    }

    /// Every positive label, content and access-only alike.
    pub fn positives(&self) -> &[Positive<'w>] {
        &self.positives
    }

    pub fn negatives(&self) -> &[&'w NegativeControl] {
        &self.negatives
    }

    /// Every label `prediction` aligns with, by index into
    /// [`Judge::positives`]. One prediction can find several labels: a
    /// match whose read range covers two adjacent labelled texts of one
    /// sender aligns with both.
    pub fn aligned<'p>(&'p self, prediction: &'p Prediction) -> impl Iterator<Item = usize> + 'p {
        self.by_reader
            .get(&(&prediction.to, prediction.reader_exchange))
            .map(Vec::as_slice)
            .unwrap_or_default()
            .iter()
            .copied()
            .filter(move |&at| aligns(prediction, self.positives[at].expected))
    }

    /// The outcome of `prediction`: the first label it aligns with, else
    /// unjudged when an exemption covers it, else the most specific
    /// negative control it violates, else what the world's coverage makes
    /// of an unlabelled prediction.
    pub fn judge(&self, prediction: &Prediction) -> (Outcome, Option<&'w NegativeControl>) {
        let candidates = self
            .by_reader
            .get(&(&prediction.to, prediction.reader_exchange))
            .map(Vec::as_slice)
            .unwrap_or_default();
        if let Some(&at) = candidates
            .iter()
            .find(|&&at| aligns(prediction, self.positives[at].expected))
        {
            // Any evidence aligned with an access-only label is right about
            // the pair and the place, so it is correct; only access evidence
            // finds that label (the scorer).
            let tier = self.positives[at].expected.label().tier;
            return (
                Outcome::Correct {
                    expectation: at,
                    tier,
                },
                None,
            );
        }
        if self
            .exemptions
            .iter()
            .any(|exemption| exempts(prediction, exemption))
        {
            return (Outcome::Unjudged, None);
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
