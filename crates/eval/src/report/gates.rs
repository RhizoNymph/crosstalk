//! Regression gates: per-dataset, per-row minimums a run must meet.
//!
//! A gate selects rows (any of dataset, route, carrier, class, tier; unset
//! means any), sums them, and checks one metric:
//!
//! ```toml
//! [[gate]]
//! name = "salt: construction labels found"
//! dataset = "salt"
//! tier = "construction"
//! metric = "recall"
//! min = 0.9
//! ```
//!
//! `recall` and `precision` take a `min`; `violations` (negative controls a
//! prediction fell under, optionally of one `reason`) takes a `max`. A gate
//! whose rows hold no data is skipped, not failed, so a small `--limit` run
//! is not failed by rows it never reached. Thresholds are regression gates
//! for the eval, not invariants of the gateway.

use serde::{Deserialize, Serialize};

use crate::keys::DatasetId;
use crate::score::{Score, Selector};
use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::aggregates::quality::MatchClass;

use crate::truth::{CarrierKind, NegativeReason, Tier};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "metric", rename_all = "snake_case")]
pub enum Check {
    Recall {
        min: f64,
    },
    Precision {
        min: f64,
    },
    Violations {
        max: u64,
        #[serde(default)]
        reason: Option<NegativeReason>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Gate {
    pub name: String,
    #[serde(default)]
    pub dataset: Option<DatasetId>,
    #[serde(default)]
    pub route: Option<RouteKind>,
    #[serde(default)]
    pub carrier: Option<CarrierKind>,
    #[serde(default)]
    pub class: Option<MatchClass>,
    #[serde(default)]
    pub tier: Option<Tier>,
    #[serde(flatten)]
    pub check: Check,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Gates {
    #[serde(default, rename = "gate")]
    pub gates: Vec<Gate>,
}

#[derive(Debug, thiserror::Error)]
pub enum GateError {
    #[error("reading gates {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("gates {path} are not valid: {source}")]
    Parse {
        path: String,
        #[source]
        source: toml::de::Error,
    },
}

impl Gates {
    pub fn parse(text: &str, path: &str) -> Result<Self, GateError> {
        toml::from_str(text).map_err(|source| GateError::Parse {
            path: path.to_owned(),
            source,
        })
    }

    pub fn load(path: &std::path::Path) -> Result<Self, GateError> {
        let shown = path.display().to_string();
        let text = std::fs::read_to_string(path).map_err(|source| GateError::Read {
            path: shown.clone(),
            source,
        })?;
        Self::parse(&text, &shown)
    }

    pub fn evaluate(&self, score: &Score) -> Vec<GateOutcome> {
        self.gates.iter().map(|gate| gate.evaluate(score)).collect()
    }
}

/// How a gate came out.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum GateStatus {
    Pass {
        value: f64,
    },
    Fail {
        value: f64,
        bound: f64,
    },
    /// No row it selects holds data for its metric.
    Skipped,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GateOutcome {
    pub name: String,
    #[serde(flatten)]
    pub status: GateStatus,
}

impl GateOutcome {
    pub fn failed(&self) -> bool {
        matches!(self.status, GateStatus::Fail { .. })
    }
}

impl Gate {
    fn selector(&self) -> Selector {
        Selector {
            dataset: self.dataset.clone(),
            route: self.route,
            carrier: self.carrier,
            class: self.class,
            tier: self.tier,
        }
    }

    pub fn evaluate(&self, score: &Score) -> GateOutcome {
        let counts = score.total(&self.selector());
        let status = match &self.check {
            Check::Recall { min } => at_least(counts.recall(), *min),
            Check::Precision { min } => at_least(counts.precision(), *min),
            Check::Violations { max, reason } => {
                let value = score.violation_count(self.dataset.as_ref(), *reason);
                // Counts are far below 2^52: exact as f64.
                let (value, bound) = (value as f64, *max as f64);
                if value <= bound {
                    GateStatus::Pass { value }
                } else {
                    GateStatus::Fail { value, bound }
                }
            }
        };
        GateOutcome {
            name: self.name.clone(),
            status,
        }
    }
}

fn at_least(value: Option<f64>, min: f64) -> GateStatus {
    match value {
        None => GateStatus::Skipped,
        Some(value) if value >= min => GateStatus::Pass { value },
        Some(value) => GateStatus::Fail { value, bound: min },
    }
}
