//! Regression gates: per-dataset, per-row minimums a run must meet.
//!
//! A gate selects rows (any of dataset, route, carrier, class, tier; unset
//! means any, except that an unset class means any content class), sums
//! them, and checks one metric:
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
//! A gate checks one detector's runs (`detector = "live"`; unset means the
//! reference matcher). `recall` and `precision` take a `min`; `violations` (negative controls a
//! prediction fell under, optionally of one `reason`) takes a `max`. A gate
//! whose rows hold no data is skipped, not failed, so a small `--limit` run
//! is not failed by rows it never reached. Thresholds are regression gates
//! for the eval, not invariants of the gateway.

use serde::{Deserialize, Serialize};

use crate::keys::DatasetId;
use crate::predict::EvidenceClass;
use crate::score::{Score, Selector};
use crosstalk_spec::aggregates::edge::RouteKind;

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

/// The detector a gate is tuned on. A gate checks only runs of its own
/// detector: the reference matcher and the gateway's live composition
/// have different baselines.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateDetector {
    /// The reference matcher (a gate that names no detector).
    #[default]
    Reference,
    /// The gateway pipeline alone (`--detector pipeline`; unscored).
    Pipeline,
    /// The gateway's live composition (`--detector live`).
    Live,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Gate {
    pub name: String,
    /// Which detector's runs it checks (default: the reference matcher).
    #[serde(default)]
    pub detector: GateDetector,
    #[serde(default)]
    pub dataset: Option<DatasetId>,
    #[serde(default)]
    pub route: Option<RouteKind>,
    #[serde(default)]
    pub carrier: Option<CarrierKind>,
    #[serde(default)]
    pub class: Option<EvidenceClass>,
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
    #[error("gates file {path} (from --gates) does not exist")]
    Missing { path: String },
}

/// The environment variable naming a gates file, after `--gates`.
pub const GATES_ENV: &str = "CT_EVAL_GATES";

/// Where the bench image installs the gates file.
pub const INSTALLED_GATES: &str = "/usr/local/share/crosstalk-eval/gates.toml";

/// Which place a run's gates file came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GatesFrom {
    /// `--gates`.
    Flag,
    /// [`GATES_ENV`].
    Env,
    /// [`INSTALLED_GATES`].
    Installed,
    /// The crate's own `gates.toml`, present in a source checkout.
    Crate,
}

/// A gates file found, and where from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatesLocation {
    pub from: GatesFrom,
    pub path: std::path::PathBuf,
}

/// The places a gates file is looked for, in order: `flag`, `env`,
/// `installed`, `crate_file`. An explicit `flag` must exist; every other
/// place is a default, skipped when missing, and with none found the run
/// has no gates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateSearch {
    pub flag: Option<std::path::PathBuf>,
    pub env: Option<std::path::PathBuf>,
    pub installed: std::path::PathBuf,
    pub crate_file: std::path::PathBuf,
}

impl GateSearch {
    /// The search for `flag`, the value of [`GATES_ENV`] (`env`; empty is
    /// unset), [`INSTALLED_GATES`] and `crate_file`.
    pub fn new(
        flag: Option<std::path::PathBuf>,
        env: Option<std::ffi::OsString>,
        crate_file: std::path::PathBuf,
    ) -> Self {
        Self {
            flag,
            env: env
                .filter(|value| !value.is_empty())
                .map(std::path::PathBuf::from),
            installed: std::path::PathBuf::from(INSTALLED_GATES),
            crate_file,
        }
    }

    /// [`GateSearch::new`] with [`GATES_ENV`] read from the process
    /// environment.
    pub fn from_env(flag: Option<std::path::PathBuf>, crate_file: std::path::PathBuf) -> Self {
        Self::new(flag, std::env::var_os(GATES_ENV), crate_file)
    }

    /// The first gates file found, or `None`; an explicit `flag` that does
    /// not exist is [`GateError::Missing`].
    pub fn locate(&self) -> Result<Option<GatesLocation>, GateError> {
        if let Some(flag) = &self.flag {
            if !flag.exists() {
                return Err(GateError::Missing {
                    path: flag.display().to_string(),
                });
            }
            return Ok(Some(GatesLocation {
                from: GatesFrom::Flag,
                path: flag.clone(),
            }));
        }
        let defaults = [
            (GatesFrom::Env, self.env.as_ref()),
            (GatesFrom::Installed, Some(&self.installed)),
            (GatesFrom::Crate, Some(&self.crate_file)),
        ];
        for (from, path) in defaults {
            let Some(path) = path else { continue };
            if path.is_file() {
                return Ok(Some(GatesLocation {
                    from,
                    path: path.clone(),
                }));
            }
            tracing::debug!(from = ?from, path = %path.display(), "no gates file here");
        }
        Ok(None)
    }

    /// The gates of the first file found, and where it was; no gates when
    /// none is.
    pub fn load(&self) -> Result<(Gates, Option<GatesLocation>), GateError> {
        match self.locate()? {
            Some(location) => Ok((Gates::load(&location.path)?, Some(location))),
            None => Ok((Gates::default(), None)),
        }
    }
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

    /// The gates tuned on `detector`; the others do not apply to its runs.
    pub fn for_detector(&self, detector: GateDetector) -> Self {
        Self {
            gates: self
                .gates
                .iter()
                .filter(|gate| gate.detector == detector)
                .cloned()
                .collect(),
        }
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
