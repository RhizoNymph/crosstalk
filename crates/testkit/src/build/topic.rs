//! Topic-model versions and their history.

use std::time::Duration;

use crosstalk_spec::aggregates::retention::{Pin, Retention};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::topic_history::{
    CompletedFit, FitRecord, TopicVersionHistory, TopicVersionInfo, TopicVersionStatus,
};
use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::support::Timestamp;

use crate::build::error::BuildError;
use crate::time::{T0, after};

/// How far apart consecutive versions' fits start.
pub const VERSION_STEP: Duration = Duration::from_secs(3600);

/// Within one version: started, then fitted, ready and activated this far
/// apart.
pub const PHASE: Duration = Duration::from_secs(60);

/// What happened to a fitted version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Planned {
    /// Ready, then activated.
    Activated,
    /// Ready, not activated.
    Ready,
}

/// Builds a [`TopicVersionHistory`] (or its versions) through the spec's
/// constructors.
///
/// Version 0 is the unfitted model, active from [`T0`]. Each later version,
/// in order, is fitted [`VERSION_STEP`] after the previous one started: it
/// starts, is fitted, ready and (if activated) active [`PHASE`] apart. A
/// version is superseded by the first later version activated, at that
/// activation. The newest version may still be fitting
/// ([`TopicHistoryBuilder::fitting`]). Statuses are derived, so the history
/// is always well formed; pins and drops are checked by the constructors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicHistoryBuilder {
    planned: Vec<Planned>,
    fitting: bool,
    pins: Vec<(TopicModelVersion, OperatorId)>,
    dropped: Vec<TopicModelVersion>,
}

impl Default for TopicHistoryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TopicHistoryBuilder {
    /// Only version 0, active.
    pub fn new() -> Self {
        Self {
            planned: Vec::new(),
            fitting: false,
            pins: Vec::new(),
            dropped: Vec::new(),
        }
    }

    /// One more version, fitted and activated.
    pub fn activated(mut self) -> Self {
        self.planned.push(Planned::Activated);
        self
    }

    /// One more version, fitted and ready but not activated.
    pub fn ready(mut self) -> Self {
        self.planned.push(Planned::Ready);
        self
    }

    /// A newest version, still fitting.
    pub fn fitting(mut self) -> Self {
        self.fitting = true;
        self
    }

    /// Pin `version`, by `by`, when it became ready (version 0 at [`T0`]).
    pub fn pin(mut self, version: TopicModelVersion, by: OperatorId) -> Self {
        self.pins.push((version, by));
        self
    }

    /// Mark `version` dropped [`PHASE`] after it was superseded.
    pub fn drop_version(mut self, version: TopicModelVersion) -> Self {
        self.dropped.push(version);
        self
    }

    /// The fit of version `n` (n ≥ 1).
    fn fit(n: u32) -> CompletedFit {
        let started_at = after(T0, VERSION_STEP * n);
        CompletedFit {
            started_at,
            fitted_at: after(started_at, PHASE),
            ready_at: after(started_at, PHASE * 2),
            topics: 3 + n,
        }
    }

    /// When version `n` was activated, if it was.
    fn activated_at(&self, n: u32) -> Option<Timestamp> {
        if n == 0 {
            return Some(T0);
        }
        let index = usize::try_from(n - 1).ok()?;
        match self.planned.get(index)? {
            Planned::Activated => Some(after(Self::fit(n).started_at, PHASE * 3)),
            Planned::Ready => None,
        }
    }

    /// Every version, oldest first.
    pub fn build_versions(&self) -> Result<Vec<TopicVersionInfo>, BuildError> {
        let fitted = u32::try_from(self.planned.len()).unwrap_or(u32::MAX - 1);
        let active = (0..=fitted)
            .rev()
            .find(|n| self.activated_at(*n).is_some())
            .unwrap_or(0);
        let mut versions = Vec::new();
        for n in 0..=fitted {
            let fit = if n == 0 {
                FitRecord::Unfitted
            } else {
                FitRecord::Fitted(Self::fit(n))
            };
            let activated_at = self.activated_at(n);
            let supersessor = (n + 1..=fitted).find_map(|later| {
                self.activated_at(later)
                    .filter(|_| later <= active)
                    .map(|at| (later, at))
            });
            let status = match (supersessor, activated_at, fit) {
                (Some((by, superseded_at)), activated_at, fit) if n < active => {
                    TopicVersionStatus::Superseded {
                        fit,
                        activated_at,
                        by: TopicModelVersion(by),
                        superseded_at,
                    }
                }
                (_, Some(activated_at), fit) if n == active => {
                    TopicVersionStatus::Active { fit, activated_at }
                }
                (_, _, FitRecord::Fitted(fit)) => TopicVersionStatus::Ready { fit },
                (_, _, FitRecord::Unfitted) => TopicVersionStatus::Active {
                    fit: FitRecord::Unfitted,
                    activated_at: T0,
                },
            };
            versions.push(self.with_retention(TopicModelVersion(n), status)?);
        }
        if self.fitting {
            let n = fitted.saturating_add(1);
            let status = TopicVersionStatus::Fitting {
                started_at: after(T0, VERSION_STEP * n),
            };
            versions.push(self.with_retention(TopicModelVersion(n), status)?);
        }
        Ok(versions)
    }

    fn with_retention(
        &self,
        version: TopicModelVersion,
        status: TopicVersionStatus,
    ) -> Result<TopicVersionInfo, BuildError> {
        let pin = self
            .pins
            .iter()
            .find(|(pinned, _)| *pinned == version)
            .map(|(_, by)| Pin {
                by: *by,
                at: match status {
                    TopicVersionStatus::Ready { fit }
                    | TopicVersionStatus::Active {
                        fit: FitRecord::Fitted(fit),
                        ..
                    }
                    | TopicVersionStatus::Superseded {
                        fit: FitRecord::Fitted(fit),
                        ..
                    } => fit.ready_at,
                    TopicVersionStatus::Active { .. }
                    | TopicVersionStatus::Superseded { .. }
                    | TopicVersionStatus::Fitting { .. } => T0,
                },
            });
        let retention = if self.dropped.contains(&version) {
            let at = match status {
                TopicVersionStatus::Superseded { superseded_at, .. } => after(superseded_at, PHASE),
                TopicVersionStatus::Fitting { .. }
                | TopicVersionStatus::Ready { .. }
                | TopicVersionStatus::Active { .. } => T0,
            };
            Retention::Dropped { at }
        } else {
            Retention::Retained { pin }
        };
        Ok(TopicVersionInfo::with_retention(
            version, status, retention,
        )?)
    }

    pub fn build(&self) -> Result<TopicVersionHistory, BuildError> {
        Ok(TopicVersionHistory::new(self.build_versions()?)?)
    }
}
