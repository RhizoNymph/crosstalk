//! [`InMemorySinkRegistry`]: the configured alert sinks and how each one's
//! last delivery went (`QueryApi::sinks`); and [`FakeSink`], an
//! [`AlertSink`] that records what it is given.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use crosstalk_spec::aggregates::alert::Alert;
use crosstalk_spec::ids::SinkId;
use crosstalk_spec::interfaces::l8_surface::{AlertSink, SinkError, SinkInfo, SinkKind};
use crosstalk_spec::support::Timestamp;

use crate::analysis::support::lock;

/// A sink config does not define.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("sink {0:?} is not configured")]
pub struct UnknownSink(pub SinkId);

/// One configured sink.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkConfig {
    pub id: SinkId,
    pub kind: SinkKind,
    pub name: String,
}

/// The configured sinks. Cloning shares the registry.
#[derive(Debug, Clone, Default)]
pub struct InMemorySinkRegistry {
    state: Arc<Mutex<BTreeMap<SinkId, SinkInfo>>>,
}

impl InMemorySinkRegistry {
    /// The sinks config defines, none delivered to yet. A sink listed twice
    /// keeps its last definition.
    pub fn new(sinks: impl IntoIterator<Item = SinkConfig>) -> Self {
        let infos = sinks
            .into_iter()
            .map(|sink| {
                (
                    sink.id,
                    SinkInfo {
                        id: sink.id,
                        kind: sink.kind,
                        name: sink.name,
                        last_delivery: None,
                    },
                )
            })
            .collect();
        Self {
            state: Arc::new(Mutex::new(infos)),
        }
    }

    /// The configured sink ids, for the rule store's check.
    pub fn ids(&self) -> BTreeSet<SinkId> {
        lock(&self.state).keys().copied().collect()
    }

    pub fn contains(&self, sink: SinkId) -> bool {
        lock(&self.state).contains_key(&sink)
    }

    /// Record how a delivery to `sink` went: when it succeeded, or why it
    /// failed. The latest delivery replaces the one before.
    pub fn record_delivery(
        &self,
        sink: SinkId,
        outcome: Result<Timestamp, SinkError>,
    ) -> Result<(), UnknownSink> {
        let mut state = lock(&self.state);
        let info = state.get_mut(&sink).ok_or(UnknownSink(sink))?;
        info.last_delivery = Some(outcome);
        Ok(())
    }

    /// Every configured sink, by id.
    pub fn sinks(&self) -> Vec<SinkInfo> {
        lock(&self.state).values().cloned().collect()
    }
}

/// An alert sink that records every alert it accepts, and fails with the
/// error the test sets while one is set.
#[derive(Debug, Clone)]
pub struct FakeSink {
    id: SinkId,
    state: Arc<Mutex<FakeSinkState>>,
}

#[derive(Debug, Default)]
struct FakeSinkState {
    delivered: Vec<Alert>,
    failure: Option<SinkError>,
}

impl FakeSink {
    pub fn new(id: SinkId) -> Self {
        Self {
            id,
            state: Arc::new(Mutex::new(FakeSinkState::default())),
        }
    }

    /// Make every later delivery fail with `failure`, or succeed again with
    /// `None`.
    pub fn fail_with(&self, failure: Option<SinkError>) {
        lock(&self.state).failure = failure;
    }

    /// The alerts accepted so far, in order.
    pub fn delivered(&self) -> Vec<Alert> {
        lock(&self.state).delivered.clone()
    }
}

impl AlertSink for FakeSink {
    fn id(&self) -> SinkId {
        self.id
    }

    async fn deliver(&self, alert: &Alert) -> Result<(), SinkError> {
        let mut state = lock(&self.state);
        if let Some(failure) = &state.failure {
            return Err(failure.clone());
        }
        state.delivered.push(alert.clone());
        Ok(())
    }
}
