//! [`InMemorySinkRegistry`]: the reference [`SinkRegistry`], the configured
//! alert sinks and how each one's last delivery went (`QueryApi::sinks`);
//! and [`FakeSink`], an [`AlertSink`] that records what it is given.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use crosstalk_spec::aggregates::alert::Alert;
use crosstalk_spec::ids::SinkId;
use crosstalk_spec::interfaces::l8_surface::sinks::{SinkRegistry, SinkRegistryError};
use crosstalk_spec::interfaces::l8_surface::{AlertSink, SinkError, SinkInfo, SinkKind};
use crosstalk_spec::support::Timestamp;

use crate::support::lock;

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
}

impl SinkRegistry for InMemorySinkRegistry {
    async fn record_delivery(
        &mut self,
        sink: SinkId,
        outcome: Result<Timestamp, SinkError>,
    ) -> Result<(), SinkRegistryError> {
        let mut state = lock(&self.state);
        let info = state
            .get_mut(&sink)
            .ok_or(SinkRegistryError::UnknownSink(sink))?;
        info.last_delivery = Some(outcome);
        Ok(())
    }

    async fn sinks(&self) -> Result<Vec<SinkInfo>, SinkRegistryError> {
        Ok(lock(&self.state).values().cloned().collect())
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
