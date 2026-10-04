//! Alert delivery: the sinks an alert rule lists, and how each one's last
//! delivery went (`QueryApi::sinks`).

use crate::aggregates::alert::Alert;
use crate::ids::SinkId;
use crate::support::Timestamp;

pub trait AlertSink {
    fn id(&self) -> SinkId;

    async fn deliver(&self, alert: &Alert) -> Result<(), SinkError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SinkKind {
    Webhook,
    Slack,
    Log,
}

/// A configured sink, as `QueryApi::sinks` reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkInfo {
    pub id: SinkId,
    pub kind: SinkKind,
    /// The name from config.
    pub name: String,
    /// When its last delivery succeeded, or why it failed. `None` before
    /// its first delivery.
    pub last_delivery: Option<Result<Timestamp, SinkError>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkError {
    Unreachable { reason: String },
    Rejected { status: u16 },
}
