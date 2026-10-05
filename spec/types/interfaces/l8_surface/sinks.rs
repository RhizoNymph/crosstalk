//! Alert delivery: the sinks an alert rule lists, and how each one's last
//! delivery went (`QueryApi::sinks`).

use serde::{Deserialize, Serialize};

use crate::aggregates::alert::Alert;
use crate::ids::SinkId;
use crate::support::Timestamp;

pub trait AlertSink {
    fn id(&self) -> SinkId;

    fn deliver(&self, alert: &Alert) -> impl Future<Output = Result<(), SinkError>> + Send;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SinkKind {
    Webhook,
    Slack,
    Log,
}

/// A configured sink, as `QueryApi::sinks` reports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct SinkInfo {
    pub id: SinkId,
    pub kind: SinkKind,
    /// The name from config.
    pub name: String,
    /// When its last delivery succeeded, or why it failed. `None` before
    /// its first delivery. On the wire, `null`,
    /// `{"type": "succeeded", "data": "<timestamp>"}` or
    /// `{"type": "failed", "data": <SinkError>}` (see `last_delivery`).
    #[serde(with = "last_delivery")]
    pub last_delivery: Option<Result<Timestamp, SinkError>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum SinkError {
    Unreachable { reason: String },
    Rejected { status: u16 },
}

/// `SinkInfo::last_delivery` on the wire: adjacently tagged like every
/// other enum, rather than serde's `{"Ok": ..}` form of a `Result`.
mod last_delivery {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::SinkError;
    use crate::support::Timestamp;

    #[derive(Serialize, Deserialize)]
    #[serde(
        tag = "type",
        content = "data",
        rename_all = "snake_case",
        deny_unknown_fields
    )]
    enum LastDelivery {
        Succeeded(Timestamp),
        Failed(SinkError),
    }

    pub fn serialize<S: Serializer>(
        value: &Option<Result<Timestamp, SinkError>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value
            .clone()
            .map(|result| match result {
                Ok(at) => LastDelivery::Succeeded(at),
                Err(error) => LastDelivery::Failed(error),
            })
            .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Result<Timestamp, SinkError>>, D::Error> {
        let delivery = Option::<LastDelivery>::deserialize(deserializer)?;
        Ok(delivery.map(|delivery| match delivery {
            LastDelivery::Succeeded(at) => Ok(at),
            LastDelivery::Failed(error) => Err(error),
        }))
    }
}

/// The configured sinks and how each one's last delivery went: the data of
/// `QueryApi::sinks`, and the set `AlertRuleStore` checks a rule's sinks
/// against. The sinks come from config; deliveries are recorded by the
/// component that delivers alerts.
pub trait SinkRegistry {
    /// Record how a delivery to `sink` went: when it succeeded, or why it
    /// failed. The latest delivery replaces the one before. `UnknownSink`
    /// for a sink config does not define, changing nothing.
    fn record_delivery(
        &mut self,
        sink: SinkId,
        outcome: Result<Timestamp, SinkError>,
    ) -> impl Future<Output = Result<(), SinkRegistryError>> + Send;

    /// Every configured sink, by id.
    fn sinks(&self) -> impl Future<Output = Result<Vec<SinkInfo>, SinkRegistryError>> + Send;
}

/// Why a sink registry call failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkRegistryError {
    Store { reason: String },
    UnknownSink(SinkId),
}
