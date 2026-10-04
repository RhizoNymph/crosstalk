//! Why a step of the flow consumer failed. A transient failure (a store or
//! the bus unavailable) leaves the step at the head of the queue for a
//! retry; a permanent one is logged and the step dropped.

use crosstalk_spec::ids::TransmissionId;
use crosstalk_spec::interfaces::l2_transport::BusError;
use crosstalk_spec::interfaces::l5_flow::RegistryError;
use crosstalk_spec::interfaces::l5_flow::channels::TrafficError;
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStoreError;

use super::publish::PublishError;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StepError {
    #[error("channel traffic: {0:?}")]
    Traffic(TrafficError),
    #[error("channel registry: {0:?}")]
    Registry(RegistryError),
    #[error("transmission store: {0:?}")]
    Transmissions(TransmissionStoreError),
    #[error("publishing: {0}")]
    Publish(#[from] PublishError),
    /// An update for a transmission the store does not hold.
    #[error("no stored transmission {}", .0.ulid_text())]
    MissingTransmission(TransmissionId),
}

impl From<TrafficError> for StepError {
    fn from(error: TrafficError) -> Self {
        Self::Traffic(error)
    }
}

impl From<RegistryError> for StepError {
    fn from(error: RegistryError) -> Self {
        Self::Registry(error)
    }
}

impl From<TransmissionStoreError> for StepError {
    fn from(error: TransmissionStoreError) -> Self {
        Self::Transmissions(error)
    }
}

impl StepError {
    /// Whether retrying cannot help: anything but a store or bus being
    /// unavailable.
    pub fn is_permanent(&self) -> bool {
        match self {
            Self::Traffic(TrafficError::Store { .. })
            | Self::Registry(RegistryError::Store { .. })
            | Self::Transmissions(TransmissionStoreError::Store { .. })
            | Self::Publish(PublishError::Bus(BusError::Disconnected)) => false,
            Self::Traffic(_)
            | Self::Registry(_)
            | Self::Publish(_)
            | Self::MissingTransmission(_) => true,
        }
    }
}
