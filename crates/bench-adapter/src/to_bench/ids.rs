//! Names and spec ids as bench keys and ids. Exchange ids are carried as
//! they are (`ExchangeId::from_raw` on the spec's 128 bits): a bench
//! exchange's id is the spec exchange's, and a recorded one (the
//! gateway's) is the gateway's.

use a2a_bench_format as bench;
use crosstalk_spec::ids::{AgentId, ExchangeId, TransmissionId};
use crosstalk_spec::support::ByteRange as SpecRange;

use super::ToBenchError;

pub fn dataset(id: &str) -> Result<bench::ids::DatasetId, ToBenchError> {
    bench::ids::DatasetId::new(id).map_err(ToBenchError::Key)
}

pub fn world(key: &str) -> Result<bench::ids::WorldKey, ToBenchError> {
    bench::ids::WorldKey::new(key).map_err(ToBenchError::Key)
}

/// An agent by its name: bench agent keys are unique within their world.
pub fn agent(name: &str) -> Result<bench::ids::AgentKey, ToBenchError> {
    bench::ids::AgentKey::new(name).map_err(ToBenchError::Key)
}

/// A detector's agent: its spec id's ULID text.
pub fn detector_agent(id: AgentId) -> Result<bench::ids::DetectorAgent, ToBenchError> {
    bench::ids::DetectorAgent::new(id.ulid_text()).map_err(ToBenchError::Key)
}

/// A detector's transmission: its spec id's ULID text.
pub fn transmission(id: TransmissionId) -> Result<bench::ids::TransmissionRef, ToBenchError> {
    bench::ids::TransmissionRef::new(id.ulid_text()).map_err(ToBenchError::Key)
}

pub fn exchange(id: ExchangeId) -> bench::ids::ExchangeId {
    bench::ids::ExchangeId::from_raw(id.as_ulid())
}

pub fn range(range: SpecRange) -> Result<bench::location::ByteRange, ToBenchError> {
    bench::location::ByteRange::new(range.start(), range.end()).map_err(ToBenchError::Range)
}
