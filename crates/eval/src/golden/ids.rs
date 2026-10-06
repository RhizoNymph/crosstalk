//! Eval keys and spec ids as bench keys and ids. Exchange ids are carried as
//! they are (`ExchangeId::from_raw` on the spec's 128 bits): a derived id is
//! the bench's `exchange_id` of the same dataset, source and time, and a
//! recorded one (the gateway's) is the gateway's.

use a2a_bench_format as bench;
use crosstalk_spec::ids::{AgentId, ExchangeId, TransmissionId};
use crosstalk_spec::support::ByteRange as SpecRange;

use super::GoldenError;
use crate::keys::{DatasetId, SourceRef, WorldKey};

pub fn dataset(id: &DatasetId) -> Result<bench::ids::DatasetId, GoldenError> {
    bench::ids::DatasetId::new(id.as_str()).map_err(GoldenError::Key)
}

pub fn world(key: &WorldKey) -> Result<bench::ids::WorldKey, GoldenError> {
    bench::ids::WorldKey::new(key.as_str()).map_err(GoldenError::Key)
}

/// An agent by its name: bench agent keys are unique within their world.
pub fn agent(name: &str) -> Result<bench::ids::AgentKey, GoldenError> {
    bench::ids::AgentKey::new(name).map_err(GoldenError::Key)
}

pub fn label(id: String) -> Result<bench::ids::LabelId, GoldenError> {
    bench::ids::LabelId::new(id).map_err(GoldenError::Key)
}

/// A detector's agent: its spec id's ULID text.
pub fn detector_agent(id: AgentId) -> Result<bench::ids::DetectorAgent, GoldenError> {
    bench::ids::DetectorAgent::new(id.ulid_text()).map_err(GoldenError::Key)
}

/// A detector's transmission: its spec id's ULID text.
pub fn transmission(id: TransmissionId) -> Result<bench::ids::TransmissionRef, GoldenError> {
    bench::ids::TransmissionRef::new(id.ulid_text()).map_err(GoldenError::Key)
}

pub fn exchange(id: ExchangeId) -> bench::ids::ExchangeId {
    bench::ids::ExchangeId::from_raw(id.as_ulid())
}

pub fn source(source: &SourceRef) -> bench::ids::SourceRef {
    bench::ids::SourceRef::new(source.file.clone(), source.path.clone())
}

pub fn range(range: SpecRange) -> Result<bench::location::ByteRange, GoldenError> {
    bench::location::ByteRange::new(range.start(), range.end()).map_err(GoldenError::Range)
}
