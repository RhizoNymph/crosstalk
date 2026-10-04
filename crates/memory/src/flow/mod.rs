//! L5 reference stores: the channel registry and directory, and the
//! verdict store.
//!
//! - [`MemoryChannels`] implements `ChannelRegistry` and
//!   `ChannelDirectory`: lookups, declarations, policy decisions with
//!   their history, promotion by `promotion::plan` (and its preview by
//!   `promotion::coverage`), supersession, and per-resource use; plus
//!   [`SeedChannels`], the flow consumer's writes.
//! - [`MemoryVerdicts`] implements `TransmissionVerdicts` over stored
//!   transmissions; plus [`SeedTransmissions`].
//!
//! `ResourceExtractor` and `Correlator` are computations, not stores, and
//! are not here.
//!
//! [`registry::model`] and [`verdicts::model`] are the model-based property
//! harnesses the Postgres stores reuse.

pub mod registry;
pub mod verdicts;

pub use registry::{DetectionUpdate, MemoryChannels, SeedChannels, SeedError};
pub use verdicts::{MemoryVerdicts, SeedTransmissions};
