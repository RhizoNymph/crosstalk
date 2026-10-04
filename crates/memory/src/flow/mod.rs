//! L5 reference stores: the channel registry and directory, and the
//! verdict store.
//!
//! - [`MemoryChannels`] implements `ChannelRegistry`, `ChannelTraffic`,
//!   `ChannelReads` and `ChannelDirectory`: lookups, declarations, policy
//!   decisions with their history, promotion by `promotion::plan` (and its
//!   preview by `promotion::coverage`), supersession, per-resource use, the
//!   flow consumer's traffic writes and the stored channels.
//! - [`MemoryVerdicts`] implements `TransmissionStore` and
//!   `TransmissionVerdicts` over one table.
//!
//! `ResourceExtractor` and `Correlator` are computations, not stores, and
//! are not here.
//!
//! [`registry::model`] and [`verdicts::model`] are the model-based property
//! harnesses the Postgres stores reuse.

pub mod registry;
pub mod verdicts;

pub use registry::MemoryChannels;
pub use verdicts::MemoryVerdicts;
