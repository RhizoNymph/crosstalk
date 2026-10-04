//! Derived tier: inferences drawn from observed data.
//!
//! - [`provenance`]: which text an agent originated, and where that text
//!   later shows up.
//! - [`flow`]: which resources agents write and read, the channels those
//!   resources form, and the transmissions that cross them.
//!
//! Every claim here points at its evidence by id. A transmission cannot be
//! confirmed without a content match, and an agent-to-agent edge cannot
//! exist without a transmission.

pub mod flow;
pub mod provenance;
