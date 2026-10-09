//! Detectors that drive the gateway's own layers.
//!
//! [`live`] runs the real detection path (L3 reconstruction, L4
//! provenance, L5 flow, L6 classification, L7 topology) through one seam,
//! `LiveBackend`: a fresh composition per world, fed the world's exchanges,
//! settled, then read back through the spec's read traits.

pub mod live;

use crosstalk_spec::interfaces::l1_canonical::NormalizedExchange;
use crosstalk_spec::support::Timestamp;

/// One exchange as a detector ingests it: its normalized form and the time
/// it is ingested at. A world's exchanges come as a slice of these, in the
/// world's time order ([`crate::convert::ConvertedWorld::timed`]).
#[derive(Debug, Clone, Copy)]
pub struct Timed<'a> {
    pub exchange: &'a NormalizedExchange,
    pub at: Timestamp,
}
