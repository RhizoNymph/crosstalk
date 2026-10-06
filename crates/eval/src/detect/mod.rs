//! Detectors that drive the gateway's own layers.
//!
//! [`live`] scores the real detection path (L3 reconstruction, L4
//! provenance, L5 flow, L6 classification, L7 topology) through one seam,
//! `LiveBackend`: a fresh composition per world, fed the world's exchanges,
//! settled, then read back through the spec's read traits.

pub mod live;

use crosstalk_spec::interfaces::l1_canonical::NormalizedExchange;
use crosstalk_spec::support::Timestamp;

/// One exchange as a detector ingests it: its normalized form and the time
/// it is ingested at. A world's exchanges come as a slice of these, in the
/// world's time order, whether they were built by a converter
/// ([`crate::corpus::World`]) or read from a bench export
/// ([`crate::bench_detect`]).
#[derive(Debug, Clone, Copy)]
pub struct Timed<'a> {
    pub exchange: &'a NormalizedExchange,
    pub at: Timestamp,
}

impl<'a> Timed<'a> {
    /// Every exchange of `world`, in its order.
    pub fn of_world(world: &'a crate::corpus::World) -> Vec<Self> {
        world
            .exchanges()
            .iter()
            .map(|exchange| Self {
                exchange: exchange.normalized(),
                at: exchange.at(),
            })
            .collect()
    }
}
