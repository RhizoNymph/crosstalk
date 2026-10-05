//! Detectors that drive the gateway's own layers.
//!
//! [`live`] scores the real detection path (L3 reconstruction, L4
//! provenance, L5 flow, L6 classification, L7 topology) through one seam,
//! `LiveBackend`: a fresh composition per world, fed the world's exchanges,
//! settled, then read back through the spec's read traits.

pub mod live;
