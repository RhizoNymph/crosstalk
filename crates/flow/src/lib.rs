//! L5 flow for crosstalk: resource extraction, the channel registry, the
//! correlator and the verdict store.
//!
//! Implements [`crosstalk_spec::interfaces::l5_flow`].
//!
//! Roadmap: P5 (L5 flow). A layer crate: it depends on the spec, never on
//! another layer crate.

pub mod consumer;
pub mod correlate;

#[cfg(test)]
mod tests {}
