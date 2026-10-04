//! L2 transport for crosstalk: the in-process event bus with consumer groups,
//! retries and dead letters, and the blob store.
//!
//! Implements [`crosstalk_spec::interfaces::l2_transport`].
//!
//! Roadmap: P2.1 (L2 transport: in-process bus) and P2.2 (blob store). A layer
//! crate and infrastructure: other layer crates may use it only as a
//! dev-dependency.

// Every crate builds on the spec; the dependency is declared before any
// code uses it.
use crosstalk_spec as _;

pub mod blob;

#[cfg(test)]
mod tests {}
