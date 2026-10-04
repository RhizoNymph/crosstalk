//! The HTTP client for the crosstalk L8 surface, used by the operator UI.
//!
//! Implements the traits of [`crosstalk_spec::interfaces::l8_surface`] over
//! HTTP, decoding the JSON of [`crosstalk_spec::wire`].
//!
//! Roadmap: P7.2 (HTTP client). A composition crate: it may depend on layer
//! crates.

// Every crate builds on the spec; the dependency is declared before any
// code uses it.
use crosstalk_spec as _;

#[cfg(test)]
mod tests {}
