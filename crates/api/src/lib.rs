//! The HTTP and SSE server for the crosstalk L8 surface.
//!
//! Serves [`crosstalk_spec::interfaces::l8_surface`] over HTTP, with request
//! and response bodies in the JSON of [`crosstalk_spec::wire`].
//!
//! Roadmap: P7.1 (HTTP API server). A composition crate: it may depend on layer
//! crates.

// Every crate builds on the spec; the dependency is declared before any
// code uses it.
use crosstalk_spec as _;

#[cfg(test)]
mod tests {}
