//! The HTTP and SSE server for the crosstalk L8 surface.
//!
//! Serves [`crosstalk_spec::interfaces::l8_surface`] over HTTP, with request
//! and response bodies in the JSON of [`crosstalk_spec::wire`].
//!
//! Today it holds [`in_process`]: the `crosstalk-surface` service built over
//! the `crosstalk-memory` reference stores in this process, the backend a UI
//! links in tests and development until the HTTP server exists.
//!
//! Roadmap: P2.6 (the in-process surface) and P7.1 (HTTP API server). A
//! composition crate: it may depend on layer crates.

pub mod in_process;

pub use in_process::{Backbone, InProcess, InProcessError, InProcessOptions, MemoryStores};

#[cfg(test)]
mod tests;
