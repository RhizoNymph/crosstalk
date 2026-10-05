//! The end-to-end smoke: the wiki relay fed through the pipeline and read
//! back through the surface.
//!
//! - [`scenario`]: the traffic itself, and what L0 and L1 make of it.
//! - [`determinism`]: two settled runs give the same transmissions.
//! - [`extract`]: the scenario's tool calls through L5's extractor.
//! - [`pipeline`]: the composition ingests it; bodies stored, every
//!   exchange published.
//! - [`serve`]: `serve --role all` on ephemeral ports, read back over HTTP.
//! - [`surface`]: the surface's view of it. The assertions that need L3 to
//!   L7 consuming the pipeline's bus are ignored until `Live` composes them.

mod determinism;
mod extract;
mod pipeline;
mod scenario;
mod serve;
mod support;
mod surface;
