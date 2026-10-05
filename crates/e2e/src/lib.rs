//! The end-to-end smoke harness: real harness traffic, fed through the
//! gateway's pipeline, read back through the L8 surface the UI uses.
//!
//! - [`scenario`]: the scripted wiki relay. Agent A (a Claude Code
//!   session) writes a wiki page carrying a distinctive sentence; agent B
//!   (another session, another credential) reads the page and repeats the
//!   sentence. Each exchange is the HTTP request Claude Code sends and the
//!   event stream Anthropic answers with, at fixed times with fixed ids.
//! - [`capture`]: turns a scripted exchange into the `NormalizedExchange`
//!   `Pipeline::ingest` takes, through L0's route table, identifier and
//!   adapter and L1's normalizer.
//! - [`compose`](mod@compose): the composition the harness drives,
//!   `crosstalk_gateway::live::Live` (the pipeline, the layer consumers
//!   and the surface over one set of stores).
//! - [`feed`](mod@feed): ingests the scenario in time order, moving the clock.
//! - [`options`]: the composition's configuration.
//! - [`read`]: the scenario read back through `QueryApi`, as the UI reads.
//!
//! The scenario is a library so `crosstalk-ui` can feed the same traffic
//! into a running gateway for a demo. A composition crate beside the
//! gateway: it may depend on layer crates, and no layer crate depends on
//! it.

pub mod capture;
pub mod compose;
pub mod feed;
pub mod options;
pub mod read;
pub mod scenario;

pub use capture::{Capture, CaptureError};
pub use compose::{ComposeError, Composition, compose, compose_with};
pub use feed::{Fed, FeedError, feed};
pub use scenario::{Scenario, WireExchange};
