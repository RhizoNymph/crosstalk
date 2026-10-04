//! The crosstalk gateway: configuration, wiring and process roles behind the
//! `crosstalk` binary.
//!
//! Composes the implementations of the layers in
//! [`crosstalk_spec::interfaces`]; it is the one place layer crates are
//! wired together. Today that is the capture slice (roadmap P3, milestone
//! M1) in single-node mode:
//!
//! - [`gateway::start`] runs a [`role::Role`]: the L0 proxy
//!   (`crosstalk-ingress`), the [`capture`] stage that normalizes each
//!   exchange with L1 (`crosstalk-canonical`), stores its bodies in the
//!   blob store and publishes `ExchangeCaptured` on the in-process bus
//!   (`crosstalk-transport`), the [`log`] consumer that persists every
//!   captured exchange (a P3 stopgap: the spec has no exchange store), and
//!   the [`ops`] listener (`/metrics`, `/healthz`, `/readyz`).
//! - [`config`] is the JSON config of the deployment contract; [`cli`]
//!   the command line (`serve`, `migrate`, `healthcheck`, `inspect`);
//!   [`store`] the Postgres side of `migrate` and `/readyz`;
//!   [`healthcheck`] the container healthcheck client; [`inspect`] reads
//!   back what was captured.
//!
//! Concurrency is tokio tasks joined by channels; the only shared state is
//! atomic counters and per-task running flags.

pub mod capture;
pub mod cli;
pub mod config;
pub mod gateway;
pub mod healthcheck;
pub mod inspect;
pub mod log;
pub mod logging;
pub mod normalize_failure;
pub mod ops;
pub mod role;
pub mod server;
pub mod store;
pub mod tasks;

#[cfg(test)]
mod tests;
