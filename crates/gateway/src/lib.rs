//! The crosstalk gateway: configuration, wiring and process roles behind the
//! `crosstalk` binary.
//!
//! Composes the implementations of the layers in
//! [`crosstalk_spec::interfaces`]; it is the one place layer crates are
//! wired together. Today that is the capture slice (roadmap P3, milestone
//! M1) in single-node mode:
//!
//! - [`pipeline::Pipeline`] is the composition behind the proxy, as a
//!   library entry point: [`pipeline::Pipeline::build`] over a blob store,
//!   a bus and an injected clock, with the [`capture`] stage (L1
//!   normalization of what the proxy hands off) and the [`log`] consumer
//!   (a P3 stopgap: the spec has no exchange store). Both the capture
//!   stage and a caller holding a pre-normalized exchange enter through
//!   [`pipeline::Pipeline::ingest`]: store the blobs in the blob store,
//!   publish `ExchangeCaptured` on the bus (`crosstalk-transport`).
//! - [`gateway::start`] runs a [`role::Role`]: the L0 proxy
//!   (`crosstalk-ingress`), a pipeline with the role's stages, and the
//!   [`ops`] listener (`/metrics`, `/healthz`, `/readyz`).
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
pub mod live;
pub mod log;
pub mod logging;
pub mod normalize_failure;
pub mod ops;
pub mod pipeline;
pub mod role;
pub mod server;
pub mod spool;
pub mod store;
pub mod tasks;

#[cfg(test)]
mod dst;
#[cfg(test)]
mod integration;
#[cfg(test)]
mod tests;
