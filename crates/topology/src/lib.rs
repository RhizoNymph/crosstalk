//! L7 topology for crosstalk: edge and access buckets, graphs, series, the
//! watermark and retention.
//!
//! Implements [`crosstalk_spec::interfaces::l7_topology`] on Postgres:
//!
//! - [`store::PgEdgeStore`]: the spec's `EdgeStore`, with range-partitioned
//!   bucket tables in the `topology` schema (decision D3: plain Postgres).
//! - [`mod@env`]: what the store reads at query time from other layers (the
//!   topic catalog, the directories, the node facts).
//! - [`outbox`]: the events the store decides, committed with the change
//!   and relayed to the bus after commit, traffic changes coalesced.
//! - [`consumer`]: the `topology` consumer group, which feeds the store
//!   from the bus and recomputes the watermark every bucket width.
//!
//! Roadmap: P6.1 (L7 topology). A layer crate: it depends on the spec and
//! `crosstalk-store`, never on another layer crate.

pub mod codec;
pub mod consumer;
pub mod env;
pub mod outbox;
pub mod store;

#[cfg(test)]
mod dst;
#[cfg(test)]
mod integration;
#[cfg(test)]
mod props;
#[cfg(test)]
mod tests;
