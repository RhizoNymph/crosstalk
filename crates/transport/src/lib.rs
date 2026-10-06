//! L2 transport for crosstalk: the in-process and Postgres event buses with
//! consumer groups, retries and dead letters, the publish spool, and the blob
//! store.
//!
//! Implements [`crosstalk_spec::interfaces::l2_transport`]:
//!
//! - [`MpscBus`] is the single-node [`EventBus`]: one tokio task owns every
//!   consumer group, delivery and dead letter, and handles talk to it over
//!   channels. Envelopes cross it as their wire JSON (the `codec` module), so a
//!   single node decodes exactly as a cluster node does.
//! - [`MpscSubscription`] is its [`Subscription`]; [`DeadLetters`] its
//!   [`DeadLetterStore`].
//! - [`Dedup`] wraps any subscription with envelope-level deduplication over
//!   a [`HandledIds`] record ([`MemoryHandledIds`] on this bus).
//! - [`BusConfig`] sizes the bus and gives the default [`RetryPolicy`].
//! - [`PgBus`] (the [`pg`] module) is the durable single-node bus: the log,
//!   groups, deliveries and dead letters in the `transport` schema, with
//!   [`PgSubscription`], [`PgDeadLetters`], [`PgBus::recover_held`],
//!   [`PgBus::group_stats`] and [`PgBus::prune`].
//! - [`SpoolingBus`] (the [`spool`] module) fronts a bus with an fsynced
//!   on-disk spool for what it cannot take while its database is down, and
//!   drains it in order under the same ids ([`DrainTarget`]).
//! - [`blob`] is the content-addressed [`BlobStore`]: [`blob::FsBlobStore`]
//!   on the filesystem and [`blob::MemoryBlobStore`] in memory.
//!
//! Roadmap: P2.1 (L2 transport: in-process bus), P2.2 (blob store) and P7.3
//! (durable bus and publish spool). A layer
//! crate and infrastructure: other layer crates may use it only as a
//! dev-dependency.
//!
//! [`EventBus`]: crosstalk_spec::interfaces::l2_transport::EventBus
//! [`Subscription`]: crosstalk_spec::interfaces::l2_transport::Subscription
//! [`DeadLetterStore`]: crosstalk_spec::interfaces::l2_transport::DeadLetterStore
//! [`RetryPolicy`]: crosstalk_spec::interfaces::l2_transport::RetryPolicy
//! [`BlobStore`]: crosstalk_spec::interfaces::l2_transport::BlobStore

mod bus;
mod codec;
mod config;
mod dedup;
mod rng;

pub use bus::{DeadLetters, GroupDepth, MemoryHandledIds, MpscBus, MpscSubscription, StartError};
pub use config::{BusConfig, DeliveryOrder, InvalidBusConfig, NonZeroDuration};
pub use dedup::{Dedup, HandledIds};
pub use pg::{
    GroupStats, InvalidPgBusConfig, PgBus, PgBusConfig, PgDeadLetters, PgSubscription, Recovered,
};
pub use spool::{
    DrainTarget, InvalidSpoolConfig, SpoolConfig, SpoolError, SpoolState, SpoolStats, SpoolingBus,
};

pub mod blob;
pub mod pg;
pub mod spool;

#[cfg(test)]
mod conformance;
#[cfg(test)]
mod dst;
#[cfg(test)]
mod integration;
#[cfg(test)]
mod testing;
#[cfg(test)]
mod tests;
