//! L2 transport for crosstalk: the in-process event bus with consumer groups,
//! retries and dead letters, and the blob store.
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
//!
//! Roadmap: P2.1 (L2 transport: in-process bus) and P2.2 (blob store). A layer
//! crate and infrastructure: other layer crates may use it only as a
//! dev-dependency.
//!
//! [`EventBus`]: crosstalk_spec::interfaces::l2_transport::EventBus
//! [`Subscription`]: crosstalk_spec::interfaces::l2_transport::Subscription
//! [`DeadLetterStore`]: crosstalk_spec::interfaces::l2_transport::DeadLetterStore
//! [`RetryPolicy`]: crosstalk_spec::interfaces::l2_transport::RetryPolicy

mod bus;
mod codec;
mod config;
mod dedup;
mod rng;

pub use bus::{DeadLetters, GroupDepth, MemoryHandledIds, MpscBus, MpscSubscription, StartError};
pub use config::{BusConfig, DeliveryOrder, InvalidBusConfig, NonZeroDuration};
pub use dedup::{Dedup, HandledIds};

#[cfg(test)]
mod dst;
#[cfg(test)]
mod testing;
#[cfg(test)]
mod tests;
