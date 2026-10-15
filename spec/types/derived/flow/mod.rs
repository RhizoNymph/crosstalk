//! Flow: who wrote where, who read where, and what crossed.
//!
//! Tool calls are turned into [`access::Access`]es against
//! [`resource::Resource`]s. Resources group into [`channel::Channel`]s. A
//! write by one agent followed by a read by another on the same channel opens
//! a [`transmission::Transmission`], which a content match confirms. Operators
//! judge transmissions with [`verdict::Verdict`]s, kept beside the detector's
//! state, never in it.

pub mod access;
pub mod channel;
pub mod evidence;
pub mod resource;
pub mod timing;
pub mod transmission;
pub mod verdict;
