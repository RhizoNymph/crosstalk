//! Temporary stand-ins for spec changes the UI needs that have not landed
//! on the gateway's spec yet. Everything here is meant to be deleted, with
//! its imports switched to `crosstalk_spec`, once the spec carries it.
//!
//! - [`channel_semantics`]: channels exist only through cross-agent
//!   transmissions (confirmation, listings, a channel's transmissions, the
//!   confirmed-only filter). Delete it when the gateway's port of that spec
//!   change lands (invariants INV-850..869).

pub mod channel_semantics;
