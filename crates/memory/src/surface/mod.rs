//! L8 reference stores ([`crosstalk_spec::interfaces::l8_surface`]):
//! everything stateful but `QueryApi`, `OperatorActions` and `LiveFeed`,
//! which `crosstalk-surface` composes over these.
//!
//! - `AuditLog` is [`audit::InMemoryAuditLog`], append-only.
//! - The stored `OperatorDirectory` is [`operators::InMemoryOperatorStore`],
//!   each config load recorded in the audit log with the change.
//! - The configured sinks and their last deliveries are
//!   [`sinks::InMemorySinkRegistry`]; [`sinks::FakeSink`] is an `AlertSink`
//!   double.
//! - The cursor book every reference store pages with is [`paging`].

pub mod audit;
pub mod operators;
pub mod paging;
pub mod sinks;

#[cfg(test)]
mod tests;
