//! The crate's tests. Paths follow the ingress invariants' evidence
//! (`crosstalk_ingress::tests::<module>::<test>`).
//!
//! Real-socket tests use testkit's corpus, `FakeUpstream` and
//! `HarnessClient` ([`support`]); `dst` tests run the same proxy in
//! `crosstalk-sim` over in-memory pipes ([`sim_support`]).

mod adapter;
mod capture;
mod client;
mod credential;
mod encoding;
mod endpoint;
mod failure;
mod framer;
mod meta;
mod passthrough;
mod routing;
mod scenario;
mod sim;
pub(crate) mod sim_support;
mod subscription;
pub(crate) mod support;
