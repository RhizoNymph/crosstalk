//! The conformance tests, one `async fn` per test, generic over the
//! harness, by area. [`suite!`](crate::suite) instantiates them.

pub mod channels;
pub mod graph;
pub mod projections;
pub mod refusals;
pub mod scenarios;
pub mod series;
