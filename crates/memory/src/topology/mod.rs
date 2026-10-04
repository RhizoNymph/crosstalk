//! L7 reference store ([`crosstalk_spec::interfaces::l7_topology`]).
//!
//! - `EdgeStore` is [`store::InMemoryEdgeStore`], computed straight from
//!   the stored contributions and accesses. Its writes and version
//!   lifecycle are in [`store`], its reads in [`reads`], and the fold every
//!   read shares in [`fold`].
//! - A `FrontierSource` the test sets is [`store::ManualFrontier`].
//! - What the store reads from other stores at query time is [`mod@env`].

pub mod env;
pub mod fold;
pub mod reads;
pub mod store;

#[cfg(test)]
mod tests;
