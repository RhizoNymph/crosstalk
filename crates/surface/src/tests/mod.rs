//! Tests of the surface over the in-memory reference stores.
//!
//! - [`world`]: every reference store wired together, the surface over
//!   them, the configured operators and seeding through the write traits.
//! - [`fakes`]: the recording bus and the evidence records.
//! - One module per area of invariants.

mod actions;
mod alerts;
mod channels;
mod content;
pub(crate) mod conversation_fakes;
mod conversations;
mod export;
pub(crate) mod fakes;
mod listing;
mod live;
mod nodes;
mod outcomes;
mod permissions;
mod props;
mod reads;
pub(crate) mod world;

use crosstalk_spec::derived::flow::resource::{Host, ResourcePattern};
use crosstalk_spec::paging::{PageRequest, PageSize};

/// A first page of `size` items.
pub fn page<L>(size: u16) -> PageRequest<L> {
    match PageSize::new(size) {
        Ok(size) => PageRequest { size, after: None },
        Err(error) => panic!("page size {size}: {error:?}"),
    }
}

/// A pattern over `wiki.example`.
pub fn pattern() -> ResourcePattern {
    ResourcePattern::Host(Host("wiki.example".to_owned()))
}
