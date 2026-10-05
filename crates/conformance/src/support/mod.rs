//! What tests share: the provisioned [`World`], paging, aligned windows,
//! reads, observable-state snapshots and the live-feed and export drains.

pub mod check;
pub mod paging;
pub mod reads;
pub mod windows;
mod world;

pub use paging::{collect, first};
pub use world::World;
