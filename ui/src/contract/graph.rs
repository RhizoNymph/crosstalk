//! What the channel area still reads from the contract: a channel's shape
//! for its name (item 24). Graphs, series, the overview and transmission
//! rows are the spec's (`crosstalk_spec::aggregates::{edge, access, node,
//! series}`, `interfaces::l8_surface::summary`).

use crosstalk_spec::derived::flow::resource::{Locator, ResourcePattern};

/// What a channel is named by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelShape {
    /// A declared channel's pattern.
    Pattern(ResourcePattern),
    /// A discovered channel's seed resource.
    Seed(Locator),
}
