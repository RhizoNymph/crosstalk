//! What the UI needs from L8 that `crosstalk-spec` does not have yet.
//!
//! Each item is numbered as in "The L8 contract" in `docs/features/ui.md`.
//! Names and shapes are the ones the UI asks the gateway for. Types that
//! replace a spec type of the same name (`TopologyFilter`, `OperatorAction`,
//! `QueryError`) say so. When the gateway's types land, this module is
//! deleted and its users import those instead.

pub mod actions;
pub mod agents;
pub mod alerts;
pub mod channels;
pub mod errors;
pub mod evidence;
pub mod graph;
pub mod lists;
pub mod research;
pub mod rules;
pub mod scope;
pub mod search;
pub mod topics;
pub mod verdict;

/// A 128-bit entity id for the entities this module introduces, in the same
/// form as `crosstalk_spec::ids`.
macro_rules! contract_id {
    ($($(#[$doc:meta])* $name:ident;)*) => {$(
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(u128);

        impl $name {
            pub const fn from_ulid(raw: u128) -> Self {
                Self(raw)
            }

            pub const fn as_ulid(self) -> u128 {
                self.0
            }
        }
    )*};
}

contract_id! {
    /// A logged agent merge (item 15).
    MergeId;
    /// A stored UMAP projection (item 9).
    ProjectionId;
    /// An alert sink (item 18).
    SinkId;
    /// An audit log entry (item 12).
    AuditId;
}

