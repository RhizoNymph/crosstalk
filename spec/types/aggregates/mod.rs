//! Aggregates: summaries computed from the derived tier.
//!
//! Edges, topics and projections can be dropped and rebuilt from
//! transmissions. Alerts cannot be rebuilt (operators act on them), but
//! their subjects always point back into the derived tier. The
//! [`filter::TopologyFilter`] selects transmissions identically in every
//! view built from them.

pub mod alert;
pub mod edge;
pub mod filter;
pub mod projection;
pub mod topic;
