//! Aggregates: summaries computed from the derived tier.
//!
//! Edges and topics can be dropped and rebuilt from transmissions. Alerts
//! cannot be rebuilt (operators act on them), but their subjects always
//! point back into the derived tier.

pub mod alert;
pub mod edge;
pub mod topic;
