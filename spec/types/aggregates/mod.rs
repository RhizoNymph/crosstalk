//! Aggregates: summaries computed from the derived tier.
//!
//! Edges, series and topics can be dropped and rebuilt from transmissions.
//! Alerts cannot be rebuilt (operators act on them), but their subjects
//! always point back into the derived tier. The topic history's statuses and
//! times record what the pipeline did; its sizes and lineage can be rebuilt
//! from topic assignments and centroids.

pub mod alert;
pub mod edge;
pub mod series;
pub mod topic;
pub mod topic_history;
