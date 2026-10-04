//! Aggregates: summaries computed from the derived tier.
//!
//! Edges, series, topics and projections can be dropped and rebuilt from
//! transmissions. Alerts cannot be rebuilt (operators act on them), but
//! their subjects always point back into the derived tier. The topic
//! history's statuses and times record what the pipeline did; its sizes and
//! lineage can be rebuilt from topic assignments and centroids. The
//! [`filter::TopologyFilter`] selects transmissions identically in every
//! view built from them. [`retention`] decides which topic-model versions
//! keep their per-version data, and [`watermark`] when an edge bucket is
//! final.

pub mod alert;
pub mod edge;
pub mod filter;
pub mod projection;
pub mod retention;
pub mod series;
pub mod topic;
pub mod topic_history;
pub mod watermark;
