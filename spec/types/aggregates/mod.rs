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
//! final. Graph responses describe their agents and channels with
//! [`node::GraphNode`]s; the channel-centred view adds access edges
//! ([`access`]). The agents list and an agent's page read [`agents`]: one
//! row per canonical agent, with its traffic counted as its graph node's.

pub mod access;
pub mod agents;
pub mod alert;
pub mod edge;
pub mod filter;
pub mod node;
pub mod projection;
pub mod quality;
pub mod retention;
pub mod series;
pub mod topic;
pub mod topic_history;
pub mod watermark;
