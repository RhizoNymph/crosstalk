//! Tests for the invariants the spec's types enforce at runtime: checked
//! constructors, pattern matching, policy routing, event subjects and the
//! shared view filter. Invariants that the type structure enforces need no
//! test; they cannot be violated.

mod fixtures;

mod action_errors;
mod agent_reads;
mod agents;
mod aggregates;
mod alerts;
mod audit;
mod channel_reads;
mod channels;
mod confirmation;
mod conversation;
mod encoding;
mod events;
mod evidence;
mod excerpt;
mod export;
mod export_stream;
mod filter;
mod flow;
mod graph;
mod infrastructure;
mod live;
mod minting;
mod observed;
mod operators;
mod overview;
mod paging;
mod part_text;
mod pattern_overlap;
mod policy;
mod projection;
mod projection_frame;
mod provenance;
mod quality;
mod query_errors;
mod repository;
mod retention;
mod rules;
mod secrets;
mod send;
mod series;
mod summary;
mod support;
mod surface;
mod topic_history;
mod topic_version;
mod verdicts;
mod watermark;
mod wire;
