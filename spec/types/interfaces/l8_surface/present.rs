//! Where the gateway is now and what it is configured with: the values a
//! client needs before it can build a valid request (`QueryApi::present`).
//!
//! Every field answers a question the other queries cannot:
//!
//! - **`now`.** A default view is "the last 24 hours". The watermark trails
//!   the newest data by the settling delay, so it is not the present; the
//!   gateway's wall clock is.
//! - **`bucket_width`.** Graph, channel, agent and series queries refuse a
//!   window off bucket boundaries (`InvalidInput(UnalignedWindow)`) and a
//!   series grid built for another width (`InvalidInput(BucketWidthMismatch)`).
//!   A client snaps its windows and builds its `SeriesGrid` with this width,
//!   which is L7's (`EdgeStore::bucket_width`).
//! - **`export_formats`.** The formats `export` writes, in the order a form
//!   offers them; any other is `InvalidInput(UnsupportedFormat)`.
//! - **`current_rule_version`.** The topic-model version a new or updated
//!   watched-topic rule must name (`AlertRuleStore::create`, `update`):
//!   the version the alerts consumer last made current on
//!   `TopicVersionReady`. It can be newer than the active version, which
//!   waits for L7's buckets, and it lags the catalog by the consumer's
//!   delay, so the topic history alone cannot give it. A rule naming
//!   another version is `Conflict(TopicVersionNotCurrent)`.
//! - **`default_remap_threshold`.** What a watched-topic rule created
//!   without a remap threshold takes (`AlertRuleConfig`), so a rule form and
//!   the topic lineage view can show it.
//! - **`frame_retention_micros`.** How long a ready projection's frame is
//!   kept after its fit ([`FrameRetention`]), so a client can say when a
//!   projection will expire (`FrameRetention::expires_at` of
//!   `Fitted::fitted_at`) instead of learning it from `ProjectionNotRetained`.
//!
//! Only `now` changes from one read to the next. `current_rule_version`
//! changes when the alerts consumer handles `TopicVersionReady` (the live
//! feed's `TopicVersionReady` is the client's cue to re-read), and the rest
//! change only with a config load, each audited as a `ConfigChange` where
//! it has one.
//!
//! A response, never a request: the server stamps `now` from its clock and
//! the rest from its config.

use serde::{Deserialize, Serialize};

use crate::aggregates::projection::FrameRetention;
use crate::aggregates::series::BucketWidth;
use crate::aggregates::topic::TopicModelVersion;
use crate::support::{Similarity, Timestamp};

use super::export::ExportFormats;

/// `QueryApi::present`'s answer. Every field's type holds its own rule
/// (formats non-empty and distinct, a non-zero bucket width and retention,
/// a threshold in `0..=1`), so no combination of fields is invalid.
///
/// On the wire:
///
/// ```json
/// {"now": "2026-10-04T12:58:30.000000Z", "bucket_width": 300000000,
///  "export_formats": ["jsonl"], "current_rule_version": 4,
///  "default_remap_threshold": 0.8, "frame_retention_micros": 15552000000000}
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Present {
    /// The gateway's wall clock when it answered.
    pub now: Timestamp,
    /// L7's bucket width (`EdgeStore::bucket_width`): every window a view
    /// sends starts and ends on a multiple of it.
    pub bucket_width: BucketWidth,
    /// The formats `export` writes, in the order a form offers them.
    pub export_formats: ExportFormats,
    /// The topic-model version watched-topic rules are written against.
    pub current_rule_version: TopicModelVersion,
    /// The remap threshold of a watched-topic rule created without one.
    pub default_remap_threshold: Similarity,
    /// How long a ready projection's frame is kept after its fit.
    pub frame_retention_micros: FrameRetention,
}
