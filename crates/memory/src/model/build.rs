//! Small value builders the harnesses and the unit tests share. Ids are
//! built from small numbers, so generated operations collide on purpose.

use std::num::{NonZeroU16, NonZeroU64};
use std::time::Duration;

use crosstalk_spec::aggregates::series::BucketWidth;
use crosstalk_spec::aggregates::topic::{Embedding, EmbeddingModel, Topic, TopicModelVersion};
use crosstalk_spec::derived::flow::timing::CorrelationTiming;
use crosstalk_spec::ids::{
    AccessId, AgentId, AlertRuleId, AuditId, ChannelId, OperatorId, ProjectionId, ResourceId,
    SinkId, TopicId, TransmissionId,
};
use crosstalk_spec::support::{Similarity, TimeWindow, Timestamp};

use crate::analysis::catalog::{CatalogConfig, InMemoryTopicCatalog, RetentionPolicy};
use crate::support::Outbox;

/// Every id kind from a small number, offset past the reserved rule ids.
pub fn raw(n: u64) -> u128 {
    (1u128 << 100) + u128::from(n)
}

pub fn agent(n: u64) -> AgentId {
    AgentId::from_ulid(raw(n))
}

pub fn channel(n: u64) -> ChannelId {
    ChannelId::from_ulid(raw(n))
}

pub fn transmission(n: u64) -> TransmissionId {
    TransmissionId::from_ulid(raw(n))
}

pub fn access(n: u64) -> AccessId {
    AccessId::from_ulid(raw(n))
}

pub fn resource(n: u64) -> ResourceId {
    ResourceId::from_ulid(raw(n))
}

pub fn topic_id(n: u64) -> TopicId {
    TopicId::from_ulid(raw(n))
}

pub fn operator(n: u64) -> OperatorId {
    OperatorId::from_ulid(raw(n))
}

pub fn sink(n: u64) -> SinkId {
    SinkId::from_ulid(raw(n))
}

pub fn projection(n: u64) -> ProjectionId {
    ProjectionId::from_ulid(raw(n))
}

pub fn audit_id(n: u64) -> AuditId {
    AuditId::from_ulid(raw(n))
}

pub fn rule_id(n: u64) -> AlertRuleId {
    AlertRuleId::from_ulid(raw(n))
}

pub fn ts(micros: u64) -> Timestamp {
    Timestamp::from_micros(micros)
}

/// `[start, end)`; `None` when empty.
pub fn window(start: u64, end: u64) -> Option<TimeWindow> {
    TimeWindow::new(ts(start), ts(end)).ok()
}

pub fn similarity(value: f32) -> Option<Similarity> {
    Similarity::new(value).ok()
}

pub fn non_zero(n: u64) -> NonZeroU64 {
    NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN)
}

/// The three-dimensional model the harnesses embed with.
pub fn test_model(name: &str) -> EmbeddingModel {
    EmbeddingModel {
        name: name.to_owned(),
        dimension: NonZeroU16::new(3).unwrap_or(NonZeroU16::MIN),
    }
}

/// `(x, y, z)` normalized under `model`; the first axis for the zero
/// vector.
pub fn unit(model: &EmbeddingModel, x: f32, y: f32, z: f32) -> Option<Embedding> {
    let norm = (x * x + y * y + z * z).sqrt();
    let values = if norm > 0.0 {
        vec![x / norm, y / norm, z / norm]
    } else {
        vec![1.0, 0.0, 0.0]
    };
    Embedding::new(model.clone(), values).ok()
}

/// A topic of `version` fitted at `fitted_at` with `centroid`.
pub fn topic(
    id: TopicId,
    version: TopicModelVersion,
    centroid: Embedding,
    fitted_at: Timestamp,
) -> Topic {
    Topic {
        id,
        version,
        label: format!("topic {}", id.as_ulid()),
        terms: Vec::new(),
        centroid,
        fitted_at,
    }
}

/// One-microsecond-aligned buckets of `micros`.
pub fn bucket_width(micros: u64) -> BucketWidth {
    BucketWidth::from_micros(non_zero(micros))
}

/// A correlator timing whose `settle_after` is `settle_micros`.
pub fn timing(settle_micros: u64) -> Option<CorrelationTiming> {
    let half = Duration::from_micros(settle_micros / 2);
    let rest = Duration::from_micros(settle_micros - settle_micros / 2);
    CorrelationTiming::new(Duration::from_micros(1), half, rest).ok()
}

/// A catalog keeping the last `keep_last` activated versions, with a
/// lineage floor of `floor`, version 0 active since the epoch, publishing
/// to `outbox`.
pub fn catalog(keep_last: u32, floor: f32, outbox: Outbox) -> Option<InMemoryTopicCatalog> {
    let config = CatalogConfig {
        retention: RetentionPolicy::new(keep_last).ok()?,
        lineage_floor: similarity(floor)?,
    };
    InMemoryTopicCatalog::new(config, ts(0), outbox).ok()
}
