//! Tests of [`PgTopicCatalog`](super::PgTopicCatalog) against Postgres: the
//! memory crate's catalog harness ([`model`]) and focused cases
//! ([`cases`]). Every test is skipped when `TEST_DATABASE_URL` is not
//! configured.

mod cases;
mod model;

use std::sync::Arc;

use crosstalk_memory::analysis::aliases::StaticDirectory;
use crosstalk_memory::model::build::{similarity, test_model, ts, unit};
use crosstalk_spec::aggregates::retention::RetentionPolicy;
use crosstalk_spec::aggregates::topic::{Topic, TopicModelVersion};
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::ids::TopicId;
use crosstalk_spec::support::Timestamp;
use sqlx::PgPool;
use tokio::sync::mpsc::UnboundedReceiver;

use super::{CatalogConfig, CatalogParts, PgTopicCatalog};
use crate::pg::ChannelSink;
use crate::pg::testing::{CURSOR_KEY, retry};

pub(crate) type TestCatalog = PgTopicCatalog<StaticDirectory, ChannelSink>;

/// Keep the two newest versions that have been active; lineage floor 0.5.
pub(crate) fn config() -> CatalogConfig {
    CatalogConfig {
        retention: RetentionPolicy::new(2).unwrap_or_else(|error| panic!("policy: {error:?}")),
        lineage_floor: similarity(0.5).unwrap_or_else(|| panic!("floor")),
    }
}

/// A catalog over `pool` (version 0 active at the epoch), and the receiver
/// of what it publishes.
pub(crate) async fn catalog(
    pool: PgPool,
    agents: StaticDirectory,
) -> (TestCatalog, UnboundedReceiver<BusEvent>) {
    let (sink, events) = ChannelSink::new();
    let catalog = PgTopicCatalog::open(
        pool,
        config(),
        ts(0),
        CatalogParts {
            agents,
            sink: Arc::new(sink),
            cursor_key: CURSOR_KEY,
            retry: retry(),
        },
    )
    .await
    .unwrap_or_else(|error| panic!("opening the catalog: {error}"));
    (catalog, events)
}

/// Topic `k` of `version`, its centroid along `(x, y, z)`, fitted at `at`.
pub(crate) fn topic_of(
    version: TopicModelVersion,
    k: u8,
    xyz: (f32, f32, f32),
    at: Timestamp,
) -> Topic {
    let model = test_model("harness");
    let centroid = unit(&model, xyz.0, xyz.1, xyz.2).unwrap_or_else(|| panic!("centroid"));
    let id = TopicId::from_ulid(crosstalk_memory::model::build::raw(
        u64::from(version.0) * 16 + u64::from(k),
    ));
    crosstalk_memory::model::build::topic(id, version, centroid, at)
}

/// Everything published so far.
pub(crate) fn drain(events: &mut UnboundedReceiver<BusEvent>) -> Vec<BusEvent> {
    let mut out = Vec::new();
    while let Ok(event) = events.try_recv() {
        out.push(event);
    }
    out
}
