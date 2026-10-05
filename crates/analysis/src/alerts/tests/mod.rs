//! Tests of [`PgAlertStore`](super::PgAlertStore) against Postgres: the
//! memory crate's model harnesses ([`model`]), and focused cases per
//! invariant ([`rules`], [`triage`], [`reads`], [`consumer`]). Every test
//! is skipped when `TEST_DATABASE_URL` is not configured.

mod consumer;
mod model;
mod reads;
mod rules;
mod triage;

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU16;
use std::sync::Arc;

use crosstalk_memory::analysis::aliases::StaticDirectory;
use crosstalk_memory::analysis::fakes::{FakeEmbedder, fake_model};
use crosstalk_memory::model::build::{similarity, sink};
use crosstalk_spec::aggregates::alert::AlertRuleConfig;
use crosstalk_spec::events::BusEvent;
use sqlx::PgPool;
use tokio::sync::mpsc::UnboundedReceiver;

use super::{AlertStoreConfig, AlertStoreParts, NoFacts, PgAlertStore, SubjectFacts};
use crate::pg::ChannelSink;
use crate::pg::testing::{CURSOR_KEY, ids, retry};

/// The store most tests use.
pub(crate) type TestStore<F = NoFacts> =
    PgAlertStore<FakeEmbedder, StaticDirectory, F, ChannelSink>;

/// Sinks 1 and 2 configured, every built-in enabled, remap threshold 0.8.
pub(crate) fn config() -> AlertStoreConfig {
    AlertStoreConfig {
        rules: AlertRuleConfig {
            default_remap_threshold: similarity(0.8).unwrap_or_else(|| panic!("threshold")),
        },
        sinks: BTreeSet::from([sink(1), sink(2)]),
        builtins: BTreeMap::new(),
    }
}

/// The embedder of model "fake" (dimension 8), refusing texts over 40
/// characters.
pub(crate) fn embedder() -> FakeEmbedder {
    FakeEmbedder::new(
        fake_model("fake", NonZeroU16::new(8).unwrap_or(NonZeroU16::MIN)),
        40,
    )
}

/// A store over `pool` with `facts`, and the receiver of what it publishes.
pub(crate) async fn store_with<F: SubjectFacts>(
    pool: PgPool,
    directory: StaticDirectory,
    facts: F,
    config: AlertStoreConfig,
) -> (
    PgAlertStore<FakeEmbedder, StaticDirectory, F, ChannelSink>,
    UnboundedReceiver<BusEvent>,
) {
    let (sink, events) = ChannelSink::new();
    let store = PgAlertStore::open(
        pool,
        config,
        AlertStoreParts {
            embedder: embedder(),
            directory,
            facts,
            sink: Arc::new(sink),
            ids: ids(7),
            cursor_key: CURSOR_KEY,
            retry: retry(),
        },
    )
    .await
    .unwrap_or_else(|error| panic!("opening the alert store: {error}"));
    (store, events)
}

/// A store with no facts and the default config.
pub(crate) async fn store(
    pool: PgPool,
) -> (TestStore, StaticDirectory, UnboundedReceiver<BusEvent>) {
    let directory = StaticDirectory::new();
    let (store, events) = store_with(pool, directory.clone(), NoFacts, config()).await;
    (store, directory, events)
}

/// Everything published so far.
pub(crate) fn drain(events: &mut UnboundedReceiver<BusEvent>) -> Vec<BusEvent> {
    let mut out = Vec::new();
    while let Ok(event) = events.try_recv() {
        out.push(event);
    }
    out
}

/// The test embedder's model.
pub(crate) fn embedder_model() -> crosstalk_spec::aggregates::topic::EmbeddingModel {
    crosstalk_spec::interfaces::l6_analysis::Embedder::model(&embedder())
}
