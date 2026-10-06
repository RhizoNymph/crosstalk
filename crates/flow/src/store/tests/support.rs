//! What the flow store tests share: a migrated test database, fresh state
//! per case, the stores over the reference agent directory, and the id
//! source the model harness compares declared ids through.

use std::sync::Arc;

use crosstalk_memory::flow::registry::model::directory;
use crosstalk_memory::model::Divergence;
use crosstalk_memory::reconstruct::MemoryAgents;
use crosstalk_memory::support::IdSequence;
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::ids::ChannelId;
use crosstalk_spec::support::Timestamp;
use crosstalk_store::{TestDb, TestDbError};
use sqlx::PgPool;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

use crate::store::outbox::ChannelSink;
use crate::store::{
    ChannelIdSource, FlowStoreError, IdSourceError, PgChannelRegistry, PgTransmissionStore, migrate,
};

/// Why a store test failed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Failure {
    #[error(transparent)]
    TestDb(#[from] TestDbError),
    #[error(transparent)]
    Store(#[from] FlowStoreError),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
    #[error("{0}")]
    Unexpected(String),
}

impl From<Divergence> for Failure {
    fn from(divergence: Divergence) -> Self {
        Failure::Unexpected(format!("step {}: {}", divergence.step, divergence.what))
    }
}

pub(crate) type TestResult = Result<(), Failure>;

/// Fail with `what` unless `condition` holds.
pub(crate) fn ensure(condition: bool, what: impl FnOnce() -> String) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(Failure::Unexpected(what()))
    }
}

/// Fail unless `got == want`.
pub(crate) fn same<T: PartialEq + std::fmt::Debug>(label: &str, got: &T, want: &T) -> TestResult {
    ensure(got == want, || {
        format!("{label}:\n  got:  {got:?}\n  want: {want:?}")
    })
}

/// A fresh, migrated test database, or `None` (and a printed reason) when
/// `TEST_DATABASE_URL` is unset.
pub(crate) async fn db(test: &str) -> Result<Option<TestDb>, Failure> {
    let Some(db) = TestDb::new_or_skip(test).await? else {
        return Ok(None);
    };
    migrate(&db.store()).await?;
    Ok(Some(db))
}

/// Empty every flow table, for the next model case.
pub(crate) async fn reset(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "TRUNCATE flow.channels, flow.resources, flow.accesses, flow.policy_decisions, \
         flow.channel_traffic, flow.transmissions, flow.verdicts, flow.outbox, flow.cursors, \
         flow.shard_ticks, flow.held_writes, flow.tool_calls, flow.checkpoints \
         RESTART IDENTITY CASCADE",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Declared channel ids from the reference's sequence: the pending id is
/// the sequence's next one, drawn only once a declaration commits, as the
/// reference draws it.
#[derive(Debug, Clone)]
pub(crate) struct SequenceIds(pub(crate) IdSequence);

impl ChannelIdSource for SequenceIds {
    fn pending(&self, _at: Timestamp) -> Result<ChannelId, IdSourceError> {
        let next = self.0.peek(1).first().copied().unwrap_or_default();
        Ok(ChannelId::from_ulid(next))
    }

    fn consume(&self, id: ChannelId) {
        if self.0.peek(1).first().copied() == Some(id.as_ulid()) {
            self.0.skip(1);
        }
    }
}

/// The reference agent directory (agent 3 merged into agent 2).
pub(crate) async fn agents() -> Result<MemoryAgents, Failure> {
    Ok(directory().await?)
}

/// A registry on `pool` over `agents`, with the events it relays.
pub(crate) async fn registry_over(
    pool: &PgPool,
    agents: MemoryAgents,
) -> Result<
    (
        PgChannelRegistry<MemoryAgents, ChannelSink>,
        UnboundedReceiver<BusEvent>,
    ),
    Failure,
> {
    let (sender, events) = unbounded_channel();
    let registry = PgChannelRegistry::open(
        pool.clone(),
        agents,
        Arc::new(SequenceIds(IdSequence::default())),
        ChannelSink::new(sender),
    )
    .await?;
    Ok((registry, events))
}

/// A registry on `pool` over the reference directory.
pub(crate) async fn registry(
    pool: &PgPool,
) -> Result<
    (
        PgChannelRegistry<MemoryAgents, ChannelSink>,
        UnboundedReceiver<BusEvent>,
    ),
    Failure,
> {
    registry_over(pool, agents().await?).await
}

/// A transmission store on `pool` over the reference directory.
pub(crate) async fn transmissions(
    pool: &PgPool,
) -> Result<
    (
        PgTransmissionStore<MemoryAgents, ChannelSink>,
        UnboundedReceiver<BusEvent>,
    ),
    Failure,
> {
    let (sender, events) = unbounded_channel();
    Ok((
        PgTransmissionStore::new(pool.clone(), agents().await?, ChannelSink::new(sender)),
        events,
    ))
}

/// Every event waiting on `receiver`.
pub(crate) fn drain(receiver: &mut UnboundedReceiver<BusEvent>) -> Vec<BusEvent> {
    let mut events = Vec::new();
    while let Ok(event) = receiver.try_recv() {
        events.push(event);
    }
    events
}

pub(crate) fn at(micros: u64) -> Timestamp {
    Timestamp::from_micros(micros)
}
