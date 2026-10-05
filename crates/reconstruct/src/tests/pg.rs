//! Postgres test support: a migrated test database, sinks that record
//! what the store publishes, and merge ids from the reference harness's
//! sequence.
//!
//! Every database test is gated on `TEST_DATABASE_URL`
//! (`crosstalk_store::TestDb::new_or_skip`): without it the test prints
//! its skip line and passes.

use std::sync::{Arc, Mutex, PoisonError};

use crosstalk_memory::support::{IdSequence, Outbox};
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::ids::MergeId;
use crosstalk_spec::ids::mint::UlidExhausted;
use crosstalk_spec::support::Timestamp;
use crosstalk_store::TestDb;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::{Semaphore, SemaphorePermit};

use crate::agents::{PgAgents, run_migrations};
use crate::ids::IdSource;
use crate::publish::{EventSink, SinkError};

/// The cursor key every test store uses.
pub(crate) const CURSOR_KEY: [u8; 32] = [7; 32];

/// Publishes into the reference harness's outbox.
pub(crate) struct OutboxSink(pub(crate) Outbox);

impl EventSink for OutboxSink {
    async fn publish(&self, events: Vec<BusEvent>) -> Result<(), SinkError> {
        self.0.publish(events);
        Ok(())
    }
}

/// Records every published event, in order.
#[derive(Debug, Clone, Default)]
pub(crate) struct Recorder(pub(crate) Arc<Mutex<Vec<BusEvent>>>);

impl Recorder {
    /// Everything published since the last call.
    pub(crate) fn take(&self) -> Vec<BusEvent> {
        std::mem::take(&mut *self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

impl EventSink for Recorder {
    async fn publish(&self, events: Vec<BusEvent>) -> Result<(), SinkError> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(events);
        Ok(())
    }
}

/// Merge ids from the reference harness's sequence.
pub(crate) struct SeqIds(pub(crate) IdSequence);

impl IdSource<MergeId> for SeqIds {
    fn next_id(&self, _at: Timestamp) -> Result<MergeId, UlidExhausted> {
        Ok(MergeId::from_ulid(self.0.next_ulid()))
    }
}

/// The store the tests use.
pub(crate) type TestAgents = PgAgents<Recorder, SeqIds>;

/// How many of this binary's database tests run at once: the test server
/// is reached through a tunnel that drops connection bursts, and its
/// connection budget is shared by every test run.
static DATABASES: Semaphore = Semaphore::const_new(3);

/// A test database, holding one of the [`DATABASES`] permits while it
/// lives.
pub(crate) struct Db {
    db: TestDb,
    _permit: SemaphorePermit<'static>,
}

impl std::ops::Deref for Db {
    type Target = TestDb;

    fn deref(&self) -> &TestDb {
        &self.db
    }
}

/// A fresh, migrated test database, or `None` (skipped) without
/// `TEST_DATABASE_URL`.
pub(crate) async fn database(test: &str) -> Option<Db> {
    let permit = match DATABASES.acquire().await {
        Ok(permit) => permit,
        Err(error) => panic!("database gate closed: {error}"),
    };
    let db = match TestDb::new_or_skip(test).await {
        Ok(db) => db?,
        Err(error) => panic!("test database unavailable (is the tunnel up?): {error}"),
    };
    if let Err(error) = run_migrations(db.pool()).await {
        panic!("L3 migrations failed: {error}");
    }
    Some(Db {
        db,
        _permit: permit,
    })
}

/// A store over `db`'s pool and the recorder it publishes to.
pub(crate) async fn agents(db: &Db) -> (TestAgents, Recorder) {
    let recorder = Recorder::default();
    match PgAgents::open(
        db.pool().clone(),
        recorder.clone(),
        SeqIds(IdSequence::default()),
        CURSOR_KEY,
    )
    .await
    {
        Ok(store) => (store, recorder),
        Err(error) => panic!("store did not open: {error}"),
    }
}

/// A small pool on `db`'s database, for a store that must live on another
/// runtime than the one the database was created on.
pub(crate) async fn pool_on(db_url: &crosstalk_store::DatabaseUrl) -> PgPool {
    match PgPoolOptions::new()
        .max_connections(2)
        .connect_with(db_url.connect_options().clone())
        .await
    {
        Ok(pool) => pool,
        Err(error) => panic!("test pool did not connect: {error}"),
    }
}

/// Empty every L3 table.
pub(crate) async fn truncate(pool: &PgPool) {
    let emptied = sqlx::query(
        "TRUNCATE reconstruct.agents, reconstruct.agent_evidence, reconstruct.merges, \
         reconstruct.vetoes, reconstruct.claims, reconstruct.activity, reconstruct.outbox, \
         reconstruct.conversations, reconstruct.conversation_entries, \
         reconstruct.thread_records, reconstruct.responses CASCADE",
    )
    .execute(pool)
    .await;
    if let Err(error) = emptied {
        panic!("tables not emptied: {error}");
    }
}

/// Close `db`, dropping its database.
pub(crate) async fn close(db: Db) {
    if let Err(error) = db.db.close().await {
        panic!("test database not dropped: {error}");
    }
}
