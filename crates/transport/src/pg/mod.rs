//! [`PgBus`]: the durable single-node event bus, over Postgres (schema
//! `transport`, `migrations/0001_bus.sql`).
//!
//! - `transport.events` is the log: every envelope `publish` returned `Ok`
//!   for (`transport.durability.pg-publish-persisted`), once per envelope
//!   id (`transport.publish.idempotent-on-id`).
//! - `transport.groups` holds each consumer group's subject set, retry
//!   policy and `admitted_through`, the last log entry it has looked at.
//! - `transport.deliveries` holds each group's admitted, unacked entries:
//!   `ready`, `held` by a consumer, or `delayed` until a bus-clock time.
//!   An ack deletes the row; a nack or ack timeout delays it, or on the
//!   last attempt moves it to `transport.dead_letters` in the same
//!   transaction.
//!
//! The group semantics are `MpscBus`'s (the conformance suite runs over
//! both): every group gets each envelope published under one of its
//! subjects after its first subscribe, its subscriptions share them, and
//! deliveries carry their attempt. What differs: the log is the queue, so
//! `publish` never waits for room (only `MpscBus` has backpressure);
//! groups, deliveries and dead letters survive the process; and a delivery
//! held by a process that stopped is returned by the next process's
//! [`PgBus::recover_held`] (`transport.restart.held-redelivered`), which
//! is safe because one pipeline process holds the database (the gateway's
//! pipeline lock).
//!
//! Time: delays and due times are read from the injected [`Clock`], never
//! from `now()` in SQL; ack deadlines are tokio `Instant`s in process
//! memory, owned by the reaper task. Waiting `next` calls wake on local
//! publishes and acks, on `NOTIFY transport_events` (the listener task),
//! and at least every poll interval.
//!
//! Transactions are `READ COMMITTED` with explicit locks (the group row
//! `FOR UPDATE`, ready rows `FOR UPDATE SKIP LOCKED`, and a transaction
//! advisory lock around log appends), the usual queue-table pattern, not
//! the `SERIALIZABLE` retries the stores use.

mod config;
mod dead_letters;
mod listen;
mod prune;
mod publish;
mod reaper;
mod row;
mod stats;
mod subscription;

use std::hash::RandomState;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use crosstalk_spec::events::{Envelope, Subject};
use crosstalk_spec::interfaces::l2_transport::{BusError, ConsumerGroup, EventBus, RetryPolicy};
use crosstalk_spec::support::{Clock, Timestamp};
use crosstalk_store::{Layer, Migrations, StoreError};
use sqlx::PgPool;
use sqlx::migrate::Migrator;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

pub use config::{InvalidPgBusConfig, PgBusConfig};
pub use dead_letters::PgDeadLetters;
pub use stats::GroupStats;
pub use subscription::PgSubscription;

use self::reaper::{RESTART_REASON, ReaperMsg};
use self::row::bus_error;
use crate::StartError;
use crate::spool::DrainTarget;

/// The transport layer's migrations, embedded.
pub static MIGRATIONS: Migrator = sqlx::migrate!("./migrations");

/// Run the transport layer's migrations in its own schema.
pub async fn migrate(pool: &PgPool) -> Result<(), StoreError> {
    crosstalk_store::migrate(pool, Layer::Transport, Migrations::Embedded(&MIGRATIONS)).await
}

/// What every handle, subscription and task of one bus shares.
pub(crate) struct Shared {
    pub(crate) pool: PgPool,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) config: PgBusConfig,
    /// Bumped whenever a waiting `next` might find something new.
    pub(crate) wake: watch::Sender<u64>,
    /// `true` once the bus is shut down or every bus handle is dropped.
    pub(crate) closed: watch::Sender<bool>,
    pub(crate) reaper: mpsc::UnboundedSender<ReaperMsg>,
    pub(crate) next_delivery: AtomicU64,
    /// Keys dead-letter cursor checks; per bus value.
    pub(crate) cursor_key: RandomState,
}

impl std::fmt::Debug for Shared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shared")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl Shared {
    pub(crate) fn wake_all(&self) {
        self.wake.send_modify(|n| *n = n.wrapping_add(1));
    }

    /// The bus clock's reading, in the tables' microseconds.
    pub(crate) fn now_micros(&self) -> i64 {
        i64::try_from(self.clock.now().as_micros()).unwrap_or(i64::MAX)
    }
}

/// The background tasks; aborted when the last [`PgBus`] handle goes.
#[derive(Debug)]
struct Tasks {
    shared: Arc<Shared>,
    handles: Vec<JoinHandle<()>>,
}

impl Tasks {
    fn stop(&self) {
        self.shared.closed.send_replace(true);
        for handle in &self.handles {
            handle.abort();
        }
    }
}

impl Drop for Tasks {
    fn drop(&mut self) {
        self.stop();
    }
}

/// How many deliveries [`PgBus::recover_held`] returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Recovered {
    /// Held deliveries made delayed, their attempt counted.
    pub redelivered: u64,
    /// Held deliveries on their last attempt, dead-lettered.
    pub dead_lettered: u64,
}

/// The durable [`EventBus`]: see the [module docs](self).
///
/// Cloning gives another handle on the same bus. The background tasks (the
/// `LISTEN` task and the reaper) stop when [`PgBus::shutdown`] is called or
/// every `PgBus` handle is dropped; subscriptions then see `None`. Nothing
/// in the database is touched by either: a dropped bus is a stopped
/// process, and the next one picks up where it left off.
#[derive(Debug, Clone)]
pub struct PgBus {
    shared: Arc<Shared>,
    tasks: Arc<Tasks>,
}

impl PgBus {
    /// A bus over `pool` (migrated with [`migrate`]), reading delays from
    /// `clock`. Touches no database: it only starts the background tasks,
    /// so it can be built while the database is down. Call
    /// [`PgBus::recover_held`] once the database answers, before any
    /// consumer subscribes.
    pub fn new(
        pool: PgPool,
        clock: Arc<dyn Clock>,
        config: PgBusConfig,
    ) -> Result<Self, StartError> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| StartError::NoRuntime)?;
        let (reaper_tx, reaper_rx) = mpsc::unbounded_channel();
        let shared = Arc::new(Shared {
            pool,
            clock,
            config,
            wake: watch::Sender::new(0),
            closed: watch::Sender::new(false),
            reaper: reaper_tx,
            next_delivery: AtomicU64::new(0),
            cursor_key: RandomState::new(),
        });
        let handles = vec![
            runtime.spawn(listen::run(Arc::clone(&shared))),
            runtime.spawn(reaper::run(Arc::clone(&shared), reaper_rx)),
        ];
        tracing::info!(
            group_capacity = config.group_capacity.get(),
            ack_timeout_ms =
                u64::try_from(config.ack_timeout.get().as_millis()).unwrap_or(u64::MAX),
            "postgres bus started"
        );
        let tasks = Arc::new(Tasks {
            shared: Arc::clone(&shared),
            handles,
        });
        Ok(Self { shared, tasks })
    }

    /// Return every delivery a stopped process left `held`: each becomes
    /// `delayed` by its group's backoff with the attempt counted, or, on
    /// its group's last attempt, a dead letter. Run at start, before any
    /// subscription of this process exists; it assumes no other process
    /// holds deliveries (the pipeline lock).
    pub async fn recover_held(&self) -> Result<Recovered, BusError> {
        let shared = &self.shared;
        let now = shared.now_micros();
        let result: Result<Recovered, sqlx::Error> = async {
            let mut tx = shared.pool.begin().await?;
            let dead = sqlx::query(
                "WITH exhausted AS ( \
                     DELETE FROM transport.deliveries d \
                     USING transport.groups g, transport.events e \
                     WHERE d.state = 'held' AND g.name = d.group_name \
                       AND d.attempt >= g.max_attempts AND e.seq = d.seq \
                     RETURNING d.group_name, e.id, d.seq, d.at, e.envelope, d.attempt) \
                 INSERT INTO transport.dead_letters \
                     (group_name, event_id, seq, at, envelope, attempts, last_error) \
                 SELECT group_name, id, seq, at, envelope, greatest(attempt, 1), $1 FROM exhausted \
                 ON CONFLICT (group_name, event_id) DO UPDATE SET seq = excluded.seq, \
                 at = excluded.at, envelope = excluded.envelope, attempts = excluded.attempts, \
                 last_error = excluded.last_error",
            )
            .bind(RESTART_REASON)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            let redelivered = sqlx::query(
                "UPDATE transport.deliveries d SET state = 'delayed', last_error = $1, \
                 available_at = $2 + least(g.max_backoff_micros::numeric, \
                     g.initial_backoff_micros::numeric \
                     * (2::numeric ^ least(greatest(d.attempt - 1, 0), 31)))::bigint \
                 FROM transport.groups g \
                 WHERE d.state = 'held' AND g.name = d.group_name",
            )
            .bind(RESTART_REASON)
            .bind(now)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            tx.commit().await?;
            Ok(Recovered {
                redelivered,
                dead_lettered: dead,
            })
        }
        .await;
        let recovered = result.map_err(|e| bus_error("recover held deliveries", &e))?;
        tracing::info!(
            redelivered = recovered.redelivered,
            dead_lettered = recovered.dead_lettered,
            "held deliveries recovered"
        );
        shared.wake_all();
        Ok(recovered)
    }

    /// The bus's dead-letter store.
    pub fn dead_letters(&self) -> PgDeadLetters {
        PgDeadLetters {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Every group's pending deliveries and dead letters, for the frontier
    /// and `/healthz`.
    pub async fn group_stats(&self) -> Result<Vec<GroupStats>, BusError> {
        stats::group_stats(&self.shared).await
    }

    /// Delete log entries every group is done with whose `at` is more than
    /// `keep` before `now` (the gateway's retention tick passes
    /// [`PgBusConfig::retention`]). Returns how many were deleted.
    pub async fn prune(&self, now: Timestamp, keep: Duration) -> Result<u64, BusError> {
        prune::prune(&self.shared, now, keep).await
    }

    /// The config the bus runs with.
    pub fn config(&self) -> &PgBusConfig {
        &self.shared.config
    }

    /// Stop the background tasks. Waiting `next` calls return `None`. The
    /// database is untouched: held deliveries stay held until a restart's
    /// [`PgBus::recover_held`].
    pub fn shutdown(&self) {
        self.tasks.stop();
        tracing::info!("postgres bus stopped");
    }
}

impl EventBus for PgBus {
    type Subscription = PgSubscription;

    /// Append the envelope to the log in its own transaction, idempotent on
    /// its id. Never waits for group room; bounded by `publish_timeout`.
    async fn publish(&self, envelope: Envelope) -> Result<(), BusError> {
        publish::publish(&self.shared, std::slice::from_ref(&envelope)).await
    }

    async fn subscribe(
        &self,
        subjects: &[Subject],
        group: ConsumerGroup,
        retry: RetryPolicy,
    ) -> Result<PgSubscription, BusError> {
        subscription::subscribe(&self.shared, subjects, group, retry).await
    }
}

impl DrainTarget for PgBus {
    async fn probe(&self) -> Result<(), BusError> {
        publish::probe(&self.shared).await
    }

    /// One transaction for the whole batch, in order, idempotent on ids.
    async fn publish_batch(&self, envelopes: Vec<Envelope>) -> Result<(), BusError> {
        publish::publish(&self.shared, &envelopes).await
    }
}
