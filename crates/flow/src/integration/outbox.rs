//! The outbox relay under crashes (`flow.outbox.stable-envelope-id`,
//! INV-1212): a relay that fails or is dropped after stamping, after
//! publishing or before deleting leaves its rows stamped, and the next
//! relay publishes each staged event exactly once in a bus that holds an
//! envelope id once, under the id stamped before its first publish.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::{EventId, TransmissionId};
use crosstalk_spec::interfaces::l2_transport::BusError;
use crosstalk_spec::support::Timestamp;
use crosstalk_store::{TestDb, TestDbError};
use sqlx::PgPool;
use tokio::sync::Notify;

use crate::store::outbox::stage;
use crate::store::{EventSink, FlowStoreError, Relay, SinkError, Stamp, migrate};

#[derive(Debug, thiserror::Error)]
enum Failure {
    #[error(transparent)]
    TestDb(#[from] TestDbError),
    #[error(transparent)]
    Store(#[from] FlowStoreError),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error("{0}")]
    Unexpected(String),
}

fn ensure(condition: bool, what: impl FnOnce() -> String) -> Result<(), Failure> {
    if condition {
        Ok(())
    } else {
        Err(Failure::Unexpected(what()))
    }
}

/// What the sink does on its next publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum Mode {
    /// Publish.
    Healthy = 0,
    /// Refuse every publish as if the bus were unreachable.
    Fail = 1,
    /// Hang before publishing (the relay is dropped there).
    HangBefore = 2,
    /// Publish, then hang (the relay is dropped before its delete).
    HangAfter = 3,
}

/// A bus that holds each envelope id once, as `PgBus` does, behind a sink
/// whose faults the test sets.
#[derive(Debug, Default)]
struct Shared {
    mode: AtomicU8,
    stamps: AtomicU64,
    /// Every envelope the bus holds, by id.
    log: Mutex<BTreeMap<EventId, Envelope>>,
    /// How many publishes reached the bus (duplicates included).
    publishes: AtomicU64,
    hung: Notify,
}

#[derive(Debug, Clone)]
struct FaultySink(Arc<Shared>);

impl FaultySink {
    fn set(&self, mode: Mode) {
        self.0.mode.store(mode as u8, Ordering::SeqCst);
    }

    fn mode(&self) -> Mode {
        match self.0.mode.load(Ordering::SeqCst) {
            1 => Mode::Fail,
            2 => Mode::HangBefore,
            3 => Mode::HangAfter,
            _ => Mode::Healthy,
        }
    }

    fn log(&self) -> BTreeMap<EventId, Envelope> {
        self.0
            .log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    async fn hang(&self) {
        self.0.hung.notify_one();
        std::future::pending::<()>().await;
    }
}

impl EventSink for FaultySink {
    fn stamp(&self) -> Result<Stamp, SinkError> {
        let n = self.0.stamps.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(Stamp {
            id: EventId::from_ulid((1 << 80) + u128::from(n)),
            at: Timestamp::from_micros(1_000 * n),
        })
    }

    async fn publish(&self, envelope: Envelope) -> Result<(), SinkError> {
        match self.mode() {
            Mode::Fail => return Err(SinkError::Bus(BusError::Disconnected)),
            Mode::HangBefore => self.hang().await,
            Mode::Healthy | Mode::HangAfter => {}
        }
        self.0.publishes.fetch_add(1, Ordering::SeqCst);
        self.0
            .log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(envelope.id)
            .or_insert(envelope);
        if self.mode() == Mode::HangAfter {
            self.hang().await;
        }
        Ok(())
    }
}

fn event(n: u128) -> BusEvent {
    BusEvent::Changed(Changed::Verdict(TransmissionId::from_ulid(n)))
}

async fn stage_committed(pool: &PgPool, events: &[BusEvent]) -> Result<(), Failure> {
    let mut tx = pool.begin().await?;
    stage(&mut tx, events)
        .await
        .map_err(|fault| Failure::Store(fault.into_error()))?;
    tx.commit().await?;
    Ok(())
}

/// Every outbox row's stamp, by seq.
async fn stamps(pool: &PgPool) -> Result<Vec<(i64, Option<String>)>, Failure> {
    Ok(
        sqlx::query_as("SELECT seq, envelope_id FROM flow.outbox ORDER BY seq")
            .fetch_all(pool)
            .await?,
    )
}

/// Run `relay` until the sink hangs, then drop it: a crash mid-relay.
async fn crash(relay: &Relay<FaultySink>, sink: &FaultySink) -> Result<(), Failure> {
    tokio::select! {
        result = relay.relay() => Err(Failure::Unexpected(format!("the relay finished instead of hanging: {result:?}"))),
        () = sink.0.hung.notified() => Ok(()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outbox_relay_publishes_each_event_once_under_its_stamped_id() -> Result<(), Failure> {
    let Some(db) =
        TestDb::new_or_skip("outbox_relay_publishes_each_event_once_under_its_stamped_id").await?
    else {
        return Ok(());
    };
    migrate(&db.store()).await?;
    let pool = db.pool().clone();
    let sink = FaultySink(Arc::new(Shared::default()));
    let relay = Relay::new(pool.clone(), sink.clone());

    // Staged but not committed: nothing is published.
    let mut open = pool.begin().await?;
    stage(&mut open, &[event(1)])
        .await
        .map_err(|fault| Failure::Store(fault.into_error()))?;
    ensure(relay.relay().await? == 0, || {
        "an uncommitted row was relayed".into()
    })?;
    open.commit().await?;

    // 1. A crash after the stamp, before any publish.
    sink.set(Mode::HangBefore);
    crash(&relay, &sink).await?;
    let first = stamps(&pool).await?;
    ensure(first.len() == 1 && first[0].1.is_some(), || {
        format!("the stamp did not commit before the publish: {first:?}")
    })?;
    ensure(sink.log().is_empty(), || {
        "published before the crash".into()
    })?;

    // 2. A publish failure: the row stays, under the same stamp.
    stage_committed(&pool, &[event(2), event(3)]).await?;
    sink.set(Mode::Fail);
    ensure(relay.relay().await.is_err(), || {
        "a failed publish was not reported".into()
    })?;
    let second = stamps(&pool).await?;
    ensure(
        second.len() == 3 && second.iter().all(|(_, id)| id.is_some()) && second[0] == first[0],
        || format!("stamps changed or are missing after a failed publish: {second:?}"),
    )?;

    // 3. A crash after publishing, before the delete.
    sink.set(Mode::HangAfter);
    crash(&relay, &sink).await?;
    ensure(stamps(&pool).await? == second, || {
        "rows changed by a relay that crashed before its delete".into()
    })?;

    // A new relay, healthy, finishes the job.
    sink.set(Mode::Healthy);
    let recovered = Relay::new(pool.clone(), sink.clone());
    recovered.relay().await?;
    ensure(stamps(&pool).await?.is_empty(), || {
        "rows left after a healthy relay".into()
    })?;

    // Each event once, under the id stamped before its first publish.
    let log = sink.log();
    let want: Vec<(EventId, BusEvent)> = second
        .iter()
        .zip([event(1), event(2), event(3)])
        .map(|((seq, id), event)| {
            let id = id
                .as_deref()
                .and_then(|text| EventId::from_ulid_text(text).ok())
                .ok_or_else(|| Failure::Unexpected(format!("row {seq} has no valid stamp")))?;
            Ok((id, event))
        })
        .collect::<Result<_, Failure>>()?;
    let got: Vec<(EventId, BusEvent)> = log
        .values()
        .map(|envelope| (envelope.id, envelope.event.clone()))
        .collect();
    ensure(got == want, || {
        format!("the bus holds\n  {got:?}\nwant\n  {want:?}")
    })?;
    ensure(sink.0.stamps.load(Ordering::SeqCst) == 3, || {
        "a row was stamped more than once".into()
    })?;
    ensure(sink.0.publishes.load(Ordering::SeqCst) > 3, || {
        "the crashes did not republish anything; the test proves nothing".into()
    })?;
    Ok(())
}
