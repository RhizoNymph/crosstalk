//! The agent store's outbox relay on Postgres
//! (`reconstruct.outbox.stable-envelope-id`, INV-1211), and the agents
//! list's cursor key derived from the deployment secret.
//!
//! The relay's crash points are modelled by a sink that fails before
//! publishing, fails after publishing (the bus holds the envelope; the
//! relay never learns it and never deletes the row), or never returns
//! (the relay future is dropped, as a killed process drops it). A store
//! opened again over the same database then relays what is left. The sink
//! keeps the bus's log the way `PgBus` does, idempotent on envelope ids.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use crosstalk_memory::reconstruct::model::evidence;
use crosstalk_memory::support::IdSequence;
use crosstalk_spec::aggregates::agents::filter::AgentFilter;
use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::{
    AgentId, DeploymentSecret, EventId, KeyedHasher, OperatorId, SecretVersion,
};
use crosstalk_spec::interfaces::l2_transport::BusError;
use crosstalk_spec::interfaces::l3_reconstruction::agents::{AgentReadError, AgentReads};
use crosstalk_spec::interfaces::l3_reconstruction::lifecycle::{
    AgentLifecycle, AgentOrigin, NewAgent,
};
use crosstalk_spec::paging::{PageRequest, PageSize};
use crosstalk_spec::support::{NonEmpty, Timestamp};
use crosstalk_testkit::ids::Ids;
use sqlx::PgPool;

use super::pg::{Recorder, SeqIds, close, database, test_stamp};
use crate::agents::PgAgents;
use crate::publish::{EventSink, SinkError, Stamp};

/// Where the faulty sink fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    /// Every publish lands.
    None,
    /// The publish fails before the bus holds the envelope.
    BeforePublish,
    /// The bus holds the envelope, but the relay sees a failure.
    AfterPublish,
    /// The publish never returns.
    Hang,
}

#[derive(Debug)]
struct BusLog {
    fault: Fault,
    /// Every envelope the bus was handed, duplicates included.
    attempts: Vec<Envelope>,
    /// The bus's log: one event per envelope id, the first one published.
    log: BTreeMap<EventId, BusEvent>,
}

/// A sink over a log idempotent on envelope ids, failing as told.
#[derive(Debug, Clone)]
struct FaultySink(Arc<Mutex<BusLog>>);

impl FaultySink {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(BusLog {
            fault: Fault::None,
            attempts: Vec::new(),
            log: BTreeMap::new(),
        })))
    }

    fn with<T>(&self, f: impl FnOnce(&mut BusLog) -> T) -> T {
        f(&mut self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }

    fn fail(&self, fault: Fault) {
        self.with(|state| state.fault = fault);
    }

    /// The envelope lands in the log, unless its id is already there.
    fn land(&self, envelope: Envelope) {
        self.with(|state| {
            state.attempts.push(envelope.clone());
            state.log.entry(envelope.id).or_insert(envelope.event);
        });
    }
}

impl EventSink for FaultySink {
    fn stamp(&self) -> Result<Stamp, SinkError> {
        Ok(test_stamp())
    }

    async fn publish(&self, envelope: Envelope) -> Result<(), SinkError> {
        let fault = self.with(|state| state.fault);
        match fault {
            Fault::None => {
                self.land(envelope);
                Ok(())
            }
            Fault::BeforePublish => Err(SinkError::Bus(BusError::Disconnected)),
            Fault::AfterPublish => {
                self.land(envelope);
                Err(SinkError::Bus(BusError::Disconnected))
            }
            Fault::Hang => std::future::pending().await,
        }
    }
}

type FaultyAgents = PgAgents<FaultySink, SeqIds>;

async fn open(pool: &PgPool, sink: &FaultySink) -> FaultyAgents {
    match PgAgents::open(
        pool.clone(),
        sink.clone(),
        SeqIds(IdSequence::default()),
        [7; 32],
    )
    .await
    {
        Ok(store) => store,
        Err(error) => panic!("store did not open: {error}"),
    }
}

fn new_agent(id: AgentId, item: u8) -> NewAgent {
    NewAgent {
        id,
        evidence: NonEmpty::new(evidence(item)),
        parent: None,
        origin: AgentOrigin::Traffic {
            first_seen: Timestamp::from_micros(1_000_000),
        },
        label: None,
    }
}

/// Every outbox row: its sequence, envelope id and event.
async fn outbox_rows(pool: &PgPool) -> Vec<(i64, Option<String>, String)> {
    match sqlx::query_as("SELECT seq, envelope_id, event FROM reconstruct.outbox ORDER BY seq")
        .fetch_all(pool)
        .await
    {
        Ok(rows) => rows,
        Err(error) => panic!("outbox not read: {error}"),
    }
}

fn event_id(text: &str) -> EventId {
    match crate::agents::codec::id_of("outbox.envelope_id", text) {
        Ok(id) => id,
        Err(error) => panic!("stored envelope id: {error}"),
    }
}

/// `reconstruct.outbox.stable-envelope-id`: writes whose relay fails before
/// the publish, after it, or is dropped mid-publish leave their rows
/// stamped; relaying again (after another failure, then from a store
/// opened anew) publishes each staged event under the id it was stamped
/// with, so the bus's log holds each event exactly once.
#[tokio::test(flavor = "multi_thread")]
async fn outbox_relay_publishes_each_event_once_under_its_stamped_id() {
    let Some(db) = database("outbox_relay_publishes_each_event_once_under_its_stamped_id").await
    else {
        return;
    };
    let pool = db.pool().clone();
    let sink = FaultySink::new();
    let mut store = open(&pool, &sink).await;
    let mut ids = Ids::new();

    sink.fail(Fault::BeforePublish);
    assert_eq!(store.create(new_agent(ids.agent(), 1)).await, Ok(()));
    sink.fail(Fault::AfterPublish);
    assert_eq!(store.create(new_agent(ids.agent(), 2)).await, Ok(()));
    sink.fail(Fault::Hang);
    let dropped = tokio::time::timeout(
        Duration::from_millis(500),
        store.create(new_agent(ids.agent(), 3)),
    )
    .await;
    assert!(dropped.is_err(), "the hanging relay returned: {dropped:?}");

    // Every write committed and every row was stamped before any publish.
    let staged = outbox_rows(&pool).await;
    assert!(staged.len() >= 3, "staged rows: {staged:?}");
    let stamped: BTreeMap<EventId, BusEvent> = staged
        .iter()
        .map(|(seq, id, event)| {
            let Some(id) = id else {
                panic!("row {seq} published unstamped");
            };
            let event = serde_json::from_str(event).unwrap_or_else(|e| panic!("event: {e}"));
            (event_id(id), event)
        })
        .collect();
    assert_eq!(stamped.len(), staged.len(), "two rows share an envelope id");

    // A relay that fails after publishing keeps the stamps.
    sink.fail(Fault::AfterPublish);
    assert!(store.flush_outbox().await.is_err());
    assert_eq!(outbox_rows(&pool).await, staged);

    // A store opened anew (the restart) relays what is left.
    sink.fail(Fault::None);
    let reopened = open(&pool, &sink).await;
    assert_eq!(reopened.flush_outbox().await.ok(), Some(staged.len()));
    assert_eq!(outbox_rows(&pool).await, Vec::new());
    assert_eq!(reopened.flush_outbox().await.ok(), Some(0));

    sink.with(|state| {
        assert_eq!(state.log, stamped, "the log is not the stamped events");
        for attempt in &state.attempts {
            assert_eq!(
                stamped.get(&attempt.id),
                Some(&attempt.event),
                "{} published under an id it was not stamped with",
                attempt.id.ulid_text()
            );
        }
    });
    close(db).await;
}

/// `reconstruct.outbox.stable-envelope-id`: a row staged in a transaction
/// that has not committed is neither stamped nor published; once it
/// commits, the relay stamps and publishes it.
#[tokio::test(flavor = "multi_thread")]
async fn outbox_relay_never_publishes_an_uncommitted_row() {
    let Some(db) = database("outbox_relay_never_publishes_an_uncommitted_row").await else {
        return;
    };
    let pool = db.pool().clone();
    let sink = FaultySink::new();
    let store = open(&pool, &sink).await;
    let event = BusEvent::Ingest(IngestEvent::AgentRenamed {
        agent: Ids::new().agent(),
        label: crosstalk_memory::reconstruct::model::label(0),
        by: OperatorId::from_ulid(1),
    });
    let mut staging = pool.begin().await.expect("transaction");
    sqlx::query("INSERT INTO reconstruct.outbox (event) VALUES ($1)")
        .bind(serde_json::to_string(&event).expect("encodes"))
        .execute(&mut *staging)
        .await
        .expect("staged");
    assert_eq!(store.flush_outbox().await.ok(), Some(0));
    assert!(sink.with(|state| state.attempts.is_empty()));
    staging.commit().await.expect("committed");
    assert_eq!(store.flush_outbox().await.ok(), Some(1));
    sink.with(|state| {
        assert_eq!(state.log.values().cloned().collect::<Vec<_>>(), vec![event]);
    });
    close(db).await;
}

/// A store's own relay publishes each committed write's events under
/// increasing stamps, in staging order, and leaves the outbox empty.
#[tokio::test(flavor = "multi_thread")]
async fn relayed_envelopes_carry_increasing_stamps() {
    let Some(db) = database("relayed_envelopes_carry_increasing_stamps").await else {
        return;
    };
    let recorder = Recorder::default();
    let mut store = match PgAgents::open(
        db.pool().clone(),
        recorder.clone(),
        SeqIds(IdSequence::default()),
        [7; 32],
    )
    .await
    {
        Ok(store) => store,
        Err(error) => panic!("store did not open: {error}"),
    };
    let mut ids = Ids::new();
    for item in 0..3 {
        assert_eq!(store.create(new_agent(ids.agent(), item)).await, Ok(()));
    }
    let envelopes = recorder.take_envelopes();
    assert!(!envelopes.is_empty());
    assert!(
        envelopes.windows(2).all(|pair| pair[0].id < pair[1].id),
        "stamps not increasing"
    );
    assert_eq!(outbox_rows(db.pool()).await, Vec::new());
    close(db).await;
}

fn secret(byte: u8) -> KeyedHasher {
    KeyedHasher::new(DeploymentSecret::new(SecretVersion(1), [byte; 32]))
}

async fn keyed(pool: &PgPool, secret: &KeyedHasher) -> PgAgents<Recorder, SeqIds> {
    match PgAgents::open_with_secret(
        pool.clone(),
        Recorder::default(),
        SeqIds(IdSequence::default()),
        secret,
    )
    .await
    {
        Ok(store) => store,
        Err(error) => panic!("store did not open: {error}"),
    }
}

/// `surface.cursor.survives-restart` for `PgAgents`: an agents-list cursor
/// issued by a store keyed from the deployment secret resolves to the same
/// page on a store opened again with that secret, and is refused under
/// another secret.
#[tokio::test(flavor = "multi_thread")]
async fn agents_cursor_keyed_from_the_secret_resolves_after_a_restart() {
    let Some(db) = database("agents_cursor_keyed_from_the_secret_resolves_after_a_restart").await
    else {
        return;
    };
    let pool = db.pool().clone();
    let mut before = keyed(&pool, &secret(1)).await;
    let mut ids = Ids::new();
    for item in 0..3 {
        assert_eq!(before.create(new_agent(ids.agent(), item)).await, Ok(()));
    }
    let size = PageSize::new(1).expect("page size");
    let every = AgentFilter::default();
    let first = before
        .list(&every, &PageRequest { size, after: None })
        .await
        .expect("first page");
    let cursor = first.next().cloned().expect("a next cursor");
    let next = PageRequest {
        size,
        after: Some(cursor),
    };
    let expected = before.list(&every, &next).await.expect("second page");

    let after = keyed(&pool, &secret(1)).await;
    assert_eq!(after.list(&every, &next).await, Ok(expected));
    let rotated = keyed(&pool, &secret(2)).await;
    assert_eq!(
        rotated.list(&every, &next).await,
        Err(AgentReadError::InvalidCursor)
    );
    close(db).await;
}
