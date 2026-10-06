//! The outbox relay's crash points (`topology.outbox.stable-envelope-id`),
//! against a real Postgres.
//!
//! A fault bus stops the relay at a seeded point: after the stamp and
//! before any publish, after some publishes, or after every publish and
//! before the delete. A "crash" drops the drain future and closes its pool
//! (every connection, so every lock and open transaction, goes away). A new
//! relay, with other entropy and a later clock, then drains what is left.
//! Over both runs, the bus log (deduplicated by id, as `PgBus` keeps it)
//! must hold each staged event exactly once, under the id stamped on its
//! row before its first publish.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use crosstalk_memory::model::build::ts;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::EventId;
use crosstalk_spec::interfaces::l2_transport::BusError;
use crosstalk_spec::interfaces::l7_topology::EdgeStore;
use crosstalk_spec::support::Watermark;
use crosstalk_store::TestDb;
use sqlx::{PgPool, Row};
use tokio::sync::mpsc;

use crate::codec;
use crate::integration::outbox_ids;
use crate::outbox::{Announce, drain, enqueue, publish_batch, stamp_batch};
use crate::tests::count;
use crate::tests::support::{contribution, database, frontier, reset, small_pool, world};

/// Where the fault bus stops the relay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    /// Never.
    None,
    /// The publish call numbered `call` (from 0) never returns; with
    /// `recorded`, its envelope reached the log first.
    Hang { call: usize, recorded: bool },
    /// The publish call numbered `call` fails, and nothing reaches the log.
    Fail { call: usize },
}

/// A bus whose log keeps every envelope it accepted, in order.
struct FaultBus {
    fault: Fault,
    calls: AtomicUsize,
    log: Mutex<Vec<Envelope>>,
    hung: mpsc::UnboundedSender<()>,
}

impl FaultBus {
    fn new(fault: Fault) -> (Self, mpsc::UnboundedReceiver<()>) {
        let (hung, signal) = mpsc::unbounded_channel();
        let bus = Self {
            fault,
            calls: AtomicUsize::new(0),
            log: Mutex::new(Vec::new()),
            hung,
        };
        (bus, signal)
    }

    fn log(&self) -> Vec<Envelope> {
        self.log.lock().expect("not poisoned").clone()
    }
}

impl Announce for FaultBus {
    async fn announce(&self, envelope: Envelope) -> Result<(), BusError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        match self.fault {
            Fault::Fail { call: failing } if failing == call => Err(BusError::Disconnected),
            Fault::Hang {
                call: hanging,
                recorded,
            } if hanging == call => {
                if recorded {
                    self.log.lock().expect("not poisoned").push(envelope);
                }
                let _ = self.hung.send(());
                std::future::pending().await
            }
            Fault::None | Fault::Hang { .. } | Fault::Fail { .. } => {
                self.log.lock().expect("not poisoned").push(envelope);
                Ok(())
            }
        }
    }
}

/// The eight events the staging below decides, in commit order.
fn staged_events() -> Vec<BusEvent> {
    [20, 40, 60, 80]
        .into_iter()
        .flat_map(|at| {
            let watermark = Watermark(ts(at));
            [
                BusEvent::Insight(InsightEvent::WatermarkAdvanced(watermark)),
                BusEvent::Changed(Changed::Watermark(watermark)),
            ]
        })
        .collect()
}

/// Stage three traffic rows (three applies, each into its own bucket) and
/// then four watermark advances (two events each): eleven rows.
async fn stage(pool: &PgPool) {
    let mut world = world(pool.clone());
    for (n, at) in [(1, 5), (2, 15), (3, 25)] {
        let one = contribution(
            n,
            1,
            2,
            crosstalk_spec::derived::flow::transmission::Route::Unobserved,
            at,
            1,
        );
        world.store.apply(&one).await.expect("applied");
    }
    for ticked in [40, 60, 80, 100] {
        world
            .store
            .advance_watermark(frontier(ticked))
            .await
            .expect("advanced");
    }
    assert_eq!(count(pool, "outbox").await, 11);
}

/// Every stamped row's (seq, envelope id), in seq order, and how many rows
/// are unstamped.
async fn stamps(pool: &PgPool) -> (Vec<(i64, EventId)>, i64) {
    let (stamped, unstamped) = stamped_rows(pool).await;
    let stamped = stamped.into_iter().map(|(seq, id, _)| (seq, id)).collect();
    (stamped, unstamped)
}

/// Every stamped row's (seq, envelope id, whether it is a traffic row), in
/// seq order, and how many rows are unstamped.
async fn stamped_rows(pool: &PgPool) -> (Vec<(i64, EventId, bool)>, i64) {
    let rows = sqlx::query(
        "SELECT seq, envelope_id, event IS NULL AS traffic FROM topology.outbox ORDER BY seq",
    )
    .fetch_all(pool)
    .await
    .expect("the outbox");
    let mut stamped = Vec::new();
    let mut unstamped = 0;
    for row in rows {
        let seq: i64 = row.get("seq");
        match row.get::<Option<String>, _>("envelope_id") {
            Some(id) => stamped.push((
                seq,
                codec::stored_event(&id).expect("an event id"),
                row.get::<bool, _>("traffic"),
            )),
            None => unstamped += 1,
        }
    }
    (stamped, unstamped)
}

/// Run a first relay that stops at `fault`, then a second, healthy one,
/// and check the log the two leave.
async fn crash_then_recover(db: &TestDb, fault: Fault) {
    reset(db.pool()).await;
    stage(db.pool()).await;

    // The first process: its own pool, closed after the crash.
    let crashing = small_pool(db.url().connect_options().clone(), 2).await;
    let (first, mut hung) = FaultBus::new(fault);
    let mut ids = outbox_ids(11);
    match fault {
        Fault::Hang { .. } => {
            tokio::select! {
                result = drain(&crashing, &mut ids, &first) => panic!("the drain finished: {result:?}"),
                _ = hung.recv() => {}
            }
        }
        Fault::Fail { .. } => {
            assert!(drain(&crashing, &mut ids, &first).await.is_err());
        }
        Fault::None => {
            drain(&crashing, &mut ids, &first).await.expect("drained");
        }
    }
    crashing.close().await;

    // What survived is stamped: the stamp committed before any publish.
    let (left, unstamped) = stamped_rows(db.pool()).await;
    assert_eq!(unstamped, 0, "{fault:?}: a row reached a publish unstamped");
    let before: Vec<Envelope> = first.log();
    let first_ids: BTreeMap<EventId, Envelope> = before.iter().map(|e| (e.id, e.clone())).collect();

    // The second process: other entropy, a later clock.
    let (second, _hung) = FaultBus::new(Fault::None);
    let mut later = outbox_ids(99);
    drain(db.pool(), &mut later, &second)
        .await
        .expect("the recovery drained");
    assert_eq!(count(db.pool(), "outbox").await, 0, "{fault:?}");
    let after = second.log();

    // Every row left after the crash is republished under its stamp.
    let republished: Vec<EventId> = after.iter().map(|e| e.id).collect();
    for (seq, id, traffic) in &left {
        // A traffic row publishes nothing until the spec has
        // `Changed::Traffic` (outbox::traffic_notification).
        assert_eq!(
            republished.contains(id),
            !traffic,
            "{fault:?}: row {seq} left after the crash"
        );
    }

    // A republished id carries what it carried the first time.
    for envelope in &after {
        if let Some(earlier) = first_ids.get(&envelope.id) {
            assert_eq!(earlier, envelope, "{fault:?}: an id changed its envelope");
        }
    }

    // The log, deduplicated by id, holds each staged event once, in order.
    let mut seen = BTreeMap::new();
    let mut log = Vec::new();
    for envelope in before.into_iter().chain(after) {
        if seen.insert(envelope.id, ()).is_none() {
            log.push(envelope);
        }
    }
    let events: Vec<BusEvent> = log.iter().map(|e| e.event.clone()).collect();
    assert_eq!(events, staged_events(), "{fault:?}");
    let mut ids: Vec<EventId> = log.iter().map(|e| e.id).collect();
    let in_order = ids.clone();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 8, "{fault:?}: one id per event");
    assert_eq!(ids, in_order, "{fault:?}: ids increase in commit order");
}

/// topology.outbox.stable-envelope-id: at every crash point, each staged
/// event lands in the log once, under the id stamped before its first
/// publish.
pub(super) async fn every_crash_point_publishes_each_event_once() {
    let Some(db) = database("outbox_relay_publishes_each_event_once_under_its_stamped_id").await
    else {
        return;
    };
    let faults = [
        Fault::None,
        Fault::Hang {
            call: 0,
            recorded: false,
        },
        Fault::Hang {
            call: 0,
            recorded: true,
        },
        Fault::Hang {
            call: 3,
            recorded: true,
        },
        Fault::Hang {
            call: 5,
            recorded: false,
        },
        Fault::Hang {
            call: 7,
            recorded: true,
        },
        Fault::Fail { call: 0 },
        Fault::Fail { call: 4 },
        Fault::Fail { call: 7 },
    ];
    for fault in faults {
        crash_then_recover(&db, fault).await;
    }
    db.close().await.expect("drop the database");
}

/// A row whose transaction has not committed is invisible to the relay: it
/// is neither stamped nor published until the commit.
#[tokio::test(flavor = "multi_thread")]
async fn outbox_relay_never_publishes_an_uncommitted_row() {
    let Some(db) = database("outbox_relay_never_publishes_an_uncommitted_row").await else {
        return;
    };
    let event = BusEvent::Insight(InsightEvent::WatermarkAdvanced(Watermark(ts(10))));
    let mut staging = db.pool().begin().await.expect("a transaction");
    enqueue(&mut staging, &event).await.expect("enqueued");
    let (bus, _hung) = FaultBus::new(Fault::None);
    let mut ids = outbox_ids(3);
    assert_eq!(drain(db.pool(), &mut ids, &bus).await.expect("drained"), 0);
    assert!(bus.log().is_empty());
    staging.commit().await.expect("committed");
    assert_eq!(drain(db.pool(), &mut ids, &bus).await.expect("drained"), 1);
    let log = bus.log();
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].event, event);
    db.close().await.expect("drop the database");
}

/// The stamp coalesces a batch's unstamped traffic rows into its last one,
/// whose window becomes their hull, and stamps every row; a second stamp
/// changes nothing, so a republish carries the same window and id.
#[tokio::test(flavor = "multi_thread")]
async fn outbox_stamp_coalesces_traffic_once() {
    let Some(db) = database("outbox_stamp_coalesces_traffic_once").await else {
        return;
    };
    stage(db.pool()).await;
    let mut ids = outbox_ids(5);
    assert_eq!(stamp_batch(db.pool(), &mut ids).await.expect("stamped"), 2);
    let traffic: Vec<(i64, i64, i64, String)> = sqlx::query_as(
        "SELECT seq, traffic_start, traffic_end, envelope_id FROM topology.outbox \
         WHERE event IS NULL",
    )
    .fetch_all(db.pool())
    .await
    .expect("the traffic rows");
    assert_eq!(traffic.len(), 1);
    let (seq, start, end, id) = traffic[0].clone();
    assert_eq!(
        (start, end),
        (0, 30),
        "the hull of [0,10), [10,20), [20,30)"
    );
    let (stamped, unstamped) = stamps(db.pool()).await;
    assert_eq!((stamped.len(), unstamped), (9, 0));
    assert_eq!(stamp_batch(db.pool(), &mut ids).await.expect("stamped"), 0);
    let (again, _) = stamps(db.pool()).await;
    assert_eq!(again, stamped);
    assert!(again.contains(&(seq, codec::stored_event(&id).expect("an id"))));
    // Stamped ids increase with seq.
    let ordered: Vec<EventId> = stamped.iter().map(|(_, id)| *id).collect();
    let mut sorted = ordered.clone();
    sorted.sort();
    assert_eq!(ordered, sorted);
    // Publishing a batch whose bus fails keeps every unpublished row.
    let (failing, _hung) = FaultBus::new(Fault::Fail { call: 2 });
    assert!(publish_batch(db.pool(), &failing).await.is_err());
    let (kept, _) = stamps(db.pool()).await;
    assert_eq!(kept, stamped[stamped.len() - kept.len()..].to_vec());
    db.close().await.expect("drop the database");
}
