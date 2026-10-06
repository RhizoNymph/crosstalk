//! Restarts of L7 over a real Postgres (`TEST_DATABASE_URL`; each test
//! passes with a skip line without it): seeded sequences of watermark
//! advances, each "process" on its own pool, stopped at seeded points by
//! dropping its store and closing its pool, and a new one built over the
//! same database.

use std::sync::Mutex;

use crosstalk_memory::model::build::ts;
use crosstalk_spec::aggregates::watermark::PipelineFrontier;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::interfaces::l2_transport::BusError;
use crosstalk_spec::interfaces::l7_topology::EdgeStore;
use crosstalk_spec::support::Watermark;
use sqlx::PgPool;

use crate::integration::outbox_ids;
use crate::outbox::{Announce, drain};
use crate::tests::support::{WIDTH, database, reset, small_pool, world};

/// A small deterministic generator (xorshift64*), so a failing seed
/// replays.
struct Seeded(u64);

impl Seeded {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

/// Every `WatermarkAdvanced` relayed, in order.
#[derive(Default)]
struct Advances(Mutex<Vec<Watermark>>);

impl Announce for Advances {
    async fn announce(&self, envelope: Envelope) -> Result<(), BusError> {
        if let BusEvent::Insight(InsightEvent::WatermarkAdvanced(at)) = envelope.event {
            self.0.lock().expect("not poisoned").push(at);
        }
        Ok(())
    }
}

/// A frontier somewhere in `[0, 2000)` µs, sometimes held back by a
/// pending event.
fn frontier(random: &mut Seeded) -> PipelineFrontier {
    let ticked = random.below(2000);
    let oldest_pending = (random.below(3) == 0).then(|| ts(random.below(2000)));
    PipelineFrontier {
        ticked_through: ts(ticked),
        oldest_pending,
    }
}

async fn process(db: &crosstalk_store::TestDb) -> PgPool {
    small_pool(db.url().connect_options().clone(), 2).await
}

/// topology.watermark.monotone: over seeded runs of advances and
/// restarts, the watermark a store reads never goes below the highest one
/// any earlier process exposed, a restarted store reads exactly that one,
/// and every `WatermarkAdvanced` the outbox relays is later than the one
/// before it.
pub(super) async fn seeded_restarts_never_lower_the_watermark() {
    let Some(db) = database("watermark_never_decreases_across_restarts").await else {
        return;
    };
    for seed in 1..=6u64 {
        reset(db.pool()).await;
        let mut random = Seeded(0x9e37_79b9_7f4a_7c15 ^ seed);
        let mut pool = process(&db).await;
        let mut current = world(pool.clone());
        let mut highest = Watermark(ts(0));
        let mut restarts = 0;
        for step in 0..40 {
            if random.below(5) == 0 {
                drop(current);
                pool.close().await;
                pool = process(&db).await;
                current = world(pool.clone());
                restarts += 1;
                assert_eq!(
                    current.store.watermark().await,
                    Ok(highest),
                    "seed {seed} step {step}: a restarted store reads the persisted watermark"
                );
                continue;
            }
            let read = frontier(&mut random);
            let advanced = current
                .store
                .advance_watermark(read)
                .await
                .expect("advanced");
            if let Some(advanced) = advanced {
                assert!(
                    advanced > highest,
                    "seed {seed} step {step}: advanced to {advanced:?} from {highest:?}"
                );
                assert_eq!(advanced.at().as_micros() % WIDTH, 0, "aligned to a bucket");
                highest = advanced;
            }
            let now = current.store.watermark().await.expect("a watermark");
            assert_eq!(now, highest, "seed {seed} step {step}");
        }
        assert!(restarts > 0, "seed {seed}: no restart drawn");
        drop(current);
        pool.close().await;

        let relayed = Advances::default();
        let mut ids = outbox_ids(seed);
        drain(db.pool(), &mut ids, &relayed).await.expect("drained");
        let relayed = relayed.0.into_inner().expect("not poisoned");
        assert!(
            relayed.windows(2).all(|pair| pair[0] < pair[1]),
            "seed {seed}: {relayed:?}"
        );
        assert_eq!(relayed.last().copied().unwrap_or(Watermark(ts(0))), highest);
    }
    db.close().await.expect("drop the database");
}
