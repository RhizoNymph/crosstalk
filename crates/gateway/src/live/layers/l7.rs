//! L7: `crosstalk-topology`'s handler over the shared in-memory edge store,
//! announcing `EdgeUpdated` on the bus, and the watermark.
//!
//! **Watermark.** On each tick at `now` (after L5's tick at the same
//! instant: slots tick in order), when every layer group from L3 to L7 is
//! empty, the stage advances the edge store's watermark from the frontier
//! `PipelineFrontier { ticked_through: now, oldest_pending: None }`. The
//! store settles it as the spec defines (`Watermark::settled`: `now` minus
//! the correlator's `settle_after`, `evidence_window + suspected_ttl`,
//! aligned down to a bucket boundary) and never lowers it. With a group
//! still holding deliveries the tick leaves the watermark where it is: a
//! pending event's time could be earlier, and the next tick (or the next
//! `Live::settle` pass) tries again. Exchanges in flight at the proxy are
//! not counted: their times are later than `now - settle_after` for any
//! request shorter than the settle bound.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crosstalk_api::in_process::Edges;
use crosstalk_spec::aggregates::watermark::PipelineFrontier;
use crosstalk_spec::events::{Envelope, Subject};
use crosstalk_spec::ids::SeededRandom;
use crosstalk_spec::interfaces::l7_topology::EdgeStore;
use crosstalk_spec::support::Timestamp;
use crosstalk_topology::consumer::{Outcome, SUBJECTS, handle};
use crosstalk_topology::outbox::BusAnnouncer;
use crosstalk_transport::MpscBus;

use crate::live::stage::{Slot, Stage, StageContext, StageError};

/// The groups whose pending deliveries can still reach a bucket.
const UPSTREAM: [Slot; 5] = [
    Slot::L3Reconstruct,
    Slot::L4Provenance,
    Slot::L5Flow,
    Slot::L6Classify,
    Slot::L7Topology,
];

/// The L7 slot's stage.
pub struct Topology {
    edges: Edges,
    announcer: BusAnnouncer<MpscBus>,
    bus: MpscBus,
    watermark: Arc<AtomicU64>,
}

impl Topology {
    pub fn new(ctx: &StageContext) -> Self {
        Self {
            edges: ctx.stores.edges.clone(),
            announcer: BusAnnouncer::new(
                ctx.stores.bus.clone(),
                Arc::clone(&ctx.clock),
                SeededRandom::new(ctx.seed ^ 0x7070),
            ),
            bus: ctx.stores.bus.clone(),
            watermark: Arc::clone(&ctx.watermark),
        }
    }

    /// Whether any group on the way to the edge store holds a delivery.
    async fn pending(&self) -> bool {
        for slot in UPSTREAM {
            match self.bus.depth(&slot.group()).await {
                Ok(Some(depth)) if depth.tracked() + depth.waiting > 0 => return true,
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(group = %slot.group().0, error = ?error, "reading a group's depth failed; watermark held");
                    return true;
                }
            }
        }
        false
    }
}

impl Stage for Topology {
    fn subjects(&self) -> Vec<Subject> {
        SUBJECTS.to_vec()
    }

    async fn handle(&mut self, envelope: &Envelope) -> Result<(), StageError> {
        match handle(&mut self.edges, &self.announcer, &envelope.event).await {
            Outcome::Ack => Ok(()),
            Outcome::Nack(reason) => Err(StageError::Retry { reason }),
        }
    }

    async fn tick(&mut self, now: Timestamp) {
        if self.pending().await {
            tracing::debug!(at = now.as_micros(), "deliveries pending; watermark held");
            return;
        }
        let frontier = PipelineFrontier {
            ticked_through: now,
            oldest_pending: None,
        };
        match self.edges.advance_watermark(frontier).await {
            Ok(Some(advanced)) => {
                self.watermark
                    .store(advanced.at().as_micros(), Ordering::SeqCst);
                tracing::debug!(watermark = advanced.at().as_micros(), "watermark advanced");
            }
            Ok(None) => {}
            Err(error) => tracing::warn!(error = ?error, "watermark advance failed"),
        }
    }
}
