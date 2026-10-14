//! L7 in Postgres mode: `crosstalk-topology`'s handler over `PgEdgeStore`,
//! announcing `EdgeUpdated` under ids derived from the delivery, and the
//! watermark from [`PgFrontierSource`].
//!
//! On each tick the stage reads the frontier (the earliest checkpointed
//! shard tick, the pipeline groups' oldest pending and dead-lettered
//! events, the spool's oldest record) and asks the edge store to advance
//! its persisted watermark; the store never lowers it, restarts included
//! (`topology.watermark.monotone`).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crosstalk_api::pg::PgEdges;
use crosstalk_spec::events::{Envelope, Subject};
use crosstalk_spec::interfaces::l7_topology::{EdgeStore, FrontierSource};
use crosstalk_spec::support::Timestamp;
use crosstalk_topology::consumer::{Outcome, SUBJECTS, handle};
use crosstalk_topology::outbox::BusAnnouncer;

use crate::live::frontier::PgFrontierSource;
use crate::live::stage::{Stage, StageError};
use crate::spool::LiveBus;

/// The L7 slot's stage over Postgres.
pub struct PgTopology {
    edges: PgEdges<LiveBus>,
    announcer: BusAnnouncer<LiveBus>,
    frontier: PgFrontierSource<LiveBus>,
    watermark: Arc<AtomicU64>,
}

impl PgTopology {
    pub fn new(
        edges: PgEdges<LiveBus>,
        bus: LiveBus,
        frontier: PgFrontierSource<LiveBus>,
        watermark: Arc<AtomicU64>,
    ) -> Self {
        Self {
            edges,
            announcer: BusAnnouncer::new(bus),
            frontier,
            watermark,
        }
    }
}

impl Stage for PgTopology {
    fn subjects(&self) -> Vec<Subject> {
        SUBJECTS.to_vec()
    }

    async fn handle(&mut self, envelope: &Envelope) -> Result<(), StageError> {
        match handle(&mut self.edges, &self.announcer, envelope).await {
            Outcome::Ack => Ok(()),
            Outcome::Nack(reason) => Err(StageError::Retry { reason }),
        }
    }

    async fn tick(&mut self, now: Timestamp) {
        let frontier = match self.frontier.frontier().await {
            Ok(frontier) => frontier,
            Err(error) => {
                tracing::warn!(error = ?error, "frontier read failed; watermark held");
                return;
            }
        };
        match self.edges.advance_watermark(frontier).await {
            Ok(Some(advanced)) => {
                self.watermark
                    .store(advanced.at().as_micros(), Ordering::SeqCst);
                tracing::debug!(
                    at = now.as_micros(),
                    watermark = advanced.at().as_micros(),
                    "watermark advanced"
                );
            }
            Ok(None) => {}
            Err(error) => tracing::warn!(error = ?error, "watermark advance failed"),
        }
    }
}
