//! L7: `crosstalk-topology`'s handler over the shared in-memory edge store,
//! announcing `EdgeUpdated` on the bus.
//!
//! The watermark is not recomputed: a live process has no frontier source
//! yet (the gateway's is Postgres-backed), so buckets stay open and every
//! contribution lands. Edges and series read the open buckets.

use std::sync::Arc;

use crosstalk_api::in_process::Edges;
use crosstalk_spec::events::{Envelope, Subject};
use crosstalk_spec::ids::SeededRandom;
use crosstalk_topology::consumer::{Outcome, SUBJECTS, handle};
use crosstalk_topology::outbox::BusAnnouncer;
use crosstalk_transport::MpscBus;

use crate::live::stage::{Stage, StageContext, StageError};

/// The L7 slot's stage.
pub struct Topology {
    edges: Edges,
    announcer: BusAnnouncer<MpscBus>,
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
        }
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
}
