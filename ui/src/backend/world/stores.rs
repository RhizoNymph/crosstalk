//! The in-process surface's memory stores as the world's [`WorldStores`]:
//! the seed writes through the spec's write traits on handles that share
//! state with the surface.

use crosstalk_api::MemoryStores;
use crosstalk_api::in_process::{Alerts, Edges, MemoryEvidence, Search};
use crosstalk_memory::analysis::catalog::InMemoryTopicCatalog;
use crosstalk_memory::analysis::projection::InMemoryProjectionStore;
use crosstalk_memory::flow::{MemoryChannels, MemoryVerdicts};
use crosstalk_memory::reconstruct::MemoryAgents;
use crosstalk_memory::surface::audit::InMemoryAuditLog;
use crosstalk_memory::surface::operators::InMemoryOperatorStore;
use crosstalk_memory::surface::sinks::InMemorySinkRegistry;
use crosstalk_transport::DeadLetters;
use crosstalk_transport::blob::MemoryBlobStore;
use crosstalk_world::WorldStores;

/// The surface's stores, while the world is written into them.
pub struct Seeding(pub MemoryStores);

impl WorldStores for Seeding {
    type Agents = MemoryAgents;
    type Spans = MemoryEvidence;
    type Channels = MemoryChannels<MemoryAgents>;
    type Transmissions = MemoryVerdicts;
    type Catalog = InMemoryTopicCatalog;
    type Search = Search;
    type Edges = Edges;
    type Alerts = Alerts;
    type Projections = InMemoryProjectionStore;
    type Operators = InMemoryOperatorStore;
    type Audit = InMemoryAuditLog;
    type Sinks = InMemorySinkRegistry;
    type Letters = DeadLetters;
    type Blobs = MemoryBlobStore;

    fn agents(&mut self) -> &mut Self::Agents {
        &mut self.0.agents
    }
    fn spans(&mut self) -> &mut Self::Spans {
        &mut self.0.evidence
    }
    fn channels(&mut self) -> &mut Self::Channels {
        &mut self.0.channels
    }
    fn transmissions(&mut self) -> &mut Self::Transmissions {
        &mut self.0.transmissions
    }
    fn catalog(&mut self) -> &mut Self::Catalog {
        &mut self.0.catalog
    }
    fn search(&mut self) -> &mut Self::Search {
        &mut self.0.search
    }
    fn edges(&mut self) -> &mut Self::Edges {
        &mut self.0.edges
    }
    fn alerts(&mut self) -> &mut Self::Alerts {
        &mut self.0.alerts
    }
    fn projections(&mut self) -> &mut Self::Projections {
        &mut self.0.projections
    }
    fn operators(&mut self) -> &mut Self::Operators {
        &mut self.0.operators
    }
    fn audit(&mut self) -> &mut Self::Audit {
        &mut self.0.audit
    }
    fn sinks(&mut self) -> &mut Self::Sinks {
        &mut self.0.sinks
    }
    fn letters(&mut self) -> &mut Self::Letters {
        &mut self.0.dead_letters
    }
    fn blobs(&mut self) -> &mut Self::Blobs {
        &mut self.0.blobs
    }
}
