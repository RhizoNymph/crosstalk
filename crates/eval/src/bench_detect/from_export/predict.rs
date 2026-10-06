//! The gateway's detections on a capture as prediction rows.
//!
//! The transmissions are those ct-eval scores (`detected::choose`): every
//! exported one with evidence, every suspected or discarded one the
//! evidence holds, less those read only outside the run window. Rows are
//! `golden::predictions::rows`, a transmission whose evidence lies outside
//! the world dropped and counted.
//!
//! **Attribution** ([`AttributionSource`]):
//!
//! - `query`: the saved `exchange-turns` answers place each exchange of the
//!   world under its canonical agent; `span-points` gives each content
//!   match's origin (`origin_at`, when the span's exchange is in the world).
//! - `evidence` (no saved answers): ct-eval's ties (`golden::swarm::ties`):
//!   a confirmed transmission's reader exchanges are its reader's, an
//!   access's exchange its canonical agent's. A sender that is neither
//!   reader nor accessor holds no exchange and is `unattributed`; no
//!   content match has an origin.
//!
//! Either way an access's own agent id is an alias of its canonical one
//! (`AccessDetail::agent`). No truth is read.

use std::collections::{BTreeMap, BTreeSet};

use a2a_bench_format as bench;
use bench::predictions::Prediction;
use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::resource::{Locator, Resource};
use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::ids::{AccessId, AgentId, ChannelId, ExchangeId, SpanId};
use crosstalk_spec::interfaces::l4_provenance::IndexedSpan;
use crosstalk_spec::interfaces::l8_surface::conversation::SpanPoint;
use crosstalk_spec::observed::message::PartRef;
use serde::Serialize;

use super::{Capture, Detections, FromExportError};
use crate::datasets::swarm_truth::bodies::{Bodies, Cached};
use crate::datasets::swarm_truth::detected::{SwarmDirectory, choose};
use crate::datasets::swarm_truth::{AgentIndex, Diagnostics};
use crate::golden::predictions::{self, Unlocated};
use crate::golden::{Lossy, ids};
use crate::keys::AgentKey;
use crate::predict::Directory;

/// Where the attribution rows came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AttributionSource {
    /// The gateway's `POST /query/exchange-turns` and `/query/span-points`.
    Query,
    /// The evidence's readers and accessors (no conversation reads saved).
    Evidence,
}

/// A run's prediction rows.
#[derive(Debug, Clone)]
pub struct Predicted {
    pub rows: Vec<Prediction>,
    pub source: AttributionSource,
    pub transmissions: u64,
    pub attributed_exchanges: u64,
}

/// The evidence's directory, answering spans from the conversation reads.
struct WithSpans<'a> {
    inner: &'a SwarmDirectory,
    spans: BTreeMap<SpanId, IndexedSpan>,
}

impl Directory for WithSpans<'_> {
    fn agent(&self, id: AgentId) -> Option<AgentKey> {
        self.inner.agent(id)
    }

    fn channel(&self, id: ChannelId) -> Option<&[Locator]> {
        self.inner.channel(id)
    }

    fn span(&self, id: SpanId) -> Option<IndexedSpan> {
        self.spans.get(&id).copied()
    }

    fn access(&self, id: AccessId) -> Option<&(Access, Resource)> {
        self.inner.access(id)
    }

    fn whole_part(&self, exchange: ExchangeId, part: PartRef) -> Option<SpanLocation> {
        self.inner.whole_part(exchange, part)
    }
}

/// The spans whose exchange the world holds, as span records.
fn located(
    points: &BTreeMap<SpanId, SpanPoint>,
    world: &BTreeSet<bench::ids::ExchangeId>,
) -> BTreeMap<SpanId, IndexedSpan> {
    points
        .iter()
        .filter(|(_, point)| world.contains(&ids::exchange(point.exchange)))
        .map(|(id, point)| {
            (
                *id,
                IndexedSpan {
                    exchange: point.exchange,
                    author: point.agent,
                    location: point.location,
                },
            )
        })
        .collect()
}

/// The rows of `detections` on `capture` (module docs).
pub fn rows<B: Bodies>(
    capture: &Capture,
    detections: &Detections,
    bodies: &mut Cached<B>,
    lossy: &mut Lossy,
) -> Result<Predicted, FromExportError> {
    let mut diagnostics = Diagnostics::default();
    let chosen = choose(
        &detections.exported,
        &detections.evidence,
        &capture.outside,
        &mut diagnostics,
    );
    let directory =
        SwarmDirectory::learn(&chosen, &AgentIndex::default(), bodies, &mut diagnostics);
    let (ties, aliases) = crate::golden::swarm::ties(&chosen, &capture.world);
    let world: BTreeSet<bench::ids::ExchangeId> = capture
        .world
        .exchanges
        .iter()
        .map(|exchange| exchange.id)
        .collect();
    let transmissions: Vec<_> = chosen
        .iter()
        .map(|item| item.transmission().clone())
        .collect();
    let (held, spans, source) = match &detections.queried {
        Some(queried) => {
            let mut held: BTreeMap<AgentId, BTreeSet<ExchangeId>> = BTreeMap::new();
            for (exchange, placement) in &queried.turns {
                if world.contains(&ids::exchange(*exchange)) {
                    held.entry(placement.agent).or_default().insert(*exchange);
                }
            }
            (
                held,
                located(&queried.spans, &world),
                AttributionSource::Query,
            )
        }
        None => (ties, BTreeMap::new(), AttributionSource::Evidence),
    };
    let attributed_exchanges = held.values().map(|held| held.len() as u64).sum();
    let directory = WithSpans {
        inner: &directory,
        spans,
    };
    let before = lossy.dropped_transmissions;
    let rows = predictions::rows(
        &transmissions,
        &directory,
        &held,
        &aliases,
        Unlocated::Drop,
        &capture.world.index,
        lossy,
    )?;
    Ok(Predicted {
        rows,
        source,
        transmissions: transmissions.len() as u64 - (lossy.dropped_transmissions - before),
        attributed_exchanges,
    })
}
