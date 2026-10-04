//! `check_edge_store`: the edge store against the reference, and against
//! the reference fold of `topology.graph.matches-fold-model`, kept by the
//! harness itself.
//!
//! Every read is compared in a normalized form (edges, nodes, accesses and
//! series sorted; shares to 1e-9), so an implementation may return them in
//! any order. After every graph read the harness also checks the graph's
//! own rules (`TopologyGraph::check`), that its shares sum to 1, and that
//! its edges are exactly the fold's; after every series read, that its
//! total is the graph's over the same window (`topology.series.total-
//! matches-graph`).

mod ops;
mod subject;

pub use subject::{EdgeSubject, ReferenceEdges, edge_config};

use std::collections::{BTreeMap, HashMap};

use proptest::prelude::*;

use crosstalk_spec::aggregates::edge::{TopologyFilter, TopologyGraph, Weighting};
use crosstalk_spec::aggregates::filter::FilterSubject;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::derived::flow::verdict::{CurrentVerdict, VerdictRevision};
use crosstalk_spec::ids::{AgentId, TransmissionId};
use crosstalk_spec::interfaces::l7_topology::EdgeContribution;
use crosstalk_spec::support::TimeWindow;

use crate::analysis::aliases::{Directories, StaticDirectory};
use crate::model::{Divergence, HarnessConfig, ModelMismatch, holds, run};
use crate::topology::fold::route_key;
use crate::topology::store::EdgeStoreConfig;

use ops::{EdgeOp, edge_op, play};

/// An edge as the fold lists it: sender, reader, route order key,
/// transmissions and matched bytes.
type FoldEdge = (AgentId, AgentId, (u8, String), u64, u64);

/// What the harness knows independently of either store: the
/// contributions applied and not dropped, and the verdicts judged.
#[derive(Debug, Default)]
struct Ledger {
    applied: BTreeMap<(TopicModelVersion, TransmissionId), EdgeContribution>,
    verdicts: BTreeMap<TransmissionId, CurrentVerdict>,
}

impl Ledger {
    fn judge(&mut self, transmission: TransmissionId, copy: CurrentVerdict) {
        match self.verdicts.get_mut(&transmission) {
            Some(held) => {
                held.observe(copy.verdict, copy.revision);
            }
            None => {
                self.verdicts.insert(transmission, copy);
            }
        }
    }

    /// The fold: per (from, to, route) over canonical agents and resolved
    /// routes, the transmissions and matched bytes of `version`'s
    /// contributions in `window` that `filter` admits.
    fn fold(
        &self,
        directory: &StaticDirectory,
        version: TopicModelVersion,
        window: TimeWindow,
        filter: &TopologyFilter,
    ) -> Vec<FoldEdge> {
        let aliases = Directories(directory);
        let mut sums: HashMap<(AgentId, AgentId, Route), (u64, u64)> = HashMap::new();
        for ((stored, transmission), contribution) in &self.applied {
            if *stored != version || !window.contains(contribution.at) {
                continue;
            }
            let from = crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory::canonical(
                directory,
                contribution.from,
            );
            let to = crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory::canonical(
                directory,
                contribution.to,
            );
            if from == to {
                continue;
            }
            let route = contribution.route.resolved(aliases);
            let subject = FilterSubject {
                from,
                to,
                route: &route,
                topic: contribution.classification.topic,
                false_detection: CurrentVerdict::is_false_detection(
                    self.verdicts.get(transmission),
                ),
            };
            if filter.admits(&subject, aliases) {
                let entry = sums.entry((from, to, route)).or_default();
                entry.0 += 1;
                entry.1 += contribution.matched_bytes.get();
            }
        }
        let mut edges: Vec<_> = sums
            .into_iter()
            .map(|((from, to, route), (count, bytes))| (from, to, route_key(&route), count, bytes))
            .collect();
        edges.sort();
        edges
    }
}

/// A graph's edges as the fold lists them.
fn graph_edges(graph: &TopologyGraph) -> Vec<FoldEdge> {
    let mut edges: Vec<_> = graph
        .edges()
        .iter()
        .map(|edge| {
            (
                edge.from,
                edge.to,
                route_key(&edge.route),
                edge.stats.transmissions.get(),
                edge.stats.matched_bytes.get(),
            )
        })
        .collect();
    edges.sort();
    edges
}

/// The graph's own rules, and the fold.
fn check_graph(
    step: usize,
    graph: &TopologyGraph,
    ledger: &Ledger,
    directory: &StaticDirectory,
    filter: &TopologyFilter,
) -> Result<(), Divergence> {
    let rules = TopologyGraph::new(graph.clone().into_parts()).map(|_| ());
    holds(step, rules.is_ok(), || {
        format!("graph breaks its rules: {rules:?}")
    })?;
    if !graph.edges().is_empty() {
        let sum: f64 = graph.edges().iter().map(|edge| edge.share.get()).sum();
        holds(step, (sum - 1.0).abs() <= 1e-9, || {
            format!("shares sum to {sum}")
        })?;
    }
    let expected = ledger.fold(directory, graph.topic_version(), graph.window(), filter);
    let got = graph_edges(graph);
    holds(step, got == expected, || {
        format!("graph edges differ from the fold:\n  graph: {got:?}\n  fold:  {expected:?}")
    })
}

/// Random applies, re-fits, activations, drops, verdicts, accesses,
/// merges, supersessions and watermark advances, with every read in
/// between, against the reference. `make` builds a fresh, empty subject
/// under the given configuration over an empty world: the catalog holding
/// only version 0 (keeping the three most recent activated versions), no
/// merge, supersession or parent.
pub fn check_edge_store<S, F, Fut>(harness: HarnessConfig, make: F) -> Result<(), ModelMismatch>
where
    S: EdgeSubject,
    F: Fn(EdgeStoreConfig) -> Fut,
    Fut: Future<Output = S>,
{
    let config =
        edge_config().ok_or_else(|| ModelMismatch::Setup("edge store config".to_owned()))?;
    let strategy = prop::collection::vec(edge_op(), 1..harness.max_ops);
    run(harness, strategy, |runtime, ops: &[EdgeOp]| {
        runtime.block_on(async {
            let mut subject = make(config).await;
            let mut reference =
                ReferenceEdges::new(config).map_err(|error| Divergence::new(0, error))?;
            let mut ledger = Ledger::default();
            let mut world = ops::World::default();
            for (step, op) in ops.iter().enumerate() {
                play(
                    step,
                    op,
                    &mut subject,
                    &mut reference,
                    &mut ledger,
                    &mut world,
                )
                .await?;
            }
            Ok(())
        })
    })
}

/// The revision `n`, at least 1.
fn revision(n: u32) -> VerdictRevision {
    VerdictRevision::new(std::num::NonZeroU32::new(n).unwrap_or(std::num::NonZeroU32::MIN))
}

/// The weighting `n` names.
fn weighting(n: bool) -> Weighting {
    if n {
        Weighting::MatchedBytes
    } else {
        Weighting::Transmissions
    }
}
