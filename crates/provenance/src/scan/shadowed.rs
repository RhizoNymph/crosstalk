//! Shadowed fragments (`provenance.match.shadowed-fragment-dropped`).
//!
//! Writers of one wiki page fill the same topic's sentence templates with
//! the topic's slot words, so they share short runs ("notes on cache
//! invalidation stil"). When a writer's output repeats such a run of an
//! earlier writer's span, output resolution relays it to that span (no
//! `ReaderOutput` match is made: it is short, or holds no rare token), so
//! the run is not posted under the writer, and the writer's own spans
//! around it leave a hole. A reader of the writer's page then matches the
//! earlier writer through the relay, or a later writer that wrote the same
//! run itself (bench run 20261006T021639Z: 7 false `ToolResult` matches of
//! 32 to 46 bytes, held by two or three agents, so the spread rule never
//! applied).
//!
//! In one decode layer of a read, an agent is **present** when one of its
//! originated spans' merged hit runs holds at least
//! `SpreadRule::distinctive_chars` normalized characters; its **extent** is
//! the stretch from the first to the last byte of all its spans' runs, and
//! its coverage the bytes those runs cover. A candidate match on a span of
//! an agent with no such run anywhere in the layer, whose every run lies
//! inside the extent of a present agent other than its own and covering
//! more of the layer than its own agent, is shadowed: the read is that
//! agent's text. It is dropped
//! when no whole token of its runs is rare (seen in at most
//! `SpreadRule::rare_bound` texts for the span's holders), so a secret or
//! id pasted into someone else's page still matches its writer.

use std::collections::BTreeMap;

use crosstalk_spec::derived::provenance::span::{Origin, SpanState};
use crosstalk_spec::ids::{AgentId, SpanId};

use super::hits::{LiveSpans, covered, long_runs, merge};

/// One agent's runs in a layer.
#[derive(Debug, Clone, Default)]
struct Presence {
    /// One of its spans holds a long run among all its hits.
    long: bool,
    /// All its spans' runs, among all its hits.
    runs: Vec<(u32, u32)>,
    /// One of its originated spans holds a long run among its counted
    /// hits (those the reader's nearer source does not explain).
    present: bool,
    /// The first and last byte of its spans' counted runs.
    extent: Option<(u32, u32)>,
    /// Its spans' counted runs.
    counted: Vec<(u32, u32)>,
}

/// The agents present in one layer of a read, and each hit span's agent.
#[derive(Debug, Clone, Default)]
pub struct Shadows {
    agents: BTreeMap<SpanId, AgentId>,
    presence: BTreeMap<AgentId, Presence>,
}

impl Shadows {
    /// The presence of every agent among the hit spans in `layer`: `every`
    /// span's hit extents and its `counted` ones (layer offsets), runs of
    /// `min_chars` normalized characters or more being long. An agent
    /// shadows others only through the hits that count for the reader, so
    /// text the reader already had never shadows anyone.
    pub fn new(
        layer: &str,
        every: &BTreeMap<SpanId, Vec<(u32, u32)>>,
        counted: &BTreeMap<SpanId, Vec<(u32, u32)>>,
        live: &LiveSpans,
        min_chars: usize,
    ) -> Self {
        let mut shadows = Self::default();
        for (span, extents) in every {
            let Some(record) = live.get(*span) else {
                continue;
            };
            let agent = record.span.agent;
            shadows.agents.insert(*span, agent);
            let runs = merge(extents.clone());
            let presence = shadows.presence.entry(agent).or_default();
            presence.long |= !long_runs(layer, &runs, min_chars).is_empty();
            presence.runs.extend(runs);
            let counted = merge(counted.get(span).cloned().unwrap_or_default());
            let (Some(first), Some(last)) = (counted.first(), counted.last()) else {
                continue;
            };
            presence.present |= record.span.state.origin() == Some(Origin::Originated)
                && !long_runs(layer, &counted, min_chars).is_empty();
            presence.extent = Some(match presence.extent {
                Some((start, end)) => (start.min(first.0), end.max(last.1)),
                None => (first.0, last.1),
            });
            presence.counted.extend(counted);
        }
        for presence in shadows.presence.values_mut() {
            presence.runs = merge(std::mem::take(&mut presence.runs));
            presence.counted = merge(std::mem::take(&mut presence.counted));
        }
        shadows
    }

    /// The agent shadowing `span`'s runs (`extents`, layer offsets, all its
    /// hits), if any: its own agent holds no long run in the layer, and
    /// every run lies inside the extent of another, present agent whose
    /// counted runs cover more of the layer than all of its own agent's
    /// runs. The one shadowing its first run is named.
    pub fn shadower(&self, span: SpanId, extents: &[(u32, u32)]) -> Option<AgentId> {
        let agent = self.agents.get(&span)?;
        let own = self.presence.get(agent)?;
        if own.long {
            return None;
        }
        let own_covered = covered(&own.runs);
        let shadowing = |(start, end): &(u32, u32)| {
            self.presence.iter().find(|(other, presence)| {
                *other != agent
                    && presence.present
                    && covered(&presence.counted) > own_covered
                    && presence
                        .extent
                        .is_some_and(|(from, to)| from <= *start && *end <= to)
            })
        };
        let runs = merge(extents.to_vec());
        let first = shadowing(runs.first()?)?;
        runs.iter()
            .all(|run| shadowing(run).is_some())
            .then_some(*first.0)
    }
}

/// How many holders `span` has for the rarity bound: its originations and
/// copies, and the reads matched on it (its `Propagated` hits).
pub fn holders(live: &LiveSpans, span: SpanId) -> usize {
    let reads = match live.get(span).map(|record| &record.span.state) {
        Some(SpanState::Propagated { hits, .. }) => {
            usize::try_from(hits.get()).unwrap_or(usize::MAX)
        }
        _ => 0,
    };
    live.originations(span).len().saturating_add(reads)
}
