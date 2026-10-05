//! Which index hits count, and the bytes they cover.
//!
//! A hit counts when its span is stored, live (`Indexed` or `Propagated`,
//! or a forwarded span whose forwarding is `Indexed`; so never after
//! expiry, `provenance.match.none-after-expiry`), was indexed
//! at or before the reader's time, and was indexed before this scan began
//! (its index sequence at most the watermark read at the start,
//! `provenance.match.indexed-before-read`).

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crosstalk_spec::derived::provenance::fingerprint::{Fingerprint, FingerprintHit};
use crosstalk_spec::ids::{AgentId, SpanId};
use crosstalk_spec::support::Timestamp;

use crate::config::SpreadRule;
use crate::fingerprint::KGram;
use crate::store::SpanRecord;

/// The spans hits may name, after filtering.
#[derive(Debug, Clone, Default)]
pub struct LiveSpans {
    records: HashMap<SpanId, SpanRecord>,
    /// Copies of the live spans in other outputs: (exchange start, agent).
    relays: HashMap<SpanId, Vec<(Timestamp, AgentId)>>,
}

impl LiveSpans {
    /// The records of `records` that hits may name at `now`, given the
    /// watermark read when the scan began.
    pub fn new(records: Vec<SpanRecord>, watermark: u64, now: Timestamp) -> Self {
        let records = records
            .into_iter()
            .filter(|record| {
                record
                    .indexed_at()
                    .is_some_and(|indexed_at| indexed_at <= now)
                    && record.index_seq.is_some_and(|seq| seq <= watermark)
            })
            .map(|record| (record.span.id, record))
            .collect();
        Self {
            records,
            relays: HashMap::new(),
        }
    }

    /// Add the copies (`Relay`s) of live spans made at or before `now`.
    pub fn add_relays(&mut self, relays: Vec<crate::store::Relay>, now: Timestamp) {
        for relay in relays {
            if relay.at <= now && self.records.contains_key(&relay.source) {
                self.relays
                    .entry(relay.source)
                    .or_default()
                    .push((relay.at, relay.agent));
            }
        }
    }

    /// Where and by whom `span` was originated or copied: its own indexing,
    /// then its copies.
    pub fn originations(&self, span: SpanId) -> Vec<(Timestamp, AgentId)> {
        let Some(record) = self.records.get(&span) else {
            return Vec::new();
        };
        record
            .indexed_at()
            .map(|at| (at, record.span.agent))
            .into_iter()
            .chain(self.relays.get(&span).into_iter().flatten().copied())
            .collect()
    }

    pub fn get(&self, span: SpanId) -> Option<&SpanRecord> {
        self.records.get(&span)
    }

    pub fn extend(&mut self, other: LiveSpans) {
        self.records.extend(other.records);
        for (span, relays) in other.relays {
            self.relays.entry(span).or_default().extend(relays);
        }
    }
}

/// Each hit span's covered byte extents in the query's coordinates, from
/// the queried k-grams (a hit's `query_offset` is its k-gram's start; a
/// k-gram and the short-span runs starting at the same offset are told
/// apart by their fingerprints).
pub fn extents_by_span(
    hits: &[FingerprintHit],
    kgrams: &[KGram],
    keep: impl Fn(SpanId) -> bool,
) -> BTreeMap<SpanId, Vec<(u32, u32)>> {
    let mut ends: HashMap<(u32, Fingerprint), u32> = HashMap::with_capacity(kgrams.len());
    for kgram in kgrams {
        let end = ends
            .entry((kgram.start, kgram.fingerprint))
            .or_insert(kgram.end);
        *end = (*end).max(kgram.end);
    }
    let mut by_span: BTreeMap<SpanId, Vec<(u32, u32)>> = BTreeMap::new();
    for hit in hits {
        if !keep(hit.span) {
            continue;
        }
        if let Some(end) = ends.get(&(hit.query_offset, hit.fingerprint)) {
            by_span
                .entry(hit.span)
                .or_default()
                .push((hit.query_offset, *end));
        }
    }
    by_span
}

/// The fingerprints among `hits` that are boilerplate for short runs by
/// the spread rule (`provenance.match.cross-agent-spread`): at least
/// `rule.agents()` distinct agents originated or copied them, at any time.
/// A lookup returns every posting of each queried fingerprint, so the
/// agents of its live hit spans, and of their copies in other outputs
/// (spans relayed from them), are its originating agents within retention.
pub fn spread_boilerplate(
    hits: &[FingerprintHit],
    live: &LiveSpans,
    rule: SpreadRule,
) -> BTreeSet<Fingerprint> {
    let mut agents: BTreeMap<Fingerprint, BTreeSet<AgentId>> = BTreeMap::new();
    for hit in hits {
        agents.entry(hit.fingerprint).or_default().extend(
            live.originations(hit.span)
                .into_iter()
                .map(|(_, agent)| agent),
        );
    }
    agents
        .into_iter()
        .filter(|(_, agents)| agents.len() >= rule.agents())
        .map(|(fingerprint, _)| fingerprint)
        .collect()
}

/// `extents` merged into disjoint sorted intervals.
pub fn merge(mut extents: Vec<(u32, u32)>) -> Vec<(u32, u32)> {
    extents.sort_unstable();
    let mut merged: Vec<(u32, u32)> = Vec::with_capacity(extents.len());
    for (start, end) in extents {
        match merged.last_mut() {
            Some(last) if start <= last.1 => last.1 = last.1.max(end),
            _ => merged.push((start, end)),
        }
    }
    merged
}

/// How many bytes the merged intervals cover.
pub fn covered(merged: &[(u32, u32)]) -> u32 {
    merged.iter().map(|(start, end)| end - start).sum()
}
