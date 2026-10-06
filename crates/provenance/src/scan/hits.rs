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
    /// For a live span holding coincident template stretches, its sources'
    /// originations and copies, one hop (`Coincidence`): the holders a hit
    /// on the source would have counted.
    coincident: HashMap<SpanId, Vec<(Timestamp, AgentId)>>,
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
            coincident: HashMap::new(),
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

    /// Add, for each coincidence of a live span, its source's indexing and
    /// copies made at or before `now` (`sources` are the sources' records,
    /// `relays` their copies). One hop: a source's own coincidences are not
    /// followed.
    pub fn add_coincident(
        &mut self,
        coincidences: &[crate::store::Coincidence],
        sources: &[SpanRecord],
        relays: &[crate::store::Relay],
        now: Timestamp,
    ) {
        for coincidence in coincidences {
            if !self.records.contains_key(&coincidence.span) {
                continue;
            }
            let held = self.coincident.entry(coincidence.span).or_default();
            if let Some(source) = sources
                .iter()
                .find(|record| record.span.id == coincidence.source)
                && let Some(at) = source.indexed_at().filter(|at| *at <= now)
            {
                held.push((at, source.span.agent));
            }
            held.extend(
                relays
                    .iter()
                    .filter(|relay| relay.source == coincidence.source && relay.at <= now)
                    .map(|relay| (relay.at, relay.agent)),
            );
        }
    }

    /// The originations and copies of the spans `span` coincides with, one
    /// hop: agents that hold its text for the spread rule's agent count
    /// (`provenance.match.cross-agent-spread`). They are not its own
    /// copies, so they never raise a rarity bound.
    pub fn coincident_holders(&self, span: SpanId) -> &[(Timestamp, AgentId)] {
        self.coincident.get(&span).map_or(&[], Vec::as_slice)
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
        for (span, held) in other.coincident {
            self.coincident.entry(span).or_default().extend(held);
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
///
/// Returns each such fingerprint with its holders: how many originations
/// and copies it has. Whether it is boilerplate also needs its
/// distinctiveness, which the scanner reads from token frequencies.
pub fn spread_boilerplate(
    hits: &[FingerprintHit],
    live: &LiveSpans,
    rule: SpreadRule,
) -> BTreeMap<Fingerprint, usize> {
    // The agents count a hit span's originations and copies and, one hop,
    // those of the spans it coincides with; the holders (for the rarity
    // bound) count its own originations and copies only.
    let mut found: BTreeMap<Fingerprint, Spread> = BTreeMap::new();
    for hit in hits {
        let (spans, holders, agents) = found.entry(hit.fingerprint).or_default();
        if spans.insert(hit.span) {
            let own = live.originations(hit.span);
            agents.extend(own.iter().map(|(_, agent)| *agent));
            agents.extend(
                live.coincident_holders(hit.span)
                    .iter()
                    .map(|(_, agent)| *agent),
            );
            holders.extend(own.into_iter().map(|(_, agent)| agent));
        }
    }
    found
        .into_iter()
        .filter_map(|(fingerprint, (_, holders, agents))| {
            (agents.len() >= rule.agents()).then_some((fingerprint, holders.len()))
        })
        .collect()
}

/// One fingerprint's hit spans, holders (for the rarity bound) and
/// agents (for the agent count).
type Spread = (BTreeSet<SpanId>, Vec<AgentId>, BTreeSet<AgentId>);

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

/// The runs of `extents` (merged, offsets into `layer`) holding at least
/// `min_chars` normalized characters, trimmed.
pub fn long_runs(layer: &str, extents: &[(u32, u32)], min_chars: usize) -> Vec<(u32, u32)> {
    merge(extents.to_vec())
        .into_iter()
        .filter(|(start, end)| {
            let slice = layer
                .get(
                    usize::try_from(*start).unwrap_or(usize::MAX)
                        ..usize::try_from(*end).unwrap_or(usize::MAX),
                )
                .unwrap_or_default();
            crate::text::normalize::trimmed_len(&crate::text::normalize::normalize(slice))
                >= min_chars
        })
        .collect()
}
