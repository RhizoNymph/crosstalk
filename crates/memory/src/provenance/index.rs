//! The in-memory fingerprint index.
//!
//! **Frequency.** `observe` is called once per scanned text (a span of any
//! origin, or a scanned input part) with that text's fingerprints, so one
//! call is one text, and a fingerprint repeated within it counts once.
//! `frequency(f, now)` is the number of observed texts containing `f` whose
//! observation time is within the retention period before `now`:
//! `now - retention <= at`. Every call that measures that window takes
//! `now` as an argument; older observations are dropped on every write.
//!
//! **Cutoff.** A fingerprint is boilerplate while its frequency is above
//! the cutoff (`frequency > cutoff`). `insert` stores no posting for it, and
//! `lookup` returns no hit on it, including postings stored while it was
//! below (`provenance.index.cutoff-not-inserted`,
//! `provenance.index.cutoff-not-returned`). A fingerprint whose frequency
//! falls back to the cutoff or below becomes matchable again, through the
//! postings stored while it was below.
//!
//! **Shards.** `insert` and `lookup` refuse, changing nothing, a call
//! holding any fingerprint whose shard this node does not own, naming the
//! first such fingerprint in input order
//! (`provenance.index.wrong-shard-rejected`). `observe`, `frequency` and
//! `evict` take any fingerprint.
//!
//! **Spans.** `SpanIndex::record` keeps where a span sits and who wrote it
//! (`IndexedSpan::of`), once per span id; `SpanIndex::spans` reads a batch
//! back, leaving out ids never recorded. `evict` keeps the records.
//!
//! **Order.** `lookup` returns hits in query order, then by span id and
//! span offset. The trait does not order hits, so the harness compares
//! them as multisets.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::num::NonZeroU16;
use std::time::Duration;

use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::provenance::fingerprint::{
    Fingerprint, FingerprintHit, PositionedFingerprint,
};
use crosstalk_spec::derived::provenance::span::OriginatedSpan;
use crosstalk_spec::ids::SpanId;
use crosstalk_spec::interfaces::l4_provenance::{
    FingerprintIndex, IndexError, IndexedSpan, SpanIndex, SpanIndexError,
};
use crosstalk_spec::support::Timestamp;

use crate::support::State;

/// How an index is configured: the boilerplate cutoff, how long frequency
/// observations count, and which shards this node owns.
///
/// Built only through [`IndexConfig::single_node`] and
/// [`IndexConfig::sharded`], so a node always owns at least one shard and
/// only shards that exist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexConfig {
    cutoff: u64,
    retention: Duration,
    shards: NonZeroU16,
    owned: BTreeSet<u16>,
}

/// A sharded configuration that owns nothing, or a shard that does not
/// exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidIndexConfig {
    #[error("the node owns no shard")]
    NoShard,
    #[error("shard {shard} does not exist among {shards}")]
    NoSuchShard { shard: u16, shards: u16 },
}

impl IndexConfig {
    /// One shard, owned by this node.
    pub fn single_node(cutoff: u64, retention: Duration) -> Self {
        Self {
            cutoff,
            retention,
            shards: NonZeroU16::MIN,
            owned: BTreeSet::from([0]),
        }
    }

    /// `shards` shards, of which this node owns `owned`.
    pub fn sharded(
        cutoff: u64,
        retention: Duration,
        shards: NonZeroU16,
        owned: BTreeSet<u16>,
    ) -> Result<Self, InvalidIndexConfig> {
        if owned.is_empty() {
            return Err(InvalidIndexConfig::NoShard);
        }
        if let Some(shard) = owned.iter().copied().find(|shard| *shard >= shards.get()) {
            return Err(InvalidIndexConfig::NoSuchShard {
                shard,
                shards: shards.get(),
            });
        }
        Ok(Self {
            cutoff,
            retention,
            shards,
            owned,
        })
    }

    pub fn cutoff(&self) -> u64 {
        self.cutoff
    }

    pub fn retention(&self) -> Duration {
        self.retention
    }

    /// How many shards there are.
    pub fn shards(&self) -> NonZeroU16 {
        self.shards
    }

    /// The shards this node owns.
    pub fn owned(&self) -> &BTreeSet<u16> {
        &self.owned
    }

    /// Whether this node owns `fingerprint`'s shard.
    pub fn owns(&self, fingerprint: Fingerprint) -> bool {
        self.owned.contains(&fingerprint.shard(self.shards))
    }

    /// Whether an observation at `at` still counts at `now`.
    fn counts(&self, at: Timestamp, now: Timestamp) -> bool {
        let retention = u64::try_from(self.retention.as_micros()).unwrap_or(u64::MAX);
        at.as_micros().saturating_add(retention) >= now.as_micros()
    }
}

/// The index's data: postings, observations and where each indexed span
/// sits, no text.
///
/// `counts` and `oldest` summarize `observations` so a frequency is one
/// map read: `counts[f]` is how many observations hold `f`, and `oldest`
/// the earliest observation time. While `oldest` still counts at a query's
/// `now`, every observation does (counting is monotone in the observation
/// time), so the count is the frequency; otherwise the observations are
/// counted one by one, as the definition says.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct IndexState {
    /// Each fingerprint's postings: the span and the k-gram's offset in it.
    pub(crate) postings: BTreeMap<Fingerprint, BTreeSet<(SpanId, u32)>>,
    /// Every span `SpanIndex::record` recorded, as first recorded. Never
    /// evicted: eviction drops postings, and spans are never deleted.
    pub(crate) spans: BTreeMap<SpanId, IndexedSpan>,
    /// One entry per observed text: its time and distinct fingerprints.
    pub(crate) observations: Vec<(Timestamp, BTreeSet<Fingerprint>)>,
    /// How many of `observations` hold each fingerprint (absent: none).
    counts: HashMap<Fingerprint, u64>,
    /// The earliest time in `observations` (`None` when there are none).
    oldest: Option<Timestamp>,
}

impl IndexState {
    fn frequency(&self, config: &IndexConfig, now: Timestamp, fingerprint: Fingerprint) -> u64 {
        match self.oldest {
            None => 0,
            Some(oldest) if config.counts(oldest, now) => {
                self.counts.get(&fingerprint).copied().unwrap_or(0)
            }
            Some(_) => {
                let count = self
                    .observations
                    .iter()
                    .filter(|(at, fingerprints)| {
                        config.counts(*at, now) && fingerprints.contains(&fingerprint)
                    })
                    .count();
                u64::try_from(count).unwrap_or(u64::MAX)
            }
        }
    }

    fn boilerplate(&self, config: &IndexConfig, now: Timestamp, fingerprint: Fingerprint) -> bool {
        self.frequency(config, now, fingerprint) > config.cutoff
    }

    fn observe(&mut self, at: Timestamp, fingerprints: BTreeSet<Fingerprint>) {
        for fingerprint in &fingerprints {
            *self.counts.entry(*fingerprint).or_insert(0) += 1;
        }
        self.oldest = Some(self.oldest.map_or(at, |oldest| oldest.min(at)));
        self.observations.push((at, fingerprints));
    }

    fn age_out(&mut self, config: &IndexConfig, now: Timestamp) {
        match self.oldest {
            Some(oldest) if !config.counts(oldest, now) => {}
            // Nothing observed, or every observation still counts.
            _ => return,
        }
        let counts = &mut self.counts;
        self.observations.retain(|(at, fingerprints)| {
            let kept = config.counts(*at, now);
            if !kept {
                for fingerprint in fingerprints {
                    if let Some(count) = counts.get_mut(fingerprint) {
                        *count -= 1;
                        if *count == 0 {
                            counts.remove(fingerprint);
                        }
                    }
                }
            }
            kept
        });
        self.oldest = self.observations.iter().map(|(at, _)| *at).min();
    }
}

/// The in-memory `FingerprintIndex`. Clones are handles on one index.
#[derive(Debug, Clone)]
pub struct MemoryFingerprintIndex {
    state: State<IndexState>,
    config: IndexConfig,
}

impl MemoryFingerprintIndex {
    pub fn new(config: IndexConfig) -> Self {
        Self {
            state: State::new(IndexState::default()),
            config,
        }
    }

    pub fn config(&self) -> &IndexConfig {
        &self.config
    }

    fn check_shards<'a>(
        &self,
        fingerprints: impl IntoIterator<Item = &'a PositionedFingerprint>,
    ) -> Result<(), IndexError> {
        match fingerprints
            .into_iter()
            .find(|positioned| !self.config.owns(positioned.fingerprint))
        {
            Some(positioned) => Err(IndexError::WrongShard {
                fingerprint: positioned.fingerprint,
            }),
            None => Ok(()),
        }
    }

    /// A copy of the data, for tests that check what the index retains.
    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> IndexState {
        self.state.read().clone()
    }
}

impl FingerprintIndex for MemoryFingerprintIndex {
    async fn insert(
        &mut self,
        span: &OriginatedSpan,
        fingerprints: &[PositionedFingerprint],
        now: Timestamp,
    ) -> Result<(), IndexError> {
        self.check_shards(fingerprints)?;
        let mut state = self.state.write();
        state.age_out(&self.config, now);
        let id = span.span().id;
        let kept: Vec<&PositionedFingerprint> = fingerprints
            .iter()
            .filter(|positioned| !state.boilerplate(&self.config, now, positioned.fingerprint))
            .collect();
        for positioned in kept {
            state
                .postings
                .entry(positioned.fingerprint)
                .or_default()
                .insert((id, positioned.offset));
        }
        Ok(())
    }

    async fn lookup(
        &self,
        fingerprints: &[PositionedFingerprint],
        now: Timestamp,
    ) -> Result<Vec<FingerprintHit>, IndexError> {
        self.check_shards(fingerprints)?;
        let state = self.state.read();
        // Postings first: a query with none needs no frequency count, so
        // large lookups (short-span token runs) stay cheap.
        let hits = fingerprints
            .iter()
            .filter_map(|query| {
                let postings = state.postings.get(&query.fingerprint)?;
                (!state.boilerplate(&self.config, now, query.fingerprint))
                    .then_some((query, postings))
            })
            .flat_map(|(query, postings)| {
                postings.iter().map(|(span, span_offset)| FingerprintHit {
                    fingerprint: query.fingerprint,
                    span: *span,
                    span_offset: *span_offset,
                    query_offset: query.offset,
                })
            })
            .collect();
        Ok(hits)
    }

    async fn frequency(&self, fingerprint: Fingerprint, now: Timestamp) -> Result<u64, IndexError> {
        Ok(self.state.read().frequency(&self.config, now, fingerprint))
    }

    async fn observe(
        &mut self,
        fingerprints: &[Fingerprint],
        at: Timestamp,
        now: Timestamp,
    ) -> Result<(), IndexError> {
        let mut state = self.state.write();
        state.age_out(&self.config, now);
        if self.config.counts(at, now) {
            state.observe(at, fingerprints.iter().copied().collect());
        }
        Ok(())
    }

    async fn evict(&mut self, spans: &[SpanId], now: Timestamp) -> Result<(), IndexError> {
        let evicted: BTreeSet<SpanId> = spans.iter().copied().collect();
        let mut state = self.state.write();
        state.age_out(&self.config, now);
        for postings in state.postings.values_mut() {
            postings.retain(|(span, _)| !evicted.contains(span));
        }
        state.postings.retain(|_, postings| !postings.is_empty());
        tracing::debug!(
            spans = evicted.len(),
            "spans evicted from the fingerprint index"
        );
        Ok(())
    }
}

/// `provenance.span-index.spans-as-recorded`,
/// `provenance.span-index.author-as-recorded`,
/// `provenance.span-index.keys-within-batch`: the first record of each
/// span, whatever was evicted since; unknown ids are left out.
impl SpanIndex for MemoryFingerprintIndex {
    async fn record(&mut self, span: &OriginatedSpan) -> Result<(), SpanIndexError> {
        self.state
            .write()
            .spans
            .entry(span.span().id)
            .or_insert_with(|| IndexedSpan::of(span));
        Ok(())
    }

    async fn spans(
        &self,
        ids: &IdBatch<SpanId>,
    ) -> Result<BTreeMap<SpanId, IndexedSpan>, SpanIndexError> {
        let state = self.state.read();
        Ok(ids
            .ids()
            .iter()
            .filter_map(|id| state.spans.get(id).map(|span| (*id, *span)))
            .collect())
    }
}
