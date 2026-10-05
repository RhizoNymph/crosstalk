//! Eval-owned in-memory stores behind the read seam ([`super::reads`]),
//! for detectors that keep their evidence in hand (the reference matcher).
//!
//! Each implements the spec's read trait, so predictions read them exactly
//! as they read the gateway's stores. They never suspend, so
//! [`super::reads::ready`] can run their reads without a runtime.

use std::collections::BTreeMap;

use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::resource::{Locator, Resource};
use crosstalk_spec::derived::provenance::span::OriginatedSpan;
use crosstalk_spec::ids::{AccessId, ChannelId, SpanId};
use crosstalk_spec::interfaces::l4_provenance::{IndexedSpan, SpanIndex, SpanIndexError};
use crosstalk_spec::interfaces::l5_flow::channels::{AccessReadError, AccessStore};

use super::reads::{ChannelResources, ReadError};

/// Span records by id: a `SpanIndex` that keeps each span's first record.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SpanTable {
    spans: BTreeMap<SpanId, IndexedSpan>,
}

impl SpanTable {
    /// Records `span` under `id` unless `id` is already recorded, as
    /// `SpanIndex::record` does.
    pub fn insert(&mut self, id: SpanId, span: IndexedSpan) {
        self.spans.entry(id).or_insert(span);
    }

    pub fn len(&self) -> usize {
        self.spans.len()
    }

    pub fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }
}

impl SpanIndex for SpanTable {
    async fn record(&mut self, span: &OriginatedSpan) -> Result<(), SpanIndexError> {
        self.insert(span.span().id, IndexedSpan::of(span));
        Ok(())
    }

    async fn spans(
        &self,
        ids: &IdBatch<SpanId>,
    ) -> Result<BTreeMap<SpanId, IndexedSpan>, SpanIndexError> {
        Ok(ids
            .ids()
            .iter()
            .filter_map(|id| self.spans.get(id).map(|span| (*id, *span)))
            .collect())
    }
}

/// Accesses with their resources by id: an `AccessStore`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AccessTable {
    accesses: BTreeMap<AccessId, (Access, Resource)>,
}

impl AccessTable {
    /// Records `access` of `resource`, keeping the first record of an id.
    pub fn insert(&mut self, access: Access, resource: Resource) {
        self.accesses.entry(access.id).or_insert((access, resource));
    }
}

impl AccessStore for AccessTable {
    async fn accesses(
        &self,
        ids: &IdBatch<AccessId>,
    ) -> Result<BTreeMap<AccessId, (Access, Resource)>, AccessReadError> {
        Ok(ids
            .ids()
            .iter()
            .filter_map(|id| self.accesses.get(id).map(|record| (*id, record.clone())))
            .collect())
    }
}

/// Each channel's resources: [`ChannelResources`] for a detector that names
/// its channels by the resources it routed through them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChannelTable {
    channels: BTreeMap<ChannelId, Vec<Locator>>,
}

impl ChannelTable {
    pub fn new(channels: BTreeMap<ChannelId, Vec<Locator>>) -> Self {
        Self { channels }
    }
}

impl ChannelResources for ChannelTable {
    async fn resources(
        &self,
        ids: &IdBatch<ChannelId>,
    ) -> Result<BTreeMap<ChannelId, Vec<Locator>>, ReadError> {
        Ok(ids
            .ids()
            .iter()
            .filter_map(|id| {
                self.channels
                    .get(id)
                    .map(|locators| (*id, locators.clone()))
            })
            .collect())
    }
}
