//! The read seam: what predictions need from a detector's stores, read
//! through the spec's read traits.
//!
//! A `Transmission` names its evidence by id: a `ContentMatch` its origin
//! span, a `CoAccess` its write and read accesses, a channel route its
//! channel. [`Resolved::gather`] reads every id a detection's transmissions
//! name, in bounded batches, through
//!
//! - `SpanIndex::spans(&IdBatch<SpanId>)` → `IndexedSpan { exchange, author,
//!   location }` (L4),
//! - `AccessStore::accesses(&IdBatch<AccessId>)` → `(Access, Resource)`
//!   (L5),
//! - [`ChannelResources::resources`] → each channel's canonical resources,
//!   which [`RegistryResources`] reads from the spec's `ChannelReads::channel`
//!   and `ChannelRegistry::resource_use` (L5),
//!
//! into one in-memory snapshot that `to_bench::predictions::rows` then
//! reads synchronously. The same code path serves crosstalk-memory's
//! `MemoryFingerprintIndex` and `MemoryChannels` (the scripted backend in
//! the tests) and the real gateway's stores (`detect::live`).

use std::collections::{BTreeMap, BTreeSet};

use crosstalk_spec::batch::{IdBatch, TooManyIds};
use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::resource::{Locator, Resource};
use crosstalk_spec::derived::flow::transmission::{Route, Transmission};
use crosstalk_spec::ids::{AccessId, ChannelId, SpanId};
use crosstalk_spec::interfaces::l4_provenance::{IndexedSpan, SpanIndex, SpanIndexError};
use crosstalk_spec::interfaces::l5_flow::channels::{AccessReadError, AccessStore, ChannelReads};
use crosstalk_spec::interfaces::l5_flow::{ChannelRegistry, RegistryError};
use crosstalk_spec::paging::{PageRequest, PageSize};
use crosstalk_spec::support::TimeWindow;

/// Why a read through the seam failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReadError {
    #[error("span index: {0:?}")]
    Spans(SpanIndexError),
    #[error("access store: {0:?}")]
    Accesses(AccessReadError),
    #[error("channel registry: {0:?}")]
    Channels(RegistryError),
    #[error("a batch of {got} ids is over the cap of {max}")]
    Batch { max: usize, got: usize },
}

impl From<TooManyIds> for ReadError {
    fn from(error: TooManyIds) -> Self {
        Self::Batch {
            max: error.max,
            got: error.got,
        }
    }
}

/// The canonical resources of channels, by batch: what channel-route
/// alignment compares a label's resource with.
pub trait ChannelResources {
    /// The resources of each channel in `ids` (a superseded channel's are its
    /// canonical channel's). An unknown id is absent from the map.
    fn resources(
        &self,
        ids: &IdBatch<ChannelId>,
    ) -> impl Future<Output = Result<BTreeMap<ChannelId, Vec<Locator>>, ReadError>> + Send;
}

/// [`ChannelResources`] over the spec's registry: `ChannelReads::channel`
/// says whether a channel exists, and `ChannelRegistry::resource_use` over
/// `window` lists its canonical channel's resources (its own and those of
/// every channel it superseded), page by page. `C` is a registry handle
/// (the reference stores' clones share one registry).
#[derive(Debug, Clone)]
pub struct RegistryResources<C> {
    registry: C,
    window: TimeWindow,
}

impl<C> RegistryResources<C> {
    /// `window` must cover every access of the world (every resource in use
    /// is listed only if accessed within it).
    pub fn new(registry: C, window: TimeWindow) -> Self {
        Self { registry, window }
    }
}

impl<C> ChannelResources for RegistryResources<C>
where
    C: ChannelReads + ChannelRegistry + Sync,
{
    async fn resources(
        &self,
        ids: &IdBatch<ChannelId>,
    ) -> Result<BTreeMap<ChannelId, Vec<Locator>>, ReadError> {
        let size = PageSize::new(PageSize::MAX).map_err(|_| ReadError::Batch {
            max: usize::from(PageSize::MAX),
            got: usize::from(PageSize::MAX),
        })?;
        let mut out = BTreeMap::new();
        for &id in ids.ids() {
            if self
                .registry
                .channel(id)
                .await
                .map_err(ReadError::Channels)?
                .is_none()
            {
                continue;
            }
            let mut locators = Vec::new();
            let mut after = None;
            loop {
                let page = self
                    .registry
                    .resource_use(id, self.window, &PageRequest { size, after })
                    .await
                    .map_err(ReadError::Channels)?;
                let (items, next) = page.page.into_parts();
                locators.extend(
                    items
                        .into_iter()
                        .map(|used| used.resource().locator.clone()),
                );
                match next {
                    Some(cursor) => after = Some(cursor),
                    None => break,
                }
            }
            out.insert(id, locators);
        }
        Ok(out)
    }
}

/// The three stores a detection's evidence is read from.
#[derive(Debug, Clone, Copy)]
pub struct Reads<'a, S, A, C> {
    pub spans: &'a S,
    pub accesses: &'a A,
    pub channels: &'a C,
}

/// Every span, access and channel a detection's transmissions name, read
/// once through the seam. Ids a store does not know are absent.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Resolved {
    spans: BTreeMap<SpanId, IndexedSpan>,
    accesses: BTreeMap<AccessId, (Access, Resource)>,
    channels: BTreeMap<ChannelId, Vec<Locator>>,
}

/// `ids` cut into batches the spec's reads take.
fn batches<T: Ord + Copy>(ids: &BTreeSet<T>) -> Result<Vec<IdBatch<T>>, ReadError> {
    let all: Vec<T> = ids.iter().copied().collect();
    all.chunks(IdBatch::<T>::MAX)
        .map(|chunk| IdBatch::new(chunk.iter().copied()).map_err(ReadError::from))
        .collect()
}

impl Resolved {
    /// Reads what `transmissions` name: the origin span of every content
    /// match, both accesses of every co-access record, the resources of
    /// every channel route.
    pub async fn gather<S, A, C>(
        transmissions: &[Transmission],
        reads: Reads<'_, S, A, C>,
    ) -> Result<Self, ReadError>
    where
        S: SpanIndex + Sync,
        A: AccessStore + Sync,
        C: ChannelResources + Sync,
    {
        let mut span_ids = BTreeSet::new();
        let mut access_ids = BTreeSet::new();
        let mut channel_ids = BTreeSet::new();
        for transmission in transmissions {
            if let Some(confirmed) = transmission.state.confirmed() {
                span_ids.extend(confirmed.content().iter().map(|content| content.origin()));
            }
            for co_access in transmission.state.co_accesses() {
                access_ids.insert(co_access.write());
                access_ids.insert(co_access.read());
            }
            if let Route::Channel(channel) = transmission.route {
                channel_ids.insert(channel);
            }
        }
        let mut resolved = Self::default();
        for batch in batches(&span_ids)? {
            let spans = reads.spans.spans(&batch).await.map_err(ReadError::Spans)?;
            resolved.spans.extend(spans);
        }
        for batch in batches(&access_ids)? {
            let accesses = reads
                .accesses
                .accesses(&batch)
                .await
                .map_err(ReadError::Accesses)?;
            resolved.accesses.extend(accesses);
        }
        for batch in batches(&channel_ids)? {
            resolved
                .channels
                .extend(reads.channels.resources(&batch).await?);
        }
        Ok(resolved)
    }

    pub fn span(&self, id: SpanId) -> Option<&IndexedSpan> {
        self.spans.get(&id)
    }

    pub fn access(&self, id: AccessId) -> Option<&(Access, Resource)> {
        self.accesses.get(&id)
    }

    /// A channel's resources; `None` for a channel the registry did not
    /// know.
    pub fn channel(&self, id: ChannelId) -> Option<&[Locator]> {
        self.channels.get(&id).map(Vec::as_slice)
    }
}
