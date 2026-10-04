//! The surface service: the stores, the clock, the configuration, the ids
//! it mints, the key its cursors are sealed with and the live feed it
//! serves, and the helpers every query and action shares.

use std::sync::Arc;

use crosstalk_spec::aggregates::series::BucketWidth;
use crosstalk_spec::aliases::Aliases;
use crosstalk_spec::ids::{AgentId, ChannelId, EntityId, SeededRandom};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_spec::interfaces::l7_topology::EdgeStore;
use crosstalk_spec::interfaces::l8_surface::{Caller, InputError, Permission, QueryError};
use crosstalk_spec::paging::{Cursor, Page, PageSize};
use crosstalk_spec::support::{Clock, NonEmpty, TimeWindow, Timestamp};

use crate::config::SurfaceConfig;
use crate::cursor::{CursorKey, SearchModels};
use crate::ids::IdMinter;
use crate::live::FeedHandle;
use crate::stores::SurfaceStores;

/// The L8 surface over `S`: implements `QueryApi`, `OperatorActions` and
/// `LiveFeed`. Cheap to share behind an `Arc`; every method takes `&self`.
pub struct Surface<S> {
    pub(crate) stores: S,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) config: SurfaceConfig,
    pub(crate) ids: IdMinter,
    pub(crate) cursors: CursorKey,
    pub(crate) search_models: SearchModels,
    pub(crate) feed: FeedHandle,
}

impl<S> std::fmt::Debug for Surface<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Surface")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl<S: SurfaceStores> Surface<S> {
    /// The surface over `stores`. `clock` stamps every acceptance time,
    /// audit entry and `present`; `random` seeds the ids the surface mints
    /// and the key its cursors are sealed with (`SeededRandom::new` in tests
    /// and simulations, `SeededRandom::from_entropy` in a running gateway);
    /// `feed` is the live feed `subscribe` attaches to
    /// ([`crate::live::FeedWriter::spawn`]).
    pub fn new(
        stores: S,
        clock: Arc<dyn Clock>,
        config: SurfaceConfig,
        mut random: SeededRandom,
        feed: FeedHandle,
    ) -> Self {
        let cursors = CursorKey::draw(&mut random);
        let ids = IdMinter::new(Arc::clone(&clock), random);
        Self {
            stores,
            clock,
            config,
            ids,
            cursors,
            search_models: SearchModels::default(),
            feed,
        }
    }

    /// The stores the surface was built over.
    pub fn stores(&self) -> &S {
        &self.stores
    }

    /// The surface's configuration.
    pub fn config(&self) -> &SurfaceConfig {
        &self.config
    }

    /// The live feed this surface subscribes callers to.
    pub fn feed(&self) -> &FeedHandle {
        &self.feed
    }

    /// The time a request is accepted at.
    pub(crate) fn now(&self) -> Timestamp {
        self.clock.now()
    }

    /// A fresh id, or `Store` past the year 10889.
    pub(crate) fn mint<I: EntityId>(&self) -> Result<I, QueryError> {
        self.ids.mint().map_err(|error| QueryError::Store {
            reason: error.to_string(),
        })
    }

    /// The canonical form of ids at the time of the call, through L3's and
    /// L5's directories.
    pub(crate) fn aliases(&self) -> impl Aliases + Copy + '_ {
        let agents = self.stores.agents();
        let channels = self.stores.channels();
        crosstalk_spec::aliases::Resolve {
            agents: move |id: AgentId| AgentDirectory::canonical(agents, id),
            channels: move |id: ChannelId| ChannelDirectory::canonical(channels, id),
        }
    }

    /// L7's bucket width.
    pub(crate) fn bucket_width(&self) -> BucketWidth {
        self.stores.edges().bucket_width()
    }

    /// `InvalidInput(UnalignedWindow)` for a window off bucket boundaries.
    pub(crate) fn aligned(&self, window: TimeWindow) -> Result<TimeWindow, QueryError> {
        let width = self.bucket_width();
        if width.is_boundary(window.start()) && width.is_boundary(window.end()) {
            Ok(window)
        } else {
            Err(QueryError::InvalidInput(InputError::UnalignedWindow))
        }
    }

    /// The bucket-aligned window that stands for "all time": from the epoch
    /// to the last bucket boundary before year 10000.
    pub(crate) fn all_time(&self) -> Result<TimeWindow, QueryError> {
        all_time(self.bucket_width())
    }
}

/// `Forbidden { missing }` unless `caller` holds `permission`. Every query
/// calls it before reading anything.
pub(crate) fn require(caller: &Caller, permission: Permission) -> Result<(), QueryError> {
    if caller.has(permission) {
        Ok(())
    } else {
        Err(QueryError::Forbidden {
            missing: permission,
        })
    }
}

/// The bucket-aligned window from the epoch to the last bucket boundary a
/// timestamp's wire text can hold (the end of year 9999), so stores that
/// bind cursors to their window's JSON can encode it.
pub(crate) fn all_time(width: BucketWidth) -> Result<TimeWindow, QueryError> {
    let width = width.as_micros().get();
    let last = crosstalk_spec::wire::time::MAX.as_micros();
    let end = last - last % width;
    TimeWindow::new(Timestamp::from_micros(0), Timestamp::from_micros(end)).map_err(|_| {
        QueryError::Store {
            reason: "the bucket width leaves no whole bucket".to_owned(),
        }
    })
}

/// The largest page size, for the full traversals the surface makes on its
/// own behalf (counts, names, the overview's queues).
pub(crate) fn largest_page() -> Result<PageSize, QueryError> {
    PageSize::new(PageSize::MAX).map_err(|error| QueryError::Store {
        reason: format!("largest page size refused: {error:?}"),
    })
}

/// A page of `items` in a list's order, with `next` when more follow:
/// `Page::more` for a non-empty page with a cursor, `Page::last` otherwise.
pub(crate) fn page_of<T, L>(
    size: PageSize,
    items: Vec<T>,
    next: Option<Cursor<L>>,
) -> Result<Page<T, L>, QueryError> {
    let overflow = |error: crosstalk_spec::paging::PageOverflow| QueryError::Store {
        reason: format!(
            "page of {} items over its size {}",
            error.got,
            error.size.get()
        ),
    };
    match (next, NonEmpty::from_vec(items)) {
        (Some(next), Some(items)) => Page::more(size, items, next).map_err(overflow),
        (_, Some(items)) => Page::last(size, items.into_vec()).map_err(overflow),
        (_, None) => Page::last(size, Vec::new()).map_err(overflow),
    }
}
