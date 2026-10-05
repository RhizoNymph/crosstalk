//! Channel rows (`QueryApi::channels` and `channel`): the stored channel,
//! its seed resource and its standing. A row in force carries its
//! cross-agent traffic over all time (the read's `CrossTraffic`, merges
//! resolved), from which its listing follows, counts writers and readers as
//! `ChannelCounts::tally` of its full resource use in the window, and
//! transmissions as `ChannelCounts::routed` of the topology graph for the
//! same window under `TopologyFilter::default()`, so the overview's active
//! channels are the rows with traffic. A superseded row carries its
//! supersession and no counts. `channels` lists the rows its filter
//! matches (`ChannelFilter::matches`: never a hidden one); `channel`
//! returns any stored channel's row, a hidden one included.

use std::collections::HashMap;

use crosstalk_spec::aggregates::edge::{TopologyFilter, Weighting};
use crosstalk_spec::aggregates::watermark::Watermarked;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::ChannelId;
use crosstalk_spec::interfaces::l8_surface::channels::ChannelRow;
use crosstalk_spec::interfaces::l8_surface::channels::ChannelStanding;
use crosstalk_spec::interfaces::l8_surface::channels::{
    ChannelActivity, ChannelCounts, SupersededInto,
};
use crosstalk_spec::interfaces::l8_surface::lists::ChannelFilter;
use crosstalk_spec::paging::{ChannelList, Page, PageRequest};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use crate::backend::Result;
use crate::backend::fixture::clock;
use crate::backend::fixture::store::ChannelRecord;
use crate::backend::fixture::world::confirmed;

use super::super::Ctx;
use super::super::graph::{self, aligned, store_error, watermarked};
use super::super::page::{self, newest_first};
use super::resources;

/// What every row of one read shares: its window and the transmissions the
/// topology graph counts on each channel in it.
struct Counting {
    window: TimeWindow,
    routed: HashMap<ChannelId, u64>,
}

impl Counting {
    /// `window` (all time when `None`) checked for bucket boundaries, and
    /// the default-filter graph's channel counts over it.
    fn new(ctx: &Ctx, window: Option<TimeWindow>) -> Result<Self> {
        let window = match window {
            Some(window) => {
                aligned(window)?;
                window
            }
            None => clock::all_time().map_err(|e| store_error("all-time window", e))?,
        };
        let graph = graph::graph(
            ctx,
            window,
            Weighting::Transmissions,
            &TopologyFilter::default(),
        )?;
        Ok(Self {
            window,
            routed: ChannelCounts::routed(&graph),
        })
    }
}

/// The latest access to a resource of `canonical` (or of a channel it
/// superseded) or confirmation of a transmission routed through one, over
/// all time.
fn last_activity(ctx: &Ctx, canonical: ChannelId) -> Option<Timestamp> {
    let on = |stored: ChannelId| ctx.channel(stored) == canonical;
    let accessed = ctx
        .world
        .accesses
        .iter()
        .filter(|a| {
            ctx.world
                .resource_channel
                .get(&a.resource)
                .is_some_and(|stored| on(*stored))
        })
        .map(|a| a.at)
        .max();
    let confirmed = ctx
        .world
        .transmissions
        .iter()
        .filter(|t| matches!(t.transmission.route, Route::Channel(c) if on(c)))
        .filter_map(|t| confirmed(&t.transmission.state).map(|c| c.at()))
        .max();
    accessed.max(confirmed)
}

/// The row for `record`.
fn row(ctx: &Ctx, record: &ChannelRecord, counting: &Counting) -> Result<ChannelRow> {
    let channel = record.channel();
    let standing = match channel.origin.supersession() {
        Some(supersession) => {
            let superseding = ctx
                .state
                .channels
                .get(&supersession.by)
                .ok_or_else(|| store_error("unknown superseding channel", supersession.by))?;
            let into = SupersededInto::of(supersession, superseding.channel())
                .map_err(|e| store_error("supersession", e))?;
            ChannelStanding::Superseded(into)
        }
        None => {
            let activity = match last_activity(ctx, channel.id) {
                None => ChannelActivity::Never,
                Some(last) => {
                    let uses = resources::uses(ctx, channel.id, counting.window)?;
                    let transmissions = counting.routed.get(&channel.id).copied().unwrap_or(0);
                    ChannelActivity::Seen {
                        last,
                        counts: ChannelCounts::tally(&uses, transmissions),
                    }
                }
            };
            ChannelStanding::InForce {
                traffic: ctx.traffic(channel.id),
                activity,
            }
        }
    };
    let seed = channel
        .origin
        .seed()
        .map(|seed| {
            ctx.world
                .resource(seed.resource)
                .cloned()
                .ok_or_else(|| store_error("unknown seed resource", seed.resource))
        })
        .transpose()?;
    ChannelRow::new(channel.clone(), seed, standing).map_err(|e| store_error("channel row", e))
}

/// `channels`: the channels `filter` matches, newest first, counted in its
/// window. An unaligned window is `InvalidInput(UnalignedWindow)`.
pub fn list(
    ctx: &Ctx,
    filter: &ChannelFilter,
    request: &PageRequest<ChannelList>,
) -> Result<Watermarked<Page<ChannelRow, ChannelList>>> {
    let counting = Counting::new(ctx, filter.window)?;
    let mut items = Vec::new();
    for record in ctx.state.channels.values() {
        let row = row(ctx, record, &counting)?;
        if filter.matches(&row) {
            items.push((
                newest_first(row.created_at(), row.channel().id.as_ulid()),
                row,
            ));
        }
    }
    page::paginate("channels", page::digest(filter), items, request).map(watermarked)
}

/// Every stored channel's row, counted over all time: what the overview's
/// queues tally.
pub fn every(ctx: &Ctx) -> Result<Vec<ChannelRow>> {
    let counting = Counting::new(ctx, None)?;
    ctx.state
        .channels
        .values()
        .map(|record| row(ctx, record, &counting))
        .collect()
}

/// `channel`: the channel stored under `id`, a superseded one with its own
/// record and supersession. `None` for an unknown channel.
pub fn one(
    ctx: &Ctx,
    id: ChannelId,
    window: Option<TimeWindow>,
) -> Result<Option<Watermarked<ChannelRow>>> {
    let counting = Counting::new(ctx, window)?;
    ctx.state
        .channels
        .get(&id)
        .map(|record| row(ctx, record, &counting).map(watermarked))
        .transpose()
}
