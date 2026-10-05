//! A channel's transmissions (`QueryApi::channel_transmissions`): the
//! cross-agent transmissions whose route resolves to the channel's
//! canonical channel, newest opened first, each as
//! `ChannelTransmission::of` builds it at this read (so one whose agents
//! have merged into one is not listed), topics under the version the first
//! page resolves and the cursor pins.

use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::ChannelId;
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::interfaces::l8_surface::channel_traffic::ChannelTransmission;
use crosstalk_spec::interfaces::l8_surface::channel_traffic::ChannelTransmissionFilter;
use crosstalk_spec::interfaces::l8_surface::channel_traffic::ChannelTransmissionPage;
use crosstalk_spec::paging::ChannelTransmissionList;
use crosstalk_spec::paging::PageRequest;

use crate::backend::Result;

use super::super::Ctx;
use super::super::linked::{pinned_version, resolve_selector};
use super::super::page::{self, newest_first};
use super::super::transmissions::topic_under;

/// The list name cursors carry (with the pinned version).
const LIST: &str = "chantx";

/// One page of `channel`'s transmissions that `filter` keeps. Unknown
/// channel is `NotFound`.
pub fn page(
    ctx: &Ctx,
    channel: ChannelId,
    filter: &ChannelTransmissionFilter,
    selector: TopicVersionSelector,
    request: &PageRequest<ChannelTransmissionList>,
) -> Result<ChannelTransmissionPage> {
    if !ctx.state.channels.contains_key(&channel) {
        return Err(QueryError::NotFound);
    }
    let canonical = ctx.channel(channel);
    let version = match page::pinned(LIST, request)? {
        Some(pinned) => pinned_version(ctx.state, pinned)?,
        None => resolve_selector(ctx.world, ctx.state, selector)?,
    };
    let items = ctx
        .world
        .transmissions
        .iter()
        .filter(|record| {
            matches!(record.transmission.route, Route::Channel(stored) if ctx.channel(stored) == canonical)
        })
        .filter_map(|record| {
            let row = ChannelTransmission::of(
                &record.transmission,
                ctx.aliases(),
                |id| ctx.verdict(id),
                |_| topic_under(record, version),
            )?;
            filter.matches(&row).then(|| {
                let key = newest_first(
                    record.transmission.opened_at,
                    record.transmission.id.as_ulid(),
                );
                (key, row)
            })
        })
        .collect();
    let listed = page::paginate(
        &page::versioned(LIST, version),
        page::digest(&(canonical, filter, selector)),
        items,
        request,
    )?;
    Ok(ChannelTransmissionPage {
        channel: canonical,
        topic_version: version,
        page: listed,
    })
}
