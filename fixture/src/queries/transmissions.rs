//! Transmission rows: the transmissions an edge counts
//! (`edge_transmissions`), a selection's rows by id (`transmissions_by_id`)
//! and the detection-quality tally over them.

use crosstalk_spec::aggregates::edge::{EdgeSelector, EdgeTransmission, EdgeTransmissionPage};
use crosstalk_spec::aggregates::filter::{TopicVersionSelector, TopologyFilter};
use crosstalk_spec::aggregates::quality::DetectionQuality;
use crosstalk_spec::aggregates::topic::{Assignment, TopicModelVersion};
use crosstalk_spec::aggregates::watermark::Watermarked;
use crosstalk_spec::interfaces::l8_surface::summary::{
    TopicUnder, TransmissionPage, TransmissionSelection, TransmissionSummary,
};
use crosstalk_spec::paging::{EdgeTransmissionList, PageRequest, TransmissionList};
use crosstalk_spec::support::TimeWindow;

use crate::Result;
use crate::world::TxRecord;

use super::Ctx;
use super::graph::watermarked;
use super::linked::{Linked, pinned_version, resolve_selector};
use super::page::{self, Key, newest_first};

/// The list names cursors carry (with the pinned version).
const EDGE: &str = "edge";
const BY_ID: &str = "byid";

/// `record`'s topic under `version`, as a summary shows it.
pub fn topic_under(record: &TxRecord, version: TopicModelVersion) -> TopicUnder {
    match record.assignment(version) {
        Some(Assignment::Topic { topic, .. }) => TopicUnder::Topic(topic),
        Some(Assignment::Outlier) => TopicUnder::Outlier,
        None => TopicUnder::Unassigned,
    }
}

/// The row of `record` ([`TransmissionSummary::of`]): agents and channel
/// resolved as of this read, the current verdict, the topic under
/// `version`.
pub fn summary(ctx: &Ctx, record: &TxRecord, version: TopicModelVersion) -> TransmissionSummary {
    TransmissionSummary::of(
        &record.transmission,
        ctx.aliases(),
        |id| ctx.verdict(id),
        |_| topic_under(record, version),
    )
}

/// One row per stored transmission of `selection`, newest id first, topics
/// under the version `selector` resolves to on the first page and the
/// cursor pins afterwards. Unknown ids are left out.
pub fn by_id(
    ctx: &Ctx,
    selection: &TransmissionSelection,
    selector: TopicVersionSelector,
    page: &PageRequest<TransmissionList>,
) -> Result<TransmissionPage> {
    let version = match page::pinned(BY_ID, page)? {
        Some(pinned) => pinned_version(ctx.state, pinned)?,
        None => resolve_selector(ctx.world, ctx.state, selector)?,
    };
    let items: Vec<(Key, TransmissionSummary)> = selection
        .ids()
        .iter()
        .filter_map(|id| ctx.world.tx(*id))
        .map(|record| {
            let key = (0, u128::MAX - record.transmission.id.as_ulid());
            (key, summary(ctx, record, version))
        })
        .collect();
    let page = page::paginate(
        &page::versioned(BY_ID, version),
        page::digest(&(selection, selector)),
        items,
        page,
    )?;
    Ok(TransmissionPage {
        topic_version: version,
        page,
    })
}

/// What `topology` counts into the edge for the window and filter, newest
/// confirmation first, under the version the first page resolved. The
/// selector's agents and channel resolve as of this read; an edge whose
/// ends resolve to one agent counts nothing.
pub fn edge(
    ctx: &Ctx,
    selector: &EdgeSelector,
    window: TimeWindow,
    filter: &TopologyFilter,
    page: &PageRequest<EdgeTransmissionList>,
) -> Result<Watermarked<EdgeTransmissionPage>> {
    let linked = Linked::paged(ctx, Some(window), filter, page::pinned(EDGE, page)?)?;
    let from = ctx.agent(selector.from());
    let to = ctx.agent(selector.to());
    let route = ctx.route(selector.route());
    let items: Vec<(Key, EdgeTransmission)> = if from == to {
        Vec::new()
    } else {
        linked
            .counted()
            .into_iter()
            .filter(|c| c.from == from && c.to == to && c.route == route)
            .map(|c| {
                let id = c.record.transmission.id;
                let row = EdgeTransmission {
                    transmission: id,
                    confirmed_at: c.at,
                    matched_bytes: c.matched_bytes,
                    topic: c.topic,
                };
                (newest_first(c.at, id.as_ulid()), row)
            })
            .collect()
    };
    let page = page::paginate(
        &page::versioned(EDGE, linked.version),
        page::digest(&(selector, window, filter)),
        items,
        page,
    )?;
    Ok(watermarked(EdgeTransmissionPage {
        topic_version: linked.version,
        page,
    }))
}

/// [`DetectionQuality::tally`] over every stored transmission with its
/// current verdict, read under one lock.
pub fn quality(ctx: &Ctx, window: TimeWindow) -> DetectionQuality {
    DetectionQuality::tally(
        window,
        ctx.world
            .transmissions
            .iter()
            .map(|record| (&record.transmission, ctx.verdict(record.transmission.id))),
    )
}
