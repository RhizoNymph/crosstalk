//! Series over the edge table, as `EdgeStore::series` defines them: the
//! graph's count cut into the grid's steps, so for the same window,
//! weighting, filter and version every value sums to the graph's total and
//! each edge series to its edge's stat.

use std::collections::BTreeMap;

use crosstalk_spec::aggregates::edge::{RouteKind, TopologyFilter, Weighting};
use crosstalk_spec::aggregates::series::{
    Series, SeriesEdge, SeriesGrid, SeriesGrouping, SeriesGroups, TopologySeries,
};
use crosstalk_spec::aggregates::watermark::Watermarked;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::{AgentId, TopicId};
use crosstalk_spec::interfaces::l7_topology::EdgeQueryError;

use crate::backend::Result;
use crate::backend::fixture::clock::BUCKET;

use super::graph::{store_error, watermarked};
use super::linked::{Counted, Linked};
use super::{Ctx, route_key};

/// Point index and stat of each counted transmission.
fn points<'a>(
    grid: SeriesGrid,
    weighting: Weighting,
    counted: &'a [Counted<'a>],
) -> impl Iterator<Item = (usize, u64, &'a Counted<'a>)> + 'a {
    let start = grid.window().start().as_micros();
    let step = grid.step().as_micros().get();
    counted.iter().filter_map(move |c| {
        let index = usize::try_from((c.at.as_micros().checked_sub(start)?) / step).ok()?;
        let value = match weighting {
            Weighting::Transmissions => 1,
            Weighting::MatchedBytes => c.matched_bytes.get(),
        };
        Some((index, value, c))
    })
}

fn add(values: &mut [u64], index: usize, value: u64) -> Result<()> {
    let slot = values
        .get_mut(index)
        .ok_or_else(|| store_error("series point outside the grid", index))?;
    *slot = slot.saturating_add(value);
    Ok(())
}

/// One series per key, in key order.
fn grouped<K, O: Ord>(
    grid: SeriesGrid,
    weighting: Weighting,
    counted: &[Counted],
    key: impl Fn(&Counted) -> (O, K),
) -> Result<Vec<Series<K>>> {
    let width = grid.points().get() as usize;
    let mut series: BTreeMap<O, (K, Vec<u64>)> = BTreeMap::new();
    for (index, value, c) in points(grid, weighting, counted) {
        let (order, key) = key(c);
        let entry = series.entry(order).or_insert_with(|| (key, vec![0; width]));
        add(&mut entry.1, index, value)?;
    }
    Ok(series
        .into_values()
        .map(|(key, values)| Series { key, values })
        .collect())
}

fn route_kind_order(kind: RouteKind) -> u8 {
    match kind {
        RouteKind::Channel => 0,
        RouteKind::Delegation => 1,
        RouteKind::Direct => 2,
        RouteKind::Unobserved => 3,
    }
}

/// A grid for another bucket width is refused before anything is read.
pub fn series(
    ctx: &Ctx,
    grid: SeriesGrid,
    weighting: Weighting,
    grouping: SeriesGrouping,
    filter: &TopologyFilter,
) -> Result<Watermarked<TopologySeries>> {
    if grid.step().bucket() != BUCKET {
        return Err(EdgeQueryError::BucketWidthMismatch {
            store: BUCKET,
            grid: grid.step().bucket(),
        }
        .into());
    }
    let linked = Linked::new(ctx, grid.window(), filter)?;
    let counted = linked.counted();
    let groups = match grouping {
        SeriesGrouping::Total => {
            let mut values = vec![0; grid.points().get() as usize];
            for (index, value, _) in points(grid, weighting, &counted) {
                add(&mut values, index, value)?;
            }
            SeriesGroups::Total(values)
        }
        SeriesGrouping::Topic => SeriesGroups::ByTopic(grouped(
            grid,
            weighting,
            &counted,
            |c| -> (Option<TopicId>, Option<TopicId>) { (c.topic, c.topic) },
        )?),
        SeriesGrouping::RouteKind => {
            SeriesGroups::ByRouteKind(grouped(grid, weighting, &counted, |c| {
                let kind = RouteKind::from(&c.route);
                (route_kind_order(kind), kind)
            })?)
        }
        SeriesGrouping::Edge => SeriesGroups::ByEdge(grouped(grid, weighting, &counted, |c| {
            let order: (AgentId, AgentId, (u8, u128, String)) = (c.from, c.to, route_key(&c.route));
            let route: Route = c.route.clone();
            (
                order,
                SeriesEdge {
                    from: c.from,
                    to: c.to,
                    route,
                },
            )
        })?),
    };
    TopologySeries::new(grid, weighting, linked.version, groups)
        .map(watermarked)
        .map_err(|e| store_error("series", e))
}
