//! Properties of series.

use std::collections::BTreeMap;
use std::num::NonZeroU64;

use crosstalk_memory::model::build::bucket_width;
use crosstalk_spec::aggregates::edge::{TopologyFilter, Weighting};
use crosstalk_spec::aggregates::series::{
    SeriesGrid, SeriesGrouping, SeriesGroups, SeriesStep, TopologySeries,
};
use crosstalk_spec::interfaces::l7_topology::EdgeStore;
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;

use super::{Scene, check, ensure, filter, fold, load, read, scene, window};
use crate::store::fold_route_key;
use crate::tests::support::World;

/// A grid of `steps` points of `per_step` buckets each, from bucket `start`.
fn grid(start: u64, steps: u64, per_step: u64) -> Option<SeriesGrid> {
    let step = SeriesStep::new(bucket_width(10), NonZeroU64::new(10 * per_step)?).ok()?;
    SeriesGrid::new(window(start * 10, start * 10 + steps * per_step * 10), step).ok()
}

fn grouping(n: u8) -> SeriesGrouping {
    match n % 4 {
        0 => SeriesGrouping::Total,
        1 => SeriesGrouping::Topic,
        2 => SeriesGrouping::RouteKind,
        _ => SeriesGrouping::Edge,
    }
}

fn weighting(bytes: bool) -> Weighting {
    if bytes {
        Weighting::MatchedBytes
    } else {
        Weighting::Transmissions
    }
}

async fn series(
    world: &World,
    grid: SeriesGrid,
    weighting: Weighting,
    grouping: SeriesGrouping,
    filter: &TopologyFilter,
) -> Result<TopologySeries, TestCaseError> {
    world
        .store
        .series(grid, weighting, grouping, filter)
        .await
        .map(|read| read.value)
        .map_err(|error| TestCaseError::fail(format!("series: {error:?}")))
}

/// Each series by a printable key.
fn keyed(series: &TopologySeries) -> BTreeMap<String, Vec<u64>> {
    match series.groups() {
        SeriesGroups::Total(values) => BTreeMap::from([("total".to_owned(), values.clone())]),
        SeriesGroups::ByTopic(all) => all
            .iter()
            .map(|one| (format!("{:?}", one.key), one.values.clone()))
            .collect(),
        SeriesGroups::ByRouteKind(all) => all
            .iter()
            .map(|one| (format!("{:?}", one.key), one.values.clone()))
            .collect(),
        SeriesGroups::ByEdge(all) => all
            .iter()
            .map(|one| {
                (
                    format!(
                        "{:?}",
                        (one.key.from, one.key.to, fold_route_key(&one.key.route))
                    ),
                    one.values.clone(),
                )
            })
            .collect(),
    }
}

type Shape = (Scene, u64, u64, u64, bool, u8, TopologyFilter);

fn shape() -> impl Strategy<Value = Shape> {
    (
        scene(),
        0u64..10,
        1u64..6,
        1u64..4,
        any::<bool>(),
        0u8..4,
        filter(),
    )
}

/// topology.series.total-matches-graph
#[test]
fn series_total_equals_graph_total() {
    check(
        "series_total_equals_graph_total",
        shape(),
        async |world: &mut World, (scene, start, steps, per_step, bytes, group, filter): &Shape| {
            load(world, scene).await?;
            let Some(grid) = grid(*start, *steps, *per_step) else {
                return Ok(());
            };
            let read_series =
                series(world, grid, weighting(*bytes), grouping(*group), filter).await?;
            let graph = read(world, grid.window(), weighting(*bytes), filter).await?;
            ensure(read_series.total() == graph.total(), || {
                format!("series {} vs graph {}", read_series.total(), graph.total())
            })
        },
    );
}

/// topology.series.edge-sums-match-graph
#[test]
fn edge_series_sum_to_graph_edge_stats() {
    check(
        "edge_series_sum_to_graph_edge_stats",
        shape(),
        async |world: &mut World, (scene, start, steps, per_step, bytes, _, filter): &Shape| {
            load(world, scene).await?;
            let Some(grid) = grid(*start, *steps, *per_step) else {
                return Ok(());
            };
            let weighting = weighting(*bytes);
            let by_edge = series(world, grid, weighting, SeriesGrouping::Edge, filter).await?;
            let graph = read(world, grid.window(), weighting, filter).await?;
            let SeriesGroups::ByEdge(all) = by_edge.groups() else {
                return Err(TestCaseError::fail("not grouped by edge"));
            };
            ensure(all.len() == graph.edges().len(), || {
                "one series per edge".to_owned()
            })?;
            for edge in graph.edges() {
                let one = all.iter().find(|one| {
                    one.key.from == edge.from
                        && one.key.to == edge.to
                        && one.key.route == edge.route
                });
                ensure(
                    one.is_some_and(|one| one.sum() == weighting.stat(edge.stats).get()),
                    || format!("{edge:?} has no matching series"),
                )?;
            }
            Ok(())
        },
    );
}

/// topology.series.concatenates
#[test]
fn series_concatenate_over_adjacent_grids() {
    check(
        "series_concatenate_over_adjacent_grids",
        (shape(), 1u64..4),
        async |world: &mut World, ((scene, start, steps, per_step, bytes, group, filter), more): &(Shape, u64)| {
            load(world, scene).await?;
            let (Some(first), Some(second), Some(union)) = (
                grid(*start, *steps, *per_step),
                grid(start + steps * per_step, *more, *per_step),
                grid(*start, steps + more, *per_step),
            ) else {
                return Ok(());
            };
            let (weighting, grouping) = (weighting(*bytes), grouping(*group));
            let a = keyed(&series(world, first, weighting, grouping, filter).await?);
            let b = keyed(&series(world, second, weighting, grouping, filter).await?);
            let u = keyed(&series(world, union, weighting, grouping, filter).await?);
            let mut keys: Vec<&String> = a.keys().chain(b.keys()).collect();
            keys.sort();
            keys.dedup();
            ensure(keys.len() == u.len(), || "the union has other keys".to_owned())?;
            for key in keys {
                let mut joined = a.get(key).cloned().unwrap_or_else(|| vec![0; *steps as usize]);
                joined.extend(b.get(key).cloned().unwrap_or_else(|| vec![0; *more as usize]));
                ensure(u.get(key) == Some(&joined), || format!("{key}: {:?} vs {joined:?}", u.get(key)))?;
            }
            Ok(())
        },
    );
}

/// topology.series.step-coarsening
#[test]
fn series_coarser_step_sums_finer_points() {
    check(
        "series_coarser_step_sums_finer_points",
        (shape(), 2u64..4),
        async |world: &mut World,
               ((scene, start, steps, per_step, bytes, group, filter), k): &(Shape, u64)| {
            load(world, scene).await?;
            let (Some(fine), Some(coarse)) = (
                grid(*start, steps * k, *per_step),
                grid(*start, *steps, per_step * k),
            ) else {
                return Ok(());
            };
            let (weighting, grouping) = (weighting(*bytes), grouping(*group));
            let f = keyed(&series(world, fine, weighting, grouping, filter).await?);
            let c = keyed(&series(world, coarse, weighting, grouping, filter).await?);
            ensure(f.len() == c.len(), || "other keys".to_owned())?;
            let k = usize::try_from(*k).map_err(|error| TestCaseError::fail(error.to_string()))?;
            for (key, values) in &f {
                let summed: Vec<u64> = values.chunks(k).map(|run| run.iter().sum()).collect();
                ensure(c.get(key) == Some(&summed), || {
                    format!("{key}: {:?} vs {summed:?}", c.get(key))
                })?;
            }
            Ok(())
        },
    );
}

/// topology.series.matches-fold-model: a total series' points and an
/// edge series' values are the fold over each point's window.
#[test]
fn series_matches_reference_fold() {
    check(
        "series_matches_reference_fold",
        shape(),
        async |world: &mut World, (scene, start, steps, per_step, bytes, _, filter): &Shape| {
            load(world, scene).await?;
            let Some(grid) = grid(*start, *steps, *per_step) else {
                return Ok(());
            };
            let weighting = weighting(*bytes);
            let stat = |edge: &(_, _, _, u64, u64)| if *bytes { edge.4 } else { edge.3 };
            let total = series(world, grid, weighting, SeriesGrouping::Total, filter).await?;
            let by_edge =
                keyed(&series(world, grid, weighting, SeriesGrouping::Edge, filter).await?);
            let mut expected_edges: BTreeMap<String, Vec<u64>> = BTreeMap::new();
            let mut expected_total = Vec::new();
            for (index, point) in grid.point_windows().enumerate() {
                let folded = fold(world, scene, point, filter);
                expected_total.push(folded.iter().map(stat).sum::<u64>());
                for edge in &folded {
                    let key = format!("{:?}", (edge.0, edge.1, fold_route_key(&edge.2)));
                    let values = expected_edges
                        .entry(key)
                        .or_insert_with(|| vec![0; grid.points().get() as usize]);
                    values[index] += stat(edge);
                }
            }
            ensure(keyed(&total).get("total") == Some(&expected_total), || {
                format!("{:?} vs {expected_total:?}", keyed(&total))
            })?;
            ensure(by_edge == expected_edges, || {
                format!("{by_edge:?} vs {expected_edges:?}")
            })
        },
    );
}
