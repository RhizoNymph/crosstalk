//! The store's reads. Each runs in one `REPEATABLE READ READ ONLY`
//! transaction: the watermark is read first, then the dropped versions and
//! the rows, all from one snapshot, so a read sees every bucket a version
//! had or none of a dropped one, and its data counts everything applied to
//! a bucket the watermark it reports finalizes.

use std::collections::{BTreeMap, BTreeSet};

use crosstalk_spec::aggregates::access::{BipartiteGraph, BipartiteParts};
use crosstalk_spec::aggregates::agents::AgentTraffic;
use crosstalk_spec::aggregates::edge::{
    EdgeTotals, TopologyFilter, TopologyGraph, TopologyGraphParts, Weighting,
};
use crosstalk_spec::aggregates::filter::{FalseDetections, VersionUnavailable};
use crosstalk_spec::aggregates::node::GraphNode;
use crosstalk_spec::aggregates::series::{SeriesGrid, SeriesGrouping, TopologySeries};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::watermark::{Watermark, Watermarked};
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::AgentId;
use crosstalk_spec::interfaces::l7_topology::EdgeQueryError;
use crosstalk_spec::support::TimeWindow;
use sqlx::{PgConnection, Postgres, Row, Transaction};

use super::error::{DbError, ReadResult};
use super::fold::{self, AccessRow, EdgeRow, Rows};
use super::{PgEdgeStore, aligned};
use crate::codec;
use crate::env::TopologyEnv;

/// A read's snapshot.
pub(super) type Snapshot = Transaction<'static, Postgres>;

impl<V> PgEdgeStore<V> {
    pub(super) async fn snapshot(&self) -> Result<Snapshot, DbError> {
        Ok(self
            .pool
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await?)
    }
}

pub(super) async fn read_watermark(conn: &mut PgConnection) -> Result<Watermark, DbError> {
    let micros: i64 = sqlx::query_scalar("SELECT watermark_micros FROM topology.state")
        .fetch_one(conn)
        .await?;
    Ok(Watermark(codec::timestamp("watermark", micros)?))
}

/// The watermark and the dropped versions, in one round trip: what every
/// read starts with.
pub(super) async fn read_head(
    conn: &mut PgConnection,
) -> Result<(Watermark, BTreeSet<TopicModelVersion>), DbError> {
    let row = sqlx::query(
        "SELECT s.watermark_micros, \
         ARRAY(SELECT version FROM topology.versions WHERE dropped) AS dropped \
         FROM topology.state s",
    )
    .fetch_one(conn)
    .await?;
    let watermark = Watermark(codec::timestamp(
        "watermark",
        row.try_get("watermark_micros")?,
    )?);
    let dropped: Vec<i64> = row.try_get("dropped")?;
    let dropped = dropped
        .into_iter()
        .map(|version| codec::stored_version(version).map_err(DbError::from))
        .collect::<Result<_, _>>()?;
    Ok((watermark, dropped))
}

/// The version a read computes under: the selector resolved against the
/// catalog's history with every version not dropped here retained, and the
/// filter's topics checked against it.
pub(super) async fn resolve_version<V: TopologyEnv>(
    env: &V,
    dropped: &BTreeSet<TopicModelVersion>,
    filter: &TopologyFilter,
) -> ReadResult<TopicModelVersion> {
    let history = env.history().await?;
    let version = filter
        .topic_version
        .resolve(&history, |version| !dropped.contains(&version))
        .map_err(EdgeQueryError::Version)?;
    if dropped.contains(&version) {
        return Err(EdgeQueryError::Version(VersionUnavailable::NotRetained(version)).into());
    }
    if !filter.topics.is_empty() {
        let topics = env.topic_ids(version).await?;
        let outside =
            filter.topics_outside(version, |topic| topics.contains(&topic).then_some(version));
        if !outside.is_empty() {
            return Err(EdgeQueryError::TopicsNotInVersion {
                version,
                topics: outside,
            }
            .into());
        }
    }
    Ok(version)
}

/// How a series read cuts its window into points: the window's start and
/// the step, in microseconds. A graph read has one point.
#[derive(Debug, Clone, Copy)]
pub(super) struct Points {
    pub start: i64,
    pub step: i64,
}

fn point(stored: i64) -> Result<usize, DbError> {
    usize::try_from(stored).map_err(|_| DbError::inconsistent("a row before the series window"))
}

fn edge_row(
    row: &sqlx::postgres::PgRow,
    point: usize,
    transmissions: u64,
) -> Result<EdgeRow, DbError> {
    let topic: Option<String> = row.try_get("topic")?;
    Ok(EdgeRow {
        from: codec::stored_agent(row.try_get("from_agent")?)?,
        to: codec::stored_agent(row.try_get("to_agent")?)?,
        route: codec::stored_route(row.try_get("route")?)?,
        topic: codec::stored_bucket_topic(topic.as_deref().unwrap_or(""))?,
        point,
        transmissions,
        bytes: codec::stored_sum("matched bytes", row.try_get("matched_bytes")?)?,
    })
}

/// The edge bucket sums of `version` in `window` per stored key (and
/// point, for a series), and under `Exclude` the false detections in it.
pub(super) async fn read_rows(
    conn: &mut PgConnection,
    version: TopicModelVersion,
    window: TimeWindow,
    points: Option<Points>,
    false_detections: FalseDetections,
) -> Result<Rows, DbError> {
    let start = codec::time(window.start())?;
    let end = codec::time(window.end())?;
    // A graph read is a series of one point as long as the window.
    let points = points.unwrap_or(Points {
        start,
        step: end - start,
    });
    let stored = codec::version(version);
    let buckets = sqlx::query(
        "SELECT from_agent, to_agent, route, topic, (bucket_start - $4) / $5 AS step_index, \
         sum(transmissions)::bigint AS transmissions, sum(matched_bytes)::bigint AS matched_bytes \
         FROM topology.edge_buckets WHERE version = $1 AND bucket_start >= $2 AND bucket_start < $3 \
         GROUP BY from_agent, to_agent, route, topic, step_index",
    )
    .bind(stored)
    .bind(start)
    .bind(end)
    .bind(points.start)
    .bind(points.step)
    .fetch_all(&mut *conn)
    .await?;
    let mut rows = Rows::default();
    for row in &buckets {
        let transmissions = codec::stored_sum("transmissions", row.try_get("transmissions")?)?;
        rows.buckets.push(edge_row(
            row,
            point(row.try_get("step_index")?)?,
            transmissions,
        )?);
    }
    if false_detections == FalseDetections::Exclude {
        let falses = sqlx::query(
            "SELECT c.from_agent, c.to_agent, c.route, c.topic, (c.at_micros - $4) / $5 AS step_index, \
             c.matched_bytes FROM topology.contributions c \
             JOIN topology.verdicts v ON v.transmission = c.transmission \
             WHERE c.version = $1 AND c.at_micros >= $2 AND c.at_micros < $3 AND v.verdict = 1",
        )
        .bind(stored)
        .bind(start)
        .bind(end)
        .bind(points.start)
        .bind(points.step)
        .fetch_all(&mut *conn)
        .await?;
        for row in &falses {
            rows.false_detections
                .push(edge_row(row, point(row.try_get("step_index")?)?, 1)?);
        }
    }
    Ok(rows)
}

/// The access bucket sums in `window`, per agent, resource and op.
pub(super) async fn read_accesses(
    conn: &mut PgConnection,
    window: TimeWindow,
) -> Result<Vec<AccessRow>, DbError> {
    let rows = sqlx::query(
        "SELECT agent, resource, op, sum(accesses)::bigint AS accesses FROM topology.access_buckets \
         WHERE bucket_start >= $1 AND bucket_start < $2 GROUP BY agent, resource, op",
    )
    .bind(codec::time(window.start())?)
    .bind(codec::time(window.end())?)
    .fetch_all(conn)
    .await?;
    rows.iter()
        .map(|row| {
            Ok(AccessRow {
                agent: codec::stored_agent(row.try_get("agent")?)?,
                resource: codec::stored_resource(row.try_get("resource")?)?,
                op: codec::stored_op(row.try_get("op")?)?,
                accesses: codec::stored_sum("accesses", row.try_get("accesses")?)?,
            })
        })
        .collect()
}

fn invalid(what: &str, error: impl std::fmt::Debug) -> DbError {
    DbError::inconsistent(format!("built an invalid {what}: {error:?}"))
}

impl<V: TopologyEnv> PgEdgeStore<V> {
    /// The watermark, the dropped versions and the resolved version, read
    /// at the start of a snapshot.
    async fn begin(
        &self,
        snapshot: &mut Snapshot,
        filter: &TopologyFilter,
    ) -> ReadResult<(Watermark, TopicModelVersion)> {
        let (watermark, dropped) = read_head(snapshot).await?;
        let version = resolve_version(self.env.as_ref(), &dropped, filter).await?;
        Ok((watermark, version))
    }

    /// The aligned graph read every graph-shaped query shares.
    pub(super) async fn graph_impl(
        &self,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> ReadResult<Watermarked<TopologyGraph>> {
        if !aligned(self.config.bucket_width, window) {
            return Err(EdgeQueryError::UnalignedWindow.into());
        }
        let mut snapshot = self.snapshot().await?;
        let (watermark, version) = self.begin(&mut snapshot, filter).await?;
        let rows = read_rows(
            &mut snapshot,
            version,
            window,
            None,
            filter.false_detections,
        )
        .await?;
        snapshot.commit().await?;
        let env = self.env.as_ref();
        let folded = fold::fold(env, filter, &rows)?;
        let edges = fold::edges(&folded, weighting)?;
        let endpoints: Vec<AgentId> = edges.iter().flat_map(|edge| [edge.from, edge.to]).collect();
        let nodes = fold::nodes(env, endpoints, [], &edges);
        let value = TopologyGraph::new(TopologyGraphParts {
            window,
            weighting,
            topic_version: version,
            nodes,
            edges,
        })
        .map_err(|error| invalid("graph", error))?;
        Ok(Watermarked { watermark, value })
    }

    pub(super) async fn totals_impl(
        &self,
        window: TimeWindow,
        filter: &TopologyFilter,
    ) -> ReadResult<Watermarked<EdgeTotals>> {
        let graph = self
            .graph_impl(window, Weighting::Transmissions, filter)
            .await?;
        Ok(Watermarked {
            watermark: graph.watermark,
            value: EdgeTotals::of(&graph.value),
        })
    }

    pub(super) async fn agent_traffic_impl(
        &self,
        window: TimeWindow,
        agents: &[AgentId],
    ) -> ReadResult<Watermarked<BTreeMap<AgentId, AgentTraffic>>> {
        let graph = self
            .graph_impl(window, Weighting::Transmissions, &TopologyFilter::default())
            .await?;
        let counts: BTreeMap<AgentId, AgentTraffic> = graph
            .value
            .nodes()
            .iter()
            .filter_map(|node| match node {
                GraphNode::Agent(agent) => Some((
                    agent.id,
                    AgentTraffic {
                        transmissions_in: agent.transmissions_in,
                        transmissions_out: agent.transmissions_out,
                    },
                )),
                GraphNode::Channel(_) => None,
            })
            .collect();
        let value = agents
            .iter()
            .map(|listed| {
                let canonical = self.env.canonical_agent(*listed);
                (*listed, counts.get(&canonical).copied().unwrap_or_default())
            })
            .collect();
        Ok(Watermarked {
            watermark: graph.watermark,
            value,
        })
    }

    pub(super) async fn channel_topology_impl(
        &self,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> ReadResult<Watermarked<BipartiteGraph>> {
        if !aligned(self.config.bucket_width, window) {
            return Err(EdgeQueryError::UnalignedWindow.into());
        }
        let mut snapshot = self.snapshot().await?;
        let (watermark, version) = self.begin(&mut snapshot, filter).await?;
        let rows = read_rows(
            &mut snapshot,
            version,
            window,
            None,
            filter.false_detections,
        )
        .await?;
        let access_rows = read_accesses(&mut snapshot, window).await?;
        snapshot.commit().await?;
        let env = self.env.as_ref();
        let folded = fold::fold(env, filter, &rows)?;
        let transmissions = fold::edges(&folded, weighting)?;
        let topics = fold::channel_topics(env, filter.false_detections, &rows)?;
        let accesses = fold::access_edges(env, filter, &access_rows, &topics)?;
        let agents: Vec<AgentId> = accesses
            .iter()
            .map(|access| access.agent)
            .chain(transmissions.iter().flat_map(|edge| [edge.from, edge.to]))
            .collect();
        let channels: BTreeSet<_> = accesses
            .iter()
            .map(|access| access.channel)
            .chain(transmissions.iter().filter_map(|edge| match edge.route {
                Route::Channel(channel) => Some(channel),
                Route::Delegation(_) | Route::Direct(_) | Route::Unobserved => None,
            }))
            .collect();
        let nodes = fold::nodes(env, agents, channels, &transmissions);
        let value = BipartiteGraph::new(BipartiteParts {
            window,
            weighting,
            topic_version: version,
            nodes,
            accesses,
            transmissions,
        })
        .map_err(|error| invalid("channel graph", error))?;
        Ok(Watermarked { watermark, value })
    }

    pub(super) async fn series_impl(
        &self,
        grid: SeriesGrid,
        weighting: Weighting,
        grouping: SeriesGrouping,
        filter: &TopologyFilter,
    ) -> ReadResult<Watermarked<TopologySeries>> {
        let store = self.config.bucket_width;
        if grid.step().bucket() != store {
            return Err(EdgeQueryError::BucketWidthMismatch {
                store,
                grid: grid.step().bucket(),
            }
            .into());
        }
        let points = Points {
            start: codec::time(grid.window().start())?,
            step: codec::micros("series step", grid.step().as_micros().get())?,
        };
        let mut snapshot = self.snapshot().await?;
        let (watermark, version) = self.begin(&mut snapshot, filter).await?;
        let rows = read_rows(
            &mut snapshot,
            version,
            grid.window(),
            Some(points),
            filter.false_detections,
        )
        .await?;
        snapshot.commit().await?;
        let folded = fold::fold(self.env.as_ref(), filter, &rows)?;
        let count = usize::try_from(grid.points().get())
            .map_err(|_| DbError::inconsistent("more points than memory"))?;
        let groups = fold::series_groups(&folded, count, weighting, grouping);
        let value = TopologySeries::new(grid, weighting, version, groups)
            .map_err(|error| invalid("series", error))?;
        Ok(Watermarked { watermark, value })
    }
}
