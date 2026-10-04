//! The rows of each dataset, as `export::rows` defines them, read from the
//! world under one query context (the agent and channel resolution and
//! the verdict copy captured when the export is planned) and returned in
//! ascending `RowKey` order.
//!
//! - Transmissions: every confirmed transmission confirmed in the settled
//!   window that the filter admits, self-edges included
//!   ([`Linked::admitted`]), as `TransmissionRow::of` builds it; with
//!   content, its match quotes cut with `ExcerptWindow::MATCH_ONLY` from
//!   the same evidence the evidence page assembles.
//! - Edges: what `topology` counts ([`Linked::counted`]), summed per bucket,
//!   sender, reader, resolved route and topic.
//! - Accesses: what `channel_topology` keeps, summed per bucket, agent,
//!   channel and operation.
//! - Topics: the version's topics (the filter's, or all), each with the
//!   admitted transmissions assigned to it, zero included.
//! - Projection: the stored frame (`projection_rows`).
//! - Verdicts: every record of each judgeable transmission opened in the
//!   settled window (`verdict_rows`).

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU64;

use crosstalk_spec::aggregates::edge::EdgeSelector;
use crosstalk_spec::aggregates::projection::Projection;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::{AgentId, ChannelId, TopicId};
use crosstalk_spec::interfaces::l8_surface::excerpt::ExcerptWindow;
use crosstalk_spec::interfaces::l8_surface::export::rows::{
    AccessRow, EdgeRow, LabelContent, TopicContent, TopicRow, TransmissionContent, TransmissionRow,
    projection_rows, verdict_rows,
};
use crosstalk_spec::interfaces::l8_surface::export::{ExportPlanError, ExportRow};
use crosstalk_spec::interfaces::l8_surface::summary::TopicUnder;
use crosstalk_spec::support::{TimeWindow, Timestamp};

use crate::backend::fixture::clock::BUCKET;
use crate::backend::fixture::queries::linked::{Counted, Linked};
use crate::backend::fixture::queries::transmissions::topic_under;
use crate::backend::fixture::queries::{Ctx, evidence, route_key};

fn store(what: &str, detail: impl std::fmt::Debug) -> ExportPlanError {
    ExportPlanError::Store {
        reason: format!("fixture export: {what}: {detail:?}"),
    }
}

/// `rows` in ascending `RowKey` order.
fn sorted(mut rows: Vec<ExportRow>) -> Vec<ExportRow> {
    rows.sort_by_cached_key(ExportRow::key);
    rows
}

/// The label of each topic of `version`.
pub fn labels(ctx: &Ctx, version: TopicModelVersion) -> HashMap<TopicId, String> {
    ctx.world
        .topics
        .topics_of(version)
        .map(|topic| (topic.id, topic.label.clone()))
        .collect()
}

/// The bucket holding `at`.
fn bucket(at: Timestamp) -> Result<TimeWindow, ExportPlanError> {
    let width = BUCKET.as_micros().get();
    let start = at.as_micros() - at.as_micros() % width;
    TimeWindow::new(
        Timestamp::from_micros(start),
        Timestamp::from_micros(start.saturating_add(width)),
    )
    .map_err(|e| store("bucket", e))
}

pub fn transmissions(linked: &Linked, content: bool) -> Result<Vec<ExportRow>, ExportPlanError> {
    let ctx = linked.ctx;
    let labels = labels(ctx, linked.version);
    let mut rows = Vec::new();
    for counted in linked.admitted() {
        let record = counted.record;
        let topic = topic_under(record, linked.version);
        let quoted = if content {
            let found = evidence::evidence(ctx, record.transmission.id, ExcerptWindow::MATCH_ONLY)
                .map_err(|e| store("evidence", e))?
                .ok_or_else(|| store("evidence missing", record.transmission.id))?;
            let label = match topic {
                TopicUnder::Topic(topic) => labels.get(&topic).cloned(),
                TopicUnder::Outlier | TopicUnder::Unassigned => None,
            };
            Some(
                TransmissionContent::of(&found, label)
                    .ok_or_else(|| store("confirmed without matches", record.transmission.id))?,
            )
        } else {
            None
        };
        let row = TransmissionRow::of(
            &record.transmission,
            ctx.aliases(),
            |id| ctx.verdict(id),
            |_| topic,
            quoted,
        )
        .map_err(|e| store("transmission row", e))?;
        rows.push(ExportRow::Transmission(Box::new(row)));
    }
    Ok(sorted(rows))
}

type EdgeKey = (u64, AgentId, AgentId, (u8, u128, String), Option<TopicId>);

pub fn edges(linked: &Linked, content: bool) -> Result<Vec<ExportRow>, ExportPlanError> {
    let labels = labels(linked.ctx, linked.version);
    let mut sums: BTreeMap<EdgeKey, (Route, u64, u64)> = BTreeMap::new();
    for counted in linked.counted() {
        let start = bucket(counted.at)?.start().as_micros();
        let entry = sums
            .entry((
                start,
                counted.from,
                counted.to,
                route_key(&counted.route),
                counted.topic,
            ))
            .or_insert_with(|| (counted.route.clone(), 0, 0));
        entry.1 = entry.1.saturating_add(1);
        entry.2 = entry.2.saturating_add(counted.matched_bytes.get());
    }
    let rows = sums
        .into_iter()
        .map(|((start, from, to, _, topic), (route, n, bytes))| {
            Ok(ExportRow::Edge(EdgeRow {
                edge: EdgeSelector::new(from, to, route).map_err(|e| store("self-edge", e))?,
                topic,
                bucket: bucket(Timestamp::from_micros(start))?,
                transmissions: NonZeroU64::new(n).ok_or_else(|| store("empty edge", from))?,
                matched_bytes: NonZeroU64::new(bytes)
                    .ok_or_else(|| store("edge without bytes", from))?,
                content: content.then(|| LabelContent {
                    topic_label: topic.and_then(|topic| labels.get(&topic).cloned()),
                }),
            }))
        })
        .collect::<Result<Vec<_>, ExportPlanError>>()?;
    Ok(sorted(rows))
}

pub fn accesses(linked: &Linked) -> Result<Vec<ExportRow>, ExportPlanError> {
    let ctx = linked.ctx;
    let topics = linked.channel_topics();
    let mut sums: BTreeMap<(u64, AgentId, ChannelId, bool), (AccessKind, u64)> = BTreeMap::new();
    for access in &ctx.world.accesses {
        if !linked.in_window(access.at) {
            continue;
        }
        let Some(raw) = ctx.world.resource_channel.get(&access.resource) else {
            continue;
        };
        let (agent, channel) = (ctx.agent(access.agent), ctx.channel(*raw));
        if !linked.admits_access(agent, channel, &topics) {
            continue;
        }
        let op = access.op.kind();
        let start = bucket(access.at)?.start().as_micros();
        let entry = sums
            .entry((start, agent, channel, matches!(op, AccessKind::Read)))
            .or_insert((op, 0));
        entry.1 = entry.1.saturating_add(1);
    }
    let rows = sums
        .into_iter()
        .map(|((start, agent, channel, _), (op, n))| {
            Ok(ExportRow::Access(AccessRow {
                agent,
                channel,
                op,
                bucket: bucket(Timestamp::from_micros(start))?,
                accesses: NonZeroU64::new(n).ok_or_else(|| store("empty access", agent))?,
            }))
        })
        .collect::<Result<Vec<_>, ExportPlanError>>()?;
    Ok(sorted(rows))
}

/// One row per topic of `version` that `listed` names (every one when it
/// names none), counting the `admitted` transmissions assigned to it.
pub fn topics(
    ctx: &Ctx,
    listed: &[TopicId],
    version: TopicModelVersion,
    admitted: &[Counted],
    content: bool,
) -> Vec<ExportRow> {
    let mut tallies: HashMap<TopicId, (u64, u64)> = HashMap::new();
    for counted in admitted {
        if let Some(topic) = counted.topic {
            let tally = tallies.entry(topic).or_default();
            tally.0 = tally.0.saturating_add(1);
            tally.1 = tally.1.saturating_add(counted.matched_bytes.get());
        }
    }
    let rows = ctx
        .world
        .topics
        .topics_of(version)
        .filter(|topic| listed.is_empty() || listed.contains(&topic.id))
        .map(|topic| {
            let (transmissions, matched_bytes) =
                tallies.get(&topic.id).copied().unwrap_or_default();
            ExportRow::Topic(TopicRow {
                topic: topic.id,
                transmissions,
                matched_bytes,
                content: content.then(|| TopicContent {
                    label: topic.label.clone(),
                    terms: topic.terms.clone(),
                }),
            })
        })
        .collect();
    sorted(rows)
}

/// The stored frame's points, labelled under the projection's version
/// when the request includes content.
pub fn points(ctx: &Ctx, projection: &Projection, content: bool) -> Vec<ExportRow> {
    let labels = content.then(|| labels(ctx, projection.topic_version()));
    projection_rows(projection, labels.as_ref())
}

/// Every verdict record of the judgeable transmissions opened in
/// `settled`.
pub fn verdicts(ctx: &Ctx, settled: TimeWindow) -> Result<Vec<ExportRow>, ExportPlanError> {
    let mut rows = Vec::new();
    for (id, log) in &ctx.state.verdicts {
        let Some(record) = ctx.world.tx(*id) else {
            continue;
        };
        if !settled.contains(record.transmission.opened_at) {
            continue;
        }
        rows.extend(verdict_rows(&record.transmission, log).map_err(|e| store("verdicts", e))?);
    }
    Ok(sorted(rows))
}
