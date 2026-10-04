//! Topics, their stats and remaps, and stored projections.
//!
//! A projection is a deterministic stand-in for UMAP: each topic (by theme)
//! is a 2-D Gaussian cluster on a ring, outliers are scattered noise, and
//! the sample and positions follow from the scope and `params.seed`.

use std::collections::{BTreeSet, HashMap};
use std::num::NonZeroU32;

use crosstalk_spec::aggregates::topic::{Topic, TopicModelVersion};
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::{AgentId, ChannelId, TopicId};

use crate::backend::Result;
use crate::backend::fixture::clock::NOW;
use crate::backend::fixture::rng::Rng;
use crate::backend::fixture::text::Theme;
use crate::backend::fixture::world::TxRecord;
use crate::contract::ProjectionId;
use crate::contract::errors::QueryError;
use crate::contract::graph::route_kind;
use crate::contract::research::{
    PointCategories, ProjectionMeta, ProjectionParams, ProjectionPoints,
};
use crate::contract::scope::Scope;
use crate::contract::topics::{TopicStats, TopicVersionRemap};

use super::graph::{bucket_of, buckets};
use super::scope::Filter;
use super::{Ctx, retained};

pub fn topics(ctx: &Ctx, version: TopicModelVersion) -> Result<Vec<Topic>> {
    retained(ctx.world, version)?;
    Ok(ctx.world.topics.topics_of(version).cloned().collect())
}

pub fn remap(ctx: &Ctx, from: TopicModelVersion) -> Result<Option<TopicVersionRemap>> {
    retained(ctx.world, from)?;
    Ok(ctx
        .world
        .topics
        .remaps
        .iter()
        .find(|r| r.from == from)
        .cloned())
}

/// Per topic of the scope's version (then outliers): confirmed
/// transmissions in scope, in total and per timeline bucket.
pub fn stats(ctx: &Ctx, scope: &Scope, n: NonZeroU32) -> Result<Vec<TopicStats>> {
    let filter = Filter::new(ctx, scope)?;
    let windows = buckets(scope.window, n);
    let mut rows: Vec<TopicStats> = ctx
        .world
        .topics
        .topics_of(scope.topic_version)
        .map(|t| TopicStats {
            topic: Some(t.id),
            transmissions: 0,
            trend: vec![0; windows.len()],
        })
        .chain(std::iter::once(TopicStats {
            topic: None,
            transmissions: 0,
            trend: vec![0; windows.len()],
        }))
        .collect();
    let index: HashMap<Option<TopicId>, usize> =
        rows.iter().enumerate().map(|(i, r)| (r.topic, i)).collect();
    for record in ctx.world.transmissions.iter() {
        if !record.is_confirmed() || !filter.keeps(record) {
            continue;
        }
        let Some(row) = index
            .get(&record.topic(scope.topic_version))
            .and_then(|i| rows.get_mut(*i))
        else {
            continue;
        };
        row.transmissions += 1;
        if let Some(slot) =
            bucket_of(&windows, record.transmission.opened_at).and_then(|b| row.trend.get_mut(b))
        {
            *slot += 1;
        }
    }
    Ok(rows)
}

fn center(theme: Theme) -> (f64, f64) {
    let angle = std::f64::consts::TAU * theme.index() as f64 / Theme::ALL.len() as f64;
    (6.0 * angle.cos(), 6.0 * angle.sin())
}

fn index_of<T: Ord + Copy>(table: &[T], value: T) -> Option<u32> {
    table
        .binary_search(&value)
        .ok()
        .and_then(|i| u32::try_from(i).ok())
}

/// Fits a projection of the scope's confirmed transmissions.
pub fn project(
    ctx: &Ctx,
    scope: &Scope,
    params: ProjectionParams,
    id: ProjectionId,
) -> Result<ProjectionPoints> {
    let filter = Filter::new(ctx, scope)?;
    let mut sample: Vec<&TxRecord> = ctx
        .world
        .transmissions
        .iter()
        .filter(|r| r.is_confirmed() && filter.keeps(r))
        .collect();
    sample.sort_by_key(|r| r.transmission.id);
    let limit = usize::try_from(params.sample_limit.get()).unwrap_or(usize::MAX);
    if sample.len() > limit {
        Rng::new(params.seed).shuffle(&mut sample);
        sample.truncate(limit);
        sample.sort_by_key(|r| r.transmission.id);
    }

    let resolved: Vec<(AgentId, AgentId, Option<ChannelId>)> = sample
        .iter()
        .map(|r| {
            let channel = match ctx.route(&r.transmission.route) {
                Route::Channel(c) => Some(c),
                _ => None,
            };
            let from = r.from.map_or(r.transmission.to, |f| ctx.agent(f));
            (from, ctx.agent(r.transmission.to), channel)
        })
        .collect();
    let agents: Vec<AgentId> = resolved
        .iter()
        .flat_map(|(f, t, _)| [*f, *t])
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let channels: Vec<ChannelId> = resolved
        .iter()
        .filter_map(|(_, _, c)| *c)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let topics: Vec<TopicId> = sample
        .iter()
        .filter_map(|r| r.topic(scope.topic_version))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    let spread = 0.35 + 0.9 * f64::from(params.min_dist());
    let (mut xs, mut ys, mut categories) = (Vec::new(), Vec::new(), Vec::new());
    for (record, (from, to, channel)) in sample.iter().zip(&resolved) {
        let raw = record.transmission.id.as_ulid();
        let mut rng = Rng::new(params.seed ^ (raw as u64) ^ ((raw >> 64) as u64));
        let topic = record.topic(scope.topic_version);
        let (x, y) = match topic {
            Some(_) => {
                let (cx, cy) = center(record.theme);
                (cx + rng.gaussian() * spread, cy + rng.gaussian() * spread)
            }
            None => (rng.unit() * 18.0 - 9.0, rng.unit() * 18.0 - 9.0),
        };
        xs.push(x as f32);
        ys.push(y as f32);
        let missing = || QueryError::Store {
            reason: "projection table index".to_owned(),
        };
        categories.push(PointCategories {
            sender: index_of(&agents, *from).ok_or_else(missing)?,
            reader: index_of(&agents, *to).ok_or_else(missing)?,
            route: route_kind(&record.transmission.route),
            channel: channel.and_then(|c| index_of(&channels, c)),
            topic: topic.and_then(|t| index_of(&topics, t)),
        });
    }
    let meta = ProjectionMeta {
        id,
        scope: scope.clone(),
        params,
        embedding_model: ctx.world.topics.model.clone(),
        fitted_at: NOW,
    };
    ProjectionPoints::new(
        meta,
        sample.iter().map(|r| r.transmission.id).collect(),
        xs,
        ys,
        categories,
        agents,
        channels,
        topics,
    )
    .map_err(|e| QueryError::Store {
        reason: e.to_string(),
    })
}
