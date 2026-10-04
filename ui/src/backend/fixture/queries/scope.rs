//! The scope filter, exactly as `contract/scope.rs` documents it: empty
//! lists do not restrict, non-empty lists combine with AND, agents match the
//! sender OR the reader after alias resolution, channels match after
//! supersession, topics are read under the scope's version and outliers
//! never match a topic filter.

use std::collections::HashSet;

use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::{AgentId, ChannelId, TopicId};
use crosstalk_spec::support::TimeWindow;

use crate::backend::Result;
use crate::contract::graph::route_kind;
use crate::contract::scope::{Scope, VerdictFilter};
use crate::contract::verdict::Verdict;

use super::{Ctx, retained};
use crate::backend::fixture::world::TxRecord;

pub struct Filter<'a> {
    pub ctx: &'a Ctx<'a>,
    pub window: TimeWindow,
    pub version: TopicModelVersion,
    agents: HashSet<AgentId>,
    channels: HashSet<ChannelId>,
    routes: HashSet<RouteKind>,
    topics: HashSet<TopicId>,
    exclude_false: bool,
}

impl<'a> Filter<'a> {
    /// Fails with `VersionNotRetained` for a version the world does not
    /// keep.
    pub fn new(ctx: &'a Ctx<'a>, scope: &Scope) -> Result<Self> {
        retained(ctx.world, scope.topic_version)?;
        let f = &scope.filter;
        Ok(Self {
            ctx,
            window: scope.window,
            version: scope.topic_version,
            agents: f.agents.iter().map(|a| ctx.agent(*a)).collect(),
            channels: f.channels.iter().map(|c| ctx.channel(*c)).collect(),
            routes: f.route_kinds.iter().copied().collect(),
            topics: f.topics.iter().copied().collect(),
            exclude_false: f.verdicts == VerdictFilter::ExcludeFalseDetections,
        })
    }

    /// Whether a transmission is in the scope. Unconfirmed transmissions
    /// match an agent filter by their reader only, and never match a topic
    /// filter.
    pub fn keeps(&self, record: &TxRecord) -> bool {
        let t = &record.transmission;
        if !self.window.contains(t.opened_at) {
            return false;
        }
        if !self.agents.is_empty() {
            let to = self.ctx.agent(t.to);
            let from = record.from.map(|f| self.ctx.agent(f));
            if !self.agents.contains(&to) && !from.is_some_and(|f| self.agents.contains(&f)) {
                return false;
            }
        }
        if !self.channels.is_empty() {
            match &t.route {
                Route::Channel(c) if self.channels.contains(&self.ctx.channel(*c)) => {}
                _ => return false,
            }
        }
        if !self.routes.is_empty() && !self.routes.contains(&route_kind(&t.route)) {
            return false;
        }
        if !self.topics.is_empty()
            && !record
                .topic(self.version)
                .is_some_and(|topic| self.topics.contains(&topic))
        {
            return false;
        }
        if self.exclude_false && self.ctx.verdict(t.id) == Some(Verdict::FalseDetection) {
            return false;
        }
        true
    }

    /// Whether an access by `agent` on `channel` (both resolved) passes the
    /// agent, channel and route-kind parts of the filter.
    pub fn keeps_access(&self, agent: AgentId, channel: ChannelId) -> bool {
        (self.agents.is_empty() || self.agents.contains(&agent))
            && (self.channels.is_empty() || self.channels.contains(&channel))
            && (self.routes.is_empty() || self.routes.contains(&RouteKind::Channel))
    }

    pub fn has_topics(&self) -> bool {
        !self.topics.is_empty()
    }
}
